//! The `history.revisions.prune` dedup record, on the store's own pool.
//!
//! Connect calls the store only after the port returned, so the prune is already committed when
//! its outcome is written. A record-only store would leave the window between the two open: a
//! same-key retry after a crash in that gap would execute against the current state and delete
//! checkpoints the first call never saw. [`SqliteHistoryDedupStore::lookup`] therefore claims the
//! key first; a claim with no outcome answers `Evicted`, which Connect turns into
//! `RECEIPT_EXPIRED` without reaching the port.
//!
//! Faults fail closed: a statement fault, and an outcome that does not decode, both answer
//! `Evicted`. Re-executing a destructive prune is the outcome this store exists to prevent.
//!
//! Each store serves one namespace — the collection its history surface routes to — and keeps it in
//! a column beside the key rather than in a framed string: a collection name may contain any
//! character, so no separator could keep `(namespace, key)` pairs distinct. Two history surfaces on
//! one database can share a wire scope, a principal and an idempotency key while reading different
//! collections; without the namespace the second would replay a prune of the first surface's
//! collection, or refuse it.
//!
//! One serving process per database is the host's obligation (see `I-000109`): the claim row stops
//! a second process from executing, but it answers `RECEIPT_EXPIRED` instead of waiting for the
//! first, so two processes would turn a replay into a refusal.
//!
//! Each principal holds at most [`HISTORY_DEDUP_CAPACITY`] rows per namespace, claims and recorded
//! outcomes alike. Nothing is ever deleted and no clock is read: the table is a hard bound, not a
//! working set that ages out, so a recorded outcome replays for the database's lifetime and no
//! forward clock jump can turn a live dedup record into an executed prune. At the bound a fresh key
//! answers `Evicted` — Connect's `RECEIPT_EXPIRED` — without the prune being executed.
//!
//! The quota is per caller, parsed out of the retry identity, so one high-churn principal can
//! exhaust only its own quota and never disable prune for another on the same collection. A key
//! that is not the canonical retry identity is accounted to
//! [`HISTORY_DEDUP_UNKNOWN_PRINCIPAL`] rather than to itself, so a stream of such keys shares one
//! quota instead of minting a fresh one per key. A host that wants more headroom raises
//! [`HISTORY_DEDUP_CAPACITY`] or recreates the database; there is no eviction to wait for.
//!
//! Claim capacity belongs to `lookup`: it inserts the claim and refuses an unseen key at the bound.
//! `record` only fills a claim that already exists, so a caller that records without claiming
//! writes nothing and can never produce an outcome for a prune that did not run.

use std::io::Write;

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::SqlitePool;

use spoke_connect::remote::{HistoryDedupLookup, HistoryDedupRecord, HistoryDedupStore};
use spoke_operations::{SpokeReject, SpokeRejectCode, SpokeResult};

/// The most rows one principal may hold in one namespace. A claim counts, because a claim is what
/// keeps a replay out of the port; a principal at the bound refuses unseen keys instead of inserting
/// past it.
const HISTORY_DEDUP_CAPACITY: i64 = 4096;

/// The quota bucket every key that is not the canonical retry identity is accounted to.
///
/// One shared bucket, not one per key: a per-key fallback would let a caller mint unparsable keys
/// and spend a fresh 4096-row quota with each, which is the hard bound bypassed.
const HISTORY_DEDUP_UNKNOWN_PRINCIPAL: &str = "unknown";

/// The requester a dedup key names, for the per-principal quota.
///
/// The key is the RFC 8785 canonical serialization of `[op, effective_scope, principal_id,
/// idempotency_key]`, so the principal is the third element and a string. Anything else — an
/// unparsable key, a truncated array, a non-string member — is accounted to
/// [`HISTORY_DEDUP_UNKNOWN_PRINCIPAL`], so every such key shares one quota and none of them can
/// widen the bound. Connect canonicalizes before it calls, so a well-formed deployment never
/// reaches that bucket.
fn principal_of(key: &str) -> String {
    serde_json::from_str::<Value>(key)
        .ok()
        .and_then(|value| {
            value
                .as_array()
                .and_then(|members| members.get(2))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| HISTORY_DEDUP_UNKNOWN_PRINCIPAL.to_owned())
}

/// One dedup record per retry identity, in the database file the knowledge store already serves
/// with, under the namespace of the collection this surface routes to. The pool holds a single
/// connection, so the claim, the port call and the record never hold two at once: each takes and
/// releases its own.
pub(crate) struct SqliteHistoryDedupStore {
    pool: SqlitePool,
    namespace: String,
}

impl SqliteHistoryDedupStore {
    /// `namespace` is the collection the routed store serves: it is stored beside the key in its
    /// own column, so two surfaces on one database that route to different collections never share
    /// a row, whatever characters either name contains.
    pub(crate) fn new(pool: SqlitePool, namespace: impl Into<String>) -> Self {
        Self {
            pool,
            namespace: namespace.into(),
        }
    }
}

#[async_trait]
impl HistoryDedupStore for SqliteHistoryDedupStore {
    async fn lookup(&self, key: &str) -> HistoryDedupLookup {
        let mut conn = match self.pool.acquire().await {
            Ok(conn) => conn,
            Err(error) => return fault("claim", error),
        };
        let principal = principal_of(key);
        // Claim and read on the same connection: the insert decides whether the row is this
        // caller's, and the read shows what the row holds — `NULL`s for a claim still in flight.
        // The insert is also the capacity gate: a principal already at the bound takes no new row,
        // so an unseen key stays unseen here and the read below answers `Evicted`. An existing key
        // is unaffected either way, so a replay still finds its recorded outcome at the bound.
        let claimed = match sqlx::query(
            "INSERT INTO history_dedup (namespace, key, principal) SELECT ?, ?, ? \
             WHERE (SELECT COUNT(*) FROM history_dedup \
                    WHERE namespace = ? AND principal = ?) < ? \
             ON CONFLICT(namespace, key) DO NOTHING",
        )
        .bind(&self.namespace)
        .bind(key)
        .bind(&principal)
        .bind(&self.namespace)
        .bind(&principal)
        .bind(HISTORY_DEDUP_CAPACITY)
        .execute(&mut *conn)
        .await
        {
            Ok(result) => result.rows_affected() == 1,
            Err(error) => return fault("claim", error),
        };
        let stored: Result<Option<(Option<String>, Option<String>)>, _> = sqlx::query_as(
            "SELECT payload_digest, outcome_json FROM history_dedup \
             WHERE namespace = ? AND key = ?",
        )
        .bind(&self.namespace)
        .bind(key)
        .fetch_optional(&mut *conn)
        .await;
        let row = match stored {
            Ok(row) => row,
            Err(error) => return fault("lookup", error),
        };
        if claimed {
            return HistoryDedupLookup::Unseen;
        }
        match row {
            // Another caller's unanswered claim, one the process never settled, or an unseen key
            // the namespace had no room to claim.
            Some((None, None)) | None => HistoryDedupLookup::Evicted,
            Some((Some(payload_digest), Some(outcome_json))) => match decode_outcome(&outcome_json)
            {
                Some(outcome) => HistoryDedupLookup::Recorded(HistoryDedupRecord {
                    payload_digest,
                    outcome,
                }),
                None => HistoryDedupLookup::Evicted,
            },
            // The `CHECK` keeps the two columns in step; a row that breaks it is not an outcome
            // this store wrote, so it never replays.
            Some(_) => HistoryDedupLookup::Evicted,
        }
    }

    async fn record(&self, key: &str, record: HistoryDedupRecord) {
        let outcome_json = encode_outcome(&record.outcome);
        let principal = principal_of(key);
        // Only fill a claim `lookup` already inserted: the `EXISTS` gate is the whole condition.
        // A trait caller that records without a claim writes nothing, so it can neither insert an
        // outcome for a key no prune ran under nor grow the table; claim capacity is `lookup`'s,
        // which refuses an unseen key at the bound. The upsert still replaces an outcome the same
        // key already recorded.
        let written = sqlx::query(
            "INSERT INTO history_dedup (namespace, key, principal, payload_digest, outcome_json) \
             SELECT ?, ?, ?, ?, ? \
             WHERE EXISTS (SELECT 1 FROM history_dedup WHERE namespace = ? AND key = ?) \
             ON CONFLICT(namespace, key) DO UPDATE SET \
               payload_digest = excluded.payload_digest, \
               outcome_json   = excluded.outcome_json",
        )
        .bind(&self.namespace)
        .bind(key)
        .bind(&principal)
        .bind(&record.payload_digest)
        .bind(&outcome_json)
        .bind(&self.namespace)
        .bind(key)
        .execute(&self.pool)
        .await;
        if let Err(error) = written {
            // The claim stays in place, so the key answers `RECEIPT_EXPIRED` rather than
            // re-executing. The diagnostic names local columns and stays host-side.
            let _ = writeln!(
                std::io::stderr(),
                "mindcore history dedup record fault: {error}"
            );
        }
    }
}

/// A storage fault under `lookup`. The trait carries no error channel, so the fault is reported on
/// the host's stderr and answered as `Evicted`: a prune is never re-executed on a storage fault.
fn fault(stage: &str, diagnostic: impl std::fmt::Display) -> HistoryDedupLookup {
    let _ = writeln!(
        std::io::stderr(),
        "mindcore history dedup {stage} fault: {diagnostic}"
    );
    HistoryDedupLookup::Evicted
}

/// `SpokeResult`, `SpokeReject` and `SpokeRejectCode` carry no serde implementation, so the stored
/// shape is written here: `{"ok": <value>}` or
/// `{"reject": {"code", "message", "details"}}`, the code as its wire string. `details: None`
/// stores as `null`, so an empty `details` object never reads back as `None`.
fn encode_outcome(outcome: &SpokeResult<Value>) -> String {
    let stored = match outcome {
        SpokeResult::Ok(value) => json!({ "ok": value }),
        SpokeResult::Reject(reject) => json!({
            "reject": {
                "code": reject.code.as_str(),
                "message": reject.message,
                "details": reject.details,
            }
        }),
    };
    stored.to_string()
}

/// The inverse of [`encode_outcome`].
///
/// Strict: exactly one variant key, and a reject carries exactly `code`, `message` and `details`.
/// It rejects any value that does not match the shape this writer emits, so a half-written or
/// hand-edited row answers `Evicted` rather than replaying. This is a shape check, not authenticity
/// or tamper detection: nothing here authenticates the row, and a writer with database access can
/// store any well-formed outcome it likes.
fn decode_outcome(stored: &str) -> Option<SpokeResult<Value>> {
    let value: Value = serde_json::from_str(stored).ok()?;
    let object = value.as_object()?;
    if object.len() != 1 {
        return None;
    }
    if let Some(ok) = object.get("ok") {
        return Some(SpokeResult::Ok(ok.clone()));
    }
    let reject = object.get("reject")?.as_object()?;
    if reject.len() != 3 {
        return None;
    }
    let code = SpokeRejectCode::try_from_str(reject.get("code")?.as_str()?)?;
    let message = reject.get("message")?.as_str()?.to_string();
    let details = match reject.get("details")? {
        Value::Null => None,
        Value::Object(map) => Some(map.clone()),
        _ => return None,
    };
    Some(SpokeResult::Reject(SpokeReject {
        code,
        message,
        details,
    }))
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Map};
    use tempfile::TempDir;

    use mindcore_sqlite::MindStore;

    use super::*;

    fn record(payload_digest: &str, outcome: SpokeResult<Value>) -> HistoryDedupRecord {
        HistoryDedupRecord {
            payload_digest: payload_digest.to_string(),
            outcome,
        }
    }

    fn recorded(lookup: HistoryDedupLookup, context: &str) -> HistoryDedupRecord {
        match lookup {
            HistoryDedupLookup::Recorded(record) => record,
            other => panic!("{context}: expected a recorded outcome, got {other:?}"),
        }
    }

    /// A key in the shape Connect sends: the RFC 8785 serialization of the four-member retry
    /// identity. The per-principal quota reads the third member.
    fn key_for(principal: &str, idempotency_key: &str) -> String {
        json!([
            "history.revisions.prune",
            { "scope_id": "scope-one" },
            principal,
            idempotency_key
        ])
        .to_string()
    }

    /// One fresh database file through the production open path, and the store bound to its pool.
    async fn open(dir: &TempDir) -> (MindStore, SqliteHistoryDedupStore) {
        open_in(dir, "collection-one").await
    }

    /// As [`open`], with the namespace the history surface's collection supplies.
    async fn open_in(dir: &TempDir, namespace: &str) -> (MindStore, SqliteHistoryDedupStore) {
        let store = MindStore::open(&dir.path().join("history_dedup.db"))
            .await
            .expect("open the store");
        let dedup = SqliteHistoryDedupStore::new(store.pool(), namespace);
        (store, dedup)
    }

    async fn row_count(store: &MindStore, namespace: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM history_dedup WHERE namespace = ?")
            .bind(namespace)
            .fetch_one(&store.pool())
            .await
            .expect("count the namespace's rows")
    }

    async fn principal_row_count(store: &MindStore, namespace: &str, principal: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM history_dedup WHERE namespace = ? AND principal = ?",
        )
        .bind(namespace)
        .bind(principal)
        .fetch_one(&store.pool())
        .await
        .expect("count the principal's rows")
    }

    #[tokio::test]
    async fn a_first_lookup_answers_unseen() {
        let dir = TempDir::new().expect("temporary directory");
        let (_store, dedup) = open(&dir).await;
        assert_eq!(dedup.lookup("key-one").await, HistoryDedupLookup::Unseen);
    }

    #[tokio::test]
    async fn an_ok_outcome_round_trips() {
        let dir = TempDir::new().expect("temporary directory");
        let (_store, dedup) = open(&dir).await;
        let written = record(
            "digest-ok",
            SpokeResult::Ok(json!({ "deleted": 2, "floor_revision": 3 })),
        );
        dedup.lookup("key-one").await;
        dedup.record("key-one", written.clone()).await;
        assert_eq!(recorded(dedup.lookup("key-one").await, "ok"), written);
    }

    #[tokio::test]
    async fn a_reject_with_details_round_trips() {
        let dir = TempDir::new().expect("temporary directory");
        let (_store, dedup) = open(&dir).await;
        let mut details = Map::new();
        details.insert("entity_kind".to_string(), json!("knowledge_entry"));
        let written = record(
            "digest-reject",
            SpokeResult::Reject(SpokeReject {
                code: SpokeRejectCode::HistoryTruncated,
                message: "the range starts below the retained floor".to_string(),
                details: Some(details),
            }),
        );
        dedup.lookup("key-one").await;
        dedup.record("key-one", written.clone()).await;
        assert_eq!(recorded(dedup.lookup("key-one").await, "reject"), written);
    }

    #[tokio::test]
    async fn a_reject_without_details_round_trips_as_none() {
        let dir = TempDir::new().expect("temporary directory");
        let (_store, dedup) = open(&dir).await;
        let written = record(
            "digest-null-details",
            SpokeResult::Reject(SpokeReject {
                code: SpokeRejectCode::InvalidInput,
                message: "keep must be at least one".to_string(),
                details: None,
            }),
        );
        dedup.lookup("key-one").await;
        dedup.record("key-one", written.clone()).await;
        assert_eq!(recorded(dedup.lookup("key-one").await, "reject"), written);
    }

    #[tokio::test]
    async fn a_second_record_replaces_the_row() {
        let dir = TempDir::new().expect("temporary directory");
        let (_store, dedup) = open(&dir).await;
        dedup.lookup("key-one").await;
        dedup
            .record(
                "key-one",
                record("digest-first", SpokeResult::Ok(json!({ "deleted": 1 }))),
            )
            .await;
        let second = record(
            "digest-second",
            SpokeResult::Ok(json!({ "deleted": 5, "floor_revision": 6 })),
        );
        dedup.record("key-one", second.clone()).await;
        assert_eq!(recorded(dedup.lookup("key-one").await, "replace"), second);
    }

    #[tokio::test]
    async fn an_unsettled_claim_answers_evicted_after_the_file_is_reopened() {
        let dir = TempDir::new().expect("temporary directory");
        {
            let (_store, dedup) = open(&dir).await;
            assert_eq!(dedup.lookup("key-one").await, HistoryDedupLookup::Unseen);
        }
        let (_store, dedup) = open(&dir).await;
        assert_eq!(dedup.lookup("key-one").await, HistoryDedupLookup::Evicted);
    }

    /// Overwrite a claimed key with a stored outcome this module never writes.
    async fn install_outcome(store: &MindStore, stored: &str) {
        sqlx::query(
            "UPDATE history_dedup SET payload_digest = ?, outcome_json = ? \
             WHERE namespace = ? AND key = ?",
        )
        .bind("digest-invalid")
        .bind(stored)
        .bind("collection-one")
        .bind("key-one")
        .execute(&store.pool())
        .await
        .expect("install an invalid outcome");
    }

    #[tokio::test]
    async fn an_outcome_that_does_not_decode_answers_evicted() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        dedup.lookup("key-one").await;
        install_outcome(&store, "{\"reject\":{\"code\":\"NOT_A_REJECT_CODE\"}}").await;
        assert_eq!(dedup.lookup("key-one").await, HistoryDedupLookup::Evicted);
    }

    #[tokio::test]
    async fn a_parseable_record_with_both_variants_answers_evicted() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        dedup.lookup("key-one").await;
        // Both keys parse, so the record must not be replayed as either variant.
        install_outcome(
            &store,
            "{\"ok\":{\"deleted\":1},\"reject\":{\"code\":\"NO_SUCH_CODE\"}}",
        )
        .await;
        assert_eq!(dedup.lookup("key-one").await, HistoryDedupLookup::Evicted);
    }

    #[tokio::test]
    async fn a_reject_with_unexpected_members_answers_evicted() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        dedup.lookup("key-one").await;
        // A well-formed reject plus a member this module never writes.
        install_outcome(
            &store,
            "{\"reject\":{\"code\":\"INVALID_INPUT\",\"message\":\"m\",\"details\":null,\
             \"extra\":1}}",
        )
        .await;
        assert_eq!(dedup.lookup("key-one").await, HistoryDedupLookup::Evicted);
    }

    /// Two history surfaces on one database, sharing a wire scope, a principal and an idempotency
    /// key, route to different collections. The second must not replay the first's prune.
    #[tokio::test]
    async fn the_same_key_in_another_namespace_does_not_replay() {
        let dir = TempDir::new().expect("temporary directory");
        let store = MindStore::open(&dir.path().join("history_dedup.db"))
            .await
            .expect("open the store");
        let first = SqliteHistoryDedupStore::new(store.pool(), "wren");
        let second = SqliteHistoryDedupStore::new(store.pool(), "mora");
        let written = record("digest-wren", SpokeResult::Ok(json!({ "deleted": 1 })));
        first.lookup("key-one").await;
        first.record("key-one", written.clone()).await;

        assert_eq!(
            recorded(first.lookup("key-one").await, "same collection"),
            written
        );
        assert_eq!(
            second.lookup("key-one").await,
            HistoryDedupLookup::Unseen,
            "another collection's key is unseen, so its own prune runs"
        );
        assert_eq!(row_count(&store, "wren").await, 1);
        assert_eq!(row_count(&store, "mora").await, 1);

        // The second surface's own outcome never reaches the first.
        let other = record("digest-mora", SpokeResult::Ok(json!({ "deleted": 4 })));
        second.record("key-one", other.clone()).await;
        assert_eq!(
            recorded(first.lookup("key-one").await, "still wren"),
            written
        );
        assert_eq!(recorded(second.lookup("key-one").await, "mora"), other);
    }

    /// A collection name is an arbitrary string, NUL included. Framing the namespace into the key
    /// with a separator made `("a\0b", "c")` and `("a", "b\0c")` the same row, and put one
    /// namespace's rows inside another's capacity range. Two columns keep both pairs apart.
    #[tokio::test]
    async fn a_nul_bearing_collection_keeps_its_rows_separate() {
        let dir = TempDir::new().expect("temporary directory");
        let store = MindStore::open(&dir.path().join("history_dedup.db"))
            .await
            .expect("open the store");
        let nested = SqliteHistoryDedupStore::new(store.pool(), "a\u{0}b");
        let plain = SqliteHistoryDedupStore::new(store.pool(), "a");
        let nested_record = record("digest-nested", SpokeResult::Ok(json!({ "deleted": 1 })));
        let plain_record = record("digest-plain", SpokeResult::Ok(json!({ "deleted": 7 })));

        // The pair that a `\0` separator collapsed onto one stored key.
        assert_eq!(
            nested.lookup("c").await,
            HistoryDedupLookup::Unseen,
            "the NUL-bearing collection's own key is fresh"
        );
        nested.record("c", nested_record.clone()).await;
        assert_eq!(
            plain.lookup("b\u{0}c").await,
            HistoryDedupLookup::Unseen,
            "the other pair must not find the nested collection's row"
        );
        plain.record("b\u{0}c", plain_record.clone()).await;

        // The pair that a prefix range swept into the neighbouring namespace's capacity count.
        assert_eq!(plain.lookup("key-one").await, HistoryDedupLookup::Unseen);
        plain.record("key-one", plain_record.clone()).await;

        assert_eq!(recorded(nested.lookup("c").await, "nested"), nested_record);
        assert_eq!(
            recorded(plain.lookup("b\u{0}c").await, "plain, own key"),
            plain_record
        );
        assert_eq!(
            recorded(plain.lookup("key-one").await, "plain, second key"),
            plain_record
        );
        assert_eq!(row_count(&store, "a\u{0}b").await, 1);
        assert_eq!(row_count(&store, "a").await, 2);
    }

    /// A principal at its bound refuses an unseen key instead of inserting one more row, so a
    /// stream of fresh keys — refusals for a missing target included — cannot exhaust storage.
    #[tokio::test]
    async fn an_unseen_key_at_capacity_answers_evicted_without_a_new_row() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        for index in 0..HISTORY_DEDUP_CAPACITY {
            let key = key_for("prn_one", &format!("key-{index}"));
            assert_eq!(dedup.lookup(&key).await, HistoryDedupLookup::Unseen);
            dedup
                .record(
                    &key,
                    record("digest", SpokeResult::Ok(json!({ "deleted": 0 }))),
                )
                .await;
        }
        assert_eq!(
            principal_row_count(&store, "collection-one", "prn_one").await,
            HISTORY_DEDUP_CAPACITY
        );

        assert_eq!(
            dedup.lookup(&key_for("prn_one", "key-overflow")).await,
            HistoryDedupLookup::Evicted,
            "the bound refuses a fresh key, so Connect answers RECEIPT_EXPIRED"
        );
        assert_eq!(
            principal_row_count(&store, "collection-one", "prn_one").await,
            HISTORY_DEDUP_CAPACITY,
            "the refused claim inserted no row"
        );

        // `record` writes nothing for a key no claim covers, at the bound or anywhere else.
        dedup
            .record(
                &key_for("prn_one", "key-unclaimed"),
                record("digest", SpokeResult::Ok(json!({ "deleted": 0 }))),
            )
            .await;
        assert_eq!(
            principal_row_count(&store, "collection-one", "prn_one").await,
            HISTORY_DEDUP_CAPACITY
        );

        // A recorded outcome inside the quota still replays at the bound.
        assert_eq!(
            recorded(
                dedup.lookup(&key_for("prn_one", "key-0")).await,
                "at the bound"
            ),
            record("digest", SpokeResult::Ok(json!({ "deleted": 0 })))
        );

        // Another namespace has its own bound and keeps working.
        let (_, elsewhere) = open_in(&dir, "collection-two").await;
        assert_eq!(
            elsewhere.lookup(&key_for("prn_one", "key-overflow")).await,
            HistoryDedupLookup::Unseen
        );
    }

    /// The quota is per caller, so one principal filling its bound cannot disable prune for another
    /// on the same collection — the failure a shared namespace bound allowed.
    #[tokio::test]
    async fn a_full_principal_quota_leaves_another_principal_claiming() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        for index in 0..HISTORY_DEDUP_CAPACITY {
            let key = key_for("prn_noisy", &format!("key-{index}"));
            assert_eq!(dedup.lookup(&key).await, HistoryDedupLookup::Unseen);
            dedup
                .record(
                    &key,
                    record("digest", SpokeResult::Ok(json!({ "deleted": 0 }))),
                )
                .await;
        }
        assert_eq!(
            dedup.lookup(&key_for("prn_noisy", "key-overflow")).await,
            HistoryDedupLookup::Evicted,
            "the high-churn caller exhausted its own quota"
        );

        let quiet = key_for("prn_quiet", "key-one");
        assert_eq!(
            dedup.lookup(&quiet).await,
            HistoryDedupLookup::Unseen,
            "the quiet principal's quota is untouched"
        );
        let written = record("digest-quiet", SpokeResult::Ok(json!({ "deleted": 3 })));
        dedup.record(&quiet, written.clone()).await;
        assert_eq!(recorded(dedup.lookup(&quiet).await, "quiet"), written);

        assert_eq!(
            principal_row_count(&store, "collection-one", "prn_noisy").await,
            HISTORY_DEDUP_CAPACITY
        );
        assert_eq!(
            principal_row_count(&store, "collection-one", "prn_quiet").await,
            1
        );
    }

    /// The dedup record carries no clock, so a forward host-clock jump cannot make a live record
    /// expire and let a same-key retry execute the destructive prune again.
    #[tokio::test]
    async fn the_record_carries_no_clock_column() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        let key = key_for("prn_one", "key-one");
        dedup.lookup(&key).await;
        let written = record("digest", SpokeResult::Ok(json!({ "deleted": 2 })));
        dedup.record(&key, written.clone()).await;

        let columns: Vec<String> =
            sqlx::query_scalar("SELECT name FROM pragma_table_info('history_dedup') ORDER BY cid")
                .fetch_all(&store.pool())
                .await
                .expect("read the table's columns");
        assert_eq!(
            columns,
            vec![
                "namespace",
                "key",
                "principal",
                "payload_digest",
                "outcome_json"
            ],
            "no timestamp column exists for a clock jump to age out"
        );

        // Nothing deletes: the record still replays after the file is reopened.
        assert_eq!(recorded(dedup.lookup(&key).await, "before reopen"), written);
        drop(dedup);
        let (_store, reopened) = open(&dir).await;
        assert_eq!(
            recorded(reopened.lookup(&key).await, "after reopen"),
            written
        );
    }

    /// Unparsable keys share one bucket, so a caller cannot mint a fresh quota per odd key.
    #[tokio::test]
    async fn keys_that_do_not_parse_share_one_principal_bucket() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        for key in ["not-a-canonical-key", "[1,2]", "{\"not\":\"an array\"}"] {
            assert_eq!(dedup.lookup(key).await, HistoryDedupLookup::Unseen);
            let written = record("digest", SpokeResult::Ok(json!({ "deleted": 1 })));
            dedup.record(key, written.clone()).await;
            assert_eq!(recorded(dedup.lookup(key).await, key), written);
        }
        assert_eq!(
            principal_row_count(&store, "collection-one", HISTORY_DEDUP_UNKNOWN_PRINCIPAL).await,
            3,
            "every unparsable key is accounted to the one shareable bucket"
        );
    }

    /// A non-string third member is not a principal either, so it lands in the same bucket.
    #[tokio::test]
    async fn a_non_string_principal_member_uses_the_unknown_bucket() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        let key = json!(["history.revisions.prune", { "scope_id": "s" }, 7, "k"]).to_string();
        assert_eq!(dedup.lookup(&key).await, HistoryDedupLookup::Unseen);
        dedup
            .record(
                &key,
                record("digest", SpokeResult::Ok(json!({ "deleted": 1 }))),
            )
            .await;
        assert_eq!(
            principal_row_count(&store, "collection-one", HISTORY_DEDUP_UNKNOWN_PRINCIPAL).await,
            1
        );
    }

    /// `record` only fills a claim. A caller that records without a preceding `lookup` writes
    /// nothing, so no outcome can exist for a key whose prune never ran — and a later `lookup`
    /// cannot replay one.
    #[tokio::test]
    async fn record_without_a_claim_writes_nothing() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        let key = key_for("prn_one", "key-one");
        dedup
            .record(
                &key,
                record("digest", SpokeResult::Ok(json!({ "deleted": 4 }))),
            )
            .await;
        assert_eq!(
            row_count(&store, "collection-one").await,
            0,
            "an unclaimed record inserted no row"
        );
        assert_eq!(
            dedup.lookup(&key).await,
            HistoryDedupLookup::Unseen,
            "the key is still fresh, so its prune runs rather than replaying"
        );
    }

    /// The bound counts one `(namespace, principal)` pair exactly. Under the old prefix range a
    /// collection whose name extended another's — `"a\0b"` beside `"a"` — had its rows swept into
    /// the shorter name's count, so a full neighbour could refuse a namespace that held nothing.
    #[tokio::test]
    async fn the_capacity_count_holds_only_the_named_namespace() {
        let dir = TempDir::new().expect("temporary directory");
        let store = MindStore::open(&dir.path().join("history_dedup.db"))
            .await
            .expect("open the store");
        let nested = SqliteHistoryDedupStore::new(store.pool(), "a\u{0}b");

        // Fill one principal's quota in the NUL-bearing namespace, and nothing else.
        for index in 0..HISTORY_DEDUP_CAPACITY {
            let key = key_for("prn_one", &format!("key-{index}"));
            assert_eq!(nested.lookup(&key).await, HistoryDedupLookup::Unseen);
            nested
                .record(
                    &key,
                    record("digest", SpokeResult::Ok(json!({ "deleted": 0 }))),
                )
                .await;
        }
        assert_eq!(
            principal_row_count(&store, "a\u{0}b", "prn_one").await,
            HISTORY_DEDUP_CAPACITY,
            "the nested namespace's quota is full"
        );
        assert_eq!(row_count(&store, "a").await, 0);

        // The shorter name is empty, so the same principal's quota there is untouched.
        let plain = SqliteHistoryDedupStore::new(store.pool(), "a");
        assert_eq!(
            plain.lookup(&key_for("prn_one", "key-fresh")).await,
            HistoryDedupLookup::Unseen,
            "the full neighbour's rows are not this namespace's"
        );
        assert_eq!(row_count(&store, "a").await, 1);
        assert_eq!(
            principal_row_count(&store, "a\u{0}b", "prn_one").await,
            HISTORY_DEDUP_CAPACITY,
            "the neighbour's claim left the full namespace alone"
        );
    }
}
