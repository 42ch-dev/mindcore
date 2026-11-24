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
//! Each store serves one namespace — the collection its history surface routes to — and stores the
//! key as `<namespace>\0<key>`. Two history surfaces on one database can share a wire scope, a
//! principal and an idempotency key while reading different collections; without the namespace the
//! second would replay a prune of the first surface's collection, or refuse it.
//!
//! One serving process per database is the host's obligation (see `I-000109`): the claim row stops
//! a second process from executing, but it answers `RECEIPT_EXPIRED` instead of waiting for the
//! first, so two processes would turn a replay into a refusal.
//!
//! A namespace holds at most [`HISTORY_DEDUP_CAPACITY`] rows, claims and recorded outcomes alike.
//! Nothing evicts: at the bound an unseen key answers `Evicted` — Connect's `RECEIPT_EXPIRED` —
//! without the prune being executed, and `record` writes no row that would cross the bound. A
//! caller that exhausts its namespace therefore gets refusals rather than a prune it cannot dedup,
//! and the table's growth stays bounded per surface.

use std::io::Write;

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::SqlitePool;

use spoke_connect::remote::{HistoryDedupLookup, HistoryDedupRecord, HistoryDedupStore};
use spoke_operations::{SpokeReject, SpokeRejectCode, SpokeResult};

/// The most rows one namespace may hold. A claim counts, because a claim is what keeps a replay
/// out of the port; a namespace at the bound refuses unseen keys instead of inserting past it.
const HISTORY_DEDUP_CAPACITY: i64 = 4096;

/// One dedup record per retry identity, in the database file the knowledge store already serves
/// with, under the namespace of the collection this surface routes to. The pool holds a single
/// connection, so the claim, the port call and the record never hold two at once: each takes and
/// releases its own.
pub(crate) struct SqliteHistoryDedupStore {
    pool: SqlitePool,
    namespace: String,
}

impl SqliteHistoryDedupStore {
    /// `namespace` is the collection the routed store serves: it is stored in front of the key, so
    /// two surfaces on one database that route to different collections never share a row.
    pub(crate) fn new(pool: SqlitePool, namespace: impl Into<String>) -> Self {
        Self {
            pool,
            namespace: namespace.into(),
        }
    }

    /// The stored key: the namespace, a NUL that no SQLite `TEXT` comparison treats specially, and
    /// the wire key. The NUL separates the two, so `ab` + `c` and `a` + `bc` stay distinct.
    fn stored_key(&self, key: &str) -> String {
        format!("{}\0{key}", self.namespace)
    }

    /// The half-open range holding exactly this namespace's rows: `\0` sorts before every other
    /// character the suffix can start with, and `\u{1}` above it ends the range.
    fn namespace_range(&self) -> (String, String) {
        (
            format!("{}\0", self.namespace),
            format!("{}\u{1}", self.namespace),
        )
    }
}

#[async_trait]
impl HistoryDedupStore for SqliteHistoryDedupStore {
    async fn lookup(&self, key: &str) -> HistoryDedupLookup {
        let stored_key = self.stored_key(key);
        let (low, high) = self.namespace_range();
        let mut conn = match self.pool.acquire().await {
            Ok(conn) => conn,
            Err(error) => return fault("claim", error),
        };
        // Claim and read on the same connection: the insert decides whether the row is this
        // caller's, and the read shows what the row holds — `NULL`s for a claim still in flight.
        // The insert is also the capacity gate: a namespace already at the bound takes no new row,
        // so an unseen key stays unseen here and the read below answers `Evicted`. An existing key
        // is unaffected either way, so a replay still finds its recorded outcome at the bound.
        let claimed = match sqlx::query(
            "INSERT INTO history_dedup (key) SELECT ? \
             WHERE (SELECT COUNT(*) FROM history_dedup WHERE key >= ? AND key < ?) < ? \
             ON CONFLICT(key) DO NOTHING",
        )
        .bind(&stored_key)
        .bind(&low)
        .bind(&high)
        .bind(HISTORY_DEDUP_CAPACITY)
        .execute(&mut *conn)
        .await
        {
            Ok(result) => result.rows_affected() == 1,
            Err(error) => return fault("claim", error),
        };
        let stored: Result<Option<(Option<String>, Option<String>)>, _> =
            sqlx::query_as("SELECT payload_digest, outcome_json FROM history_dedup WHERE key = ?")
                .bind(&stored_key)
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
        let stored_key = self.stored_key(key);
        let (low, high) = self.namespace_range();
        // Insert only when `lookup` claimed the key or it is already present, and only while the
        // namespace has room. An unclaimed key at the bound would otherwise grow the table past
        // the cap through `record` alone; this write leaves the row count alone instead.
        let written = sqlx::query(
            "INSERT INTO history_dedup (key, payload_digest, outcome_json) SELECT ?, ?, ? \
             WHERE EXISTS (SELECT 1 FROM history_dedup WHERE key = ?) \
                OR (SELECT COUNT(*) FROM history_dedup WHERE key >= ? AND key < ?) < ? \
             ON CONFLICT(key) DO UPDATE SET \
               payload_digest = excluded.payload_digest, \
               outcome_json   = excluded.outcome_json",
        )
        .bind(&stored_key)
        .bind(&record.payload_digest)
        .bind(&outcome_json)
        .bind(&stored_key)
        .bind(&low)
        .bind(&high)
        .bind(HISTORY_DEDUP_CAPACITY)
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
        sqlx::query_scalar("SELECT COUNT(*) FROM history_dedup WHERE key >= ? AND key < ?")
            .bind(format!("{namespace}\0"))
            .bind(format!("{namespace}\u{1}"))
            .fetch_one(&store.pool())
            .await
            .expect("count the namespace's rows")
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
        sqlx::query("UPDATE history_dedup SET payload_digest = ?, outcome_json = ? WHERE key = ?")
            .bind("digest-invalid")
            .bind(stored)
            .bind("collection-one\u{0}key-one")
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

    /// A namespace at its bound refuses an unseen key instead of inserting one more row, so a
    /// stream of fresh keys — refusals for a missing target included — cannot exhaust storage.
    #[tokio::test]
    async fn an_unseen_key_at_capacity_answers_evicted_without_a_new_row() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        for index in 0..HISTORY_DEDUP_CAPACITY {
            let key = format!("key-{index}");
            assert_eq!(dedup.lookup(&key).await, HistoryDedupLookup::Unseen);
            dedup
                .record(
                    &key,
                    record("digest", SpokeResult::Ok(json!({ "deleted": 0 }))),
                )
                .await;
        }
        assert_eq!(
            row_count(&store, "collection-one").await,
            HISTORY_DEDUP_CAPACITY
        );

        assert_eq!(
            dedup.lookup("key-overflow").await,
            HistoryDedupLookup::Evicted,
            "the bound refuses a fresh key, so Connect answers RECEIPT_EXPIRED"
        );
        assert_eq!(
            row_count(&store, "collection-one").await,
            HISTORY_DEDUP_CAPACITY,
            "the refused claim inserted no row"
        );

        // `record` cannot grow the namespace either: an unclaimed key at the bound writes nothing.
        dedup
            .record(
                "key-unclaimed",
                record("digest", SpokeResult::Ok(json!({ "deleted": 0 }))),
            )
            .await;
        assert_eq!(
            row_count(&store, "collection-one").await,
            HISTORY_DEDUP_CAPACITY
        );

        // A recorded outcome inside the namespace still replays at the bound.
        assert_eq!(
            recorded(dedup.lookup("key-0").await, "at the bound"),
            record("digest", SpokeResult::Ok(json!({ "deleted": 0 })))
        );

        // Another namespace has its own bound and keeps working.
        let (_, elsewhere) = open_in(&dir, "collection-two").await;
        assert_eq!(
            elsewhere.lookup("key-overflow").await,
            HistoryDedupLookup::Unseen
        );
    }
}
