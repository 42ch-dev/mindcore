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
//! One serving process per database is the host's obligation (see `I-000109`): the claim row stops
//! a second process from executing, but it answers `RECEIPT_EXPIRED` instead of waiting for the
//! first, so two processes would turn a replay into a refusal.

use std::io::Write;

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::SqlitePool;

use spoke_connect::remote::{HistoryDedupLookup, HistoryDedupRecord, HistoryDedupStore};
use spoke_operations::{SpokeReject, SpokeRejectCode, SpokeResult};

/// One dedup record per retry identity, in the database file the knowledge store already serves
/// with. The pool holds a single connection, so the claim, the port call and the record never
/// hold two at once: each takes and releases its own.
pub(crate) struct SqliteHistoryDedupStore {
    pool: SqlitePool,
}

impl SqliteHistoryDedupStore {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl HistoryDedupStore for SqliteHistoryDedupStore {
    async fn lookup(&self, key: &str) -> HistoryDedupLookup {
        let mut conn = match self.pool.acquire().await {
            Ok(conn) => conn,
            Err(error) => return fault("claim", error),
        };
        // Claim and read on the same connection: the insert decides whether the row is this
        // caller's, and the read shows what the row holds — `NULL`s for a claim still in flight.
        let claimed = match sqlx::query(
            "INSERT INTO history_dedup (key) VALUES (?) ON CONFLICT(key) DO NOTHING",
        )
        .bind(key)
        .execute(&mut *conn)
        .await
        {
            Ok(result) => result.rows_affected() == 1,
            Err(error) => return fault("claim", error),
        };
        let stored: Result<Option<(Option<String>, Option<String>)>, _> =
            sqlx::query_as("SELECT payload_digest, outcome_json FROM history_dedup WHERE key = ?")
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
            // Another caller's unanswered claim, or one the process never settled.
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
        let written = sqlx::query(
            "INSERT INTO history_dedup (key, payload_digest, outcome_json) VALUES (?, ?, ?) \
             ON CONFLICT(key) DO UPDATE SET \
               payload_digest = excluded.payload_digest, \
               outcome_json   = excluded.outcome_json",
        )
        .bind(key)
        .bind(&record.payload_digest)
        .bind(&outcome_json)
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
        let store = MindStore::open(&dir.path().join("history_dedup.db"))
            .await
            .expect("open the store");
        let dedup = SqliteHistoryDedupStore::new(store.pool());
        (store, dedup)
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
}
