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
//! The namespace is the admitted Mind's registered collection, resolved per call: Connect's key is
//! the canonical serialization of `[op, effective_scope, principal_id, idempotency_key]`, so
//! [`scope_of`] reads the second member's `scope_id` — the Mind — and
//! [`mindcore_sqlite::minds::registered_collection`] answers the collection that Mind registered.
//! It is stored beside the key in its own column rather than in a framed string: a collection name
//! may contain any character, so no separator could keep `(namespace, key)` pairs distinct. Two
//! Minds can share a wire scope, a principal and an idempotency key while reading different
//! collections; without the namespace the second would replay a prune of the first Mind's
//! collection, or refuse it. A key that is not the canonical tuple, and a Mind with no registry
//! row, answer `Evicted`: the prune is not executed.
//!
//! Resolution is per call rather than at construction, so a Mind created after the surface was
//! assembled is deduplicated from its first prune and needs no reassembly.
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
//!
//! A settlement that fails is never answered as success. `record` carries no error channel
//! (`serve_ports.rs:464-465`) and `HistoryPortService` answers the port's result whatever `record`
//! did, so returning normally after a failed write would hand the caller a prune result it could
//! not recover on a retry — the second call reads the claim back as `RECEIPT_EXPIRED`. The failure
//! therefore leaves through Connect's own fault channel, the detached serve task's dropped result
//! channel, which the responder answers `OUTCOME_UNCERTAIN`: the outcome is unknown, the caller
//! replays the key, and no acknowledged receipt exists that was never written
//! ([`outcome_not_settled`]).
//!
//! Not everything the port answers is a settled outcome, either. A **storage fault** — a busy
//! begin, a failed grant statement, a store read that could not run — is a transient condition of
//! the database, not this key's answer about the Mind's history, so recording it would make one
//! failure permanent: every later same-key retry would replay `INTERNAL_ERROR` long after the fault
//! cleared. `record` therefore settles only a genuine execution result — success, and the refusals
//! that are decisions: `NOT_AUTHORIZED`, `NOT_FOUND`, `HISTORY_TRUNCATED`,
//! `HISTORY_ENTITY_UNSUPPORTED`, `INVALID_INPUT`. [`is_storage_fault`] is that rule, it reads the
//! reject code rather than the message text, and it is the one place to change when the served
//! path gains a code.
//!
//! On a fault the claim is **released**, not retained: [`release_claim`] deletes the unanswered row
//! before the caller is answered, so once the fault clears the same `idempotency_key` can be
//! claimed again and the prune can execute. Leaving the claim behind would be indistinguishable —
//! to every later caller — from a prune that ran and never settled, and would answer
//! `RECEIPT_EXPIRED` for the database's lifetime. The same release runs when the outcome read after
//! the claim insert fails: that call made the claim itself, and keeping it would spend the key on a
//! statement that never ran.
//!
//! The release is **best-effort**, and the recovery that follows it is conditional on its success.
//! It is a `DELETE` against the same database that just faulted, so it can fail for the same
//! reasons; when it does, the claim survives and the key answers `RECEIPT_EXPIRED` on every later
//! retry, exactly as a claim left by a dead serve loop does. Nothing is lost by that fallback: the
//! caller was already told the outcome is unknown, so the surviving claim contradicts nothing.
//!
//! The caller is answered [`outcome_not_settled`]'s uncertain outcome whether or not the release
//! succeeds: it was not told the prune succeeded, and nothing in that answer asserts the prune had
//! no effect. The prune may in fact have committed — the commit acknowledgement is one of the
//! faults this path covers — so a retry that does execute runs against the current state, which is
//! the same exposure a crash between the commit and the record has always carried.

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

/// The members of Connect's canonical retry identity, in order.
fn retry_identity(key: &str) -> Option<Vec<Value>> {
    serde_json::from_str::<Value>(key).ok()?.as_array().cloned()
}

/// The requester a dedup key names, for the per-principal quota.
///
/// The key is the RFC 8785 canonical serialization of `[op, effective_scope, principal_id,
/// idempotency_key]`, so the principal is the third element and a string. Anything else — an
/// unparsable key, a truncated array, a non-string member — is accounted to
/// [`HISTORY_DEDUP_UNKNOWN_PRINCIPAL`], so every such key shares one quota and none of them can
/// widen the bound. Connect canonicalizes before it calls, so a well-formed deployment never
/// reaches that bucket.
fn principal_of(key: &str) -> String {
    retry_identity(key)
        .and_then(|members| members.get(2).and_then(Value::as_str).map(str::to_owned))
        .unwrap_or_else(|| HISTORY_DEDUP_UNKNOWN_PRINCIPAL.to_owned())
}

/// The Mind a dedup key names: the `scope_id` of the second member, the effective scope.
///
/// A key that is not the canonical tuple — an unparsable value, an array too short, a scope member
/// that is not an object, or one whose `scope_id` is not a string — names no Mind, so its row is
/// never claimed and its prune never runs.
fn scope_of(key: &str) -> Option<String> {
    retry_identity(key)?
        .get(1)?
        .get("scope_id")?
        .as_str()
        .map(str::to_owned)
}

/// The namespace a dedup key's row lives in: the collection the Mind it names registered.
///
/// `Ok(None)` for a key that names no Mind and for a Mind with no registry row — neither may claim
/// a row or settle one. `Err` is a registry read that failed, which is not the same answer: on the
/// settlement path it must not read as "nothing to settle", because the prune may have committed.
async fn namespace_of(pool: &SqlitePool, key: &str) -> Result<Option<String>, SpokeReject> {
    let Some(scope_id) = scope_of(key) else {
        return Ok(None);
    };
    match mindcore_sqlite::minds::registered_collection(pool, &scope_id).await {
        SpokeResult::Ok(collection) => Ok(collection),
        SpokeResult::Reject(reject) => Err(reject),
    }
}

/// The codes that make a reject a storage fault rather than the key's settled answer.
///
/// **`INTERNAL_ERROR` is the whole list, and it is the list because of how the path is built.**
/// Every fault on the way to a prune answer is reported as `InternalError` and nothing else:
/// `mindcore_sqlite`'s `internal`, which every failed statement on the store, the route and the
/// checkpoint reads maps to; `MindHistoryPort`'s `storage_unavailable`, which is the grant read's
/// one failure answer; and Connect's own transport decode fault. A code is a fault when it says the
/// work could not be carried out — never when it is a decision the work reached.
///
/// **What a code outside this list does.** It is settled: its reject is written as the key's
/// outcome and replayed verbatim to a same-key retry. That is the whole rule — there is no second
/// check, no inspection of the message, and no code that reaches a third path. So a new code added
/// to the served path without being added here is classified as a decision and persisted, which is
/// the wrong answer if it actually reports a fault. The rule is stated as an allow-list of faults
/// precisely so that mistake has to be made explicitly, in this function, by someone adding a
/// variant; the alternative — treating everything but an enumerated set of decisions as a fault —
/// would silently stop settling a genuine refusal whose code nobody thought to list, and a refusal
/// that is not settled is a prune a retry re-executes.
fn is_storage_fault(code: SpokeRejectCode) -> bool {
    matches!(code, SpokeRejectCode::InternalError)
}

/// The answer a prune gives when its outcome is not settled: a failed write, a registry fault, or
/// a fault the port answered.
///
/// The claim has been released by the time this runs, so the key is claimable again; this answer
/// tells the caller the outcome is unknown rather than that the prune did not happen.
///
/// **Why a panic, deliberately.** `HistoryDedupStore::record` answers `()`
/// (`serve_ports.rs:464-465`): it carries no error channel, and `HistoryPortService` returns the
/// port's result regardless of how `record` fared. Returning normally here would therefore tell
/// the caller its prune settled while a same-key retry reads the claim back as
/// `RECEIPT_EXPIRED` — an acknowledged outcome that does not exist. Connect's own fault channel
/// is the same one the runtime's admission uses: the whole served invoke runs in a detached
/// `tokio::spawn` and the responder receives its result over a oneshot
/// (`responder.rs:2286-2300`), so a panic here drops that channel, and the responder answers the
/// prune `OUTCOME_UNCERTAIN` with `details.retry_class = "uncertain"` rather than any success
/// (`responder.rs:1006-1016`). The prune may well have committed; the transport says so instead
/// of asserting a receipt it does not hold, and the host-side diagnostic names no wire bytes.
fn outcome_not_settled(diagnostic: impl std::fmt::Display) -> ! {
    panic!(
        "history dedup outcome could not be settled; answered as this request's uncertain \
         outcome: {diagnostic}"
    )
}

/// One dedup record per retry identity, in the database file the knowledge store already serves
/// with, under the namespace of the admitted Mind's registered collection. The pool holds a single
/// connection, so the claim, the port call and the record never hold two at once: each takes and
/// releases its own.
pub(crate) struct SqliteHistoryDedupStore {
    pool: SqlitePool,
    /// Test-only: fail the outcome read that follows the claim insert. SQLite cannot express a
    /// fault that breaks a `SELECT` on a writable table — every construct that poisons the read
    /// also poisons the insert or the guarded delete — so the seam is explicit here rather than a
    /// fixture that would pass without exercising the path.
    #[cfg(test)]
    fault_after_claim: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl SqliteHistoryDedupStore {
    /// No namespace: the admitted Mind names it, and the registry resolves it on every call.
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            #[cfg(test)]
            fault_after_claim: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Test-only: make the outcome read that follows the claim insert fail.
    #[cfg(test)]
    fn fail_the_read_after_the_claim(&self) {
        self.fault_after_claim
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Test-only: clear the seam, as a fault clearing would.
    #[cfg(test)]
    fn read_is_healthy_again(&self) {
        self.fault_after_claim
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// Drop the claim `lookup` inserted, so the key is claimable again and a retry can execute.
    ///
    /// This is the whole difference between "this key has no outcome" and "this key may never run
    /// again". A storage fault says the work could not be carried out, so retrying it is the
    /// caller's remedy; a retained claim would turn that into `RECEIPT_EXPIRED` for the database's
    /// lifetime. Only an unanswered claim is removed — the guard is in the statement below — so a
    /// settled outcome, this key's or another caller's, is never deleted here.
    ///
    /// **Callers.** Two methods, on four paths:
    ///
    /// - `lookup`, when the outcome read after its own claim insert fails. The release runs only if
    ///   that call made the insert (`claimed`), and what follows is `HistoryDedupLookup::Evicted`
    ///   through [`fault`] — not [`outcome_not_settled`], which is the served invoke's answer, while
    ///   this is the dedup probe's.
    /// - `record`, on each of the three ways it cannot settle: the registry read failed, the port
    ///   answered a [storage fault](is_storage_fault), or the settlement write itself failed. Each
    ///   of those then answers through [`outcome_not_settled`], which diverges.
    ///
    /// So the release is attempted exactly where a call has just created a claim, or arrived to
    /// settle one and failed. Paths that answer without it never had a claim to release: `lookup`'s
    /// namespace fault and its pool-acquisition and claim-statement faults answer `Evicted` before
    /// any row exists, and `record` returns early, writing nothing, for a key whose namespace
    /// resolves to no Mind — `lookup` answered `Evicted` for that key, so no prune ran and there is
    /// no outcome to settle.
    ///
    /// The namespace is resolved only to narrow the delete; it is not a precondition. A registry
    /// fault — one of the faults this path covers — must not be able to skip the release by making
    /// the namespace unreadable, so the statement falls back to the key alone, still restricted to
    /// an unanswered row. That fallback is safe because the key is the canonical retry identity,
    /// which embeds the effective scope: the same key string can only name one Mind, so the rows it
    /// can match are that Mind's own claims, and an unanswered claim is precisely the state this
    /// function removes. Every other row keeps its settled outcome.
    ///
    /// A delete that itself fails is reported host-side and does not change the answer: the caller
    /// is already being told the outcome is uncertain, and the row's removal is what a later retry
    /// depends on rather than what this answer asserts.
    async fn release_claim(&self, key: &str) {
        let released = match self.namespace_for_release(key).await {
            Some(namespace) => sqlx::query(
                "DELETE FROM history_dedup \
                 WHERE namespace = ? AND key = ? \
                   AND payload_digest IS NULL AND outcome_json IS NULL",
            )
            .bind(&namespace)
            .bind(key)
            .execute(&self.pool)
            .await
            .map(|result| result.rows_affected()),
            None => sqlx::query(
                "DELETE FROM history_dedup WHERE key = ? \
                   AND payload_digest IS NULL AND outcome_json IS NULL",
            )
            .bind(key)
            .execute(&self.pool)
            .await
            .map(|result| result.rows_affected()),
        };
        if let Err(error) = released {
            let _ = writeln!(
                std::io::stderr(),
                "mindcore history dedup claim release fault: {error}"
            );
        }
    }

    /// The read that follows the claim insert: the row's digest and outcome, `None` when the key
    /// has no row at all.
    ///
    /// Split out only so the test seam can fail this statement without touching the claim that
    /// precedes it — see the struct's `fault_after_claim`. The production body is the one query the
    /// comment on the claim already describes.
    async fn outcome_read(
        &self,
        namespace: &str,
        key: &str,
        conn: &mut sqlx::SqliteConnection,
    ) -> Result<Option<(Option<String>, Option<String>)>, sqlx::Error> {
        #[cfg(test)]
        if self
            .fault_after_claim
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(sqlx::Error::Protocol(
                "test seam: the outcome read failed after the claim".to_string(),
            ));
        }
        sqlx::query_as(
            "SELECT payload_digest, outcome_json FROM history_dedup \
             WHERE namespace = ? AND key = ?",
        )
        .bind(namespace)
        .bind(key)
        .fetch_optional(&mut *conn)
        .await
    }

    /// The namespace a release needs, resolved without the fault branch: a key naming no Mind has
    /// no row to release, and a registry fault here leaves the claim for the retry's own `lookup`
    /// to answer.
    async fn namespace_for_release(&self, key: &str) -> Option<String> {
        let scope_id = scope_of(key)?;
        match mindcore_sqlite::minds::registered_collection(&self.pool, &scope_id).await {
            SpokeResult::Ok(collection) => collection,
            SpokeResult::Reject(_) => None,
        }
    }
}

#[async_trait]
impl HistoryDedupStore for SqliteHistoryDedupStore {
    async fn lookup(&self, key: &str) -> HistoryDedupLookup {
        let namespace = match namespace_of(&self.pool, key).await {
            Ok(Some(namespace)) => namespace,
            Ok(None) => return HistoryDedupLookup::Evicted,
            Err(reject) => return fault("namespace", reject.message),
        };
        let mut conn = match self.pool.acquire().await {
            Ok(conn) => conn,
            Err(error) => return fault("claim", error),
        };
        let principal = principal_of(key);
        let claimed = match sqlx::query(
            "INSERT INTO history_dedup (namespace, key, principal) SELECT ?, ?, ? \
             WHERE (SELECT COUNT(*) FROM history_dedup \
                    WHERE namespace = ? AND principal = ?) < ? \
             ON CONFLICT(namespace, key) DO NOTHING",
        )
        .bind(&namespace)
        .bind(key)
        .bind(&principal)
        .bind(&namespace)
        .bind(&principal)
        .bind(HISTORY_DEDUP_CAPACITY)
        .execute(&mut *conn)
        .await
        {
            Ok(result) => result.rows_affected() == 1,
            Err(error) => return fault("claim", error),
        };
        let stored = self.outcome_read(&namespace, key, &mut conn).await;
        let row = match stored {
            Ok(row) => row,
            Err(error) => {
                if claimed {
                    drop(conn);
                    self.release_claim(key).await;
                }
                return fault("lookup", error);
            }
        };
        if claimed {
            return HistoryDedupLookup::Unseen;
        }
        match row {
            Some((None, None)) | None => HistoryDedupLookup::Evicted,
            Some((Some(payload_digest), Some(outcome_json))) => match decode_outcome(&outcome_json)
            {
                Some(outcome) => HistoryDedupLookup::Recorded(HistoryDedupRecord {
                    payload_digest,
                    outcome,
                }),
                None => HistoryDedupLookup::Evicted,
            },
            Some(_) => HistoryDedupLookup::Evicted,
        }
    }

    async fn record(&self, key: &str, record: HistoryDedupRecord) {
        let namespace = match namespace_of(&self.pool, key).await {
            Ok(Some(namespace)) => namespace,
            Ok(None) => return,
            Err(reject) => {
                self.release_claim(key).await;
                outcome_not_settled(reject.message);
            }
        };
        if let SpokeResult::Reject(reject) = &record.outcome {
            if is_storage_fault(reject.code) {
                self.release_claim(key).await;
                outcome_not_settled(format!(
                    "the port answered {}: {}",
                    reject.code.as_str(),
                    reject.message
                ));
            }
        }
        let outcome_json = encode_outcome(&record.outcome);
        let principal = principal_of(key);
        let written = sqlx::query(
            "INSERT INTO history_dedup (namespace, key, principal, payload_digest, outcome_json) \
             SELECT ?, ?, ?, ?, ? \
             WHERE EXISTS (SELECT 1 FROM history_dedup WHERE namespace = ? AND key = ?) \
             ON CONFLICT(namespace, key) DO UPDATE SET \
               payload_digest = excluded.payload_digest, \
               outcome_json   = excluded.outcome_json",
        )
        .bind(&namespace)
        .bind(key)
        .bind(&principal)
        .bind(&record.payload_digest)
        .bind(&outcome_json)
        .bind(&namespace)
        .bind(key)
        .execute(&self.pool)
        .await;
        if let Err(error) = written {
            self.release_claim(key).await;
            outcome_not_settled(error);
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
    use std::sync::Arc;

    use serde_json::{json, Map};
    use tempfile::TempDir;

    use mindcore_sqlite::minds::CreateMind;
    use mindcore_sqlite::MindStore;

    use mindcore_base::boundary::CallerId;
    use mindcore_base::ids::MindId;

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
    /// identity. The per-principal quota reads the third member and the namespace the second's
    /// `scope_id`.
    fn key_in(scope_id: &str, principal: &str, idempotency_key: &str) -> String {
        json!([
            "history.revisions.prune",
            { "scope_id": scope_id },
            principal,
            idempotency_key
        ])
        .to_string()
    }

    fn key_for(principal: &str, idempotency_key: &str) -> String {
        key_in(MIND, principal, idempotency_key)
    }

    /// The Mind both [`open`] and the key builders name, and the collection `create` gives it.
    const MIND: &str = "spc_wren";
    const OTHER_MIND: &str = "spc_mora";
    const COLLECTION: &str = "mind:spc_wren";
    const OTHER_COLLECTION: &str = "mind:spc_mora";

    /// One fresh database file through the production open path, with one registered Mind and the
    /// store bound to its pool. The store resolves its namespace from the registry, so no test
    /// passes a collection in.
    async fn open(dir: &TempDir) -> (MindStore, SqliteHistoryDedupStore) {
        let store = MindStore::open(&dir.path().join("history_dedup.db"))
            .await
            .expect("open the store");
        register(&store, MIND, COLLECTION).await;
        let dedup = SqliteHistoryDedupStore::new(store.pool());
        (store, dedup)
    }

    /// As [`open`], with a second Mind in its own collection.
    async fn open_two(dir: &TempDir) -> (MindStore, SqliteHistoryDedupStore) {
        let (store, dedup) = open(dir).await;
        register(&store, OTHER_MIND, OTHER_COLLECTION).await;
        (store, dedup)
    }

    /// A registry row for `mind`, written through the production `create` so the row is the one a
    /// served Mind has. The collection `create` assigns is `mind:<id>`; `collection` is asserted
    /// against it so a test never names a collection the registry would not. A reopen reuses the
    /// row the first pass wrote.
    async fn register(store: &MindStore, mind: &str, collection: &str) {
        let created = store
            .create_mind(&CreateMind {
                mind_id: Some(MindId::new(mind)),
                display_name: None,
                owner: CallerId::new("caller_wren"),
            })
            .await;
        match created {
            SpokeResult::Ok(mind) => assert_eq!(mind.knowledge.collection, collection),
            SpokeResult::Reject(reject) => {
                assert_eq!(
                    reject.details,
                    Some(Map::from_iter([(
                        "reason".to_string(),
                        json!(mindcore_sqlite::minds::MIND_ALREADY_EXISTS)
                    )])),
                    "seed the registry: {reject:?}"
                );
                let registered =
                    mindcore_sqlite::minds::registered_collection(&store.pool(), mind).await;
                match registered {
                    SpokeResult::Ok(Some(registered)) => assert_eq!(registered, collection),
                    other => panic!("a seeded Mind reads back its collection: {other:?}"),
                }
            }
        }
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
        assert_eq!(
            dedup.lookup(&key_for("prn_one", "key-one")).await,
            HistoryDedupLookup::Unseen
        );
    }

    #[tokio::test]
    async fn an_ok_outcome_round_trips() {
        let dir = TempDir::new().expect("temporary directory");
        let (_store, dedup) = open(&dir).await;
        let key = key_for("prn_one", "key-one");
        let written = record(
            "digest-ok",
            SpokeResult::Ok(json!({ "deleted": 2, "floor_revision": 3 })),
        );
        dedup.lookup(&key).await;
        dedup.record(&key, written.clone()).await;
        assert_eq!(recorded(dedup.lookup(&key).await, "ok"), written);
    }

    #[tokio::test]
    async fn a_reject_with_details_round_trips() {
        let dir = TempDir::new().expect("temporary directory");
        let (_store, dedup) = open(&dir).await;
        let key = key_for("prn_one", "key-one");
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
        dedup.lookup(&key).await;
        dedup.record(&key, written.clone()).await;
        assert_eq!(recorded(dedup.lookup(&key).await, "reject"), written);
    }

    #[tokio::test]
    async fn a_reject_without_details_round_trips_as_none() {
        let dir = TempDir::new().expect("temporary directory");
        let (_store, dedup) = open(&dir).await;
        let key = key_for("prn_one", "key-one");
        let written = record(
            "digest-null-details",
            SpokeResult::Reject(SpokeReject {
                code: SpokeRejectCode::InvalidInput,
                message: "keep must be at least one".to_string(),
                details: None,
            }),
        );
        dedup.lookup(&key).await;
        dedup.record(&key, written.clone()).await;
        assert_eq!(recorded(dedup.lookup(&key).await, "reject"), written);
    }

    #[tokio::test]
    async fn a_second_record_replaces_the_row() {
        let dir = TempDir::new().expect("temporary directory");
        let (_store, dedup) = open(&dir).await;
        let key = key_for("prn_one", "key-one");
        dedup.lookup(&key).await;
        dedup
            .record(
                &key,
                record("digest-first", SpokeResult::Ok(json!({ "deleted": 1 }))),
            )
            .await;
        let second = record(
            "digest-second",
            SpokeResult::Ok(json!({ "deleted": 5, "floor_revision": 6 })),
        );
        dedup.record(&key, second.clone()).await;
        assert_eq!(recorded(dedup.lookup(&key).await, "replace"), second);
    }

    #[tokio::test]
    async fn an_unsettled_claim_answers_evicted_after_the_file_is_reopened() {
        let dir = TempDir::new().expect("temporary directory");
        {
            let (_store, dedup) = open(&dir).await;
            assert_eq!(
                dedup.lookup(&key_for("prn_one", "key-one")).await,
                HistoryDedupLookup::Unseen
            );
        }
        let (_store, dedup) = open(&dir).await;
        assert_eq!(
            dedup.lookup(&key_for("prn_one", "key-one")).await,
            HistoryDedupLookup::Evicted
        );
    }

    /// Overwrite a claimed key with a stored outcome this module never writes.
    async fn install_outcome(store: &MindStore, key: &str, stored: &str) {
        sqlx::query(
            "UPDATE history_dedup SET payload_digest = ?, outcome_json = ? \
             WHERE namespace = ? AND key = ?",
        )
        .bind("digest-invalid")
        .bind(stored)
        .bind(COLLECTION)
        .bind(key)
        .execute(&store.pool())
        .await
        .expect("install an invalid outcome");
    }

    #[tokio::test]
    async fn an_outcome_that_does_not_decode_answers_evicted() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        let key = key_for("prn_one", "key-one");
        dedup.lookup(&key).await;
        install_outcome(
            &store,
            &key,
            "{\"reject\":{\"code\":\"NOT_A_REJECT_CODE\"}}",
        )
        .await;
        assert_eq!(dedup.lookup(&key).await, HistoryDedupLookup::Evicted);
    }

    #[tokio::test]
    async fn a_parseable_record_with_both_variants_answers_evicted() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        let key = key_for("prn_one", "key-one");
        dedup.lookup(&key).await;
        install_outcome(
            &store,
            &key,
            "{\"ok\":{\"deleted\":1},\"reject\":{\"code\":\"NO_SUCH_CODE\"}}",
        )
        .await;
        assert_eq!(dedup.lookup(&key).await, HistoryDedupLookup::Evicted);
    }

    #[tokio::test]
    async fn a_reject_with_unexpected_members_answers_evicted() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        let key = key_for("prn_one", "key-one");
        dedup.lookup(&key).await;
        install_outcome(
            &store,
            &key,
            "{\"reject\":{\"code\":\"INVALID_INPUT\",\"message\":\"m\",\"details\":null,\
             \"extra\":1}}",
        )
        .await;
        assert_eq!(dedup.lookup(&key).await, HistoryDedupLookup::Evicted);
    }

    /// Two registered Minds share a wire scope, a principal and an idempotency key. The second must
    /// not replay the first's prune: its namespace is its own registered collection.
    #[tokio::test]
    async fn the_same_key_in_another_namespace_does_not_replay() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open_two(&dir).await;
        let first = key_for("prn_one", "key-one");
        let second = key_in(OTHER_MIND, "prn_one", "key-one");
        let written = record("digest-wren", SpokeResult::Ok(json!({ "deleted": 1 })));
        dedup.lookup(&first).await;
        dedup.record(&first, written.clone()).await;

        assert_eq!(
            recorded(dedup.lookup(&first).await, "same collection"),
            written
        );
        assert_eq!(
            dedup.lookup(&second).await,
            HistoryDedupLookup::Unseen,
            "another Mind's key is unseen, so its own prune runs"
        );
        assert_eq!(row_count(&store, COLLECTION).await, 1);
        assert_eq!(row_count(&store, OTHER_COLLECTION).await, 1);

        let other = record("digest-mora", SpokeResult::Ok(json!({ "deleted": 4 })));
        dedup.record(&second, other.clone()).await;
        assert_eq!(recorded(dedup.lookup(&first).await, "still wren"), written);
        assert_eq!(recorded(dedup.lookup(&second).await, "mora"), other);
    }

    /// A principal at its bound refuses an unseen key instead of inserting one more row, so a
    /// stream of fresh keys — refusals for a missing target included — cannot exhaust storage.
    #[tokio::test]
    async fn an_unseen_key_at_capacity_answers_evicted_without_a_new_row() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open_two(&dir).await;
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
            principal_row_count(&store, COLLECTION, "prn_one").await,
            HISTORY_DEDUP_CAPACITY
        );

        assert_eq!(
            dedup.lookup(&key_for("prn_one", "key-overflow")).await,
            HistoryDedupLookup::Evicted,
            "the bound refuses a fresh key, so Connect answers RECEIPT_EXPIRED"
        );
        assert_eq!(
            principal_row_count(&store, COLLECTION, "prn_one").await,
            HISTORY_DEDUP_CAPACITY,
            "the refused claim inserted no row"
        );

        dedup
            .record(
                &key_for("prn_one", "key-unclaimed"),
                record("digest", SpokeResult::Ok(json!({ "deleted": 0 }))),
            )
            .await;
        assert_eq!(
            principal_row_count(&store, COLLECTION, "prn_one").await,
            HISTORY_DEDUP_CAPACITY
        );

        assert_eq!(
            recorded(
                dedup.lookup(&key_for("prn_one", "key-0")).await,
                "at the bound"
            ),
            record("digest", SpokeResult::Ok(json!({ "deleted": 0 })))
        );

        assert_eq!(
            dedup
                .lookup(&key_in(OTHER_MIND, "prn_one", "key-overflow"))
                .await,
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
            principal_row_count(&store, COLLECTION, "prn_noisy").await,
            HISTORY_DEDUP_CAPACITY
        );
        assert_eq!(
            principal_row_count(&store, COLLECTION, "prn_quiet").await,
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

        assert_eq!(recorded(dedup.lookup(&key).await, "before reopen"), written);
        drop(dedup);
        let (_store, reopened) = open(&dir).await;
        assert_eq!(
            recorded(reopened.lookup(&key).await, "after reopen"),
            written
        );
    }

    /// A settlement failure is never answered as success, and never leaves a phantom success
    /// behind: nothing is written, and the claim is released, so a same-key retry claims fresh and
    /// executes instead of reading back an outcome the caller was told it had.
    ///
    /// The fault is installed as a database-level trigger refusing the `record` upsert, which is
    /// what a full disk, a locked page or a corrupt file looks like to this statement. The claim
    /// `lookup` makes is a plain insert and still lands, so the failure is the settlement alone.
    #[tokio::test]
    async fn a_settlement_fault_is_not_answered_as_success_and_leaves_no_phantom() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        let dedup = Arc::new(dedup);
        let key = key_for("prn_one", "key-one");
        assert_eq!(dedup.lookup(&key).await, HistoryDedupLookup::Unseen);

        sqlx::query(
            "CREATE TRIGGER refuse_settlement BEFORE UPDATE ON history_dedup \
             WHEN NEW.payload_digest IS NOT NULL \
             BEGIN SELECT RAISE(ABORT, 'settlement refused'); END",
        )
        .execute(&store.pool())
        .await
        .expect("install the settlement fault");

        let settle = {
            let dedup = Arc::clone(&dedup);
            let key = key.clone();
            tokio::spawn(async move {
                dedup
                    .record(
                        &key,
                        record("digest", SpokeResult::Ok(json!({ "deleted": 2 }))),
                    )
                    .await
            })
        };
        let joined = settle.await;
        let join = match joined {
            Ok(()) => panic!("a failed settlement returned normally, which reads as success"),
            Err(join) => join,
        };
        assert!(join.is_panic(), "the settlement fault aborted the invoke");
        let message = join
            .into_panic()
            .downcast_ref::<String>()
            .cloned()
            .unwrap_or_else(|| "<non-string panic payload>".to_string());
        assert!(
            message.contains("could not be settled"),
            "the fault names its stage for the host's log: {message}"
        );

        assert_eq!(
            row_count(&store, COLLECTION).await,
            0,
            "the failed settlement neither wrote an outcome nor left a claim"
        );
        sqlx::query("DROP TRIGGER refuse_settlement")
            .execute(&store.pool())
            .await
            .expect("remove the settlement fault");
        assert_eq!(
            dedup.lookup(&key).await,
            HistoryDedupLookup::Unseen,
            "the released key is claimable again"
        );
    }

    /// The discrimination rule itself: `INTERNAL_ERROR` is a storage fault, and every decision the
    /// work can reach is not. A code added to the first list is a deliberate act, which is why the
    /// rule is a list and not a text match.
    #[test]
    fn only_a_storage_fault_is_refused_settlement() {
        assert!(
            is_storage_fault(SpokeRejectCode::InternalError),
            "the served path's one fault code"
        );
        for decision in [
            SpokeRejectCode::NotAuthorized,
            SpokeRejectCode::NotFound,
            SpokeRejectCode::KnowledgeEntryNotFound,
            SpokeRejectCode::HistoryTruncated,
            SpokeRejectCode::HistoryNotRetained,
            SpokeRejectCode::HistoryEntityUnsupported,
            SpokeRejectCode::InvalidInput,
            SpokeRejectCode::IdempotencyConflict,
            SpokeRejectCode::OutcomeUncertain,
        ] {
            assert!(
                !is_storage_fault(decision),
                "{} settles as the key's answer",
                decision.as_str()
            );
        }
    }

    /// A storage fault the port answers is neither the key's settled outcome nor the end of the
    /// key: the claim is released, so a retry once the fault clears claims again and executes
    /// instead of answering `RECEIPT_EXPIRED` for the database's lifetime. The caller who hit the
    /// fault is answered as unsettled.
    #[tokio::test]
    async fn a_port_fault_releases_the_claim_so_the_key_can_execute_again() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        let dedup = Arc::new(dedup);
        let key = key_for("prn_one", "key-one");
        assert_eq!(dedup.lookup(&key).await, HistoryDedupLookup::Unseen);

        let faulted = record(
            "digest",
            SpokeResult::Reject(SpokeReject {
                code: SpokeRejectCode::InternalError,
                message: "knowledge storage is unavailable".to_string(),
                details: None,
            }),
        );
        let settle = {
            let dedup = Arc::clone(&dedup);
            let key = key.clone();
            let faulted = faulted.clone();
            tokio::spawn(async move { dedup.record(&key, faulted).await })
        };
        let join = match settle.await {
            Ok(()) => panic!("a fault was settled and returned as the caller's answer"),
            Err(join) => join,
        };
        assert!(
            join.is_panic(),
            "the fault left through the unsettled channel"
        );

        assert_eq!(
            row_count(&store, COLLECTION).await,
            0,
            "the fault was neither written as the key's outcome nor left as a claim"
        );

        assert_eq!(
            dedup.lookup(&key).await,
            HistoryDedupLookup::Unseen,
            "the released key is claimable again, not refused"
        );
        let executed = record(
            "digest",
            SpokeResult::Ok(json!({ "deleted": 1, "floor_revision": 2 })),
        );
        dedup.record(&key, executed.clone()).await;
        assert_eq!(
            recorded(dedup.lookup(&key).await, "after the fault cleared"),
            executed,
            "the recovered key settles and replays its own result"
        );
    }

    /// The other side of the rule: a refusal that is a decision settles and replays, because it
    /// is the answer about this request, not a condition of the database.
    #[tokio::test]
    async fn a_decision_refusal_settles_and_replays() {
        let dir = TempDir::new().expect("temporary directory");
        let (_store, dedup) = open(&dir).await;
        for (index, code) in [
            SpokeRejectCode::NotAuthorized,
            SpokeRejectCode::NotFound,
            SpokeRejectCode::HistoryTruncated,
            SpokeRejectCode::HistoryEntityUnsupported,
            SpokeRejectCode::InvalidInput,
        ]
        .into_iter()
        .enumerate()
        {
            let key = key_for("prn_one", &format!("key-{index}"));
            let written = record(
                "digest",
                SpokeResult::Reject(SpokeReject {
                    code,
                    message: format!("{} is this key's answer", code.as_str()),
                    details: None,
                }),
            );
            dedup.lookup(&key).await;
            dedup.record(&key, written.clone()).await;
            assert_eq!(
                recorded(dedup.lookup(&key).await, code.as_str()),
                written,
                "{} settles and replays",
                code.as_str()
            );
        }
    }

    /// A registry fault during settlement is not "nothing to settle". The claim landed, so the
    /// prune may have committed; the failure takes the same fault path as a failed write rather
    /// than returning as success.
    #[tokio::test]
    async fn a_registry_fault_during_settlement_is_not_answered_as_success() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        let dedup = Arc::new(dedup);
        let key = key_for("prn_one", "key-one");
        assert_eq!(dedup.lookup(&key).await, HistoryDedupLookup::Unseen);
        sqlx::query("DROP TABLE minds")
            .execute(&store.pool())
            .await
            .expect("drop the registry");

        let settle = {
            let dedup = Arc::clone(&dedup);
            let key = key.clone();
            tokio::spawn(async move {
                dedup
                    .record(
                        &key,
                        record("digest", SpokeResult::Ok(json!({ "deleted": 2 }))),
                    )
                    .await
            })
        };
        let join = match settle.await {
            Ok(()) => panic!("a settlement it could not resolve returned as success"),
            Err(join) => join,
        };
        assert!(join.is_panic(), "the settlement fault aborted the invoke");
        assert_eq!(
            dedup.lookup(&key).await,
            HistoryDedupLookup::Evicted,
            "an unresolvable namespace never answers Unseen or a recorded outcome"
        );
    }

    /// A read fault **after** the claim insert must not spend the key. `lookup` inserts the claim,
    /// then reads the row back; when that read cannot run, the call has created a row it will never
    /// fill, so answering `Evicted` without removing it would answer `RECEIPT_EXPIRED` for the key's
    /// lifetime. The claim is released and the key is claimable again once the fault clears.
    ///
    /// The fault is the struct's test seam, not a database fixture: SQLite cannot express a failure
    /// that breaks only the follow-up `SELECT` on a writable table (a broken view breaks the claim
    /// insert and the guarded delete too, which is how an earlier version of this test passed
    /// without exercising the path at all). The seam sits exactly at the read, so the claim is
    /// genuinely made and only the read after it fails.
    #[tokio::test]
    async fn a_lookup_read_fault_releases_the_claim_it_just_made() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        let key = key_for("prn_one", "key-one");

        dedup.fail_the_read_after_the_claim();
        assert_eq!(
            dedup.lookup(&key).await,
            HistoryDedupLookup::Evicted,
            "a read that cannot run answers Evicted, so the prune is not executed"
        );
        assert_eq!(
            row_count(&store, COLLECTION).await,
            0,
            "the claim this call inserted was released, not left to spend the key"
        );

        dedup.read_is_healthy_again();
        assert_eq!(
            dedup.lookup(&key).await,
            HistoryDedupLookup::Unseen,
            "the key is claimable again, not spent by a failed read"
        );
        let executed = record("digest", SpokeResult::Ok(json!({ "deleted": 1 })));
        dedup.record(&key, executed.clone()).await;
        assert_eq!(
            recorded(dedup.lookup(&key).await, "after the read fault cleared"),
            executed,
            "the recovered key settles and replays its own result"
        );
    }

    /// The release path never removes a row that carries an outcome. A settled outcome is the key's
    /// answer; a release is only ever for an unanswered claim.
    #[tokio::test]
    async fn a_release_leaves_a_settled_outcome_alone() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        let key = key_for("prn_one", "key-one");
        dedup.lookup(&key).await;
        let written = record("digest", SpokeResult::Ok(json!({ "deleted": 3 })));
        dedup.record(&key, written.clone()).await;

        dedup.release_claim(&key).await;
        assert_eq!(
            recorded(dedup.lookup(&key).await, "a release cannot un-settle a key"),
            written
        );
        assert_eq!(row_count(&store, COLLECTION).await, 1);
    }

    /// The `record` path a caller reaches with a key that names no Mind writes nothing and does
    /// not fault: `lookup` answered `Evicted`, so no prune ran and no outcome needs settling.
    #[tokio::test]
    async fn a_settlement_for_an_unresolvable_namespace_is_silent() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        let unregistered = key_in("spc_nobody", "prn_one", "key-one");
        dedup
            .record(
                &unregistered,
                record("digest", SpokeResult::Ok(json!({ "deleted": 1 }))),
            )
            .await;
        assert_eq!(row_count(&store, COLLECTION).await, 0);
        assert_eq!(row_count(&store, "mind:spc_nobody").await, 0);
    }

    /// Unparsable keys share one bucket, so a caller cannot mint a fresh quota per odd key. They
    /// also name no Mind, so no namespace resolves and the prune is never executed.
    #[tokio::test]
    async fn keys_that_do_not_parse_share_one_principal_bucket() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        for key in ["not-a-canonical-key", "[1,2]", "{\"not\":\"an array\"}"] {
            assert_eq!(
                dedup.lookup(key).await,
                HistoryDedupLookup::Evicted,
                "a key that names no Mind never claims"
            );
            assert_eq!(principal_of(key), HISTORY_DEDUP_UNKNOWN_PRINCIPAL);
            dedup
                .record(
                    key,
                    record("digest", SpokeResult::Ok(json!({ "deleted": 1 }))),
                )
                .await;
        }
        assert_eq!(
            row_count(&store, COLLECTION).await,
            0,
            "no unparsable key reached the table"
        );
    }

    /// A non-string third member is not a principal either, so it lands in the same bucket.
    #[tokio::test]
    async fn a_non_string_principal_member_uses_the_unknown_bucket() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        let key = json!(["history.revisions.prune", { "scope_id": MIND }, 7, "k"]).to_string();
        assert_eq!(dedup.lookup(&key).await, HistoryDedupLookup::Unseen);
        dedup
            .record(
                &key,
                record("digest", SpokeResult::Ok(json!({ "deleted": 1 }))),
            )
            .await;
        assert_eq!(
            principal_row_count(&store, COLLECTION, HISTORY_DEDUP_UNKNOWN_PRINCIPAL).await,
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
            row_count(&store, COLLECTION).await,
            0,
            "an unclaimed record inserted no row"
        );
        assert_eq!(
            dedup.lookup(&key).await,
            HistoryDedupLookup::Unseen,
            "the key is still fresh, so its prune runs rather than replaying"
        );
    }

    /// A key that is not the canonical tuple, and one naming a Mind with no registry row, both
    /// answer `Evicted` — the prune is not executed — and neither writes a row.
    #[tokio::test]
    async fn a_key_that_names_no_registered_mind_answers_evicted() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open(&dir).await;
        let noncanonical = [
            "not-a-canonical-key".to_string(),
            "[1,2]".to_string(),
            json!([
                "history.revisions.prune",
                "not-a-scope",
                "prn_one",
                "key-one"
            ])
            .to_string(),
            json!(["history.revisions.prune", { "scope_id": 7 }, "prn_one", "key-one"]).to_string(),
        ];
        for key in &noncanonical {
            assert_eq!(
                dedup.lookup(key).await,
                HistoryDedupLookup::Evicted,
                "{key}"
            );
            dedup
                .record(
                    key,
                    record("digest", SpokeResult::Ok(json!({ "deleted": 1 }))),
                )
                .await;
        }
        let unregistered = key_in("spc_nobody", "prn_one", "key-one");
        assert_eq!(
            dedup.lookup(&unregistered).await,
            HistoryDedupLookup::Evicted,
            "a Mind with no registry row never claims"
        );
        dedup
            .record(
                &unregistered,
                record("digest", SpokeResult::Ok(json!({ "deleted": 1 }))),
            )
            .await;
        assert_eq!(row_count(&store, COLLECTION).await, 0);
        assert_eq!(row_count(&store, "mind:spc_nobody").await, 0);
    }

    /// The bound counts one `(namespace, principal)` pair exactly: filling one Mind's collection
    /// leaves another Mind's quota untouched.
    #[tokio::test]
    async fn the_capacity_count_holds_only_the_named_namespace() {
        let dir = TempDir::new().expect("temporary directory");
        let (store, dedup) = open_two(&dir).await;
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
            principal_row_count(&store, COLLECTION, "prn_one").await,
            HISTORY_DEDUP_CAPACITY,
            "the Mind's quota is full"
        );
        assert_eq!(row_count(&store, OTHER_COLLECTION).await, 0);

        assert_eq!(
            dedup
                .lookup(&key_in(OTHER_MIND, "prn_one", "key-fresh"))
                .await,
            HistoryDedupLookup::Unseen,
            "the full neighbour's rows are not this namespace's"
        );
        assert_eq!(row_count(&store, OTHER_COLLECTION).await, 1);
        assert_eq!(
            principal_row_count(&store, COLLECTION, "prn_one").await,
            HISTORY_DEDUP_CAPACITY,
            "the neighbour's claim left the full namespace alone"
        );
    }
}
