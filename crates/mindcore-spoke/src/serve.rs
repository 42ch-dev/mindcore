//! The serving seam: the memory service this crate binds to a Connect responder, and the
//! `memory.*` operations it can prove it dispatches.

use async_trait::async_trait;
use serde_json::Value;
use spoke_connect::remote::{MemoryAdmission, RemoteMemoryService};
use spoke_operations::{spoke_reject, SpokeRejectCode, SpokeResult};
use sqlx::SqlitePool;

use crate::reject::Reject;
use crate::transfer;
use crate::{MemoryAdapter, MemoryStage};

/// §7.28 receipt polling: what a client is told to resolve an `OUTCOME_UNCERTAIN` transfer
/// commit through, so the face that answers the five transfers must answer this too.
pub(crate) const RECEIPT_GET: &str = "memory.receipts.get";

/// What this service dispatches: the five §7.29–§7.33 operations plus receipt polling, which
/// reads the same receipt namespace over the same store.
static SERVED: [&str; 6] = [
    transfer::CREATE,
    transfer::GET,
    transfer::STAGE,
    transfer::ACTIVATE,
    transfer::DISCARD,
    RECEIPT_GET,
];

/// A memory service that can name the `memory.*` operations it dispatches.
///
/// Advertisement is derived from this answer, never from a caller-declared claim. A service that
/// implements only `RemoteMemoryService` takes the default and reports nothing, so attaching it
/// claims only the family its installation proves.
pub trait ServedMemory: RemoteMemoryService {
    fn operations(&self) -> &'static [&'static str] {
        &[]
    }
}

/// The five §7.29–§7.33 operations and §7.28 receipt polling over the host's store.
///
/// `RemoteMemoryService::invoke` hands the service no transaction, so the service is the caller:
/// each op runs in one transaction on the host's pool — `BEGIN IMMEDIATE` for the four mutations,
/// a read for `exports.get` and `receipts.get` — and commits only after the adapter answers
/// `Ready`. Any other `memory.*` op is refused as `CapabilityPortMissing`; a host that needs the
/// rest of the family binds it separately.
pub struct TransferService {
    pool: SqlitePool,
    admission: MemoryAdmission,
    adapter: MemoryAdapter,
}

impl TransferService {
    /// `admission` maps the authenticated peer to its principal, exactly as the host's other
    /// memory face does; the principal never comes from the payload.
    #[must_use]
    pub fn new(pool: SqlitePool, admission: MemoryAdmission) -> Self {
        Self {
            pool,
            admission,
            // why: the transfer family reads no reflection policy.
            adapter: MemoryAdapter::new(false),
        }
    }
}

impl ServedMemory for TransferService {
    fn operations(&self) -> &'static [&'static str] {
        &SERVED
    }
}

#[async_trait]
impl RemoteMemoryService for TransferService {
    async fn invoke(&self, peer_id: &str, op: &str, payload: Value) -> SpokeResult<Value> {
        let principal = match (self.admission)(peer_id) {
            SpokeResult::Ok(principal) => principal,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        // why: no op ran, so a refused reservation has no outcome in doubt.
        let read = op == transfer::GET || op == RECEIPT_GET;
        let begun = match read {
            true => self.pool.begin().await,
            false => self.pool.begin_with("BEGIN IMMEDIATE").await,
        };
        let mut tx = match begun {
            Ok(tx) => tx,
            Err(_) => return reject(Reject::internal(format!("{op} could not begin"))),
        };
        let staged = match op {
            transfer::CREATE => {
                self.adapter
                    .create_export(&mut tx, &principal, payload)
                    .await
            }
            transfer::GET => self.adapter.get_export(&mut tx, &principal, payload).await,
            transfer::STAGE => {
                self.adapter
                    .stage_import(&mut tx, &principal, payload)
                    .await
            }
            transfer::ACTIVATE => {
                self.adapter
                    .activate_import(&mut tx, &principal, payload)
                    .await
            }
            transfer::DISCARD => {
                self.adapter
                    .discard_import(&mut tx, &principal, payload)
                    .await
            }
            RECEIPT_GET => self.adapter.get_receipt(&mut tx, &principal, payload).await,
            _ => {
                return spoke_reject(
                    SpokeRejectCode::CapabilityPortMissing,
                    format!("the mindcore transfer service does not serve {op}"),
                    None,
                )
            }
        };
        let body = match staged {
            SpokeResult::Ok(MemoryStage::Ready(body)) => body,
            // why: every routed op above settles inside its own transaction; a staged settlement
            // would need a second caller phase this seam does not own.
            SpokeResult::Ok(MemoryStage::JournalPending { .. })
            | SpokeResult::Ok(MemoryStage::ReflectionPending(_)) => {
                return reject(Reject::internal(format!(
                    "{op} staged an outcome the serving seam cannot finish"
                )))
            }
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        match tx.commit().await {
            Ok(()) => SpokeResult::Ok(body),
            // §14.2/§14.3: the effect may or may not be durable; never claim either way,
            // and name the receipt the client can poll so it can resolve without a retry.
            Err(_) => reject(commit_uncertain(op, &body)),
        }
    }
}

/// The commit acknowledgement was lost. The receipt id is the client's resolution
/// handle (§7.28 `memory.receipts.get`); it is omitted only if the settled body carried
/// no receipt, which no routed mutation does.
fn commit_uncertain(op: &str, body: &Value) -> Reject {
    let mut reject = Reject::uncertain(format!("{op} commit outcome was not confirmed"));
    if let Some(receipt_id) = body.get("receipt_id").cloned() {
        reject = reject.detail("receipt_id", receipt_id);
    }
    reject
}

fn reject(reject: Reject) -> SpokeResult<Value> {
    SpokeResult::Reject(reject.into_spoke())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use spoke_operations::SpokeRejectCode;

    use super::commit_uncertain;

    #[test]
    fn an_unconfirmed_commit_names_the_receipt_that_resolves_it() {
        let body = json!({ "schema_version": 1, "receipt_id": "rct_1", "status": "committed" });
        let reject = commit_uncertain("memory.imports.activate", &body).into_spoke();
        assert_eq!(reject.code, SpokeRejectCode::OutcomeUncertain);
        let details = reject.details.expect("refusal details");
        assert_eq!(details["receipt_id"], json!("rct_1"));
        assert_eq!(details["retry_class"], json!("uncertain"));
    }
}
