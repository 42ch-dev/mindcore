//! The serving seam: the memory service this crate binds to a Connect responder, and the
//! `memory.*` operations it can prove it dispatches.

use async_trait::async_trait;
use serde_json::Value;
use spoke_connect::remote::{MemoryAdmission, RemoteMemoryService};
use spoke_operations::{spoke_reject, SpokeRejectCode, SpokeResult};
use sqlx::SqlitePool;

use crate::reject::Reject;
use crate::transfer::{self, OPERATIONS};
use crate::{MemoryAdapter, MemoryStage};

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

/// The five §7.29–§7.33 operations over the host's store.
///
/// `RemoteMemoryService::invoke` hands the service no transaction, so the service is the caller:
/// each op runs in one transaction on the host's pool — `BEGIN IMMEDIATE` for the four mutations,
/// a read for `exports.get` — and commits only after the adapter answers `Ready`. Any other
/// `memory.*` op is refused as `CapabilityPortMissing`; a host that needs the rest of the family
/// binds it separately.
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
        &OPERATIONS
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
        let begun = match op == transfer::GET {
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
            // §14.2/§14.3: the effect may or may not be durable; never claim either way.
            Err(_) => reject(Reject::uncertain(format!(
                "{op} commit outcome was not confirmed"
            ))),
        }
    }
}

fn reject(reject: Reject) -> SpokeResult<Value> {
    SpokeResult::Reject(reject.into_spoke())
}
