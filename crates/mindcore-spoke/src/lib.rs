//! SPOKE memory protocol adapter over the borrowed-transaction
//! [`MemoryRepository`](mindcore_base::ir::MemoryRepository).
//!
//! The adapter owns every protocol rule — wire validation, the wire↔IR
//! boundary, reject mapping, admission, replay and staging — and delegates
//! storage only. It never opens, commits or rolls back a transaction: a
//! successful stage is the caller's to commit, and a [`MemoryStage`] value
//! (never wire data) says whether that stage is durable as-is or still owes a
//! publication phase.

mod admission;
mod content;
mod control;
mod effects;
mod policy;
mod project;
mod reflection;
mod reject;
mod serve;
mod transfer;
mod wire;
mod workflows;

pub use serve::{ServedMemory, TransferService};
pub use transfer::OPERATIONS as TRANSFER_OPERATIONS;
pub use workflows::{admitted_carrier_entry, MemoryWorkflowBinding};

pub(crate) use mindcore_base::ids::mint;

use chrono::Utc;
use serde::de::DeserializeOwned;
use serde_json::Value;
use spoke_operations::{GrantRequirement, SpokeReject, SpokeRejectCode, SpokeResult};
use spoke_schemas::Principal;
use sqlx::{Sqlite, Transaction};

use mindcore_base::ids::{SpaceId, SubjectId};
use mindcore_spi::SpiError;
use mindcore_base::ir::{MemoryRepository, ReceiptFailure, ReceiptState, ReviewReceiptIr};
use mindcore_sqlite::{read_entry, SqliteMemoryRepository};

use crate::admission::Op;
use crate::reject::{AResult, Reject};

/// The knowledge-read capability. A grant carrying it admits its principal to
/// knowledge reads in that grant's space; the open capability vocabulary names
/// no knowledge capability, and no memory capability (`recall`, `records_read`)
/// stands in for it.
pub(crate) const KNOWLEDGE_READ: &str = "knowledge_read";

/// The carrier-write capability. Reading a collection is not authority to
/// modify the judgment it carries, so the write transaction requires this on
/// its own.
pub(crate) const KNOWLEDGE_WRITE: &str = "knowledge_write";

/// The memory capability the subject page was read under; the write rechecks
/// it because a verdict over that page depends on it.
pub(crate) const RECALL: &str = "recall";

/// The derived judgment of one Mind's cognition carrier: the carrier revision
/// the read served, its current-view records, and the `MindState` projection of
/// them. Persisted nowhere; it is derived on every read.
#[derive(Debug, Clone)]
pub struct Cognition {
    /// The carrier revision the read served.
    pub carrier_revision: u64,
    /// The carrier's current-view cognition records, in carrier order.
    pub records: Vec<Value>,
    /// The derived `MindState` wire value for this read.
    pub mind_state: Value,
}

/// The caller-owned transaction every entry point reads and writes through.
pub type Tx<'a> = Transaction<'a, Sqlite>;

/// The outcome of one operation, held inside the caller's transaction.
#[derive(Debug)]
pub enum MemoryStage {
    /// The pinned success response; the caller may commit and return it.
    Ready(Value),
    /// A journaled effect is staged: pending receipt, durable intent and the
    /// file targets the caller must publish before settling the intent.
    JournalPending { space: SpaceId, receipt_id: String },
    /// A Reflection generation is leased: the caller commits transaction 1,
    /// composes the result outside it, then completes in transaction 2.
    ReflectionPending(ReflectionWork),
}

/// Everything the second Reflection transaction needs after transaction 1
/// committed: the leased pending receipt and the basis it was pinned to.
#[derive(Debug, Clone)]
pub struct ReflectionWork {
    pub space: SpaceId,
    pub receipt_id: String,
    /// The lease owner stamped on the pending receipt (§7.0.3).
    pub owner_id: String,
    pub subject_id: String,
    pub selection: Value,
    pub filter: Option<Value>,
    pub basis: Value,
    pub engine_policy_version: &'static str,
}

impl ReflectionWork {
    /// Compose the deterministic §7.27 result outside any transaction: the
    /// body the pinned basis renders, the cost it owes, and the receipt and
    /// lease facts transaction 2 settles against. Composing here — after the
    /// commit — keeps the parked window free of any unfinished envelope.
    ///
    /// simplify: the local engine renders the digest-pinned basis snapshot,
    /// not record bodies; upgrade: when the real cognition engine plugs in at this seam.
    pub fn compose(self) -> ReflectionEffect {
        let record_count = self
            .basis
            .get("record_count")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let state_digest = self
            .basis
            .get("state_digest")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let body_text = format!(
            "# Reflection\n\nSubject `{}` reflected over {record_count} live basis \
             records at state `{}` (engine `{}`).\n",
            self.subject_id,
            &state_digest[..state_digest.len().min(16)],
            self.engine_policy_version,
        );
        ReflectionEffect {
            space: self.space,
            receipt_id: self.receipt_id,
            owner_id: self.owner_id,
            subject_id: self.subject_id,
            selection: self.selection,
            filter: self.filter,
            basis: self.basis,
            body_text,
            body_format: "markdown".to_string(),
            cost: ("basis_record".to_string(), record_count as f64),
            engine_policy_version: self.engine_policy_version,
        }
    }
}

/// The composed §7.27 effect `complete_reflection` settles in transaction 2:
/// the envelope members plus the receipt identity and lease owner it must
/// still match.
#[derive(Debug)]
pub struct ReflectionEffect {
    pub space: SpaceId,
    pub receipt_id: String,
    /// The lease owner transaction 2 must still find on the pending receipt;
    /// any other owner or state rolls the envelope back with the settlement.
    pub owner_id: String,
    pub subject_id: String,
    pub selection: Value,
    pub filter: Option<Value>,
    pub basis: Value,
    pub body_text: String,
    pub body_format: String,
    /// `(unit_kind, units)` the engine charges this generation.
    pub cost: (String, f64),
    pub engine_policy_version: &'static str,
}

/// The in-process SPOKE memory adapter.
pub struct MemoryAdapter {
    repository: SqliteMemoryRepository,
    reflections: bool,
}

impl MemoryAdapter {
    /// `reflections` is the instance's own policy switch: it decides both what
    /// `describe` advertises and whether §7.25–§7.27 are served.
    #[must_use]
    pub fn new(reflections: bool) -> Self {
        Self {
            repository: SqliteMemoryRepository,
            reflections,
        }
    }

    /// Validate and validate-again one rebuilt success body against its pinned
    /// response type.
    pub(crate) fn ready<T: DeserializeOwned>(body: Value) -> AResult<MemoryStage> {
        Ok(MemoryStage::Ready(project::checked::<T>(body)?))
    }

    /// Validate one already-built success body against an op-specific response
    /// type; a staged result passes through untouched.
    pub(crate) fn recheck<T: DeserializeOwned>(stage: MemoryStage) -> AResult<MemoryStage> {
        match stage {
            MemoryStage::Ready(body) => Self::ready::<T>(body),
            stage => Ok(stage),
        }
    }

    /// §7.0.5: a receipt the dedup namespace holds resolves the key. A
    /// committed replay is a published success response, so it passes the
    /// operation's pinned response type before it can leave the adapter.
    pub(crate) fn replay<T: DeserializeOwned>(receipt: &ReviewReceiptIr) -> AResult<MemoryStage> {
        match &receipt.state {
            ReceiptState::Committed { .. } => Self::ready::<T>(project::receipt_body(receipt)),
            ReceiptState::Failed { error, .. } => Err(recorded_failure(error)),
            ReceiptState::Pending => Err(Reject::uncertain(format!(
                "receipt {} is still pending",
                receipt.receipt_id
            ))),
        }
    }

    /// Project one op's request facts; the digest binds the original request.
    pub(crate) fn op<'a>(
        space: &str,
        principal: &'a str,
        operation: &'static str,
        idempotency_key: Option<&str>,
        request: &Value,
    ) -> AResult<Op<'a>> {
        Ok(Op {
            space: SpaceId::new(space),
            requester: principal,
            operation,
            idempotency_key: idempotency_key.map(str::to_string),
            payload_digest: policy::payload_digest(request)?,
            now: chrono::Utc::now(),
        })
    }

    /// §5.2/D-10: the caller's subject content stays withheld while any of its
    /// durable intents is unsettled. Authorized read paths probe this on the
    /// same borrowed transaction as the read — after their admission and
    /// visibility checks and before any content is selected or projected.
    async fn withhold_if_unsettled(
        &self,
        tx: &mut Tx<'_>,
        space: &SpaceId,
        subject_id: &str,
    ) -> AResult<()> {
        let unsettled = self
            .repository
            .has_unsettled_intent(tx, space, &SubjectId::new(subject_id))
            .await?;
        if unsettled {
            Err(Reject::unreconciled(subject_id))
        } else {
            Ok(())
        }
    }

    /// The space-level withhold of a subjectless deliberation: refuses while any
    /// durable intent in the space is unsettled, whichever subject it belongs to.
    pub(crate) async fn withhold_space_if_unsettled(
        &self,
        tx: &mut Tx<'_>,
        space: &SpaceId,
    ) -> AResult<()> {
        let unsettled = self
            .repository
            .has_unsettled_intent_in_space(tx, space)
            .await?;
        if unsettled {
            Err(Reject::unreconciled_space(space.as_str()))
        } else {
            Ok(())
        }
    }

    /// The caller's space-wide live `recall` grant, rechecked on `tx`: the
    /// requirement shape `memory.spaces.freeze` uses, moved here from the
    /// subject-level revalidation for a subjectless deliberation.
    pub(crate) async fn revalidate_space(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        space_id: &str,
    ) -> Result<(), SpiError> {
        self.require_space_grants(tx, principal, space_id, &[RECALL])
            .await
    }

    /// `principal`'s own live grant for every one of `capabilities` in `space_id`, on `tx`.
    ///
    /// Live grants are re-derived on every call, so a read after provider work runs the same
    /// admission and a revoked grant refuses it. A memory grant never substitutes for a
    /// knowledge capability: only a capability the caller names admits its principal.
    pub async fn require_space_grants(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        space_id: &str,
        capabilities: &[&str],
    ) -> Result<(), SpiError> {
        let caller = principal_id(principal);
        let space = SpaceId::new(space_id.to_string());
        let rows = admission::grants(&self.repository, tx, &space)
            .await
            .map_err(|reject| SpiError::Settlement(reject.into_spoke()))?;
        let grants = admission::wire_grants(&rows)
            .map_err(|reject| SpiError::Settlement(reject.into_spoke()))?;
        for capability in capabilities {
            let requirement = GrantRequirement {
                capability,
                subject_id: None,
                context_ids: Vec::new(),
                reads_shared: false,
                subject_all: false,
            };
            if !admission::covered(&grants, caller, space.as_str(), Utc::now(), &requirement) {
                return Err(SpiError::Settlement(Reject::not_authorized(format!(
                    "principal {caller} holds no live {capability} grant in space {}",
                    space.as_str()
                )).into_spoke()));
            }
        }
        Ok(())
    }

    /// Read one Mind's judgment over the caller's transaction: admission first, then the carrier
    /// the registry row names, then the derived `MindState` — all inside `tx`, so a revocation
    /// cannot land between admission and the read. Admission is [`workflows::admitted_carrier`]:
    /// the caller's own grant rows and the registry row, read together, answering one fixed
    /// refusal with no details whether the Mind is absent or the caller was never admitted.
    /// A fresh carrier answers `records: []` and `mind_state: null`; the derivation failures a
    /// stored carrier could raise are internal errors, because the carrier passed validation at
    /// its write. The transaction is the caller's to end.
    pub async fn read_cognition(
        tx: &mut Tx<'_>,
        principal: &Principal,
        space_id: &str,
    ) -> Result<Cognition, SpokeReject> {
        let carrier = workflows::admitted_carrier(tx, principal_id(principal), space_id).await?;
        let carrier_entry =
            match read_entry(tx, &carrier.collection, &carrier.carrier_entry_id).await {
                SpokeResult::Ok(entry) => entry,
                SpokeResult::Reject(reject) => return Err(reject),
            };
        let revision = match carrier_entry.revision {
            Some(revision) => revision,
            None => {
                return Err(Reject::internal(format!(
                    "carrier {} holds no assigned revision",
                    carrier.carrier_entry_id
                ))
                .into_spoke())
            }
        };
        let view = match mindcore_base::cognition::derive_mind_state(&carrier_entry) {
            Ok(view) => view,
            Err(mindcore_base::cognition::MindError::NoRecords) => {
                return Ok(Cognition {
                    carrier_revision: revision,
                    records: Vec::new(),
                    mind_state: Value::Null,
                })
            }
            Err(error) => {
                return Err(Reject::internal(format!(
                    "carrier {} cannot be derived: {error}",
                    carrier.carrier_entry_id
                ))
                .into_spoke())
            }
        };
        let mind_state = match mindcore_base::cognition::mind_state_value(&view, space_id) {
            Ok(mind_state) => mind_state,
            Err(error) => {
                return Err(Reject::internal(format!(
                    "the mind state of {} cannot be projected: {error}",
                    carrier.carrier_entry_id
                ))
                .into_spoke())
            }
        };
        Ok(Cognition {
            carrier_revision: revision,
            records: view.records,
            mind_state,
        })
    }
}

/// The authenticated principal the caller supplied; never payload-asserted.
pub(crate) fn principal_id(principal: &Principal) -> &str {
    principal.principal_id.as_str()
}

pub(crate) fn recorded_failure(failure: &ReceiptFailure) -> Reject {
    let Some(code) = SpokeRejectCode::try_from_str(&failure.code) else {
        return Reject::terminal(
            SpokeRejectCode::InternalError,
            "the recorded failure names an unrecognized reject code",
        )
        .detail(
            "reason",
            Value::String("unrecognized_recorded_reject_code".to_string()),
        );
    };
    let class = failure
        .details
        .get("retry_class")
        .and_then(Value::as_str)
        .unwrap_or("terminal")
        .to_string();
    let mut reject = Reject::terminal(code, failure.message.clone());
    reject
        .details
        .insert("retry_class".to_string(), Value::String(class));
    for (key, value) in &failure.details {
        if key != "retry_class" {
            reject.details.insert(key.clone(), value.clone());
        }
    }
    reject
}

/// One stored row's revision bumped by one.
pub(crate) fn bump(revision: &serde_json::Number) -> serde_json::Number {
    serde_json::Number::from(revision.as_u64().unwrap_or(0).saturating_add(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(code: &str, retry_class: &str) -> ReceiptFailure {
        ReceiptFailure {
            code: code.to_string(),
            message: "the provider refused this op".to_string(),
            details: serde_json::json!({ "retry_class": retry_class })
                .as_object()
                .expect("details object")
                .clone(),
            extensions: serde_json::Map::new(),
        }
    }

    /// A persisted reject the closed wire vocabulary cannot name answers one generic
    /// terminal refusal: a same-key replay can only re-play it, so neither the
    /// recorded message nor a `retryable` class invites a second attempt. A known
    /// code replays its own class verbatim.
    #[test]
    fn a_recorded_reject_replays_its_class_and_degrades_only_an_unknown_code() {
        let unknown = recorded_failure(&failure("NO_SUCH_CODE", "retryable")).into_spoke();
        assert_eq!(unknown.code, SpokeRejectCode::InternalError);
        assert_eq!(
            unknown.message,
            "the recorded failure names an unrecognized reject code"
        );
        let details = unknown.details.expect("details");
        assert_eq!(
            details.get("retry_class").and_then(Value::as_str),
            Some("terminal")
        );
        assert_eq!(
            details.get("reason").and_then(Value::as_str),
            Some("unrecognized_recorded_reject_code")
        );

        let known = recorded_failure(&failure("NOT_FOUND", "retryable")).into_spoke();
        assert_eq!(known.code, SpokeRejectCode::NotFound);
        assert_eq!(known.message, "the provider refused this op");
        assert_eq!(
            known
                .details
                .as_ref()
                .and_then(|details| details.get("retry_class"))
                .and_then(Value::as_str),
            Some("retryable")
        );
    }
}
