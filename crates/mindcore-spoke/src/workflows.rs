//! The workflow binding: `memory.recall` plus the admitted knowledge read on the
//! caller's own transaction, projected onto the SPI's DTO-only seam.
//!
//! The adapter never opens or commits a transaction across provider work: one
//! call reads and returns, so an embedder, reasoner or policy hook runs with no
//! transaction open and the workflow can revalidate afterwards.

use async_trait::async_trait;
use chrono::Utc;
use serde_json::{json, Map, Value};
use spoke_operations::{GrantRequirement, SpokeReject, SpokeRejectCode, SpokeResult};
use spoke_schemas::data::cognition::{CognitionReference, CognitionReferenceKind};
use spoke_schemas::data::memory_candidate::PlacementScope;
use spoke_schemas::{
    KnowledgeEntry, MemoryCandidate, MemoryCandidatesDismissRequest, MemoryCandidatesReviewRequest,
    MemoryCaptureRequest, MemoryGrant, MemoryRecallRequest, MemoryRecordsUpdateRequest, Principal,
    RetainedRecord,
};
use sqlx::SqlitePool;

use mindcore_base::boundary::CallerId;
use mindcore_base::cognition::{
    append_cognition_record, carrier_records, derive_mind_state, CARRIER_NAMESPACE,
};
use mindcore_base::ids::SpaceId;
use mindcore_base::ir::{CandidateState, MemoryRepository, ReceiptKey, ReceiptRead, ReceiptState};
use mindcore_base::knowledge::KnowledgeScope;
use mindcore_spi::{
    AbsorbSnapshot, CandidateLifecycle, CaptureOutcome, CapturedCandidate, CitedRecord,
    DeliberateSnapshot, RecallSnapshot, SpiError, WorkflowBinding,
};
use mindcore_sqlite::{list_entries_in, read_entry, write_entry, SqliteMemoryRepository};

use crate::admission;
use crate::reject::Reject;
use crate::{principal_id, MemoryAdapter, MemoryStage, Tx};

/// Publishes one intent's file targets and confirms them: `Ok(())` only when
/// every target's exact bytes landed at its path.
type JournalPublisher = dyn Fn(&[Value]) -> Result<(), SpiError> + Send + Sync;

/// The knowledge-read capability. A grant carrying it admits its principal to
/// knowledge reads in that grant's space; the open capability vocabulary names
/// no knowledge capability, and no memory capability (`recall`, `records_read`)
/// stands in for it.
const KNOWLEDGE_READ: &str = "knowledge_read";

/// The carrier-write capability. Reading a collection is not authority to
/// modify the judgment it carries, so the write transaction requires this on
/// its own.
const KNOWLEDGE_WRITE: &str = "knowledge_write";

/// The memory capability the subject page was read under; the write rechecks
/// it because a verdict over that page depends on it.
const RECALL: &str = "recall";

/// One collection together with the caller it is admitted to.
struct AdmittedCollection {
    caller: String,
    scope: KnowledgeScope,
}

/// The in-process workflow binding over one SQLite pool.
///
/// `knowledge` fixes the one collection this binding serves and its one caller:
/// without it the binding serves no knowledge at all, and a collection bound to
/// another caller is refused rather than read. `publisher` is the host's file
/// medium; the settlement legs require it before they stage anything, and a
/// binding without one refuses them with no write at all.
pub struct MemoryWorkflowBinding {
    adapter: MemoryAdapter,
    pool: SqlitePool,
    knowledge: Option<AdmittedCollection>,
    publisher: Option<Box<JournalPublisher>>,
}

impl MemoryWorkflowBinding {
    /// Bind to `pool`; `reflections_enabled` matches the adapter instance the
    /// host serves.
    #[must_use]
    pub fn new(pool: SqlitePool, reflections_enabled: bool) -> Self {
        Self {
            adapter: MemoryAdapter::new(reflections_enabled),
            pool,
            knowledge: None,
            publisher: None,
        }
    }

    /// Publish journaled file targets into the host's medium.
    ///
    /// The closure returns `Ok(())` only after every target's exact bytes are
    /// durable; a settlement is staged but never settled without it.
    #[must_use]
    pub fn with_publisher(
        mut self,
        publish: impl Fn(&[Value]) -> Result<(), SpiError> + Send + Sync + 'static,
    ) -> Self {
        self.publisher = Some(Box::new(publish));
        self
    }

    /// The file targets one staged intent still owes.
    async fn intent_targets(
        &self,
        transaction: &mut Tx<'_>,
        space: &SpaceId,
        receipt_id: &str,
    ) -> Result<Vec<Value>, SpiError> {
        SqliteMemoryRepository
            .journal_intent(transaction, space, receipt_id)
            .await
            .map_err(|error| {
                internal(format!(
                    "journal intent {receipt_id} could not be read: {error}"
                ))
            })?
            .map(|intent| intent.targets)
            .ok_or_else(|| internal(format!("receipt {receipt_id} has no journaled intent")))
    }

    /// The host publisher, or the typed refusal a settlement leg owes before it
    /// stages anything: a binding that cannot publish must not commit an intent.
    fn publisher(&self) -> Result<&JournalPublisher, SpiError> {
        self.publisher.as_deref().ok_or_else(|| {
            SpiError::InvalidInput(
                "this workflow binding has no publisher, so it cannot settle a journal intent"
                    .to_string(),
            )
        })
    }

    /// Revalidate every cited record on `transaction`: the same authorized,
    /// settled record at the revision the prompt page served, or the whole
    /// staging rolls back.
    ///
    /// The adapter's per-record read is the check itself — a citation that
    /// disappeared, became withheld, lost its grant or moved its revision never
    /// reaches the staged settlement.
    async fn revalidate_cited(
        &self,
        transaction: &mut Tx<'_>,
        principal: &Principal,
        space_id: &str,
        cited: &[CitedRecord],
    ) -> Result<(), SpiError> {
        for record in cited {
            let body = ready(
                self.adapter
                    .get_record(
                        transaction,
                        principal,
                        json!({ "space_id": space_id, "record_id": record.record_id }),
                    )
                    .await,
                "memory.records.get",
            )?;
            let revision = body
                .pointer("/record/revision")
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    internal("memory.records.get carries no record revision".to_string())
                })?;
            if revision != record.revision {
                return Err(SpiError::Settlement(
                    Reject::retryable(
                        SpokeRejectCode::RevisionConflict,
                        format!(
                            "cited record {} is at revision {revision}, the decision saw {}; \
                             retry the absorb",
                            record.record_id, record.revision
                        ),
                    )
                    .into_spoke(),
                ));
            }
        }
        Ok(())
    }

    /// Publish every staged intent's targets, then settle them together on one
    /// fresh caller transaction. Both intents settle or neither does.
    ///
    /// The staging commit already happened, so from here an unresolved
    /// publication or settlement leaves pending journal work: every failure is
    /// reported as an uncertain outcome naming the receipts, never a clean
    /// refusal the caller could retry as if nothing were durable.
    async fn settle_staged(
        &self,
        publisher: &JournalPublisher,
        space: &SpaceId,
        intents: &[(String, Vec<Value>)],
    ) -> Result<Option<Value>, SpiError> {
        let targets: Vec<Value> = intents
            .iter()
            .flat_map(|(_, owed)| owed.iter().cloned())
            .collect();
        let receipts: Vec<&str> = intents
            .iter()
            .map(|(receipt_id, _)| receipt_id.as_str())
            .collect();
        publisher(&targets)
            .map_err(|error| unresolved(&receipts, "publication", format!("{error}")))?;
        let mut transaction = self.pool.begin().await.map_err(|error| {
            unresolved(&receipts, "settlement transaction begin", error.to_string())
        })?;
        let mut settled = None;
        for (receipt_id, _) in intents {
            match self
                .adapter
                .settle_journal(&mut transaction, space, receipt_id)
                .await
            {
                SpokeResult::Ok(body) => settled = Some(body),
                SpokeResult::Reject(reject) => return Err(SpiError::Settlement(reject)),
            }
        }
        transaction
            .commit()
            .await
            .map_err(|error| uncertain_commit("journal settlement", error))?;
        Ok(settled)
    }

    /// Fill the pack's knowledge slot from `scope`, admitted to `caller` only.
    ///
    /// The collection is one caller's knowledge scope: every read needs that
    /// caller to match and to hold a live [`KNOWLEDGE_READ`] grant in the
    /// request's space, so another principal's memory grant never reaches it.
    #[must_use]
    pub fn with_knowledge_collection(
        mut self,
        caller: impl Into<String>,
        scope: KnowledgeScope,
    ) -> Self {
        self.knowledge = Some(AdmittedCollection {
            caller: caller.into(),
            scope,
        });
        self
    }

    /// Every live-admission candidate of `space_id`, read on `tx`.
    async fn space_grants(
        &self,
        tx: &mut Tx<'_>,
        space_id: &str,
    ) -> Result<Vec<MemoryGrant>, SpiError> {
        let space = SpaceId::new(space_id.to_string());
        let rows = admission::grants(&SqliteMemoryRepository, tx, &space)
            .await
            .map_err(|reject| SpiError::Settlement(reject.into_spoke()))?;
        admission::wire_grants(&rows).map_err(|reject| SpiError::Settlement(reject.into_spoke()))
    }

    /// The bound collection, admitted to `principal` on `tx`: the caller must
    /// be the one this binding serves and hold a live grant for every one of
    /// `capabilities` in `space_id`.
    ///
    /// Caller identity, live grants and collection are re-derived on every call,
    /// so a read after provider work runs the same admission and a revoked
    /// grant refuses it. A memory grant never substitutes for this, and a
    /// knowledge read never stands in for [`KNOWLEDGE_WRITE`].
    async fn admitted_collection(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        space_id: &str,
        capabilities: &[&str],
    ) -> Result<&str, SpiError> {
        let Some(admitted) = &self.knowledge else {
            return Err(not_authorized(
                "this workflow binding serves no knowledge collection".to_string(),
            ));
        };
        let caller = principal_id(principal);
        if caller != admitted.caller.as_str() {
            return Err(not_authorized(format!(
                "principal {caller} is not admitted to knowledge collection {}",
                admitted.scope.collection
            )));
        }
        let space = SpaceId::new(space_id.to_string());
        let grants = self.space_grants(tx, space_id).await?;
        for capability in capabilities {
            let requirement = GrantRequirement {
                capability,
                subject_id: None,
                context_ids: Vec::new(),
                reads_shared: false,
                subject_all: false,
            };
            if !admission::covered(&grants, caller, space.as_str(), Utc::now(), &requirement) {
                return Err(not_authorized(format!(
                    "principal {caller} holds no live {capability} grant in space {}",
                    space.as_str()
                )));
            }
        }
        Ok(admitted.scope.collection.as_str())
    }

    /// The subject's own admission, rechecked inside the write transaction: the
    /// snapshot read needs it, so a grant revoked while the reasoner ran refuses
    /// the write even when the verdict cites no memory record at all.
    async fn revalidate_subject(
        &self,
        transaction: &mut Tx<'_>,
        principal: &Principal,
        space_id: &str,
        subject_id: &str,
    ) -> Result<(), SpiError> {
        let space = SpaceId::new(space_id.to_string());
        let grants = self.space_grants(transaction, space_id).await?;
        let requirement = admission::selection_requirement(
            RECALL,
            subject_id,
            &mindcore_base::ir::PlacementSelection::SubjectAll,
        );
        let caller = principal_id(principal);
        if !admission::covered(&grants, caller, space.as_str(), Utc::now(), &requirement) {
            return Err(not_authorized(format!(
                "principal {caller} holds no live {RECALL} grant covering subject {subject_id}"
            )));
        }
        Ok(())
    }

    /// Fill the pack's knowledge slot from the admitted collection.
    async fn knowledge_slot(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: &MemoryRecallRequest,
    ) -> Result<Vec<KnowledgeEntry>, SpiError> {
        if self.knowledge.is_none() {
            return Ok(Vec::new());
        }
        let collection = self
            .admitted_collection(tx, principal, request.space_id.as_str(), &[KNOWLEDGE_READ])
            .await?;
        match list_entries_in(tx, collection).await {
            SpokeResult::Ok(entries) => Ok(entries),
            SpokeResult::Reject(reject) => Err(SpiError::Settlement(reject)),
        }
    }
}

#[async_trait]
impl WorkflowBinding for MemoryWorkflowBinding {
    async fn recall_snapshot(
        &self,
        principal: &Principal,
        request: &MemoryRecallRequest,
    ) -> Result<RecallSnapshot, SpiError> {
        let raw = serde_json::to_value(request)
            .map_err(|error| internal(format!("recall request could not be rendered: {error}")))?;
        let mut transaction =
            self.pool.begin().await.map_err(|error| {
                internal(format!("recall transaction could not begin: {error}"))
            })?;
        let stage = match self.adapter.recall(&mut transaction, principal, raw).await {
            SpokeResult::Ok(stage) => stage,
            SpokeResult::Reject(reject) => return Err(SpiError::Settlement(reject)),
        };
        let records = match stage {
            MemoryStage::Ready(body) => decode_records(body)?,
            other => {
                return Err(internal(format!(
                    "memory.recall returned an unserveable stage: {other:?}"
                )))
            }
        };
        let knowledge = self
            .knowledge_slot(&mut transaction, principal, request)
            .await?;
        transaction
            .commit()
            .await
            .map_err(|error| internal(format!("recall transaction could not commit: {error}")))?;
        Ok(RecallSnapshot { records, knowledge })
    }

    async fn capture(
        &self,
        principal: &Principal,
        request: &MemoryCaptureRequest,
    ) -> Result<CaptureOutcome, SpiError> {
        let mut transaction =
            self.pool.begin().await.map_err(|error| {
                internal(format!("capture transaction could not begin: {error}"))
            })?;
        let (body, replayed) = match self
            .adapter
            .capture_with_replay(&mut transaction, principal, render(request, "capture")?)
            .await
        {
            SpokeResult::Ok((MemoryStage::Ready(body), replayed)) => (body, replayed),
            SpokeResult::Ok((other, _)) => {
                return Err(internal(format!(
                    "memory.capture returned an unserveable stage: {other:?}"
                )))
            }
            SpokeResult::Reject(reject) => return Err(SpiError::Settlement(reject)),
        };
        let candidate_ids = candidate_ids(&body)?;
        let mut candidates = Vec::with_capacity(candidate_ids.len());
        for candidate_id in candidate_ids {
            // A fresh capture just inserted its candidate pending; a replay
            // reports the state its recorded run left, read under the receipt
            // that names it.
            let lifecycle = if replayed {
                replayed_lifecycle(&mut transaction, request.space_id.as_str(), &candidate_id)
                    .await?
            } else {
                CandidateLifecycle::Pending
            };
            candidates.push(CapturedCandidate {
                candidate_id,
                lifecycle,
            });
        }
        transaction
            .commit()
            .await
            .map_err(|error| internal(format!("capture transaction could not commit: {error}")))?;
        Ok(CaptureOutcome {
            candidates,
            replayed,
        })
    }

    async fn absorb_snapshot(
        &self,
        principal: &Principal,
        space_id: &str,
        candidate_id: &str,
        max_items: u64,
    ) -> Result<AbsorbSnapshot, SpiError> {
        let mut transaction = self.pool.begin().await.map_err(|error| {
            internal(format!("absorb read transaction could not begin: {error}"))
        })?;
        let body = ready(
            self.adapter
                .get_candidate(
                    &mut transaction,
                    principal,
                    json!({ "space_id": space_id, "candidate_id": candidate_id }),
                )
                .await,
            "memory.candidates.get",
        )?;
        let candidate: MemoryCandidate =
            serde_json::from_value(body.get("candidate").cloned().ok_or_else(|| {
                internal("memory.candidates.get carries no candidate".to_string())
            })?)
            .map_err(|error| {
                internal(format!(
                    "memory.candidates.get candidate does not decode: {error}"
                ))
            })?;
        // A consumed candidate owes no page: the workflow reports its recorded
        // outcome instead of deciding again.
        let MemoryCandidate::PendingCandidate(pending) = &candidate else {
            transaction.commit().await.map_err(|error| {
                internal(format!("absorb read transaction could not commit: {error}"))
            })?;
            return Ok(AbsorbSnapshot {
                candidate,
                records: Vec::new(),
            });
        };
        let mut request = json!({
            "space_id": space_id,
            "subject_id": &pending.subject_id,
            "selection": selection(&pending.capture_scope),
            "budget": { "max_items": max_items },
        });
        let text = pending.proposed.content.text.trim();
        if !text.is_empty() {
            request["query"] = json!(text);
        }
        let records = ready(
            self.adapter
                .recall(&mut transaction, principal, request)
                .await,
            "memory.recall",
        );
        let records = decode_records(records?)?;
        transaction.commit().await.map_err(|error| {
            internal(format!("absorb read transaction could not commit: {error}"))
        })?;
        Ok(AbsorbSnapshot { candidate, records })
    }

    async fn review_candidate(
        &self,
        principal: &Principal,
        request: &MemoryCandidatesReviewRequest,
        cited: &[CitedRecord],
    ) -> Result<Vec<String>, SpiError> {
        let publisher = self.publisher()?;
        let space = SpaceId::new(request.space_id.to_string());
        let mut transaction = self.pool.begin().await.map_err(|error| {
            internal(format!(
                "review staging transaction could not begin: {error}"
            ))
        })?;
        self.revalidate_cited(&mut transaction, principal, &request.space_id, cited)
            .await?;
        let receipt_id = journal_pending(
            self.adapter
                .review_candidate(&mut transaction, principal, render(request, "review")?)
                .await,
        )?;
        let targets = self
            .intent_targets(&mut transaction, &space, &receipt_id)
            .await?;
        transaction.commit().await.map_err(|error| {
            uncertain_commit(&format!("review staging for receipt {receipt_id}"), error)
        })?;
        let body = self
            .settle_staged(publisher, &space, &[(receipt_id, targets)])
            .await?
            .ok_or_else(|| internal("the settled review carries no receipt".to_string()))?;
        record_ids(&body)
    }

    async fn update_candidate(
        &self,
        principal: &Principal,
        space_id: &str,
        update: &MemoryRecordsUpdateRequest,
        dismiss: &MemoryCandidatesDismissRequest,
        cited: &[CitedRecord],
    ) -> Result<(), SpiError> {
        let publisher = self.publisher()?;
        let space = SpaceId::new(space_id.to_string());
        let mut transaction = self.pool.begin().await.map_err(|error| {
            internal(format!(
                "update staging transaction could not begin: {error}"
            ))
        })?;
        self.revalidate_cited(&mut transaction, principal, space_id, cited)
            .await?;
        let update_receipt = journal_pending(
            self.adapter
                .update_record(&mut transaction, principal, render(update, "update")?)
                .await,
        )?;
        let dismiss_receipt = journal_pending(
            self.adapter
                .dismiss_candidate(&mut transaction, principal, render(dismiss, "dismiss")?)
                .await,
        )?;
        let mut intents = Vec::with_capacity(2);
        for receipt_id in [&update_receipt, &dismiss_receipt] {
            let targets = self
                .intent_targets(&mut transaction, &space, receipt_id)
                .await?;
            intents.push((receipt_id.clone(), targets));
        }
        // Both effects are in this one staging transaction: a refusal above
        // rolls the transaction back with both of them.
        transaction.commit().await.map_err(|error| {
            uncertain_commit(
                &format!("update staging for receipts {update_receipt}, {dismiss_receipt}"),
                error,
            )
        })?;
        let _ = self.settle_staged(publisher, &space, &intents).await?;
        Ok(())
    }

    async fn recorded_update(
        &self,
        principal: &Principal,
        space_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<String>, SpiError> {
        let key = ReceiptKey {
            space: SpaceId::new(space_id.to_string()),
            requester: CallerId::new(principal_id(principal)),
            operation: "memory.records.update".to_string(),
            idempotency_key: idempotency_key.to_string(),
        };
        let mut transaction = self.pool.begin().await.map_err(|error| {
            internal(format!(
                "update receipt transaction could not begin: {error}"
            ))
        })?;
        let receipt = SqliteMemoryRepository
            .review_receipt(&mut transaction, &key)
            .await
            .map_err(|error| internal(format!("update receipt could not be read: {error}")))?;
        transaction.commit().await.map_err(|error| {
            internal(format!(
                "update receipt transaction could not commit: {error}"
            ))
        })?;
        // Retention: capture and update receipts are rows of one table under
        // one aged policy, and this crate has no per-op retention branch; the
        // capture receipt is minted first, so a replay is refused there before
        // an aged-out update receipt could be read here.
        let Some(ReceiptRead::Retained(receipt)) = receipt else {
            return Ok(None);
        };
        let ReceiptState::Committed { result, .. } = receipt.state else {
            return Ok(None);
        };
        Ok(result
            .get("record_id")
            .and_then(Value::as_str)
            .map(str::to_string))
    }

    async fn deliberate_snapshot(
        &self,
        principal: &Principal,
        space_id: &str,
        subject_id: &str,
        holder: &CognitionReference,
        max_items: u64,
    ) -> Result<DeliberateSnapshot, SpiError> {
        let mut transaction = self.pool.begin().await.map_err(|error| {
            internal(format!(
                "deliberation read transaction could not begin: {error}"
            ))
        })?;
        let records = ready(
            self.adapter
                .recall(
                    &mut transaction,
                    principal,
                    json!({
                        "space_id": space_id,
                        "subject_id": subject_id,
                        "selection": { "mode": "subject_all" },
                        "budget": { "max_items": max_items },
                    }),
                )
                .await,
            "memory.recall",
        );
        let records = decode_records(records?)?;
        let collection = self
            .admitted_collection(&mut transaction, principal, space_id, &[KNOWLEDGE_READ])
            .await?;
        let carrier = match read_entry(&mut *transaction, collection, holder.id.as_str()).await {
            SpokeResult::Ok(carrier) => carrier,
            SpokeResult::Reject(reject) => return Err(SpiError::Settlement(reject)),
        };
        carrier_holder(holder, &carrier)?;
        let carrier_revision = carrier.revision.ok_or_else(|| {
            internal(format!(
                "carrier {} holds no assigned revision",
                carrier.entry_id
            ))
        })?;
        let cognition = carrier_records(&carrier);
        transaction.commit().await.map_err(|error| {
            internal(format!(
                "deliberation read transaction could not commit: {error}"
            ))
        })?;
        Ok(DeliberateSnapshot {
            records,
            cognition,
            carrier_revision,
        })
    }

    async fn commit_cognition(
        &self,
        principal: &Principal,
        space_id: &str,
        subject_id: &str,
        holder: &CognitionReference,
        expected_carrier_revision: u64,
        record: Value,
        cited: &[CitedRecord],
    ) -> Result<u64, SpiError> {
        let mut transaction = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|error| {
                internal(format!(
                    "cognition settlement transaction could not begin: {error}"
                ))
            })?;
        let collection = self
            .admitted_collection(
                &mut transaction,
                principal,
                space_id,
                &[KNOWLEDGE_READ, KNOWLEDGE_WRITE],
            )
            .await?;
        let space = SpaceId::new(space_id.to_string());
        self.revalidate_subject(&mut transaction, principal, space_id, subject_id)
            .await?;
        self.adapter
            .withhold_if_unsettled(&mut transaction, &space, subject_id)
            .await
            .map_err(|reject| SpiError::Settlement(reject.into_spoke()))?;
        self.revalidate_cited(&mut transaction, principal, space_id, cited)
            .await?;
        let carrier = match read_entry(&mut *transaction, collection, holder.id.as_str()).await {
            SpokeResult::Ok(carrier) => carrier,
            SpokeResult::Reject(reject) => return Err(SpiError::Settlement(reject)),
        };
        carrier_holder(holder, &carrier)?;
        let current = carrier.revision.ok_or_else(|| {
            internal(format!(
                "carrier {} holds no assigned revision",
                carrier.entry_id
            ))
        })?;
        if current != expected_carrier_revision {
            return Err(SpiError::Settlement(
                Reject::retryable(
                    SpokeRejectCode::RevisionConflict,
                    format!(
                        "carrier {} moved to revision {current}, the verdict was derived from \
                         {expected_carrier_revision}; retry the deliberation",
                        carrier.entry_id
                    ),
                )
                .into_spoke(),
            ));
        }
        let next = append_cognition_record(&carrier, record).map_err(|error| {
            SpiError::InvalidInput(format!("cognition record could not be appended: {error}"))
        })?;
        let committed = match write_entry(&mut transaction, collection, next, Some(current)).await {
            SpokeResult::Ok(committed) => committed,
            SpokeResult::Reject(reject) => return Err(SpiError::Settlement(reject)),
        };
        transaction
            .commit()
            .await
            .map_err(|error| uncertain_commit("cognition settlement", error))?;
        let view = derive_mind_state(&committed).map_err(|error| {
            internal(format!(
                "committed carrier {} derives no MindState: {error}",
                committed.entry_id
            ))
        })?;
        Ok(view.carrier_revision)
    }
}

/// The ready body of one adapter operation, or its refusal.
fn ready(stage: SpokeResult<MemoryStage>, operation: &str) -> Result<Value, SpiError> {
    match stage {
        SpokeResult::Ok(MemoryStage::Ready(body)) => Ok(body),
        SpokeResult::Ok(other) => Err(internal(format!(
            "{operation} returned an unserveable stage: {other:?}"
        ))),
        SpokeResult::Reject(reject) => Err(SpiError::Settlement(reject)),
    }
}

/// The receipt id of one staged journaled intent, or its refusal.
fn journal_pending(stage: SpokeResult<MemoryStage>) -> Result<String, SpiError> {
    match stage {
        SpokeResult::Ok(MemoryStage::JournalPending { receipt_id, .. }) => Ok(receipt_id),
        SpokeResult::Ok(other) => Err(internal(format!(
            "expected a journaled stage, got {other:?}"
        ))),
        SpokeResult::Reject(reject) => Err(SpiError::Settlement(reject)),
    }
}

fn render<T: serde::Serialize>(request: &T, operation: &str) -> Result<Value, SpiError> {
    serde_json::to_value(request).map_err(|error| {
        internal(format!(
            "{operation} request could not be rendered: {error}"
        ))
    })
}

/// The candidate's own placement, read back as the recall selection it admits.
fn selection(scope: &PlacementScope) -> Value {
    match scope {
        PlacementScope::Shared => json!({ "mode": "shared_only" }),
        PlacementScope::Context(context_id) => json!({
            "mode": "selected_contexts",
            "context_ids": [context_id],
            "include_shared": true,
        }),
    }
}

fn candidate_ids(body: &Value) -> Result<Vec<String>, SpiError> {
    let ids = body
        .pointer("/result/candidate_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| internal("memory.capture carries no candidate_ids".to_string()))?;
    ids.iter()
        .map(|id| {
            id.as_str()
                .map(str::to_string)
                .ok_or_else(|| internal("a captured candidate id is not a string".to_string()))
        })
        .collect()
}

/// Whether one candidate the caller's own capture receipt holds is still
/// pending or was consumed.
///
/// The id comes from that receipt, so this read stays inside the capture's own
/// authority: it reports one lifecycle bit and never the candidate's content,
/// review or consumption, which keep their own grants.
async fn replayed_lifecycle(
    transaction: &mut Tx<'_>,
    space_id: &str,
    candidate_id: &str,
) -> Result<CandidateLifecycle, SpiError> {
    let space = SpaceId::new(space_id.to_string());
    let candidate = SqliteMemoryRepository
        .candidate(transaction, &space, candidate_id)
        .await
        .map_err(|error| {
            internal(format!(
                "candidate {candidate_id} could not be read: {error}"
            ))
        })?
        .ok_or_else(|| {
            internal(format!(
                "the capture receipt names absent candidate {candidate_id}"
            ))
        })?;
    Ok(match candidate.state {
        CandidateState::Pending { .. } => CandidateLifecycle::Pending,
        CandidateState::Consumed { .. } => CandidateLifecycle::Consumed,
    })
}

fn record_ids(body: &Value) -> Result<Vec<String>, SpiError> {
    let ids = body
        .pointer("/result/record_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| internal("the settled review carries no record_ids".to_string()))?;
    ids.iter()
        .map(|id| {
            id.as_str()
                .map(str::to_string)
                .ok_or_else(|| internal("a settled record id is not a string".to_string()))
        })
        .collect()
}

fn decode_records(body: Value) -> Result<Vec<RetainedRecord>, SpiError> {
    let Value::Object(mut body) = body else {
        return Err(internal(
            "memory.recall ready stage is not a response object".to_string(),
        ));
    };
    let records = body
        .remove("records")
        .ok_or_else(|| internal("memory.recall ready stage carries no records".to_string()))?;
    serde_json::from_value(records)
        .map_err(|error| internal(format!("memory.recall records do not decode: {error}")))
}

fn not_authorized(message: String) -> SpiError {
    SpiError::Settlement(Reject::not_authorized(message).into_spoke())
}

/// The holder must be this carrier entry in the Mindcore realm: the carrier
/// record invariant accepts nothing else.
fn carrier_holder(holder: &CognitionReference, carrier: &KnowledgeEntry) -> Result<(), SpiError> {
    if holder.kind == CognitionReferenceKind::KnowledgeEntry
        && holder.namespace.as_str() == CARRIER_NAMESPACE
        && holder.id.as_str() == carrier.entry_id.as_str()
    {
        return Ok(());
    }
    Err(SpiError::InvalidInput(format!(
        "holder {}:{}:{} is not the carrier knowledge_entry:{CARRIER_NAMESPACE}:{}",
        holder.kind,
        holder.namespace.as_str(),
        holder.id.as_str(),
        carrier.entry_id
    )))
}

fn internal(message: String) -> SpiError {
    let mut details = Map::new();
    details.insert("retry_class".to_string(), Value::String("retryable".into()));
    SpiError::Settlement(SpokeReject {
        code: SpokeRejectCode::InternalError,
        message,
        details: Some(details),
    })
}

/// A failure after the staging commit: the journal intent is durable and its
/// publication or settlement is unconfirmed, so the outcome is unknown. The
/// cause is preserved and the receipts name the recovery path.
fn unresolved(receipts: &[&str], stage: &str, cause: String) -> SpiError {
    SpiError::Settlement(
        Reject::uncertain(format!(
            "{stage} failed after receipts {} were staged: {cause}",
            receipts.join(", ")
        ))
        .into_spoke(),
    )
}

/// A write transaction whose commit failed left its outcome unknown: the row
/// may or may not be durable, so the caller must resolve it instead of retrying
/// on the assumption that nothing happened.
fn uncertain_commit(operation: &str, error: sqlx::Error) -> SpiError {
    SpiError::Settlement(
        Reject::uncertain(format!("{operation} committed an unknown outcome: {error}"))
            .into_spoke(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failed_write_commit_is_uncertain_never_retryable() {
        let error = uncertain_commit("cognition settlement", sqlx::Error::Protocol("boom".into()));
        let SpiError::Settlement(reject) = error else {
            panic!("a failed commit is a settlement refusal");
        };
        assert_eq!(reject.code, SpokeRejectCode::OutcomeUncertain);
        assert_eq!(
            reject
                .details
                .as_ref()
                .and_then(|details| details.get("retry_class"))
                .and_then(Value::as_str),
            Some("uncertain")
        );
    }

    #[test]
    fn an_unresolved_staging_keeps_its_receipts_and_cause() {
        let error = unresolved(
            &["rct_update", "rct_dismiss"],
            "publication",
            "invalid input: the medium is read-only".to_string(),
        );
        let SpiError::Settlement(reject) = error else {
            panic!("an unresolved staging is a settlement refusal");
        };
        assert_eq!(reject.code, SpokeRejectCode::OutcomeUncertain);
        assert_eq!(
            reject
                .details
                .as_ref()
                .and_then(|details| details.get("retry_class"))
                .and_then(Value::as_str),
            Some("uncertain")
        );
        assert!(reject.message.contains("rct_update, rct_dismiss"));
        assert!(reject.message.contains("the medium is read-only"));
    }
}
