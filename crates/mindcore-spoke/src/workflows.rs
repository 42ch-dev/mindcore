//! The workflow binding: `memory.recall` plus the admitted knowledge read on the
//! caller's own transaction, projected onto the SPI's DTO-only seam.
//!
//! The adapter never opens or commits a transaction across provider work: one
//! call reads and returns, so an embedder, reasoner or policy hook runs with no
//! transaction open and the workflow can revalidate afterwards.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::{json, Map, Value};
use spoke_operations::{
    knowledge_entry_visible_to_viewpoint, GrantRequirement, SpokeReject, SpokeRejectCode,
    SpokeResult,
};
use spoke_schemas::data::cognition::{CognitionReference, CognitionReferenceKind};
use spoke_schemas::data::memory_candidate::PlacementScope;
use spoke_schemas::{
    KnowledgeEntry, MemoryCandidate, MemoryCandidatesDismissRequest, MemoryCandidatesReviewRequest,
    MemoryCaptureRequest, MemoryGrant, MemoryRecallRequest, MemoryRecordsUpdateRequest, Principal,
    RetainedRecord,
};
use sqlx::SqlitePool;

use mindcore_base::boundary::CallerId;
use mindcore_base::cognition::{carrier_records, CARRIER_NAMESPACE};
use mindcore_base::ids::{SpaceId, SubjectId};
use mindcore_base::ir::{
    CandidateState, MemoryJournalRow, MemoryRepository, ReceiptKey, ReceiptRead, ReceiptState,
};
use mindcore_base::knowledge::KnowledgeScope;
use mindcore_spi::{
    page_fact, snapshot_fact, AbsorbSnapshot, CandidateLifecycle, CaptureOutcome,
    CapturedCandidate, CitedRecord, DeliberateSnapshot, EmbeddingIdentity, PageFact, PageVector,
    PageVectors, RecallSnapshot, RecordFill, SettledRecord, SettledRevision, SnapshotFact,
    SpiError, WorkflowBinding,
};
use mindcore_sqlite::{
    append_cognition, list_entries_in, read_carrier_header, read_entry, vectors,
    SqliteMemoryRepository,
};

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

/// The journal op of the durable absorb-decision marker. The row is applied,
/// carries no targets and settles nothing: it only tells a capture replay that
/// this capture's absorb run decided `Pending` for the ids it names.
const ABSORB_DECISION_OP: &str = "mindcore.absorb.decision";

/// The in-process workflow binding over one SQLite pool.
///
/// `knowledge_routes` maps each admitted principal id to the one collection its workflow calls
/// reach: a principal with no route reads no knowledge and never writes a carrier. `publisher` is
/// the host's file medium; the settlement legs require it before they stage anything, and a
/// binding without one refuses them with no write at all.
pub struct MemoryWorkflowBinding {
    adapter: MemoryAdapter,
    pool: SqlitePool,
    knowledge_routes: BTreeMap<String, KnowledgeScope>,
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
            knowledge_routes: BTreeMap::new(),
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

    /// The pending intent one staged receipt holds: the file targets it still
    /// owes and the result that names the records it wrote.
    async fn staged_intent(
        &self,
        transaction: &mut Tx<'_>,
        space: &SpaceId,
        receipt_id: &str,
    ) -> Result<MemoryJournalRow, SpiError> {
        SqliteMemoryRepository
            .journal_intent(transaction, space, receipt_id)
            .await
            .map_err(|error| {
                internal(format!(
                    "journal intent {receipt_id} could not be read: {error}"
                ))
            })?
            .ok_or_else(|| internal(format!("receipt {receipt_id} has no journaled intent")))
    }

    /// The records one staged intent's own result names, each at the revision
    /// its staging wrote.
    ///
    /// Read on the staging transaction that wrote those rows: it holds the only
    /// writable handle, so no later commit can move a revision between the write
    /// and this read. The result, not a publication path, owns record identity,
    /// so a projection-path change can neither alter a fresh outcome nor
    /// silently disable the warm.
    async fn staged_revisions(
        &self,
        transaction: &mut Tx<'_>,
        space: &SpaceId,
        intent: &MemoryJournalRow,
    ) -> Result<Vec<SettledRevision>, SpiError> {
        let mut settled = Vec::new();
        for record_id in result_record_ids(intent.result.as_ref()) {
            let Some(record) = SqliteMemoryRepository
                .record(transaction, space, record_id)
                .await
                .map_err(|error| {
                    internal(format!(
                        "staged record {record_id} could not be read: {error}"
                    ))
                })?
            else {
                continue;
            };
            let Some(revision) = record.revision.as_u64() else {
                return Err(internal(format!(
                    "staged record {record_id} carries no integer revision"
                )));
            };
            settled.push(SettledRevision {
                record_id: record_id.to_string(),
                revision,
            });
        }
        Ok(settled)
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
                SpokeResult::Reject(reject) => {
                    return Err(unresolved(
                        &receipts,
                        "settlement",
                        format!("{}: {}", reject.code, reject.message),
                    ))
                }
            }
        }
        transaction
            .commit()
            .await
            .map_err(|error| uncertain_commit("journal settlement", error))?;
        Ok(settled)
    }

    /// Bind the host's route map: each exact principal id to the collection its workflow calls
    /// reach.
    ///
    /// The map is the workflow collection allowlist, so a route need not name a collection that
    /// already holds rows. An empty key or destination collection is the host's input error and
    /// leaves the binding unrouted. Nothing else is checked here: a map cannot repeat a key, and
    /// `KnowledgeScope` is not a wire `Scope`, so no admission call applies.
    pub fn with_knowledge_routes(
        mut self,
        routes: BTreeMap<String, KnowledgeScope>,
    ) -> Result<Self, SpokeReject> {
        let invalid = |message: String| SpokeReject {
            code: SpokeRejectCode::InvalidInput,
            message,
            details: None,
        };
        for (principal, scope) in &routes {
            if principal.is_empty() {
                return Err(invalid(
                    "a knowledge route needs a non-empty principal id".to_string(),
                ));
            }
            if scope.collection.is_empty() {
                return Err(invalid(format!(
                    "the knowledge route for principal {principal} names an empty collection"
                )));
            }
        }
        self.knowledge_routes = routes;
        Ok(self)
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

    /// The collection route the host configured for `principal`, by its exact id.
    ///
    /// The one lookup both the carrier write and the recall slot use, so two calls for the same
    /// admitted principal can never reach different collections. Borrowed, never cloned: the
    /// route map is immutable for the binding's lifetime.
    fn knowledge_route(&self, principal: &Principal) -> Option<&KnowledgeScope> {
        self.knowledge_routes.get(principal_id(principal))
    }

    /// `principal`'s own live grant for every one of `capabilities` in `space_id`, on `tx`.
    ///
    /// Live grants are re-derived on every call, so a read after provider work runs the same
    /// admission and a revoked grant refuses it. A memory grant never substitutes for this, and a
    /// knowledge read never stands in for [`KNOWLEDGE_WRITE`].
    async fn require_knowledge_grants(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        space_id: &str,
        capabilities: &[&str],
    ) -> Result<(), SpiError> {
        let caller = principal_id(principal);
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
        Ok(())
    }

    /// The collection the carrier write serves: `principal` must have a route and hold a live
    /// grant for every one of `capabilities` in `space_id`.
    ///
    /// Resolved once, so the carrier read and its CAS append stay pinned to one collection. A
    /// missing route is the terminal refusal before any carrier access: it is as target-free as a
    /// missing grant, so a peer never learns whether another principal has a route.
    async fn admitted_collection(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        space_id: &str,
        capabilities: &[&str],
    ) -> Result<&str, SpiError> {
        let collection = self
            .knowledge_route(principal)
            .map(|route| route.collection.as_str())
            .ok_or_else(|| {
                not_authorized("principal has no workflow knowledge route".to_string())
            })?;
        self.require_knowledge_grants(tx, principal, space_id, capabilities)
            .await?;
        Ok(collection)
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

    /// Fill the pack's knowledge slot from `principal`'s own routed collection.
    ///
    /// The slot is independent of the memory page: a principal with no route, or without a live
    /// [`KNOWLEDGE_READ`] grant in the request's space, reads its records with an empty slot
    /// exactly as it would with no route map. A missing route never reveals whether another
    /// principal has one.
    ///
    /// The slot applies the same disclosure predicate as the served knowledge list. This binding
    /// carries no admitted viewpoint, so an entry a viewpoint-free read hides — an `owner`-private
    /// one — never reaches the pack, however large the recall's budget.
    ///
    /// Best effort: the slot never decides whether the recall is authorized. Storage and list
    /// failures are not authorization, and still fail the call.
    async fn knowledge_slot(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: &MemoryRecallRequest,
    ) -> Result<Vec<KnowledgeEntry>, SpiError> {
        let Some(collection) = self
            .knowledge_route(principal)
            .map(|route| route.collection.as_str())
        else {
            return Ok(Vec::new());
        };
        match self
            .require_knowledge_grants(tx, principal, request.space_id.as_str(), &[KNOWLEDGE_READ])
            .await
        {
            Ok(()) => {}
            Err(SpiError::Settlement(reject)) if reject.code == SpokeRejectCode::NotAuthorized => {
                return Ok(Vec::new())
            }
            Err(error) => return Err(error),
        }
        match list_entries_in(tx, collection).await {
            SpokeResult::Ok(entries) => Ok(entries
                .into_iter()
                .filter(|entry| knowledge_entry_visible_to_viewpoint(entry, None))
                .collect()),
            SpokeResult::Reject(reject) => Err(SpiError::Settlement(reject)),
        }
    }

    /// The authorized recall page of `request`, read through `memory.recall`
    /// on `transaction`: ACL, withholding and the SPOKE page budget are the
    /// adapter's own checks, so no storage read precedes them.
    async fn authorized_page(
        &self,
        transaction: &mut Tx<'_>,
        principal: &Principal,
        request: &MemoryRecallRequest,
    ) -> Result<Vec<RetainedRecord>, SpiError> {
        let raw = serde_json::to_value(request)
            .map_err(|error| internal(format!("recall request could not be rendered: {error}")))?;
        let stage = match self.adapter.recall(transaction, principal, raw).await {
            SpokeResult::Ok(stage) => stage,
            SpokeResult::Reject(reject) => return Err(SpiError::Settlement(reject)),
        };
        match stage {
            MemoryStage::Ready(body) => decode_records(body),
            other => Err(internal(format!(
                "memory.recall returned an unserveable stage: {other:?}"
            ))),
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
        let mut transaction =
            self.pool.begin().await.map_err(|error| {
                internal(format!("recall transaction could not begin: {error}"))
            })?;
        let records = self
            .authorized_page(&mut transaction, principal, request)
            .await?;
        let knowledge = self
            .knowledge_slot(&mut transaction, principal, request)
            .await?;
        transaction
            .commit()
            .await
            .map_err(|error| internal(format!("recall transaction could not commit: {error}")))?;
        Ok(RecallSnapshot { records, knowledge })
    }

    async fn recall_vectors(
        &self,
        principal: &Principal,
        request: &MemoryRecallRequest,
        identity: &EmbeddingIdentity,
        expected: &[PageFact],
    ) -> Result<PageVectors, SpiError> {
        let identity_hash = vector_identity(identity)?;
        let mut transaction = self.pool.begin().await.map_err(|error| {
            internal(format!("vector read transaction could not begin: {error}"))
        })?;
        // The same authorized read as the recall itself: a page that lost its
        // grant or became withheld refuses here, so no row is probed for it.
        let records = self
            .authorized_page(&mut transaction, principal, request)
            .await?;
        // The page the caller holds is the only one this read may serve: a
        // record that moved after the first read is refused before its new
        // revision is looked up, so a decode of the newer row can never
        // surface as a storage error instead of the conflict.
        if page_fact(&records) != expected {
            return Err(SpiError::Settlement(
                Reject::retryable(
                    SpokeRejectCode::RevisionConflict,
                    "the authorized page moved while the query was embedded; retry the recall",
                )
                .into_spoke(),
            ));
        }
        let page: Vec<(&str, u64)> = records
            .iter()
            .filter_map(|record| match record {
                RetainedRecord::LiveRecord(live) => {
                    Some((live.record_id.as_str(), live.revision.get()))
                }
                RetainedRecord::TombstonedRecord(_) => None,
            })
            .collect();
        let mut stored = match vectors::read_record_vectors(
            &mut *transaction,
            &identity_hash,
            &page,
            identity.dim,
        )
        .await
        {
            SpokeResult::Ok(vectors) => vectors.into_iter(),
            SpokeResult::Reject(reject) => return Err(SpiError::Settlement(reject)),
        };
        let rows = records
            .into_iter()
            .map(|record| {
                let vector = match &record {
                    RetainedRecord::LiveRecord(_) => {
                        stored.next().expect("one lookup per live record")
                    }
                    RetainedRecord::TombstonedRecord(_) => None,
                };
                PageVector { record, vector }
            })
            .collect();
        transaction.commit().await.map_err(|error| {
            internal(format!("vector read transaction could not commit: {error}"))
        })?;
        Ok(rows)
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
        let decision = if replayed {
            absorb_decision(
                &mut transaction,
                request.space_id.as_str(),
                request.subject_id.as_str(),
                &body,
                &candidate_ids,
            )
            .await?
        } else {
            None
        };
        let mut candidates = Vec::with_capacity(candidate_ids.len());
        for candidate_id in candidate_ids {
            // A fresh capture just inserted its candidate pending. A replay
            // reports the Pending decision its absorb run recorded, and only
            // otherwise the state that run left, read under the receipt that
            // names it.
            let lifecycle = if replayed {
                if matches!(&decision, Some(pending) if pending.contains(&candidate_id)) {
                    CandidateLifecycle::PendingDecision
                } else {
                    replayed_lifecycle(&mut transaction, request.space_id.as_str(), &candidate_id)
                        .await?
                }
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

    async fn record_absorb_pending(
        &self,
        principal: &Principal,
        request: &MemoryCaptureRequest,
        pending_candidate_ids: &[&str],
    ) -> Result<(), SpiError> {
        if pending_candidate_ids.is_empty() {
            return Ok(());
        }
        let op = MemoryAdapter::op(
            request.space_id.as_str(),
            principal_id(principal),
            "memory.capture",
            Some(request.idempotency_key.as_str()),
            &render(request, "capture")?,
        )
        .map_err(|reject| SpiError::Settlement(reject.into_spoke()))?;
        let space = SpaceId::new(request.space_id.to_string());
        // The capture transaction has already committed: take the write
        // reservation, verify that exact receipt and insert the marker, with
        // no provider call or settlement inside this short transaction.
        let mut transaction = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|error| {
                internal(format!(
                    "absorb decision transaction could not begin: {error}"
                ))
            })?;
        let receipt = admission::probe(&SqliteMemoryRepository, &mut transaction, &op)
            .await
            .map_err(|reject| SpiError::Settlement(reject.into_spoke()))?
            .ok_or_else(|| internal("no committed capture receipt for this absorb".to_string()))?;
        let ReceiptState::Committed { result, .. } = &receipt.state else {
            return Err(internal(format!(
                "capture receipt {} is not committed",
                receipt.receipt_id
            )));
        };
        let captured = result
            .get("candidate_ids")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                internal(format!(
                    "capture receipt {} carries no candidate_ids",
                    receipt.receipt_id
                ))
            })?;
        // Capture receipt order, not the caller's order, owns the marker.
        let mut pending = Vec::with_capacity(pending_candidate_ids.len());
        for candidate in captured {
            let candidate = candidate.as_str().ok_or_else(|| {
                internal(format!(
                    "capture receipt {} holds a non-string candidate id",
                    receipt.receipt_id
                ))
            })?;
            if pending_candidate_ids.contains(&candidate) {
                pending.push(candidate.to_string());
            }
        }
        if pending.len() != pending_candidate_ids.len() {
            return Err(internal(format!(
                "absorb decision names a candidate capture receipt {} does not hold",
                receipt.receipt_id
            )));
        }
        let now = crate::policy::timestamp(op.now);
        let marker = MemoryJournalRow {
            seq: 0,
            receipt_id: receipt.receipt_id.clone(),
            space_id: space.clone(),
            subject_id: SubjectId::new(request.subject_id.to_string()),
            requester: CallerId::new(principal_id(principal)),
            operation: ABSORB_DECISION_OP.to_string(),
            idempotency_key: Some(request.idempotency_key.to_string()),
            state: "applied".to_string(),
            targets: Vec::new(),
            result: Some(json!({ "pending_candidate_ids": pending })),
            created_at: now.clone(),
            updated_at: now,
        };
        match SqliteMemoryRepository
            .journal_intent(&mut transaction, &space, &receipt.receipt_id)
            .await
            .map_err(|error| internal(format!("absorb decision could not be read: {error}")))?
        {
            // The journal's unique receipt id makes the write idempotent: only
            // a marker equal in its complete identity and decision is
            // authoritative; any other row under that receipt is corruption.
            Some(existing)
                if existing.subject_id != marker.subject_id
                    || existing.requester != marker.requester
                    || existing.operation != marker.operation
                    || existing.idempotency_key != marker.idempotency_key
                    || existing.state != marker.state
                    || !existing.targets.is_empty()
                    || existing.result.as_ref() != marker.result.as_ref() =>
            {
                return Err(internal(format!(
                    "capture receipt {} holds an inconsistent absorb decision",
                    receipt.receipt_id
                )))
            }
            Some(_) => {}
            None => SqliteMemoryRepository
                .insert_journal_intent(&mut transaction, &marker)
                .await
                .map_err(|error| {
                    internal(format!("absorb decision could not be written: {error}"))
                })?,
        }
        transaction.commit().await.map_err(|error| {
            internal(format!(
                "absorb decision transaction could not commit: {error}"
            ))
        })?;
        Ok(())
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
    ) -> Result<Vec<SettledRevision>, SpiError> {
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
        let intent = self
            .staged_intent(&mut transaction, &space, &receipt_id)
            .await?;
        let settled = self
            .staged_revisions(&mut transaction, &space, &intent)
            .await?;
        transaction.commit().await.map_err(|error| {
            uncertain_commit(&format!("review staging for receipt {receipt_id}"), error)
        })?;
        self.settle_staged(publisher, &space, &[(receipt_id, intent.targets)])
            .await?
            .ok_or_else(|| internal("the settled review carries no receipt".to_string()))?;
        Ok(settled)
    }

    async fn update_candidate(
        &self,
        principal: &Principal,
        space_id: &str,
        update: &MemoryRecordsUpdateRequest,
        dismiss: &MemoryCandidatesDismissRequest,
        cited: &[CitedRecord],
    ) -> Result<Vec<SettledRevision>, SpiError> {
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
        let mut settled = Vec::new();
        for receipt_id in [&update_receipt, &dismiss_receipt] {
            let intent = self
                .staged_intent(&mut transaction, &space, receipt_id)
                .await?;
            // The dismissal leg names no record in its result, so only the
            // update leg's own result yields a revision here.
            settled.extend(
                self.staged_revisions(&mut transaction, &space, &intent)
                    .await?,
            );
            intents.push((receipt_id.clone(), intent.targets));
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
        Ok(settled)
    }

    async fn settled_record(
        &self,
        principal: &Principal,
        space_id: &str,
        record_id: &str,
    ) -> Result<Option<SettledRecord>, SpiError> {
        let mut transaction = self.pool.begin().await.map_err(|error| {
            internal(format!(
                "settled record read transaction could not begin: {error}"
            ))
        })?;
        let body = ready(
            self.adapter
                .get_record(
                    &mut transaction,
                    principal,
                    json!({ "space_id": space_id, "record_id": record_id }),
                )
                .await,
            "memory.records.get",
        )?;
        transaction.commit().await.map_err(|error| {
            internal(format!(
                "settled record read transaction could not commit: {error}"
            ))
        })?;
        let record = decode_record(body)?;
        Ok(match record {
            RetainedRecord::LiveRecord(live) => Some(SettledRecord {
                record_id: live.record_id.as_str().to_string(),
                revision: live.revision.get(),
                text: live.content.text,
            }),
            RetainedRecord::TombstonedRecord(_) => None,
        })
    }

    async fn settle_recall(
        &self,
        principal: &Principal,
        request: &MemoryRecallRequest,
        identity: Option<&EmbeddingIdentity>,
        expected: &SnapshotFact,
        fills: &[RecordFill],
    ) -> Result<(), SpiError> {
        // A fill writes under the transaction that reauthorizes it. Take the
        // write reservation at begin so no other connection can commit
        // between the reauthorization read and the first fill: a deferred
        // begin would upgrade a snapshot another writer already invalidated
        // and fail the write after the release was warranted. A recall with
        // nothing to fill only reads, so it keeps the cheaper transaction.
        let identity_hash = identity
            .filter(|_| !fills.is_empty())
            .and_then(|identity| vector_identity(identity).ok());
        let mut transaction = match identity_hash {
            Some(_) => self.pool.begin_with("BEGIN IMMEDIATE").await,
            None => self.pool.begin().await,
        }
        .map_err(|error| {
            internal(format!(
                "recall release transaction could not begin: {error}"
            ))
        })?;
        let records = self
            .authorized_page(&mut transaction, principal, request)
            .await?;
        let knowledge = self
            .knowledge_slot(&mut transaction, principal, request)
            .await?;
        if &snapshot_fact(&records, &knowledge) != expected {
            return Err(SpiError::Settlement(
                Reject::retryable(
                    SpokeRejectCode::RevisionConflict,
                    "the authorized snapshot moved while provider work ran; retry the recall",
                )
                .into_spoke(),
            ));
        }
        // Only the page read above authorizes a fill; with the write
        // reservation held from begin, a cache-only failure below is a miss,
        // never an invalidated release.
        if let Some(identity_hash) = identity_hash {
            for fill in fills {
                let live = records.iter().any(|record| match record {
                    RetainedRecord::LiveRecord(live) => {
                        live.record_id.as_str() == fill.record.record_id
                            && live.revision.get() == fill.record.revision
                    }
                    RetainedRecord::TombstonedRecord(_) => false,
                });
                if !live {
                    continue;
                }
                let _ = vectors::upsert_record_vector(
                    &mut *transaction,
                    &fill.record.record_id,
                    &identity_hash,
                    fill.record.revision,
                    &fill.vector,
                )
                .await;
            }
        }
        // The reauthorization above is what the release rests on: a failed
        // cache write rolls back to misses and still releases.
        let _ = transaction.commit().await;
        Ok(())
    }

    async fn fill_record_vectors(
        &self,
        principal: &Principal,
        space_id: &str,
        identity: &EmbeddingIdentity,
        fills: &[RecordFill],
    ) -> Result<(), SpiError> {
        if fills.is_empty() {
            return Ok(());
        }
        let identity_hash = vector_identity(identity)?;
        let mut transaction = self.pool.begin().await.map_err(|error| {
            internal(format!("vector fill transaction could not begin: {error}"))
        })?;
        for fill in fills {
            // The fill rechecks in this transaction what the warm read saw:
            // work that outlived a newer revision or a withheld subject writes
            // nothing.
            let body = ready(
                self.adapter
                    .get_record(
                        &mut transaction,
                        principal,
                        json!({ "space_id": space_id, "record_id": fill.record.record_id }),
                    )
                    .await,
                "memory.records.get",
            )?;
            let current = match decode_record(body)? {
                RetainedRecord::LiveRecord(live) => live.revision.get(),
                RetainedRecord::TombstonedRecord(_) => continue,
            };
            if current != fill.record.revision {
                continue;
            }
            match vectors::upsert_record_vector(
                &mut *transaction,
                &fill.record.record_id,
                &identity_hash,
                fill.record.revision,
                &fill.vector,
            )
            .await
            {
                SpokeResult::Ok(()) => {}
                SpokeResult::Reject(reject) => return Err(SpiError::Settlement(reject)),
            }
        }
        transaction.commit().await.map_err(|error| {
            internal(format!("vector fill transaction could not commit: {error}"))
        })?;
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
        let carrier = match read_entry(&mut transaction, collection, holder.id.as_str()).await {
            SpokeResult::Ok(carrier) => carrier,
            SpokeResult::Reject(reject) => return Err(SpiError::Settlement(reject)),
        };
        carrier_holder(holder, carrier.entry_id.as_str())?;
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
        let header =
            match read_carrier_header(&mut *transaction, collection, holder.id.as_str()).await {
                SpokeResult::Ok(header) => header,
                SpokeResult::Reject(reject) => return Err(SpiError::Settlement(reject)),
            };
        carrier_holder(holder, holder.id.as_str())?;
        let current = header.revision;
        if current != expected_carrier_revision {
            return Err(SpiError::Settlement(
                Reject::retryable(
                    SpokeRejectCode::RevisionConflict,
                    format!(
                        "carrier {} moved to revision {current}, the verdict was derived from \
                         {expected_carrier_revision}; retry the deliberation",
                        holder.id.as_str()
                    ),
                )
                .into_spoke(),
            ));
        }
        // The append validates the record, inserts its one row and advances the head and
        // the carrier revision inside this transaction; the workflow's grant, withholding
        // and citation rechecks above already ran against the same snapshot.
        let assigned = match append_cognition(
            &mut transaction,
            collection,
            holder.id.as_str(),
            current,
            &record,
        )
        .await
        {
            SpokeResult::Ok(assigned) => assigned,
            SpokeResult::Reject(reject) if reject.code == SpokeRejectCode::InvalidInput => {
                return Err(SpiError::InvalidInput(format!(
                    "cognition record could not be appended: {}",
                    reject.message
                )));
            }
            SpokeResult::Reject(reject) => return Err(SpiError::Settlement(reject)),
        };
        transaction
            .commit()
            .await
            .map_err(|error| uncertain_commit("cognition settlement", error))?;
        Ok(assigned)
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

/// The record ids one staged result names: a review lists them, an update
/// names its single target. A candidate-only result names none.
fn result_record_ids(result: Option<&Value>) -> Vec<&str> {
    let Some(result) = result else {
        return Vec::new();
    };
    match result.get("record_ids").and_then(Value::as_array) {
        Some(ids) => ids.iter().filter_map(Value::as_str).collect(),
        None => result
            .get("record_id")
            .and_then(Value::as_str)
            .into_iter()
            .collect(),
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

/// The durable absorb decision a replayed capture receipt holds, when its
/// own absorb run recorded one: the candidates that run left pending, in
/// receipt order.
///
/// Read on the capture's admitted transaction, keyed by the receipt id the
/// body carries. A row under that receipt id that is not this marker, names
/// another space than the admitted capture, or names anything but a nonempty
/// receipt-ordered part of what the receipt holds, is storage corruption:
/// never permission to project the candidate's current state.
async fn absorb_decision(
    transaction: &mut Tx<'_>,
    space_id: &str,
    subject_id: &str,
    body: &Value,
    capture_candidates: &[String],
) -> Result<Option<Vec<String>>, SpiError> {
    let receipt_id = body
        .get("receipt_id")
        .and_then(Value::as_str)
        .ok_or_else(|| internal("a replayed capture carries no receipt_id".to_string()))?;
    let space = SpaceId::new(space_id.to_string());
    // The receipt id is globally unique, so the row under it is read whole and
    // its space validated below: filtering by space first would hide a
    // corrupted marker as one that was never written.
    let Some(marker) = SqliteMemoryRepository
        .journal_by_receipt(transaction, receipt_id)
        .await
        .map_err(|error| {
            internal(format!(
                "absorb decision {receipt_id} could not be read: {error}"
            ))
        })?
    else {
        return Ok(None);
    };
    let requester = body.get("requester_principal_id").and_then(Value::as_str);
    let key = body.get("idempotency_key").and_then(Value::as_str);
    // The payload is exactly the receipt-ordered pending list: a right list
    // beside any other member is as malformed as a wrong one, so replay
    // accepts no shape the repeat write refuses.
    let pending = marker
        .result
        .as_ref()
        .and_then(Value::as_object)
        .filter(|result| result.len() == 1)
        .and_then(|result| result.get("pending_candidate_ids"))
        .and_then(Value::as_array)
        .and_then(|ids| {
            ids.iter()
                .map(|id| id.as_str().map(str::to_string))
                .collect::<Option<Vec<_>>>()
        });
    if marker.operation != ABSORB_DECISION_OP
        || marker.state != "applied"
        || !marker.targets.is_empty()
        || marker.space_id.as_str() != space.as_str()
        || marker.subject_id.as_str() != subject_id
        || Some(marker.requester.as_str()) != requester
        || marker.idempotency_key.as_deref() != key
    {
        return Err(internal(format!(
            "capture receipt {receipt_id} holds a malformed absorb decision"
        )));
    }
    let Some(pending) = pending else {
        return Err(internal(format!(
            "capture receipt {receipt_id} holds a malformed absorb decision"
        )));
    };
    // The decision is a nonempty subsequence of the capture receipt's own
    // list: order and uniqueness come from that list, so an empty, repeated or
    // reordered decision is corruption, never the recorded one.
    let mut held = capture_candidates.iter();
    if pending.is_empty()
        || !pending
            .iter()
            .all(|id| held.any(|candidate| candidate == id))
    {
        return Err(internal(format!(
            "absorb decision {receipt_id} is not a nonempty receipt-ordered part of the capture receipt"
        )));
    }
    Ok(Some(pending))
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

fn decode_record(body: Value) -> Result<RetainedRecord, SpiError> {
    let record = body
        .get("record")
        .cloned()
        .ok_or_else(|| internal("memory.records.get carries no record".to_string()))?;
    serde_json::from_value(record).map_err(|error| {
        internal(format!(
            "memory.records.get record does not decode: {error}"
        ))
    })
}

fn not_authorized(message: String) -> SpiError {
    SpiError::Settlement(Reject::not_authorized(message).into_spoke())
}

/// The storage partition of one serving embedder.
fn vector_identity(identity: &EmbeddingIdentity) -> Result<String, SpiError> {
    match vectors::identity_hash(
        &identity.provider_id,
        &identity.model_id,
        &identity.model_version,
        identity.dim,
    ) {
        SpokeResult::Ok(hash) => Ok(hash),
        SpokeResult::Reject(reject) => Err(SpiError::Settlement(reject)),
    }
}

/// The holder must be this carrier entry in the Mindcore realm: the carrier
/// record invariant accepts nothing else. The carrier row is addressed by its
/// own id, so the id check identifies the holder's row.
fn carrier_holder(holder: &CognitionReference, entry_id: &str) -> Result<(), SpiError> {
    if holder.kind == CognitionReferenceKind::KnowledgeEntry
        && holder.namespace.as_str() == CARRIER_NAMESPACE
        && holder.id.as_str() == entry_id
    {
        return Ok(());
    }
    Err(SpiError::InvalidInput(format!(
        "holder {}:{}:{} is not the carrier knowledge_entry:{CARRIER_NAMESPACE}:{}",
        holder.kind,
        holder.namespace.as_str(),
        holder.id.as_str(),
        entry_id
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

    /// Fresh settlement ids come from the staged result, never from a
    /// publication path: a review lists its records, an update names one.
    #[test]
    fn a_settlement_result_names_the_records_to_warm() {
        assert_eq!(
            result_record_ids(Some(&json!({
                "outcome": "promoted",
                "record_ids": ["rec_a", "rec_b"],
            }))),
            ["rec_a", "rec_b"]
        );
        assert_eq!(
            result_record_ids(Some(&json!({ "record_id": "rec_a", "revision": 4 }))),
            ["rec_a"]
        );
        assert!(result_record_ids(Some(&json!({ "outcome": "dismissed" }))).is_empty());
        assert!(result_record_ids(None).is_empty());
    }

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
