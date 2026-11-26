//! The workflow binding: `memory.recall` plus the admitted knowledge read on the
//! caller's own transaction, projected onto the SPI's DTO-only seam.
//!
//! The adapter never opens or commits a transaction across provider work: one
//! call reads and returns, so an embedder, reasoner or policy hook runs with no
//! transaction open and the workflow can revalidate afterwards.

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
use sqlx::{Connection, SqlitePool};

use mindcore_base::boundary::CallerId;
use mindcore_base::cognition::{carrier_records, CARRIER_NAMESPACE};
use mindcore_base::ids::{SpaceId, SubjectId};
use mindcore_base::ir::{
    CandidateState, MemoryGrantRow, MemoryJournalRow, MemoryRepository, ReceiptKey, ReceiptRead,
    ReceiptState,
};
use mindcore_spi::{
    page_fact, snapshot_fact, AbsorbSnapshot, CandidateLifecycle, CaptureOutcome,
    CapturedCandidate, CitedRecord, DeliberateSnapshot, EmbeddingIdentity, PageFact, PageVector,
    PageVectors, RecallSnapshot, RecordFill, SettledRecord, SettledRevision, SnapshotFact,
    SpiError, WorkflowBinding,
};
use mindcore_sqlite::{
    append_cognition, list_entries_in, read_carrier_header, read_entry, vectors,
    SqliteMemoryRepository, SqliteRepositoryError,
};

use crate::admission;
use crate::reject::Reject;
use crate::{principal_id, MemoryAdapter, MemoryStage, Tx, KNOWLEDGE_READ, KNOWLEDGE_WRITE, RECALL};

/// Publishes one intent's file targets and confirms them: `Ok(())` only when
/// every target's exact bytes landed at its path.
type JournalPublisher = dyn Fn(&[Value]) -> Result<(), SpiError> + Send + Sync;

/// The journal op of the durable absorb-decision marker. The row is applied,
/// carries no targets and settles nothing: it only tells a capture replay that
/// this capture's absorb run decided `Pending` for the ids it names.
const ABSORB_DECISION_OP: &str = "mindcore.absorb.decision";

/// The carrier location of one admitted Mind: the registry row the `space_id` names.
pub(crate) struct AdmittedCarrier {
    /// The knowledge collection the Mind's carrier lives in.
    pub(crate) collection: String,
    /// The entry id the Mind's carrier row names.
    pub(crate) carrier_entry_id: String,
}

/// Mind admission for the host-side `deliberate` handler: the caller's grant rows and the
/// registry row read in one transaction, so a revocation cannot land between them, and every
/// liveness decision runs the one grant rule (`live_grant`: unrevoked, unexpired, an
/// unparseable expiry fails closed). The deliberate handler needs a space-wide `knowledge_read`
/// grant of its own before it touches a carrier, so a caller admitted through another capability
/// only is refused here, before any carrier is read. Returns the row-named carrier entry id, or
/// the fixed admission refusal — byte-equal whether the Mind exists, the grant is missing,
/// revoked or expired — so an unadmitted caller cannot tell the cases apart.
pub async fn admitted_carrier_entry(
    pool: &SqlitePool,
    principal_id: &str,
    space_id: &str,
) -> Result<String, SpokeReject> {
    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| Reject::internal(format!("the mind admission could not open: {e}")).into_spoke())?;
    let mut tx = conn
        .begin()
        .await
        .map_err(|e| Reject::internal(format!("the mind admission could not begin: {e}")).into_spoke())?;
    let space = SpaceId::new(space_id.to_string());
    let rows = caller_grants(&mut tx, principal_id, &space).await?;
    let now = Utc::now();
    let admitted = rows.iter().any(|row| {
        row.principal_id.as_str() == principal_id
            && row.subject_id.is_none()
            && row.capabilities.iter().any(|cap| cap == KNOWLEDGE_READ)
            && admission::live_grant(row, now)
    });
    if !admitted {
        return Err(Reject::admission_refusal().into_spoke());
    }
    let carrier = registry_carrier(&mut tx, space_id).await?;
    let _ = tx.rollback().await;
    carrier
        .map(|carrier| carrier.carrier_entry_id)
        .ok_or_else(|| Reject::admission_refusal().into_spoke())
}

/// Admission of one Mind-level read, in the order the plan fixes: a live grant of the caller's in
/// the named space — any capability admits, and an unknown, an unadmitted and a frozen-ungranted
/// Mind answer the one target-free refusal — then the space row, then the registry row the carrier
/// comes from, and only then the space-wide `knowledge_read` coverage.
///
/// The two refusals are deliberately different answers. Admission answers the fixed refusal with
/// no details, because it must not say whether the named Mind exists. Coverage answers
/// [`admission::refuse_space_coverage`], naming the capability the caller lacks: it is reachable
/// only by a caller already known to hold a live grant in this Mind, so it discloses nothing about
/// a Mind the caller was never admitted to.
///
/// Every read is confined to the caller's own grant rows. Another principal's malformed row never
/// reaches a decision, and a decode failure on the caller's own row answers the same target-free
/// refusal as a Mind that does not exist.
pub(crate) async fn admitted_carrier(
    tx: &mut Tx<'_>,
    principal_id: &str,
    space_id: &str,
) -> Result<AdmittedCarrier, SpokeReject> {
    let repository = SqliteMemoryRepository;
    let space = SpaceId::new(space_id.to_string());
    let rows = caller_grants(tx, principal_id, &space).await?;
    let now = Utc::now();
    if !rows
        .iter()
        .any(|row| row.principal_id.as_str() == principal_id && admission::live_grant(row, now))
    {
        return Err(Reject::admission_refusal().into_spoke());
    }
    // The space row is the plan's second step, and it is what `admit_mind` itself checks: a grant
    // outlives its space only if the space row is deleted underneath it, which the store never
    // does — a Mind's space row, registry row and carrier entry are written in one transaction.
    // Keeping the read makes this path refuse exactly the Minds `admit_mind` refuses.
    let space_row = repository.space(tx, &space).await.map_err(|error| {
        Reject::internal(format!("the mind admission could not be read: {error}")).into_spoke()
    })?;
    if space_row.is_none() {
        return Err(Reject::admission_refusal().into_spoke());
    }
    let carrier = registry_carrier(tx, space_id)
        .await?
        .ok_or_else(|| Reject::admission_refusal().into_spoke())?;
    // Coverage over the same caller-only rows: another principal's grants are neither needed for
    // a space-wide requirement nor allowed to reach this decision.
    let grants = admission::wire_grants(&rows).map_err(|reject| reject.into_spoke())?;
    let requirement = GrantRequirement {
        capability: KNOWLEDGE_READ,
        subject_id: None,
        context_ids: Vec::new(),
        reads_shared: false,
        subject_all: false,
    };
    if !admission::covered(&grants, principal_id, space_id, now, &requirement) {
        return Err(
            admission::refuse_space_coverage(principal_id, space_id, KNOWLEDGE_READ).into_spoke(),
        );
    }
    Ok(carrier)
}

/// The caller's own grant rows in `space`, decoded on the caller's transaction.
///
/// Only the caller's rows, and only what a liveness decision needs. Another principal's malformed
/// row must not reach it: an indistinguishable refusal is the answer whether the decode failure
/// lives on the caller's own row or the space is not a Mind at all, and a row that cannot decode
/// admits nothing.
async fn caller_grants(
    tx: &mut Tx<'_>,
    principal_id: &str,
    space: &SpaceId,
) -> Result<Vec<MemoryGrantRow>, SpokeReject> {
    SqliteMemoryRepository
        .list_grants(tx, space, Some(principal_id))
        .await
        .map_err(|error| match error {
            SqliteRepositoryError::InvalidRow(_) => Reject::admission_refusal().into_spoke(),
            other => Reject::internal(format!("the mind admission could not be read: {other}"))
                .into_spoke(),
        })
}

/// The carrier location the registry row of `space_id` names, read on the caller's transaction.
async fn registry_carrier(
    tx: &mut Tx<'_>,
    space_id: &str,
) -> Result<Option<AdmittedCarrier>, SpokeReject> {
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT knowledge_collection, carrier_entry_id FROM minds WHERE space_id = ?",
    )
    .bind(space_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|e| {
        Reject::internal(format!("the mind admission could not be read: {e}")).into_spoke()
    })?;
    Ok(row.map(|(collection, carrier_entry_id)| AdmittedCarrier {
        collection,
        carrier_entry_id,
    }))
}

/// The in-process workflow binding over one SQLite pool.
///
/// Every carrier access resolves through the Mind the request names: a facade
/// handle's bound carrier, or the registry row of the request's `space_id` read
/// on the caller's transaction. `publisher` is
/// the host's file medium; the settlement legs require it before they stage anything, and a
/// binding without one refuses them with no write at all.
pub struct MemoryWorkflowBinding {
    adapter: MemoryAdapter,
    pool: SqlitePool,
    publisher: Option<Box<JournalPublisher>>,
    /// The one carrier a facade handle binds: `(space_id, collection, carrier_entry_id)`.
    /// The served surface binds none; its carrier access resolves the registry
    /// row of the request's `space_id` instead.
    carrier_binding: Option<(String, String, String)>,
}

impl MemoryWorkflowBinding {
    /// Bind to `pool`; `reflections` matches the adapter instance the host
    /// serves.
    #[must_use]
    pub fn new(pool: SqlitePool, reflections: bool) -> Self {
        Self {
            adapter: MemoryAdapter::new(reflections),
            pool,
            publisher: None,
            carrier_binding: None,
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

    /// Bind the carrier its row states, for the facade handle that resolved the
    /// Mind's registry row at open: `with_carrier_collection(space_id,
    /// collection, carrier_entry_id)` pins one space's deliberation to it. The
    /// served surface binds none; its carrier access reads the registry row on
    /// the caller's transaction.
    #[must_use]
    pub fn with_carrier_collection(
        mut self,
        space_id: &str,
        collection: &str,
        carrier_entry_id: &str,
    ) -> Self {
        self.carrier_binding = Some((
            space_id.to_string(),
            collection.to_string(),
            carrier_entry_id.to_string(),
        ));
        self
    }

    /// The collection one carrier access serves: a bound Mind answers its own
    /// collection first, and otherwise the registry row of `space_id`, read on
    /// the caller's transaction. A `space_id` with no row answers `None`, so a
    /// carrier write refuses before any entry is read, exactly as an
    /// unadmitted Mind answers `memory.spaces.list` nothing.
    async fn carrier_collection(
        &self,
        tx: &mut Tx<'_>,
        space_id: &str,
    ) -> Result<Option<String>, SpiError> {
        if let Some((bound_space, collection, _)) = &self.carrier_binding {
            if bound_space == space_id {
                return Ok(Some(collection.clone()));
            }
        }
        sqlx::query_scalar::<_, String>(
            "SELECT knowledge_collection FROM minds WHERE space_id = ?",
        )
        .bind(space_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| internal(format!("the mind registry could not be read: {error}")))
    }

    /// A bound carrier is the only carrier this binding serves in its space: a
    /// holder naming any other entry is refused before any carrier read or
    /// append, so the binding's identity is enforced and not merely resolved.
    fn require_bound_carrier(
        &self,
        space_id: &str,
        holder: &CognitionReference,
    ) -> Result<(), SpiError> {
        if let Some((bound_space, _, carrier_entry_id)) = &self.carrier_binding {
            if bound_space == space_id && carrier_entry_id != holder.id.as_str() {
                return Err(SpiError::InvalidInput(format!(
                    "holder {} does not name the carrier this binding serves",
                    holder.id.as_str()
                )));
            }
        }
        Ok(())
    }

    /// The collection the carrier write serves: `principal` must have a route —
    /// or the binding a bound carrier — and hold a live grant for every one of
    /// `capabilities` in `space_id`.
    ///
    /// Resolved once, so the carrier read and its CAS append stay pinned to one collection. A
    /// space with no registry row is the admission refusal before any carrier access: it is as
    /// target-free as a missing grant, so a peer never learns whether the named Mind exists.
    async fn admitted_collection(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        space_id: &str,
        capabilities: &[&str],
    ) -> Result<String, SpiError> {
        let collection = self
            .carrier_collection(tx, space_id)
            .await?
            .ok_or_else(|| {
                SpiError::Settlement(Reject::admission_refusal().into_spoke())
            })?;
        self.adapter
            .require_space_grants(tx, principal, space_id, capabilities)
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
    /// Fill the pack's knowledge slot from the collection the request's `space_id` names.
    ///
    /// The slot is independent of the memory page: a space with no registry row — a Mind the
    /// caller never listed, or none at all — or a caller without a live [`KNOWLEDGE_READ`] grant
    /// in the request's space reads its records with an empty slot. A missing row never reveals
    /// whether the named Mind exists.
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
            .carrier_collection(tx, request.space_id.as_str())
            .await?
        else {
            return Ok(Vec::new());
        };
        match self
            .adapter
            .require_space_grants(tx, principal, request.space_id.as_str(), &[KNOWLEDGE_READ])
            .await
        {
            Ok(()) => {}
            Err(SpiError::Settlement(reject)) if reject.code == SpokeRejectCode::NotAuthorized => {
                return Ok(Vec::new())
            }
            Err(error) => return Err(error),
        }
        match list_entries_in(tx, &collection).await {
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
        let records = self
            .authorized_page(&mut transaction, principal, request)
            .await?;
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
            let lifecycle = if replayed {
                if matches!(
                    &decision,
                    Some(pending)
                        if pending
                            .iter()
                            .any(|id| id.as_str() == Some(candidate_id.as_str()))
                ) {
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
            settled.extend(
                self.staged_revisions(&mut transaction, &space, &intent)
                    .await?,
            );
            intents.push((receipt_id.clone(), intent.targets));
        }
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
        subject_id: Option<&str>,
        holder: &CognitionReference,
        max_items: u64,
    ) -> Result<DeliberateSnapshot, SpiError> {
        self.require_bound_carrier(space_id, holder)?;
        let mut transaction = self.pool.begin().await.map_err(|error| {
            internal(format!(
                "deliberation read transaction could not begin: {error}"
            ))
        })?;
        // With no subject there is no page to read: the space-level revalidation
        // and unsettled-intent check stand in place of the subject page's own
        // admission and withhold, and the snapshot serves an empty memory page.
        let records = match subject_id {
            Some(subject_id) => {
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
                decode_records(records?)?
            }
            None => {
                self.adapter
                    .revalidate_space(&mut transaction, principal, space_id)
                    .await?;
                let space = SpaceId::new(space_id.to_string());
                self.adapter
                    .withhold_space_if_unsettled(&mut transaction, &space)
                    .await
                    .map_err(|reject| SpiError::Settlement(reject.into_spoke()))?;
                Vec::new()
            }
        };
        let collection = self
            .admitted_collection(&mut transaction, principal, space_id, &[KNOWLEDGE_READ])
            .await?;
        let carrier = match read_entry(&mut transaction, &collection, holder.id.as_str()).await {
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
        subject_id: Option<&str>,
        holder: &CognitionReference,
        expected_carrier_revision: u64,
        record: Value,
        cited: &[CitedRecord],
        deadline: std::time::Instant,
    ) -> Result<u64, SpiError> {
        self.require_bound_carrier(space_id, holder)?;
        // The pool holds one connection in production, so an acquire can wait
        // for another statement indefinitely: the request deadline bounds that
        // wait, and a connection in hand is re-checked before the begin. Both
        // refusals start no write; a transaction already begun keeps its
        // outcome, and the write-lock wait inside the begin is outside this
        // bound.
        let mut connection =
            tokio::time::timeout_at(tokio::time::Instant::from(deadline), self.pool.acquire())
                .await
                .map_err(|_| SpiError::DeadlineExceeded)?
                .map_err(|error| {
                    internal(format!(
                        "cognition settlement transaction could not begin: {error}"
                    ))
                })?;
        deadline_still_open(std::time::Instant::now(), deadline)?;
        let mut transaction = connection
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
        match subject_id {
            Some(subject_id) => {
                self.revalidate_subject(&mut transaction, principal, space_id, subject_id)
                    .await?;
                self.adapter
                    .withhold_if_unsettled(&mut transaction, &space, subject_id)
                    .await
                    .map_err(|reject| SpiError::Settlement(reject.into_spoke()))?;
            }
            None => {
                self.adapter
                    .revalidate_space(&mut transaction, principal, space_id)
                    .await?;
                self.adapter
                    .withhold_space_if_unsettled(&mut transaction, &space)
                    .await
                    .map_err(|reject| SpiError::Settlement(reject.into_spoke()))?;
            }
        }
        self.revalidate_cited(&mut transaction, principal, space_id, cited)
            .await?;
        let header =
            match read_carrier_header(&mut *transaction, &collection, holder.id.as_str()).await {
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
        let assigned = match append_cognition(
            &mut transaction,
            &collection,
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
) -> Result<Option<Vec<Value>>, SpiError> {
    let receipt_id = body
        .get("receipt_id")
        .and_then(Value::as_str)
        .ok_or_else(|| internal("a replayed capture carries no receipt_id".to_string()))?;
    let space = SpaceId::new(space_id.to_string());
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
    let pending = marker
        .result
        .and_then(|result| match result {
            Value::Object(mut result) if result.len() == 1 => {
                result.remove("pending_candidate_ids")
            }
            _ => None,
        })
        .and_then(|value| match value {
            Value::Array(ids) if ids.iter().all(Value::is_string) => Some(ids),
            _ => None,
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
    let mut held = capture_candidates.iter();
    if pending.is_empty()
        || !pending
            .iter()
            .all(|id| held.any(|candidate| Some(candidate.as_str()) == id.as_str()))
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

/// An acquire can complete with the deadline already past: the pool hands out
/// a connection that was idle when the wait began. The begin that follows is
/// where the write starts, so the expiry is rechecked here rather than trusted
/// to the acquire's own bound. Pure in `now` so the refusal is testable without
/// a pool.
fn deadline_still_open(
    now: std::time::Instant,
    deadline: std::time::Instant,
) -> Result<(), SpiError> {
    if now >= deadline {
        return Err(SpiError::DeadlineExceeded);
    }
    Ok(())
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
    fn an_acquired_connection_is_refused_once_its_deadline_elapsed() {
        let start = std::time::Instant::now();
        let deadline = start + std::time::Duration::from_secs(1);
        assert!(
            deadline_still_open(start, deadline).is_ok(),
            "time left lets the begin start"
        );
        let SpiError::DeadlineExceeded = deadline_still_open(deadline, deadline)
            .expect_err("the deadline instant itself is closed")
        else {
            panic!("an elapsed deadline refuses with a deadline refusal");
        };
        let SpiError::DeadlineExceeded =
            deadline_still_open(deadline + std::time::Duration::from_secs(1), deadline)
                .expect_err("a passed deadline refuses")
        else {
            panic!("a passed deadline refuses with a deadline refusal");
        };
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
