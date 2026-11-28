//! The MIND workflow engine and its one DTO-only storage seam.
//!
//! [`WorkflowBinding`] keeps storage below this crate: it runs `memory.recall`,
//! the scoped knowledge read and the capture/review/update operations on the
//! caller's own transactions and returns DTOs. [`SpiMindWorkflows`] only
//! ranks, validates and budgets that snapshot, so embedder, reasoner and
//! policy calls never run inside a transaction.

use std::collections::{HashMap, HashSet};
use std::future::{poll_fn, Future};
use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::task::Poll;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use serde_json::{json, Map, Value};
use spoke_operations::{
    jcs_sha256_hex, validate_cognition_record, SpokeReject, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::data::cognition::CognitionReference;
use spoke_schemas::data::memory_candidate::{ConsumedCandidate, Outcome, PendingCandidate};
use spoke_schemas::{
    CognitionRecord, KnowledgeEntry, MemoryCandidate, MemoryCandidatesDismissRequest,
    MemoryCandidatesReviewRequest, MemoryCaptureRequest, MemoryRecallRequest,
    MemoryRecordsUpdateRequest, Principal, RetainedRecord,
};

use crate::judgment::deliberate_judgment;
use crate::{
    AbsorbCandidateOutcome, CognitiveTaskKind, ContextPack, DefaultPolicy, DeliberateOutcome,
    DeliberateRequest, DomainLens, Embedder, EmbeddingIdentity, Evidence, LensOutput,
    MindCapabilities, MindWorkflows, PolicyDecision, PolicyHook, ProposalDraft, Reasoner,
    Recommendation, SettlementDraft, SpiError, StructuredReasonRequest,
};

/// BM25 term-frequency saturation and length normalisation (the standard
/// defaults; the scoring runs in memory over the bounded recall page only).
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;

/// Fixed skeleton of the rerank leg; a lens contributes nothing here because
/// reranking adds no domain judgement to the pack.
const RERANK_PROMPT: &str = "Rank the supplied memory records by how directly each \
     one serves the query. Return every record_id exactly once, most relevant \
     first, in record_ids. Do not add, remove or repeat ids.";

/// One authorized read snapshot: the single SPOKE recall page plus the
/// caller's admitted knowledge entries, read on one transaction.
#[derive(Debug, Clone)]
pub struct RecallSnapshot {
    /// The bounded recall page in adapter order.
    pub records: Vec<RetainedRecord>,
    /// Admitted knowledge entries in stored order; empty when the binding
    /// serves no knowledge collection for this caller.
    pub knowledge: Vec<KnowledgeEntry>,
}

/// One record of an authorized page: its id and the revision the page served.
///
/// Two reads served the same page only when every pair matches in page order.
pub type PageFact = (String, u64);

/// The facts a released recall must still serve: the page and the admitted
/// knowledge entries, each in read order.
pub type SnapshotFact = (Vec<PageFact>, Vec<(String, Option<u64>)>);

/// One authorized page row and the vector stored for it under one identity.
///
/// One entry per page row, so an adapter cannot misalign a vector with a record.
#[derive(Debug, Clone)]
pub struct PageVector {
    /// The page record this read served.
    pub record: RetainedRecord,
    /// The stored vector for a live record still served at this revision under
    /// this identity; `None` is a miss — no row, another revision, another
    /// dimension, a retired identity or a tombstone.
    pub vector: Option<Vec<f32>>,
}

/// One authorized page read taken for the vector cache: one entry per page row.
pub type PageVectors = Vec<PageVector>;

/// One record vector a recall or warm computed, waiting for the binding's cache
/// write.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordFill {
    /// The live record the provider embedded, at the revision it was read at.
    pub record: SettledRecord,
    /// The provider vector for that record's text under the serving identity.
    pub vector: Vec<f32>,
}

/// The DTO-only storage seam of the workflow engine.
///
/// One call is one transaction over the caller's scope: `memory.recall`
/// (ACL, withholding and the SPOKE budget applied by the adapter) and the
/// scoped knowledge read, or — for absorb — the existing capture, candidate
/// read, review and record-update operations. Nothing that leaves it is
/// storage-shaped, so the workflow can call a provider between two calls and
/// revalidate afterwards.
#[async_trait]
#[allow(clippy::too_many_arguments)]
pub trait WorkflowBinding: Send + Sync {
    /// Read the current authorized snapshot for `request`.
    async fn recall_snapshot(
        &self,
        principal: &Principal,
        request: &MemoryRecallRequest,
    ) -> Result<RecallSnapshot, SpiError>;

    /// Read the authorized page again and, for every record it still serves
    /// live at that revision, the vector stored under `identity`.
    ///
    /// This is the snapshot that selects vector rows after a query provider
    /// call: admission and withholding are rebuilt on the same transaction, so
    /// a page that lost its grant or became withheld is a refusal, never a
    /// cache hit. `expected` is the page the caller already holds: a read that
    /// would serve any other page is refused with `REVISION_CONFLICT` before a
    /// single row is probed, so a record that moved during provider work is
    /// never read at its new revision. An absent row, another revision,
    /// another dimension or a retired identity is `None`.
    async fn recall_vectors(
        &self,
        principal: &Principal,
        request: &MemoryRecallRequest,
        identity: &EmbeddingIdentity,
        expected: &[PageFact],
    ) -> Result<PageVectors, SpiError>;

    /// Run the existing capture path — admission, idempotency probe and the
    /// deterministic classifier — and return the candidates it holds.
    ///
    /// A replay returns the ids its recorded receipt holds and reports itself,
    /// so nothing it names was captured by this call.
    async fn capture(
        &self,
        principal: &Principal,
        request: &MemoryCaptureRequest,
    ) -> Result<CaptureOutcome, SpiError>;

    /// Persist the durable decision marker for one absorb run that left
    /// `pending_candidate_ids` pending, then return.
    ///
    /// The marker is keyed by the validated committed capture receipt of
    /// `request`: its space, subject, requester and idempotency key, with no
    /// targets and the pending ids in capture receipt order. An equal marker
    /// already present is a no-op; a mismatched or malformed one is a storage
    /// error. It records a decision, never a settlement, and a capture replay
    /// reads it to report the original `Pending` outcome instead of the
    /// candidate's later state. Only the run that made the decision writes it:
    /// a replay never calls it, so a run that failed before this write leaves
    /// its decision unrecorded and later replays project current state.
    /// Never call it for an empty set or a failed decision.
    async fn record_absorb_pending(
        &self,
        principal: &Principal,
        request: &MemoryCaptureRequest,
        pending_candidate_ids: &[&str],
    ) -> Result<(), SpiError>;

    /// Read `candidate_id` and one bounded authorized recall page over the
    /// same subject and placement, on one transaction. The page is empty for
    /// an already-consumed candidate.
    async fn absorb_snapshot(
        &self,
        principal: &Principal,
        space_id: &str,
        candidate_id: &str,
        max_items: u64,
    ) -> Result<AbsorbSnapshot, SpiError>;

    /// Submit one review decision over `cited`, publish its targets and settle
    /// its receipt; returns the records the review created, each with the
    /// revision the settlement committed.
    ///
    /// Every cited record is revalidated in the same staging transaction: one
    /// that changed, disappeared, became withheld or lost its admission
    /// refuses the decision before anything is staged.
    async fn review_candidate(
        &self,
        principal: &Principal,
        request: &MemoryCandidatesReviewRequest,
        cited: &[CitedRecord],
    ) -> Result<Vec<SettledRevision>, SpiError>;

    /// Stage `memory.records.update` and `memory.candidates.dismiss` in one
    /// transaction, publish both intents' targets and settle both receipts;
    /// returns the updated record with the revision the settlement committed.
    ///
    /// `cited` is revalidated in that same staging transaction first.
    async fn update_candidate(
        &self,
        principal: &Principal,
        space_id: &str,
        update: &MemoryRecordsUpdateRequest,
        dismiss: &MemoryCandidatesDismissRequest,
        cited: &[CitedRecord],
    ) -> Result<Vec<SettledRevision>, SpiError>;

    /// The record id this caller's update leg already settled under
    /// `idempotency_key`.
    ///
    /// The dismissal that consumes an updated candidate carries no record id,
    /// so a replay reads the update leg's own receipt instead.
    async fn recorded_update(
        &self,
        principal: &Principal,
        space_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<String>, SpiError>;

    /// Read the live projection of one record a settlement just committed.
    ///
    /// `None` when the record is now a tombstone: there is nothing to warm. A
    /// record this caller may no longer read, or whose subject became withheld,
    /// surfaces as a refusal — the warm treats it as a miss too.
    async fn settled_record(
        &self,
        principal: &Principal,
        space_id: &str,
        record_id: &str,
    ) -> Result<Option<SettledRecord>, SpiError>;

    /// Reauthorize the released recall and store its remaining vectors, all in
    /// one transaction.
    ///
    /// The read rebuilds admission and withholding and compares the page and
    /// the knowledge set to `expected`: a refusal or a moved set aborts before
    /// anything is written. When there is a fill to write, the transaction
    /// takes the write reservation at begin (`BEGIN IMMEDIATE`), so no other
    /// connection can commit between that reauthorization and the write: a
    /// caller that sees `Ok` therefore releases a page whose cache write
    /// observed the same live reauthorization. A fill that no page row still
    /// serves live at its revision is skipped, and with the reservation held a
    /// cache-only storage failure is a miss rather than a refused release.
    async fn settle_recall(
        &self,
        principal: &Principal,
        request: &MemoryRecallRequest,
        identity: Option<&EmbeddingIdentity>,
        expected: &SnapshotFact,
        fills: &[RecordFill],
    ) -> Result<(), SpiError>;

    /// Store every warmed vector for `space_id` under `identity` on one
    /// transaction, rechecking each record's live revision and withholding.
    ///
    /// A tombstone, a moved revision or a withheld subject writes nothing, and
    /// an unreadable record refuses the leg. The commit binds every element
    /// together; the caller treats a refusal as a cache miss.
    async fn fill_record_vectors(
        &self,
        principal: &Principal,
        space_id: &str,
        identity: &EmbeddingIdentity,
        fills: &[RecordFill],
    ) -> Result<(), SpiError>;

    /// Read one deliberation snapshot: the subject's authorized memory page
    /// and the caller's admitted holder carrier, on one transaction.
    ///
    /// The holder must name the existing carrier entry of the caller's bound
    /// knowledge collection. A memory grant is not an admission to the
    /// carrier; only the binding's knowledge admission is. With no subject
    /// there is no page to read: the snapshot serves an empty memory page, and
    /// the space-level revalidation and unsettled-intent checks run in place of
    /// the subject-level pair.
    async fn deliberate_snapshot(
        &self,
        principal: &Principal,
        space_id: &str,
        subject_id: Option<&str>,
        holder: &CognitionReference,
        max_items: u64,
    ) -> Result<DeliberateSnapshot, SpiError>;

    /// Append `record` to the holder carrier and persist the replacement under
    /// one caller-managed write transaction.
    ///
    /// Rechecks the caller's admission to *write* the carrier (a read grant is
    /// not one), the recall admission and the unsettled-intent withhold — over
    /// the subject when one is named, over the whole space when none is — and
    /// every cited record revision against `cited`, and refuses a carrier that
    /// no longer holds `expected_carrier_revision`. Returns the carrier
    /// revision of the derived `MindState` after a confirmed commit.
    ///
    /// An implementation must observe `deadline` itself, because the caller
    /// cannot bound the wait for its storage: while acquiring whatever the
    /// implementation writes through, and again before beginning its write
    /// transaction, an expiry answers [`SpiError::DeadlineExceeded`] and starts
    /// no write. Once that transaction has begun the deadline no longer ends
    /// it: the write's actual or uncertain outcome is preserved even when it
    /// lands after `deadline`. The wait for SQLite's own write lock inside the
    /// begin is outside the bound.
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
    ) -> Result<u64, SpiError>;
}

/// One deliberation read: the subject's authorized memory page and the holder
/// carrier's current-view cognition records, read on one transaction.
#[derive(Debug, Clone)]
pub struct DeliberateSnapshot {
    /// The bounded, ACL- and withholding-checked subject page, in adapter
    /// order.
    pub records: Vec<RetainedRecord>,
    /// The carrier's current-view cognition records, in carrier order.
    pub cognition: Vec<Value>,
    /// The carrier revision the read served; a settlement CAS expects it.
    pub carrier_revision: u64,
}

/// One absorb read: the candidate and the bounded evidence page it was decided
/// against, from one transaction over the same subject and placement.
#[derive(Debug, Clone)]
pub struct AbsorbSnapshot {
    /// The candidate as the authorized read served it.
    pub candidate: MemoryCandidate,
    /// The bounded recall page, in adapter order.
    pub records: Vec<RetainedRecord>,
}

/// One capture call: the candidates it addresses and whether it replayed an
/// earlier capture receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureOutcome {
    /// The candidates the capture receipt holds, in receipt order.
    pub candidates: Vec<CapturedCandidate>,
    /// The capture key already held a receipt, so no candidate here is fresh.
    pub replayed: bool,
}

/// One candidate a capture receipt holds, with the state a replay reports
/// instead of deciding it again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedCandidate {
    /// The candidate id the capture receipt holds.
    pub candidate_id: String,
    /// A fresh capture's candidate is always pending; a replay reports the
    /// state its recorded run left.
    pub lifecycle: CandidateLifecycle,
}

/// What one candidate a capture receipt holds has become.
///
/// A replay reports `Consumed` only as the pointer to the authorized lifecycle
/// read: the projection of a consumed candidate needs the review and recall
/// grants that consumed it, so this state alone discloses nothing about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateLifecycle {
    /// Nothing has consumed it: the receipt's own candidate, still undecided.
    Pending,
    /// The capture's own absorb run recorded `Pending` for it: a replay reports
    /// that decision, whatever state the candidate has reached since, and
    /// settles nothing.
    PendingDecision,
    /// Something consumed it; read its recorded outcome under its own grants.
    Consumed,
}

/// One record a settlement just committed: the revision and the exact live
/// text the recall path scores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettledRecord {
    /// The record id the settlement named.
    pub record_id: String,
    /// The revision the settlement committed.
    pub revision: u64,
    /// The live `record_text` a warmed vector must embed.
    pub text: String,
}

/// One record a settlement just committed: the id it named and the revision it
/// left behind.
///
/// A settlement reports this pair, not only the id, so the warm can tell work
/// that is still the settled revision from work a later writer already made
/// obsolete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettledRevision {
    /// The record id the settlement named.
    pub record_id: String,
    /// The revision the settlement committed for that record.
    pub revision: u64,
}

/// One record an absorb decision cited, with the revision the prompted page
/// served it at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CitedRecord {
    /// The cited record id.
    pub record_id: String,
    /// The revision the prompt page served.
    pub revision: u64,
}

/// The workflow engine over one [`WorkflowBinding`].
pub struct SpiMindWorkflows {
    binding: Box<dyn WorkflowBinding>,
    /// The one recent validated query vector: replaced, never grown. A recall
    /// installs it only after it reauthorized the page, and [`Self::retire_identity`]
    /// clears it.
    query_slot: Mutex<Option<QuerySlot>>,
    /// Test-only: every query-slot lookup, so a refused read can prove it
    /// never reached the in-memory cache. In-crate tests only; production
    /// builds compile neither the counter nor its increment.
    #[cfg(test)]
    query_slot_probes: std::sync::atomic::AtomicUsize,
}

/// One validated query vector and the exact scope allowed to reuse it: another
/// principal, space, subject, query text or embedding identity is a miss.
struct QuerySlot {
    /// `(principal, space, subject, exact trimmed query)` as authorized.
    scope: (String, String, String, String),
    identity: EmbeddingIdentity,
    vector: Vec<f32>,
}

impl SpiMindWorkflows {
    /// Build the engine over `binding`.
    pub fn new(binding: impl WorkflowBinding + 'static) -> Self {
        Self {
            binding: Box::new(binding),
            query_slot: Mutex::new(None),
            #[cfg(test)]
            query_slot_probes: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// How many recalls have looked the in-memory query slot up.
    #[cfg(test)]
    fn query_slot_probes(&self) -> usize {
        self.query_slot_probes
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The slot's query vector, only for the exact scope it was validated in.
    fn cached_query(
        &self,
        scope: &(String, String, String, String),
        identity: &EmbeddingIdentity,
    ) -> Option<Vec<f32>> {
        #[cfg(test)]
        self.query_slot_probes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let slot = self.query_slot.lock().ok()?;
        let slot = slot.as_ref()?;
        (slot.scope == *scope && slot.identity == *identity).then(|| slot.vector.clone())
    }

    /// Keep `vector` for `scope`: the caller installs it only after the recall
    /// reauthorized the page this vector was validated against.
    fn install_query(&self, slot: QuerySlot) {
        if let Ok(mut current) = self.query_slot.lock() {
            *current = Some(slot);
        }
    }

    /// Retire one serving identity's query vector.
    ///
    /// The host calls this when it rotates identity, together with deleting
    /// that identity's stored partition: no slot may outlive the rows it was
    /// validated against, so a later recall re-embeds its query instead of
    /// reusing a vector the retired identity produced. Another identity's slot
    /// stays warm.
    pub fn retire_identity(&self, identity: &EmbeddingIdentity) {
        if let Ok(mut slot) = self.query_slot.lock() {
            if slot.as_ref().is_some_and(|slot| slot.identity == *identity) {
                *slot = None;
            }
        }
    }

    /// Pack an authorized context pack under `max_tokens`.
    ///
    /// One recall page is read, reranked by the injected capabilities, then
    /// reauthorized and filled through one binding call before anything is
    /// released: a refused reread propagates and a changed page or knowledge
    /// set returns `REVISION_CONFLICT`, so no record revision or grant that
    /// moved during provider work reaches the pack. The page's missing vectors
    /// are written inside that same call, so no awaited cache work can observe
    /// a later refusal after the release was authorized. The query slot is
    /// installed only once that call succeeded, so a refused recall leaves no
    /// cache entry behind.
    pub async fn recall_context(
        &self,
        principal: &Principal,
        request: &MemoryRecallRequest,
        max_tokens: NonZeroUsize,
        caps: &MindCapabilities<'_>,
    ) -> Result<ContextPack, SpiError> {
        let RecallSnapshot { records, knowledge } =
            self.binding.recall_snapshot(principal, request).await?;
        let expected = snapshot_fact(&records, &knowledge);
        let RankedRecall {
            records: ranked,
            fills,
            query,
        } = self.rank(principal, request, records, caps).await?;
        let identity = caps.embedder.and_then(|embedder| embedder.identity());
        self.binding
            .settle_recall(principal, request, identity, &expected, &fills)
            .await?;
        if let Some(slot) = query {
            self.install_query(slot);
        }
        pack(ranked, knowledge, max_tokens)
    }

    /// Decide what each captured candidate becomes.
    ///
    /// The capture runs the existing admission, idempotency probe and
    /// deterministic classifier. Without a reasoner every fresh candidate
    /// stays pending and no review is written, reported straight from the
    /// capture receipt: reading it back needs review and recall grants the
    /// capture admission never required. A candidate an earlier run already
    /// consumed reports that run's recorded outcome, with or without a
    /// reasoner. A replay — the capture key already held a receipt — reports
    /// `Pending` for a candidate that is still pending without reading it back:
    /// the recorded run did not settle it, the caller holds the receipt that
    /// names it, and a replay must never settle what it held. Only a consumed
    /// candidate a replay names is read, under the grants that consumed it.
    ///
    /// With a reasoner, each fresh candidate is read with one bounded
    /// authorized evidence page, the reasoner runs outside any transaction,
    /// its reply is validated against that exact page, and the policy hook
    /// decides whether the validated settlement is submitted. Only the run
    /// that decided writes: before a run that leaves fresh candidates pending
    /// returns, the binding records one durable decision marker for them, and a
    /// replay reads that marker and reports the original `Pending` outcome even
    /// after the candidate was resolved elsewhere. A replay never writes a
    /// marker — without one it projects each candidate's current state and
    /// records nothing, because a snapshot of the receipt cannot tell a run
    /// that failed before settling from another run still in flight. A
    /// decision that was never recorded stays unrecorded.
    pub async fn absorb(
        &self,
        principal: &Principal,
        request: &MemoryCaptureRequest,
        lens: Option<&dyn DomainLens>,
        caps: &MindCapabilities<'_>,
    ) -> Result<Vec<AbsorbCandidateOutcome>, SpiError> {
        let CaptureOutcome {
            candidates,
            replayed,
        } = self.binding.capture(principal, request).await?;
        let mut outcomes = Vec::with_capacity(candidates.len());
        let mut held: Vec<String> = Vec::new();
        for CapturedCandidate {
            candidate_id,
            lifecycle,
        } in candidates
        {
            if replayed {
                let pending = matches!(
                    lifecycle,
                    CandidateLifecycle::Pending | CandidateLifecycle::PendingDecision
                );
                outcomes.push(if pending {
                    AbsorbCandidateOutcome::Pending { candidate_id }
                } else {
                    self.replay_candidate(principal, request, candidate_id)
                        .await?
                });
                continue;
            }
            let Some(reasoner) = caps.reasoner else {
                held.push(candidate_id.clone());
                outcomes.push(AbsorbCandidateOutcome::Pending { candidate_id });
                continue;
            };
            let outcome = self
                .absorb_candidate(
                    principal,
                    request,
                    candidate_id.clone(),
                    reasoner,
                    lens,
                    caps,
                )
                .await?;
            if matches!(&outcome, AbsorbCandidateOutcome::Pending { .. }) {
                held.push(candidate_id);
            }
            outcomes.push(outcome);
        }
        if !held.is_empty() {
            let pending: Vec<&str> = held.iter().map(String::as_str).collect();
            self.binding
                .record_absorb_pending(principal, request, &pending)
                .await?;
        }
        Ok(outcomes)
    }

    /// Decide and settle one freshly captured candidate.
    async fn absorb_candidate(
        &self,
        principal: &Principal,
        request: &MemoryCaptureRequest,
        candidate_id: String,
        reasoner: &dyn Reasoner,
        lens: Option<&dyn DomainLens>,
        caps: &MindCapabilities<'_>,
    ) -> Result<AbsorbCandidateOutcome, SpiError> {
        let space = request.space_id.to_string();
        let capture_key = request.idempotency_key.to_string();
        let AbsorbSnapshot { candidate, records } = self
            .binding
            .absorb_snapshot(principal, &space, &candidate_id, ABSORB_PAGE_ITEMS)
            .await?;
        let pending = match &candidate {
            MemoryCandidate::PendingCandidate(pending) => pending,
            MemoryCandidate::ConsumedCandidate(consumed) => {
                return self
                    .recorded_outcome(principal, &space, &capture_key, consumed)
                    .await
            }
        };
        let action = decide(reasoner, pending, &records, lens).await?;
        if let Some(lens) = lens {
            let admitted: Vec<&str> = records.iter().map(record_id).collect();
            let cited: Vec<&str> = action
                .cited()
                .iter()
                .map(|record| record.record_id.as_str())
                .collect();
            lens.validate_proposal(
                &ProposalDraft::Absorb {
                    action: action.kind(),
                    target_record_id: action.target(),
                    rationale: action.rationale(),
                    cited_ids: &cited,
                },
                &admitted,
            )
            .map_err(SpiError::PolicyRejected)?;
        }
        let expected_revision = pending.revision.get();
        let review_key = absorb_key(&capture_key, &candidate_id, "review")?;
        let fallback = DefaultPolicy;
        let policy: &dyn PolicyHook = caps.policy_hook.unwrap_or(&fallback);
        match action {
            AbsorbAction::Promote { cited } => {
                let review = review_request(
                    &space,
                    &candidate_id,
                    expected_revision,
                    &review_key,
                    json!({ "action": "promote" }),
                )?;
                let verdict = policy
                    .evaluate_settlement(SettlementDraft::CandidateReview {
                        candidate: &candidate,
                        proposed: &review.decision,
                    })
                    .await?;
                match verdict {
                    PolicyDecision::AutoSettle => {
                        let settled = self
                            .binding
                            .review_candidate(principal, &review, &cited)
                            .await?;
                        self.warm_settled(principal, &space, &settled, caps).await;
                        Ok(AbsorbCandidateOutcome::Settled {
                            candidate_id,
                            record_ids: settled
                                .into_iter()
                                .map(|record| record.record_id)
                                .collect(),
                        })
                    }
                    PolicyDecision::RequireHumanReview { .. } => {
                        Ok(AbsorbCandidateOutcome::Pending { candidate_id })
                    }
                    PolicyDecision::Reject { reason } => {
                        drop_candidate(
                            self.binding.as_ref(),
                            principal,
                            &space,
                            &candidate_id,
                            expected_revision,
                            &review_key,
                            reason,
                            &cited,
                        )
                        .await
                    }
                }
            }
            AbsorbAction::Dismiss { rationale, cited } => {
                let review = review_request(
                    &space,
                    &candidate_id,
                    expected_revision,
                    &review_key,
                    json!({ "action": "drop", "rationale": rationale }),
                )?;
                let verdict = policy
                    .evaluate_settlement(SettlementDraft::CandidateReview {
                        candidate: &candidate,
                        proposed: &review.decision,
                    })
                    .await?;
                match verdict {
                    PolicyDecision::AutoSettle => {
                        self.binding
                            .review_candidate(principal, &review, &cited)
                            .await?;
                        Ok(AbsorbCandidateOutcome::Dropped { candidate_id })
                    }
                    PolicyDecision::RequireHumanReview { .. } => {
                        Ok(AbsorbCandidateOutcome::Pending { candidate_id })
                    }
                    PolicyDecision::Reject { reason } => {
                        drop_candidate(
                            self.binding.as_ref(),
                            principal,
                            &space,
                            &candidate_id,
                            expected_revision,
                            &review_key,
                            reason,
                            &cited,
                        )
                        .await
                    }
                }
            }
            AbsorbAction::Update {
                record_id,
                revision: target_revision,
                cited,
            } => {
                let content = serde_json::to_value(&pending.proposed.content)
                    .map_err(|error| SpiError::InvalidInput(error.to_string()))?;
                let update = update_request(
                    &space,
                    &record_id,
                    target_revision,
                    &absorb_key(&capture_key, &candidate_id, "update")?,
                    content,
                )?;
                let dismiss = dismiss_request(
                    &space,
                    &candidate_id,
                    expected_revision,
                    &absorb_key(&capture_key, &candidate_id, "dismiss")?,
                )?;
                let verdict = policy
                    .evaluate_settlement(SettlementDraft::CandidateUpdate {
                        candidate: &candidate,
                        proposed: &update,
                    })
                    .await?;
                match verdict {
                    PolicyDecision::AutoSettle => {
                        let settled = self
                            .binding
                            .update_candidate(principal, &space, &update, &dismiss, &cited)
                            .await?;
                        self.warm_settled(principal, &space, &settled, caps).await;
                        Ok(AbsorbCandidateOutcome::Settled {
                            candidate_id,
                            record_ids: settled
                                .into_iter()
                                .map(|record| record.record_id)
                                .collect(),
                        })
                    }
                    PolicyDecision::RequireHumanReview { .. } => {
                        Ok(AbsorbCandidateOutcome::Pending { candidate_id })
                    }
                    PolicyDecision::Reject { reason } => {
                        drop_candidate(
                            self.binding.as_ref(),
                            principal,
                            &space,
                            &candidate_id,
                            expected_revision,
                            &review_key,
                            reason,
                            &cited,
                        )
                        .await
                    }
                }
            }
        }
    }

    /// Warm the vector cache for records a settlement just committed.
    ///
    /// Runs after that settlement transaction closed and before the outcome is
    /// reported, so the live read and the provider call hold no transaction.
    /// Every failure is a cache miss: a post-commit warm never undoes, replays
    /// or misreports the durable Memory effect it serves.
    async fn warm_settled(
        &self,
        principal: &Principal,
        space: &str,
        settled: &[SettledRevision],
        caps: &MindCapabilities<'_>,
    ) {
        let Some(embedder) = caps.embedder else {
            return;
        };
        let Some(identity) = embedder.identity() else {
            return;
        };
        for settled in settled {
            let _ = self
                .warm_record(principal, space, settled, embedder, identity)
                .await;
        }
    }

    /// Warm one settled record: the live read, one provider call outside every
    /// transaction, then a fill that rechecks the revision and withholding.
    ///
    /// The live read must still serve the revision the settlement committed. A
    /// record that moved since makes the settled work obsolete: its text was
    /// never this revision's, so nothing is embedded and no row is written.
    async fn warm_record(
        &self,
        principal: &Principal,
        space: &str,
        settled: &SettledRevision,
        embedder: &dyn Embedder,
        identity: &EmbeddingIdentity,
    ) -> Result<(), SpiError> {
        let Some(record) = self
            .binding
            .settled_record(principal, space, &settled.record_id)
            .await?
        else {
            return Ok(());
        };
        if record.revision != settled.revision {
            return Ok(());
        }
        let mut vectors = checked_vectors(
            embedder
                .embed_batch(std::slice::from_ref(&record.text))
                .await?,
            1,
            identity,
        )?;
        let vector = vectors.pop().expect("one text yields one vector");
        self.binding
            .fill_record_vectors(principal, space, identity, &[RecordFill { record, vector }])
            .await
    }

    /// What an earlier run of this exact capture recorded for one candidate
    /// that run consumed: a replay never decides again.
    ///
    /// Only a consumed candidate is read here. The receipt already reported
    /// whether a candidate is still pending, so this authorized read runs for
    /// the one projection that needs the review and recall grants that
    /// consumed it.
    async fn replay_candidate(
        &self,
        principal: &Principal,
        request: &MemoryCaptureRequest,
        candidate_id: String,
    ) -> Result<AbsorbCandidateOutcome, SpiError> {
        let space = request.space_id.to_string();
        let AbsorbSnapshot { candidate, .. } = self
            .binding
            .absorb_snapshot(principal, &space, &candidate_id, ABSORB_PAGE_ITEMS)
            .await?;
        match &candidate {
            MemoryCandidate::ConsumedCandidate(consumed) => {
                let capture_key = request.idempotency_key.to_string();
                self.recorded_outcome(principal, &space, &capture_key, consumed)
                    .await
            }
            MemoryCandidate::PendingCandidate(_) => {
                Ok(AbsorbCandidateOutcome::Pending { candidate_id })
            }
        }
    }

    /// What an earlier run of this exact capture recorded for a consumed
    /// candidate.
    ///
    /// The dismissal that consumes an updated candidate carries no record id,
    /// so that case reads the update leg's own receipt under the key this
    /// capture derives; any other consumption reports its recorded outcome.
    async fn recorded_outcome(
        &self,
        principal: &Principal,
        space: &str,
        capture_key: &str,
        consumed: &ConsumedCandidate,
    ) -> Result<AbsorbCandidateOutcome, SpiError> {
        let candidate_id = consumed.candidate_id.to_string();
        let consumption = &consumed.consumed_by;
        match consumption.outcome {
            Outcome::Dismissed => {
                let key = absorb_key(capture_key, &candidate_id, "update")?;
                match self.binding.recorded_update(principal, space, &key).await? {
                    Some(record_id) => Ok(AbsorbCandidateOutcome::Settled {
                        candidate_id,
                        record_ids: vec![record_id],
                    }),
                    None => Ok(AbsorbCandidateOutcome::Dropped { candidate_id }),
                }
            }
            Outcome::Promoted | Outcome::Fragmented => Ok(AbsorbCandidateOutcome::Settled {
                candidate_id,
                record_ids: consumption.record_ids.clone(),
            }),
            Outcome::Dropped => Ok(AbsorbCandidateOutcome::Dropped { candidate_id }),
        }
    }

    /// Derive a validated verdict over the supplied evidence and settle the
    /// record it promotes onto the holder's cognition carrier.
    ///
    /// One authorized snapshot is read, the reasoner runs outside every
    /// transaction, its verdict is validated against that exact snapshot, the
    /// lens and policy decide, and the carrier is appended and CAS-persisted in
    /// one write transaction. The one deadline bounds the reasoner and policy
    /// calls themselves, not just the boundaries between phases: a call still
    /// running at expiry is dropped and reported as a deadline refusal, so no
    /// pre-commit phase can outrun it. Once the write begins its actual or
    /// uncertain outcome is preserved. Only a definite
    /// stale-revision conflict starts another attempt — at most twice, and
    /// never after the request deadline.
    pub async fn deliberate(
        &self,
        principal: &Principal,
        request: DeliberateRequest<'_>,
        lens: Option<&dyn DomainLens>,
        caps: &MindCapabilities<'_>,
    ) -> Result<DeliberateOutcome, SpiError> {
        if request.assessment_context_id.is_none() && request.subject_id.is_none() {
            return Err(SpiError::InvalidInput(
                "the request names neither an assessment context nor a subject to derive it from"
                    .to_string(),
            ));
        }
        let Some(reasoner) = caps.reasoner else {
            return Err(SpiError::ReasonerUnavailable);
        };
        let deadline = deadline_after(request.timeout);
        let mut retries = 0usize;
        loop {
            ensure_time_left(deadline)?;
            match self
                .deliberate_attempt(principal, &request, reasoner, lens, caps, deadline)
                .await
            {
                Err(error) if stale_revision(&error) => {
                    if retries == DELIBERATE_RETRIES {
                        return Err(error);
                    }
                    retries += 1;
                }
                outcome => return outcome,
            }
        }
    }

    async fn deliberate_attempt(
        &self,
        principal: &Principal,
        request: &DeliberateRequest<'_>,
        reasoner: &dyn Reasoner,
        lens: Option<&dyn DomainLens>,
        caps: &MindCapabilities<'_>,
        deadline: Instant,
    ) -> Result<DeliberateOutcome, SpiError> {
        let snapshot = self
            .binding
            .deliberate_snapshot(
                principal,
                request.space_id,
                request.subject_id,
                request.holder,
                DELIBERATE_PAGE_ITEMS,
            )
            .await?;
        ensure_time_left(deadline)?;
        let admitted = admitted_references(request.evidence, &snapshot);
        let payload = deliberate_payload(request, &snapshot);
        let schema = deliberate_schema();
        let prompt = task_prompt(DELIBERATE_PROMPT, CognitiveTaskKind::Deliberate, lens);
        let verdict = before_deadline(
            deadline,
            reasoner.reason_structured(StructuredReasonRequest {
                task_kind: CognitiveTaskKind::Deliberate,
                system_prompt: &prompt,
                user_payload: &payload,
                output_schema: &schema,
            }),
        )
        .await?;
        ensure_time_left(deadline)?;
        let DeliberateProposal {
            recommendation,
            record,
            cited,
            cited_ids,
        } = deliberate_proposal(&verdict, request, principal, &snapshot)?;
        if let Some(lens) = lens {
            let admitted: Vec<&str> = admitted.iter().map(String::as_str).collect();
            let cited_ids: Vec<&str> = cited_ids.iter().map(String::as_str).collect();
            lens.validate_proposal(
                &ProposalDraft::Cognition {
                    recommendation: &recommendation,
                    record: record.as_ref(),
                    cited_ids: &cited_ids,
                },
                &admitted,
            )
            .map_err(SpiError::PolicyRejected)?;
        }
        ensure_time_left(deadline)?;
        let Some(record) = record else {
            let judgment = deliberate_judgment(principal, request, "no_change", &verdict, &cited)?;
            ensure_time_left(deadline)?;
            return Ok(DeliberateOutcome::NoChange { verdict, judgment });
        };
        let fallback = DefaultPolicy;
        let policy: &dyn PolicyHook = caps.policy_hook.unwrap_or(&fallback);
        let decision = before_deadline(
            deadline,
            policy.evaluate_settlement(SettlementDraft::Cognition { proposed: &record }),
        )
        .await?;
        match decision {
            PolicyDecision::AutoSettle => {}
            PolicyDecision::RequireHumanReview { .. } => {
                let judgment =
                    deliberate_judgment(principal, request, "needs_review", &verdict, &cited)?;
                ensure_time_left(deadline)?;
                return Ok(DeliberateOutcome::NeedsReview {
                    verdict,
                    draft: record,
                    judgment,
                });
            }
            PolicyDecision::Reject { reason } => return Err(SpiError::PolicyRejected(reason)),
        }
        let judgment = deliberate_judgment(principal, request, "settled", &verdict, &cited)?;
        let stored = serde_json::to_value(&record)
            .map_err(|error| SpiError::InvalidInput(error.to_string()))?;
        ensure_time_left(deadline)?;
        let mind_state_version = self
            .binding
            .commit_cognition(
                principal,
                request.space_id,
                request.subject_id,
                request.holder,
                snapshot.carrier_revision,
                stored,
                &cited,
                deadline,
            )
            .await?;
        Ok(DeliberateOutcome::Settled {
            verdict,
            record,
            mind_state_version,
            judgment,
        })
    }
}

#[async_trait]
impl MindWorkflows for SpiMindWorkflows {
    async fn recall_context(
        &self,
        principal: &Principal,
        request: &MemoryRecallRequest,
        max_tokens: NonZeroUsize,
        caps: &MindCapabilities<'_>,
    ) -> Result<ContextPack, SpiError> {
        SpiMindWorkflows::recall_context(self, principal, request, max_tokens, caps).await
    }

    async fn absorb(
        &self,
        principal: &Principal,
        request: &MemoryCaptureRequest,
        lens: Option<&dyn DomainLens>,
        caps: &MindCapabilities<'_>,
    ) -> Result<Vec<AbsorbCandidateOutcome>, SpiError> {
        SpiMindWorkflows::absorb(self, principal, request, lens, caps).await
    }

    async fn deliberate(
        &self,
        principal: &Principal,
        request: DeliberateRequest<'_>,
        lens: Option<&dyn DomainLens>,
        caps: &MindCapabilities<'_>,
    ) -> Result<DeliberateOutcome, SpiError> {
        SpiMindWorkflows::deliberate(self, principal, request, lens, caps).await
    }
}

/// Bounded evidence page one absorb decision may cite: one recall page per
/// candidate, never a second page.
const ABSORB_PAGE_ITEMS: u64 = 32;

/// Bounded subject page one deliberation reads: one recall page, never a
/// second page.
const DELIBERATE_PAGE_ITEMS: u64 = 32;

/// Retries after the first attempt, so a stale carrier or citation never
/// loops past three snapshots.
const DELIBERATE_RETRIES: usize = 2;

/// The identity every visible memory record's reference carries: the record
/// kind and the Mindcore realm namespace. A basis entry naming any other pair
/// claims an identity this snapshot never admitted.
pub(crate) const MEMORY_RECORD_IDENTITY: (&str, &str) = ("memory_record", "mindcore");

/// The longest request deadline the engine honours. A timeout is clamped here
/// before the instant is computed, so no absurd value can overflow the instant
/// and silently leave the call with no deadline at all.
const MAX_TIMEOUT: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);

/// The request's single deadline: `timeout` clamped to [`MAX_TIMEOUT`].
///
/// The clamp keeps the deadline a real instant for any caller-supplied
/// duration; a residual instant overflow fails closed at "now" (every phase
/// refuses) rather than dropping the bound.
fn deadline_after(timeout: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(timeout.min(MAX_TIMEOUT))
        .or_else(|| now.checked_add(MAX_TIMEOUT))
        .unwrap_or(now)
}

/// The request's single deadline: no phase starts once it elapsed, so a
/// reasoner, lens or policy that returns late never starts the carrier write.
/// An outcome already in flight is never reported as a deadline refusal.
fn ensure_time_left(deadline: Instant) -> Result<(), SpiError> {
    if Instant::now() >= deadline {
        return Err(SpiError::DeadlineExceeded);
    }
    Ok(())
}

/// Await one provider call under the request's deadline and drop it at expiry,
/// so a hung or late reasoner or policy surfaces as a deadline refusal instead
/// of blocking the caller forever. A provider result that is already ready is
/// kept; only a write that already began is never cancelled.
async fn before_deadline<T>(
    deadline: Instant,
    work: impl Future<Output = Result<T, SpiError>>,
) -> Result<T, SpiError> {
    let mut work = Box::pin(work);
    let mut delay = Box::pin(futures_timer::Delay::new(
        deadline.saturating_duration_since(Instant::now()),
    ));
    poll_fn(move |cx| {
        if let Poll::Ready(outcome) = work.as_mut().poll(cx) {
            return Poll::Ready(outcome);
        }
        if delay.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(SpiError::DeadlineExceeded));
        }
        Poll::Pending
    })
    .await
}

/// Fixed skeleton of the absorb leg; a lens only adds guidance below it.
const ABSORB_PROMPT: &str = "Decide what one captured memory candidate becomes for its \
     subject, against the subject's memory records supplied with it. Return exactly one \
     decision. promote: the candidate states something the supplied records do not, so it \
     becomes a new record. update: it restates or corrects exactly one supplied record, so \
     record_id names that record. dismiss: it adds nothing worth keeping, so rationale says \
     why. evidence_ids lists the supplied record ids the decision rests on: unique, never \
     repeated, never invented; an update cites the record it targets among them. Revisions, \
     ids and requests are fixed outside this reply; never supply them.";

/// Output schema handed to the reasoner. The workflow validates the reply and
/// every cited id itself; the schema only describes the closed shape.
fn absorb_schema() -> Value {
    let evidence = json!({ "type": "array", "items": { "type": "string" } });
    json!({
        "type": "object",
        "oneOf": [
            {
                "type": "object",
                "properties": {
                    "action": { "const": "promote" },
                    "evidence_ids": evidence.clone(),
                },
                "required": ["action", "evidence_ids"],
                "additionalProperties": false,
            },
            {
                "type": "object",
                "properties": {
                    "action": { "const": "update" },
                    "record_id": { "type": "string" },
                    "evidence_ids": evidence.clone(),
                },
                "required": ["action", "record_id", "evidence_ids"],
                "additionalProperties": false,
            },
            {
                "type": "object",
                "properties": {
                    "action": { "const": "dismiss" },
                    "rationale": { "type": "string" },
                    "evidence_ids": evidence,
                },
                "required": ["action", "rationale", "evidence_ids"],
                "additionalProperties": false,
            },
        ],
    })
}

/// Fixed skeleton of the deliberate leg; a lens only adds guidance below it.
const DELIBERATE_PROMPT: &str = "Deliberate what one cognition holder should record for the \
     supplied cognition target, resting only on the supplied evidence and the holder's visible \
     records. Return exactly one verdict. recommendation.kind is none when nothing should be \
     recorded, or promote with target_kind naming what the record promotes (an observation, a \
     belief, an ADR...). rationale explains the verdict in prose. evidence_ids lists the supplied \
     evidence the verdict rests on and record_ids the visible record ids it rests on: unique, \
     never repeated, never invented. A promotion carries record with the cognition record's own \
     content; record_id, holder, target, assessor, assessment_context, assessed_at, effective_at, \
     view and extensions are fixed outside this reply and never supplied.";

/// The fixed skeleton plus a lens's named domain-guidance block, inserted
/// after the instructions for either workflow's task kind.
fn task_prompt(
    skeleton: &str,
    task_kind: CognitiveTaskKind,
    lens: Option<&dyn DomainLens>,
) -> String {
    let Some(lens) = lens else {
        return skeleton.to_string();
    };
    let LensOutput {
        persona_and_stance,
        salience_criteria,
        evidence_bar,
        redlines,
    } = lens.describe(task_kind);
    let mut prompt = format!(
        "{skeleton}\n\n## Domain guidance\nPersona and stance: {persona_and_stance}\n\
         Salience criteria: {salience_criteria}\nEvidence bar: {evidence_bar}\nRedlines:\n"
    );
    for redline in redlines {
        prompt.push_str(&format!("- {redline}\n"));
    }
    prompt
}

/// Output schema handed to the deliberate reasoner. The workflow validates
/// the reply and every cited id itself; the inner record is validated by the
/// pinned cognition schema after the workflow stamps its own fields.
fn deliberate_schema() -> Value {
    let strings = json!({ "type": "array", "items": { "type": "string" } });
    json!({
        "type": "object",
        "properties": {
            "recommendation": {
                "oneOf": [
                    {
                        "type": "object",
                        "properties": { "kind": { "const": "none" } },
                        "required": ["kind"],
                        "additionalProperties": false,
                    },
                    {
                        "type": "object",
                        "properties": {
                            "kind": { "const": "promote" },
                            "target_kind": { "type": "string" },
                        },
                        "required": ["kind", "target_kind"],
                        "additionalProperties": false,
                    },
                ],
            },
            "rationale": { "type": "string" },
            "evidence_ids": strings.clone(),
            "record_ids": strings,
            "record": { "type": "object" },
        },
        "required": ["recommendation", "rationale", "evidence_ids", "record_ids"],
        "additionalProperties": false,
    })
}

/// One validated deliberation verdict, with everything the workflow will
/// settle or revalidate.
struct DeliberateProposal {
    recommendation: Recommendation,
    record: Option<CognitionRecord>,
    cited: Vec<CitedRecord>,
    cited_ids: Vec<String>,
}

/// Every reference a verdict may cite: the supplied evidence ids plus the
/// memory and cognition records this snapshot made visible.
fn admitted_references(evidence: &[Evidence<'_>], snapshot: &DeliberateSnapshot) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    let mut admit = |id: &str| {
        if !ids.iter().any(|known| known == id) {
            ids.push(id.to_string());
        }
    };
    for item in evidence {
        admit(item.id);
    }
    for record in &snapshot.records {
        admit(record_id(record));
    }
    for record in &snapshot.cognition {
        if let Some(id) = record.get("record_id").and_then(Value::as_str) {
            admit(id);
        }
    }
    ids
}

/// The reasoner payload: the admitted evidence as data, beside the visible
/// memory page, the visible cognition records and the target.
fn deliberate_payload(request: &DeliberateRequest<'_>, snapshot: &DeliberateSnapshot) -> String {
    let evidence: Vec<Value> = request
        .evidence
        .iter()
        .map(|item| json!({ "evidence_id": item.id, "text": item.text }))
        .collect();
    let records: Vec<Value> = snapshot
        .records
        .iter()
        .map(|record| json!({ "record_id": record_id(record), "text": record_text(record) }))
        .collect();
    json!({
        "target": request.target,
        "holder": request.holder,
        "evidence": evidence,
        "records": records,
        "cognition": snapshot.cognition,
    })
    .to_string()
}

/// Validate the closed verdict shape, every cited reference and the proposed
/// record, stamping the workflow-owned fields from trusted inputs.
fn deliberate_proposal(
    verdict: &Value,
    request: &DeliberateRequest<'_>,
    principal: &Principal,
    snapshot: &DeliberateSnapshot,
) -> Result<DeliberateProposal, SpiError> {
    let object = verdict
        .as_object()
        .ok_or_else(|| invalid_verdict("the verdict is not an object"))?;
    let allowed = [
        "recommendation",
        "rationale",
        "evidence_ids",
        "record_ids",
        "record",
    ];
    if let Some(unexpected) = object
        .keys()
        .find(|member| !allowed.contains(&member.as_str()))
    {
        return Err(invalid_verdict(format!("the verdict carries {unexpected}")));
    }
    if !object
        .get("rationale")
        .and_then(Value::as_str)
        .is_some_and(|rationale| !rationale.trim().is_empty())
    {
        return Err(invalid_verdict("the verdict carries no rationale"));
    }
    let recommendation = deliberate_recommendation(object)?;
    let (cited, cited_ids) = deliberate_citations(object, request, snapshot)?;
    let record = match recommendation {
        Recommendation::None => None,
        Recommendation::Promote { .. } => {
            let draft = object
                .get("record")
                .ok_or_else(|| invalid_verdict("a promotion carries no record"))?;
            validate_basis(draft, snapshot, &cited)?;
            Some(stamp_cognition_record(draft, request, principal)?)
        }
    };
    Ok(DeliberateProposal {
        recommendation,
        record,
        cited,
        cited_ids,
    })
}

/// Every persisted basis reference must name the exact visible memory record it
/// claims — kind, namespace and id — and be one of the verdict's own record
/// citations, whose revisions the commit revalidates. An id with a fabricated
/// kind/namespace, or one the verdict never cited, names no source the write
/// transaction can recheck.
fn validate_basis(
    draft: &Value,
    snapshot: &DeliberateSnapshot,
    cited: &[CitedRecord],
) -> Result<(), SpiError> {
    for reference in draft
        .get("basis")
        .and_then(|basis| basis.get("evidence"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let field = |name: &str| {
            reference
                .get(name)
                .and_then(Value::as_str)
                .unwrap_or_default()
        };
        let (id, kind, namespace) = (field("id"), field("kind"), field("namespace"));
        if (kind, namespace) != MEMORY_RECORD_IDENTITY {
            return Err(invalid_verdict(format!(
                "basis evidence {kind}:{namespace}:{id} is not a {}:{} reference",
                MEMORY_RECORD_IDENTITY.0, MEMORY_RECORD_IDENTITY.1
            )));
        }
        if !snapshot
            .records
            .iter()
            .any(|record| record_id(record) == id)
        {
            return Err(invalid_verdict(format!(
                "basis evidence id {id} is not visible to this deliberation"
            )));
        }
        if !cited.iter().any(|record| record.record_id == id) {
            return Err(invalid_verdict(format!(
                "basis evidence id {id} is not cited in record_ids"
            )));
        }
    }
    Ok(())
}

/// The structured recommendation of one verdict.
fn deliberate_recommendation(object: &Map<String, Value>) -> Result<Recommendation, SpiError> {
    let recommendation = object
        .get("recommendation")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid_verdict("the verdict names no recommendation"))?;
    match recommendation.get("kind").and_then(Value::as_str) {
        Some("none") => {
            if recommendation.len() != 1 || object.contains_key("record") {
                return Err(invalid_verdict("a none recommendation carries no record"));
            }
            Ok(Recommendation::None)
        }
        Some("promote") => {
            if recommendation.len() != 2 {
                return Err(invalid_verdict(
                    "a promote recommendation carries kind and target_kind",
                ));
            }
            let target_kind = recommendation
                .get("target_kind")
                .and_then(Value::as_str)
                .filter(|kind| !kind.trim().is_empty())
                .ok_or_else(|| invalid_verdict("a promotion names no target_kind"))?;
            Ok(Recommendation::Promote {
                target_kind: target_kind.to_string(),
            })
        }
        Some(other) => Err(invalid_verdict(format!("{other} is not a recommendation"))),
        None => Err(invalid_verdict("the recommendation names no kind")),
    }
}

/// The exact references the verdict cites, each checked against the allowlist
/// the snapshot authorized.
fn deliberate_citations(
    object: &Map<String, Value>,
    request: &DeliberateRequest<'_>,
    snapshot: &DeliberateSnapshot,
) -> Result<(Vec<CitedRecord>, Vec<String>), SpiError> {
    let mut cited = Vec::new();
    let mut cited_ids = Vec::new();
    for id in string_ids(object, "evidence_ids")? {
        if !request.evidence.iter().any(|item| item.id == id) {
            return Err(invalid_verdict(format!(
                "evidence id {id} was not supplied to this deliberation"
            )));
        }
        cited_ids.push(id);
    }
    for id in string_ids(object, "record_ids")? {
        match snapshot
            .records
            .iter()
            .find(|record| record_id(record) == id)
        {
            Some(record) => cited.push(CitedRecord {
                record_id: id.clone(),
                revision: record_revision(record),
            }),
            None if snapshot.cognition.iter().any(|record| {
                record.get("record_id").and_then(Value::as_str) == Some(id.as_str())
            }) => {}
            None => {
                return Err(invalid_verdict(format!(
                    "record id {id} is not visible to this deliberation"
                )))
            }
        }
        cited_ids.push(id);
    }
    Ok((cited, cited_ids))
}

/// `field`: unique nonempty ids, never repeated.
fn string_ids(object: &Map<String, Value>, field: &str) -> Result<Vec<String>, SpiError> {
    let values = object
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_verdict(format!("the verdict names no {field} array")))?;
    let mut ids: Vec<String> = Vec::with_capacity(values.len());
    for value in values {
        let id = value
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| invalid_verdict(format!("a {field} entry is not a nonempty string")))?;
        if ids.iter().any(|known| known == id) {
            return Err(invalid_verdict(format!("{field} entry {id} appears twice")));
        }
        ids.push(id.to_string());
    }
    Ok(ids)
}

/// Stamp the workflow-owned fields onto the proposed record, then validate it
/// against the pinned cognition schema. The model supplies only content; the
/// identity, holder, target, context, timestamps and view are trusted inputs.
fn stamp_cognition_record(
    draft: &Value,
    request: &DeliberateRequest<'_>,
    principal: &Principal,
) -> Result<CognitionRecord, SpiError> {
    let mut record = draft
        .as_object()
        .cloned()
        .ok_or_else(|| invalid_verdict("the proposed record is not an object"))?;
    let Some(context_id) = request.assessment_context_id.or(request.subject_id) else {
        return Err(SpiError::InvalidInput(
            "the request names neither an assessment context nor a subject to derive it from"
                .to_string(),
        ));
    };
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true);
    let stamped = [
        ("record_id", Value::String(new_record_id(request)?)),
        ("holder", render_value(request.holder)?),
        ("target", render_value(request.target)?),
        (
            "assessor",
            json!({
                "kind": "principal",
                "namespace": "mindcore",
                "id": principal.principal_id.as_str(),
            }),
        ),
        (
            "assessment_context",
            json!({
                "context_id": context_id,
            }),
        ),
        ("assessed_at", Value::String(now.clone())),
        ("effective_at", Value::String(now)),
        ("view", Value::String("current".to_string())),
        ("extensions", Value::Object(Map::new())),
    ];
    for (field, value) in stamped {
        record.insert(field.to_string(), value);
    }
    let record = Value::Object(record);
    if let SpokeResult::Reject(reject) = validate_cognition_record(&record) {
        if request.assessment_context_id.is_some()
            && validates_with_derived_context(&record, request)
        {
            return Err(SpiError::InvalidInput(format!(
                "assessment_context_id is not a valid context: {}",
                reject.message
            )));
        }
        return Err(invalid_verdict(format!(
            "the proposed record was rejected ({}): {}",
            reject.code, reject.message
        )));
    }
    serde_json::from_value(record)
        .map_err(|error| invalid_verdict(format!("the proposed record does not decode: {error}")))
}

/// Whether the record validates with the assessment context derived from the
/// subject, as a request that supplied none would have stamped it. A subjectless
/// request derives nothing, so its supplied context never gets this fallback.
fn validates_with_derived_context(record: &Value, request: &DeliberateRequest<'_>) -> bool {
    let Some(subject_id) = request.subject_id else {
        return false;
    };
    let mut derived = record.clone();
    let Some(context) = derived
        .get_mut("assessment_context")
        .and_then(Value::as_object_mut)
    else {
        return false;
    };
    context.insert(
        "context_id".to_string(),
        Value::String(subject_id.to_string()),
    );
    !matches!(validate_cognition_record(&derived), SpokeResult::Reject(_))
}

/// A fresh record id: the workflow clock plus a digest of the trusted target
/// identity, unique within this carrier without a stored counter.
fn new_record_id(request: &DeliberateRequest<'_>) -> Result<String, SpiError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let digest = match jcs_sha256_hex(&json!({
        "space_id": request.space_id,
        "subject_id": request.subject_id,
        "holder": request.holder,
        "target": request.target,
    })) {
        SpokeResult::Ok(digest) => digest,
        SpokeResult::Reject(reject) => return Err(SpiError::InvalidInput(reject.message)),
    };
    Ok(format!("cog_{nanos:x}_{}", &digest[..16]))
}

fn render_value<T: Serialize>(value: &T) -> Result<Value, SpiError> {
    serde_json::to_value(value).map_err(|error| SpiError::InvalidInput(error.to_string()))
}

/// A definite stale-revision conflict: no effect committed, so a fresh attempt
/// over a fresh snapshot can succeed.
fn stale_revision(error: &SpiError) -> bool {
    matches!(
        error,
        SpiError::Settlement(reject)
            if matches!(
                reject.code,
                SpokeRejectCode::RevisionConflict | SpokeRejectCode::StoredRevisionStale
            )
    )
}

/// One validated settle-or-not decision over the prompted page, with the page
/// records it rests on.
enum AbsorbAction {
    Promote {
        cited: Vec<CitedRecord>,
    },
    Update {
        record_id: String,
        revision: u64,
        cited: Vec<CitedRecord>,
    },
    Dismiss {
        rationale: String,
        cited: Vec<CitedRecord>,
    },
}

impl AbsorbAction {
    /// The wire action the decision maps to.
    fn kind(&self) -> &'static str {
        match self {
            Self::Promote { .. } => "promote",
            Self::Update { .. } => "update",
            Self::Dismiss { .. } => "dismiss",
        }
    }

    /// The record an update replaces.
    fn target(&self) -> Option<&str> {
        match self {
            Self::Update { record_id, .. } => Some(record_id),
            _ => None,
        }
    }

    /// The validated reason a dismissal gives.
    fn rationale(&self) -> Option<&str> {
        match self {
            Self::Dismiss { rationale, .. } => Some(rationale),
            _ => None,
        }
    }

    /// The prompted-page records the decision cites.
    fn cited(&self) -> &[CitedRecord] {
        match self {
            Self::Promote { cited } | Self::Update { cited, .. } | Self::Dismiss { cited, .. } => {
                cited
            }
        }
    }
}

/// Ask the reasoner once and validate its reply against `records`.
async fn decide(
    reasoner: &dyn Reasoner,
    pending: &PendingCandidate,
    records: &[RetainedRecord],
    lens: Option<&dyn DomainLens>,
) -> Result<AbsorbAction, SpiError> {
    let admitted: Vec<Value> = records
        .iter()
        .map(|record| json!({ "record_id": record_id(record), "text": record_text(record) }))
        .collect();
    let payload = json!({
        "candidate": {
            "candidate_id": pending.candidate_id.to_string(),
            "kind": pending.proposed.kind,
            "text": pending.proposed.content.text,
        },
        "records": admitted,
    })
    .to_string();
    let schema = absorb_schema();
    let prompt = task_prompt(ABSORB_PROMPT, CognitiveTaskKind::Absorb, lens);
    let verdict = reasoner
        .reason_structured(StructuredReasonRequest {
            task_kind: CognitiveTaskKind::Absorb,
            system_prompt: &prompt,
            user_payload: &payload,
            output_schema: &schema,
        })
        .await?;
    absorb_action(&verdict, records)
}

/// Validate the closed decision shape and every cited id against the exact
/// page the prompt carried.
fn absorb_action(verdict: &Value, records: &[RetainedRecord]) -> Result<AbsorbAction, SpiError> {
    let object = verdict
        .as_object()
        .ok_or_else(|| invalid_verdict("the decision is not an object"))?;
    let action = object
        .get("action")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_verdict("the decision names no action"))?;
    let allowed: &[&str] = match action {
        "promote" => &["action", "evidence_ids"],
        "update" => &["action", "record_id", "evidence_ids"],
        "dismiss" => &["action", "rationale", "evidence_ids"],
        other => return Err(invalid_verdict(format!("{other} is not an absorb action"))),
    };
    if let Some(unexpected) = object
        .keys()
        .find(|member| !allowed.contains(&member.as_str()))
    {
        return Err(invalid_verdict(format!(
            "the decision carries {unexpected}"
        )));
    }
    let cited = cited_records(object, records)?;
    match action {
        "promote" => Ok(AbsorbAction::Promote { cited }),
        "update" => {
            let record_id = object
                .get("record_id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .ok_or_else(|| invalid_verdict("an update names no record"))?;
            let Some(revision) = records.iter().find_map(|record| match record {
                RetainedRecord::LiveRecord(record) if record.record_id.as_str() == record_id => {
                    Some(record.revision.get())
                }
                _ => None,
            }) else {
                return Err(invalid_verdict(format!(
                    "record {record_id} is not a live record of the prompted page"
                )));
            };
            if !cited.iter().any(|record| record.record_id == record_id) {
                return Err(invalid_verdict(
                    "an update does not cite the record it targets",
                ));
            }
            Ok(AbsorbAction::Update {
                record_id: record_id.to_string(),
                revision,
                cited,
            })
        }
        _ => {
            let rationale = object
                .get("rationale")
                .and_then(Value::as_str)
                .filter(|rationale| !rationale.trim().is_empty())
                .ok_or_else(|| invalid_verdict("a dismissal carries no rationale"))?;
            Ok(AbsorbAction::Dismiss {
                rationale: rationale.to_string(),
                cited,
            })
        }
    }
}

/// `evidence_ids`: unique nonempty ids, every one on the prompted page, each
/// carried with the revision that page served it at.
fn cited_records(
    object: &Map<String, Value>,
    records: &[RetainedRecord],
) -> Result<Vec<CitedRecord>, SpiError> {
    let ids = object
        .get("evidence_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_verdict("the decision names no evidence_ids array"))?;
    let mut cited: Vec<CitedRecord> = Vec::with_capacity(ids.len());
    for id in ids {
        let id = id
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| invalid_verdict("an evidence id is not a nonempty string"))?;
        let Some(record) = records.iter().find(|record| record_id(record) == id) else {
            return Err(invalid_verdict(format!(
                "evidence id {id} is not in the prompted page"
            )));
        };
        if cited.iter().any(|cited| cited.record_id == id) {
            return Err(invalid_verdict(format!("evidence id {id} appears twice")));
        }
        cited.push(CitedRecord {
            record_id: id.to_string(),
            revision: record_revision(record),
        });
    }
    Ok(cited)
}

fn invalid_verdict(message: impl Into<String>) -> SpiError {
    SpiError::InvalidReasonerOutput(message.into())
}

/// The policy rejected the proposal: the candidate is dropped through the same
/// review operation, with the policy's reason as its rationale.
///
/// The refusal is the policy's, not the cited page's — evidence that moved
/// cannot keep a refused candidate alive. The citations are still revalidated,
/// because the drop consumes the candidate: a consumption write never rests on
/// evidence that changed or lost its grant after the page was served.
#[allow(clippy::too_many_arguments)]
async fn drop_candidate(
    binding: &dyn WorkflowBinding,
    principal: &Principal,
    space: &str,
    candidate_id: &str,
    expected_revision: u64,
    review_key: &str,
    reason: String,
    cited: &[CitedRecord],
) -> Result<AbsorbCandidateOutcome, SpiError> {
    let review = review_request(
        space,
        candidate_id,
        expected_revision,
        review_key,
        json!({ "action": "drop", "rationale": reason }),
    )?;
    binding.review_candidate(principal, &review, cited).await?;
    Ok(AbsorbCandidateOutcome::Dropped {
        candidate_id: candidate_id.to_string(),
    })
}

/// A stable operation key for one absorb leg, bounded to the wire's
/// 128-character idempotency limit whatever the capture key's own length.
fn absorb_key(capture_key: &str, candidate_id: &str, leg: &str) -> Result<String, SpiError> {
    match jcs_sha256_hex(&json!({
        "capture_key": capture_key,
        "candidate_id": candidate_id,
        "leg": leg,
    })) {
        SpokeResult::Ok(digest) => Ok(format!("absorb-{digest}")),
        SpokeResult::Reject(reject) => Err(SpiError::InvalidInput(reject.message)),
    }
}

fn review_request(
    space: &str,
    candidate_id: &str,
    expected_revision: u64,
    idempotency_key: &str,
    decision: Value,
) -> Result<MemoryCandidatesReviewRequest, SpiError> {
    serde_json::from_value(json!({
        "space_id": space,
        "candidate_id": candidate_id,
        "expected_revision": expected_revision,
        "idempotency_key": idempotency_key,
        "decision": decision,
    }))
    .map_err(|error| SpiError::InvalidInput(format!("absorb review request: {error}")))
}

fn update_request(
    space: &str,
    record_id: &str,
    expected_revision: u64,
    idempotency_key: &str,
    content: Value,
) -> Result<MemoryRecordsUpdateRequest, SpiError> {
    serde_json::from_value(json!({
        "space_id": space,
        "record_id": record_id,
        "expected_revision": expected_revision,
        "idempotency_key": idempotency_key,
        "content": content,
    }))
    .map_err(|error| SpiError::InvalidInput(format!("absorb update request: {error}")))
}

fn dismiss_request(
    space: &str,
    candidate_id: &str,
    expected_revision: u64,
    idempotency_key: &str,
) -> Result<MemoryCandidatesDismissRequest, SpiError> {
    serde_json::from_value(json!({
        "space_id": space,
        "candidate_id": candidate_id,
        "expected_revision": expected_revision,
        "idempotency_key": idempotency_key,
        "reason": "the candidate updated an existing record",
    }))
    .map_err(|error| SpiError::InvalidInput(format!("absorb dismiss request: {error}")))
}

/// The page facts of `records`, in page order: two reads that agree on them
/// served the same page.
pub fn page_fact(records: &[RetainedRecord]) -> Vec<PageFact> {
    records
        .iter()
        .map(|record| (record_id(record).to_string(), record_revision(record)))
        .collect()
}

/// The facts of one authorized recall read: the page and the knowledge set.
pub fn snapshot_fact(records: &[RetainedRecord], knowledge: &[KnowledgeEntry]) -> SnapshotFact {
    (
        page_fact(records),
        knowledge
            .iter()
            .map(|entry| (entry.entry_id.clone(), entry.revision))
            .collect(),
    )
}

fn revision_conflict() -> SpiError {
    let mut details = Map::new();
    details.insert("retry_class".to_string(), Value::String("retryable".into()));
    SpiError::Settlement(SpokeReject {
        code: SpokeRejectCode::RevisionConflict,
        message: "the authorized snapshot changed while provider work ran; retry the recall"
            .to_string(),
        details: Some(details),
    })
}

/// One ranked page and what the caller may keep once it reauthorized the page.
struct RankedRecall {
    /// The page in release order.
    records: Vec<RetainedRecord>,
    /// Record vectors the release boundary writes in its own transaction.
    fills: Vec<RecordFill>,
    /// The query vector this recall embedded, validated against the vector
    /// snapshot, for the caller to install only after final reauthorization.
    query: Option<QuerySlot>,
}

impl RankedRecall {
    /// The adapter's lexical order, with nothing to cache or fill.
    fn lexical(records: Vec<RetainedRecord>) -> Self {
        Self {
            records,
            fills: Vec::new(),
            query: None,
        }
    }
}

impl SpiMindWorkflows {
    /// Hybrid rerank of the page, or the adapter order when it cannot run.
    ///
    /// The page is never widened: an empty query, an embedder without a serving
    /// identity or `EmbeddingsUnavailable` all preserve the lexical order the
    /// adapter returned, and every other provider failure propagates.
    ///
    /// The query vector is resolved first — from the one process-local slot or
    /// the provider — before any persisted record is probed. The stored vectors
    /// then come from one snapshot that rebuilds admission and withholding and
    /// refuses a page that moved before probing a row, only missing or stale
    /// revisions are embedded, and the freshly embedded ones come back as fills
    /// the caller writes after it reauthorized the page. A freshly embedded
    /// query vector comes back for the caller to install the same way.
    async fn rank(
        &self,
        principal: &Principal,
        request: &MemoryRecallRequest,
        records: Vec<RetainedRecord>,
        caps: &MindCapabilities<'_>,
    ) -> Result<RankedRecall, SpiError> {
        let Some(query) = request
            .query
            .as_deref()
            .map(str::trim)
            .filter(|query| !query.is_empty())
        else {
            return Ok(RankedRecall::lexical(records));
        };
        let Some(embedder) = caps.embedder else {
            return Ok(RankedRecall::lexical(records));
        };
        let Some(identity) = embedder.identity() else {
            return Ok(RankedRecall::lexical(records));
        };
        if records.is_empty() {
            return Ok(RankedRecall::lexical(records));
        }

        let scope = (
            principal.principal_id.as_str().to_string(),
            request.space_id.as_str().to_string(),
            request.subject_id.as_str().to_string(),
            query.to_string(),
        );
        let (query_vector, fresh_query) = match self.cached_query(&scope, identity) {
            Some(vector) => (vector, None),
            None => {
                let batch = [query.to_string()];
                let vectors = match embedder.embed_batch(&batch).await {
                    Ok(vectors) => vectors,
                    Err(SpiError::EmbeddingsUnavailable) => {
                        return Ok(RankedRecall::lexical(records))
                    }
                    Err(error) => return Err(error),
                };
                let vector = checked_vectors(vectors, 1, identity)?
                    .pop()
                    .expect("one text yields one vector");
                let slot = QuerySlot {
                    scope,
                    identity: identity.clone(),
                    vector: vector.clone(),
                };
                (vector, Some(slot))
            }
        };

        let expected = page_fact(&records);
        let page = self
            .binding
            .recall_vectors(principal, request, identity, &expected)
            .await?;
        if page
            .iter()
            .map(|row| {
                (
                    record_id(&row.record).to_string(),
                    record_revision(&row.record),
                )
            })
            .ne(expected.iter().cloned())
        {
            return Err(revision_conflict());
        }

        let mut records = Vec::with_capacity(page.len());
        let mut stored = Vec::with_capacity(page.len());
        for row in page {
            records.push(row.record);
            stored.push(row.vector);
        }
        let texts: Vec<&str> = records.iter().map(record_text).collect();
        let misses: Vec<usize> = stored
            .iter()
            .enumerate()
            .filter_map(|(index, vector)| {
                (vector.is_none() && matches!(records[index], RetainedRecord::LiveRecord(_)))
                    .then_some(index)
            })
            .collect();
        let mut fresh = Vec::new();
        if !misses.is_empty() {
            let batch: Vec<String> = misses
                .iter()
                .map(|index| texts[*index].to_string())
                .collect();
            let vectors = match embedder.embed_batch(&batch).await {
                Ok(vectors) => vectors,
                Err(SpiError::EmbeddingsUnavailable) => return Ok(RankedRecall::lexical(records)),
                Err(error) => return Err(error),
            };
            fresh = checked_vectors(vectors, misses.len(), identity)?;
        }

        let mut vectors: Vec<Option<Vec<f32>>> = Vec::with_capacity(texts.len());
        let mut fills = Vec::with_capacity(misses.len());
        let mut fresh = fresh.into_iter();
        for (index, stored) in stored.into_iter().enumerate() {
            let vector = match stored {
                Some(vector) => Some(vector),
                None => match &records[index] {
                    RetainedRecord::LiveRecord(live) => {
                        let vector = fresh.next().expect("one vector per live miss");
                        fills.push(RecordFill {
                            record: SettledRecord {
                                record_id: live.record_id.as_str().to_string(),
                                revision: live.revision.get(),
                                text: live.content.text.clone(),
                            },
                            vector: vector.clone(),
                        });
                        Some(vector)
                    }
                    RetainedRecord::TombstonedRecord(_) => None,
                },
            };
            vectors.push(vector);
        }

        let scores = hybrid_scores(&texts, query, &query_vector, &vectors);
        let mut order: Vec<usize> = (0..records.len()).collect();
        order.sort_by(|&left, &right| scores[right].total_cmp(&scores[left]));
        let mut records = reorder(records, &order);
        if let Some(reasoner) = caps.reasoner {
            let permutation = reasoner_permutation(reasoner, query, &records).await?;
            records = reorder(records, &permutation);
        }
        Ok(RankedRecall {
            records,
            fills,
            query: fresh_query,
        })
    }
}

fn checked_vectors(
    vectors: Vec<Vec<f32>>,
    expected: usize,
    identity: &EmbeddingIdentity,
) -> Result<Vec<Vec<f32>>, SpiError> {
    if vectors.len() != expected {
        return Err(SpiError::EmbeddingFailed(format!(
            "provider returned {} vectors for {expected} texts",
            vectors.len()
        )));
    }
    for vector in &vectors {
        if vector.len() != identity.dim {
            return Err(SpiError::EmbeddingFailed(format!(
                "provider returned dimension {} for a {}-dimension identity",
                vector.len(),
                identity.dim
            )));
        }
        if vector.iter().any(|value| !value.is_finite()) {
            return Err(SpiError::EmbeddingFailed(
                "provider returned a non-finite component".to_string(),
            ));
        }
        if vector.iter().all(|value| *value == 0.0) {
            return Err(SpiError::EmbeddingFailed(
                "provider returned a zero vector".to_string(),
            ));
        }
    }
    Ok(vectors)
}

/// `0.5 × normalised_BM25 + 0.5 × (1 + cosine)/2` over the page only.
///
/// This is the page ranking scorer, not a reproduction of a pack's order: the
/// engine sorts by it descending and keeps the adapter's order on ties, but a
/// configured reasoner may then reorder the page.
///
/// One score per text. `None` is a text with no serving vector — a tombstone —
/// and scores with `0` similarity, whose term still contributes `0.25`;
/// mismatched slices never panic, so pass one entry per text for the documented
/// formula.
pub(crate) fn hybrid_scores(
    texts: &[&str],
    query: &str,
    query_vector: &[f32],
    vectors: &[Option<Vec<f32>>],
) -> Vec<f64> {
    let lexical = bm25_scores(texts, query);
    let maximum = lexical.iter().copied().fold(0.0f64, f64::max);
    texts
        .iter()
        .enumerate()
        .map(|(index, _)| {
            let normalised = if maximum > 0.0 {
                lexical[index] / maximum
            } else {
                0.0
            };
            let similarity = vectors
                .get(index)
                .and_then(Option::as_deref)
                .map_or(0.0, |vector| cosine(query_vector, vector));
            0.5 * normalised + 0.5 * (1.0 + similarity) / 2.0
        })
        .collect()
}

fn bm25_scores(texts: &[&str], query: &str) -> Vec<f64> {
    let terms = query_terms(query);
    let documents: Vec<Vec<String>> = texts.iter().map(|text| terms_of(text)).collect();
    let count = documents.len() as f64;
    let average = documents.iter().map(|doc| doc.len() as f64).sum::<f64>() / count;
    let mut scores = vec![0.0f64; documents.len()];
    if average == 0.0 {
        return scores;
    }
    for term in &terms {
        let matching = documents.iter().filter(|doc| doc.contains(term)).count() as f64;
        let idf = (1.0 + (count - matching + 0.5) / (matching + 0.5)).ln();
        for (index, document) in documents.iter().enumerate() {
            let frequency = document.iter().filter(|token| *token == term).count() as f64;
            if frequency == 0.0 {
                continue;
            }
            let length = document.len() as f64;
            let denominator = frequency + BM25_K1 * (1.0 - BM25_B + BM25_B * length / average);
            scores[index] += idf * frequency * (BM25_K1 + 1.0) / denominator;
        }
    }
    scores
}

fn query_terms(query: &str) -> Vec<String> {
    let mut terms: Vec<String> = Vec::new();
    for term in query.split(|character: char| !character.is_alphanumeric()) {
        if term.is_empty() {
            continue;
        }
        let term = term.to_lowercase();
        if !terms.contains(&term) {
            terms.push(term);
        }
    }
    terms
}

fn terms_of(text: &str) -> Vec<String> {
    text.split(|character: char| !character.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(str::to_lowercase)
        .collect()
}

fn cosine(left: &[f32], right: &[f32]) -> f64 {
    let mut dot = 0.0f64;
    let mut left_norm = 0.0f64;
    let mut right_norm = 0.0f64;
    for (left, right) in left.iter().zip(right) {
        let left = f64::from(*left);
        let right = f64::from(*right);
        dot += left * right;
        left_norm += left * left;
        right_norm += right * right;
    }
    (dot / (left_norm.sqrt() * right_norm.sqrt())).clamp(-1.0, 1.0)
}

fn reorder(records: Vec<RetainedRecord>, order: &[usize]) -> Vec<RetainedRecord> {
    let mut slots: Vec<Option<RetainedRecord>> = records.into_iter().map(Some).collect();
    order
        .iter()
        .map(|&index| slots[index].take().expect("a permutation of the same page"))
        .collect()
}

/// The reasoner may only reorder the admitted page: every id exactly once.
async fn reasoner_permutation(
    reasoner: &dyn Reasoner,
    query: &str,
    records: &[RetainedRecord],
) -> Result<Vec<usize>, SpiError> {
    let admitted: Vec<Value> = records
        .iter()
        .map(|record| json!({ "record_id": record_id(record), "text": record_text(record) }))
        .collect();
    let payload = json!({ "query": query, "records": admitted }).to_string();
    let schema = json!({
        "type": "object",
        "properties": { "record_ids": { "type": "array", "items": { "type": "string" } } },
        "required": ["record_ids"],
        "additionalProperties": false,
    });
    let verdict = reasoner
        .reason_structured(StructuredReasonRequest {
            task_kind: CognitiveTaskKind::Rerank,
            system_prompt: RERANK_PROMPT,
            user_payload: &payload,
            output_schema: &schema,
        })
        .await?;
    let ids = verdict
        .get("record_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            SpiError::InvalidReasonerOutput("record_ids must be an array".to_string())
        })?;
    let positions: HashMap<&str, usize> = records
        .iter()
        .enumerate()
        .map(|(index, record)| (record_id(record), index))
        .collect();
    let mut seen = HashSet::new();
    let mut permutation = Vec::with_capacity(ids.len());
    for id in ids {
        let id = id.as_str().ok_or_else(|| {
            SpiError::InvalidReasonerOutput("record_ids must name strings".to_string())
        })?;
        if !seen.insert(id) {
            return Err(SpiError::InvalidReasonerOutput(format!(
                "record id {id} appears twice"
            )));
        }
        let Some(&position) = positions.get(id) else {
            return Err(SpiError::InvalidReasonerOutput(format!(
                "record id {id} is not in the admitted page"
            )));
        };
        permutation.push(position);
    }
    if permutation.len() != records.len() {
        return Err(SpiError::InvalidReasonerOutput(
            "record_ids is not a permutation of the admitted page".to_string(),
        ));
    }
    Ok(permutation)
}

/// Whole items only, memory records first, then knowledge entries, each in its
/// own order; one token is charged per UTF-8 byte of the serialized item and
/// an item that does not fit is skipped rather than cut.
fn pack(
    records: Vec<RetainedRecord>,
    knowledge: Vec<KnowledgeEntry>,
    max_tokens: NonZeroUsize,
) -> Result<ContextPack, SpiError> {
    let budget = max_tokens.get();
    let mut tokens_used = 0usize;
    let mut packed_records = Vec::new();
    for record in records {
        let size = serialized_len(&record)?;
        if size > budget.saturating_sub(tokens_used) {
            continue;
        }
        tokens_used += size;
        packed_records.push(record);
    }
    let mut packed_knowledge = Vec::new();
    for entry in knowledge {
        let size = serialized_len(&entry)?;
        if size > budget.saturating_sub(tokens_used) {
            continue;
        }
        tokens_used += size;
        packed_knowledge.push(entry);
    }
    Ok(ContextPack {
        records: packed_records,
        knowledge: packed_knowledge,
        tokens_used,
    })
}

fn serialized_len<T: Serialize>(item: &T) -> Result<usize, SpiError> {
    serde_json::to_string(item)
        .map(|serialized| serialized.len())
        .map_err(|error| SpiError::InvalidInput(error.to_string()))
}

fn record_id(record: &RetainedRecord) -> &str {
    match record {
        RetainedRecord::LiveRecord(record) => record.record_id.as_str(),
        RetainedRecord::TombstonedRecord(record) => record.record_id.as_str(),
    }
}

fn record_revision(record: &RetainedRecord) -> u64 {
    match record {
        RetainedRecord::LiveRecord(record) => record.revision.get(),
        RetainedRecord::TombstonedRecord(record) => record.revision.get(),
    }
}

fn record_text(record: &RetainedRecord) -> &str {
    match record {
        RetainedRecord::LiveRecord(record) => record.content.text.as_str(),
        RetainedRecord::TombstonedRecord(record) => record.content.text.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An absurd timeout is clamped, never dropped: the deadline stays a real
    /// future instant, so every phase keeps its bound.
    #[test]
    fn an_absurd_timeout_still_yields_a_deadline() {
        let deadline = deadline_after(Duration::MAX);
        let now = Instant::now();
        assert!(deadline > now, "an absurd timeout must keep a deadline");
        assert!(
            deadline.saturating_duration_since(now) <= MAX_TIMEOUT,
            "the deadline must be clamped, not unbounded"
        );
    }

    /// The page read refuses before any provider work; every later leg would
    /// be a bug if it ran.
    struct RefusingBinding;

    #[async_trait]
    impl WorkflowBinding for RefusingBinding {
        async fn recall_snapshot(
            &self,
            _principal: &Principal,
            _request: &MemoryRecallRequest,
        ) -> Result<RecallSnapshot, SpiError> {
            Err(SpiError::Settlement(SpokeReject {
                code: SpokeRejectCode::NotAuthorized,
                message: "subject is withheld".to_string(),
                details: None,
            }))
        }

        async fn recall_vectors(
            &self,
            _principal: &Principal,
            _request: &MemoryRecallRequest,
            _identity: &EmbeddingIdentity,
            _expected: &[PageFact],
        ) -> Result<PageVectors, SpiError> {
            unreachable!("a refused page never reaches the vector snapshot")
        }

        async fn settle_recall(
            &self,
            _principal: &Principal,
            _request: &MemoryRecallRequest,
            _identity: Option<&EmbeddingIdentity>,
            _expected: &SnapshotFact,
            _fills: &[RecordFill],
        ) -> Result<(), SpiError> {
            unreachable!("a refused page never reaches the release boundary")
        }

        async fn capture(
            &self,
            _principal: &Principal,
            _request: &MemoryCaptureRequest,
        ) -> Result<CaptureOutcome, SpiError> {
            unreachable!("this double serves recall only")
        }

        async fn record_absorb_pending(
            &self,
            _principal: &Principal,
            _request: &MemoryCaptureRequest,
            _pending_candidate_ids: &[&str],
        ) -> Result<(), SpiError> {
            unreachable!("this double serves recall only")
        }

        async fn absorb_snapshot(
            &self,
            _principal: &Principal,
            _space_id: &str,
            _candidate_id: &str,
            _max_items: u64,
        ) -> Result<AbsorbSnapshot, SpiError> {
            unreachable!("this double serves recall only")
        }

        async fn review_candidate(
            &self,
            _principal: &Principal,
            _request: &MemoryCandidatesReviewRequest,
            _cited: &[CitedRecord],
        ) -> Result<Vec<SettledRevision>, SpiError> {
            unreachable!("this double serves recall only")
        }

        async fn update_candidate(
            &self,
            _principal: &Principal,
            _space_id: &str,
            _update: &MemoryRecordsUpdateRequest,
            _dismiss: &MemoryCandidatesDismissRequest,
            _cited: &[CitedRecord],
        ) -> Result<Vec<SettledRevision>, SpiError> {
            unreachable!("this double serves recall only")
        }

        async fn settled_record(
            &self,
            _principal: &Principal,
            _space_id: &str,
            _record_id: &str,
        ) -> Result<Option<SettledRecord>, SpiError> {
            unreachable!("this double serves recall only")
        }

        async fn fill_record_vectors(
            &self,
            _principal: &Principal,
            _space_id: &str,
            _identity: &EmbeddingIdentity,
            _fills: &[RecordFill],
        ) -> Result<(), SpiError> {
            unreachable!("a refused page never reaches the warm")
        }

        async fn recorded_update(
            &self,
            _principal: &Principal,
            _space_id: &str,
            _idempotency_key: &str,
        ) -> Result<Option<String>, SpiError> {
            unreachable!("this double serves recall only")
        }

        async fn deliberate_snapshot(
            &self,
            _principal: &Principal,
            _space_id: &str,
            _subject_id: Option<&str>,
            _holder: &CognitionReference,
            _max_items: u64,
        ) -> Result<DeliberateSnapshot, SpiError> {
            unreachable!("this double serves recall only")
        }

        async fn commit_cognition(
            &self,
            _principal: &Principal,
            _space_id: &str,
            _subject_id: Option<&str>,
            _holder: &CognitionReference,
            _expected_carrier_revision: u64,
            _record: Value,
            _cited: &[CitedRecord],
            _deadline: Instant,
        ) -> Result<u64, SpiError> {
            unreachable!("this double serves recall only")
        }
    }

    struct StubEmbedder(EmbeddingIdentity);

    #[async_trait]
    impl Embedder for StubEmbedder {
        fn identity(&self) -> Option<&EmbeddingIdentity> {
            Some(&self.0)
        }

        async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, SpiError> {
            Ok(texts.iter().map(|_| vec![1.0, 0.0]).collect())
        }
    }

    /// A refused page is decided by the read alone: it never consults the warm
    /// query slot, which survives for a later authorized recall.
    #[tokio::test]
    async fn a_refused_page_never_probes_the_query_slot() {
        let workflows = SpiMindWorkflows::new(RefusingBinding);
        let identity = EmbeddingIdentity {
            provider_id: "provider".to_string(),
            model_id: "model".to_string(),
            model_version: "version".to_string(),
            dim: 2,
        };
        workflows.install_query(QuerySlot {
            scope: (
                "prn_1".to_string(),
                "spc_1".to_string(),
                "subj_1".to_string(),
                "north gate".to_string(),
            ),
            identity: identity.clone(),
            vector: vec![1.0, 0.0],
        });
        let embedder = StubEmbedder(identity);
        let caps = MindCapabilities {
            embedder: Some(&embedder),
            ..MindCapabilities::default()
        };
        let principal: Principal =
            serde_json::from_value(json!({ "principal_id": "prn_1", "extensions": {} }))
                .expect("principal");
        let request: MemoryRecallRequest = serde_json::from_value(json!({
            "space_id": "spc_1",
            "subject_id": "subj_1",
            "selection": { "mode": "shared_only" },
            "budget": { "max_items": 10 },
            "query": "north gate",
        }))
        .expect("recall request");

        let error = workflows
            .recall_context(
                &principal,
                &request,
                NonZeroUsize::new(64).expect("budget"),
                &caps,
            )
            .await
            .expect_err("a refused page must not release a pack");

        let SpiError::Settlement(reject) = error else {
            panic!("expected a settlement refusal, got {error:?}");
        };
        assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
        assert_eq!(
            workflows.query_slot_probes(),
            0,
            "a refused page never looks the query slot up"
        );
    }

    /// A caller that passes fewer vectors than texts gets a defined score
    /// instead of a panic.
    #[test]
    fn a_short_vector_slice_scores_without_panicking() {
        let scores = hybrid_scores(
            &["north gate", "market dawn"],
            "north gate",
            &[1.0, 0.0],
            &[Some(vec![1.0, 0.0])],
        );
        assert_eq!(scores.len(), 2);
        assert!(scores.iter().all(|score| score.is_finite()));
    }
}
