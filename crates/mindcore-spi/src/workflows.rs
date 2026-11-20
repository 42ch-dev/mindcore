//! The MIND workflow engine and its one DTO-only storage seam.
//!
//! [`WorkflowBinding`] keeps storage below this crate: it runs `memory.recall`,
//! the scoped knowledge read and the capture/review/update operations on the
//! caller's own transactions and returns DTOs. [`SpiMindWorkflows`] only
//! ranks, validates and budgets that snapshot, so embedder, reasoner and
//! policy calls never run inside a transaction.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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

use crate::{
    AbsorbCandidateOutcome, CognitiveTaskKind, ContextPack, DefaultPolicy, DeliberateOutcome,
    DeliberateRequest, DomainLens, EmbeddingIdentity, Evidence, LensOutput, MindCapabilities,
    MindWorkflows, PolicyDecision, PolicyHook, ProposalDraft, Reasoner, Recommendation,
    SettlementDraft, SpiError, StructuredReasonRequest,
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
    /// its receipt; returns the record ids the review created.
    ///
    /// Every cited record is revalidated in the same staging transaction: one
    /// that changed, disappeared, became withheld or lost its admission
    /// refuses the decision before anything is staged.
    async fn review_candidate(
        &self,
        principal: &Principal,
        request: &MemoryCandidatesReviewRequest,
        cited: &[CitedRecord],
    ) -> Result<Vec<String>, SpiError>;

    /// Stage `memory.records.update` and `memory.candidates.dismiss` in one
    /// transaction, publish both intents' targets and settle both receipts.
    ///
    /// `cited` is revalidated in that same staging transaction first.
    async fn update_candidate(
        &self,
        principal: &Principal,
        space_id: &str,
        update: &MemoryRecordsUpdateRequest,
        dismiss: &MemoryCandidatesDismissRequest,
        cited: &[CitedRecord],
    ) -> Result<(), SpiError>;

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

    /// Read one deliberation snapshot: the subject's authorized memory page
    /// and the caller's admitted holder carrier, on one transaction.
    ///
    /// The holder must name the existing carrier entry of the caller's bound
    /// knowledge collection. A memory grant is not an admission to the
    /// carrier; only the binding's knowledge admission is.
    async fn deliberate_snapshot(
        &self,
        principal: &Principal,
        space_id: &str,
        subject_id: &str,
        holder: &CognitionReference,
        max_items: u64,
    ) -> Result<DeliberateSnapshot, SpiError>;

    /// Append `record` to the holder carrier and persist the replacement under
    /// one caller-managed write transaction.
    ///
    /// Rechecks the caller's admission to *write* the carrier (a read grant is
    /// not one), the subject's recall admission, its withholding and every
    /// cited record revision against `cited`, and refuses a carrier that no
    /// longer holds `expected_carrier_revision`. Returns the carrier revision
    /// of the derived `MindState` after a confirmed commit.
    async fn commit_cognition(
        &self,
        principal: &Principal,
        space_id: &str,
        subject_id: &str,
        holder: &CognitionReference,
        expected_carrier_revision: u64,
        record: Value,
        cited: &[CitedRecord],
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
    /// The candidate ids the capture receipt holds.
    pub candidate_ids: Vec<String>,
    /// The capture key already held a receipt, so no candidate here is fresh.
    pub replayed: bool,
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
}

impl SpiMindWorkflows {
    /// Build the engine over `binding`.
    pub fn new(binding: impl WorkflowBinding + 'static) -> Self {
        Self {
            binding: Box::new(binding),
        }
    }

    /// Pack an authorized context pack under `max_tokens`.
    ///
    /// One recall page is read, reranked by the injected capabilities, then
    /// revalidated against the same read path before anything is released: a
    /// refused reread propagates and a changed page returns
    /// `REVISION_CONFLICT`, so no record revision or grant that moved during
    /// provider work reaches the pack.
    pub async fn recall_context(
        &self,
        principal: &Principal,
        request: &MemoryRecallRequest,
        max_tokens: NonZeroUsize,
        caps: &MindCapabilities<'_>,
    ) -> Result<ContextPack, SpiError> {
        let RecallSnapshot { records, knowledge } =
            self.binding.recall_snapshot(principal, request).await?;
        let before = snapshot_fact(&records, &knowledge);
        let ranked = rank(records, request.query.as_deref(), caps).await?;
        let released = self.binding.recall_snapshot(principal, request).await?;
        if snapshot_fact(&released.records, &released.knowledge) != before {
            return Err(revision_conflict());
        }
        pack(ranked, knowledge, max_tokens)
    }

    /// Decide what each captured candidate becomes.
    ///
    /// The capture runs the existing admission, idempotency probe and
    /// deterministic classifier. A candidate an earlier run already consumed
    /// reports that run's recorded outcome, with or without a reasoner. A
    /// replay — the capture key already held a receipt — reports `Pending` for
    /// a candidate that is still pending: the recorded run did not settle it,
    /// and a replay must never settle what it held. Without a reasoner every
    /// fresh candidate stays pending and no review is written. With one, each
    /// fresh candidate is read with one bounded authorized evidence page, the
    /// reasoner runs outside any transaction, its reply is validated against
    /// that exact page, and the policy hook decides whether the validated
    /// settlement is submitted.
    pub async fn absorb(
        &self,
        principal: &Principal,
        request: &MemoryCaptureRequest,
        lens: Option<&dyn DomainLens>,
        caps: &MindCapabilities<'_>,
    ) -> Result<Vec<AbsorbCandidateOutcome>, SpiError> {
        let CaptureOutcome {
            candidate_ids,
            replayed,
        } = self.binding.capture(principal, request).await?;
        let mut outcomes = Vec::with_capacity(candidate_ids.len());
        for candidate_id in candidate_ids {
            outcomes.push(
                self.absorb_candidate(
                    principal,
                    request,
                    candidate_id,
                    !replayed,
                    caps.reasoner,
                    lens,
                    caps,
                )
                .await?,
            );
        }
        Ok(outcomes)
    }

    #[allow(clippy::too_many_arguments)]
    async fn absorb_candidate(
        &self,
        principal: &Principal,
        request: &MemoryCaptureRequest,
        candidate_id: String,
        fresh: bool,
        reasoner: Option<&dyn Reasoner>,
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
            // A replay of an earlier absorb reports what that run recorded.
            MemoryCandidate::ConsumedCandidate(consumed) => {
                return self
                    .recorded_outcome(principal, &space, &capture_key, consumed)
                    .await
            }
        };
        // The recorded run left this candidate pending — a policy hold, or no
        // reasoner at all. Deciding again would let a replay settle it.
        if !fresh {
            return Ok(AbsorbCandidateOutcome::Pending { candidate_id });
        }
        let Some(reasoner) = reasoner else {
            return Ok(AbsorbCandidateOutcome::Pending { candidate_id });
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
                        let record_ids = self
                            .binding
                            .review_candidate(principal, &review, &cited)
                            .await?;
                        Ok(AbsorbCandidateOutcome::Settled {
                            candidate_id,
                            record_ids,
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
                        self.binding
                            .update_candidate(principal, &space, &update, &dismiss, &cited)
                            .await?;
                        Ok(AbsorbCandidateOutcome::Settled {
                            candidate_id,
                            record_ids: vec![record_id],
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
    /// one write transaction. Every pre-commit phase checks the one deadline,
    /// so a phase that returns after it never starts the write. Only a definite
    /// stale-revision conflict starts another attempt — at most twice, and
    /// never after the request deadline.
    pub async fn deliberate(
        &self,
        principal: &Principal,
        request: DeliberateRequest<'_>,
        lens: Option<&dyn DomainLens>,
        caps: &MindCapabilities<'_>,
    ) -> Result<DeliberateOutcome, SpiError> {
        let Some(reasoner) = caps.reasoner else {
            return Err(SpiError::ReasonerUnavailable);
        };
        let deadline = Instant::now().checked_add(request.timeout);
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
        deadline: Option<Instant>,
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
        let verdict = reasoner
            .reason_structured(StructuredReasonRequest {
                task_kind: CognitiveTaskKind::Deliberate,
                system_prompt: &prompt,
                user_payload: &payload,
                output_schema: &schema,
            })
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
            // Both recommendation kinds reach the lens: a domain that refuses a
            // `none` keeps its redline even when nothing would be written.
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
            // `none`: the lens was consulted, but no policy and no write follow.
            return Ok(DeliberateOutcome::NoChange { verdict });
        };
        let fallback = DefaultPolicy;
        let policy: &dyn PolicyHook = caps.policy_hook.unwrap_or(&fallback);
        match policy
            .evaluate_settlement(SettlementDraft::Cognition { proposed: &record })
            .await?
        {
            PolicyDecision::AutoSettle => {}
            PolicyDecision::RequireHumanReview { .. } => {
                return Ok(DeliberateOutcome::NeedsReview {
                    verdict,
                    draft: record,
                })
            }
            PolicyDecision::Reject { reason } => return Err(SpiError::PolicyRejected(reason)),
        }
        // Last point before the write begins: after it, an unknown outcome is
        // reported as one rather than flattened into a deadline refusal.
        ensure_time_left(deadline)?;
        let mind_state_version = self
            .binding
            .commit_cognition(
                principal,
                request.space_id,
                request.subject_id,
                request.holder,
                snapshot.carrier_revision,
                serde_json::to_value(&record)
                    .map_err(|error| SpiError::InvalidInput(error.to_string()))?,
                &cited,
            )
            .await?;
        Ok(DeliberateOutcome::Settled {
            verdict,
            record,
            mind_state_version,
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
const MEMORY_RECORD_IDENTITY: (&str, &str) = ("memory_record", "mindcore");

/// The request's single deadline: no phase starts once it elapsed, so a
/// reasoner, lens or policy that returns late never starts the carrier write.
/// An outcome already in flight is never reported as a deadline refusal.
fn ensure_time_left(deadline: Option<Instant>) -> Result<(), SpiError> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(SpiError::DeadlineExceeded);
    }
    Ok(())
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
            // A carrier record needs no revision check of its own: the
            // carrier CAS covers every change to it.
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
            json!({ "context_id": request.subject_id }),
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
        return Err(invalid_verdict(format!(
            "the proposed record was rejected ({}): {}",
            reject.code, reject.message
        )));
    }
    serde_json::from_value(record)
        .map_err(|error| invalid_verdict(format!("the proposed record does not decode: {error}")))
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

/// What decides which items a snapshot holds: each item's identity and
/// revision, in page order.
type SnapshotFact = (Vec<(String, u64)>, Vec<(String, Option<u64>)>);

fn snapshot_fact(records: &[RetainedRecord], knowledge: &[KnowledgeEntry]) -> SnapshotFact {
    (
        records
            .iter()
            .map(|record| (record_id(record).to_string(), record_revision(record)))
            .collect(),
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

/// Hybrid rerank of the page, or the adapter order when it cannot run.
///
/// The page is never widened: an empty query, an embedder without a serving
/// identity or `EmbeddingsUnavailable` all preserve the lexical order the
/// adapter returned, and every other provider failure propagates.
async fn rank(
    records: Vec<RetainedRecord>,
    query: Option<&str>,
    caps: &MindCapabilities<'_>,
) -> Result<Vec<RetainedRecord>, SpiError> {
    let Some(query) = query.map(str::trim).filter(|query| !query.is_empty()) else {
        return Ok(records);
    };
    let Some(embedder) = caps.embedder else {
        return Ok(records);
    };
    let Some(identity) = embedder.identity() else {
        return Ok(records);
    };
    if records.is_empty() {
        return Ok(records);
    }

    let texts: Vec<&str> = records.iter().map(record_text).collect();
    let mut batch = Vec::with_capacity(texts.len() + 1);
    batch.push(query.to_string());
    batch.extend(texts.iter().map(|text| (*text).to_string()));
    let vectors = match embedder.embed_batch(&batch).await {
        Ok(vectors) => vectors,
        Err(SpiError::EmbeddingsUnavailable) => return Ok(records),
        Err(error) => return Err(error),
    };
    let vectors = checked_vectors(vectors, batch.len(), identity)?;
    let scores = hybrid_scores(&texts, query, &vectors[0], &vectors[1..]);
    let mut order: Vec<usize> = (0..records.len()).collect();
    // Stable: equal scores keep the adapter's order.
    order.sort_by(|&left, &right| scores[right].total_cmp(&scores[left]));
    let mut records = reorder(records, &order);
    if let Some(reasoner) = caps.reasoner {
        let permutation = reasoner_permutation(reasoner, query, &records).await?;
        records = reorder(records, &permutation);
    }
    Ok(records)
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
fn hybrid_scores(
    texts: &[&str],
    query: &str,
    query_vector: &[f32],
    vectors: &[Vec<f32>],
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
            let similarity = cosine(query_vector, &vectors[index]);
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
