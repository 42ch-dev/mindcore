//! `mindcore-spi` — the MIND ladder contract crate.
//!
//! Host-facing capability and workflow contracts for embedding, structured
//! reasoning, settlement policy and domain lenses, pinned to the published
//! SPOKE 0.17.0 DTOs. This crate depends on the SPOKE base crates, the storage-agnostic
//! `mindcore-base` domain core, and serde/async infrastructure: no `mindcore-*` storage crate,
//! no Nexus, no network or model SDK enters the contract surface.
//!
//! [`SpiMindWorkflows`] implements `recall_context`, `absorb` and `deliberate`
//! over a DTO-only [`WorkflowBinding`].
//! [`PromptReasoner`] is the only included capability bridge: it adapts a
//! text-prompt executor closure (the Nexus `PromptExecutor` seam, bound by the
//! caller) to [`Reasoner`].

pub mod adoption;
pub mod workflows;

pub use adoption::{adopt_knowledge_entry, adopt_knowledge_entry_typed, AdoptionOutcome};
pub use workflows::{
    page_fact, snapshot_fact, AbsorbSnapshot, CandidateLifecycle, CaptureOutcome,
    CapturedCandidate, CitedRecord, DeliberateSnapshot, PageFact, PageVector, PageVectors,
    RecallSnapshot, RecordFill, SettledRecord, SettledRevision, SnapshotFact, SpiMindWorkflows,
    WorkflowBinding,
};

use std::future::Future;

use async_trait::async_trait;
use serde::Serialize;
use serde_json::{json, Value};
use spoke_operations::SpokeReject;
use spoke_schemas::data::cognition::{CognitionReference, CognitionTarget};
use spoke_schemas::ops::memory_candidates_review_request::Decision;
use spoke_schemas::{
    CognitionRecord, KnowledgeEntry, MemoryCandidate, MemoryCaptureRequest, MemoryRecallRequest,
    MemoryRecordsUpdateRequest, Principal, RetainedRecord,
};

/// Everything a capability call or workflow can surface, in typed form.
///
/// [`SpiError::Settlement`] preserves the structured [`SpokeReject`] —
/// including its retry details — so revision and outcome-uncertain refusals
/// never collapse into provider failures.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum SpiError {
    /// No embedder was injected, or the embedder declared no serving identity.
    #[error("embeddings are unavailable")]
    EmbeddingsUnavailable,
    /// The embedding provider failed.
    #[error("embedding failed: {0}")]
    EmbeddingFailed(String),
    /// No reasoner was injected where one is required.
    #[error("reasoner is unavailable")]
    ReasonerUnavailable,
    /// The prompt transport or provider refused; never a partial success.
    #[error("reasoner failed: {0}")]
    ReasonerFailed(String),
    /// The reasoner returned text that is not complete valid JSON.
    #[error("invalid reasoner output: {0}")]
    InvalidReasonerOutput(String),
    /// Caller input failed contract validation.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The policy hook rejected the settlement draft.
    #[error("policy rejected settlement: {0}")]
    PolicyRejected(String),
    /// A SPOKE write was rejected; the typed reject travels unchanged.
    #[error("settlement rejected ({code}): {message}", code = .0.code, message = .0.message)]
    Settlement(SpokeReject),
    /// The caller's deadline elapsed before a workflow finished.
    #[error("deadline exceeded")]
    DeadlineExceeded,
}

/// Stable identity of the model behind an [`Embedder`].
///
/// Equality covers all four fields, including `model_version` and the declared
/// `dim`. A serving identity has nonempty identifiers/version and a positive
/// dimension; vectors returned by `embed_batch` preserve batch order and
/// count and each has length `dim`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingIdentity {
    /// Provider namespace (e.g. an ACP host or vendor id).
    pub provider_id: String,
    /// Model identifier as served by the provider.
    pub model_id: String,
    /// Model version pin; two builds of the same model differ here.
    pub model_version: String,
    /// Declared vector dimension.
    pub dim: usize,
}

/// Embedding capability. Object-safe; hosts pass `&dyn Embedder`.
///
/// `identity()` returns `None` when the provider cannot name a stable
/// serving identity — the caller must not assume dimension or provenance.
#[async_trait]
pub trait Embedder: Send + Sync {
    /// The serving identity of this embedder, when stable and known.
    fn identity(&self) -> Option<&EmbeddingIdentity>;

    /// Embed a batch of texts, preserving input order and count.
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, SpiError>;
}

/// Explicitly empty embedder: every call reports unavailability.
///
/// `identity()` returns `None` rather than a fake zero-dimensional identity,
/// so callers cannot mistake the absence of embeddings for a real model.
#[derive(Debug, Clone, Copy)]
pub struct NoEmbeddings;

#[async_trait]
impl Embedder for NoEmbeddings {
    fn identity(&self) -> Option<&EmbeddingIdentity> {
        None
    }

    async fn embed_batch(&self, _texts: &[String]) -> Result<Vec<Vec<f32>>, SpiError> {
        Err(SpiError::EmbeddingsUnavailable)
    }
}

/// The cognitive task a structured reasoning request serves.
///
/// Serialized snake_case in the wire envelope built by [`PromptReasoner`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CognitiveTaskKind {
    /// Salience classification and decision proposal for captured candidates.
    Absorb,
    /// Verdict synthesis over supplied evidence toward a cognition record.
    Deliberate,
    /// Reordering an authorized candidate set for recall.
    Rerank,
}

/// One structured reasoning request. All content is already authorized —
/// the reasoner never widens the payload.
#[derive(Debug, Clone, Copy)]
pub struct StructuredReasonRequest<'a> {
    /// Which workflow leg produced this request.
    pub task_kind: CognitiveTaskKind,
    /// The fixed system prompt of the calling workflow; a lens may contribute
    /// declarative fragments to it but never replaces the skeleton.
    pub system_prompt: &'a str,
    /// The authorized user payload, passed through as a JSON string.
    pub user_payload: &'a str,
    /// The output schema the returned JSON must satisfy; the workflow
    /// validates it and all evidence/record IDs itself — the reasoner
    /// only parses.
    pub output_schema: &'a Value,
}

/// Structured-reasoning capability. Object-safe; hosts pass `&dyn Reasoner`.
#[async_trait]
pub trait Reasoner: Send + Sync {
    /// Produce one complete JSON value for `request`, or fail.
    async fn reason_structured(
        &self,
        request: StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError>;
}

/// [`Reasoner`] over a text-prompt executor closure.
///
/// The closure bridges this crate to a host's prompt plane without importing
/// it: Nexus binds `PromptExecutor::execute(PromptRequest)` — supplying its
/// own run/task/role, cancellation and `ToolPolicy::DenyAll` — maps a
/// transport refusal to [`SpiError::ReasonerFailed`] and returns
/// `PromptResult.full_text` on success. All prompt/provider calls made
/// through this bridge run outside a transaction.
pub struct PromptReasoner<F>(F);

impl<F> PromptReasoner<F> {
    /// Wrap an executor closure `Fn(prompt) -> Result<full_text, SpiError>`.
    pub fn new(execute: F) -> Self {
        Self(execute)
    }
}

#[async_trait]
impl<F, Fut> Reasoner for PromptReasoner<F>
where
    F: Fn(String) -> Fut + Send + Sync,
    Fut: Future<Output = Result<String, SpiError>> + Send,
{
    /// Render the system prompt followed by a JSON envelope and make exactly
    /// one prompt call. The complete returned text must parse as JSON;
    /// malformed text is [`SpiError::InvalidReasonerOutput`] — no prose
    /// stripping, retry, or partial success.
    async fn reason_structured(
        &self,
        request: StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        let envelope = json!({
            "task_kind": request.task_kind,
            "user_payload": request.user_payload,
            "output_schema": request.output_schema,
        });
        let prompt = format!(
            "{system}\n\n```json\n{envelope}\n```",
            system = request.system_prompt,
            envelope = serde_json::to_string(&envelope)
                .map_err(|e| SpiError::InvalidInput(e.to_string()))?,
        );
        let text = (self.0)(prompt).await?;
        serde_json::from_str(&text).map_err(|e| SpiError::InvalidReasonerOutput(e.to_string()))
    }
}

/// One settlement awaiting policy evaluation, borrowed from the workflow.
#[derive(Debug, Clone)]
pub enum SettlementDraft<'a> {
    /// An existing pending candidate with the decision the workflow proposes
    /// to submit to `memory.candidates.review`.
    CandidateReview {
        /// The captured pending candidate.
        candidate: &'a MemoryCandidate,
        /// The proposed review decision.
        proposed: &'a Decision,
    },
    /// A pending candidate that would replace one record's content in place.
    /// The wire review decision has no update action, so the workflow
    /// composes `memory.records.update` with `memory.candidates.dismiss`.
    CandidateUpdate {
        /// The captured pending candidate.
        candidate: &'a MemoryCandidate,
        /// The typed update the draft would submit.
        proposed: &'a MemoryRecordsUpdateRequest,
    },
    /// A proposed cognition record; review returns a proposal without
    /// claiming persistence.
    Cognition {
        /// The cognition record under review.
        proposed: &'a CognitionRecord,
    },
}

/// The policy verdict for a [`SettlementDraft`].
///
/// For a pending candidate: [`AutoSettle`] submits the proposed typed
/// decision to `memory.candidates.review`; [`RequireHumanReview`] leaves the
/// candidate `pending` with no review write; [`Reject`] submits the same
/// review operation with `action: "drop"` and the reason as rationale.
/// Settle/drop still require current grants, CAS and a receipt on the
/// caller's transaction, and revalidate every record the decision cited —
/// refusals propagate, never a second write route.
/// For a candidate update draft, [`AutoSettle`] composes
/// `memory.records.update` with `memory.candidates.dismiss`; the other two
/// verdicts resolve exactly as they do for a review draft.
/// For a cognition draft, rejection surfaces as [`SpiError::PolicyRejected`].
///
/// [`AutoSettle`]: Self::AutoSettle
/// [`RequireHumanReview`]: Self::RequireHumanReview
/// [`Reject`]: Self::Reject
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyDecision {
    /// Submit the proposed settlement now.
    AutoSettle,
    /// Leave the draft unsettled for a human decision.
    RequireHumanReview {
        /// Why an automatic grant was not made.
        reason: String,
    },
    /// Drop the proposal, with the recorded rationale.
    Reject {
        /// Why the draft must not settle.
        reason: String,
    },
}

/// Settlement policy hook. Object-safe; hosts pass `&dyn PolicyHook`.
#[async_trait]
pub trait PolicyHook: Send + Sync {
    /// Evaluate one settlement draft.
    async fn evaluate_settlement(
        &self,
        draft: SettlementDraft<'_>,
    ) -> Result<PolicyDecision, SpiError>;
}

/// Conservative default used when no hook is injected.
///
/// Returns `RequireHumanReview` for every draft: it has no trusted risk
/// classifier on which to base an automatic grant.
#[derive(Debug, Clone, Copy)]
pub struct DefaultPolicy;

#[async_trait]
impl PolicyHook for DefaultPolicy {
    async fn evaluate_settlement(
        &self,
        _draft: SettlementDraft<'_>,
    ) -> Result<PolicyDecision, SpiError> {
        Ok(PolicyDecision::RequireHumanReview {
            reason: "default policy holds every settlement for human review".to_string(),
        })
    }
}

/// Declarative prompt fragments a domain lens contributes to one task kind.
///
/// Fragments are text only: no database handle, no unfiltered evidence, and
/// they cannot replace the fixed prompt skeleton, the output schema,
/// evidence validation or authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LensOutput<'a> {
    /// Persona and stance framing for the task.
    pub persona_and_stance: &'a str,
    /// What makes an item salient for this domain.
    pub salience_criteria: &'a str,
    /// The evidence standard claims must meet.
    pub evidence_bar: &'a str,
    /// Hard refusal lines the domain imposes.
    pub redlines: &'a [&'a str],
}

/// The structured recommendation of one deliberation verdict, separated from
/// its descriptive prose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Recommendation {
    /// The verdict records nothing.
    None,
    /// The verdict promotes one record whose `target_kind` names what it
    /// promotes (for example an observation, a belief or an ADR).
    Promote {
        /// What the promoted record is, in the lens's own vocabulary.
        target_kind: String,
    },
}

/// One proposal a workflow validated against its authorized snapshot, handed
/// to [`DomainLens::validate_proposal`] before policy or any write.
///
/// It carries what the workflow would settle — never raw reasoner text, a
/// storage handle or an unfiltered evidence set. Every id it cites already
/// passed the workflow's reference check against the admitted snapshot.
#[derive(Debug, Clone, Copy)]
pub enum ProposalDraft<'a> {
    /// One validated absorb decision over the prompted record page.
    Absorb {
        /// The decision: `promote`, `update` or `dismiss`.
        action: &'a str,
        /// The record an `update` decision replaces.
        target_record_id: Option<&'a str>,
        /// The validated dismissal rationale; `Some` exactly for a `dismiss`,
        /// so a lens can key a redline on what the dismissal claims.
        rationale: Option<&'a str>,
        /// The page ids the decision cites.
        cited_ids: &'a [&'a str],
    },
    /// One validated deliberation verdict.
    Cognition {
        /// The structured recommendation.
        recommendation: &'a Recommendation,
        /// The stamped record a promotion would append; `None` when the
        /// recommendation is [`Recommendation::None`].
        record: Option<&'a CognitionRecord>,
        /// The supplied evidence ids and visible record ids the verdict cites.
        cited_ids: &'a [&'a str],
    },
}

/// Domain lens: declarative prompt fragments keyed by task kind.
///
/// `None` in place of a lens means the fixed skeleton alone is used.
/// Synchronous and pure — a lens has no capability surface of its own.
pub trait DomainLens: Send + Sync {
    /// Fragments for one cognitive task kind.
    fn describe(&self, task_kind: CognitiveTaskKind) -> LensOutput<'_>;

    /// Refuse a proposal this domain will not settle, or accept it.
    ///
    /// Runs after the workflow's schema and reference validation and before
    /// the policy hook or any write, in both `absorb` and `deliberate`. A
    /// refusal surfaces as [`SpiError::PolicyRejected`] and settles nothing;
    /// `admitted` is the exact reference set the snapshot authorized.
    fn validate_proposal(&self, draft: &ProposalDraft<'_>, admitted: &[&str])
        -> Result<(), String>;
}

/// Capabilities injected into one workflow call.
///
/// An absent embedder or [`SpiError::EmbeddingsUnavailable`] selects lexical
/// recall; other provider errors are never silently downgraded. `absorb`
/// without a reasoner keeps the deterministic classifier path; `deliberate`
/// without one returns [`SpiError::ReasonerUnavailable`].
#[derive(Default)]
pub struct MindCapabilities<'a> {
    /// Embedding capability, when the host provides one.
    pub embedder: Option<&'a dyn Embedder>,
    /// Structured-reasoning capability, when the host provides one.
    pub reasoner: Option<&'a dyn Reasoner>,
    /// Settlement policy hook; [`DefaultPolicy`] when `None`.
    pub policy_hook: Option<&'a dyn PolicyHook>,
}

/// An authorized context pack for one recall.
#[derive(Debug, Clone)]
pub struct ContextPack {
    /// Records admitted to the context, with source/revision data.
    pub records: Vec<RetainedRecord>,
    /// Knowledge projections admitted to the context.
    pub knowledge: Vec<KnowledgeEntry>,
    /// Tokens spent packing this response. `max_tokens` caps packing
    /// separately from the SPOKE recall wire-byte budget.
    pub tokens_used: usize,
}

/// The outcome of one candidate through `absorb`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AbsorbCandidateOutcome {
    /// The candidate settled; `record_ids` are the records it produced.
    Settled {
        /// The reviewed candidate id.
        candidate_id: String,
        /// Record ids created or updated by the settlement.
        record_ids: Vec<String>,
    },
    /// The candidate remains pending for review.
    Pending {
        /// The pending candidate id.
        candidate_id: String,
    },
    /// The candidate was dropped.
    Dropped {
        /// The dropped candidate id.
        candidate_id: String,
    },
}

/// One piece of supplied evidence, already admitted to the caller's scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Evidence<'a> {
    /// Stable evidence id the workflow validates against the verdict.
    pub id: &'a str,
    /// Evidence text.
    pub text: &'a str,
}

/// One deliberation request: derive a verdict for a cognition target.
#[derive(Debug, Clone)]
pub struct DeliberateRequest<'a> {
    /// Memory space the deliberation runs in.
    pub space_id: &'a str,
    /// Subject the cognition belongs to.
    pub subject_id: &'a str,
    /// Assessment context stamped onto the record. `None` derives it from
    /// [`Self::subject_id`]; a supplied id is only validated by the record
    /// schema and never widens admission or the storage route.
    pub assessment_context_id: Option<&'a str>,
    /// The holder this verdict is attributed to.
    pub holder: &'a CognitionReference,
    /// The cognition target being assessed.
    pub target: &'a CognitionTarget,
    /// Evidence admitted for this deliberation.
    pub evidence: &'a [Evidence<'a>],
    /// Elapsed-time budget for the whole call. It bounds the reasoner and
    /// policy executions, not only the boundaries between phases: one still
    /// running at expiry is dropped and overrun surfaces
    /// [`SpiError::DeadlineExceeded`] without starting a write.
    pub timeout: std::time::Duration,
}

/// The outcome of one `deliberate` call.
#[derive(Debug, Clone)]
pub enum DeliberateOutcome {
    /// The verdict settled and a `MindState` was derived.
    Settled {
        /// The validated structured verdict.
        verdict: Value,
        /// The settled cognition record.
        record: CognitionRecord,
        /// Version of the committed source the derived `MindState` descends
        /// from.
        mind_state_version: u64,
    },
    /// Policy held the verdict for human review; nothing settled.
    NeedsReview {
        /// The validated structured verdict.
        verdict: Value,
        /// The unsettled cognition record draft.
        draft: CognitionRecord,
    },
    /// The verdict recommended no record, so nothing was written.
    NoChange {
        /// The validated structured verdict.
        verdict: Value,
    },
}

/// The MIND workflow surface, implemented by [`SpiMindWorkflows`] over a
/// [`WorkflowBinding`]. `principal` is caller-supplied trusted context, never
/// parsed from request JSON.
#[async_trait]
pub trait MindWorkflows: Send + Sync {
    /// Pack an authorized context pack under a token budget.
    async fn recall_context(
        &self,
        principal: &Principal,
        request: &MemoryRecallRequest,
        max_tokens: std::num::NonZeroUsize,
        caps: &MindCapabilities<'_>,
    ) -> Result<ContextPack, SpiError>;

    /// Classify and settle captured candidates.
    async fn absorb(
        &self,
        principal: &Principal,
        request: &MemoryCaptureRequest,
        lens: Option<&dyn DomainLens>,
        caps: &MindCapabilities<'_>,
    ) -> Result<Vec<AbsorbCandidateOutcome>, SpiError>;

    /// Derive a validated verdict over supplied evidence.
    async fn deliberate(
        &self,
        principal: &Principal,
        request: DeliberateRequest<'_>,
        lens: Option<&dyn DomainLens>,
        caps: &MindCapabilities<'_>,
    ) -> Result<DeliberateOutcome, SpiError>;
}
