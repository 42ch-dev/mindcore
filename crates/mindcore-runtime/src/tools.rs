//! The three Mind workflow tools served over Connect.
//!
//! Connect owns the signed `{ "arguments": … }` / `{ "result": … }` wrappers; these handlers see
//! only the argument object and return only the result value. Each handler binds to the responder
//! session it was installed on, takes the authenticated peer from that session, and resolves it to
//! the host-admitted [`Principal`] — a principal is never decoded from the arguments.
//!
//! The mapping stays thin: decode, call the workflow, encode. [`SpiError::Settlement`] keeps its
//! reject unchanged (code, message and details); the other variants map to the seam's stable
//! kinds, and provider internals never reach the wire message.

use std::num::NonZeroUsize;
use std::sync::{Arc, Weak};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use mindcore_spi::{
    AbsorbCandidateOutcome, ContextPack, DeliberateOutcome, DeliberateRequest, DomainLens, Embedder,
    Evidence, MindCapabilities, MindWorkflows, PolicyHook, Reasoner, SpiError,
};
use spoke_connect::remote::{ConnectResponder, MemoryAdmission, ToolHandler};
use spoke_operations::{SpokeReject, SpokeRejectCode, SpokeResult};
use spoke_schemas::data::cognition::{CognitionReference, CognitionTarget};
use spoke_schemas::host_capability_manifest::ToolDescriptor;
use spoke_schemas::{MemoryCaptureRequest, MemoryRecallRequest, Principal};

use crate::ToolRegistration;

/// Stable capability id of the context-pack workflow.
pub const RECALL_CONTEXT: &str = "tools.mindcore.recall_context";
/// Stable capability id of the capture/settle workflow.
pub const ABSORB: &str = "tools.mindcore.absorb";
/// Stable capability id of the deliberation workflow.
pub const DELIBERATE: &str = "tools.mindcore.deliberate";

/// The provider capabilities a tool call injects into its workflow.
///
/// Held as owned handles so a handler can live for the whole responder session; each call borrows
/// them into [`MindCapabilities`].
#[derive(Clone, Default)]
pub struct MindProviders {
    /// Embedding capability, when the host provides one.
    pub embedder: Option<Arc<dyn Embedder>>,
    /// Structured-reasoning capability, when the host provides one.
    pub reasoner: Option<Arc<dyn Reasoner>>,
    /// Settlement policy hook; the workflow's default policy when `None`.
    pub policy_hook: Option<Arc<dyn PolicyHook>>,
    /// Domain lens applied to absorb and deliberate.
    pub lens: Option<Arc<dyn DomainLens>>,
}

impl MindProviders {
    fn capabilities(&self) -> MindCapabilities<'_> {
        MindCapabilities {
            embedder: self.embedder.as_deref(),
            reasoner: self.reasoner.as_deref(),
            policy_hook: self.policy_hook.as_deref(),
        }
    }
}

/// The Mind workflow tools: three registrations plus the thin serving path they share.
pub struct WorkflowTools {
    workflows: Arc<dyn MindWorkflows>,
    principals: MemoryAdmission,
    providers: MindProviders,
}

impl WorkflowTools {
    /// Bind the tools to one workflow engine, the host's peer-to-principal admission and its
    /// providers.
    pub fn new(
        workflows: Arc<dyn MindWorkflows>,
        principals: MemoryAdmission,
        providers: MindProviders,
    ) -> Self {
        Self {
            workflows,
            principals,
            providers,
        }
    }

    /// The three served tools, advertised and installable.
    pub fn registrations(&self) -> Vec<ToolRegistration> {
        vec![
            self.registration(
                RECALL_CONTEXT,
                "Recall a context pack under a token budget.",
                recall_input(),
                recall_output(),
            ),
            self.registration(
                ABSORB,
                "Capture candidates and settle what each one becomes.",
                absorb_input(),
                absorb_output(),
            ),
            self.registration(
                DELIBERATE,
                "Derive a validated verdict over supplied evidence.",
                deliberate_input(),
                deliberate_output(),
            ),
        ]
    }

    /// Serve one call for an already authenticated `peer_id`.
    ///
    /// Missing peer or principal is `NotAuthorized`; the resolver's own reject travels unchanged.
    pub async fn call(
        &self,
        capability_id: &str,
        peer_id: &str,
        arguments: Value,
    ) -> SpokeResult<Value> {
        serve(
            Arc::clone(&self.workflows),
            Arc::clone(&self.principals),
            self.providers.clone(),
            capability_id,
            peer_id,
            arguments,
        )
        .await
    }

    fn registration(
        &self,
        capability_id: &'static str,
        description: &str,
        input: Value,
        output: Value,
    ) -> ToolRegistration {
        let descriptor = descriptor(capability_id, description, input, output);
        let workflows = Arc::clone(&self.workflows);
        let principals = Arc::clone(&self.principals);
        let providers = self.providers.clone();
        ToolRegistration::new(descriptor, move |responder| {
            // Weak: the handler serves its session without extending its lifetime.
            let session: Weak<ConnectResponder> = Arc::downgrade(responder);
            let workflows = Arc::clone(&workflows);
            let principals = Arc::clone(&principals);
            let providers = providers.clone();
            let handler: ToolHandler = Arc::new(move |arguments: Value| {
                let session = session.clone();
                let workflows = Arc::clone(&workflows);
                let principals = Arc::clone(&principals);
                let providers = providers.clone();
                Box::pin(async move {
                    let Some(peer_id) = session
                        .upgrade()
                        .and_then(|responder| responder.remote_peer_id())
                    else {
                        return SpokeResult::Reject(SpokeReject {
                            code: SpokeRejectCode::NotAuthorized,
                            message: "the responder session carries no authenticated peer".into(),
                            details: None,
                        });
                    };
                    serve(
                        workflows,
                        principals,
                        providers,
                        capability_id,
                        &peer_id,
                        arguments,
                    )
                    .await
                })
            });
            handler
        })
    }
}

async fn serve(
    workflows: Arc<dyn MindWorkflows>,
    principals: MemoryAdmission,
    providers: MindProviders,
    capability_id: &str,
    peer_id: &str,
    arguments: Value,
) -> SpokeResult<Value> {
    let principal = match principals(peer_id) {
        SpokeResult::Ok(principal) => principal,
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    match capability_id {
        RECALL_CONTEXT => recall_context(&*workflows, &principal, arguments, &providers).await,
        ABSORB => absorb(&*workflows, &principal, arguments, &providers).await,
        DELIBERATE => deliberate(&*workflows, &principal, arguments, &providers).await,
        other => SpokeResult::Reject(SpokeReject {
            code: SpokeRejectCode::InvalidInput,
            message: format!("unknown mindcore tool {other}"),
            details: None,
        }),
    }
}

async fn recall_context(
    workflows: &dyn MindWorkflows,
    principal: &Principal,
    arguments: Value,
    providers: &MindProviders,
) -> SpokeResult<Value> {
    let arguments: RecallArguments = match decode(arguments) {
        Ok(arguments) => arguments,
        Err(reject) => return SpokeResult::Reject(reject),
    };
    let caps = providers.capabilities();
    match workflows
        .recall_context(principal, &arguments.request, arguments.max_tokens, &caps)
        .await
    {
        Ok(ContextPack {
            records,
            knowledge,
            tokens_used,
        }) => SpokeResult::Ok(json!({
            "records": records,
            "knowledge": knowledge,
            "tokens_used": tokens_used,
        })),
        Err(error) => SpokeResult::Reject(workflow_reject(error)),
    }
}

async fn absorb(
    workflows: &dyn MindWorkflows,
    principal: &Principal,
    arguments: Value,
    providers: &MindProviders,
) -> SpokeResult<Value> {
    let arguments: AbsorbArguments = match decode(arguments) {
        Ok(arguments) => arguments,
        Err(reject) => return SpokeResult::Reject(reject),
    };
    let caps = providers.capabilities();
    match workflows
        .absorb(
            principal,
            &arguments.request,
            providers.lens.as_deref(),
            &caps,
        )
        .await
    {
        Ok(outcomes) => SpokeResult::Ok(Value::Array(
            outcomes.into_iter().map(absorb_outcome).collect(),
        )),
        Err(error) => SpokeResult::Reject(workflow_reject(error)),
    }
}

async fn deliberate(
    workflows: &dyn MindWorkflows,
    principal: &Principal,
    arguments: Value,
    providers: &MindProviders,
) -> SpokeResult<Value> {
    let arguments: DeliberateArguments = match decode(arguments) {
        Ok(arguments) => arguments,
        Err(reject) => return SpokeResult::Reject(reject),
    };
    let evidence: Vec<Evidence<'_>> = arguments
        .evidence
        .iter()
        .map(|evidence| Evidence {
            id: evidence.id.as_str(),
            text: evidence.text.as_str(),
        })
        .collect();
    let request = DeliberateRequest {
        space_id: &arguments.space_id,
        subject_id: &arguments.subject_id,
        holder: &arguments.holder,
        target: &arguments.target,
        evidence: &evidence,
        timeout: Duration::from_millis(arguments.timeout_ms.get() as u64),
    };
    let caps = providers.capabilities();
    match workflows
        .deliberate(principal, request, providers.lens.as_deref(), &caps)
        .await
    {
        Ok(outcome) => SpokeResult::Ok(deliberate_outcome(outcome)),
        Err(error) => SpokeResult::Reject(workflow_reject(error)),
    }
}

fn absorb_outcome(outcome: AbsorbCandidateOutcome) -> Value {
    match outcome {
        AbsorbCandidateOutcome::Pending { candidate_id } => {
            json!({ "status": "pending", "candidate_id": candidate_id })
        }
        AbsorbCandidateOutcome::Dropped { candidate_id } => {
            json!({ "status": "dropped", "candidate_id": candidate_id })
        }
        AbsorbCandidateOutcome::Settled {
            candidate_id,
            record_ids,
        } => json!({
            "status": "settled",
            "candidate_id": candidate_id,
            "record_ids": record_ids,
        }),
    }
}

fn deliberate_outcome(outcome: DeliberateOutcome) -> Value {
    match outcome {
        DeliberateOutcome::NoChange { verdict } => {
            json!({ "status": "no_change", "verdict": verdict })
        }
        DeliberateOutcome::NeedsReview { verdict, draft } => {
            json!({ "status": "needs_review", "verdict": verdict, "draft": draft })
        }
        DeliberateOutcome::Settled {
            verdict,
            record,
            mind_state_version,
        } => json!({
            "status": "settled",
            "verdict": verdict,
            "record": record,
            "mind_state_version": mind_state_version,
        }),
    }
}

/// One workflow failure as the wire reject the seam documents.
fn workflow_reject(error: SpiError) -> SpokeReject {
    match error {
        // The settlement refusal travels whole: code, message and structured details.
        SpiError::Settlement(reject) => reject,
        SpiError::InvalidInput(message) => SpokeReject {
            code: SpokeRejectCode::InvalidInput,
            message,
            details: None,
        },
        SpiError::PolicyRejected(_) => SpokeReject {
            code: SpokeRejectCode::NotAuthorized,
            message: "the settlement policy refused this proposal".into(),
            details: Some(kind_details("policy_rejected")),
        },
        SpiError::DeadlineExceeded => workflow_internal("deadline_exceeded"),
        SpiError::EmbeddingsUnavailable => workflow_internal("embeddings_unavailable"),
        SpiError::EmbeddingFailed(_) => workflow_internal("embedding_failed"),
        SpiError::ReasonerUnavailable => workflow_internal("reasoner_unavailable"),
        SpiError::ReasonerFailed(_) => workflow_internal("reasoner_failed"),
        SpiError::InvalidReasonerOutput(_) => workflow_internal("invalid_reasoner_output"),
    }
}

/// A provider failure: the kind is structured, and the message never reflects provider internals.
fn workflow_internal(kind: &str) -> SpokeReject {
    SpokeReject {
        code: SpokeRejectCode::InternalError,
        message: "the mind workflow could not complete".into(),
        details: Some(kind_details(kind)),
    }
}

fn kind_details(kind: &str) -> Map<String, Value> {
    Map::from_iter([("kind".to_string(), Value::String(kind.to_string()))])
}

fn decode<T: DeserializeOwned>(arguments: Value) -> Result<T, SpokeReject> {
    serde_json::from_value(arguments).map_err(|error| SpokeReject {
        code: SpokeRejectCode::InvalidInput,
        message: format!("invalid tool arguments: {error}"),
        details: None,
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecallArguments {
    request: MemoryRecallRequest,
    max_tokens: NonZeroUsize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AbsorbArguments {
    request: MemoryCaptureRequest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeliberateArguments {
    space_id: String,
    subject_id: String,
    holder: CognitionReference,
    target: CognitionTarget,
    evidence: Vec<EvidenceArgument>,
    timeout_ms: NonZeroUsize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceArgument {
    id: String,
    text: String,
}

fn descriptor(
    capability_id: &str,
    description: &str,
    input: Value,
    output: Value,
) -> ToolDescriptor {
    serde_json::from_value(json!({
        "schema_version": 1,
        "capability_id": capability_id,
        "op": capability_id,
        "description": description,
        "input": input,
        "output": output,
    }))
    .expect("the static mindcore tool descriptor matches the schema")
}

fn recall_input() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["request", "max_tokens"],
        "properties": {
            "request": {
                "type": "object",
                "description": "SPOKE MemoryRecallRequest.",
            },
            "max_tokens": { "type": "integer", "minimum": 1 },
        },
    })
}

fn recall_output() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["records", "knowledge", "tokens_used"],
        "properties": {
            "records": { "type": "array", "items": { "type": "object" } },
            "knowledge": { "type": "array", "items": { "type": "object" } },
            "tokens_used": { "type": "integer", "minimum": 0 },
        },
    })
}

fn absorb_input() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["request"],
        "properties": {
            "request": {
                "type": "object",
                "description": "SPOKE MemoryCaptureRequest.",
            },
        },
    })
}

fn absorb_output() -> Value {
    json!({
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["status", "candidate_id"],
            "properties": {
                "status": { "enum": ["pending", "dropped", "settled"] },
                "candidate_id": { "type": "string" },
                "record_ids": { "type": "array", "items": { "type": "string" } },
            },
        },
    })
}

fn deliberate_input() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["space_id", "subject_id", "holder", "target", "evidence", "timeout_ms"],
        "properties": {
            "space_id": { "type": "string" },
            "subject_id": { "type": "string" },
            "holder": {
                "type": "object",
                "description": "SPOKE CognitionReference.",
            },
            "target": {
                "type": "object",
                "description": "SPOKE CognitionTarget.",
            },
            "evidence": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["id", "text"],
                    "properties": {
                        "id": { "type": "string" },
                        "text": { "type": "string" },
                    },
                },
            },
            "timeout_ms": { "type": "integer", "minimum": 1 },
        },
    })
}

fn deliberate_output() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["status", "verdict"],
        "properties": {
            "status": { "enum": ["no_change", "needs_review", "settled"] },
            "verdict": { "type": "object" },
            "draft": { "type": "object" },
            "record": { "type": "object" },
            "mind_state_version": { "type": "integer", "minimum": 0 },
        },
    })
}
