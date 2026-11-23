//! The Mind tools served over Connect: the three workflow tools.
//!
//! Connect owns the signed `{ "arguments": … }` / `{ "result": … }` wrappers; these handlers see
//! only the argument object and return only the result value. Each handler binds to the responder
//! session it was installed on, takes the authenticated peer from that session, and resolves it to
//! the host-admitted [`Principal`] — a principal is never decoded from the arguments.
//!
//! The mapping stays thin: decode, call the workflow, encode. [`SpiError::Settlement`] keeps its
//! reject unchanged (code, message and details); the other variants map to the seam's stable
//! kinds. A policy veto carries the host's reason and a deadline its retry class; provider
//! internals never reach the wire message.

use std::num::NonZeroUsize;
use std::sync::{Arc, Weak};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use mindcore_spi::{
    AbsorbCandidateOutcome, ContextPack, DeliberateOutcome, DeliberateRequest, DomainLens,
    Embedder, Evidence, MindCapabilities, MindWorkflows, PolicyHook, Reasoner, SpiError,
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
///
/// Hosts never build this: they assemble [`crate::MindcoreServe::surface`], and the surface
/// installs these registrations. Its descriptor and dispatch surface stay crate-internal, because
/// only the surface carries the deliberation route invariant.
pub(crate) struct WorkflowTools {
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
        Self::advertised()
            .into_iter()
            .map(|(capability_id, description, input, output)| {
                self.install(
                    capability_id,
                    descriptor(capability_id, description, input(), output()),
                )
            })
            .collect()
    }

    /// The descriptors alone, as the mapping tests read the advertised ABI; advertisement and
    /// installation both come from [`Self::registrations`].
    #[cfg(test)]
    pub(crate) fn descriptors(&self) -> Vec<ToolDescriptor> {
        Self::advertised()
            .into_iter()
            .map(|(capability_id, description, input, output)| {
                descriptor(capability_id, description, input(), output())
            })
            .collect()
    }

    /// The one list both advertisement and installation read.
    #[allow(clippy::type_complexity)]
    fn advertised() -> [(&'static str, &'static str, fn() -> Value, fn() -> Value); 3] {
        [
            (
                RECALL_CONTEXT,
                "Recall a context pack under a token budget.",
                recall_input,
                recall_output,
            ),
            (
                ABSORB,
                "Capture candidates and settle what each one becomes.",
                absorb_input,
                absorb_output,
            ),
            (
                DELIBERATE,
                "Derive a validated verdict over supplied evidence.",
                deliberate_input,
                deliberate_output,
            ),
        ]
    }

    /// Serve one call for an already authenticated `peer_id`.
    ///
    /// Test-only: the served entry is the handler [`Self::install`] builds, which takes the peer
    /// from the responder session it serves. Missing peer or principal is `NotAuthorized`; the
    /// resolver's own reject travels unchanged.
    #[cfg(test)]
    pub(crate) async fn call(
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

    fn install(&self, capability_id: &'static str, descriptor: ToolDescriptor) -> ToolRegistration {
        let workflows = Arc::clone(&self.workflows);
        let principals = Arc::clone(&self.principals);
        let providers = self.providers.clone();
        ToolRegistration::new(descriptor, move |responder| {
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
                    let peer_id = match authenticated_peer(&session) {
                        Ok(peer_id) => peer_id,
                        Err(reject) => return SpokeResult::Reject(reject),
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

/// The authenticated peer of the session a handler was installed on.
///
/// The session is held weakly, so a handler serves its session without extending its lifetime; a
/// handler bound to one session must not be installed on another, so an upgraded session that
/// carries no peer is the terminal refusal rather than a call resolved against another caller.
fn authenticated_peer(session: &Weak<ConnectResponder>) -> Result<String, SpokeReject> {
    match session
        .upgrade()
        .and_then(|responder| responder.remote_peer_id())
    {
        Some(peer_id) => Ok(peer_id),
        None => Err(SpokeReject {
            code: SpokeRejectCode::NotAuthorized,
            message: "the responder session carries no authenticated peer".into(),
            details: None,
        }),
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
        assessment_context_id: None,
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
        SpiError::Settlement(reject) => reject,
        SpiError::InvalidInput(message) => SpokeReject {
            code: SpokeRejectCode::InvalidInput,
            message,
            details: None,
        },
        SpiError::PolicyRejected(reason) => SpokeReject {
            code: SpokeRejectCode::InvalidInput,
            message: reason,
            details: Some(kind_details("policy_rejected")),
        },
        SpiError::DeadlineExceeded => workflow_retryable("deadline_exceeded"),
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

/// A refusal the caller may repeat unchanged: the deadline left nothing committed behind it.
fn workflow_retryable(kind: &str) -> SpokeReject {
    let mut reject = workflow_internal(kind);
    if let Some(details) = reject.details.as_mut() {
        details.insert("retry_class".to_string(), Value::String("retryable".into()));
    }
    reject
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

/// The served argument shapes. This one, [`absorb_input`] and [`deliberate_input`] each build a
/// self-contained draft-07 subschema — own `$id`, so its local `#/definitions/…` resolve inside a
/// manifest — mirroring the pinned `spoke-schemas` request DTOs the handler decodes. A client that
/// only reads the manifest can build a valid call; the mapping tests pin each shape against the
/// DTO. The cognition shapes are too large to restate by hand, so [`absorb_input`] and
/// [`deliberate_input`] splice the pinned document's definitions in.
fn recall_input() -> Value {
    json!({
        "$id": "https://mindcore.invalid/schemas/tools/recall-context.input.schema.json",
        "type": "object",
        "additionalProperties": false,
        "required": ["request", "max_tokens"],
        "properties": {
            "request": {
                "type": "object",
                "additionalProperties": false,
                "required": ["space_id", "subject_id", "selection", "budget"],
                "description": "A SPOKE MemoryRecallRequest. `cognition_filter` is refused by this \
                                provider, so it is not advertised.",
                "properties": {
                    "space_id": { "type": "string", "minLength": 1 },
                    "subject_id": { "type": "string", "minLength": 1 },
                    "selection": { "$ref": "#/definitions/read_selection" },
                    "filter": { "$ref": "#/definitions/provenance_filter" },
                    "query": {
                        "type": "string",
                        "description": "Ranking hint only; never an authorization input.",
                    },
                    "budget": { "$ref": "#/definitions/budget" },
                    "cursor": { "type": "string", "minLength": 1 },
                    "extensions": { "$ref": "#/definitions/extension_map" },
                },
            },
            "max_tokens": { "type": "integer", "minimum": 1 },
        },
        "definitions": {
            "read_selection": {
                "description": "Placement selection (§6.12): one closed variant per mode.",
                "oneOf": [
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["mode"],
                        "properties": { "mode": { "const": "subject_all" } },
                    },
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["mode"],
                        "properties": { "mode": { "const": "shared_only" } },
                    },
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["mode", "context_ids"],
                        "properties": {
                            "mode": { "const": "selected_contexts" },
                            "context_ids": {
                                "type": "array",
                                "minItems": 1,
                                "items": { "type": "string" },
                            },
                            "include_shared": { "type": "boolean" },
                        },
                    },
                ],
            },
            "provenance_filter": {
                "type": "object",
                "additionalProperties": false,
                "required": ["source_context_ids"],
                "properties": {
                    "source_context_ids": {
                        "type": "array",
                        "minItems": 1,
                        "items": { "type": "string" },
                    },
                },
            },
            "budget": {
                "type": "object",
                "additionalProperties": false,
                "required": ["max_items"],
                "properties": {
                    "max_items": { "type": "integer", "minimum": 1 },
                    "max_bytes": { "type": "integer", "minimum": 1 },
                },
            },
            "extension_map": {
                "type": "object",
                "propertyNames": { "pattern": "^[a-z][a-z0-9_-]*$" },
                "additionalProperties": { "type": "object" },
            },
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

/// The pinned `spoke-schemas` 0.17.0 cognition document, verbatim from spoke commit `470e7ad`,
/// `schemas/data/cognition.schema.json` — the source that crate was generated from.
const COGNITION_SCHEMA: &str = include_str!("schemas/cognition.schema.json");
/// The same commit's `schemas/common/common.schema.json`; only the definitions the cognition
/// document names are spliced.
const COMMON_SCHEMA: &str = include_str!("schemas/common.schema.json");
/// Upstream `$ref` prefixes of the pinned documents; every reference uses one of them.
const COGNITION_REFERENCE: &str =
    "https://spoke42.invalid/schemas/data/cognition.schema.json#/definitions/";
const COMMON_REFERENCE: &str =
    "https://spoke42.invalid/schemas/common/common.schema.json#/definitions/";

/// The pinned definitions `roots` name, closed over every definition they reference, as an
/// advertised subschema's flat `definitions`.
///
/// Two mechanical rewrites keep the advertised manifest self-contained, and the mapping tests hold
/// every descriptor to both: absolute `$ref`s become local, and every object states its
/// `additionalProperties`, so openness is never an omission a client must guess at. Only the
/// definitions the roots reach are spliced, so one advertised document carries no unused pinned
/// shape.
fn cognition_definitions(roots: &[&str]) -> Map<String, Value> {
    let pinned: Value =
        serde_json::from_str(COGNITION_SCHEMA).expect("the pinned cognition schema parses");
    let common: Value =
        serde_json::from_str(COMMON_SCHEMA).expect("the pinned common schema parses");

    let mut record = pinned
        .as_object()
        .expect("the pinned cognition schema is an object")
        .clone();
    let mut definitions = record
        .remove("definitions")
        .and_then(|definitions| definitions.as_object().cloned())
        .expect("the pinned cognition schema declares its definitions");
    record.remove("$id");
    record.remove("$schema");
    definitions.insert("cognition_record".to_string(), Value::Object(record));

    let common = common["definitions"]
        .as_object()
        .expect("the pinned common schema declares its definitions");
    for name in ["ExtensionMap", "ForkId", "OpaqueJson", "Timestamp"] {
        definitions.insert(
            name.to_string(),
            common
                .get(name)
                .unwrap_or_else(|| panic!("the pinned common schema defines {name}"))
                .clone(),
        );
    }

    for definition in definitions.values_mut() {
        localize_refs(definition);
        state_openness(definition);
    }

    let mut selected = Map::new();
    let mut pending: Vec<String> = roots.iter().map(|root| (*root).to_string()).collect();
    while let Some(name) = pending.pop() {
        if selected.contains_key(&name) {
            continue;
        }
        let definition = definitions
            .get(&name)
            .unwrap_or_else(|| panic!("the pinned cognition schema defines {name}"))
            .clone();
        referenced(&definition, &mut pending);
        selected.insert(name, definition);
    }
    selected
}

/// Every local definition name one advertised definition references.
fn referenced(schema: &Value, names: &mut Vec<String>) {
    match schema {
        Value::Object(fields) => {
            if let Some(reference) = fields.get("$ref").and_then(Value::as_str) {
                names.push(
                    reference
                        .strip_prefix("#/definitions/")
                        .unwrap_or_else(|| {
                            panic!("the advertised definition references {reference}")
                        })
                        .to_string(),
                );
            }
            for field in fields.values() {
                referenced(field, names);
            }
        }
        Value::Array(items) => items.iter().for_each(|item| referenced(item, names)),
        _ => {}
    }
}

/// Point a pinned definition's `$ref`s at the advertised document's own `definitions`.
fn localize_refs(schema: &mut Value) {
    match schema {
        Value::Object(fields) => {
            if let Some(reference) = fields.get("$ref").and_then(Value::as_str) {
                let local = reference
                    .strip_prefix(COGNITION_REFERENCE)
                    .or_else(|| reference.strip_prefix(COMMON_REFERENCE))
                    .unwrap_or_else(|| {
                        panic!("the pinned cognition schema references {reference}")
                    });
                fields.insert("$ref".into(), Value::from(format!("#/definitions/{local}")));
            }
            for field in fields.values_mut() {
                localize_refs(field);
            }
        }
        Value::Array(items) => items.iter_mut().for_each(localize_refs),
        _ => {}
    }
}

/// Spell out draft-07's implicit open-object default.
fn state_openness(schema: &mut Value) {
    match schema {
        Value::Object(fields) => {
            if fields.get("type").and_then(Value::as_str) == Some("object")
                && !fields.contains_key("additionalProperties")
            {
                fields.insert("additionalProperties".into(), Value::Object(Map::new()));
            }
            for field in fields.values_mut() {
                state_openness(field);
            }
        }
        Value::Array(items) => items.iter_mut().for_each(state_openness),
        _ => {}
    }
}

fn absorb_input() -> Value {
    let mut schema = json!({
        "$id": "https://mindcore.invalid/schemas/tools/absorb.input.schema.json",
        "type": "object",
        "additionalProperties": false,
        "required": ["request"],
        "properties": {
            "request": {
                "type": "object",
                "additionalProperties": false,
                "required": ["space_id", "subject_id", "content", "capture_scope", "idempotency_key"],
                "description": "A SPOKE MemoryCaptureRequest.",
                "properties": {
                    "space_id": { "type": "string", "minLength": 1 },
                    "subject_id": { "type": "string", "minLength": 1 },
                    "content": { "$ref": "#/definitions/content" },
                    "capture_scope": { "$ref": "#/definitions/placement_scope" },
                    "source_context_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "anchors": {
                        "type": "array",
                        "items": { "$ref": "#/definitions/source_anchor" },
                    },
                    "kind_hint": { "type": "string", "minLength": 1 },
                    "occurred_at": { "type": "string", "format": "date-time" },
                    "idempotency_key": { "type": "string", "minLength": 1, "maxLength": 128 },
                    "extensions": { "$ref": "#/definitions/extension_map" },
                },
            },
        },
        "definitions": {
            "content": {
                "type": "object",
                "additionalProperties": false,
                "required": ["text"],
                "properties": {
                    "text": { "type": "string" },
                    "structured": {
                        "description": "Opaque product JSON; stored and returned unchanged.",
                    },
                    "cognition": {
                        "type": "array",
                        "description": "Recorded-at-time cognition: each entry is a closed pinned \
                                        SPOKE CognitionRecord.",
                        "items": { "$ref": "#/definitions/cognition_record" },
                    },
                },
            },
            "placement_scope": {
                "oneOf": [
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["visibility"],
                        "properties": { "visibility": { "const": "shared" } },
                    },
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["visibility", "context_id"],
                        "properties": {
                            "visibility": { "const": "context" },
                            "context_id": { "type": "string", "minLength": 1 },
                        },
                    },
                ],
            },
            "source_anchor": {
                "type": "object",
                "additionalProperties": false,
                "required": ["schema_version", "source_id", "extensions"],
                "properties": {
                    "schema_version": { "type": "integer", "minimum": 1 },
                    "source_id": { "type": "string" },
                    "span": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["start", "end"],
                        "properties": {
                            "start": { "type": "number" },
                            "end": { "type": "number" },
                        },
                    },
                    "label": { "type": "string" },
                    "mime_type": { "type": "string" },
                    "extensions": { "$ref": "#/definitions/extension_map" },
                },
            },
            "extension_map": {
                "type": "object",
                "propertyNames": { "pattern": "^[a-z][a-z0-9_-]*$" },
                "additionalProperties": { "type": "object" },
            },
        },
    });
    schema["definitions"]
        .as_object_mut()
        .expect("the advertised absorb input declares its definitions")
        .extend(cognition_definitions(&["cognition_record"]));
    schema
}

/// The absorb results as the seam's closed branches: a manifest-only consumer reads `status` and
/// knows exactly which members its outcome carries.
fn absorb_output() -> Value {
    json!({
        "type": "array",
        "items": {
            "oneOf": [
                {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["status", "candidate_id"],
                    "properties": {
                        "status": { "const": "pending" },
                        "candidate_id": { "type": "string" },
                    },
                },
                {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["status", "candidate_id"],
                    "properties": {
                        "status": { "const": "dropped" },
                        "candidate_id": { "type": "string" },
                    },
                },
                {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["status", "candidate_id", "record_ids"],
                    "properties": {
                        "status": { "const": "settled" },
                        "candidate_id": { "type": "string" },
                        "record_ids": { "type": "array", "items": { "type": "string" } },
                    },
                },
            ],
        },
    })
}

/// The deliberation arguments, mirroring the pinned `spoke-schemas` request DTOs. `holder` and
/// `target` splice the same pinned definitions `absorb` advertises — the workflow decodes exactly
/// those DTOs, so the manifest and the handler cannot drift apart.
fn deliberate_input() -> Value {
    let mut schema = json!({
        "$id": "https://mindcore.invalid/schemas/tools/deliberate.input.schema.json",
        "type": "object",
        "additionalProperties": false,
        "required": ["space_id", "subject_id", "holder", "target", "evidence", "timeout_ms"],
        "properties": {
            "space_id": { "type": "string", "minLength": 1 },
            "subject_id": { "type": "string", "minLength": 1 },
            "holder": { "$ref": "#/definitions/CognitionReference" },
            "target": { "$ref": "#/definitions/CognitionTarget" },
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
    });
    schema["definitions"] = Value::Object(cognition_definitions(&[
        "CognitionReference",
        "CognitionTarget",
    ]));
    schema
}

/// The deliberation results as the seam's closed branches: `status` alone tells a manifest-only
/// consumer which members the outcome carries. `verdict`, `draft` and `record` are opaque product
/// objects, so their openness is stated rather than left to draft-07's implicit default.
fn deliberate_output() -> Value {
    json!({
        "oneOf": [
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["status", "verdict"],
                "properties": {
                    "status": { "const": "no_change" },
                    "verdict": {
                        "type": "object",
                        "additionalProperties": {},
                        "description": "The validated reasoner verdict.",
                    },
                },
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["status", "verdict", "draft"],
                "properties": {
                    "status": { "const": "needs_review" },
                    "verdict": {
                        "type": "object",
                        "additionalProperties": {},
                        "description": "The validated reasoner verdict.",
                    },
                    "draft": {
                        "type": "object",
                        "additionalProperties": {},
                        "description": "The pinned SPOKE CognitionRecord under review.",
                    },
                },
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["status", "verdict", "record", "mind_state_version"],
                "properties": {
                    "status": { "const": "settled" },
                    "verdict": {
                        "type": "object",
                        "additionalProperties": {},
                        "description": "The validated reasoner verdict.",
                    },
                    "record": {
                        "type": "object",
                        "additionalProperties": {},
                        "description": "The pinned SPOKE CognitionRecord the carrier holds.",
                    },
                    "mind_state_version": { "type": "integer", "minimum": 0 },
                },
            },
        ],
    })
}

#[cfg(test)]
mod tests;
