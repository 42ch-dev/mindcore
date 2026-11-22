//! The tool mapping: the closed argument/result shapes, the authenticated peer resolved to an
//! admitted `Principal`, and the reject kinds a failed workflow surfaces.
//!
//! The handler's responder binding (weak session handle, authenticated peer) is exercised by the
//! loopback task; here the serving path is driven with an already authenticated peer id. The
//! mapping is crate-internal, so these assertions live beside it rather than behind a public API.

use std::sync::Mutex;

use async_trait::async_trait;
use tempfile::TempDir;

use mindcore_spi::{
    AbsorbCandidateOutcome, ContextPack, DeliberateOutcome, DeliberateRequest, DomainLens,
    MindCapabilities, MindWorkflows, SpiError,
};
use mindcore_sqlite::MindStore;
use spoke_operations::{spoke_reject, validate_tool_arguments};
use spoke_schemas::{CognitionRecord, KnowledgeEntry, RetainedRecord};

use super::*;

/// A workflow engine that records what it was handed and answers a configured result.
#[derive(Default)]
struct Fake {
    recall: Mutex<Option<Result<ContextPack, SpiError>>>,
    absorb: Mutex<Option<Result<Vec<AbsorbCandidateOutcome>, SpiError>>>,
    deliberate: Mutex<Option<Result<DeliberateOutcome, SpiError>>>,
    seen: Mutex<Vec<String>>,
}

impl Fake {
    fn reply_recall(&self, result: Result<ContextPack, SpiError>) {
        *self.recall.lock().expect("recall reply") = Some(result);
    }

    fn reply_absorb(&self, result: Result<Vec<AbsorbCandidateOutcome>, SpiError>) {
        *self.absorb.lock().expect("absorb reply") = Some(result);
    }

    fn reply_deliberate(&self, result: Result<DeliberateOutcome, SpiError>) {
        *self.deliberate.lock().expect("deliberate reply") = Some(result);
    }

    fn seen(&self) -> Vec<String> {
        self.seen.lock().expect("seen calls").clone()
    }

    fn record(&self, call: String) {
        self.seen.lock().expect("seen calls").push(call);
    }
}

#[async_trait]
impl MindWorkflows for Fake {
    async fn recall_context(
        &self,
        principal: &Principal,
        request: &MemoryRecallRequest,
        max_tokens: NonZeroUsize,
        _caps: &MindCapabilities<'_>,
    ) -> Result<ContextPack, SpiError> {
        self.record(format!(
            "recall:{}:{}:{}",
            principal.principal_id.as_str(),
            request.space_id.as_str(),
            max_tokens
        ));
        self.recall
            .lock()
            .expect("recall reply")
            .clone()
            .expect("a recall reply is configured")
    }

    async fn absorb(
        &self,
        principal: &Principal,
        request: &MemoryCaptureRequest,
        _lens: Option<&dyn DomainLens>,
        _caps: &MindCapabilities<'_>,
    ) -> Result<Vec<AbsorbCandidateOutcome>, SpiError> {
        self.record(format!(
            "absorb:{}:{}",
            principal.principal_id.as_str(),
            request.idempotency_key.as_str()
        ));
        self.absorb
            .lock()
            .expect("absorb reply")
            .clone()
            .expect("an absorb reply is configured")
    }

    async fn deliberate(
        &self,
        principal: &Principal,
        request: DeliberateRequest<'_>,
        _lens: Option<&dyn DomainLens>,
        _caps: &MindCapabilities<'_>,
    ) -> Result<DeliberateOutcome, SpiError> {
        let evidence: Vec<&str> = request.evidence.iter().map(|item| item.id).collect();
        self.record(format!(
            "deliberate:{}:{}:{}:{}:{}",
            principal.principal_id.as_str(),
            request.space_id,
            request.subject_id,
            request.timeout.as_millis(),
            evidence.join(",")
        ));
        self.deliberate
            .lock()
            .expect("deliberate reply")
            .clone()
            .expect("a deliberate reply is configured")
    }
}

fn principal() -> Principal {
    serde_json::from_value(json!({ "principal_id": "prn_driver", "extensions": {} }))
        .expect("principal JSON matches the schema")
}

fn admission() -> MemoryAdmission {
    Arc::new(|peer_id: &str| match peer_id {
        "peer-driver" => SpokeResult::Ok(principal()),
        _ => spoke_reject(
            SpokeRejectCode::NotAuthorized,
            format!("peer {peer_id} holds no workflow admission"),
            None,
        ),
    })
}

fn tools(fake: Arc<Fake>) -> WorkflowTools {
    WorkflowTools::new(fake, admission(), MindProviders::default())
}

fn expect_ok<T>(result: SpokeResult<T>, context: &str) -> T {
    match result {
        SpokeResult::Ok(value) => value,
        SpokeResult::Reject(reject) => panic!("{context}: unexpectedly rejected with {reject:?}"),
    }
}

fn expect_reject<T: std::fmt::Debug>(result: SpokeResult<T>, context: &str) -> SpokeReject {
    match result {
        SpokeResult::Reject(reject) => reject,
        SpokeResult::Ok(value) => panic!("{context}: unexpectedly accepted with {value:?}"),
    }
}

fn record() -> RetainedRecord {
    serde_json::from_value(json!({
        "schema_version": 1,
        "record_id": "rec_1",
        "space_id": "spc_1",
        "subject_id": "subj_1",
        "kind": "fact",
        "content": { "text": "the north gate is closed at dusk" },
        "scope": { "visibility": "shared" },
        "provenance": {
            "source_context_ids": ["ctx_a"],
            "anchors": [],
            "captured_by": "prn_driver",
            "captured_at": "2026-11-20T09:00:00Z"
        },
        "created_at": "2026-11-20T09:00:00Z",
        "updated_at": "2026-11-20T09:00:00Z",
        "revision": 1,
        "extensions": {}
    }))
    .expect("record JSON matches the schema")
}

fn entry() -> KnowledgeEntry {
    serde_json::from_value(json!({
        "schema_version": 1,
        "entry_id": "ke_1",
        "entry_type": "location",
        "canonical_name": "north gate",
        "status": "confirmed",
        "body": { "summary": "the north gate closes at dusk" },
        "revision": 1,
        "extensions": {}
    }))
    .expect("entry JSON matches the schema")
}

/// One current-view belief over the north-gate proposition.
fn cognition() -> CognitionRecord {
    serde_json::from_value(json!({
        "kind": "belief",
        "order": 1,
        "record_id": "cog_1",
        "holder": { "kind": "knowledge_entry", "namespace": "mindcore", "id": "ke_wren" },
        "target": {
            "carrier": { "kind": "knowledge_entry", "namespace": "mindcore", "id": "ke_gate" },
            "target_id": "prop_gate_closed",
            "kind": "proposition"
        },
        "assessor": { "kind": "knowledge_entry", "namespace": "mindcore", "id": "ke_wren" },
        "assessor_role": "holder_self_report",
        "basis": {
            "kind": "evidence",
            "evidence": [{ "kind": "memory_record", "namespace": "mindcore", "id": "rec_1" }]
        },
        "assessment_context": { "context_id": "ctx_dusk" },
        "effective_at": "2026-11-20T18:00:00Z",
        "assessed_at": "2026-11-20T18:05:00Z",
        "view": "current",
        "q": {
            "value": 0.7,
            "assertion": "belief_attribution",
            "method": "single_self_report",
            "calibration": "uncalibrated"
        },
        "extensions": {}
    }))
    .expect("cognition record JSON matches the schema")
}

fn recall_arguments() -> Value {
    json!({
        "request": {
            "space_id": "spc_1",
            "subject_id": "subj_1",
            "selection": { "mode": "shared_only" },
            "budget": { "max_items": 10 }
        },
        "max_tokens": 64
    })
}

fn absorb_arguments() -> Value {
    json!({
        "request": {
            "space_id": "spc_1",
            "subject_id": "subj_1",
            "capture_scope": { "visibility": "shared" },
            "content": { "text": "the north gate is closed at dusk" },
            "idempotency_key": "cap_1"
        }
    })
}

fn deliberate_arguments() -> Value {
    json!({
        "space_id": "spc_1",
        "subject_id": "subj_1",
        "holder": { "kind": "knowledge_entry", "namespace": "mindcore", "id": "ke_wren" },
        "target": {
            "carrier": { "kind": "knowledge_entry", "namespace": "mindcore", "id": "ke_gate" },
            "target_id": "prop_gate_closed",
            "kind": "proposition"
        },
        "evidence": [{ "id": "ev_1", "text": "the north gate closed at dusk" }],
        "timeout_ms": 5000
    })
}

#[tokio::test]
async fn recall_context_round_trips_the_pack() {
    let fake = Arc::new(Fake::default());
    fake.reply_recall(Ok(ContextPack {
        records: vec![record()],
        knowledge: vec![entry()],
        tokens_used: 7,
    }));

    let value = expect_ok(
        tools(Arc::clone(&fake))
            .call(RECALL_CONTEXT, "peer-driver", recall_arguments())
            .await,
        "recall context",
    );

    assert_eq!(value["tokens_used"], 7);
    assert_eq!(value["records"][0]["record_id"], "rec_1");
    assert_eq!(value["knowledge"][0]["entry_id"], "ke_1");
    assert_eq!(
        fake.seen(),
        ["recall:prn_driver:spc_1:64"],
        "the admitted principal and decoded budget reach the workflow"
    );
}

#[tokio::test]
async fn absorb_round_trips_every_candidate_status() {
    let fake = Arc::new(Fake::default());
    fake.reply_absorb(Ok(vec![
        AbsorbCandidateOutcome::Pending {
            candidate_id: "cand_pending".into(),
        },
        AbsorbCandidateOutcome::Dropped {
            candidate_id: "cand_dropped".into(),
        },
        AbsorbCandidateOutcome::Settled {
            candidate_id: "cand_settled".into(),
            record_ids: vec!["rec_1".into()],
        },
    ]));

    let tool = tools(Arc::clone(&fake));
    let value = expect_ok(
        tool.call(ABSORB, "peer-driver", absorb_arguments()).await,
        "absorb",
    );

    assert_eq!(
        value,
        json!([
            { "status": "pending", "candidate_id": "cand_pending" },
            { "status": "dropped", "candidate_id": "cand_dropped" },
            {
                "status": "settled",
                "candidate_id": "cand_settled",
                "record_ids": ["rec_1"]
            }
        ])
    );
    assert_eq!(fake.seen(), ["absorb:prn_driver:cap_1"]);

    let output = output_of(&tool, ABSORB);
    assert_closed(&output, &output, &mut Vec::new());
    assert!(
        accepts(&output, &output, &value),
        "every emitted outcome matches an advertised branch: {value}"
    );
    assert!(
        !accepts(
            &output,
            &output,
            &json!([{ "status": "settled", "candidate_id": "cand_settled" }])
        ),
        "a settled outcome carries record_ids; the advertised branch without it is not settled"
    );
    assert!(
        !accepts(
            &output,
            &output,
            &json!([{ "status": "pending", "candidate_id": "cand_pending", "record_ids": [] }])
        ),
        "record_ids belong to the settled branch, never to pending"
    );
}

#[tokio::test]
async fn deliberate_round_trips_every_outcome_branch() {
    let fake = Arc::new(Fake::default());
    let tool = tools(Arc::clone(&fake));

    fake.reply_deliberate(Ok(DeliberateOutcome::NoChange {
        verdict: json!({ "recommendation": { "kind": "none" } }),
    }));
    let value = expect_ok(
        tool.call(DELIBERATE, "peer-driver", deliberate_arguments())
            .await,
        "deliberate no change",
    );
    assert_eq!(value["status"], "no_change");
    assert_eq!(value["verdict"]["recommendation"]["kind"], "none");
    assert!(value.get("draft").is_none(), "no draft is written");
    let output = output_of(&tool, DELIBERATE);
    assert!(
        accepts(&output, &output, &value),
        "a no_change outcome matches its advertised branch: {value}"
    );

    fake.reply_deliberate(Ok(DeliberateOutcome::NeedsReview {
        verdict: json!({ "recommendation": { "kind": "promote" } }),
        draft: cognition(),
    }));
    let value = expect_ok(
        tool.call(DELIBERATE, "peer-driver", deliberate_arguments())
            .await,
        "deliberate needs review",
    );
    assert_eq!(value["status"], "needs_review");
    assert_eq!(value["draft"]["record_id"], "cog_1");
    assert!(value.get("record").is_none(), "nothing settled");
    assert!(
        accepts(&output, &output, &value),
        "a needs_review outcome matches its advertised branch: {value}"
    );

    fake.reply_deliberate(Ok(DeliberateOutcome::Settled {
        verdict: json!({ "recommendation": { "kind": "promote" } }),
        record: cognition(),
        mind_state_version: 3,
    }));
    let value = expect_ok(
        tool.call(DELIBERATE, "peer-driver", deliberate_arguments())
            .await,
        "deliberate settled",
    );
    assert_eq!(value["status"], "settled");
    assert_eq!(value["record"]["record_id"], "cog_1");
    assert_eq!(value["mind_state_version"], 3);
    assert_closed(&output, &output, &mut Vec::new());
    assert!(
        accepts(&output, &output, &value),
        "a settled outcome matches its advertised branch: {value}"
    );
    assert!(
        !accepts(
            &output,
            &output,
            &json!({ "status": "needs_review", "verdict": {} })
        ),
        "a review outcome carries its draft"
    );
    assert!(
        !accepts(
            &output,
            &output,
            &json!({ "status": "no_change", "verdict": {}, "draft": {} })
        ),
        "a no_change outcome carries no draft"
    );

    assert_eq!(
        fake.seen(),
        [
            "deliberate:prn_driver:spc_1:subj_1:5000:ev_1",
            "deliberate:prn_driver:spc_1:subj_1:5000:ev_1",
            "deliberate:prn_driver:spc_1:subj_1:5000:ev_1",
        ]
    );
}

#[tokio::test]
async fn malformed_arguments_are_invalid_input() {
    let cases = [
        (
            "recall without a budget",
            RECALL_CONTEXT,
            json!({ "request": recall_arguments()["request"].clone() }),
        ),
        (
            "recall with a zero budget",
            RECALL_CONTEXT,
            json!({ "request": recall_arguments()["request"].clone(), "max_tokens": 0 }),
        ),
        (
            "recall carrying a caller principal",
            RECALL_CONTEXT,
            json!({
                "request": recall_arguments()["request"].clone(),
                "max_tokens": 64,
                "principal_id": "prn_forged"
            }),
        ),
        ("absorb without a request", ABSORB, json!({})),
        (
            "deliberate without evidence ids",
            DELIBERATE,
            json!({
                "space_id": "spc_1",
                "subject_id": "subj_1",
                "holder": { "kind": "knowledge_entry", "namespace": "mindcore", "id": "ke_wren" },
                "target": {
                    "carrier": { "kind": "knowledge_entry", "namespace": "mindcore", "id": "ke_gate" },
                    "target_id": "prop_gate_closed",
                    "kind": "proposition"
                },
                "evidence": [{ "text": "the north gate closed at dusk" }],
                "timeout_ms": 5000
            }),
        ),
        (
            "deliberate with a zero timeout",
            DELIBERATE,
            json!({
                "space_id": "spc_1",
                "subject_id": "subj_1",
                "holder": { "kind": "knowledge_entry", "namespace": "mindcore", "id": "ke_wren" },
                "target": {
                    "carrier": { "kind": "knowledge_entry", "namespace": "mindcore", "id": "ke_gate" },
                    "target_id": "prop_gate_closed",
                    "kind": "proposition"
                },
                "evidence": [{ "id": "ev_1", "text": "the north gate closed at dusk" }],
                "timeout_ms": 0
            }),
        ),
    ];

    for (case, capability_id, arguments) in cases {
        let fake = Arc::new(Fake::default());
        let reject = expect_reject(
            tools(fake)
                .call(capability_id, "peer-driver", arguments)
                .await,
            case,
        );
        assert_eq!(reject.code, SpokeRejectCode::InvalidInput, "{case}");
    }
}

#[tokio::test]
async fn unadmitted_peers_are_not_authorized() {
    let fake = Arc::new(Fake::default());
    let reject = expect_reject(
        tools(Arc::clone(&fake))
            .call(RECALL_CONTEXT, "peer-stranger", recall_arguments())
            .await,
        "stranger",
    );

    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
    assert!(
        fake.seen().is_empty(),
        "no workflow runs for a peer the host does not admit"
    );
}

#[tokio::test]
async fn settlement_refusals_keep_their_code_and_details() {
    let refusal = SpokeReject {
        code: SpokeRejectCode::RevisionConflict,
        message: "the record moved during provider work".into(),
        details: Some(Map::from_iter([("retry_after_ms".to_string(), json!(25))])),
    };
    let fake = Arc::new(Fake::default());
    fake.reply_absorb(Err(SpiError::Settlement(refusal.clone())));

    let reject = expect_reject(
        tools(fake)
            .call(ABSORB, "peer-driver", absorb_arguments())
            .await,
        "settlement refusal",
    );

    assert_eq!(reject, refusal);
}

#[tokio::test]
async fn workflow_failures_map_to_stable_reject_kinds() {
    let fake = Arc::new(Fake::default());
    fake.reply_recall(Err(SpiError::DeadlineExceeded));
    let reject = expect_reject(
        tools(fake)
            .call(RECALL_CONTEXT, "peer-driver", recall_arguments())
            .await,
        "deadline",
    );
    assert_eq!(reject.code, SpokeRejectCode::InternalError);
    assert_eq!(
        reject
            .details
            .as_ref()
            .and_then(|details| details.get("kind"))
            .and_then(Value::as_str),
        Some("deadline_exceeded")
    );
    assert_eq!(
        reject
            .details
            .as_ref()
            .and_then(|details| details.get("retry_class"))
            .and_then(Value::as_str),
        Some("retryable"),
        "a bounded timeout committed nothing, so the caller may repeat the call"
    );

    let internal_cases = [
        (SpiError::EmbeddingsUnavailable, "embeddings_unavailable"),
        (
            SpiError::EmbeddingFailed("provider 500".into()),
            "embedding_failed",
        ),
        (SpiError::ReasonerUnavailable, "reasoner_unavailable"),
        (
            SpiError::ReasonerFailed("socket closed".into()),
            "reasoner_failed",
        ),
        (
            SpiError::InvalidReasonerOutput("not json".into()),
            "invalid_reasoner_output",
        ),
    ];

    for (error, kind) in internal_cases {
        let fake = Arc::new(Fake::default());
        fake.reply_recall(Err(error));
        let reject = expect_reject(
            tools(fake)
                .call(RECALL_CONTEXT, "peer-driver", recall_arguments())
                .await,
            kind,
        );
        assert_eq!(reject.code, SpokeRejectCode::InternalError, "{kind}");
        assert_eq!(
            reject
                .details
                .as_ref()
                .and_then(|details| details.get("kind"))
                .and_then(Value::as_str),
            Some(kind)
        );
        assert!(
            !reject.message.contains("provider 500") && !reject.message.contains("socket closed"),
            "{kind}: provider internals stay off the wire"
        );
    }

    let fake = Arc::new(Fake::default());
    fake.reply_recall(Err(SpiError::PolicyRejected(
        "domain redline on the north gate".into(),
    )));
    let reject = expect_reject(
        tools(fake)
            .call(RECALL_CONTEXT, "peer-driver", recall_arguments())
            .await,
        "policy",
    );
    assert_eq!(
        reject.code,
        SpokeRejectCode::InvalidInput,
        "a policy veto is the proposal's refusal, not a lost admission"
    );
    assert_eq!(
        reject
            .details
            .as_ref()
            .and_then(|details| details.get("kind"))
            .and_then(Value::as_str),
        Some("policy_rejected")
    );
    assert_eq!(
        reject.message, "domain redline on the north gate",
        "the host's reason reaches the caller"
    );

    let fake = Arc::new(Fake::default());
    fake.reply_recall(Err(SpiError::InvalidInput(
        "max_tokens is unsupported".into(),
    )));
    let reject = expect_reject(
        tools(fake)
            .call(RECALL_CONTEXT, "peer-driver", recall_arguments())
            .await,
        "invalid input",
    );
    assert_eq!(reject.code, SpokeRejectCode::InvalidInput);
    assert_eq!(reject.message, "max_tokens is unsupported");
}

/// The advertised ABI is the served ABI: an argument built only from the manifest decodes, and every
/// member the manifest requires is a member the handler requires.
#[tokio::test]
async fn advertised_input_schemas_match_the_decoded_arguments() {
    let fake = Arc::new(Fake::default());
    let tool = tools(Arc::clone(&fake));

    for descriptor in tool.descriptors() {
        let capability_id = descriptor.capability_id.as_str();
        let input = Value::Object(descriptor.input.clone());
        assert_closed(&input, &input, &mut Vec::new());

        match capability_id {
            RECALL_CONTEXT => fake.reply_recall(Ok(ContextPack {
                records: Vec::new(),
                knowledge: Vec::new(),
                tokens_used: 0,
            })),
            ABSORB => fake.reply_absorb(Ok(Vec::new())),
            DELIBERATE => {
                fake.reply_deliberate(Ok(DeliberateOutcome::NoChange { verdict: json!({}) }))
            }
            other => panic!("unadvertised tool {other}"),
        }

        let arguments = minimal(&input, &input);
        expect_ok(
            tool.call(capability_id, "peer-driver", arguments.clone())
                .await,
            &format!("{capability_id}: the manifest's own minimal argument"),
        );

        let required: Vec<String> = input["required"]
            .as_array()
            .expect("an advertised input lists its required members")
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
        for member in required {
            let mut missing = arguments.clone();
            missing
                .as_object_mut()
                .expect("the advertised argument is an object")
                .remove(&member);

            // why: the advertised schema refuses the argument, not only serde.
            match validate_tool_arguments(&descriptor, &missing) {
                SpokeResult::Reject(reject) => {
                    assert_eq!(
                        reject.code,
                        SpokeRejectCode::InvalidInput,
                        "{capability_id}: dropping {member}"
                    );
                    let named: Vec<&str> = reject
                        .details
                        .as_ref()
                        .and_then(|details| details.get("missing"))
                        .and_then(Value::as_array)
                        .expect("the structural gate names the missing members")
                        .iter()
                        .filter_map(Value::as_str)
                        .collect();
                    assert!(
                        named.contains(&member.as_str()),
                        "{capability_id}: dropping {member} names {named:?}"
                    );
                }
                SpokeResult::Ok(()) => panic!(
                    "{capability_id}: the advertised schema admits an argument without {member}"
                ),
            }

            // why: the handler requires it too, so advertisement and behavior agree.
            let reject = expect_reject(
                tool.call(capability_id, "peer-driver", missing).await,
                &format!("{capability_id}: argument without {member}"),
            );
            assert_eq!(
                reject.code,
                SpokeRejectCode::InvalidInput,
                "{capability_id}: dropping {member}"
            );
        }
    }

    // why: the provider refuses `cognition_filter`, so the served shape does not advertise it either.
    let recall = tool
        .descriptors()
        .into_iter()
        .find(|descriptor| descriptor.capability_id.as_str() == RECALL_CONTEXT)
        .expect("recall context is advertised");
    assert!(
        recall.input["properties"]["request"]["properties"]
            .get("cognition_filter")
            .is_none(),
        "a refused member is never advertised"
    );
}

/// One advertised output, resolved against itself: the result ABI a manifest-only consumer reads.
fn output_of(tool: &WorkflowTools, capability_id: &str) -> Value {
    tool.descriptors()
        .into_iter()
        .find(|descriptor| descriptor.capability_id.as_str() == capability_id)
        .map(|descriptor| Value::Object(descriptor.output))
        .unwrap_or_else(|| panic!("{capability_id} is advertised"))
}

/// Whether `value` is admitted by the advertised `schema`, over the constructs these descriptors
/// use: branches, `const`/`enum`, the primitive types, closed objects and typed members.
fn accepts(schema: &Value, root: &Value, value: &Value) -> bool {
    let schema = resolve(schema, root);
    if let Some(branches) = schema.get("oneOf").and_then(Value::as_array) {
        return branches
            .iter()
            .filter(|branch| accepts(branch, root, value))
            .count()
            == 1;
    }
    if let Some(expected) = schema.get("const") {
        return value == expected;
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        return values.contains(value);
    }
    match schema.get("type").and_then(Value::as_str) {
        Some("object") => {
            let Some(object) = value.as_object() else {
                return false;
            };
            let properties = schema.get("properties").and_then(Value::as_object);
            for name in schema
                .get("required")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                if !object.contains_key(name) {
                    return false;
                }
            }
            object.iter().all(|(name, member)| {
                match properties.and_then(|properties| properties.get(name)) {
                    Some(property) => accepts(property, root, member),
                    None => schema.get("additionalProperties") != Some(&Value::Bool(false)),
                }
            })
        }
        Some("array") => value.as_array().is_some_and(|items| {
            items
                .iter()
                .all(|item| accepts(&schema["items"], root, item))
        }),
        Some("integer") => value.as_i64().is_some() || value.as_u64().is_some(),
        Some("number") => value.is_number(),
        Some("string") => value.is_string(),
        Some("boolean") => value.is_boolean(),
        Some("null") => value.is_null(),
        None => true,
        other => panic!("the advertised schema uses an unsupported type: {other:?}"),
    }
}

/// Resolve the local `#/…` references an advertised subschema uses against the advertised document.
fn resolve<'a>(schema: &'a Value, root: &'a Value) -> &'a Value {
    match schema.get("$ref").and_then(Value::as_str) {
        Some(reference) => root
            .pointer(&reference[1..])
            .unwrap_or_else(|| panic!("unresolvable $ref {reference}")),
        None => schema,
    }
}

/// Every object an advertised subschema describes is closed: an `{}` object cannot pass the manifest
/// and then fail decoding. Every `$ref` must resolve inside the manifest; a manifest-only client
/// never fetches a schema.
fn assert_closed(schema: &Value, root: &Value, seen: &mut Vec<String>) {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        assert!(
            reference.starts_with('#'),
            "an advertised $ref must stay inside the manifest: {reference}"
        );
        if seen.iter().any(|visited| visited == reference) {
            return;
        }
        seen.push(reference.to_string());
    }
    let schema = resolve(schema, root);

    for key in ["oneOf", "anyOf"] {
        for branch in schema
            .get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            assert_closed(branch, root, seen);
        }
    }
    if let Some(nested) = schema.get("definitions").and_then(Value::as_object) {
        for definition in nested.values() {
            assert_closed(definition, root, seen);
        }
    }

    match schema.get("type").and_then(Value::as_str) {
        Some("object") => {
            assert!(
                schema
                    .get("additionalProperties")
                    .is_some_and(|closed| closed == &Value::Bool(false) || closed.is_object()),
                "an advertised object must be closed: {schema}"
            );
            let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
                return;
            };
            for property in properties.values() {
                assert_closed(property, root, seen);
            }
            for name in schema
                .get("required")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                assert!(
                    properties.contains_key(name),
                    "advertised required member {name} has no schema: {schema}"
                );
            }
        }
        Some("array") => assert_closed(
            schema
                .get("items")
                .expect("an advertised array names its items"),
            root,
            seen,
        ),
        _ => {}
    }
}

/// The smallest argument an advertised subschema admits — what a client reading only the manifest
/// would build. Strings are `x`-repeat, which satisfies every advertised `minLength` and pattern.
fn minimal(schema: &Value, root: &Value) -> Value {
    let schema = resolve(schema, root);
    if let Some(value) = schema.get("const") {
        return value.clone();
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        return values
            .first()
            .expect("an advertised enum has a member")
            .clone();
    }
    if let Some(branches) = schema
        .get("oneOf")
        .or_else(|| schema.get("anyOf"))
        .and_then(Value::as_array)
    {
        return minimal(&branches[0], root);
    }

    match schema.get("type").and_then(Value::as_str) {
        Some("object") => {
            let properties = schema.get("properties").and_then(Value::as_object);
            let mut object = Map::new();
            for name in schema
                .get("required")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                let member = properties
                    .and_then(|properties| properties.get(name))
                    .unwrap_or_else(|| panic!("advertised required member {name} has no schema"));
                object.insert(name.to_string(), minimal(member, root));
            }
            Value::Object(object)
        }
        Some("array") => {
            let items = schema
                .get("items")
                .expect("an advertised array names its items");
            let count = schema
                .get("minItems")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .max(1);
            Value::Array((0..count).map(|_| minimal(items, root)).collect())
        }
        Some("integer") => Value::from(schema.get("minimum").and_then(Value::as_i64).unwrap_or(1)),
        Some("number") => Value::from(schema.get("minimum").and_then(Value::as_f64).unwrap_or(1.0)),
        Some("boolean") => Value::Bool(false),
        Some("string") => {
            if schema.get("format").and_then(Value::as_str) == Some("date-time") {
                return Value::from("2026-01-01T00:00:00Z");
            }
            let length = schema
                .get("minLength")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .max(1);
            Value::from("x".repeat(length as usize))
        }
        other => {
            panic!("the advertised schema leaves a member unconstrained ({other:?}): {schema}")
        }
    }
}

/// The adoption tool over a real store: a shared source holding the entry to adopt and one unrelated
/// occupant, the caller's destination already holding a different entry under that occupant's id, and
/// one admitted principal with a route beside one admitted without.
async fn adoption_tool() -> (TempDir, AdoptionTool) {
    let dir = TempDir::new().expect("temporary directory");
    let store = MindStore::open(&dir.path().join("adoption.db"))
        .await
        .expect("open store");
    for (collection, entry_id) in [
        ("shared", "ke_shared"),
        ("shared", "ke_occupant"),
        // the destination holds an entry this adoption does not own under the source's id.
        ("wren", "ke_occupant"),
    ] {
        let mut seeded = entry();
        seeded.entry_id = entry_id.to_string();
        match store
            .knowledge(KnowledgeScope::new(collection))
            .put_knowledge_entry(seeded, None)
            .await
        {
            SpokeResult::Ok(_) => {}
            SpokeResult::Reject(reject) => panic!("seed {collection}/{entry_id}: {reject:?}"),
        }
    }
    let tool = AdoptionTool::new(
        store.pool(),
        Arc::new(|peer_id: &str| match peer_id {
            "peer-driver" => SpokeResult::Ok(principal()),
            "peer-unrouted" => SpokeResult::Ok(
                serde_json::from_value(json!({ "principal_id": "prn_other", "extensions": {} }))
                    .expect("principal JSON matches the schema"),
            ),
            _ => spoke_reject(
                SpokeRejectCode::NotAuthorized,
                format!("peer {peer_id} holds no workflow admission"),
                None,
            ),
        }),
        "shared".to_string(),
        Arc::new(BTreeMap::from([(
            "prn_driver".to_string(),
            KnowledgeScope::new("wren"),
        )])),
    );
    (dir, tool)
}

/// The routed principal adopts onto its own route: the four refusals the local function produces
/// travel unchanged, the first call creates the copy and the repeat serves the stored one.
#[tokio::test]
async fn a_routed_principal_adopts_once_and_the_local_refusals_pass_through() {
    let (_dir, tool) = adoption_tool().await;

    for (case, arguments, code) in [
        (
            "an entry the shared collection does not hold",
            json!({ "entry_id": "ke_absent", "expected_source_revision": 1 }),
            SpokeRejectCode::KnowledgeEntryNotFound,
        ),
        (
            "a stale expected source revision",
            json!({ "entry_id": "ke_shared", "expected_source_revision": 2 }),
            SpokeRejectCode::RevisionConflict,
        ),
        (
            "a destination slot another entry holds",
            json!({ "entry_id": "ke_occupant", "expected_source_revision": 1 }),
            SpokeRejectCode::KnowledgeEntryAlreadyExists,
        ),
        (
            "no source revision to anchor at",
            json!({ "entry_id": "ke_shared", "expected_source_revision": 0 }),
            SpokeRejectCode::InvalidInput,
        ),
    ] {
        let reject = expect_reject(tool.dispatch("peer-driver", arguments).await, case);
        assert_eq!(reject.code, code, "{case}: {reject:?}");
    }

    let arguments = json!({ "entry_id": "ke_shared", "expected_source_revision": 1 });
    let created = expect_ok(
        tool.dispatch("peer-driver", arguments.clone()).await,
        "adopt",
    );
    assert_eq!(created["outcome"], "created");
    assert_eq!(created["entry"]["entry_id"], "ke_shared");
    assert_eq!(created["entry"]["revision"], 1);

    let again = expect_ok(
        tool.dispatch("peer-driver", arguments).await,
        "repeat adoption",
    );
    assert_eq!(again["outcome"], "already_present");
    assert_eq!(
        again["entry"]["revision"], 1,
        "the repeat serves the stored copy unchanged"
    );
}

/// An admitted principal without a route is refused even though another principal is routed, and the
/// refusal names neither a collection nor a principal.
#[tokio::test]
async fn an_unrouted_principal_is_refused_without_target_details() {
    let (_dir, tool) = adoption_tool().await;
    let reject = expect_reject(
        tool.dispatch(
            "peer-unrouted",
            json!({ "entry_id": "ke_shared", "expected_source_revision": 1 }),
        )
        .await,
        "unrouted adoption",
    );

    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
    assert!(reject.details.is_none(), "{reject:?}");
    assert!(
        !reject.message.contains("wren"),
        "the refusal names no destination collection: {}",
        reject.message
    );
}

/// The source half of the adoption is read-only by construction: the write its port must expose
/// refuses before it touches the store, and the shared entry keeps the revision it was seeded at.
#[tokio::test]
async fn the_adoption_source_handle_refuses_a_write() {
    let dir = TempDir::new().expect("temporary directory");
    let store = MindStore::open(&dir.path().join("source.db"))
        .await
        .expect("open store");
    let mut seeded = entry();
    seeded.entry_id = "ke_shared".to_string();
    expect_ok(
        store
            .knowledge(KnowledgeScope::new("shared"))
            .put_knowledge_entry(seeded, None)
            .await,
        "seed the shared source",
    );

    let pool = store.pool();
    let mut transaction = pool.begin().await.expect("begin the call's transaction");
    let refusal = {
        let gate = tokio::sync::Mutex::new(&mut transaction);
        let source = AdoptionSource {
            collection: "shared",
            gate: &gate,
        };
        source.put_knowledge_entry(entry(), None).await
    };
    transaction
        .rollback()
        .await
        .expect("drop the call's transaction");

    let reject = expect_reject(refusal, "a write through the adoption source");
    assert_eq!(reject.code, SpokeRejectCode::InternalError, "{reject:?}");
    assert_eq!(
        reject.message, "the adoption source is read-only",
        "{reject:?}"
    );
    let stored = expect_ok(
        store
            .knowledge(KnowledgeScope::new("shared"))
            .get_knowledge_entry("ke_shared")
            .await,
        "read the shared source back",
    );
    assert_eq!(
        stored.revision,
        Some(1),
        "the shared entry is the one seeded, not a rewrite: {stored:?}"
    );
}

/// Routing is this tool's authorization, so it is answered before the payload is looked at: an
/// admitted principal without a route is refused with `NotAuthorized` even when the arguments would
/// not decode.
#[tokio::test]
async fn an_unrouted_principal_is_refused_before_its_arguments_are_read() {
    let (_dir, tool) = adoption_tool().await;
    let reject = expect_reject(
        tool.dispatch(
            "peer-unrouted",
            json!({ "entry_id": "ke_shared", "unexpected": true }),
        )
        .await,
        "an unrouted adoption with arguments that do not decode",
    );

    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized, "{reject:?}");
    assert!(reject.details.is_none(), "{reject:?}");
}

/// Neither a refused payload nor a storage fault carries its diagnostic: the wire keeps the closed
/// class, and the serde, SQL and stored-payload text stays host-side.
#[tokio::test]
async fn adoption_refusals_carry_no_raw_diagnostics() {
    let (_dir, tool) = adoption_tool().await;

    let reject = expect_reject(
        tool.dispatch("peer-driver", json!({ "entry_id": "ke_shared" }))
            .await,
        "adoption without an expected source revision",
    );
    assert_eq!(reject.code, SpokeRejectCode::InvalidInput, "{reject:?}");
    assert_eq!(
        reject.message, "the adoption arguments are entry_id and expected_source_revision",
        "the refusal names the advertised shape, not the serde diagnostic"
    );
    assert_eq!(
        reject
            .details
            .as_ref()
            .and_then(|details| details.get("kind"))
            .and_then(Value::as_str),
        Some("arguments"),
        "{reject:?}"
    );

    // why: a stored payload the store cannot decode fails the source read itself, which is the
    // storage fault whose decoder text must not travel.
    sqlx::query(
        "UPDATE knowledge_entries SET payload = 'not an entry' \
         WHERE collection = 'shared' AND entry_id = 'ke_shared'",
    )
    .execute(&tool.pool)
    .await
    .expect("corrupt the stored payload");
    let reject = expect_reject(
        tool.dispatch(
            "peer-driver",
            json!({ "entry_id": "ke_shared", "expected_source_revision": 1 }),
        )
        .await,
        "adoption of a stored payload that does not decode",
    );
    assert_eq!(reject.code, SpokeRejectCode::InternalError, "{reject:?}");
    assert_eq!(reject.message, "knowledge adoption could not complete");
    assert_eq!(
        reject
            .details
            .as_ref()
            .and_then(|details| details.get("kind"))
            .and_then(Value::as_str),
        Some("store"),
        "{reject:?}"
    );
    let refusal = format!("{reject:?}");
    assert!(
        !refusal.contains("payload") && !refusal.contains("shared") && !refusal.contains("SELECT"),
        "the refusal carries no stored text, no collection and no statement: {refusal}"
    );
}
