//! The tool mapping: the closed argument/result shapes, the authenticated peer resolved to an
//! admitted `Principal`, and the reject kinds a failed workflow surfaces.
//!
//! The handler's responder binding (weak session handle, authenticated peer) is exercised by the
//! loopback task; here the serving path is driven with an already authenticated peer id.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Map, Value};

use mindcore_runtime::tools::{ABSORB, DELIBERATE, RECALL_CONTEXT};
use mindcore_runtime::{MindProviders, WorkflowTools};
use mindcore_spi::{
    AbsorbCandidateOutcome, ContextPack, DeliberateOutcome, DeliberateRequest, DomainLens,
    MindCapabilities, MindWorkflows, SpiError,
};
use spoke_connect::remote::MemoryAdmission;
use spoke_operations::{spoke_reject, SpokeReject, SpokeRejectCode, SpokeResult};
use spoke_schemas::{
    CognitionRecord, KnowledgeEntry, MemoryCaptureRequest, MemoryRecallRequest, Principal,
    RetainedRecord,
};

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

fn expect_ok(result: SpokeResult<Value>, context: &str) -> Value {
    match result {
        SpokeResult::Ok(value) => value,
        SpokeResult::Reject(reject) => panic!("{context}: unexpectedly rejected with {reject:?}"),
    }
}

fn expect_reject(result: SpokeResult<Value>, context: &str) -> SpokeReject {
    match result {
        SpokeResult::Reject(reject) => reject,
        SpokeResult::Ok(value) => panic!("{context}: unexpectedly accepted with {value}"),
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

    let value = expect_ok(
        tools(Arc::clone(&fake))
            .call(ABSORB, "peer-driver", absorb_arguments())
            .await,
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
            tools(fake).call(capability_id, "peer-driver", arguments).await,
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
        details: Some(Map::from_iter([(
            "retry_after_ms".to_string(),
            json!(25),
        )])),
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
    let internal_cases = [
        (SpiError::DeadlineExceeded, "deadline_exceeded"),
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
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
    assert_eq!(
        reject
            .details
            .as_ref()
            .and_then(|details| details.get("kind"))
            .and_then(Value::as_str),
        Some("policy_rejected")
    );
    assert!(!reject.message.contains("domain redline"));

    let fake = Arc::new(Fake::default());
    fake.reply_recall(Err(SpiError::InvalidInput("max_tokens is unsupported".into())));
    let reject = expect_reject(
        tools(fake)
            .call(RECALL_CONTEXT, "peer-driver", recall_arguments())
            .await,
        "invalid input",
    );
    assert_eq!(reject.code, SpokeRejectCode::InvalidInput);
    assert_eq!(reject.message, "max_tokens is unsupported");
}
