//! External adapter proof: a consumer of only the public `mindcore-spi` API
//! implements every capability trait and the `MindWorkflows` surface, then
//! drives the included `PromptReasoner`, `NoEmbeddings` and `DefaultPolicy`
//! bridges. No storage crate, Nexus or network dependency is involved.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use mindcore_spi::{
    AbsorbCandidateOutcome, CognitiveTaskKind, ContextPack, DefaultPolicy, DeliberateOutcome,
    DeliberateRequest, DomainLens, Embedder, EmbeddingIdentity, Evidence, LensOutput,
    MindCapabilities, MindWorkflows, NoEmbeddings, PolicyDecision, PolicyHook, PromptReasoner,
    Reasoner, SettlementDraft, SpiError, StructuredReasonRequest,
};
use spoke_schemas::data::cognition::{CognitionReference, CognitionTarget};
use spoke_schemas::ops::memory_candidates_review_request::Decision;
use spoke_schemas::{
    CognitionRecord, MemoryCandidate, MemoryCaptureRequest, MemoryRecallRequest, Principal,
};

// ---------- wire fixtures (same shapes the SPOKE schema tests pin) ----------

fn principal() -> Principal {
    serde_json::from_value(json!({ "principal_id": "p_1", "extensions": {} }))
        .expect("principal parses")
}

fn recall_request() -> MemoryRecallRequest {
    serde_json::from_value(json!({
        "space_id": "spc_local",
        "subject_id": "subj_mira",
        "selection": { "mode": "subject_all" },
        "budget": { "max_items": 4, "max_bytes": 1024 },
    }))
    .expect("recall request parses")
}

fn capture_request() -> MemoryCaptureRequest {
    serde_json::from_value(json!({
        "space_id": "spc_local",
        "subject_id": "subj_mira",
        "capture_scope": { "visibility": "shared" },
        "content": { "text": "a pending capture the mock adapter settles" },
        "idempotency_key": "cap-mock-1"
    }))
    .expect("capture request parses")
}

fn pending_candidate() -> MemoryCandidate {
    serde_json::from_str(
        r#"{
          "schema_version": 1,
          "candidate_id": "cand_pending",
          "space_id": "spc_local",
          "subject_id": "subj_mira",
          "capture_scope": {"visibility": "shared"},
          "proposed": {
            "kind": "fact",
            "content": {"text": "Mira promised to return before dawn."}
          },
          "classification": {"disposition": "promote", "rationale_codes": []},
          "status": "pending",
          "provenance": {
            "source_context_ids": [],
            "anchors": [],
            "captured_by": "prn_host",
            "captured_at": "2026-10-28T09:00:00Z"
          },
          "created_at": "2026-10-28T09:00:00Z",
          "revision": 1,
          "extensions": {}
        }"#,
    )
    .expect("pending candidate parses")
}

fn promote_decision() -> Decision {
    serde_json::from_value(json!({ "action": "promote" })).expect("promote decision parses")
}

fn belief_record() -> CognitionRecord {
    serde_json::from_value(json!({
        "kind": "belief",
        "order": 1,
        "record_id": "cog_mock",
        "holder": { "kind": "knowledge_entry", "id": "ke_wren", "namespace": "mindcore" },
        "target": {
            "carrier": { "kind": "knowledge_entry", "id": "ke_shared", "namespace": "mindcore" },
            "target_id": "prop_north_gate_closed",
            "kind": "proposition"
        },
        "assessor": { "kind": "knowledge_entry", "id": "ke_wren", "namespace": "mindcore" },
        "assessor_role": "holder_self_report",
        "basis": {
            "kind": "evidence",
            "evidence": [{ "kind": "memory_record", "id": "mem_hearsay", "namespace": "mindcore" }]
        },
        "assessment_context": { "context_id": "ctx_dusk" },
        "effective_at": "2026-11-19T18:00:00Z",
        "assessed_at": "2026-11-19T18:05:00Z",
        "view": "current",
        "q": {
            "value": 0.6,
            "assertion": "belief_attribution",
            "method": "single_self_report",
            "calibration": "uncalibrated"
        },
        "extensions": {}
    }))
    .expect("belief record parses")
}

fn holder() -> CognitionReference {
    serde_json::from_value(
        json!({ "kind": "knowledge_entry", "id": "ke_wren", "namespace": "mindcore" }),
    )
    .expect("holder reference parses")
}

fn target() -> CognitionTarget {
    serde_json::from_value(json!({
        "carrier": { "kind": "knowledge_entry", "id": "ke_shared", "namespace": "mindcore" },
        "target_id": "prop_north_gate_closed",
        "kind": "proposition"
    }))
    .expect("cognition target parses")
}

// ---------- the external test double ----------

struct MockEmbedder {
    identity: EmbeddingIdentity,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Embedder for MockEmbedder {
    fn identity(&self) -> Option<&EmbeddingIdentity> {
        Some(&self.identity)
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, SpiError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(texts
            .iter()
            .map(|text| vec![text.len() as f32; self.identity.dim])
            .collect())
    }
}

struct AutoPolicy;

#[async_trait]
impl PolicyHook for AutoPolicy {
    async fn evaluate_settlement(
        &self,
        _draft: SettlementDraft<'_>,
    ) -> Result<PolicyDecision, SpiError> {
        Ok(PolicyDecision::AutoSettle)
    }
}

struct MockLens;

impl DomainLens for MockLens {
    fn describe(&self, task_kind: CognitiveTaskKind) -> LensOutput<'_> {
        let persona = match task_kind {
            CognitiveTaskKind::Absorb => "absorb persona",
            CognitiveTaskKind::Deliberate => "deliberate persona",
            CognitiveTaskKind::Rerank => "rerank persona",
        };
        LensOutput {
            persona_and_stance: persona,
            salience_criteria: "recent and specific",
            evidence_bar: "one named anchor",
            redlines: &["never reveal secrets"],
        }
    }
}

fn reason_request(schema: &Value) -> StructuredReasonRequest<'_> {
    StructuredReasonRequest {
        task_kind: CognitiveTaskKind::Absorb,
        system_prompt: "fixed skeleton",
        user_payload: "{}",
        output_schema: schema,
    }
}

struct MockWorkflows;

#[async_trait]
impl MindWorkflows for MockWorkflows {
    async fn recall_context(
        &self,
        _principal: &Principal,
        request: &MemoryRecallRequest,
        max_tokens: NonZeroUsize,
        caps: &MindCapabilities<'_>,
    ) -> Result<ContextPack, SpiError> {
        let payload =
            serde_json::to_string(request).map_err(|e| SpiError::InvalidInput(e.to_string()))?;
        let mut tokens_used = 0usize;
        if let Some(embedder) = caps.embedder {
            match embedder.embed_batch(&[payload]).await {
                Ok(vectors) => tokens_used = vectors.iter().map(Vec::len).sum(),
                // An unavailable embedder selects lexical recall; other
                // provider errors propagate untouched.
                Err(SpiError::EmbeddingsUnavailable) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(ContextPack {
            records: vec![],
            knowledge: vec![],
            tokens_used: tokens_used.min(max_tokens.get()),
        })
    }

    async fn absorb(
        &self,
        _principal: &Principal,
        request: &MemoryCaptureRequest,
        lens: Option<&dyn DomainLens>,
        caps: &MindCapabilities<'_>,
    ) -> Result<Vec<AbsorbCandidateOutcome>, SpiError> {
        let mut prompt = String::from("absorb skeleton");
        if let Some(lens) = lens {
            prompt.push_str(lens.describe(CognitiveTaskKind::Absorb).persona_and_stance);
        }
        if let Some(reasoner) = caps.reasoner {
            let payload = serde_json::to_string(request)
                .map_err(|e| SpiError::InvalidInput(e.to_string()))?;
            let schema = json!({ "type": "object" });
            let _classified = reasoner
                .reason_structured(StructuredReasonRequest {
                    task_kind: CognitiveTaskKind::Absorb,
                    system_prompt: &prompt,
                    user_payload: &payload,
                    output_schema: &schema,
                })
                .await?;
        }
        let candidate = pending_candidate();
        let proposed = promote_decision();
        let draft = SettlementDraft::CandidateReview {
            candidate: &candidate,
            proposed: &proposed,
        };
        let policy: &dyn PolicyHook = caps.policy_hook.unwrap_or(&DefaultPolicy);
        let candidate_id = "cand_pending".to_string();
        let outcome = match policy.evaluate_settlement(draft).await? {
            PolicyDecision::AutoSettle => AbsorbCandidateOutcome::Settled {
                candidate_id,
                record_ids: vec!["rec_mock".to_string()],
            },
            PolicyDecision::RequireHumanReview { .. } => {
                AbsorbCandidateOutcome::Pending { candidate_id }
            }
            PolicyDecision::Reject { .. } => AbsorbCandidateOutcome::Dropped { candidate_id },
        };
        Ok(vec![outcome])
    }

    async fn deliberate(
        &self,
        _principal: &Principal,
        request: DeliberateRequest<'_>,
        lens: Option<&dyn DomainLens>,
        caps: &MindCapabilities<'_>,
    ) -> Result<DeliberateOutcome, SpiError> {
        let reasoner = caps.reasoner.ok_or(SpiError::ReasonerUnavailable)?;
        let mut prompt = String::from("deliberate skeleton");
        if let Some(lens) = lens {
            prompt.push_str(
                lens.describe(CognitiveTaskKind::Deliberate)
                    .persona_and_stance,
            );
        }
        let payload = json!({ "evidence": request.evidence.len() }).to_string();
        let schema = json!({ "type": "object" });
        let verdict = reasoner
            .reason_structured(StructuredReasonRequest {
                task_kind: CognitiveTaskKind::Deliberate,
                system_prompt: &prompt,
                user_payload: &payload,
                output_schema: &schema,
            })
            .await?;
        let draft_record = belief_record();
        let policy: &dyn PolicyHook = caps.policy_hook.unwrap_or(&DefaultPolicy);
        match policy
            .evaluate_settlement(SettlementDraft::Cognition {
                proposed: &draft_record,
            })
            .await?
        {
            PolicyDecision::AutoSettle => Ok(DeliberateOutcome::Settled {
                verdict,
                record: draft_record,
                mind_state_version: 1,
            }),
            PolicyDecision::RequireHumanReview { .. } => Ok(DeliberateOutcome::NeedsReview {
                verdict,
                draft: draft_record,
            }),
            PolicyDecision::Reject { reason } => Err(SpiError::PolicyRejected(reason)),
        }
    }
}

// ---------- capability assertions ----------

#[test]
fn embedding_identity_compares_all_four_fields() {
    let embedder = MockEmbedder {
        identity: EmbeddingIdentity {
            provider_id: "mock-host".to_string(),
            model_id: "mock-embed".to_string(),
            model_version: "v1".to_string(),
            dim: 3,
        },
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let baseline = embedder.identity().expect("serving identity").clone();
    for variant in [
        EmbeddingIdentity {
            provider_id: "other".to_string(),
            ..baseline.clone()
        },
        EmbeddingIdentity {
            model_id: "other".to_string(),
            ..baseline.clone()
        },
        EmbeddingIdentity {
            model_version: "v2".to_string(),
            ..baseline.clone()
        },
        EmbeddingIdentity {
            dim: 8,
            ..baseline.clone()
        },
    ] {
        assert_ne!(Some(&variant), embedder.identity());
    }
}

#[tokio::test]
async fn no_embeddings_reports_unavailable() {
    let embedder = NoEmbeddings;
    assert!(embedder.identity().is_none());
    assert_eq!(
        embedder.embed_batch(&["text".to_string()]).await,
        Err(SpiError::EmbeddingsUnavailable)
    );
}

// ---------- PromptReasoner over a local text-prompt fixture ----------

#[tokio::test]
async fn prompt_reasoner_wraps_full_text_in_the_wire_envelope() {
    let captured = Arc::new(Mutex::new(Vec::<String>::new()));
    let sink = Arc::clone(&captured);
    // The Nexus PromptExecutor seam: execute(PromptRequest) → full_text.
    let reasoner = PromptReasoner::new(move |prompt: String| {
        let sink = Arc::clone(&sink);
        async move {
            sink.lock().expect("prompt sink").push(prompt);
            Ok(r#"{"verdict": "settle"}"#.to_string())
        }
    });
    let schema = json!({ "type": "object", "required": ["verdict"] });
    let verdict = reasoner
        .reason_structured(reason_request(&schema))
        .await
        .expect("valid JSON parses");
    assert_eq!(verdict, json!({ "verdict": "settle" }));

    let prompts = captured.lock().expect("prompt sink");
    assert_eq!(prompts.len(), 1, "the bridge calls the executor once");
    let body = prompts[0]
        .strip_prefix("fixed skeleton\n\n```json\n")
        .and_then(|rest| rest.strip_suffix("\n```"))
        .expect("prompt is the system prompt plus one fenced JSON block");
    // Decode the outbound envelope: all three fields travel as values, so a
    // dropped output_schema or a reshaped payload fails here.
    let envelope: Value = serde_json::from_str(body).expect("envelope is JSON");
    assert_eq!(envelope["task_kind"], json!("absorb"));
    assert_eq!(envelope["user_payload"], json!("{}"));
    assert_eq!(envelope["output_schema"], schema);
    assert_eq!(envelope.as_object().expect("object").len(), 3);
}

#[tokio::test]
async fn prompt_reasoner_rejects_malformed_full_text() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let reasoner = PromptReasoner::new(move |_prompt: String| {
        let counter = Arc::clone(&counter);
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            // A valid JSON value embedded in prose: a bridge that extracted
            // the JSON would wrongly succeed instead of reporting
            // InvalidReasonerOutput on the complete reply.
            Ok(r#"the verdict is {"verdict": "settle"}, trust me"#.to_string())
        }
    });
    let schema = json!({ "type": "object" });
    match reasoner.reason_structured(reason_request(&schema)).await {
        Err(SpiError::InvalidReasonerOutput(_)) => {}
        other => panic!("expected InvalidReasonerOutput, got {other:?}"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1, "exactly one prompt call");
}

#[tokio::test]
async fn prompt_reasoner_propagates_transport_refusals() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let reasoner = PromptReasoner::new(move |_prompt: String| {
        let counter = Arc::clone(&counter);
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Err(SpiError::ReasonerFailed(
                "prompt executor refused".to_string(),
            ))
        }
    });
    let schema = json!({ "type": "object" });
    match reasoner.reason_structured(reason_request(&schema)).await {
        Err(SpiError::ReasonerFailed(message)) => {
            assert_eq!(message, "prompt executor refused");
        }
        other => panic!("expected ReasonerFailed, got {other:?}"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1, "exactly one prompt call");
}

// ---------- MindWorkflows over the injected capabilities ----------

#[tokio::test]
async fn recall_embeds_when_available_and_degrades_lexically() {
    let workflows = MockWorkflows;
    let principal = principal();
    let request = recall_request();
    let budget = NonZeroUsize::new(64).expect("nonzero budget");

    // The injected embedder is called; a degraded embedder reports
    // EmbeddingsUnavailable and a valid pack still returns.
    let calls = Arc::new(AtomicUsize::new(0));
    let embedder = MockEmbedder {
        identity: EmbeddingIdentity {
            provider_id: "mock-host".to_string(),
            model_id: "mock-embed".to_string(),
            model_version: "v1".to_string(),
            dim: 4,
        },
        calls: Arc::clone(&calls),
    };
    let caps = MindCapabilities {
        embedder: Some(&embedder),
        reasoner: None,
        policy_hook: None,
    };
    let _pack = workflows
        .recall_context(&principal, &request, budget, &caps)
        .await
        .expect("recall");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // The degrade path: NoEmbeddings reports unavailability → lexical recall.
    let empty = NoEmbeddings;
    let caps = MindCapabilities {
        embedder: Some(&empty),
        ..caps
    };
    workflows
        .recall_context(&principal, &request, budget, &caps)
        .await
        .expect("degraded recall");

    // No embedder at all also stays on the lexical path.
    let caps = MindCapabilities {
        embedder: None,
        ..caps
    };
    workflows
        .recall_context(&principal, &request, budget, &caps)
        .await
        .expect("lexical recall without an embedder");
}

#[tokio::test]
async fn absorb_follows_the_injected_policy_hook() {
    let workflows = MockWorkflows;
    let principal = principal();
    let request = capture_request();
    let reasoner = PromptReasoner::new(|_prompt: String| async {
        Ok(r#"{"disposition": "promote"}"#.to_string())
    });
    let lens = MockLens;

    // DefaultPolicy holds every draft: the candidate stays pending.
    let caps = MindCapabilities {
        embedder: None,
        reasoner: Some(&reasoner),
        policy_hook: None,
    };
    let outcomes = workflows
        .absorb(&principal, &request, Some(&lens), &caps)
        .await
        .expect("absorb");
    assert_eq!(
        outcomes,
        vec![AbsorbCandidateOutcome::Pending {
            candidate_id: "cand_pending".to_string()
        }]
    );

    // An injected auto-settle hook settles the same draft.
    let auto = AutoPolicy;
    let caps = MindCapabilities {
        policy_hook: Some(&auto),
        ..caps
    };
    let outcomes = workflows
        .absorb(&principal, &request, None, &caps)
        .await
        .expect("absorb with hook");
    assert_eq!(
        outcomes,
        vec![AbsorbCandidateOutcome::Settled {
            candidate_id: "cand_pending".to_string(),
            record_ids: vec!["rec_mock".to_string()]
        }]
    );
}

#[tokio::test]
async fn deliberate_holds_for_review_under_the_default_policy() {
    let workflows = MockWorkflows;
    let principal = principal();
    let holder = holder();
    let target = target();
    let evidence = [Evidence {
        id: "ev_gate",
        text: "the north gate was closed at dusk",
    }];
    let request = DeliberateRequest {
        space_id: "spc_local",
        subject_id: "subj_mira",
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: Duration::from_secs(5),
    };
    let reasoner = PromptReasoner::new(|_prompt: String| async {
        Ok(r#"{"verdict": "gate was closed"}"#.to_string())
    });
    let caps = MindCapabilities {
        embedder: None,
        reasoner: Some(&reasoner),
        policy_hook: None,
    };
    match workflows
        .deliberate(&principal, request.clone(), Some(&MockLens), &caps)
        .await
        .expect("deliberate")
    {
        DeliberateOutcome::NeedsReview { verdict, .. } => {
            assert_eq!(verdict, json!({ "verdict": "gate was closed" }));
        }
        DeliberateOutcome::Settled { .. } => {
            panic!("DefaultPolicy must hold the verdict for review")
        }
    }

    // Without a reasoner the deliberate contract fails closed.
    let caps = MindCapabilities {
        reasoner: None,
        ..caps
    };
    match workflows.deliberate(&principal, request, None, &caps).await {
        Err(SpiError::ReasonerUnavailable) => {}
        other => panic!("expected ReasonerUnavailable, got {other:?}"),
    }
}
