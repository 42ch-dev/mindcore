//! External adapter proof: a consumer of only the public `mindcore-spi` API
//! implements every capability trait and the `MindWorkflows` surface
//! (compile-time conformance — the bodies are stubs, so no assertion here
//! relies on test-double logic), then drives the shipped `PromptReasoner`,
//! `NoEmbeddings` and `DefaultPolicy` implementations.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};

use mindcore_spi::{
    adopt_knowledge_entry, AbsorbCandidateOutcome, AdoptionOutcome, CognitiveTaskKind, ContextPack,
    DefaultPolicy, DeliberateOutcome, DeliberateRequest, DomainLens, Embedder, EmbeddingIdentity,
    LensOutput, MindCapabilities, MindWorkflows, NoEmbeddings, PolicyDecision, PolicyHook,
    PromptReasoner, ProposalDraft, Reasoner, SettlementDraft, SpiError, StructuredReasonRequest,
};
use spoke_operations::{spoke_reject, KnowledgeEntryPort, SpokeRejectCode, SpokeResult};
use spoke_schemas::ops::memory_candidates_review_request::Decision;
use spoke_schemas::{
    CognitionRecord, KnowledgeEntry, MemoryCandidate, MemoryCaptureRequest, MemoryRecallRequest,
    Principal,
};

// ---------- wire fixtures (same shapes the SPOKE schema tests pin) ----------

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

fn decision(wire: Value) -> Decision {
    serde_json::from_value(wire).expect("decision parses")
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

// ---------- the external test double (conformance proof, not executed) ----------

struct MockEmbedder {
    identity: EmbeddingIdentity,
}

#[async_trait]
impl Embedder for MockEmbedder {
    fn identity(&self) -> Option<&EmbeddingIdentity> {
        Some(&self.identity)
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, SpiError> {
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

    fn validate_proposal(
        &self,
        _draft: &ProposalDraft<'_>,
        _admitted: &[&str],
    ) -> Result<(), String> {
        Ok(())
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

/// External `MindWorkflows` implementation. The method bodies are stubs:
/// this impl only proves the trait signatures are implementable by a
/// consumer of the public API, so no test asserts on its behavior.
struct MockWorkflows;

#[async_trait]
impl MindWorkflows for MockWorkflows {
    async fn recall_context(
        &self,
        _principal: &Principal,
        _request: &MemoryRecallRequest,
        _max_tokens: NonZeroUsize,
        _caps: &MindCapabilities<'_>,
    ) -> Result<ContextPack, SpiError> {
        unimplemented!("conformance stub")
    }

    async fn absorb(
        &self,
        _principal: &Principal,
        _request: &MemoryCaptureRequest,
        _lens: Option<&dyn DomainLens>,
        _caps: &MindCapabilities<'_>,
    ) -> Result<Vec<AbsorbCandidateOutcome>, SpiError> {
        unimplemented!("conformance stub")
    }

    async fn deliberate(
        &self,
        _principal: &Principal,
        _request: DeliberateRequest<'_>,
        _lens: Option<&dyn DomainLens>,
        _caps: &MindCapabilities<'_>,
    ) -> Result<DeliberateOutcome, SpiError> {
        unimplemented!("conformance stub")
    }
}

// ---------- conformance: the public trait surface binds externally ----------

#[test]
fn external_impls_bind_to_every_capability_trait() {
    let embedder = MockEmbedder {
        identity: EmbeddingIdentity {
            provider_id: "mock-host".to_string(),
            model_id: "mock-embed".to_string(),
            model_version: "v1".to_string(),
            dim: 4,
        },
    };
    let reasoner =
        PromptReasoner::new(|_prompt: String| async { Ok(r#"{"ok": true}"#.to_string()) });
    let policy = AutoPolicy;
    let lens = MockLens;
    let workflows = MockWorkflows;

    // Object-safety: every capability trait takes `&dyn`, and the workflows
    // surface itself is object-safe.
    let embedder_dyn: &dyn Embedder = &embedder;
    let reasoner_dyn: &dyn Reasoner = &reasoner;
    let policy_dyn: &dyn PolicyHook = &policy;
    let lens_dyn: &dyn DomainLens = &lens;
    let workflows_dyn: &dyn MindWorkflows = &workflows;
    let _ = (
        embedder_dyn,
        reasoner_dyn,
        policy_dyn,
        lens_dyn,
        workflows_dyn,
    );

    // The bundle accepting the injected implementors is the compile-time
    // proof; asserting on `Some(x).is_some()` would pin the literal, not the
    // contract.
    let _caps = MindCapabilities {
        embedder: Some(embedder_dyn),
        reasoner: Some(reasoner_dyn),
        policy_hook: Some(policy_dyn),
    };
}

// ---------- shipped code assertions ----------

#[test]
fn embedding_identity_compares_all_four_fields() {
    let embedder = MockEmbedder {
        identity: EmbeddingIdentity {
            provider_id: "mock-host".to_string(),
            model_id: "mock-embed".to_string(),
            model_version: "v1".to_string(),
            dim: 3,
        },
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

#[test]
fn mind_capabilities_default_to_empty() {
    let caps = MindCapabilities::default();
    assert!(caps.embedder.is_none());
    assert!(caps.reasoner.is_none());
    assert!(caps.policy_hook.is_none());
}

#[tokio::test]
async fn default_policy_holds_every_draft_for_human_review() {
    let policy = DefaultPolicy;
    let candidate = pending_candidate();
    let record = belief_record();

    // Each canonical review action leaves the candidate pending.
    for proposed in [
        decision(json!({ "action": "promote" })),
        decision(json!({
            "action": "fragment",
            "fragments": [
                { "kind": "note", "content": { "text": "first fragment" } },
                { "kind": "note", "content": { "text": "second fragment" } }
            ]
        })),
        decision(json!({ "action": "drop", "rationale": "not worth keeping" })),
    ] {
        let verdict = policy
            .evaluate_settlement(SettlementDraft::CandidateReview {
                candidate: &candidate,
                proposed: &proposed,
            })
            .await
            .expect("candidate review evaluates");
        match verdict {
            PolicyDecision::RequireHumanReview { .. } => {}
            other => panic!("DefaultPolicy must hold the draft, got {other:?}"),
        }
    }

    // A cognition draft is held the same way.
    let verdict = policy
        .evaluate_settlement(SettlementDraft::Cognition { proposed: &record })
        .await
        .expect("cognition review evaluates");
    match verdict {
        PolicyDecision::RequireHumanReview { .. } => {}
        other => panic!("DefaultPolicy must hold the draft, got {other:?}"),
    }
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

// ---------- adoption helper over an in-memory port double ----------

#[derive(Default)]
struct MemoryKnowledgeEntries {
    entries: Mutex<HashMap<String, KnowledgeEntry>>,
}

#[async_trait]
impl KnowledgeEntryPort for MemoryKnowledgeEntries {
    async fn get_knowledge_entry(&self, entry_id: &str) -> SpokeResult<KnowledgeEntry> {
        match self.entries.lock().expect("entries lock").get(entry_id) {
            Some(entry) => SpokeResult::Ok(entry.clone()),
            None => spoke_reject(
                SpokeRejectCode::KnowledgeEntryNotFound,
                "knowledge entry not found",
                None,
            ),
        }
    }

    async fn put_knowledge_entry(
        &self,
        mut entry: KnowledgeEntry,
        expected_base_revision: Option<u64>,
    ) -> SpokeResult<KnowledgeEntry> {
        let mut entries = self.entries.lock().expect("entries lock");
        let current = entries
            .get(&entry.entry_id)
            .and_then(|stored| stored.revision);
        let next = match expected_base_revision {
            None if current.is_none() => 1,
            None => {
                return spoke_reject(
                    SpokeRejectCode::KnowledgeEntryAlreadyExists,
                    format!("knowledge entry {} already exists", entry.entry_id),
                    None,
                )
            }
            Some(expected) if current == Some(expected) => expected + 1,
            Some(_) => {
                return spoke_reject(
                    SpokeRejectCode::StoredRevisionStale,
                    "stored revision is stale",
                    None,
                )
            }
        };
        entry.revision = Some(next);
        entries.insert(entry.entry_id.clone(), entry.clone());
        SpokeResult::Ok(entry)
    }
}

fn shared_entry(entry_id: &str) -> KnowledgeEntry {
    serde_json::from_value(json!({
        "schema_version": 1,
        "entry_id": entry_id,
        "entry_type": "location",
        "canonical_name": format!("{entry_id} name"),
        "status": "confirmed",
        "body": { "summary": "the north gate is shut after dusk" },
        "extensions": {},
        "modules": { "cognition": { "records": [] } }
    }))
    .expect("knowledge entry fixture parses")
}

#[tokio::test]
async fn adoption_over_a_port_double_creates_once_and_refuses_an_absent_source() {
    let src = MemoryKnowledgeEntries::default();
    let dst = MemoryKnowledgeEntries::default();
    let entry_id = "ke_shared_gate";
    let source = match src.put_knowledge_entry(shared_entry(entry_id), None).await {
        SpokeResult::Ok(entry) => entry,
        SpokeResult::Reject(reject) => panic!("seed refused: {reject:?}"),
    };

    let created = match adopt_knowledge_entry(&src, &dst, entry_id, 1).await {
        SpokeResult::Ok(AdoptionOutcome::Created(created)) => created,
        other => panic!("the first adoption must create, got {other:?}"),
    };
    assert_eq!(created.revision, Some(1));
    let anchor = serde_json::to_value(created.source_anchor.as_ref().expect("anchor"))
        .expect("anchor serializes");
    assert_eq!(anchor["source_id"], json!("shared:ke_shared_gate"));
    assert_eq!(
        anchor["extensions"]["mindcore_adoption"]["source_revision"],
        json!(1)
    );

    let repeat = match adopt_knowledge_entry(&src, &dst, entry_id, 1).await {
        SpokeResult::Ok(AdoptionOutcome::AlreadyPresent(existing)) => existing,
        other => panic!("a repeat must return the existing copy, got {other:?}"),
    };
    assert_eq!(
        serde_json::to_value(&repeat).expect("copy serializes"),
        serde_json::to_value(&created).expect("copy serializes")
    );

    // The source is untouched: same revision, still no anchor.
    let source_after = match src.get_knowledge_entry(entry_id).await {
        SpokeResult::Ok(entry) => entry,
        SpokeResult::Reject(reject) => panic!("source read refused: {reject:?}"),
    };
    assert_eq!(source_after.revision, source.revision);
    assert!(source_after.source_anchor.is_none());

    match adopt_knowledge_entry(&src, &dst, "ke_absent", 1).await {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::KnowledgeEntryNotFound);
            assert_eq!(reject.message, "source knowledge entry not found");
            assert!(reject.details.is_none());
        }
        other => panic!("an absent source must be refused, got {other:?}"),
    }
}
