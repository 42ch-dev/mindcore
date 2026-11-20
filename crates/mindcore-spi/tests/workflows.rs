//! Workflow behaviour over a pinned binding double: the recall page order,
//! hybrid rerank, pack budget and snapshot revalidation, with no storage.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};

use mindcore_spi::{
    AbsorbCandidateOutcome, AbsorbSnapshot, CaptureOutcome, CitedRecord, CognitiveTaskKind,
    DomainLens, Embedder, EmbeddingIdentity, LensOutput, MindCapabilities, NoEmbeddings,
    PolicyDecision, PolicyHook, Reasoner, RecallSnapshot, SettlementDraft, SpiError,
    SpiMindWorkflows, StructuredReasonRequest, WorkflowBinding,
};
use spoke_operations::SpokeRejectCode;
use spoke_schemas::{
    KnowledgeEntry, MemoryCandidate, MemoryCandidatesDismissRequest, MemoryCandidatesReviewRequest,
    MemoryCaptureRequest, MemoryRecallRequest, MemoryRecordsUpdateRequest, Principal,
    RetainedRecord,
};

fn principal() -> Principal {
    serde_json::from_value(json!({ "principal_id": "prn_1", "extensions": {} })).expect("principal")
}

fn request(query: Option<&str>) -> MemoryRecallRequest {
    let mut value = json!({
        "space_id": "spc_1",
        "subject_id": "subj_1",
        "selection": { "mode": "shared_only" },
        "budget": { "max_items": 10 },
    });
    if let Some(query) = query {
        value["query"] = json!(query);
    }
    serde_json::from_value(value).expect("recall request")
}

fn record(record_id: &str, text: &str, revision: u64) -> RetainedRecord {
    serde_json::from_value(json!({
        "schema_version": 1,
        "record_id": record_id,
        "space_id": "spc_1",
        "subject_id": "subj_1",
        "kind": "fact",
        "content": { "text": text },
        "scope": { "visibility": "shared" },
        "provenance": {
            "source_context_ids": ["ctx_a"],
            "anchors": [],
            "captured_by": "prn_1",
            "captured_at": "2026-11-20T09:00:00Z"
        },
        "created_at": "2026-11-20T09:00:00Z",
        "updated_at": "2026-11-20T09:00:00Z",
        "revision": revision,
        "extensions": {}
    }))
    .expect("record")
}

fn entry(entry_id: &str, summary: &str) -> KnowledgeEntry {
    serde_json::from_value(json!({
        "schema_version": 1,
        "entry_id": entry_id,
        "entry_type": "location",
        "canonical_name": entry_id,
        "status": "confirmed",
        "body": { "summary": summary },
        "revision": 1,
        "extensions": {}
    }))
    .expect("entry")
}

fn snapshot(records: Vec<RetainedRecord>, knowledge: Vec<KnowledgeEntry>) -> RecallSnapshot {
    RecallSnapshot { records, knowledge }
}

/// Repeats its last snapshot once the list runs out, so a stable page reads
/// back identically.
struct StubBinding {
    snapshots: Mutex<Vec<RecallSnapshot>>,
    calls: Arc<AtomicUsize>,
}

impl StubBinding {
    fn new(snapshots: Vec<RecallSnapshot>) -> Self {
        Self {
            snapshots: Mutex::new(snapshots),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// A counter the test keeps after the engine takes ownership.
    fn calls(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.calls)
    }
}

#[async_trait]
impl WorkflowBinding for StubBinding {
    async fn recall_snapshot(
        &self,
        _principal: &Principal,
        _request: &MemoryRecallRequest,
    ) -> Result<RecallSnapshot, SpiError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let snapshots = self.snapshots.lock().expect("snapshots");
        Ok(snapshots[call.min(snapshots.len() - 1)].clone())
    }

    async fn capture(
        &self,
        _principal: &Principal,
        _request: &MemoryCaptureRequest,
    ) -> Result<CaptureOutcome, SpiError> {
        unserved()
    }

    async fn absorb_snapshot(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _candidate_id: &str,
        _max_items: u64,
    ) -> Result<AbsorbSnapshot, SpiError> {
        unserved()
    }

    async fn review_candidate(
        &self,
        _principal: &Principal,
        _request: &MemoryCandidatesReviewRequest,
        _cited: &[CitedRecord],
    ) -> Result<Vec<String>, SpiError> {
        unserved()
    }

    async fn update_candidate(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _update: &MemoryRecordsUpdateRequest,
        _dismiss: &MemoryCandidatesDismissRequest,
        _cited: &[CitedRecord],
    ) -> Result<(), SpiError> {
        unserved()
    }

    async fn recorded_update(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _idempotency_key: &str,
    ) -> Result<Option<String>, SpiError> {
        unserved()
    }
}

/// A read-only double: it serves the recall page and refuses the settlement
/// legs it does not model.
fn unserved<T>() -> Result<T, SpiError> {
    Err(SpiError::InvalidInput(
        "this double serves no settlement leg".to_string(),
    ))
}

struct RefusingBinding(SpiError);

#[async_trait]
impl WorkflowBinding for RefusingBinding {
    async fn recall_snapshot(
        &self,
        _principal: &Principal,
        _request: &MemoryRecallRequest,
    ) -> Result<RecallSnapshot, SpiError> {
        Err(self.0.clone())
    }

    async fn capture(
        &self,
        _principal: &Principal,
        _request: &MemoryCaptureRequest,
    ) -> Result<CaptureOutcome, SpiError> {
        unserved()
    }

    async fn absorb_snapshot(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _candidate_id: &str,
        _max_items: u64,
    ) -> Result<AbsorbSnapshot, SpiError> {
        unserved()
    }

    async fn review_candidate(
        &self,
        _principal: &Principal,
        _request: &MemoryCandidatesReviewRequest,
        _cited: &[CitedRecord],
    ) -> Result<Vec<String>, SpiError> {
        unserved()
    }

    async fn update_candidate(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _update: &MemoryRecordsUpdateRequest,
        _dismiss: &MemoryCandidatesDismissRequest,
        _cited: &[CitedRecord],
    ) -> Result<(), SpiError> {
        unserved()
    }

    async fn recorded_update(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _idempotency_key: &str,
    ) -> Result<Option<String>, SpiError> {
        unserved()
    }
}

/// Two planted axes: the query and the "north gate" records share the first,
/// the market record the second.
struct PlantedEmbedder {
    identity: EmbeddingIdentity,
}

#[async_trait]
impl Embedder for PlantedEmbedder {
    fn identity(&self) -> Option<&EmbeddingIdentity> {
        Some(&self.identity)
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, SpiError> {
        Ok(texts
            .iter()
            .map(|text| {
                if text.contains("market") {
                    vec![0.0, 1.0]
                } else if text.contains("guards") {
                    vec![0.5, 0.5]
                } else {
                    vec![1.0, 0.0]
                }
            })
            .collect())
    }
}

fn planted_embedder() -> PlantedEmbedder {
    PlantedEmbedder {
        identity: EmbeddingIdentity {
            provider_id: "planted".to_string(),
            model_id: "axes".to_string(),
            model_version: "1".to_string(),
            dim: 2,
        },
    }
}

struct FixedReasoner {
    record_ids: Vec<&'static str>,
}

#[async_trait]
impl Reasoner for FixedReasoner {
    async fn reason_structured(
        &self,
        request: StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        assert_eq!(request.task_kind, CognitiveTaskKind::Rerank);
        Ok(json!({ "record_ids": self.record_ids }))
    }
}

fn ids(pack: &mindcore_spi::ContextPack) -> Vec<&str> {
    pack.records
        .iter()
        .map(|record| match record {
            RetainedRecord::LiveRecord(record) => record.record_id.as_str(),
            RetainedRecord::TombstonedRecord(record) => record.record_id.as_str(),
        })
        .collect()
}

fn budget(tokens: usize) -> NonZeroUsize {
    NonZeroUsize::new(tokens).expect("non-zero budget")
}

#[tokio::test]
async fn no_embeddings_preserves_lexical_order_and_provenance() {
    let page = snapshot(
        vec![
            record("rec_b", "the market opens at dawn", 3),
            record("rec_a", "the north gate is closed", 1),
        ],
        Vec::new(),
    );
    let workflows = SpiMindWorkflows::new(StubBinding::new(vec![page]));
    let caps = MindCapabilities {
        embedder: Some(&NoEmbeddings),
        ..MindCapabilities::default()
    };

    let pack = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &caps,
        )
        .await
        .expect("pack");

    assert_eq!(ids(&pack), ["rec_b", "rec_a"]);
    let RetainedRecord::LiveRecord(first) = &pack.records[1] else {
        panic!("expected a live record");
    };
    assert_eq!(first.space_id.as_str(), "spc_1");
    assert_eq!(first.subject_id.as_str(), "subj_1");
    assert_eq!(first.record_id.as_str(), "rec_a");
    assert_eq!(first.revision.get(), 1);
    assert_eq!(first.provenance.source_context_ids, ["ctx_a"]);
    assert_eq!(first.provenance.captured_by.as_str(), "prn_1");
}

#[tokio::test]
async fn planted_vectors_rerank_only_the_page() {
    let page = snapshot(
        vec![
            record("rec_b", "the market opens at dawn", 1),
            record("rec_a", "the north gate is closed", 1),
            record("rec_c", "north gate guards sleep", 1),
        ],
        Vec::new(),
    );
    let workflows = SpiMindWorkflows::new(StubBinding::new(vec![page]));
    let embedder = planted_embedder();
    let caps = MindCapabilities {
        embedder: Some(&embedder),
        ..MindCapabilities::default()
    };

    let pack = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &caps,
        )
        .await
        .expect("pack");

    assert_eq!(ids(&pack), ["rec_a", "rec_c", "rec_b"]);
    assert_eq!(pack.records.len(), 3);
}

#[tokio::test]
async fn budget_counts_both_slots_and_emits_whole_items() {
    let record_a = record("rec_a", "the north gate is closed", 1);
    let record_b = record("rec_b", "the market opens at dawn", 1);
    let entry_a = entry("ke_gate", "closed after dusk");
    let entry_b = entry("ke_market", "opens at dawn");
    let sizes =
        [&record_a, &record_b].map(|record| serde_json::to_string(record).expect("size").len());
    let entry_sizes =
        [&entry_a, &entry_b].map(|entry| serde_json::to_string(entry).expect("size").len());
    let workflows = SpiMindWorkflows::new(StubBinding::new(vec![snapshot(
        vec![record_a, record_b],
        vec![entry_a, entry_b],
    )]));
    let caps = MindCapabilities::default();

    let exact = workflows
        .recall_context(
            &principal(),
            &request(None),
            budget(sizes[0] + entry_sizes[0]),
            &caps,
        )
        .await
        .expect("pack");
    assert_eq!(exact.tokens_used, sizes[0] + entry_sizes[0]);
    assert_eq!(ids(&exact), ["rec_a"]);
    assert_eq!(exact.knowledge.len(), 1);
    assert_eq!(exact.knowledge[0].entry_id, "ke_gate");

    let partial = workflows
        .recall_context(
            &principal(),
            &request(None),
            budget(sizes[0] + entry_sizes[0] - 1),
            &caps,
        )
        .await
        .expect("pack");
    assert_eq!(partial.tokens_used, sizes[0]);
    assert_eq!(ids(&partial), ["rec_a"]);
    assert!(partial.knowledge.is_empty());

    let empty = workflows
        .recall_context(&principal(), &request(None), budget(1), &caps)
        .await
        .expect("pack");
    assert_eq!(empty.tokens_used, 0);
    assert!(empty.records.is_empty() && empty.knowledge.is_empty());
}

#[tokio::test]
async fn changed_revision_returns_revision_conflict() {
    let binding = StubBinding::new(vec![
        snapshot(
            vec![record("rec_a", "the north gate is closed", 1)],
            Vec::new(),
        ),
        snapshot(
            vec![record("rec_a", "the north gate is closed", 2)],
            Vec::new(),
        ),
    ]);
    let workflows = SpiMindWorkflows::new(binding);

    let error = workflows
        .recall_context(
            &principal(),
            &request(None),
            budget(4096),
            &MindCapabilities::default(),
        )
        .await
        .expect_err("a changed snapshot must not release a pack");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::RevisionConflict);
}

#[tokio::test]
async fn current_read_refusal_propagates() {
    let workflows = SpiMindWorkflows::new(RefusingBinding(SpiError::Settlement(
        spoke_operations::SpokeReject {
            code: SpokeRejectCode::NotAuthorized,
            message: "principal holds no recall grant".to_string(),
            details: None,
        },
    )));

    let error = workflows
        .recall_context(
            &principal(),
            &request(None),
            budget(4096),
            &MindCapabilities::default(),
        )
        .await
        .expect_err("a refused read must not release a pack");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
}

#[tokio::test]
async fn reasoner_permutation_reranks_the_hybrid_page() {
    let page = snapshot(
        vec![
            record("rec_b", "the market opens at dawn", 1),
            record("rec_a", "the north gate is closed", 1),
            record("rec_c", "north gate guards sleep", 1),
        ],
        Vec::new(),
    );
    let embedder = planted_embedder();
    let hybrid = SpiMindWorkflows::new(StubBinding::new(vec![page.clone()]));
    let without_reasoner = hybrid
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &MindCapabilities {
                embedder: Some(&embedder),
                ..MindCapabilities::default()
            },
        )
        .await
        .expect("hybrid pack");
    assert_eq!(ids(&without_reasoner), ["rec_a", "rec_c", "rec_b"]);

    let reranked = SpiMindWorkflows::new(StubBinding::new(vec![page]));
    let reasoner = FixedReasoner {
        record_ids: vec!["rec_b", "rec_c", "rec_a"],
    };
    let pack = reranked
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &MindCapabilities {
                embedder: Some(&embedder),
                reasoner: Some(&reasoner),
                ..MindCapabilities::default()
            },
        )
        .await
        .expect("reranked pack");
    assert_eq!(ids(&pack), ["rec_b", "rec_c", "rec_a"]);
}

#[tokio::test]
async fn reasoner_may_not_widen_or_repeat_the_page() {
    let page = snapshot(
        vec![
            record("rec_a", "the north gate is closed", 1),
            record("rec_b", "the market opens at dawn", 1),
        ],
        Vec::new(),
    );
    let embedder = planted_embedder();
    let reasoner = FixedReasoner {
        record_ids: vec!["rec_a", "rec_ghost"],
    };
    let workflows = SpiMindWorkflows::new(StubBinding::new(vec![page]));

    let error = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &MindCapabilities {
                embedder: Some(&embedder),
                reasoner: Some(&reasoner),
                ..MindCapabilities::default()
            },
        )
        .await
        .expect_err("a widened permutation must be refused");

    assert!(matches!(error, SpiError::InvalidReasonerOutput(_)));
}

/// Two reads per released pack: the snapshot and its revalidation.
#[tokio::test]
async fn a_released_pack_reads_the_snapshot_twice() {
    let binding = StubBinding::new(vec![snapshot(
        vec![record("rec_a", "the north gate is closed", 1)],
        Vec::new(),
    )]);
    let calls = binding.calls();
    let workflows = SpiMindWorkflows::new(binding);
    let pack = workflows
        .recall_context(
            &principal(),
            &request(None),
            budget(4096),
            &MindCapabilities::default(),
        )
        .await
        .expect("pack");
    assert_eq!(pack.records.len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

// ---------- absorb: decisions over an absorbed candidate double ----------

fn pending_candidate() -> MemoryCandidate {
    serde_json::from_value(json!({
        "schema_version": 1,
        "candidate_id": "cnd_1",
        "space_id": "spc_1",
        "subject_id": "subj_1",
        "capture_scope": { "visibility": "shared" },
        "proposed": {
            "kind": "fact",
            "content": { "text": "the north gate is closed at dusk" }
        },
        "classification": { "disposition": "promote", "rationale_codes": [] },
        "status": "pending",
        "provenance": {
            "source_context_ids": [],
            "anchors": [],
            "captured_by": "prn_1",
            "captured_at": "2026-11-20T09:00:00Z"
        },
        "created_at": "2026-11-20T09:00:00Z",
        "revision": 1,
        "extensions": {}
    }))
    .expect("pending candidate")
}

fn consumed_candidate() -> MemoryCandidate {
    serde_json::from_value(json!({
        "schema_version": 1,
        "candidate_id": "cnd_1",
        "space_id": "spc_1",
        "subject_id": "subj_1",
        "capture_scope": { "visibility": "shared" },
        "classification": { "disposition": "promote", "rationale_codes": [] },
        "status": "consumed",
        "consumed_by": {
            "op": "memory.candidates.review",
            "outcome": "promoted",
            "receipt_id": "rct_1",
            "record_ids": ["rec_new"]
        },
        "content_digest": "0000000000000000000000000000000000000000000000000000000000000000",
        "provenance": {
            "source_context_ids": [],
            "anchors": [],
            "captured_by": "prn_1",
            "captured_at": "2026-11-20T09:00:00Z"
        },
        "created_at": "2026-11-20T09:00:00Z",
        "revision": 2,
        "extensions": {}
    }))
    .expect("consumed candidate")
}

fn dismissed_candidate() -> MemoryCandidate {
    serde_json::from_value(json!({
        "schema_version": 1,
        "candidate_id": "cnd_1",
        "space_id": "spc_1",
        "subject_id": "subj_1",
        "capture_scope": { "visibility": "shared" },
        "classification": { "disposition": "promote", "rationale_codes": [] },
        "status": "consumed",
        "consumed_by": {
            "op": "memory.candidates.dismiss",
            "outcome": "dismissed",
            "receipt_id": "rct_2",
            "record_ids": []
        },
        "content_digest": "0000000000000000000000000000000000000000000000000000000000000000",
        "provenance": {
            "source_context_ids": [],
            "anchors": [],
            "captured_by": "prn_1",
            "captured_at": "2026-11-20T09:00:00Z"
        },
        "created_at": "2026-11-20T09:00:00Z",
        "revision": 2,
        "extensions": {}
    }))
    .expect("dismissed candidate")
}

fn capture_request() -> MemoryCaptureRequest {
    serde_json::from_value(json!({
        "space_id": "spc_1",
        "subject_id": "subj_1",
        "capture_scope": { "visibility": "shared" },
        "content": { "text": "the north gate is closed at dusk" },
        "idempotency_key": "cap_1"
    }))
    .expect("capture request")
}

/// The settlement legs one absorb run asked a binding to submit.
#[derive(Clone, Default)]
struct Recorded {
    reviews: Arc<Mutex<Vec<Value>>>,
    updates: Arc<Mutex<Vec<Value>>>,
    update_keys: Arc<Mutex<Vec<String>>>,
}

/// One captured candidate over a fixed page, with every submitted settlement
/// recorded for the assertion.
struct AbsorbDouble {
    candidate: MemoryCandidate,
    page: Vec<RetainedRecord>,
    recorded: Recorded,
    /// The record id this binding's update leg already settled, if any.
    settled_update: Option<String>,
    /// Captures served so far: the first commits the receipt, later ones replay it.
    captures: AtomicUsize,
}

#[async_trait]
impl WorkflowBinding for AbsorbDouble {
    async fn recall_snapshot(
        &self,
        _principal: &Principal,
        _request: &MemoryRecallRequest,
    ) -> Result<RecallSnapshot, SpiError> {
        unserved()
    }

    async fn capture(
        &self,
        _principal: &Principal,
        _request: &MemoryCaptureRequest,
    ) -> Result<CaptureOutcome, SpiError> {
        // The first capture commits the receipt; every later one replays it.
        let call = self.captures.fetch_add(1, Ordering::SeqCst);
        Ok(CaptureOutcome {
            candidate_ids: vec!["cnd_1".to_string()],
            replayed: call > 0,
        })
    }

    async fn absorb_snapshot(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _candidate_id: &str,
        _max_items: u64,
    ) -> Result<AbsorbSnapshot, SpiError> {
        Ok(AbsorbSnapshot {
            candidate: self.candidate.clone(),
            records: self.page.clone(),
        })
    }

    async fn review_candidate(
        &self,
        _principal: &Principal,
        request: &MemoryCandidatesReviewRequest,
        cited: &[CitedRecord],
    ) -> Result<Vec<String>, SpiError> {
        let mut object = serde_json::to_value(request).expect("review request");
        object["cited"] = cited_json(cited);
        self.recorded.reviews.lock().expect("reviews").push(object);
        Ok(vec!["rec_new".to_string()])
    }

    async fn update_candidate(
        &self,
        _principal: &Principal,
        _space_id: &str,
        update: &MemoryRecordsUpdateRequest,
        dismiss: &MemoryCandidatesDismissRequest,
        cited: &[CitedRecord],
    ) -> Result<(), SpiError> {
        self.recorded.updates.lock().expect("updates").push(json!({
            "update": serde_json::to_value(update).expect("update request"),
            "dismiss": serde_json::to_value(dismiss).expect("dismiss request"),
            "cited": cited_json(cited),
        }));
        Ok(())
    }

    async fn recorded_update(
        &self,
        _principal: &Principal,
        _space_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<String>, SpiError> {
        self.recorded
            .update_keys
            .lock()
            .expect("update keys")
            .push(idempotency_key.to_string());
        Ok(self.settled_update.clone())
    }
}

fn cited_json(cited: &[CitedRecord]) -> Value {
    Value::Array(
        cited
            .iter()
            .map(|record| json!({ "record_id": record.record_id, "revision": record.revision }))
            .collect(),
    )
}

struct FixedVerdict(Value);

#[async_trait]
impl Reasoner for FixedVerdict {
    async fn reason_structured(
        &self,
        request: StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        assert_eq!(request.task_kind, CognitiveTaskKind::Absorb);
        Ok(self.0.clone())
    }
}

struct UnreachableReasoner;

#[async_trait]
impl Reasoner for UnreachableReasoner {
    async fn reason_structured(
        &self,
        _request: StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        panic!("a consumed candidate needs no decision")
    }
}

/// Counts the decisions it is asked for and promotes the candidate.
struct CountingReasoner {
    asked: Arc<AtomicUsize>,
}

#[async_trait]
impl Reasoner for CountingReasoner {
    async fn reason_structured(
        &self,
        _request: StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        Ok(json!({ "action": "promote", "evidence_ids": ["rec_gate"] }))
    }
}

/// Records the draft each verdict was made over, then returns `decision`.
struct RecordingPolicy {
    decision: PolicyDecision,
    seen: Arc<Mutex<Vec<Value>>>,
}

#[async_trait]
impl PolicyHook for RecordingPolicy {
    async fn evaluate_settlement(
        &self,
        draft: SettlementDraft<'_>,
    ) -> Result<PolicyDecision, SpiError> {
        let seen = match draft {
            SettlementDraft::CandidateReview { proposed, .. } => {
                json!({ "kind": "review", "proposed": proposed })
            }
            SettlementDraft::CandidateUpdate { proposed, .. } => {
                json!({ "kind": "update", "proposed": proposed })
            }
            SettlementDraft::Cognition { .. } => json!({ "kind": "cognition" }),
        };
        self.seen.lock().expect("seen").push(seen);
        Ok(self.decision.clone())
    }
}

fn page() -> Vec<RetainedRecord> {
    vec![record("rec_gate", "the north gate is closed", 3)]
}

fn absorb(candidate: MemoryCandidate) -> (SpiMindWorkflows, Recorded, Arc<Mutex<Vec<Value>>>) {
    absorb_after_update(candidate, None)
}

/// An absorb double whose update leg already settled `settled_update`.
fn absorb_after_update(
    candidate: MemoryCandidate,
    settled_update: Option<&str>,
) -> (SpiMindWorkflows, Recorded, Arc<Mutex<Vec<Value>>>) {
    let recorded = Recorded::default();
    let double = AbsorbDouble {
        candidate,
        page: page(),
        recorded: recorded.clone(),
        settled_update: settled_update.map(str::to_string),
        captures: AtomicUsize::new(0),
    };
    let seen = Arc::new(Mutex::new(Vec::new()));
    (SpiMindWorkflows::new(double), recorded, seen)
}

#[tokio::test]
async fn promote_settles_through_one_review() {
    let (workflows, recorded, seen) = absorb(pending_candidate());
    let policy = RecordingPolicy {
        decision: PolicyDecision::AutoSettle,
        seen: Arc::clone(&seen),
    };
    let verdict = json!({ "action": "promote", "evidence_ids": ["rec_gate"] });
    let caps = MindCapabilities {
        reasoner: Some(&FixedVerdict(verdict)),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let outcomes = workflows
        .absorb(&principal(), &capture_request(), None, &caps)
        .await
        .expect("absorb");

    assert_eq!(
        outcomes,
        vec![AbsorbCandidateOutcome::Settled {
            candidate_id: "cnd_1".to_string(),
            record_ids: vec!["rec_new".to_string()],
        }]
    );
    assert_eq!(
        *seen.lock().expect("seen"),
        vec![json!({ "kind": "review", "proposed": { "action": "promote" } })]
    );
    let reviews = recorded.reviews.lock().expect("reviews");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0]["candidate_id"], "cnd_1");
    assert_eq!(reviews[0]["expected_revision"], 1);
    assert_eq!(
        reviews[0]["cited"],
        json!([{ "record_id": "rec_gate", "revision": 3 }])
    );
    assert!(recorded.updates.lock().expect("updates").is_empty());
}

#[tokio::test]
async fn update_hands_policy_the_typed_target_and_settles_it() {
    let (workflows, recorded, seen) = absorb(pending_candidate());
    let policy = RecordingPolicy {
        decision: PolicyDecision::AutoSettle,
        seen: Arc::clone(&seen),
    };
    let verdict = json!({
        "action": "update",
        "record_id": "rec_gate",
        "evidence_ids": ["rec_gate"]
    });
    let caps = MindCapabilities {
        reasoner: Some(&FixedVerdict(verdict)),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let outcomes = workflows
        .absorb(&principal(), &capture_request(), None, &caps)
        .await
        .expect("absorb");

    assert_eq!(
        outcomes,
        vec![AbsorbCandidateOutcome::Settled {
            candidate_id: "cnd_1".to_string(),
            record_ids: vec!["rec_gate".to_string()],
        }]
    );
    let seen = seen.lock().expect("seen");
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0]["kind"], "update");
    // The policy sees the record and the revision the snapshot read, and the
    // candidate's own content — not anything the model supplied.
    assert_eq!(seen[0]["proposed"]["record_id"], "rec_gate");
    assert_eq!(seen[0]["proposed"]["expected_revision"], 3);
    assert_eq!(
        seen[0]["proposed"]["content"]["text"],
        "the north gate is closed at dusk"
    );
    let updates = recorded.updates.lock().expect("updates");
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0]["update"], seen[0]["proposed"]);
    assert_eq!(updates[0]["dismiss"]["candidate_id"], "cnd_1");
    assert_eq!(updates[0]["dismiss"]["expected_revision"], 1);
    assert_eq!(
        updates[0]["cited"],
        json!([{ "record_id": "rec_gate", "revision": 3 }])
    );
    assert!(recorded.reviews.lock().expect("reviews").is_empty());
}

#[tokio::test]
async fn dismiss_submits_a_drop_review() {
    let (workflows, recorded, seen) = absorb(pending_candidate());
    let policy = RecordingPolicy {
        decision: PolicyDecision::AutoSettle,
        seen: Arc::clone(&seen),
    };
    let verdict = json!({
        "action": "dismiss",
        "rationale": "the records already say this",
        "evidence_ids": ["rec_gate"]
    });
    let caps = MindCapabilities {
        reasoner: Some(&FixedVerdict(verdict)),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let outcomes = workflows
        .absorb(&principal(), &capture_request(), None, &caps)
        .await
        .expect("absorb");

    assert_eq!(
        outcomes,
        vec![AbsorbCandidateOutcome::Dropped {
            candidate_id: "cnd_1".to_string(),
        }]
    );
    let reviews = recorded.reviews.lock().expect("reviews");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0]["decision"]["action"], "drop");
    assert_eq!(
        reviews[0]["decision"]["rationale"],
        "the records already say this"
    );
    assert_eq!(
        reviews[0]["cited"],
        json!([{ "record_id": "rec_gate", "revision": 3 }])
    );
}

#[tokio::test]
async fn fabricated_evidence_never_reaches_policy_or_a_write() {
    let (workflows, recorded, seen) = absorb(pending_candidate());
    let policy = RecordingPolicy {
        decision: PolicyDecision::AutoSettle,
        seen: Arc::clone(&seen),
    };
    let verdict = json!({ "action": "promote", "evidence_ids": ["rec_ghost"] });
    let caps = MindCapabilities {
        reasoner: Some(&FixedVerdict(verdict)),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = workflows
        .absorb(&principal(), &capture_request(), None, &caps)
        .await
        .expect_err("a fabricated id must not settle");

    assert!(matches!(error, SpiError::InvalidReasonerOutput(_)));
    assert!(seen.lock().expect("seen").is_empty());
    assert!(recorded.reviews.lock().expect("reviews").is_empty());
    assert!(recorded.updates.lock().expect("updates").is_empty());
}

#[tokio::test]
async fn an_update_must_cite_the_record_it_targets() {
    let (workflows, recorded, _) = absorb(pending_candidate());
    let verdict = json!({
        "action": "update",
        "record_id": "rec_gate",
        "evidence_ids": []
    });
    let caps = MindCapabilities {
        reasoner: Some(&FixedVerdict(verdict)),
        ..MindCapabilities::default()
    };

    let error = workflows
        .absorb(&principal(), &capture_request(), None, &caps)
        .await
        .expect_err("an uncited target must not settle");

    assert!(matches!(error, SpiError::InvalidReasonerOutput(_)));
    assert!(recorded.reviews.lock().expect("reviews").is_empty());
    assert!(recorded.updates.lock().expect("updates").is_empty());
}

#[tokio::test]
async fn no_reasoner_keeps_the_capture_result_pending() {
    let (workflows, recorded, _) = absorb(pending_candidate());
    let caps = MindCapabilities::default();

    let outcomes = workflows
        .absorb(&principal(), &capture_request(), None, &caps)
        .await
        .expect("absorb");
    // A replay reports the same pending state and still writes nothing.
    let replay = workflows
        .absorb(&principal(), &capture_request(), None, &caps)
        .await
        .expect("replayed absorb");

    assert_eq!(
        outcomes,
        vec![AbsorbCandidateOutcome::Pending {
            candidate_id: "cnd_1".to_string(),
        }]
    );
    assert_eq!(replay, outcomes);
    assert!(recorded.reviews.lock().expect("reviews").is_empty());
}

#[tokio::test]
async fn default_policy_leaves_the_candidate_pending() {
    let (workflows, recorded, _) = absorb(pending_candidate());
    let verdict = json!({ "action": "promote", "evidence_ids": ["rec_gate"] });
    let caps = MindCapabilities {
        reasoner: Some(&FixedVerdict(verdict)),
        ..MindCapabilities::default()
    };

    let outcomes = workflows
        .absorb(&principal(), &capture_request(), None, &caps)
        .await
        .expect("absorb");

    assert_eq!(
        outcomes,
        vec![AbsorbCandidateOutcome::Pending {
            candidate_id: "cnd_1".to_string(),
        }]
    );
    assert!(recorded.reviews.lock().expect("reviews").is_empty());
}

#[tokio::test]
async fn a_replay_never_settles_what_a_policy_held() {
    let (workflows, recorded, seen) = absorb(pending_candidate());
    let asked = Arc::new(AtomicUsize::new(0));
    let reasoner = CountingReasoner {
        asked: Arc::clone(&asked),
    };

    // The default policy holds every settlement, and writes nothing.
    let held = workflows
        .absorb(
            &principal(),
            &capture_request(),
            None,
            &MindCapabilities {
                reasoner: Some(&reasoner),
                ..MindCapabilities::default()
            },
        )
        .await
        .expect("held absorb");
    assert_eq!(
        held,
        vec![AbsorbCandidateOutcome::Pending {
            candidate_id: "cnd_1".to_string(),
        }]
    );

    // The same capture replayed under a policy that would settle it: the replay
    // reports the recorded hold instead of deciding again.
    let policy = RecordingPolicy {
        decision: PolicyDecision::AutoSettle,
        seen: Arc::clone(&seen),
    };
    let replay = workflows
        .absorb(
            &principal(),
            &capture_request(),
            None,
            &MindCapabilities {
                reasoner: Some(&reasoner),
                policy_hook: Some(&policy),
                ..MindCapabilities::default()
            },
        )
        .await
        .expect("replayed absorb");

    assert_eq!(replay, held);
    assert_eq!(asked.load(Ordering::SeqCst), 1);
    assert!(seen.lock().expect("seen").is_empty());
    assert!(recorded.reviews.lock().expect("reviews").is_empty());
}

#[tokio::test]
async fn a_policy_rejection_drops_over_the_cited_evidence() {
    let (workflows, recorded, seen) = absorb(pending_candidate());
    let policy = RecordingPolicy {
        decision: PolicyDecision::Reject {
            reason: "the gate record is not trusted".to_string(),
        },
        seen: Arc::clone(&seen),
    };
    let verdict = json!({
        "action": "update",
        "record_id": "rec_gate",
        "evidence_ids": ["rec_gate"]
    });
    let caps = MindCapabilities {
        reasoner: Some(&FixedVerdict(verdict)),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let outcomes = workflows
        .absorb(&principal(), &capture_request(), None, &caps)
        .await
        .expect("absorb");

    assert_eq!(
        outcomes,
        vec![AbsorbCandidateOutcome::Dropped {
            candidate_id: "cnd_1".to_string(),
        }]
    );
    let reviews = recorded.reviews.lock().expect("reviews");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0]["decision"]["action"], "drop");
    assert_eq!(
        reviews[0]["decision"]["rationale"],
        "the gate record is not trusted"
    );
    // The refusal is the policy's, but the drop consumes the candidate over the
    // same page the decision cited, so the binding revalidates it first.
    assert_eq!(
        reviews[0]["cited"],
        json!([{ "record_id": "rec_gate", "revision": 3 }])
    );
}

#[tokio::test]
async fn a_consumed_candidate_reports_its_recorded_outcome() {
    let (workflows, recorded, _) = absorb(consumed_candidate());
    let caps = MindCapabilities {
        reasoner: Some(&UnreachableReasoner),
        ..MindCapabilities::default()
    };

    let outcomes = workflows
        .absorb(&principal(), &capture_request(), None, &caps)
        .await
        .expect("absorb");

    assert_eq!(
        outcomes,
        vec![AbsorbCandidateOutcome::Settled {
            candidate_id: "cnd_1".to_string(),
            record_ids: vec!["rec_new".to_string()],
        }]
    );
    assert!(recorded.reviews.lock().expect("reviews").is_empty());
}

#[tokio::test]
async fn no_reasoner_replay_reports_the_recorded_outcome() {
    let (workflows, recorded, _) = absorb(consumed_candidate());
    let caps = MindCapabilities::default();

    let outcomes = workflows
        .absorb(&principal(), &capture_request(), None, &caps)
        .await
        .expect("absorb");

    assert_eq!(
        outcomes,
        vec![AbsorbCandidateOutcome::Settled {
            candidate_id: "cnd_1".to_string(),
            record_ids: vec!["rec_new".to_string()],
        }]
    );
    assert!(recorded.reviews.lock().expect("reviews").is_empty());
}

#[tokio::test]
async fn a_dismissed_candidate_reports_the_record_it_updated() {
    let (workflows, recorded, _) = absorb_after_update(dismissed_candidate(), Some("rec_gate"));
    let caps = MindCapabilities {
        reasoner: Some(&UnreachableReasoner),
        ..MindCapabilities::default()
    };

    let outcomes = workflows
        .absorb(&principal(), &capture_request(), None, &caps)
        .await
        .expect("absorb");

    assert_eq!(
        outcomes,
        vec![AbsorbCandidateOutcome::Settled {
            candidate_id: "cnd_1".to_string(),
            record_ids: vec!["rec_gate".to_string()],
        }]
    );
    // The update leg's own receipt is read under this capture's stable key.
    let keys = recorded.update_keys.lock().expect("update keys");
    assert_eq!(keys.len(), 1);
    assert!(keys[0].starts_with("absorb-"));
}

#[tokio::test]
async fn a_dismissed_candidate_without_a_settled_update_is_dropped() {
    let (workflows, recorded, _) = absorb(dismissed_candidate());
    let caps = MindCapabilities {
        reasoner: Some(&UnreachableReasoner),
        ..MindCapabilities::default()
    };

    let outcomes = workflows
        .absorb(&principal(), &capture_request(), None, &caps)
        .await
        .expect("absorb");

    assert_eq!(
        outcomes,
        vec![AbsorbCandidateOutcome::Dropped {
            candidate_id: "cnd_1".to_string(),
        }]
    );
    assert_eq!(recorded.update_keys.lock().expect("update keys").len(), 1);
}

struct StubLens;

impl DomainLens for StubLens {
    fn describe(&self, task_kind: CognitiveTaskKind) -> LensOutput<'_> {
        assert_eq!(task_kind, CognitiveTaskKind::Absorb);
        LensOutput {
            persona_and_stance: "a gate warden",
            salience_criteria: "anything that changes the watch",
            evidence_bar: "one named record",
            redlines: &["never invent a record id"],
        }
    }
}

/// Promotes only when the lens block reached the prompt.
struct LensAwareReasoner(Arc<Mutex<Option<String>>>);

#[async_trait]
impl Reasoner for LensAwareReasoner {
    async fn reason_structured(
        &self,
        request: StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        *self.0.lock().expect("prompt") = Some(request.system_prompt.to_string());
        Ok(json!({ "action": "promote", "evidence_ids": [] }))
    }
}

#[tokio::test]
async fn a_lens_adds_its_domain_guidance_to_the_absorb_prompt() {
    let (workflows, _, seen) = absorb(pending_candidate());
    let prompt = Arc::new(Mutex::new(None));
    let policy = RecordingPolicy {
        decision: PolicyDecision::AutoSettle,
        seen: Arc::clone(&seen),
    };
    let reasoner = LensAwareReasoner(Arc::clone(&prompt));
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let outcomes = workflows
        .absorb(&principal(), &capture_request(), Some(&StubLens), &caps)
        .await
        .expect("absorb");

    assert!(matches!(
        outcomes.as_slice(),
        [AbsorbCandidateOutcome::Settled { .. }]
    ));
    let prompt = prompt.lock().expect("prompt").clone().expect("prompt");
    assert!(prompt.starts_with("Decide what one captured memory candidate"));
    assert!(prompt.contains("## Domain guidance"));
    assert!(prompt.contains("a gate warden"));
    assert!(prompt.contains("- never invent a record id"));
}
