//! Workflow behaviour over a pinned binding double: the recall page order,
//! hybrid rerank, pack budget and snapshot revalidation, with no storage.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};

use mindcore_spi::{
    AbsorbCandidateOutcome, AbsorbSnapshot, CandidateLifecycle, CaptureOutcome, CapturedCandidate,
    CitedRecord, CognitiveTaskKind, DeliberateOutcome, DeliberateRequest, DeliberateSnapshot,
    DomainLens, Embedder, EmbeddingIdentity, Evidence, LensOutput, MindCapabilities, NoEmbeddings,
    PolicyDecision, PolicyHook, ProposalDraft, Reasoner, RecallSnapshot, Recommendation,
    SettlementDraft, SpiError, SpiMindWorkflows, StructuredReasonRequest, WorkflowBinding,
};
use spoke_operations::SpokeRejectCode;
use spoke_schemas::data::cognition::{CognitionReference, CognitionTarget};
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

    async fn deliberate_snapshot(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _subject_id: &str,
        _holder: &CognitionReference,
        _max_items: u64,
    ) -> Result<DeliberateSnapshot, SpiError> {
        unserved()
    }

    async fn commit_cognition(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _subject_id: &str,
        _holder: &CognitionReference,
        _expected_carrier_revision: u64,
        _record: Value,
        _cited: &[CitedRecord],
    ) -> Result<u64, SpiError> {
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

    async fn deliberate_snapshot(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _subject_id: &str,
        _holder: &CognitionReference,
        _max_items: u64,
    ) -> Result<DeliberateSnapshot, SpiError> {
        Err(self.0.clone())
    }

    async fn commit_cognition(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _subject_id: &str,
        _holder: &CognitionReference,
        _expected_carrier_revision: u64,
        _record: Value,
        _cited: &[CitedRecord],
    ) -> Result<u64, SpiError> {
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
    /// Candidate reads served so far.
    snapshots: Arc<AtomicUsize>,
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
        // The first capture commits the receipt; every later one replays it,
        // reporting the state the double's candidate is in.
        let call = self.captures.fetch_add(1, Ordering::SeqCst);
        let lifecycle = match &self.candidate {
            MemoryCandidate::PendingCandidate(_) => CandidateLifecycle::Pending,
            MemoryCandidate::ConsumedCandidate(_) => CandidateLifecycle::Consumed,
        };
        Ok(CaptureOutcome {
            candidates: vec![CapturedCandidate {
                candidate_id: "cnd_1".to_string(),
                lifecycle,
            }],
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
        self.recorded.snapshots.fetch_add(1, Ordering::SeqCst);
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

    async fn deliberate_snapshot(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _subject_id: &str,
        _holder: &CognitionReference,
        _max_items: u64,
    ) -> Result<DeliberateSnapshot, SpiError> {
        unserved()
    }

    async fn commit_cognition(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _subject_id: &str,
        _holder: &CognitionReference,
        _expected_carrier_revision: u64,
        _record: Value,
        _cited: &[CitedRecord],
    ) -> Result<u64, SpiError> {
        unserved()
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
    absorb_at(candidate, settled_update, 0)
}

/// An absorb double whose capture key is already held: its first call replays.
fn absorb_replayed(
    candidate: MemoryCandidate,
) -> (SpiMindWorkflows, Recorded, Arc<Mutex<Vec<Value>>>) {
    absorb_at(candidate, None, 1)
}

fn absorb_at(
    candidate: MemoryCandidate,
    settled_update: Option<&str>,
    captures: usize,
) -> (SpiMindWorkflows, Recorded, Arc<Mutex<Vec<Value>>>) {
    let recorded = Recorded::default();
    let double = AbsorbDouble {
        candidate,
        page: page(),
        recorded: recorded.clone(),
        settled_update: settled_update.map(str::to_string),
        captures: AtomicUsize::new(captures),
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
    // Neither run read the candidate back: a fresh capture's candidate is
    // pending by construction, and a replay reports a pending candidate
    // straight from its receipt, needing no review or recall grant.
    assert_eq!(
        recorded.snapshots.load(Ordering::SeqCst),
        0,
        "a pending candidate is reported without a candidate read"
    );
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
    // The capture key is already held: this call is a replay, and its consumed
    // candidate still projects the recorded outcome without a reasoner.
    let (workflows, recorded, _) = absorb_replayed(consumed_candidate());
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

    fn validate_proposal(
        &self,
        _draft: &ProposalDraft<'_>,
        _admitted: &[&str],
    ) -> Result<(), String> {
        Ok(())
    }
}

/// Promotes only when the lens block reached the prompt, recording the system
/// prompt and the user payload so two runs can be compared for everything but
/// the lens.
struct LensAwareReasoner(Arc<Mutex<Option<(String, String)>>>);

#[async_trait]
impl Reasoner for LensAwareReasoner {
    async fn reason_structured(
        &self,
        request: StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        *self.0.lock().expect("prompt") = Some((
            request.system_prompt.to_string(),
            request.user_payload.to_string(),
        ));
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
    let (prompt, _payload) = prompt.lock().expect("prompt").clone().expect("prompt");
    assert!(prompt.starts_with("Decide what one captured memory candidate"));
    assert!(prompt.contains("## Domain guidance"));
    assert!(prompt.contains("a gate warden"));
    assert!(prompt.contains("- never invent a record id"));
}

// ---------- deliberate ----------

/// One deliberation over a fixed snapshot, recording every settlement it is
/// asked to commit.
struct DeliberateDouble {
    snapshot: DeliberateSnapshot,
    snapshots: Arc<AtomicUsize>,
    results: Mutex<std::collections::VecDeque<Result<u64, SpiError>>>,
    settlements: Arc<Mutex<Vec<Value>>>,
    citations: Arc<Mutex<Vec<(String, u64)>>>,
}

impl DeliberateDouble {
    fn new() -> Self {
        Self {
            snapshot: DeliberateSnapshot {
                records: vec![record("rec_gate", "the north gate is closed", 3)],
                cognition: Vec::new(),
                carrier_revision: 1,
            },
            snapshots: Arc::new(AtomicUsize::new(0)),
            results: Mutex::new(std::collections::VecDeque::new()),
            settlements: Arc::new(Mutex::new(Vec::new())),
            citations: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn failing(error: SpiError) -> Self {
        let double = Self::new();
        double
            .results
            .lock()
            .expect("results")
            .push_back(Err(error));
        double
    }

    fn then_settling(self, version: u64) -> Self {
        self.results.lock().expect("results").push_back(Ok(version));
        self
    }

    fn snapshots(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.snapshots)
    }

    fn settlements(&self) -> Arc<Mutex<Vec<Value>>> {
        Arc::clone(&self.settlements)
    }

    fn citations(&self) -> Arc<Mutex<Vec<(String, u64)>>> {
        Arc::clone(&self.citations)
    }
}

#[async_trait]
impl WorkflowBinding for DeliberateDouble {
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

    async fn deliberate_snapshot(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _subject_id: &str,
        _holder: &CognitionReference,
        _max_items: u64,
    ) -> Result<DeliberateSnapshot, SpiError> {
        self.snapshots.fetch_add(1, Ordering::SeqCst);
        Ok(self.snapshot.clone())
    }

    async fn commit_cognition(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _subject_id: &str,
        _holder: &CognitionReference,
        _expected_carrier_revision: u64,
        record: Value,
        cited: &[CitedRecord],
    ) -> Result<u64, SpiError> {
        self.settlements.lock().expect("settlements").push(record);
        self.citations.lock().expect("citations").extend(
            cited
                .iter()
                .map(|record| (record.record_id.clone(), record.revision)),
        );
        let queued = self.results.lock().expect("results").pop_front();
        match queued {
            Some(result) => result,
            None => Ok(self.snapshot.carrier_revision + 1),
        }
    }
}

fn deliberate_holder() -> CognitionReference {
    serde_json::from_value(json!({
        "kind": "knowledge_entry",
        "namespace": "mindcore",
        "id": "ke_wren"
    }))
    .expect("holder")
}

fn deliberate_target() -> CognitionTarget {
    serde_json::from_value(json!({
        "carrier": {
            "kind": "knowledge_entry",
            "namespace": "mindcore",
            "id": "ke_shared_gate"
        },
        "target_id": "prop_gate_closed",
        "kind": "proposition"
    }))
    .expect("target")
}

fn promote_verdict() -> Value {
    json!({
        "recommendation": { "kind": "promote", "target_kind": "adr" },
        "rationale": "the gate test passed on the supplied evidence",
        "evidence_ids": ["ev_test"],
        "record_ids": ["rec_gate"],
        "record": {
            "kind": "belief",
            "order": 1,
            "q": {
                "value": 0.8,
                "assertion": "belief_attribution",
                "method": "two_witness_reports",
                "calibration": "uncalibrated"
            },
            "assessor_role": "holder_self_report",
            "basis": {
                "kind": "evidence",
                "evidence": [
                    { "kind": "memory_record", "namespace": "mindcore", "id": "rec_gate" }
                ]
            }
        }
    })
}

struct DeliberateReasoner {
    verdict: Value,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Reasoner for DeliberateReasoner {
    async fn reason_structured(
        &self,
        request: StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        assert_eq!(request.task_kind, CognitiveTaskKind::Deliberate);
        assert!(request
            .system_prompt
            .starts_with("Deliberate what one cognition holder"));
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.verdict.clone())
    }
}

/// A policy that must never be reached.
struct UnreachablePolicy;

#[async_trait]
impl PolicyHook for UnreachablePolicy {
    async fn evaluate_settlement(
        &self,
        _draft: SettlementDraft<'_>,
    ) -> Result<PolicyDecision, SpiError> {
        panic!("this proposal must not reach the policy hook");
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

/// Records the validated draft it was handed, then refuses an ADR promotion.
struct RefusingAdrLens(Arc<Mutex<Option<String>>>);

impl DomainLens for RefusingAdrLens {
    fn describe(&self, _task_kind: CognitiveTaskKind) -> LensOutput<'_> {
        LensOutput {
            persona_and_stance: "an engineering evaluator",
            salience_criteria: "test and CI evidence",
            evidence_bar: "reproducible on the head commit",
            redlines: &["never promote an unverified observation to an ADR"],
        }
    }

    fn validate_proposal(
        &self,
        draft: &ProposalDraft<'_>,
        admitted: &[&str],
    ) -> Result<(), String> {
        let ProposalDraft::Cognition {
            recommendation,
            cited_ids,
            ..
        } = draft
        else {
            panic!("a deliberate proposal is a cognition draft");
        };
        let Recommendation::Promote { target_kind } = recommendation else {
            panic!("expected a promotion");
        };
        assert_eq!(*target_kind, "adr");
        assert_eq!(*cited_ids, ["ev_test", "rec_gate"]);
        assert_eq!(admitted, ["ev_test", "rec_gate"]);
        *self.0.lock().expect("draft") = Some(target_kind.clone());
        Err("an unverified observation cannot become an ADR".to_string())
    }
}

fn revision_conflict() -> SpiError {
    SpiError::Settlement(spoke_operations::SpokeReject {
        code: SpokeRejectCode::RevisionConflict,
        message: "the snapshot moved".to_string(),
        details: None,
    })
}

/// The refusal a binding returns when it cannot tell whether its write
/// committed.
fn uncertain_write() -> SpiError {
    SpiError::Settlement(spoke_operations::SpokeReject {
        code: SpokeRejectCode::OutcomeUncertain,
        message: "the commit outcome is unknown".to_string(),
        details: None,
    })
}

#[tokio::test]
async fn deliberate_settles_the_stamped_record_onto_the_committed_carrier() {
    let binding = DeliberateDouble::new();
    let settlements = binding.settlements();
    let citations = binding.citations();
    let workflows = SpiMindWorkflows::new(binding);
    let reasoner = DeliberateReasoner {
        verdict: promote_verdict(),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };
    let holder = deliberate_holder();
    let target = deliberate_target();
    let evidence = [Evidence {
        id: "ev_test",
        text: "the gate test passed on this commit",
    }];
    let request = DeliberateRequest {
        space_id: "spc_1",
        subject_id: "subj_1",
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: std::time::Duration::from_secs(30),
    };

    let outcome = workflows
        .deliberate(&principal(), request, None, &caps)
        .await
        .expect("deliberate");

    let DeliberateOutcome::Settled {
        record,
        mind_state_version,
        ..
    } = outcome
    else {
        panic!("expected a settlement, got {outcome:?}");
    };
    assert_eq!(mind_state_version, 2);
    let settled = settlements.lock().expect("settlements");
    assert_eq!(settled.len(), 1);
    let stored = &settled[0];
    assert_eq!(stored["holder"], json!(holder));
    assert_eq!(stored["target"], json!(target));
    assert_eq!(stored["view"], "current");
    assert_eq!(stored["assessor"]["id"], "prn_1");
    assert_eq!(stored["assessment_context"]["context_id"], "subj_1");
    assert!(stored["record_id"]
        .as_str()
        .expect("stamped record id")
        .starts_with("cog_"));
    // The returned record is the one that was committed, stamp included.
    assert_eq!(
        serde_json::to_value(&record).expect("record serializes"),
        *stored
    );
    assert_eq!(
        citations.lock().expect("citations").as_slice(),
        [("rec_gate".to_string(), 3)]
    );
}

#[tokio::test]
async fn deliberate_refuses_fabricated_references_before_any_write() {
    let mut verdicts = vec![
        {
            let mut verdict = promote_verdict();
            verdict["evidence_ids"] = json!(["ev_ghost"]);
            verdict
        },
        {
            let mut verdict = promote_verdict();
            verdict["record_ids"] = json!(["rec_ghost"]);
            verdict
        },
        {
            let mut verdict = promote_verdict();
            verdict["record"]["basis"]["evidence"][0]["id"] = json!("rec_ghost");
            verdict
        },
        {
            // Visible id, fabricated identity: not the admitted reference.
            let mut verdict = promote_verdict();
            verdict["record"]["basis"]["evidence"][0]["namespace"] = json!("external");
            verdict
        },
        {
            // Visible id, fabricated kind: not the admitted reference either.
            let mut verdict = promote_verdict();
            verdict["record"]["basis"]["evidence"][0]["kind"] = json!("knowledge_entry");
            verdict
        },
        {
            // A visible record the verdict never cited has no revision the
            // commit could revalidate.
            let mut verdict = promote_verdict();
            verdict["record_ids"] = json!([]);
            verdict
        },
        {
            // Supplied evidence carries no record identity a basis can bind to.
            let mut verdict = promote_verdict();
            verdict["record"]["basis"]["evidence"][0]["id"] = json!("ev_test");
            verdict
        },
    ];
    for verdict in verdicts.drain(..) {
        let binding = DeliberateDouble::new();
        let settlements = binding.settlements();
        let workflows = SpiMindWorkflows::new(binding);
        let reasoner = DeliberateReasoner {
            verdict,
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let policy = AutoPolicy;
        let caps = MindCapabilities {
            reasoner: Some(&reasoner),
            policy_hook: Some(&policy),
            ..MindCapabilities::default()
        };
        let holder = deliberate_holder();
        let target = deliberate_target();
        let evidence = [Evidence {
            id: "ev_test",
            text: "the gate test passed on this commit",
        }];
        let request = DeliberateRequest {
            space_id: "spc_1",
            subject_id: "subj_1",
            holder: &holder,
            target: &target,
            evidence: &evidence,
            timeout: std::time::Duration::from_secs(30),
        };

        let error = workflows
            .deliberate(&principal(), request, None, &caps)
            .await
            .expect_err("a fabricated reference must be refused");

        assert!(
            matches!(error, SpiError::InvalidReasonerOutput(_)),
            "expected an invalid verdict, got {error:?}"
        );
        assert!(settlements.lock().expect("settlements").is_empty());
    }
}

#[tokio::test]
async fn deliberate_lens_violation_refuses_before_policy_and_write() {
    let binding = DeliberateDouble::new();
    let settlements = binding.settlements();
    let workflows = SpiMindWorkflows::new(binding);
    let reasoner = DeliberateReasoner {
        verdict: promote_verdict(),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let lens = RefusingAdrLens(Arc::new(Mutex::new(None)));
    let policy = UnreachablePolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };
    let holder = deliberate_holder();
    let target = deliberate_target();
    let evidence = [Evidence {
        id: "ev_test",
        text: "the gate test passed on this commit",
    }];
    let request = DeliberateRequest {
        space_id: "spc_1",
        subject_id: "subj_1",
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: std::time::Duration::from_secs(30),
    };

    let error = workflows
        .deliberate(&principal(), request, Some(&lens), &caps)
        .await
        .expect_err("a lens redline must refuse the proposal");

    assert!(
        matches!(error, SpiError::PolicyRejected(_)),
        "expected a policy refusal, got {error:?}"
    );
    assert_eq!(
        lens.0.lock().expect("draft").as_deref(),
        Some("adr"),
        "the lens saw the structured recommendation"
    );
    assert!(settlements.lock().expect("settlements").is_empty());
}

/// Refuses a `none` recommendation: this domain needs a record for every
/// deliberation, so its redline binds even when nothing would be written.
struct RefusingNoneLens(Arc<Mutex<Option<bool>>>);

impl DomainLens for RefusingNoneLens {
    fn describe(&self, _task_kind: CognitiveTaskKind) -> LensOutput<'_> {
        LensOutput {
            persona_and_stance: "an auditor",
            salience_criteria: "anything that changes the record",
            evidence_bar: "the supplied evidence",
            redlines: &["never leave a deliberation without a record"],
        }
    }

    fn validate_proposal(
        &self,
        draft: &ProposalDraft<'_>,
        _admitted: &[&str],
    ) -> Result<(), String> {
        let ProposalDraft::Cognition {
            recommendation,
            record,
            ..
        } = draft
        else {
            panic!("a deliberate proposal is a cognition draft");
        };
        assert!(matches!(recommendation, Recommendation::None));
        assert!(record.is_none(), "a none recommendation carries no record");
        *self.0.lock().expect("draft") = Some(true);
        Err("this domain requires a record for every deliberation".to_string())
    }
}

#[tokio::test]
async fn deliberate_lens_refuses_a_none_recommendation() {
    let binding = DeliberateDouble::new();
    let settlements = binding.settlements();
    let workflows = SpiMindWorkflows::new(binding);
    let reasoner = DeliberateReasoner {
        verdict: json!({
            "recommendation": { "kind": "none" },
            "rationale": "the evidence says nothing new",
            "evidence_ids": ["ev_test"],
            "record_ids": []
        }),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let lens = RefusingNoneLens(Arc::new(Mutex::new(None)));
    let policy = UnreachablePolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };
    let holder = deliberate_holder();
    let target = deliberate_target();
    let evidence = [Evidence {
        id: "ev_test",
        text: "the gate test passed on this commit",
    }];
    let request = DeliberateRequest {
        space_id: "spc_1",
        subject_id: "subj_1",
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: std::time::Duration::from_secs(30),
    };

    let error = workflows
        .deliberate(&principal(), request, Some(&lens), &caps)
        .await
        .expect_err("a lens redline binds a none recommendation too");

    assert!(
        matches!(error, SpiError::PolicyRejected(_)),
        "expected a policy refusal, got {error:?}"
    );
    assert_eq!(
        *lens.0.lock().expect("draft"),
        Some(true),
        "the lens saw the none recommendation"
    );
    assert!(settlements.lock().expect("settlements").is_empty());
}

/// A reasoner that never resolves: only the deadline can end the call.
struct HungReasoner;

#[async_trait]
impl Reasoner for HungReasoner {
    async fn reason_structured(
        &self,
        _request: StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        std::future::pending().await
    }
}

/// A policy that never resolves: only the deadline can end the call.
struct HungPolicy;

#[async_trait]
impl PolicyHook for HungPolicy {
    async fn evaluate_settlement(
        &self,
        _draft: SettlementDraft<'_>,
    ) -> Result<PolicyDecision, SpiError> {
        std::future::pending().await
    }
}

/// The deadline under test, and the outer guard that fails the run instead of
/// hanging if a provider is awaited rather than dropped.
const TIGHT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(10);
const CANCELLATION_GUARD: std::time::Duration = std::time::Duration::from_secs(5);

#[tokio::test]
async fn deliberate_drops_a_hung_reasoner_at_the_deadline() {
    let binding = DeliberateDouble::new();
    let settlements = binding.settlements();
    let workflows = SpiMindWorkflows::new(binding);
    let reasoner = HungReasoner;
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };
    let holder = deliberate_holder();
    let target = deliberate_target();
    let evidence = [Evidence {
        id: "ev_test",
        text: "the gate test passed on this commit",
    }];
    let request = DeliberateRequest {
        space_id: "spc_1",
        subject_id: "subj_1",
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: TIGHT_TIMEOUT,
    };

    let outcome = tokio::time::timeout(
        CANCELLATION_GUARD,
        workflows.deliberate(&principal(), request, None, &caps),
    )
    .await
    .expect("a hung reasoner must not block the call");
    let error = outcome.expect_err("a hung reasoner settles nothing");

    assert!(matches!(error, SpiError::DeadlineExceeded));
    assert!(settlements.lock().expect("settlements").is_empty());
}

#[tokio::test]
async fn deliberate_starts_no_write_after_the_policy_overran_the_deadline() {
    let binding = DeliberateDouble::new();
    let settlements = binding.settlements();
    let workflows = SpiMindWorkflows::new(binding);
    let reasoner = DeliberateReasoner {
        verdict: promote_verdict(),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let policy = HungPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };
    let holder = deliberate_holder();
    let target = deliberate_target();
    let evidence = [Evidence {
        id: "ev_test",
        text: "the gate test passed on this commit",
    }];
    let request = DeliberateRequest {
        space_id: "spc_1",
        subject_id: "subj_1",
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: TIGHT_TIMEOUT,
    };

    let outcome = tokio::time::timeout(
        CANCELLATION_GUARD,
        workflows.deliberate(&principal(), request, None, &caps),
    )
    .await
    .expect("a hung policy must not block the call");
    let error = outcome.expect_err("a policy that ran past the deadline starts no write");

    assert!(matches!(error, SpiError::DeadlineExceeded));
    assert!(settlements.lock().expect("settlements").is_empty());
}

#[tokio::test]
async fn deliberate_retries_a_stale_carrier_twice_then_surfaces_it() {
    let binding = DeliberateDouble::failing(revision_conflict());
    binding
        .results
        .lock()
        .expect("results")
        .push_back(Err(revision_conflict()));
    binding
        .results
        .lock()
        .expect("results")
        .push_back(Err(revision_conflict()));
    let snapshots = binding.snapshots();
    let settlements = binding.settlements();
    let workflows = SpiMindWorkflows::new(binding);
    let calls = Arc::new(AtomicUsize::new(0));
    let reasoner = DeliberateReasoner {
        verdict: promote_verdict(),
        calls: Arc::clone(&calls),
    };
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };
    let holder = deliberate_holder();
    let target = deliberate_target();
    let evidence = [Evidence {
        id: "ev_test",
        text: "the gate test passed on this commit",
    }];
    let request = DeliberateRequest {
        space_id: "spc_1",
        subject_id: "subj_1",
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: std::time::Duration::from_secs(30),
    };

    let error = workflows
        .deliberate(&principal(), request, None, &caps)
        .await
        .expect_err("an exhausted retry budget surfaces the conflict");

    assert!(
        matches!(error, SpiError::Settlement(ref reject) if reject.code == SpokeRejectCode::RevisionConflict),
        "expected the conflict to surface, got {error:?}"
    );
    assert_eq!(
        snapshots.load(Ordering::SeqCst),
        3,
        "one attempt plus two retries"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "every attempt reasons again"
    );
    assert_eq!(settlements.lock().expect("settlements").len(), 3);
}

#[tokio::test]
async fn deliberate_retries_a_stale_carrier_until_the_snapshot_holds() {
    let binding = DeliberateDouble::failing(revision_conflict()).then_settling(4);
    let snapshots = binding.snapshots();
    let workflows = SpiMindWorkflows::new(binding);
    let reasoner = DeliberateReasoner {
        verdict: promote_verdict(),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };
    let holder = deliberate_holder();
    let target = deliberate_target();
    let evidence = [Evidence {
        id: "ev_test",
        text: "the gate test passed on this commit",
    }];
    let request = DeliberateRequest {
        space_id: "spc_1",
        subject_id: "subj_1",
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: std::time::Duration::from_secs(30),
    };

    let outcome = workflows
        .deliberate(&principal(), request, None, &caps)
        .await
        .expect("the fresh snapshot settles");

    assert!(matches!(
        outcome,
        DeliberateOutcome::Settled {
            mind_state_version: 4,
            ..
        }
    ));
    assert_eq!(snapshots.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn deliberate_does_not_retry_a_refusal_or_an_uncertain_write() {
    // A snapshot refusal is terminal: nothing was reasoned and no write ran.
    let holder = deliberate_holder();
    let target = deliberate_target();
    let evidence = [Evidence {
        id: "ev_test",
        text: "the gate test passed on this commit",
    }];
    let request = DeliberateRequest {
        space_id: "spc_1",
        subject_id: "subj_1",
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: std::time::Duration::from_secs(30),
    };
    let refusing = RefusingBinding(SpiError::Settlement(spoke_operations::SpokeReject {
        code: SpokeRejectCode::NotAuthorized,
        message: "no admission".to_string(),
        details: None,
    }));
    let workflows = SpiMindWorkflows::new(refusing);
    let reasoner = DeliberateReasoner {
        verdict: promote_verdict(),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };
    let error = workflows
        .deliberate(&principal(), request, None, &caps)
        .await
        .expect_err("a refused read does not settle");
    assert!(matches!(
        error,
        SpiError::Settlement(ref reject) if reject.code == SpokeRejectCode::NotAuthorized
    ));

    // An uncertain write is surfaced once, never turned into a fresh attempt.
    let binding = DeliberateDouble::failing(uncertain_write());
    let snapshots = binding.snapshots();
    let settlements = binding.settlements();
    let workflows = SpiMindWorkflows::new(binding);
    let reasoner = DeliberateReasoner {
        verdict: promote_verdict(),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let holder = deliberate_holder();
    let target = deliberate_target();
    let evidence = [Evidence {
        id: "ev_test",
        text: "the gate test passed on this commit",
    }];
    let request = DeliberateRequest {
        space_id: "spc_1",
        subject_id: "subj_1",
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: std::time::Duration::from_secs(30),
    };
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };
    let error = workflows
        .deliberate(&principal(), request, None, &caps)
        .await
        .expect_err("an uncertain outcome is not a clean refusal");
    assert!(
        matches!(
            &error,
            SpiError::Settlement(reject) if reject.code == SpokeRejectCode::OutcomeUncertain
        ),
        "the typed uncertain refusal must travel unchanged, got {error:?}"
    );
    assert_eq!(snapshots.load(Ordering::SeqCst), 1);
    assert_eq!(settlements.lock().expect("settlements").len(), 1);
}

#[tokio::test]
async fn deliberate_none_recommendation_writes_nothing() {
    let binding = DeliberateDouble::new();
    let settlements = binding.settlements();
    let workflows = SpiMindWorkflows::new(binding);
    let reasoner = DeliberateReasoner {
        verdict: json!({
            "recommendation": { "kind": "none" },
            "rationale": "the evidence says nothing new",
            "evidence_ids": ["ev_test"],
            "record_ids": []
        }),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let policy = UnreachablePolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };
    let holder = deliberate_holder();
    let target = deliberate_target();
    let evidence = [Evidence {
        id: "ev_test",
        text: "the gate test passed on this commit",
    }];
    let request = DeliberateRequest {
        space_id: "spc_1",
        subject_id: "subj_1",
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: std::time::Duration::from_secs(30),
    };

    let outcome = workflows
        .deliberate(&principal(), request, None, &caps)
        .await
        .expect("deliberate");

    assert!(matches!(outcome, DeliberateOutcome::NoChange { .. }));
    assert!(settlements.lock().expect("settlements").is_empty());
}

#[tokio::test]
async fn deliberate_without_a_reasoner_is_unavailable() {
    let binding = DeliberateDouble::new();
    let snapshots = binding.snapshots();
    let workflows = SpiMindWorkflows::new(binding);
    let holder = deliberate_holder();
    let target = deliberate_target();
    let evidence = [Evidence {
        id: "ev_test",
        text: "the gate test passed on this commit",
    }];
    let request = DeliberateRequest {
        space_id: "spc_1",
        subject_id: "subj_1",
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: std::time::Duration::from_secs(30),
    };

    let error = workflows
        .deliberate(&principal(), request, None, &MindCapabilities::default())
        .await
        .expect_err("no reasoner, no verdict");

    assert!(matches!(error, SpiError::ReasonerUnavailable));
    assert_eq!(snapshots.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn deliberate_starts_no_attempt_once_the_deadline_passed() {
    let binding = DeliberateDouble::new();
    let snapshots = binding.snapshots();
    let settlements = binding.settlements();
    let workflows = SpiMindWorkflows::new(binding);
    let reasoner = DeliberateReasoner {
        verdict: promote_verdict(),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };
    let holder = deliberate_holder();
    let target = deliberate_target();
    let evidence = [Evidence {
        id: "ev_test",
        text: "the gate test passed on this commit",
    }];
    let request = DeliberateRequest {
        space_id: "spc_1",
        subject_id: "subj_1",
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: std::time::Duration::ZERO,
    };

    let error = workflows
        .deliberate(&principal(), request, None, &caps)
        .await
        .expect_err("no attempt starts without elapsed-time budget");

    assert!(matches!(error, SpiError::DeadlineExceeded));
    assert_eq!(
        snapshots.load(Ordering::SeqCst),
        0,
        "no attempt, and so no retry, starts after the deadline"
    );
    assert!(settlements.lock().expect("settlements").is_empty());
}

#[tokio::test]
async fn deliberate_clamps_an_absurd_timeout_and_still_settles() {
    let binding = DeliberateDouble::new();
    let settlements = binding.settlements();
    let workflows = SpiMindWorkflows::new(binding);
    let reasoner = DeliberateReasoner {
        verdict: promote_verdict(),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };
    let holder = deliberate_holder();
    let target = deliberate_target();
    let evidence = [Evidence {
        id: "ev_test",
        text: "the gate test passed on this commit",
    }];
    let request = DeliberateRequest {
        space_id: "spc_1",
        subject_id: "subj_1",
        holder: &holder,
        target: &target,
        evidence: &evidence,
        // A timeout no instant can hold: the deadline is clamped, not dropped.
        timeout: std::time::Duration::MAX,
    };

    let outcome = workflows
        .deliberate(&principal(), request, None, &caps)
        .await
        .expect("a clamped deadline never refuses a call that fits inside it");

    assert!(matches!(outcome, DeliberateOutcome::Settled { .. }));
    assert_eq!(settlements.lock().expect("settlements").len(), 1);
}

/// Refuses every absorb decision: prompt fragments guide, but only this
/// deterministic check can refuse before the review write.
struct RefusingAbsorbLens;

impl DomainLens for RefusingAbsorbLens {
    fn describe(&self, _task_kind: CognitiveTaskKind) -> LensOutput<'_> {
        LensOutput {
            persona_and_stance: "a gate warden",
            salience_criteria: "anything that changes the watch",
            evidence_bar: "one named record",
            redlines: &["never promote without a witness"],
        }
    }

    fn validate_proposal(
        &self,
        draft: &ProposalDraft<'_>,
        admitted: &[&str],
    ) -> Result<(), String> {
        let ProposalDraft::Absorb {
            action,
            target_record_id,
            rationale,
            cited_ids,
        } = draft
        else {
            panic!("an absorb proposal is an absorb draft");
        };
        assert_eq!(*action, "promote");
        assert_eq!(*target_record_id, None);
        assert_eq!(*rationale, None, "a promotion gives no dismissal rationale");
        assert_eq!(*cited_ids, ["rec_gate"]);
        assert_eq!(admitted, ["rec_gate"]);
        Err("a promotion needs a witness the page does not hold".to_string())
    }
}

#[tokio::test]
async fn an_absorb_lens_violation_refuses_before_policy_and_write() {
    let (workflows, recorded, seen) = absorb(pending_candidate());
    let policy = RecordingPolicy {
        decision: PolicyDecision::AutoSettle,
        seen: Arc::clone(&seen),
    };
    let reasoner = FixedVerdict(json!({ "action": "promote", "evidence_ids": ["rec_gate"] }));
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = workflows
        .absorb(
            &principal(),
            &capture_request(),
            Some(&RefusingAbsorbLens),
            &caps,
        )
        .await
        .expect_err("a lens redline must refuse the proposal");

    assert!(
        matches!(error, SpiError::PolicyRejected(_)),
        "expected a policy refusal, got {error:?}"
    );
    assert!(seen.lock().expect("seen").is_empty(), "policy never ran");
    assert!(recorded.reviews.lock().expect("reviews").is_empty());
}

/// Refuses a dismissal whose validated rationale the domain will not stand
/// behind: citations alone do not tell two dismissals apart.
struct RefusingDismissLens;

impl DomainLens for RefusingDismissLens {
    fn describe(&self, _task_kind: CognitiveTaskKind) -> LensOutput<'_> {
        LensOutput {
            persona_and_stance: "a gate warden",
            salience_criteria: "anything that changes the watch",
            evidence_bar: "one named record",
            redlines: &["a dismissal must name what the record gets wrong"],
        }
    }

    fn validate_proposal(
        &self,
        draft: &ProposalDraft<'_>,
        admitted: &[&str],
    ) -> Result<(), String> {
        let ProposalDraft::Absorb {
            action,
            target_record_id,
            rationale,
            cited_ids,
        } = draft
        else {
            panic!("an absorb proposal is an absorb draft");
        };
        assert_eq!(*action, "dismiss");
        assert_eq!(*target_record_id, None);
        assert_eq!(admitted, ["rec_gate"]);
        assert_eq!(*cited_ids, ["rec_gate"]);
        assert_eq!(*rationale, Some("this adds nothing worth keeping"));
        Err("a dismissal needs a reason the domain accepts".to_string())
    }
}

#[tokio::test]
async fn an_absorb_lens_enforces_a_redline_on_the_dismissal_rationale() {
    let (workflows, recorded, seen) = absorb(pending_candidate());
    let policy = RecordingPolicy {
        decision: PolicyDecision::AutoSettle,
        seen: Arc::clone(&seen),
    };
    let reasoner = FixedVerdict(json!({
        "action": "dismiss",
        "rationale": "this adds nothing worth keeping",
        "evidence_ids": ["rec_gate"],
    }));
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = workflows
        .absorb(
            &principal(),
            &capture_request(),
            Some(&RefusingDismissLens),
            &caps,
        )
        .await
        .expect_err("a lens redline on the rationale must refuse the proposal");

    assert!(
        matches!(error, SpiError::PolicyRejected(_)),
        "expected a policy refusal, got {error:?}"
    );
    assert!(seen.lock().expect("seen").is_empty(), "policy never ran");
    assert!(recorded.reviews.lock().expect("reviews").is_empty());
}

// ---------- second lens: engineering evaluator vs Nexus narrative ----------

/// The observations the engineering fixture has independently verified as
/// reproducible test/CI/performance evidence. A record's text, a verdict's
/// rationale or a target label never adds to this set.
const VERIFIED_EVIDENCE: &[&str] = &["ev_test"];

/// Engineering-evaluator lens: a second domain over the same engine.
struct EngineeringEvaluatorLens;

impl DomainLens for EngineeringEvaluatorLens {
    fn describe(&self, _task_kind: CognitiveTaskKind) -> LensOutput<'_> {
        LensOutput {
            persona_and_stance: "an engineering evaluator weighing test, CI and performance signal",
            salience_criteria: "test, CI and performance signals on the head commit",
            evidence_bar: "reproducible evidence: a named test, CI run or benchmark",
            redlines: &["never promote an unverified observation to an ADR"],
        }
    }

    fn validate_proposal(
        &self,
        draft: &ProposalDraft<'_>,
        admitted: &[&str],
    ) -> Result<(), String> {
        let ProposalDraft::Cognition {
            recommendation: Recommendation::Promote { target_kind },
            cited_ids,
            ..
        } = draft
        else {
            // The redline binds ADR promotion only: an absorb decision or a
            // `none` recommendation rests on the engine's own checks.
            return Ok(());
        };
        if target_kind != "adr" {
            return Ok(());
        }
        if cited_ids
            .iter()
            .any(|id| VERIFIED_EVIDENCE.contains(id) && admitted.contains(id))
        {
            return Ok(());
        }
        Err("an ADR cites no test, CI or performance observation this fixture verified".to_string())
    }
}

/// Nexus-style narrative lens over the same trait: no redline against the
/// proposal this domain is asked to accept.
struct NarrativeLens;

impl DomainLens for NarrativeLens {
    fn describe(&self, _task_kind: CognitiveTaskKind) -> LensOutput<'_> {
        LensOutput {
            persona_and_stance: "a narrative continuity editor for the Nexus chronicle",
            salience_criteria: "voice, motive and continuity of the told events",
            evidence_bar: "a cited passage of the supplied narrative record",
            redlines: &["never contradict a settled scene"],
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

/// The one request both domains deliberate over.
fn lens_request<'a>(
    holder: &'a CognitionReference,
    target: &'a CognitionTarget,
    evidence: &'a [Evidence<'a>],
) -> DeliberateRequest<'a> {
    DeliberateRequest {
        space_id: "spc_1",
        subject_id: "subj_1",
        holder,
        target,
        evidence,
        timeout: std::time::Duration::from_secs(30),
    }
}

/// A promote-to-adr verdict citing exactly these ids. The rationale is model
/// text: it can claim verification but never confers it.
fn adr_verdict(rationale: &str, evidence_ids: &[&str], record_ids: &[&str]) -> Value {
    let mut verdict = promote_verdict();
    verdict["rationale"] = json!(rationale);
    verdict["evidence_ids"] = json!(evidence_ids);
    verdict["record_ids"] = json!(record_ids);
    verdict
}

/// Returns a fixed verdict and records the prompt it was handed, so two runs
/// over one request can be compared for everything but the lens.
struct PromptedVerdict {
    verdict: Value,
    prompt: Arc<Mutex<Option<(String, String)>>>,
}

#[async_trait]
impl Reasoner for PromptedVerdict {
    async fn reason_structured(
        &self,
        request: StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        *self.prompt.lock().expect("prompt") = Some((
            request.system_prompt.to_string(),
            request.user_payload.to_string(),
        ));
        Ok(self.verdict.clone())
    }
}

#[tokio::test]
async fn only_the_lens_differs_across_the_narrative_and_engineering_deliberation() {
    let verdict = adr_verdict(
        "the gate test passed on the supplied evidence",
        &["ev_test"],
        &["rec_gate"],
    );
    let holder = deliberate_holder();
    let target = deliberate_target();
    let evidence = [Evidence {
        id: "ev_test",
        text: "the gate test passed on this commit",
    }];

    let narrative_prompt = Arc::new(Mutex::new(None));
    let narrative_reasoner = PromptedVerdict {
        verdict: verdict.clone(),
        prompt: Arc::clone(&narrative_prompt),
    };
    let narrative_policy = AutoPolicy;
    let narrative_caps = MindCapabilities {
        reasoner: Some(&narrative_reasoner),
        policy_hook: Some(&narrative_policy),
        ..MindCapabilities::default()
    };
    let binding = DeliberateDouble::new();
    let settlements = binding.settlements();
    let workflows = SpiMindWorkflows::new(binding);
    let outcome = workflows
        .deliberate(
            &principal(),
            lens_request(&holder, &target, &evidence),
            Some(&NarrativeLens),
            &narrative_caps,
        )
        .await
        .expect("the narrative lens settles the promotion");
    assert!(matches!(outcome, DeliberateOutcome::Settled { .. }));
    assert_eq!(settlements.lock().expect("settlements").len(), 1);

    let engineering_prompt = Arc::new(Mutex::new(None));
    let engineering_reasoner = PromptedVerdict {
        verdict: verdict.clone(),
        prompt: Arc::clone(&engineering_prompt),
    };
    let engineering_policy = AutoPolicy;
    let engineering_caps = MindCapabilities {
        reasoner: Some(&engineering_reasoner),
        policy_hook: Some(&engineering_policy),
        ..MindCapabilities::default()
    };
    let binding = DeliberateDouble::new();
    let settlements = binding.settlements();
    let workflows = SpiMindWorkflows::new(binding);
    let outcome = workflows
        .deliberate(
            &principal(),
            lens_request(&holder, &target, &evidence),
            Some(&EngineeringEvaluatorLens),
            &engineering_caps,
        )
        .await
        .expect("the engineering lens settles the verified promotion");
    assert!(matches!(outcome, DeliberateOutcome::Settled { .. }));
    assert_eq!(settlements.lock().expect("settlements").len(), 1);

    let narrative = narrative_prompt
        .lock()
        .expect("prompt")
        .clone()
        .expect("prompt");
    let engineering = engineering_prompt
        .lock()
        .expect("prompt")
        .clone()
        .expect("prompt");
    assert_eq!(narrative.1, engineering.1, "only the lens input differs");
    assert!(narrative.0.contains("- never contradict a settled scene"));
    assert!(engineering
        .0
        .contains("test, CI and performance signals on the head commit"));
    assert!(engineering
        .0
        .contains("- never promote an unverified observation to an ADR"));
}

#[tokio::test]
async fn an_engineering_adr_needs_a_verified_citation_before_the_hook() {
    let holder = deliberate_holder();
    let target = deliberate_target();
    let evidence = [Evidence {
        id: "ev_test",
        text: "the gate test passed on this commit",
    }];

    // The proposal rests on the resolvable local observation only; the prose
    // claiming CI verification is model text.
    let reasoner = DeliberateReasoner {
        verdict: adr_verdict(
            "verified: the gate test passed in CI on the head commit",
            &[],
            &["rec_gate"],
        ),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let policy = UnreachablePolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };
    let binding = DeliberateDouble::new();
    let settlements = binding.settlements();
    let workflows = SpiMindWorkflows::new(binding);

    let error = workflows
        .deliberate(
            &principal(),
            lens_request(&holder, &target, &evidence),
            Some(&EngineeringEvaluatorLens),
            &caps,
        )
        .await
        .expect_err("an unverified observation cannot settle as an ADR");

    assert!(
        matches!(error, SpiError::PolicyRejected(_)),
        "expected a lens refusal, got {error:?}"
    );
    assert!(
        settlements.lock().expect("settlements").is_empty(),
        "the carrier is unchanged"
    );

    // The same proposal resting on the fixture-verified observation settles.
    let reasoner = DeliberateReasoner {
        verdict: adr_verdict(
            "the gate test passed on the supplied evidence",
            &["ev_test"],
            &["rec_gate"],
        ),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };
    let binding = DeliberateDouble::new();
    let settlements = binding.settlements();
    let workflows = SpiMindWorkflows::new(binding);

    let outcome = workflows
        .deliberate(
            &principal(),
            lens_request(&holder, &target, &evidence),
            Some(&EngineeringEvaluatorLens),
            &caps,
        )
        .await
        .expect("a verified citation settles");

    assert!(matches!(outcome, DeliberateOutcome::Settled { .. }));
    assert_eq!(settlements.lock().expect("settlements").len(), 1);
}

#[tokio::test]
async fn only_the_lens_differs_across_the_narrative_and_engineering_absorb() {
    let narrative_prompt = Arc::new(Mutex::new(None));
    let narrative_reasoner = LensAwareReasoner(Arc::clone(&narrative_prompt));
    let narrative_policy = AutoPolicy;
    let narrative_caps = MindCapabilities {
        reasoner: Some(&narrative_reasoner),
        policy_hook: Some(&narrative_policy),
        ..MindCapabilities::default()
    };
    let (narrative_workflows, narrative_recorded, _) = absorb(pending_candidate());
    let outcomes = narrative_workflows
        .absorb(
            &principal(),
            &capture_request(),
            Some(&NarrativeLens),
            &narrative_caps,
        )
        .await
        .expect("the narrative lens settles the promotion");
    assert!(matches!(
        outcomes.as_slice(),
        [AbsorbCandidateOutcome::Settled { .. }]
    ));
    assert_eq!(narrative_recorded.reviews.lock().expect("reviews").len(), 1);

    let engineering_prompt = Arc::new(Mutex::new(None));
    let engineering_reasoner = LensAwareReasoner(Arc::clone(&engineering_prompt));
    let engineering_policy = AutoPolicy;
    let engineering_caps = MindCapabilities {
        reasoner: Some(&engineering_reasoner),
        policy_hook: Some(&engineering_policy),
        ..MindCapabilities::default()
    };
    let (engineering_workflows, engineering_recorded, _) = absorb(pending_candidate());
    let outcomes = engineering_workflows
        .absorb(
            &principal(),
            &capture_request(),
            Some(&EngineeringEvaluatorLens),
            &engineering_caps,
        )
        .await
        .expect("the engineering lens binds ADR promotion only");
    assert!(matches!(
        outcomes.as_slice(),
        [AbsorbCandidateOutcome::Settled { .. }]
    ));
    assert_eq!(
        engineering_recorded.reviews.lock().expect("reviews").len(),
        1
    );

    let (narrative, narrative_payload) = narrative_prompt
        .lock()
        .expect("prompt")
        .clone()
        .expect("prompt");
    let (engineering, engineering_payload) = engineering_prompt
        .lock()
        .expect("prompt")
        .clone()
        .expect("prompt");
    assert_eq!(
        narrative_payload, engineering_payload,
        "only the lens input differs"
    );
    assert!(narrative.starts_with("Decide what one captured memory candidate"));
    assert!(engineering.starts_with("Decide what one captured memory candidate"));
    assert!(narrative.contains("a narrative continuity editor"));
    assert!(engineering.contains("an engineering evaluator"));
    assert!(engineering.contains("- never promote an unverified observation to an ADR"));
}
