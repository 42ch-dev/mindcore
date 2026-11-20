//! Workflow behaviour over a pinned binding double: the recall page order,
//! hybrid rerank, pack budget and snapshot revalidation, with no storage.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};

use mindcore_spi::{
    CognitiveTaskKind, Embedder, EmbeddingIdentity, MindCapabilities, NoEmbeddings, Reasoner,
    RecallSnapshot, SpiError, SpiMindWorkflows, StructuredReasonRequest, WorkflowBinding,
};
use spoke_operations::SpokeRejectCode;
use spoke_schemas::{KnowledgeEntry, MemoryRecallRequest, Principal, RetainedRecord};

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
