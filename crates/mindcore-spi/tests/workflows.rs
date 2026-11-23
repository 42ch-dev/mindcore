//! Workflow behaviour over a pinned binding double: the recall page order,
//! hybrid rerank, pack budget and snapshot revalidation, with no storage.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};

use mindcore_spi::{
    page_fact, snapshot_fact, AbsorbCandidateOutcome, AbsorbSnapshot, CandidateLifecycle,
    CaptureOutcome, CapturedCandidate, CitedRecord, CognitiveTaskKind, DeliberateOutcome,
    DeliberateRequest, DeliberateSnapshot, DomainLens, Embedder, EmbeddingIdentity, Evidence,
    LensOutput, MindCapabilities, NoEmbeddings, PageFact, PageVector, PageVectors, PolicyDecision,
    PolicyHook, ProposalDraft, Reasoner, RecallSnapshot, Recommendation, RecordFill, SettledRecord,
    SettledRevision, SettlementDraft, SnapshotFact, SpiError, SpiMindWorkflows,
    StructuredReasonRequest, WorkflowBinding,
};
use spoke_operations::{validate_judgment_typed, SpokeRejectCode};
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

/// A retained record whose body the tombstone scrubbed: metadata only, no text left
/// to embed.
fn tombstoned(record_id: &str, revision: u64) -> RetainedRecord {
    serde_json::from_value(json!({
        "schema_version": 1,
        "record_id": record_id,
        "space_id": "spc_1",
        "subject_id": "subj_1",
        "kind": "fact",
        "content": { "text": "" },
        "scope": { "visibility": "shared" },
        "provenance": {
            "source_context_ids": ["ctx_a"],
            "anchors": [],
            "captured_by": "prn_1",
            "captured_at": "2026-11-20T09:00:00Z"
        },
        "tombstone": {
            "deleted_at": "2026-11-20T10:00:00Z",
            "deleted_by": "prn_1",
            "reason": ""
        },
        "created_at": "2026-11-20T09:00:00Z",
        "updated_at": "2026-11-20T10:00:00Z",
        "revision": revision,
        "extensions": {}
    }))
    .expect("tombstoned record")
}

/// Repeats its last snapshot once the list runs out, so a stable page reads
/// back identically, and serves the vectors a warm table already holds under
/// `(identity, record, revision)`.
#[derive(Clone)]
struct StubBinding {
    inner: Arc<StubInner>,
}

struct StubInner {
    snapshots: Mutex<Vec<RecallSnapshot>>,
    calls: AtomicUsize,
    stored: Mutex<HashMap<(String, String, u64), Vec<f32>>>,
    vector_reads: AtomicUsize,
    settle_calls: AtomicUsize,
    settled_fills: Mutex<Vec<RecordFill>>,
}

impl StubBinding {
    fn new(snapshots: Vec<RecallSnapshot>) -> Self {
        Self {
            inner: Arc::new(StubInner {
                snapshots: Mutex::new(snapshots),
                calls: AtomicUsize::new(0),
                stored: Mutex::new(HashMap::new()),
                vector_reads: AtomicUsize::new(0),
                settle_calls: AtomicUsize::new(0),
                settled_fills: Mutex::new(Vec::new()),
            }),
        }
    }

    /// The page the read of `index` serves.
    fn page(&self, index: usize) -> RecallSnapshot {
        let snapshots = self.inner.snapshots.lock().expect("snapshots");
        snapshots[index.min(snapshots.len() - 1)].clone()
    }

    /// Seed the vectors the table already holds under `identity`.
    fn seeded(self, identity: &EmbeddingIdentity, vectors: &[(&str, u64, Vec<f32>)]) -> Self {
        let mut stored = self.inner.stored.lock().expect("stored vectors");
        for (record_id, revision, vector) in vectors {
            stored.insert(
                (partition(identity), (*record_id).to_string(), *revision),
                vector.clone(),
            );
        }
        drop(stored);
        self
    }

    /// How many times the vector snapshot was read.
    fn vector_reads(&self) -> usize {
        self.inner.vector_reads.load(Ordering::SeqCst)
    }

    /// How many release boundaries the recall crossed.
    fn settle_calls(&self) -> usize {
        self.inner.settle_calls.load(Ordering::SeqCst)
    }

    /// The fills a successful release boundary carried, in call order.
    fn settled_fills(&self) -> Vec<RecordFill> {
        self.inner
            .settled_fills
            .lock()
            .expect("settled fills")
            .clone()
    }
}

/// The cache partition of one identity, as the double keys it.
fn partition(identity: &EmbeddingIdentity) -> String {
    format!(
        "{}|{}|{}|{}",
        identity.provider_id, identity.model_id, identity.model_version, identity.dim
    )
}

#[async_trait]
impl WorkflowBinding for StubBinding {
    async fn recall_snapshot(
        &self,
        _principal: &Principal,
        _request: &MemoryRecallRequest,
    ) -> Result<RecallSnapshot, SpiError> {
        let call = self.inner.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.page(call))
    }

    async fn recall_vectors(
        &self,
        _principal: &Principal,
        _request: &MemoryRecallRequest,
        identity: &EmbeddingIdentity,
        expected: &[PageFact],
    ) -> Result<PageVectors, SpiError> {
        self.inner.vector_reads.fetch_add(1, Ordering::SeqCst);
        let page = self.page(self.inner.calls.load(Ordering::SeqCst).saturating_sub(1));
        if page_fact(&page.records) != expected {
            return Err(revision_conflict());
        }
        let partition = partition(identity);
        let stored = self.inner.stored.lock().expect("stored vectors");
        let rows: PageVectors = page
            .records
            .into_iter()
            .map(|record| {
                let vector = match &record {
                    RetainedRecord::LiveRecord(live) => stored
                        .get(&(
                            partition.clone(),
                            live.record_id.as_str().to_string(),
                            live.revision.get(),
                        ))
                        .cloned(),
                    RetainedRecord::TombstonedRecord(_) => None,
                };
                PageVector { record, vector }
            })
            .collect();
        Ok(rows)
    }

    async fn capture(
        &self,
        _principal: &Principal,
        _request: &MemoryCaptureRequest,
    ) -> Result<CaptureOutcome, SpiError> {
        unserved()
    }

    async fn record_absorb_pending(
        &self,
        _principal: &Principal,
        _request: &MemoryCaptureRequest,
        _pending_candidate_ids: &[&str],
    ) -> Result<(), SpiError> {
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
    ) -> Result<Vec<SettledRevision>, SpiError> {
        unserved()
    }

    async fn update_candidate(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _update: &MemoryRecordsUpdateRequest,
        _dismiss: &MemoryCandidatesDismissRequest,
        _cited: &[CitedRecord],
    ) -> Result<Vec<SettledRevision>, SpiError> {
        unserved()
    }

    async fn settled_record(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _record_id: &str,
    ) -> Result<Option<SettledRecord>, SpiError> {
        unserved()
    }

    async fn settle_recall(
        &self,
        _principal: &Principal,
        _request: &MemoryRecallRequest,
        _identity: Option<&EmbeddingIdentity>,
        expected: &SnapshotFact,
        fills: &[RecordFill],
    ) -> Result<(), SpiError> {
        self.inner.settle_calls.fetch_add(1, Ordering::SeqCst);
        let call = self.inner.calls.fetch_add(1, Ordering::SeqCst);
        let page = self.page(call);
        if &snapshot_fact(&page.records, &page.knowledge) != expected {
            return Err(revision_conflict());
        }
        self.inner
            .settled_fills
            .lock()
            .expect("settled fills")
            .extend(fills.iter().cloned());
        Ok(())
    }

    async fn fill_record_vectors(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _identity: &EmbeddingIdentity,
        _fills: &[RecordFill],
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

    /// A page read that refuses never reaches the vector cache; a probe here
    /// would mean the refusal came after the lookup, so it fails loudly.
    async fn recall_vectors(
        &self,
        _principal: &Principal,
        _request: &MemoryRecallRequest,
        _identity: &EmbeddingIdentity,
        _expected: &[PageFact],
    ) -> Result<PageVectors, SpiError> {
        panic!("a refused page must never probe the vector cache");
    }

    async fn capture(
        &self,
        _principal: &Principal,
        _request: &MemoryCaptureRequest,
    ) -> Result<CaptureOutcome, SpiError> {
        unserved()
    }

    async fn record_absorb_pending(
        &self,
        _principal: &Principal,
        _request: &MemoryCaptureRequest,
        _pending_candidate_ids: &[&str],
    ) -> Result<(), SpiError> {
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
    ) -> Result<Vec<SettledRevision>, SpiError> {
        unserved()
    }

    async fn update_candidate(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _update: &MemoryRecordsUpdateRequest,
        _dismiss: &MemoryCandidatesDismissRequest,
        _cited: &[CitedRecord],
    ) -> Result<Vec<SettledRevision>, SpiError> {
        unserved()
    }

    async fn settled_record(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _record_id: &str,
    ) -> Result<Option<SettledRecord>, SpiError> {
        unserved()
    }

    async fn settle_recall(
        &self,
        _principal: &Principal,
        _request: &MemoryRecallRequest,
        _identity: Option<&EmbeddingIdentity>,
        _expected: &SnapshotFact,
        _fills: &[RecordFill],
    ) -> Result<(), SpiError> {
        unreachable!("a refused page never reaches the release boundary")
    }

    async fn fill_record_vectors(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _identity: &EmbeddingIdentity,
        _fills: &[RecordFill],
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
    /// Every text this embedder was asked for, in call order.
    texts: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Embedder for PlantedEmbedder {
    fn identity(&self) -> Option<&EmbeddingIdentity> {
        Some(&self.identity)
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, SpiError> {
        self.texts
            .lock()
            .expect("embedded texts")
            .extend(texts.iter().cloned());
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

impl PlantedEmbedder {
    fn embedded(&self) -> Vec<String> {
        self.texts.lock().expect("embedded texts").clone()
    }
}

/// The planted identity under `model_version`, so a test can name a second
/// serving identity over the same page.
fn planted(model_version: &str) -> EmbeddingIdentity {
    EmbeddingIdentity {
        provider_id: "planted".to_string(),
        model_id: "axes".to_string(),
        model_version: model_version.to_string(),
        dim: 2,
    }
}

fn planted_embedder() -> PlantedEmbedder {
    PlantedEmbedder {
        identity: planted("1"),
        texts: Arc::new(Mutex::new(Vec::new())),
    }
}

/// Names its identity but never serves a vector.
struct UnavailableEmbedder(EmbeddingIdentity);

#[async_trait]
impl Embedder for UnavailableEmbedder {
    fn identity(&self) -> Option<&EmbeddingIdentity> {
        Some(&self.0)
    }

    async fn embed_batch(&self, _texts: &[String]) -> Result<Vec<Vec<f32>>, SpiError> {
        Err(SpiError::EmbeddingsUnavailable)
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

/// A tombstone can neither fill nor be scored by a vector: the page's miss set
/// holds live records only, so a recall over a tombstoned record embeds the live
/// page and the query, never the scrubbed body, and writes no fill for it.
#[tokio::test]
async fn a_tombstone_is_never_embedded_or_filled() {
    let page = snapshot(
        vec![
            record("rec_live", "the north gate is closed", 1),
            tombstoned("rec_gone", 2),
        ],
        Vec::new(),
    );
    let binding = StubBinding::new(vec![page]);
    let workflows = SpiMindWorkflows::new(binding.clone());
    let embedder = planted_embedder();
    let caps = embedding_caps(&embedder);

    let pack = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &caps,
        )
        .await
        .expect("pack");

    let embedded = embedder.embedded();
    assert_eq!(
        embedded.len(),
        2,
        "one query and one live text are embedded, got {embedded:?}"
    );
    assert!(
        embedded
            .iter()
            .any(|text| text == "the north gate is closed"),
        "the live record is embedded: {embedded:?}"
    );
    let fills = binding.settled_fills();
    let filled: Vec<&str> = fills
        .iter()
        .map(|fill| fill.record.record_id.as_str())
        .collect();
    assert_eq!(filled, ["rec_live"], "a tombstone is never filled");
    assert_eq!(ids(&pack), ["rec_live", "rec_gone"]);
}

/// The fixture page in adapter order: the market, the closed gate and the
/// sleeping guards.
fn vector_page(guards_revision: u64) -> Vec<RetainedRecord> {
    vec![
        record("rec_b", "the market opens at dawn", 1),
        record("rec_a", "the north gate is closed", 1),
        record("rec_c", "north gate guards sleep", guards_revision),
    ]
}

fn embedding_caps(embedder: &dyn Embedder) -> MindCapabilities<'_> {
    MindCapabilities {
        embedder: Some(embedder),
        ..MindCapabilities::default()
    }
}

/// The vectors a warm table already holds for the fixture page at revision 1.
fn warm_vectors() -> [(&'static str, u64, Vec<f32>); 3] {
    [
        ("rec_b", 1, vec![0.0, 1.0]),
        ("rec_a", 1, vec![1.0, 0.0]),
        ("rec_c", 1, vec![0.5, 0.5]),
    ]
}

#[tokio::test]
async fn a_warm_identical_recall_needs_no_provider_call() {
    let binding = StubBinding::new(vec![snapshot(vector_page(1), Vec::new())])
        .seeded(&planted("1"), &warm_vectors());
    let workflows = SpiMindWorkflows::new(binding.clone());
    let embedder = planted_embedder();
    let caps = embedding_caps(&embedder);

    let first = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &caps,
        )
        .await
        .expect("pack");
    assert_eq!(
        embedder.embedded(),
        ["north gate"],
        "a warm page embeds the query alone"
    );
    assert_eq!(ids(&first), ["rec_a", "rec_c", "rec_b"]);
    assert_eq!(binding.vector_reads(), 1);

    let second = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &caps,
        )
        .await
        .expect("pack");

    assert_eq!(
        embedder.embedded().len(),
        1,
        "a warm identical recall makes no provider call"
    );
    assert_eq!(ids(&second), ids(&first));
}

#[tokio::test]
async fn a_new_query_with_warm_records_embeds_only_the_query() {
    let binding = StubBinding::new(vec![snapshot(vector_page(1), Vec::new())])
        .seeded(&planted("1"), &warm_vectors());
    let workflows = SpiMindWorkflows::new(binding.clone());
    let embedder = planted_embedder();
    let caps = embedding_caps(&embedder);
    workflows
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &caps,
        )
        .await
        .expect("warm recall");

    let pack = workflows
        .recall_context(
            &principal(),
            &request(Some("market dawn")),
            budget(4096),
            &caps,
        )
        .await
        .expect("pack");

    assert_eq!(
        embedder.embedded()[1..],
        ["market dawn"],
        "a new query embeds its own text only"
    );
    assert_eq!(ids(&pack), ["rec_b", "rec_c", "rec_a"]);
}

#[tokio::test]
async fn a_stale_revision_embeds_only_that_record() {
    let binding = StubBinding::new(vec![snapshot(vector_page(2), Vec::new())])
        .seeded(&planted("1"), &warm_vectors());
    let workflows = SpiMindWorkflows::new(binding.clone());
    let embedder = planted_embedder();

    workflows
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &embedding_caps(&embedder),
        )
        .await
        .expect("pack");

    assert_eq!(
        embedder.embedded(),
        ["north gate", "north gate guards sleep"],
        "only the moved revision is embedded"
    );
}

#[tokio::test]
async fn a_new_identity_embeds_every_record_again() {
    let binding = StubBinding::new(vec![snapshot(vector_page(1), Vec::new())])
        .seeded(&planted("1"), &warm_vectors());
    let workflows = SpiMindWorkflows::new(binding.clone());
    let other = PlantedEmbedder {
        identity: planted("2"),
        texts: Arc::new(Mutex::new(Vec::new())),
    };
    workflows
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &embedding_caps(&other),
        )
        .await
        .expect("pack");

    assert_eq!(
        other.embedded().len(),
        4,
        "another identity hits no stored vector"
    );
    assert_eq!(
        binding.settle_calls(),
        1,
        "one release boundary owns the whole miss batch"
    );
    assert_eq!(
        binding.settled_fills().len(),
        3,
        "every miss reaches that one boundary together"
    );
}

#[tokio::test]
async fn an_embedder_free_or_unavailable_recall_probes_no_vector_cache() {
    let binding = StubBinding::new(vec![snapshot(vector_page(1), Vec::new())])
        .seeded(&planted("1"), &warm_vectors());

    let workflows = SpiMindWorkflows::new(binding.clone());
    let pack = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &MindCapabilities::default(),
        )
        .await
        .expect("pack");
    assert_eq!(ids(&pack), ["rec_b", "rec_a", "rec_c"], "adapter order");
    assert_eq!(binding.vector_reads(), 0, "no embedder probes nothing");

    let workflows = SpiMindWorkflows::new(binding.clone());
    let cold = UnavailableEmbedder(planted("1"));
    let pack = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &embedding_caps(&cold),
        )
        .await
        .expect("pack");
    assert_eq!(ids(&pack), ["rec_b", "rec_a", "rec_c"], "adapter order");
    assert_eq!(
        binding.vector_reads(),
        0,
        "an unavailable provider probes nothing"
    );
}

#[tokio::test]
async fn a_miss_batch_that_goes_unavailable_keeps_the_warm_slot() {
    /// A freshly embedded query vector, then an unavailable record batch: a
    /// recall that installed its discarded query is visible in a later recall.
    struct DistinctQueryEmbedder(EmbeddingIdentity);

    #[async_trait]
    impl Embedder for DistinctQueryEmbedder {
        fn identity(&self) -> Option<&EmbeddingIdentity> {
            Some(&self.0)
        }

        async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, SpiError> {
            if texts.len() > 1 {
                Err(SpiError::EmbeddingsUnavailable)
            } else {
                Ok(texts.iter().map(|_| vec![0.0, 1.0]).collect())
            }
        }
    }

    let binding = StubBinding::new(vec![snapshot(vector_page(1), Vec::new())]);
    let workflows = SpiMindWorkflows::new(binding.clone());
    let warm = planted_embedder();
    let warm_pack = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &embedding_caps(&warm),
        )
        .await
        .expect("warm pack");
    assert_eq!(ids(&warm_pack), ["rec_a", "rec_c", "rec_b"]);

    let cold = DistinctQueryEmbedder(planted("1"));
    let pack = workflows
        .recall_context(
            &principal(),
            &request(Some("market dawn")),
            budget(4096),
            &embedding_caps(&cold),
        )
        .await
        .expect("lexical pack");
    assert_eq!(
        ids(&pack),
        ["rec_b", "rec_a", "rec_c"],
        "hybrid work is discarded"
    );
    assert_eq!(binding.vector_reads(), 2, "a read-only lookup may have run");

    let after = planted_embedder();
    let pack = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &embedding_caps(&after),
        )
        .await
        .expect("pack");
    assert_eq!(
        after.embedded().first().map(String::as_str),
        Some("the market opens at dawn"),
        "the discarded query vector never replaced the warm slot"
    );
    assert_eq!(ids(&pack), ["rec_a", "rec_c", "rec_b"]);

    let market = planted_embedder();
    workflows
        .recall_context(
            &principal(),
            &request(Some("market dawn")),
            budget(4096),
            &embedding_caps(&market),
        )
        .await
        .expect("pack");
    assert_eq!(
        market.embedded().first().map(String::as_str),
        Some("market dawn"),
        "a late unavailable recall installed no query slot of its own"
    );
}

#[tokio::test]
async fn a_refused_recall_leaves_no_cached_query_vector() {
    let binding = StubBinding::new(vec![
        snapshot(vector_page(1), Vec::new()),
        snapshot(vector_page(2), Vec::new()),
    ]);
    let workflows = SpiMindWorkflows::new(binding.clone());
    let embedder = planted_embedder();
    let caps = embedding_caps(&embedder);

    let error = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &caps,
        )
        .await
        .expect_err("a page that moved after the miss batch must not release a pack");
    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::RevisionConflict);
    assert_eq!(
        embedder.embedded(),
        [
            "north gate",
            "the market opens at dawn",
            "the north gate is closed",
            "north gate guards sleep",
        ],
        "the refused recall embedded the query and the miss batch"
    );

    workflows
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &caps,
        )
        .await
        .expect("the retried recall");

    assert_eq!(
        embedder.embedded()[4],
        "north gate",
        "the refused recall cached no query vector"
    );
}

#[tokio::test]
async fn no_provider_work_runs_before_a_withheld_read_is_refused() {
    let embedder = planted_embedder();
    let workflows = SpiMindWorkflows::new(RefusingBinding(SpiError::Settlement(
        spoke_operations::SpokeReject {
            code: SpokeRejectCode::NotAuthorized,
            message: "subject is withheld".to_string(),
            details: None,
        },
    )));

    let error = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate")),
            budget(4096),
            &embedding_caps(&embedder),
        )
        .await
        .expect_err("a withheld read must not release a pack");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
    assert!(
        embedder.embedded().is_empty(),
        "no provider work runs for a refused page"
    );
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

/// The same candidate under another id, the way a receipt holding several
/// candidates names them.
fn named(candidate: MemoryCandidate, candidate_id: &str) -> MemoryCandidate {
    let mut candidate = serde_json::to_value(candidate).expect("candidate");
    candidate["candidate_id"] = json!(candidate_id);
    serde_json::from_value(candidate).expect("candidate")
}

fn candidate_id_of(candidate: &MemoryCandidate) -> &str {
    match candidate {
        MemoryCandidate::PendingCandidate(pending) => pending.candidate_id.as_str(),
        MemoryCandidate::ConsumedCandidate(consumed) => consumed.candidate_id.as_str(),
    }
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
    /// Every vector the warm asked this binding to fill, with its revision.
    fills: Arc<Mutex<Vec<(String, u64)>>>,
    /// Pending candidate sets the coordinator recorded as its decision.
    pending: Arc<Mutex<Vec<Vec<String>>>>,
    /// The candidate rows the capture receipt holds, in receipt order; a test
    /// moves one to consumed between runs, the way another writer would.
    candidates: Arc<Mutex<Vec<MemoryCandidate>>>,
    /// The next marker write refuses: the decision its run made stays unrecorded.
    fail_pending_write: Arc<AtomicBool>,
}

/// One captured candidate over a fixed page, with every submitted settlement
/// recorded for the assertion.
struct AbsorbDouble {
    page: Vec<RetainedRecord>,
    recorded: Recorded,
    /// The record id this binding's update leg already settled, if any.
    settled_update: Option<String>,
    /// Captures served so far: the first commits the receipt, later ones replay it.
    captures: AtomicUsize,
    /// The live projection the post-settlement warm reads, when a test drives one.
    live: Option<SettledRecord>,
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

    async fn recall_vectors(
        &self,
        _principal: &Principal,
        _request: &MemoryRecallRequest,
        _identity: &EmbeddingIdentity,
        _expected: &[PageFact],
    ) -> Result<PageVectors, SpiError> {
        unserved()
    }

    async fn capture(
        &self,
        _principal: &Principal,
        _request: &MemoryCaptureRequest,
    ) -> Result<CaptureOutcome, SpiError> {
        let call = self.captures.fetch_add(1, Ordering::SeqCst);
        let decision = self
            .recorded
            .pending
            .lock()
            .expect("pending writes")
            .clone();
        let candidates = self.recorded.candidates.lock().expect("candidates").clone();
        let candidates = candidates
            .iter()
            .map(|candidate| {
                let candidate_id = candidate_id_of(candidate).to_string();
                let lifecycle = if decision
                    .iter()
                    .flatten()
                    .any(|named| named == &candidate_id)
                {
                    CandidateLifecycle::PendingDecision
                } else {
                    match candidate {
                        MemoryCandidate::PendingCandidate(_) => CandidateLifecycle::Pending,
                        MemoryCandidate::ConsumedCandidate(_) => CandidateLifecycle::Consumed,
                    }
                };
                CapturedCandidate {
                    candidate_id,
                    lifecycle,
                }
            })
            .collect();
        Ok(CaptureOutcome {
            candidates,
            replayed: call > 0,
        })
    }

    async fn record_absorb_pending(
        &self,
        _principal: &Principal,
        _request: &MemoryCaptureRequest,
        pending_candidate_ids: &[&str],
    ) -> Result<(), SpiError> {
        if self
            .recorded
            .fail_pending_write
            .swap(false, Ordering::SeqCst)
        {
            return Err(marker_write_failed());
        }
        self.recorded.pending.lock().expect("pending writes").push(
            pending_candidate_ids
                .iter()
                .map(|id| id.to_string())
                .collect(),
        );
        Ok(())
    }

    async fn absorb_snapshot(
        &self,
        _principal: &Principal,
        _space_id: &str,
        candidate_id: &str,
        _max_items: u64,
    ) -> Result<AbsorbSnapshot, SpiError> {
        self.recorded.snapshots.fetch_add(1, Ordering::SeqCst);
        let candidate = self
            .recorded
            .candidates
            .lock()
            .expect("candidates")
            .iter()
            .find(|candidate| candidate_id_of(candidate) == candidate_id)
            .cloned()
            .expect("a snapshot names a candidate the receipt holds");
        Ok(AbsorbSnapshot {
            candidate,
            records: self.page.clone(),
        })
    }

    async fn review_candidate(
        &self,
        _principal: &Principal,
        request: &MemoryCandidatesReviewRequest,
        cited: &[CitedRecord],
    ) -> Result<Vec<SettledRevision>, SpiError> {
        let mut object = serde_json::to_value(request).expect("review request");
        object["cited"] = cited_json(cited);
        self.recorded.reviews.lock().expect("reviews").push(object);
        Ok(vec![SettledRevision {
            record_id: "rec_new".to_string(),
            revision: 1,
        }])
    }

    async fn update_candidate(
        &self,
        _principal: &Principal,
        _space_id: &str,
        update: &MemoryRecordsUpdateRequest,
        dismiss: &MemoryCandidatesDismissRequest,
        cited: &[CitedRecord],
    ) -> Result<Vec<SettledRevision>, SpiError> {
        let rendered = serde_json::to_value(update).expect("update request");
        self.recorded.updates.lock().expect("updates").push(json!({
            "update": rendered,
            "dismiss": serde_json::to_value(dismiss).expect("dismiss request"),
            "cited": cited_json(cited),
        }));
        Ok(vec![SettledRevision {
            record_id: rendered["record_id"]
                .as_str()
                .expect("update record id")
                .to_string(),
            revision: rendered["expected_revision"]
                .as_u64()
                .expect("update expected revision")
                + 1,
        }])
    }

    async fn settled_record(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _record_id: &str,
    ) -> Result<Option<SettledRecord>, SpiError> {
        Ok(self.live.clone())
    }

    async fn settle_recall(
        &self,
        _principal: &Principal,
        _request: &MemoryRecallRequest,
        _identity: Option<&EmbeddingIdentity>,
        _expected: &SnapshotFact,
        _fills: &[RecordFill],
    ) -> Result<(), SpiError> {
        unserved()
    }

    async fn fill_record_vectors(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _identity: &EmbeddingIdentity,
        fills: &[RecordFill],
    ) -> Result<(), SpiError> {
        self.recorded.fills.lock().expect("fills").extend(
            fills
                .iter()
                .map(|fill| (fill.record.record_id.clone(), fill.record.revision)),
        );
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
    *recorded.candidates.lock().expect("candidates") = vec![candidate];
    let double = AbsorbDouble {
        page: page(),
        recorded: recorded.clone(),
        settled_update: settled_update.map(str::to_string),
        captures: AtomicUsize::new(captures),
        live: None,
    };
    let seen = Arc::new(Mutex::new(Vec::new()));
    (SpiMindWorkflows::new(double), recorded, seen)
}

/// An absorb double whose capture receipt holds two pending candidates.
fn absorb_two_pending() -> (SpiMindWorkflows, Recorded) {
    let recorded = Recorded::default();
    *recorded.candidates.lock().expect("candidates") =
        vec![pending_candidate(), named(pending_candidate(), "cnd_2")];
    let double = AbsorbDouble {
        page: page(),
        recorded: recorded.clone(),
        settled_update: None,
        captures: AtomicUsize::new(0),
        live: None,
    };
    (SpiMindWorkflows::new(double), recorded)
}

/// An absorb double whose post-settlement warm reads `live`.
fn absorb_with_live(live: SettledRecord) -> (SpiMindWorkflows, Recorded) {
    let recorded = Recorded::default();
    *recorded.candidates.lock().expect("candidates") = vec![pending_candidate()];
    let double = AbsorbDouble {
        page: page(),
        recorded: recorded.clone(),
        settled_update: None,
        captures: AtomicUsize::new(0),
        live: Some(live),
    };
    (SpiMindWorkflows::new(double), recorded)
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

/// The warm embeds the record whose live read still serves the revision the
/// settlement committed, and fills exactly that revision.
#[tokio::test]
async fn the_warm_embeds_the_settled_revision() {
    let (workflows, recorded) = absorb_with_live(SettledRecord {
        record_id: "rec_new".to_string(),
        revision: 1,
        text: "the north gate is closed at dusk".to_string(),
    });
    let embedder = planted_embedder();
    let policy = RecordingPolicy {
        decision: PolicyDecision::AutoSettle,
        seen: Arc::new(Mutex::new(Vec::new())),
    };
    let caps = MindCapabilities {
        reasoner: Some(&FixedVerdict(
            json!({ "action": "promote", "evidence_ids": ["rec_gate"] }),
        )),
        policy_hook: Some(&policy),
        embedder: Some(&embedder),
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
        embedder.embedded(),
        vec!["the north gate is closed at dusk".to_string()]
    );
    assert_eq!(
        *recorded.fills.lock().expect("fills"),
        vec![("rec_new".to_string(), 1)]
    );
}

/// A live read that already serves a later revision is not the settled work:
/// its text belongs to a mutation the settlement never made.
#[tokio::test]
async fn a_record_that_moved_before_the_live_read_warms_nothing() {
    let (workflows, recorded) = absorb_with_live(SettledRecord {
        record_id: "rec_new".to_string(),
        revision: 2,
        text: "the north gate is closed for the night".to_string(),
    });
    let embedder = planted_embedder();
    let policy = RecordingPolicy {
        decision: PolicyDecision::AutoSettle,
        seen: Arc::new(Mutex::new(Vec::new())),
    };
    let caps = MindCapabilities {
        reasoner: Some(&FixedVerdict(
            json!({ "action": "promote", "evidence_ids": ["rec_gate"] }),
        )),
        policy_hook: Some(&policy),
        embedder: Some(&embedder),
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
        }],
        "the settled Memory outcome is unchanged"
    );
    assert!(
        embedder.embedded().is_empty(),
        "obsolete settled work makes no provider call"
    );
    assert!(
        recorded.fills.lock().expect("fills").is_empty(),
        "and fills no row"
    );
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
    assert!(
        recorded.pending.lock().expect("pending writes").is_empty(),
        "a failed decision records no Pending marker"
    );
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
    assert_eq!(
        recorded.snapshots.load(Ordering::SeqCst),
        0,
        "a pending candidate is reported without a candidate read"
    );
    assert!(recorded.reviews.lock().expect("reviews").is_empty());
    assert_eq!(
        recorded.pending.lock().expect("pending writes").as_slice(),
        [vec!["cnd_1".to_string()]],
        "the fresh Pending decision is recorded once and its replay writes no second marker"
    );
}

#[tokio::test]
async fn a_replay_that_finds_no_marker_writes_none() {
    let (workflows, recorded) = absorb_two_pending();
    let caps = MindCapabilities::default();
    recorded.fail_pending_write.store(true, Ordering::SeqCst);

    let failed = workflows
        .absorb(&principal(), &capture_request(), None, &caps)
        .await;
    assert!(
        matches!(failed, Err(SpiError::Settlement(_))),
        "a failed marker write fails the run"
    );
    assert!(recorded.pending.lock().expect("pending writes").is_empty());

    let replayed = workflows
        .absorb(&principal(), &capture_request(), None, &caps)
        .await
        .expect("replayed absorb");
    assert_eq!(
        replayed,
        vec![
            AbsorbCandidateOutcome::Pending {
                candidate_id: "cnd_1".to_string(),
            },
            AbsorbCandidateOutcome::Pending {
                candidate_id: "cnd_2".to_string(),
            },
        ]
    );
    assert!(
        recorded.pending.lock().expect("pending writes").is_empty(),
        "a replay writes no marker"
    );

    let again = workflows
        .absorb(&principal(), &capture_request(), None, &caps)
        .await
        .expect("replayed absorb");
    assert_eq!(again, replayed);
    assert!(
        recorded.pending.lock().expect("pending writes").is_empty(),
        "no replay writes a decision marker"
    );
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
    assert_eq!(
        recorded.pending.lock().expect("pending writes").as_slice(),
        [vec!["cnd_1".to_string()]],
        "a policy hold records its Pending decision"
    );
}

#[tokio::test]
async fn a_replay_never_settles_what_a_policy_held() {
    let (workflows, recorded, seen) = absorb(pending_candidate());
    let asked = Arc::new(AtomicUsize::new(0));
    let reasoner = CountingReasoner {
        asked: Arc::clone(&asked),
    };

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
        Self::serving(3)
    }

    /// One double whose single served record carries `revision`, so a test can
    /// hand the projection a stored revision the judgment wire cannot carry.
    fn serving(revision: u64) -> Self {
        Self {
            snapshot: DeliberateSnapshot {
                records: vec![record("rec_gate", "the north gate is closed", revision)],
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

    async fn recall_vectors(
        &self,
        _principal: &Principal,
        _request: &MemoryRecallRequest,
        _identity: &EmbeddingIdentity,
        _expected: &[PageFact],
    ) -> Result<PageVectors, SpiError> {
        unserved()
    }

    async fn capture(
        &self,
        _principal: &Principal,
        _request: &MemoryCaptureRequest,
    ) -> Result<CaptureOutcome, SpiError> {
        unserved()
    }

    async fn record_absorb_pending(
        &self,
        _principal: &Principal,
        _request: &MemoryCaptureRequest,
        _pending_candidate_ids: &[&str],
    ) -> Result<(), SpiError> {
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
    ) -> Result<Vec<SettledRevision>, SpiError> {
        unserved()
    }

    async fn update_candidate(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _update: &MemoryRecordsUpdateRequest,
        _dismiss: &MemoryCandidatesDismissRequest,
        _cited: &[CitedRecord],
    ) -> Result<Vec<SettledRevision>, SpiError> {
        unserved()
    }

    async fn settled_record(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _record_id: &str,
    ) -> Result<Option<SettledRecord>, SpiError> {
        unserved()
    }

    async fn settle_recall(
        &self,
        _principal: &Principal,
        _request: &MemoryRecallRequest,
        _identity: Option<&EmbeddingIdentity>,
        _expected: &SnapshotFact,
        _fills: &[RecordFill],
    ) -> Result<(), SpiError> {
        unserved()
    }

    async fn fill_record_vectors(
        &self,
        _principal: &Principal,
        _space_id: &str,
        _identity: &EmbeddingIdentity,
        _fills: &[RecordFill],
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

/// Holds every cognition draft for human review.
struct HoldingPolicy;

#[async_trait]
impl PolicyHook for HoldingPolicy {
    async fn evaluate_settlement(
        &self,
        _draft: SettlementDraft<'_>,
    ) -> Result<PolicyDecision, SpiError> {
        Ok(PolicyDecision::RequireHumanReview {
            reason: "a human must read this one".to_string(),
        })
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

/// The refusal a binding returns when the marker its run owed could not be
/// written, after the capture it names had already committed.
fn marker_write_failed() -> SpiError {
    SpiError::Settlement(spoke_operations::SpokeReject {
        code: SpokeRejectCode::InternalError,
        message: "the absorb decision could not be written".to_string(),
        details: None,
    })
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
        assessment_context_id: None,
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
        judgment,
        ..
    } = outcome
    else {
        panic!("expected a settlement, got {outcome:?}");
    };
    assert_eq!(mind_state_version, 2);
    assert!(
        validate_judgment_typed(&judgment).is_ok(),
        "a returned judgment always passes the released validator"
    );
    let wire = serde_json::to_value(&judgment).expect("judgment serializes");
    assert_eq!(wire["verdict"]["outcome"], json!("mindcore:settled"));
    assert_eq!(
        wire["target"],
        json!({
            "kind": "proposition",
            "id": "prop_gate_closed",
            "namespace": "mindcore",
        })
    );
    assert_eq!(
        wire["evidence"],
        json!([{
            "kind": "memory_record",
            "namespace": "mindcore",
            "id": "rec_gate",
            "revision": 3,
        }]),
        "the evidence pins the revisions the snapshot served"
    );
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
async fn an_empty_explicit_assessment_context_is_a_caller_input_error() {
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
        assessment_context_id: Some(""),
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: std::time::Duration::from_secs(30),
    };

    let error = workflows
        .deliberate(&principal(), request, None, &caps)
        .await
        .expect_err("an empty context id is not a context");
    assert!(
        matches!(error, SpiError::InvalidInput(_)),
        "the caller's own field must not be blamed on the reasoner, got {error:?}"
    );
    assert!(
        settlements.lock().expect("settlements").is_empty(),
        "a rejected input writes nothing"
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
            let mut verdict = promote_verdict();
            verdict["record"]["basis"]["evidence"][0]["namespace"] = json!("external");
            verdict
        },
        {
            let mut verdict = promote_verdict();
            verdict["record"]["basis"]["evidence"][0]["kind"] = json!("knowledge_entry");
            verdict
        },
        {
            let mut verdict = promote_verdict();
            verdict["record_ids"] = json!([]);
            verdict
        },
        {
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
            assessment_context_id: None,
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
        assessment_context_id: None,
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
        assessment_context_id: None,
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
        assessment_context_id: None,
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
        assessment_context_id: None,
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
        assessment_context_id: None,
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
        assessment_context_id: None,
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
    let holder = deliberate_holder();
    let target = deliberate_target();
    let evidence = [Evidence {
        id: "ev_test",
        text: "the gate test passed on this commit",
    }];
    let request = DeliberateRequest {
        space_id: "spc_1",
        subject_id: "subj_1",
        assessment_context_id: None,
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
        assessment_context_id: None,
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
        assessment_context_id: None,
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: std::time::Duration::from_secs(30),
    };

    let outcome = workflows
        .deliberate(&principal(), request, None, &caps)
        .await
        .expect("deliberate");

    let DeliberateOutcome::NoChange { judgment, .. } = outcome else {
        panic!("expected no change, got {outcome:?}");
    };
    assert!(
        validate_judgment_typed(&judgment).is_ok(),
        "an uncited judgment still validates"
    );
    let wire = serde_json::to_value(&judgment).expect("judgment serializes");
    assert_eq!(wire["verdict"]["outcome"], json!("mindcore:no_change"));
    assert_eq!(
        wire["evidence"],
        json!([]),
        "no record is cited, so the evidence is empty rather than invented"
    );
    assert!(settlements.lock().expect("settlements").is_empty());
}

#[tokio::test]
async fn deliberate_needs_review_carries_the_pending_judgment_and_writes_nothing() {
    let binding = DeliberateDouble::new();
    let settlements = binding.settlements();
    let workflows = SpiMindWorkflows::new(binding);
    let reasoner = DeliberateReasoner {
        verdict: promote_verdict(),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let policy = HoldingPolicy;
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
        assessment_context_id: None,
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: std::time::Duration::from_secs(30),
    };

    let outcome = workflows
        .deliberate(&principal(), request, None, &caps)
        .await
        .expect("deliberate");

    let DeliberateOutcome::NeedsReview { judgment, .. } = outcome else {
        panic!("expected a held draft, got {outcome:?}");
    };
    assert!(
        validate_judgment_typed(&judgment).is_ok(),
        "a held judgment always passes the released validator"
    );
    let wire = serde_json::to_value(&judgment).expect("judgment serializes");
    assert_eq!(wire["verdict"]["outcome"], json!("mindcore:needs_review"));
    assert_eq!(
        wire["evidence"],
        json!([{
            "kind": "memory_record",
            "namespace": "mindcore",
            "id": "rec_gate",
            "revision": 3,
        }])
    );
    assert!(
        settlements.lock().expect("settlements").is_empty(),
        "a held judgment commits nothing"
    );
}

/// A cited revision above the judgment wire's `2^53 - 1` integer ceiling is
/// one the page can serve and the released validator refuses. On an
/// auto-settle branch that refusal must land before `commit_cognition`: the
/// double records the settlement and answers success, so a misordered call
/// would still answer this projection refusal, and only the empty settlement
/// log below would catch it.
#[tokio::test]
async fn deliberate_refuses_a_cited_revision_the_released_validator_rejects() {
    let binding = DeliberateDouble::serving(u64::pow(2, 53));
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
        assessment_context_id: None,
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: std::time::Duration::from_secs(30),
    };

    let error = workflows
        .deliberate(&principal(), request, None, &caps)
        .await
        .expect_err("a judgment the validator refuses is never returned");

    let SpiError::Settlement(reject) = &error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::InternalError);
    assert_eq!(
        reject
            .details
            .as_ref()
            .and_then(|details| details.get("kind")),
        Some(&json!("judgment_projection"))
    );
    assert!(
        settlements.lock().expect("settlements").is_empty(),
        "the projection refusal lands before the commit"
    );
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
        assessment_context_id: None,
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
        assessment_context_id: None,
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
        assessment_context_id: None,
        holder: &holder,
        target: &target,
        evidence: &evidence,
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
        assessment_context_id: None,
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
