//! The MIND workflow engine and its one DTO-only storage seam.
//!
//! [`WorkflowBinding`] keeps storage below this crate: it runs `memory.recall`
//! and the scoped knowledge read on the caller's own transaction and returns
//! DTOs. [`SpiMindWorkflows`] only ranks, validates and budgets that snapshot,
//! so embedder and reasoner calls never run inside a transaction.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;

use async_trait::async_trait;
use serde::Serialize;
use serde_json::{json, Map, Value};
use spoke_operations::{SpokeReject, SpokeRejectCode};
use spoke_schemas::{KnowledgeEntry, MemoryRecallRequest, Principal, RetainedRecord};

use crate::{
    CognitiveTaskKind, ContextPack, EmbeddingIdentity, MindCapabilities, Reasoner, SpiError,
    StructuredReasonRequest,
};

/// BM25 term-frequency saturation and length normalisation (the standard
/// defaults; the scoring runs in memory over the bounded recall page only).
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;

/// Fixed skeleton of the rerank leg; a lens contributes nothing here because
/// reranking adds no domain judgement to the pack.
const RERANK_PROMPT: &str = "Rank the supplied memory records by how directly each \
     one serves the query. Return every record_id exactly once, most relevant \
     first, in record_ids. Do not add, remove or repeat ids.";

/// One authorized read snapshot: the single SPOKE recall page plus the
/// caller's admitted knowledge entries, read on one transaction.
#[derive(Debug, Clone)]
pub struct RecallSnapshot {
    /// The bounded recall page in adapter order.
    pub records: Vec<RetainedRecord>,
    /// Admitted knowledge entries in stored order; empty when the binding
    /// serves no knowledge collection for this caller.
    pub knowledge: Vec<KnowledgeEntry>,
}

/// The DTO-only storage seam of the workflow engine.
///
/// One call is one read transaction over the caller's scope: `memory.recall`
/// (ACL, withholding and the SPOKE budget applied by the adapter) and the
/// scoped knowledge read. Nothing that leaves it is storage-shaped, so the
/// workflow can call a provider between two calls and revalidate afterwards.
#[async_trait]
pub trait WorkflowBinding: Send + Sync {
    /// Read the current authorized snapshot for `request`.
    async fn recall_snapshot(
        &self,
        principal: &Principal,
        request: &MemoryRecallRequest,
    ) -> Result<RecallSnapshot, SpiError>;
}

/// The workflow engine over one [`WorkflowBinding`].
pub struct SpiMindWorkflows {
    binding: Box<dyn WorkflowBinding>,
}

impl SpiMindWorkflows {
    /// Build the engine over `binding`.
    pub fn new(binding: impl WorkflowBinding + 'static) -> Self {
        Self {
            binding: Box::new(binding),
        }
    }

    /// Pack an authorized context pack under `max_tokens`.
    ///
    /// One recall page is read, reranked by the injected capabilities, then
    /// revalidated against the same read path before anything is released: a
    /// refused reread propagates and a changed page returns
    /// `REVISION_CONFLICT`, so no record revision or grant that moved during
    /// provider work reaches the pack.
    pub async fn recall_context(
        &self,
        principal: &Principal,
        request: &MemoryRecallRequest,
        max_tokens: NonZeroUsize,
        caps: &MindCapabilities<'_>,
    ) -> Result<ContextPack, SpiError> {
        let RecallSnapshot { records, knowledge } =
            self.binding.recall_snapshot(principal, request).await?;
        let before = snapshot_fact(&records, &knowledge);
        let ranked = rank(records, request.query.as_deref(), caps).await?;
        let released = self.binding.recall_snapshot(principal, request).await?;
        if snapshot_fact(&released.records, &released.knowledge) != before {
            return Err(revision_conflict());
        }
        pack(ranked, knowledge, max_tokens)
    }
}

/// What decides which items a snapshot holds: each item's identity and
/// revision, in page order.
type SnapshotFact = (Vec<(String, u64)>, Vec<(String, Option<u64>)>);

fn snapshot_fact(records: &[RetainedRecord], knowledge: &[KnowledgeEntry]) -> SnapshotFact {
    (
        records
            .iter()
            .map(|record| (record_id(record).to_string(), record_revision(record)))
            .collect(),
        knowledge
            .iter()
            .map(|entry| (entry.entry_id.clone(), entry.revision))
            .collect(),
    )
}

fn revision_conflict() -> SpiError {
    let mut details = Map::new();
    details.insert("retry_class".to_string(), Value::String("retryable".into()));
    SpiError::Settlement(SpokeReject {
        code: SpokeRejectCode::RevisionConflict,
        message: "the authorized snapshot changed while provider work ran; retry the recall"
            .to_string(),
        details: Some(details),
    })
}

/// Hybrid rerank of the page, or the adapter order when it cannot run.
///
/// The page is never widened: an empty query, an embedder without a serving
/// identity or `EmbeddingsUnavailable` all preserve the lexical order the
/// adapter returned, and every other provider failure propagates.
async fn rank(
    records: Vec<RetainedRecord>,
    query: Option<&str>,
    caps: &MindCapabilities<'_>,
) -> Result<Vec<RetainedRecord>, SpiError> {
    let Some(query) = query.map(str::trim).filter(|query| !query.is_empty()) else {
        return Ok(records);
    };
    let Some(embedder) = caps.embedder else {
        return Ok(records);
    };
    let Some(identity) = embedder.identity() else {
        return Ok(records);
    };
    if records.is_empty() {
        return Ok(records);
    }

    let texts: Vec<&str> = records.iter().map(record_text).collect();
    let mut batch = Vec::with_capacity(texts.len() + 1);
    batch.push(query.to_string());
    batch.extend(texts.iter().map(|text| (*text).to_string()));
    let vectors = match embedder.embed_batch(&batch).await {
        Ok(vectors) => vectors,
        Err(SpiError::EmbeddingsUnavailable) => return Ok(records),
        Err(error) => return Err(error),
    };
    let vectors = checked_vectors(vectors, batch.len(), identity)?;
    let scores = hybrid_scores(&texts, query, &vectors[0], &vectors[1..]);
    let mut order: Vec<usize> = (0..records.len()).collect();
    // Stable: equal scores keep the adapter's order.
    order.sort_by(|&left, &right| scores[right].total_cmp(&scores[left]));
    let mut records = reorder(records, &order);
    if let Some(reasoner) = caps.reasoner {
        let permutation = reasoner_permutation(reasoner, query, &records).await?;
        records = reorder(records, &permutation);
    }
    Ok(records)
}

fn checked_vectors(
    vectors: Vec<Vec<f32>>,
    expected: usize,
    identity: &EmbeddingIdentity,
) -> Result<Vec<Vec<f32>>, SpiError> {
    if vectors.len() != expected {
        return Err(SpiError::EmbeddingFailed(format!(
            "provider returned {} vectors for {expected} texts",
            vectors.len()
        )));
    }
    for vector in &vectors {
        if vector.len() != identity.dim {
            return Err(SpiError::EmbeddingFailed(format!(
                "provider returned dimension {} for a {}-dimension identity",
                vector.len(),
                identity.dim
            )));
        }
        if vector.iter().any(|value| !value.is_finite()) {
            return Err(SpiError::EmbeddingFailed(
                "provider returned a non-finite component".to_string(),
            ));
        }
        if vector.iter().all(|value| *value == 0.0) {
            return Err(SpiError::EmbeddingFailed(
                "provider returned a zero vector".to_string(),
            ));
        }
    }
    Ok(vectors)
}

/// `0.5 × normalised_BM25 + 0.5 × (1 + cosine)/2` over the page only.
fn hybrid_scores(
    texts: &[&str],
    query: &str,
    query_vector: &[f32],
    vectors: &[Vec<f32>],
) -> Vec<f64> {
    let lexical = bm25_scores(texts, query);
    let maximum = lexical.iter().copied().fold(0.0f64, f64::max);
    texts
        .iter()
        .enumerate()
        .map(|(index, _)| {
            let normalised = if maximum > 0.0 {
                lexical[index] / maximum
            } else {
                0.0
            };
            let similarity = cosine(query_vector, &vectors[index]);
            0.5 * normalised + 0.5 * (1.0 + similarity) / 2.0
        })
        .collect()
}

fn bm25_scores(texts: &[&str], query: &str) -> Vec<f64> {
    let terms = query_terms(query);
    let documents: Vec<Vec<String>> = texts.iter().map(|text| terms_of(text)).collect();
    let count = documents.len() as f64;
    let average = documents.iter().map(|doc| doc.len() as f64).sum::<f64>() / count;
    let mut scores = vec![0.0f64; documents.len()];
    if average == 0.0 {
        return scores;
    }
    for term in &terms {
        let matching = documents.iter().filter(|doc| doc.contains(term)).count() as f64;
        let idf = (1.0 + (count - matching + 0.5) / (matching + 0.5)).ln();
        for (index, document) in documents.iter().enumerate() {
            let frequency = document.iter().filter(|token| *token == term).count() as f64;
            if frequency == 0.0 {
                continue;
            }
            let length = document.len() as f64;
            let denominator = frequency + BM25_K1 * (1.0 - BM25_B + BM25_B * length / average);
            scores[index] += idf * frequency * (BM25_K1 + 1.0) / denominator;
        }
    }
    scores
}

fn query_terms(query: &str) -> Vec<String> {
    let mut terms: Vec<String> = Vec::new();
    for term in query.split(|character: char| !character.is_alphanumeric()) {
        if term.is_empty() {
            continue;
        }
        let term = term.to_lowercase();
        if !terms.contains(&term) {
            terms.push(term);
        }
    }
    terms
}

fn terms_of(text: &str) -> Vec<String> {
    text.split(|character: char| !character.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(str::to_lowercase)
        .collect()
}

fn cosine(left: &[f32], right: &[f32]) -> f64 {
    let mut dot = 0.0f64;
    let mut left_norm = 0.0f64;
    let mut right_norm = 0.0f64;
    for (left, right) in left.iter().zip(right) {
        let left = f64::from(*left);
        let right = f64::from(*right);
        dot += left * right;
        left_norm += left * left;
        right_norm += right * right;
    }
    (dot / (left_norm.sqrt() * right_norm.sqrt())).clamp(-1.0, 1.0)
}

fn reorder(records: Vec<RetainedRecord>, order: &[usize]) -> Vec<RetainedRecord> {
    let mut slots: Vec<Option<RetainedRecord>> = records.into_iter().map(Some).collect();
    order
        .iter()
        .map(|&index| slots[index].take().expect("a permutation of the same page"))
        .collect()
}

/// The reasoner may only reorder the admitted page: every id exactly once.
async fn reasoner_permutation(
    reasoner: &dyn Reasoner,
    query: &str,
    records: &[RetainedRecord],
) -> Result<Vec<usize>, SpiError> {
    let admitted: Vec<Value> = records
        .iter()
        .map(|record| json!({ "record_id": record_id(record), "text": record_text(record) }))
        .collect();
    let payload = json!({ "query": query, "records": admitted }).to_string();
    let schema = json!({
        "type": "object",
        "properties": { "record_ids": { "type": "array", "items": { "type": "string" } } },
        "required": ["record_ids"],
        "additionalProperties": false,
    });
    let verdict = reasoner
        .reason_structured(StructuredReasonRequest {
            task_kind: CognitiveTaskKind::Rerank,
            system_prompt: RERANK_PROMPT,
            user_payload: &payload,
            output_schema: &schema,
        })
        .await?;
    let ids = verdict
        .get("record_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            SpiError::InvalidReasonerOutput("record_ids must be an array".to_string())
        })?;
    let positions: HashMap<&str, usize> = records
        .iter()
        .enumerate()
        .map(|(index, record)| (record_id(record), index))
        .collect();
    let mut seen = HashSet::new();
    let mut permutation = Vec::with_capacity(ids.len());
    for id in ids {
        let id = id.as_str().ok_or_else(|| {
            SpiError::InvalidReasonerOutput("record_ids must name strings".to_string())
        })?;
        if !seen.insert(id) {
            return Err(SpiError::InvalidReasonerOutput(format!(
                "record id {id} appears twice"
            )));
        }
        let Some(&position) = positions.get(id) else {
            return Err(SpiError::InvalidReasonerOutput(format!(
                "record id {id} is not in the admitted page"
            )));
        };
        permutation.push(position);
    }
    if permutation.len() != records.len() {
        return Err(SpiError::InvalidReasonerOutput(
            "record_ids is not a permutation of the admitted page".to_string(),
        ));
    }
    Ok(permutation)
}

/// Whole items only, memory records first, then knowledge entries, each in its
/// own order; one token is charged per UTF-8 byte of the serialized item and
/// an item that does not fit is skipped rather than cut.
fn pack(
    records: Vec<RetainedRecord>,
    knowledge: Vec<KnowledgeEntry>,
    max_tokens: NonZeroUsize,
) -> Result<ContextPack, SpiError> {
    let budget = max_tokens.get();
    let mut tokens_used = 0usize;
    let mut packed_records = Vec::new();
    for record in records {
        let size = serialized_len(&record)?;
        if size > budget.saturating_sub(tokens_used) {
            continue;
        }
        tokens_used += size;
        packed_records.push(record);
    }
    let mut packed_knowledge = Vec::new();
    for entry in knowledge {
        let size = serialized_len(&entry)?;
        if size > budget.saturating_sub(tokens_used) {
            continue;
        }
        tokens_used += size;
        packed_knowledge.push(entry);
    }
    Ok(ContextPack {
        records: packed_records,
        knowledge: packed_knowledge,
        tokens_used,
    })
}

fn serialized_len<T: Serialize>(item: &T) -> Result<usize, SpiError> {
    serde_json::to_string(item)
        .map(|serialized| serialized.len())
        .map_err(|error| SpiError::InvalidInput(error.to_string()))
}

fn record_id(record: &RetainedRecord) -> &str {
    match record {
        RetainedRecord::LiveRecord(record) => record.record_id.as_str(),
        RetainedRecord::TombstonedRecord(record) => record.record_id.as_str(),
    }
}

fn record_revision(record: &RetainedRecord) -> u64 {
    match record {
        RetainedRecord::LiveRecord(record) => record.revision.get(),
        RetainedRecord::TombstonedRecord(record) => record.revision.get(),
    }
}

fn record_text(record: &RetainedRecord) -> &str {
    match record {
        RetainedRecord::LiveRecord(record) => record.content.text.as_str(),
        RetainedRecord::TombstonedRecord(record) => record.content.text.as_str(),
    }
}
