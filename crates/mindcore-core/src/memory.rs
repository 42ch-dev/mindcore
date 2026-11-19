//! The memory port: candidates awaiting review, retained records and scoped recall.

use chrono::{DateTime, Utc};

use crate::ids::{ContextId, SpaceId, SubjectId};

/// A candidate memory awaiting review. Capturing one is not retaining it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryCandidate {
    pub candidate_id: String,
    pub space: SpaceId,
    pub subject: SubjectId,
    pub context: ContextId,
    pub content: String,
    pub observed_at: DateTime<Utc>,
}

/// The durable outcome of reviewing a candidate; only records are recallable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryRecord {
    pub record_id: String,
    pub candidate_id: String,
    pub space: SpaceId,
    pub subject: SubjectId,
    pub context: ContextId,
    pub content: String,
    pub recorded_at: DateTime<Utc>,
}

/// Selects retained records by placement; `context: None` recalls every context.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecallQuery {
    pub space: SpaceId,
    pub subject: SubjectId,
    pub context: Option<ContextId>,
}

#[derive(Debug, thiserror::Error)]
pub enum MemoryError {
    #[error("candidate not found: {0}")]
    CandidateNotFound(String),
    #[error("candidate already reviewed by a different request")]
    CandidateAlreadyReviewed,
    #[error("id reused for a different operation or payload")]
    IdempotencyConflict,
    #[error(transparent)]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// The memory path over one backing store.
///
/// `capture` is idempotent by `candidate_id`: an equal payload returns `Ok(())` without a write, a
/// conflicting payload returns [`MemoryError::IdempotencyConflict`].
///
/// `retain` checks the receipt and the candidate, inserts the record and writes the receipt in one
/// transaction that holds SQLite's write reservation from its first read, so concurrent handles on
/// one file serialize. Repeating the same `(request_id, candidate_id)` returns the original record;
/// reusing the request for a different candidate returns [`MemoryError::IdempotencyConflict`]; a
/// different request for an already retained candidate returns
/// [`MemoryError::CandidateAlreadyReviewed`].
///
/// `recall` returns only retained records, filtered by `space`, `subject` and the optional
/// `context` (`None` recalls every context), ordered by `recorded_at ASC, record_id ASC`; a
/// candidate that was never retained is not recallable.
#[async_trait::async_trait]
pub trait MemoryStore {
    async fn capture(&self, candidate: MemoryCandidate) -> Result<(), MemoryError>;

    async fn candidate(&self, candidate_id: &str) -> Result<Option<MemoryCandidate>, MemoryError>;

    async fn retain(
        &self,
        candidate_id: &str,
        request_id: &str,
    ) -> Result<MemoryRecord, MemoryError>;

    async fn recall(&self, query: &RecallQuery) -> Result<Vec<MemoryRecord>, MemoryError>;
}
