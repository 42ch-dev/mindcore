//! Storage-independent records and the caller-owned transaction boundary.
//!
//! Timestamps keep their original strings, and revision numbers are not narrowed on decode.
//! Opaque nested payloads retain their value trees; the wire codec checks closed shapes.

use serde_json::{Map, Number, Value};

use crate::{boundary::CallerId, ids::SpaceId, memory::RecallQuery};

pub mod wire;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryCandidateIr {
    pub candidate_id: String,
    pub space_id: String,
    pub subject_id: String,
    pub capture_scope: Placement,
    pub classification: Classification,
    pub provenance: Provenance,
    pub state: CandidateState,
    pub occurred_at: Option<String>,
    pub created_at: String,
    pub revision: Number,
    pub extensions: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CandidateState {
    Pending {
        kind: String,
        content: MemoryContent,
        expires_at: Option<String>,
    },
    Consumed {
        proposed_kind: Option<String>,
        consumed_by: Consumption,
        content_digest: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Consumption {
    pub operation: String,
    pub receipt_id: String,
    pub outcome: String,
    pub record_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Classification {
    pub disposition: String,
    pub rationale_codes: Vec<String>,
    pub confidence: Option<Number>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Placement {
    Shared,
    Context(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryContent {
    pub text: String,
    /// `Some(Value::Null)` is present structured content, not omission.
    pub structured: Option<Value>,
    pub cognition: Option<Vec<Value>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Provenance {
    pub source_context_ids: Vec<String>,
    pub anchors: Vec<Value>,
    pub captured_by: String,
    pub captured_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryRecordIr {
    pub record_id: String,
    pub space_id: String,
    pub subject_id: String,
    pub kind: String,
    pub scope: Placement,
    pub provenance: Provenance,
    pub state: RecordState,
    pub occurred_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub revision: Number,
    pub extensions: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordState {
    Live {
        content: MemoryContent,
        import_ref: Option<ImportReference>,
    },
    Tombstoned {
        deleted_at: String,
        deleted_by: String,
        reason: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportReference {
    pub import_id: String,
    pub source_record_id: String,
    pub source_revision: Number,
}

/// Operation receipt, not the local `MemoryStore` review row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReviewReceiptIr {
    pub receipt_id: String,
    pub space_id: String,
    pub requester: String,
    pub operation: String,
    pub idempotency_key: Option<String>,
    pub payload_digest: String,
    pub created_at: String,
    pub state: ReceiptState,
    pub extensions: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReceiptState {
    Pending,
    Committed {
        completed_at: String,
        result: Value,
    },
    Failed {
        completed_at: String,
        error: ReceiptFailure,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiptFailure {
    pub code: String,
    pub message: String,
    pub details: Map<String, Value>,
    pub extensions: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KnowledgeEntryIr {
    pub entry_id: String,
    pub entry_type: String,
    pub canonical_name: String,
    pub status: String,
    pub body: KnowledgeBody,
    pub source_anchor: Option<Value>,
    pub revision: Option<Number>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub owner: Option<String>,
    pub disclosure: Option<String>,
    pub modules: Option<KnowledgeModules>,
    pub extensions: Map<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KnowledgeBody {
    pub summary: Option<String>,
    pub tags: Option<Vec<String>>,
    pub attributes: Option<Vec<Value>>,
    pub state: Option<Map<String, Value>>,
    pub computable: Option<Map<String, Value>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KnowledgeModules {
    /// Opaque object/array modules, excluding the separately modeled cognition module.
    pub other: Map<String, Value>,
    pub cognition: Option<CognitionModule>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CognitionModule {
    pub records: Vec<Value>,
    pub world_facts: Vec<Value>,
    pub extensions: Map<String, Value>,
}

/// The replay lookup namespace, independent of any request or response envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiptKey {
    pub space: SpaceId,
    pub requester: CallerId,
    pub operation: String,
    pub idempotency_key: String,
}

/// Stage and read records only in the caller's already-open transaction or read snapshot.
///
/// The caller acquires, begins, commits and rolls back. It uses the same handle for admission,
/// replay and precondition reads and for the effect and its receipt. The repository enforces
/// storage constraints; it does not open or migrate storage, settle transactions, or own
/// protocol authorization, replay or revision policy. Inserts must not overwrite existing rows.
#[async_trait::async_trait]
pub trait MemoryRepository: Send + Sync {
    type Tx<'a>: Send
    where
        Self: 'a;
    type Error: std::error::Error + Send + Sync;

    async fn candidate(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> Result<Option<MemoryCandidateIr>, Self::Error>;

    async fn insert_candidate(
        &self,
        tx: &mut Self::Tx<'_>,
        candidate: &MemoryCandidateIr,
    ) -> Result<(), Self::Error>;

    async fn record(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> Result<Option<MemoryRecordIr>, Self::Error>;

    async fn review_receipt(
        &self,
        tx: &mut Self::Tx<'_>,
        key: &ReceiptKey,
    ) -> Result<Option<ReviewReceiptIr>, Self::Error>;

    async fn insert_review(
        &self,
        tx: &mut Self::Tx<'_>,
        record: &MemoryRecordIr,
        receipt: &ReviewReceiptIr,
    ) -> Result<(), Self::Error>;

    async fn recall(
        &self,
        tx: &mut Self::Tx<'_>,
        query: &RecallQuery,
    ) -> Result<Vec<MemoryRecordIr>, Self::Error>;
}
