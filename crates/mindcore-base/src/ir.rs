//! Storage-independent records and the caller-owned transaction boundary.
//!
//! Timestamps keep their original strings, and revision numbers are not narrowed on decode.
//! Opaque nested payloads retain their value trees; the wire codec checks closed shapes.

use serde_json::{Map, Number, Value};

use crate::{
    boundary::CallerId,
    ids::{ContextId, SpaceId, SubjectId},
    memory::RecallQuery,
};

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

/// The candidate facts a by-ID read admits on: subject, capture placement.
/// Metadata-only by design — by-ID reads admit and withhold before any
/// `content_json` is selected or decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryCandidateHead {
    pub subject_id: String,
    pub capture_scope: Placement,
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

/// The record facts a by-ID read admits on: subject, scope and whether the row
/// is tombstoned. Metadata-only by design — by-ID reads admit and withhold
/// before any `content_json` is selected or decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryRecordHead {
    pub subject_id: String,
    pub scope: Placement,
    pub tombstoned: bool,
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

/// One `memory_spaces` row. `pending_receipts` is a read-side snapshot derived from
/// `memory_receipts`; it is never persisted and guarded updates ignore it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemorySpaceRow {
    pub space_id: SpaceId,
    pub display_name: Option<String>,
    pub authority_host_id: String,
    pub authority_generation: i64,
    pub status: String,
    pub revision: Number,
    pub created_at: String,
    pub pending_receipts: u64,
}

/// One `memory_subjects` row; `kind` is the paired `(kind_ns, kind_id)` naming or `None`.
/// `extensions` is not persisted on this schema: reads reconstruct an empty map and guarded
/// writes drop a non-empty one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemorySubjectRow {
    pub space_id: SpaceId,
    pub subject_id: SubjectId,
    pub kind: Option<(String, String)>,
    pub status: String,
    pub fenced_contexts: Vec<ContextId>,
    pub revision: Number,
    pub created_at: String,
    pub extensions: Map<String, Value>,
}

/// One `memory_grants` row. `extensions` is not persisted: reads reconstruct an empty map,
/// writes drop it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryGrantRow {
    pub grant_id: String,
    pub space_id: SpaceId,
    pub subject_id: Option<SubjectId>,
    pub principal_id: CallerId,
    pub capabilities: Vec<String>,
    pub context_ids: Vec<ContextId>,
    pub shared_access: bool,
    pub expires_at: Option<String>,
    pub revoked_at: Option<String>,
    pub revision: Number,
    pub created_at: String,
    pub extensions: Map<String, Value>,
}

/// The adapter-resolved placement scope of a read: which placements the admitted set may
/// cover. Paging, ranking and authorization stay outside the repository.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum PlacementSelection {
    /// Every placement of the subject.
    #[default]
    SubjectAll,
    /// Shared rows only.
    SharedOnly,
    /// The named contexts plus shared rows when `include_shared` holds.
    SelectedContexts {
        context_ids: Vec<ContextId>,
        include_shared: bool,
    },
}

/// The adapter-resolved record-list predicate: placement scope plus the optional
/// provenance `source_context_ids` filter, kind restriction and tombstone inclusion.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecordSelection {
    pub placement: PlacementSelection,
    /// `source_context_ids` values a row's provenance must overlap; `Some(&[])` selects none.
    pub provenance_filter: Option<Vec<String>>,
    /// Restrict to these `kind` values; empty selects every kind.
    pub kinds: Vec<String>,
    /// Include tombstoned rows (basis/digest reads); record lists keep them out.
    pub include_tombstoned: bool,
}

/// The adapter-resolved predicate of a candidate list: the admitted placement scope the
/// caller may see plus the optional `status` filter (`"pending"`/`"consumed"`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CandidateSelection {
    pub placement: PlacementSelection,
    pub status: Option<String>,
}

/// The Reflection-generation lease stamped on a pending receipt (§7.0.3 liveness).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiptLease {
    pub owner_id: String,
    pub expires_at: String,
}

/// What `receipt` found for one row: an evicted row keeps its identity and last status but
/// its payload body was reclaimed (§6.10 retention tombstone).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReceiptRead {
    /// Payload columns intact; the row decodes to its full IR.
    Retained(Box<ReviewReceiptIr>),
    /// `result`/`error` evicted after the dedup window; identity, the payload digest
    /// and last status remain, so a replay can tell expiry from a payload conflict.
    Evicted {
        requester: String,
        operation: String,
        status: String,
        payload_digest: String,
    },
}

/// One `memory_profiles` row keyed by `(space, subject, profile_kind)`. `extensions` is not
/// persisted: reads reconstruct an empty map, writes drop it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryProfileRow {
    pub space_id: SpaceId,
    pub subject_id: SubjectId,
    pub profile_kind: String,
    pub document: Value,
    pub updated_by: CallerId,
    pub revision: Number,
    pub created_at: String,
    pub updated_at: String,
    pub extensions: Map<String, Value>,
}

/// One `memory_reflections` envelope row (append-only; `seq` is store-assigned on insert).
/// `extensions` is the only extension map this schema persists.
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryReflectionRow {
    pub seq: i64,
    pub reflection_id: String,
    pub space_id: SpaceId,
    pub subject_id: SubjectId,
    pub requester: CallerId,
    pub selection: Value,
    pub filter: Option<Value>,
    pub cognition_filter: Option<Value>,
    pub basis: Value,
    pub engine_policy_version: String,
    pub model: Option<String>,
    pub schema_version: i64,
    pub revision: Number,
    pub extensions: Map<String, Value>,
    pub created_at: String,
    pub body_text: String,
    pub body_format: String,
    pub cost: Option<(String, f64)>,
    /// Write-time staleness hint; reads recompute, so it is stored but never authoritative.
    pub stale_cache: bool,
}

/// One `memory_journal` durable intent row keyed by `receipt_id`; `seq` is store-assigned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryJournalRow {
    pub seq: i64,
    pub receipt_id: String,
    pub space_id: SpaceId,
    pub subject_id: SubjectId,
    pub requester: CallerId,
    pub operation: String,
    pub idempotency_key: Option<String>,
    pub state: String,
    /// Pending file effects the intent still owes; settlement scrubs the list.
    pub targets: Vec<Value>,
    pub result: Option<Value>,
    pub created_at: String,
    pub updated_at: String,
}

/// One `memory_provider_meta` marker row. Keys are caller-composed (they already carry
/// their scope); the pinned consumers are the completion/invalidation/clock markers only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryProviderMetaRow {
    pub key: String,
    pub value: String,
    pub updated_at: String,
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

    /// The admission metadata of one candidate row, without selecting or
    /// decoding its payload columns; by-ID reads conceal and withhold on it.
    async fn candidate_head(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> Result<Option<MemoryCandidateHead>, Self::Error>;

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

    /// The admission metadata of one record row, without selecting or decoding
    /// its payload columns; by-ID reads conceal and withhold on it.
    async fn record_head(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> Result<Option<MemoryRecordHead>, Self::Error>;

    /// Dedup-namespace lookup keyed by space, requester, op and idempotency key;
    /// the same retained/evicted distinction as `receipt`, so the caller can tell
    /// an expired receipt from a payload conflict.
    async fn review_receipt(
        &self,
        tx: &mut Self::Tx<'_>,
        key: &ReceiptKey,
    ) -> Result<Option<ReceiptRead>, Self::Error>;

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

    /// One space row including its pending-receipt snapshot, `Ok(None)` when absent.
    async fn space(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
    ) -> Result<Option<MemorySpaceRow>, Self::Error>;

    /// Every space row, each carrying its snapshot's pending-receipt count.
    async fn list_spaces(&self, tx: &mut Self::Tx<'_>) -> Result<Vec<MemorySpaceRow>, Self::Error>;

    /// Replaces a space row guarded by its previously observed revision.
    async fn update_space(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemorySpaceRow,
        observed_revision: &Number,
    ) -> Result<bool, Self::Error>;

    async fn subject(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> Result<Option<MemorySubjectRow>, Self::Error>;

    /// The subject registered under the `(kind_ns, kind_id)` naming, smallest id first.
    async fn subject_by_kind(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        namespace: &str,
        kind: &str,
    ) -> Result<Option<MemorySubjectRow>, Self::Error>;

    async fn insert_subject(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemorySubjectRow,
    ) -> Result<(), Self::Error>;

    async fn update_subject(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemorySubjectRow,
        observed_revision: &Number,
    ) -> Result<bool, Self::Error>;

    /// Grants of one space; `principal_filter` narrows to that principal's rows.
    async fn list_grants(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        principal_filter: Option<&str>,
    ) -> Result<Vec<MemoryGrantRow>, Self::Error>;

    async fn grant(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> Result<Option<MemoryGrantRow>, Self::Error>;

    async fn insert_grant(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryGrantRow,
    ) -> Result<(), Self::Error>;

    async fn update_grant(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryGrantRow,
        observed_revision: &Number,
    ) -> Result<bool, Self::Error>;

    /// Candidates of one subject, oldest first, filtered before any paging by the
    /// adapter-resolved selection: only rows whose placement the admitted set covers,
    /// optionally narrowed to one `status` (`"pending"`/`"consumed"`).
    async fn list_candidates(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        subject: &str,
        selection: &CandidateSelection,
    ) -> Result<Vec<MemoryCandidateIr>, Self::Error>;

    /// Replaces a candidate row guarded by its previously observed revision.
    async fn update_candidate(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryCandidateIr,
        observed_revision: &Number,
    ) -> Result<bool, Self::Error>;

    /// Records of one subject matching the adapter-resolved selection, newest first.
    async fn list_records(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        subject: &str,
        selection: &RecordSelection,
    ) -> Result<Vec<MemoryRecordIr>, Self::Error>;

    async fn insert_record(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryRecordIr,
    ) -> Result<(), Self::Error>;

    /// Replaces a record row guarded by its previously observed revision.
    async fn update_record(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryRecordIr,
        observed_revision: &Number,
    ) -> Result<bool, Self::Error>;

    /// The receipt row by space and id, distinguishing a retained body from an evicted one.
    async fn receipt(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> Result<Option<ReceiptRead>, Self::Error>;

    /// Insert-only operation receipt; `lease` stamps the Reflection pending lease pair.
    async fn insert_receipt(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &ReviewReceiptIr,
        lease: Option<&ReceiptLease>,
    ) -> Result<(), Self::Error>;

    /// Replaces a receipt's status/payload guarded by its previously observed status and
    /// owner; `observed_owner: None` matches an unleased (`NULL`) owner, never a wildcard.
    /// Owner matching is exact in both directions: only the persisted lease owner can
    /// settle a pending row, so a retry takes an abandoned lease over through
    /// `claim_receipt_lease` before it can settle.
    async fn update_receipt(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &ReviewReceiptIr,
        observed_state: &str,
        observed_owner: Option<&str>,
    ) -> Result<bool, Self::Error>;

    /// Re-leases one pending receipt to a new owner, guarded by space, receipt id, pending
    /// status and a persisted lease whose expiry instant has already passed (§7.0.3), so a
    /// crashed generation cannot wedge its idempotency key. Both the expiry guard and the
    /// stamped `expires_at` are computed at claim time (`lease_seconds` measured from the
    /// claim instant, never from the request instant captured earlier in the transaction).
    /// `true` means the caller's transaction now owns the new lease pair; `false` means the
    /// row no longer qualifies (settled, missing or re-leased) and the caller falls back to
    /// the replay/uncertainty contract instead of issuing work.
    async fn claim_receipt_lease(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
        owner_id: &str,
        lease_seconds: i64,
    ) -> Result<bool, Self::Error>;

    /// The stored lease expiry instant of one receipt: `Some` for a leased row, `None` for
    /// an unleased or missing row. Probes read it to decide whether a pending row is
    /// abandoned enough to take over.
    async fn receipt_lease(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> Result<Option<String>, Self::Error>;

    /// Profiles of one subject; `kinds` restricts the profile kinds, empty selects all.
    async fn list_profiles(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        subject: &str,
        kinds: &[String],
    ) -> Result<Vec<MemoryProfileRow>, Self::Error>;

    /// Insert-only profile row; a create-or-update goes through `update_profile`.
    async fn insert_profile(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryProfileRow,
    ) -> Result<(), Self::Error>;

    async fn update_profile(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryProfileRow,
        observed_revision: &Number,
    ) -> Result<bool, Self::Error>;

    /// Reflection envelopes of one subject, newest sequence first. `selection`, when
    /// present, keeps only rows whose pinned selection the request's scope covers.
    async fn list_reflections(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        subject: &str,
        selection: Option<&PlacementSelection>,
    ) -> Result<Vec<MemoryReflectionRow>, Self::Error>;

    /// Append-only envelope; `seq` is store-assigned, the supplied value is ignored.
    async fn insert_reflection(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryReflectionRow,
    ) -> Result<(), Self::Error>;

    /// The durable intent of one receipt in one space.
    async fn journal_intent(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        receipt_id: &str,
    ) -> Result<Option<MemoryJournalRow>, Self::Error>;

    /// Every unsettled durable intent of one subject in one space, oldest `created_at`
    /// first. The unsettled set is exactly the non-`applied` lifecycle states
    /// (`pending`, `deferred`, `quarantined`); read paths withhold subject content while
    /// any of them exists.
    async fn list_unsettled_intents(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        subject: &SubjectId,
    ) -> Result<Vec<MemoryJournalRow>, Self::Error>;

    /// Whether any non-`applied` durable intent of the subject exists in the
    /// space. An existence probe over the same literal predicate as
    /// `list_unsettled_intents`: no payload column is selected or decoded, so
    /// an unreadable `targets_json`/`result_json` cannot mask the withhold.
    async fn has_unsettled_intent(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        subject: &SubjectId,
    ) -> Result<bool, Self::Error>;
    async fn insert_journal_intent(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryJournalRow,
    ) -> Result<(), Self::Error>;

    /// Replaces an intent guarded by its previously observed lifecycle state.
    async fn update_journal_intent(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryJournalRow,
        observed_state: &str,
    ) -> Result<bool, Self::Error>;

    /// One provider marker; `key` is the caller-composed namespaced marker key (`space`
    /// documents which space's marker namespace the key belongs to).
    async fn provider_meta(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        key: &str,
    ) -> Result<Option<MemoryProviderMetaRow>, Self::Error>;

    /// Upserts one marker row for the pinned completion/invalidation markers.
    async fn put_provider_meta(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryProviderMetaRow,
    ) -> Result<(), Self::Error>;

    async fn delete_provider_meta(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        key: &str,
    ) -> Result<bool, Self::Error>;
}
