//! `MemoryRepository` over a caller-owned transaction against the Nexus-shaped memory schema.
//!
//! Every statement reborrows the caller's `&mut Transaction`; this module never acquires a
//! connection, opens a pool, or begins/commits/rolls back a transaction. Rows decode through
//! the wire codec's JSON shapes, so invalid persisted payloads surface as
//! [`SqliteRepositoryError::InvalidRow`], and `extensions` on candidates, records and
//! receipts is reconstructed empty (the pinned schema has no column for it; see
//! `task-1-schema-map.md` §3 D-1).

use std::future::poll_fn;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::{json, Map, Number, Value};
use sqlx::Sqlite;

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::{ContextId, SpaceId, SubjectId};
use mindcore_base::ir::wire::{
    memory_candidate_from_wire, memory_candidate_to_wire, memory_record_from_wire,
    memory_record_to_wire, review_receipt_from_wire, review_receipt_to_wire, WireVersion,
};
use mindcore_base::ir::{
    CandidateSelection, CandidateState, MemoryCandidateHead, MemoryCandidateIr, MemoryGrantRow,
    MemoryJournalRow, MemoryProfileRow, MemoryProviderMetaRow, MemoryRecordHead, MemoryRecordIr,
    MemoryReflectionRow, MemoryRepository, MemorySpaceRow, MemorySubjectRow, Placement,
    PlacementSelection, ReceiptKey, ReceiptLease, ReceiptRead, RecordSelection, ReviewReceiptIr,
};
use mindcore_base::memory::RecallQuery;

/// A storage failure on the borrowed transaction; protocol semantics stay with the caller.
#[derive(Debug, thiserror::Error)]
pub enum SqliteRepositoryError {
    /// A driver error or a violated constraint; the caller rolls back the transaction.
    #[error("sqlite statement failed: {0}")]
    Sql(#[from] sqlx::Error),
    /// An insert collided with an existing key; nothing was overwritten.
    #[error("row already exists: {0}")]
    Conflict(String),
    /// A persisted row, its JSON payload or a supplied field did not match its shape.
    #[error("invalid persisted row: {0}")]
    InvalidRow(String),
}

fn conflict(error: sqlx::Error, what: impl std::fmt::Display) -> SqliteRepositoryError {
    match error {
        sqlx::Error::Database(error) if error.is_unique_violation() => {
            SqliteRepositoryError::Conflict(what.to_string())
        }
        other => SqliteRepositoryError::Sql(other),
    }
}

fn invalid_row(message: impl std::fmt::Display) -> SqliteRepositoryError {
    SqliteRepositoryError::InvalidRow(message.to_string())
}

fn to_json(value: &Value, what: &str) -> Result<String, SqliteRepositoryError> {
    serde_json::to_string(value).map_err(|error| invalid_row(format_args!("{what}: {error}")))
}

fn from_json(raw: &str, what: &str) -> Result<Value, SqliteRepositoryError> {
    serde_json::from_str(raw).map_err(|error| invalid_row(format_args!("{what}: {error}")))
}

fn revision_i64(revision: &Number) -> Result<i64, SqliteRepositoryError> {
    revision
        .as_i64()
        .or_else(|| {
            revision
                .as_u64()
                .and_then(|value| i64::try_from(value).ok())
        })
        .ok_or_else(|| invalid_row(format_args!("revision {revision} is not an integer")))
}

fn version() -> WireVersion {
    WireVersion::new(Number::from(1)).expect("1 is a positive schema version")
}

fn wire_object<'a>(value: &'a mut Value, what: &str) -> &'a mut Map<String, Value> {
    value
        .as_object_mut()
        .unwrap_or_else(|| panic!("{what} is an object"))
}

type Tx<'a> = sqlx::Transaction<'a, Sqlite>;
type RepoResult<T> = Result<T, SqliteRepositoryError>;

/// Maps a macro-query fetch failure back to the borrowed path's contract: a stored
/// column that cannot decode is an `InvalidRow`, everything else stays a drive error.
fn decode_row(error: sqlx::Error) -> SqliteRepositoryError {
    match error {
        sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::ColumnIndexOutOfBounds { .. } => invalid_row(format_args!("row: {error}")),
        other => other.into(),
    }
}

// note: candidate row family

/// The `Placement` one candidate/record visibility column pair encodes, shared
/// by the head lookups and the full-row decoders.
fn placement_of(visibility: &str, context_id: Option<String>, what: &str) -> RepoResult<Placement> {
    match visibility {
        "shared" => Ok(Placement::Shared),
        "context" => context_id
            .map(Placement::Context)
            .ok_or_else(|| invalid_row(format_args!("{what} placement has no context id"))),
        other => Err(invalid_row(format_args!("{what} visibility {other}"))),
    }
}

/// A `memory_candidates` row; `Option` mirrors stored nullability so `query_as!`
/// compile-time decoding and the dynamic-list decode share one shape.
struct CandidateRow {
    candidate_id: String,
    space_id: String,
    subject_id: String,
    capture_visibility: String,
    capture_context_id: Option<String>,
    kind: String,
    content_json: Option<String>,
    provenance_json: String,
    classification_json: String,
    status: String,
    consumed_by_json: Option<String>,
    content_digest: Option<String>,
    occurred_at: Option<String>,
    expires_at: Option<String>,
    revision: i64,
    created_at: String,
}

fn candidate_ir(row: CandidateRow) -> RepoResult<MemoryCandidateIr> {
    let status = row.status;
    let mut proposed = Map::new();
    // why: a consumed row with an empty stored kind has no representable proposal;
    // the consumed codec accepts `proposed` absent.
    if !row.kind.is_empty() {
        proposed.insert("kind".into(), row.kind.into());
    }
    let mut wire = Map::new();
    wire.insert("schema_version".into(), 1.into());
    wire.insert("candidate_id".into(), row.candidate_id.into());
    wire.insert("space_id".into(), row.space_id.into());
    wire.insert("subject_id".into(), row.subject_id.into());
    wire.insert(
        "capture_scope".into(),
        placement_wire_value(
            row.capture_visibility.as_str(),
            row.capture_context_id,
            "capture",
        )?,
    );
    wire.insert(
        "classification".into(),
        from_json(&row.classification_json, "classification")?,
    );
    wire.insert(
        "provenance".into(),
        from_json(&row.provenance_json, "provenance")?,
    );
    wire.insert("created_at".into(), row.created_at.into());
    wire.insert("revision".into(), row.revision.into());
    wire.insert("extensions".into(), Map::new().into());
    if let Some(occurred_at) = row.occurred_at {
        wire.insert("occurred_at".into(), occurred_at.into());
    }
    match status.as_str() {
        "pending" => {
            let content = row
                .content_json
                .ok_or_else(|| invalid_row("pending candidate has no content_json"))
                .and_then(|raw| from_json(&raw, "candidate content"))?;
            proposed.insert("content".into(), content);
            if let Some(expires_at) = row.expires_at {
                wire.insert("expires_at".into(), expires_at.into());
            }
        }
        "consumed" => {
            wire.insert(
                "consumed_by".into(),
                from_json(
                    &row.consumed_by_json
                        .ok_or_else(|| invalid_row("consumed candidate has no consumed_by"))?,
                    "candidate outcome",
                )?,
            );
            wire.insert(
                "content_digest".into(),
                row.content_digest
                    .ok_or_else(|| invalid_row("consumed candidate has no content_digest"))?
                    .into(),
            );
        }
        other => return Err(invalid_row(format_args!("candidate status {other}"))),
    }
    wire.insert("status".into(), status.into());
    if !proposed.is_empty() {
        wire.insert("proposed".into(), proposed.into());
    }
    let (_, ir) = memory_candidate_from_wire(wire.into()).map_err(invalid_row)?;
    Ok(ir)
}

/// The wire `placement` document one stored visibility/context pair encodes.
fn placement_wire_value(
    visibility: &str,
    context_id: Option<String>,
    what: &str,
) -> RepoResult<Value> {
    match placement_of(visibility, context_id, what)? {
        Placement::Shared => Ok(json!({"visibility": "shared"})),
        Placement::Context(context_id) => Ok(json!({
            "visibility": "context",
            "context_id": context_id,
        })),
    }
}

struct CandidateColumns {
    // why: `None` when the row has no representable kind (a consumed candidate whose
    // IR dropped `proposed_kind`): writes keep the stored kind instead.
    kind: Option<String>,
    content: Option<String>,
    provenance: String,
    classification: String,
    status: &'static str,
    consumed_by: Option<String>,
    content_digest: Option<String>,
    expires_at: Option<String>,
}

fn candidate_columns(candidate: &MemoryCandidateIr) -> RepoResult<CandidateColumns> {
    let mut wire = memory_candidate_to_wire(version(), candidate.clone());
    let object = wire_object(&mut wire, "candidate wire");
    let mut proposed = object.remove("proposed").and_then(|mut proposed| {
        proposed
            .as_object_mut()
            .map(|proposed| (proposed.remove("kind"), proposed.remove("content")))
    });
    match &candidate.state {
        CandidateState::Pending { .. } => {
            let (kind, content) = proposed
                .take()
                .ok_or_else(|| invalid_row("pending candidate wire misses proposed"))?;
            Ok(CandidateColumns {
                kind: Some(
                    kind.and_then(|kind| kind.as_str().map(str::to_string))
                        .filter(|kind| !kind.is_empty())
                        .ok_or_else(|| invalid_row("pending candidate kind is empty"))?,
                ),
                content: Some(to_json(
                    // why: unreachable via public IR today; kept as a defensive guard so a future
                    // encoder change cannot silently write an unreadable row
                    &content.ok_or_else(|| {
                        invalid_row("pending candidate wire misses proposed.content")
                    })?,
                    "candidate content",
                )?),
                provenance: to_json(
                    &object.remove("provenance").unwrap_or_default(),
                    "provenance",
                )?,
                classification: to_json(
                    &object.remove("classification").unwrap_or_default(),
                    "classification",
                )?,
                status: "pending",
                consumed_by: None,
                content_digest: None,
                expires_at: object
                    .remove("expires_at")
                    .and_then(|value| value.as_str().map(str::to_string)),
            })
        }
        CandidateState::Consumed {
            proposed_kind,
            content_digest,
            ..
        } => {
            let kind = proposed_kind.clone();
            Ok(CandidateColumns {
                kind,
                content: None,
                provenance: to_json(
                    &object.remove("provenance").unwrap_or_default(),
                    "provenance",
                )?,
                classification: to_json(
                    &object.remove("classification").unwrap_or_default(),
                    "classification",
                )?,
                status: "consumed",
                consumed_by: Some(to_json(
                    &object.remove("consumed_by").unwrap_or_default(),
                    "candidate outcome",
                )?),
                content_digest: Some(content_digest.clone()),
                expires_at: None,
            })
        }
    }
}

struct CandidateHeadRow {
    subject_id: String,
    capture_visibility: String,
    capture_context_id: Option<String>,
}

/// The subject and capture placement only: `content_json` and the other
/// payload columns stay unselected so admission and the withheld probe precede
/// any content decode.
async fn candidate_head_by_key(
    tx: &mut Tx<'_>,
    space: &SpaceId,
    id: &str,
) -> RepoResult<Option<MemoryCandidateHead>> {
    sqlx::query_as!(
        CandidateHeadRow,
        "SELECT subject_id, capture_visibility, capture_context_id \
         FROM memory_candidates WHERE space_id = ? AND candidate_id = ?",
        space.as_str(),
        id,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(decode_row)?
    .map(|row| {
        placement_of(
            row.capture_visibility.as_str(),
            row.capture_context_id,
            "candidate capture",
        )
        .map(|capture_scope| MemoryCandidateHead {
            subject_id: row.subject_id,
            capture_scope,
        })
    })
    .transpose()
}

async fn candidate_by_key(
    tx: &mut Tx<'_>,
    space: &SpaceId,
    id: &str,
) -> RepoResult<Option<MemoryCandidateIr>> {
    sqlx::query_as!(
        CandidateRow,
        "SELECT candidate_id, space_id, subject_id, capture_visibility, \
         capture_context_id, kind, content_json, provenance_json, classification_json, status, \
         consumed_by_json, content_digest, occurred_at, expires_at, revision, created_at \
         FROM memory_candidates WHERE space_id = ? AND candidate_id = ?",
        space.as_str(),
        id,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(decode_row)?
    .map(candidate_ir)
    .transpose()
}

async fn insert_candidate_row(tx: &mut Tx<'_>, candidate: &MemoryCandidateIr) -> RepoResult<()> {
    let columns = candidate_columns(candidate)?;
    // invariant: C-2 policy — the `kind` column is `NOT NULL` and must decode back to a non-empty
    // `proposed.kind`. A consume update without `proposed_kind` therefore preserves the
    // stored kind; a fresh row has nothing to preserve and is rejected instead of
    // writing an empty string it could never read back.
    let kind = columns
        .kind
        .clone()
        .ok_or_else(|| invalid_row("candidate has no kind to persist"))?;
    let (visibility, context_id) = placement_columns(&candidate.capture_scope);
    sqlx::query!(
        "INSERT INTO memory_candidates \
         (candidate_id, space_id, subject_id, capture_visibility, capture_context_id, kind, \
          content_json, provenance_json, classification_json, status, consumed_by_json, \
          content_digest, occurred_at, expires_at, revision, created_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        &candidate.candidate_id,
        &candidate.space_id,
        &candidate.subject_id,
        visibility,
        context_id,
        &kind,
        &columns.content,
        &columns.provenance,
        &columns.classification,
        columns.status,
        &columns.consumed_by,
        &columns.content_digest,
        &candidate.occurred_at,
        &columns.expires_at,
        revision_i64(&candidate.revision)?,
        &candidate.created_at,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| conflict(error, &candidate.candidate_id))?;
    Ok(())
}

fn placement_columns(scope: &Placement) -> (&'static str, Option<String>) {
    match scope {
        Placement::Shared => ("shared", None),
        Placement::Context(context_id) => ("context", Some(context_id.clone())),
    }
}

// note: record row family

struct RecordRow {
    record_id: String,
    space_id: String,
    subject_id: String,
    kind: String,
    content_json: String,
    scope_visibility: String,
    scope_context_id: Option<String>,
    provenance_json: String,
    import_ref_json: Option<String>,
    occurred_at: Option<String>,
    tombstone_json: Option<String>,
    revision: i64,
    created_at: String,
    updated_at: String,
}

fn record_ir(row: RecordRow) -> RepoResult<MemoryRecordIr> {
    let scope = placement_wire_value(
        row.scope_visibility.as_str(),
        row.scope_context_id,
        "record scope",
    )?;
    let mut wire = Map::new();
    wire.insert("schema_version".into(), 1.into());
    wire.insert("record_id".into(), row.record_id.into());
    wire.insert("space_id".into(), row.space_id.into());
    wire.insert("subject_id".into(), row.subject_id.into());
    wire.insert("kind".into(), row.kind.into());
    wire.insert("scope".into(), scope);
    wire.insert(
        "content".into(),
        from_json(&row.content_json, "record content")?,
    );
    wire.insert(
        "provenance".into(),
        from_json(&row.provenance_json, "record provenance")?,
    );
    // why: pinned projection — the tombstone marks the row dead; a surviving
    // `import_ref_json` (the native delete leaves it) is dropped, and an explicit
    // JSON null `reason` normalizes to the wire's optional-member shape.
    if let Some(raw) = row.tombstone_json {
        let mut tombstone = from_json(&raw, "record tombstone")?;
        if let Some(object) = tombstone.as_object_mut() {
            if object.get("reason") == Some(&Value::Null) {
                object.remove("reason");
            }
        }
        wire.insert("tombstone".into(), tombstone);
    } else if let Some(import_ref) = row.import_ref_json {
        wire.insert(
            "import_ref".into(),
            from_json(&import_ref, "record import ref")?,
        );
    }
    if let Some(occurred_at) = row.occurred_at {
        wire.insert("occurred_at".into(), occurred_at.into());
    }
    wire.insert("created_at".into(), row.created_at.into());
    wire.insert("updated_at".into(), row.updated_at.into());
    wire.insert("revision".into(), row.revision.into());
    wire.insert("extensions".into(), Map::new().into());
    let (_, ir) = memory_record_from_wire(wire.into()).map_err(invalid_row)?;
    Ok(ir)
}

struct RecordColumns {
    content: String,
    provenance: String,
    tombstone: Option<String>,
    import_ref: Option<String>,
}

fn record_columns(record: &MemoryRecordIr) -> RepoResult<RecordColumns> {
    let mut wire = memory_record_to_wire(version(), record.clone());
    let object = wire_object(&mut wire, "record wire");
    Ok(RecordColumns {
        content: to_json(
            &object.remove("content").unwrap_or_default(),
            "record content",
        )?,
        provenance: to_json(
            &object.remove("provenance").unwrap_or_default(),
            "record provenance",
        )?,
        tombstone: object
            .remove("tombstone")
            .map(|value| to_json(&value, "record tombstone"))
            .transpose()?,
        import_ref: object
            .remove("import_ref")
            .map(|value| to_json(&value, "record import ref"))
            .transpose()?,
    })
}

struct RecordHeadRow {
    subject_id: String,
    scope_visibility: String,
    scope_context_id: Option<String>,
    tombstone_json: Option<String>,
}

/// The subject, scope and tombstone marker only: `content_json` and the other
/// payload columns stay unselected so admission and the withheld probe precede
/// any content decode.
async fn record_head_by_key(
    tx: &mut Tx<'_>,
    space: &SpaceId,
    id: &str,
) -> RepoResult<Option<MemoryRecordHead>> {
    sqlx::query_as!(
        RecordHeadRow,
        "SELECT subject_id, scope_visibility, scope_context_id, tombstone_json \
         FROM memory_records WHERE space_id = ? AND record_id = ?",
        space.as_str(),
        id,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(decode_row)?
    .map(|row| {
        placement_of(
            row.scope_visibility.as_str(),
            row.scope_context_id,
            "record scope",
        )
        .map(|scope| MemoryRecordHead {
            subject_id: row.subject_id,
            scope,
            tombstoned: row.tombstone_json.is_some(),
        })
    })
    .transpose()
}

async fn record_by_key(
    tx: &mut Tx<'_>,
    space: &SpaceId,
    id: &str,
) -> RepoResult<Option<MemoryRecordIr>> {
    sqlx::query_as!(
        RecordRow,
        "SELECT record_id, space_id, subject_id, kind, content_json, \
         scope_visibility, scope_context_id, provenance_json, import_ref_json, occurred_at, \
         tombstone_json, revision, created_at, updated_at \
         FROM memory_records WHERE space_id = ? AND record_id = ?",
        space.as_str(),
        id,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(decode_row)?
    .map(record_ir)
    .transpose()
}

async fn insert_record_row(tx: &mut Tx<'_>, record: &MemoryRecordIr) -> RepoResult<()> {
    let columns = record_columns(record)?;
    let (visibility, context_id) = placement_columns(&record.scope);
    sqlx::query!(
        "INSERT INTO memory_records \
         (record_id, space_id, subject_id, kind, content_json, scope_visibility, \
          scope_context_id, provenance_json, import_ref_json, occurred_at, tombstone_json, \
          revision, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        &record.record_id,
        &record.space_id,
        &record.subject_id,
        &record.kind,
        &columns.content,
        visibility,
        context_id,
        &columns.provenance,
        &columns.import_ref,
        &record.occurred_at,
        &columns.tombstone,
        revision_i64(&record.revision)?,
        &record.created_at,
        &record.updated_at,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| conflict(error, &record.record_id))?;
    Ok(())
}

// note: receipt row family

struct ReceiptRow {
    receipt_id: String,
    space_id: String,
    requester_principal_id: String,
    op: String,
    idempotency_key: Option<String>,
    payload_digest: String,
    status: String,
    result_json: Option<String>,
    error_json: Option<String>,
    created_at: String,
    completed_at: Option<String>,
}

fn receipt_wire(row: ReceiptRow) -> RepoResult<Value> {
    let mut wire = Map::new();
    wire.insert("schema_version".into(), 1.into());
    wire.insert("receipt_id".into(), row.receipt_id.into());
    wire.insert("space_id".into(), row.space_id.into());
    wire.insert(
        "requester_principal_id".into(),
        row.requester_principal_id.into(),
    );
    wire.insert("op".into(), row.op.into());
    if let Some(key) = row.idempotency_key {
        wire.insert("idempotency_key".into(), key.into());
    }
    wire.insert("payload_digest".into(), row.payload_digest.into());
    wire.insert("created_at".into(), row.created_at.into());
    wire.insert("extensions".into(), Map::new().into());
    wire.insert("status".into(), row.status.into());
    if let Some(completed_at) = row.completed_at {
        wire.insert("completed_at".into(), completed_at.into());
    }
    if let Some(result) = row.result_json {
        wire.insert("result".into(), from_json(&result, "receipt result")?);
    }
    if let Some(error) = row.error_json {
        wire.insert("error".into(), from_json(&error, "receipt error")?);
    }
    Ok(wire.into())
}

/// Decodes one receipt row: a settled row whose payload columns were reclaimed by
/// retention reports `Evicted` (digest + last status survive), anything else must
/// decode as a full retained receipt.
fn receipt_read(row: ReceiptRow) -> RepoResult<ReceiptRead> {
    let evicted = row.result_json.is_none() && row.error_json.is_none() && row.status != "pending";
    if evicted {
        return Ok(ReceiptRead::Evicted {
            requester: row.requester_principal_id,
            operation: row.op,
            status: row.status,
            payload_digest: row.payload_digest,
        });
    }
    let wire = receipt_wire(row)?;
    let (_, ir) = review_receipt_from_wire(wire).map_err(invalid_row)?;
    Ok(ReceiptRead::Retained(Box::new(ir)))
}

struct ReceiptColumns {
    status: &'static str,
    result: Option<String>,
    error: Option<String>,
    completed_at: Option<String>,
}

fn receipt_columns(receipt: &ReviewReceiptIr) -> RepoResult<ReceiptColumns> {
    let mut wire = review_receipt_to_wire(version(), receipt.clone());
    let object = wire_object(&mut wire, "receipt wire");
    Ok(ReceiptColumns {
        status: match &receipt.state {
            mindcore_base::ir::ReceiptState::Pending => "pending",
            mindcore_base::ir::ReceiptState::Committed { .. } => "committed",
            mindcore_base::ir::ReceiptState::Failed { .. } => "failed",
        },
        result: object
            .remove("result")
            .map(|value| to_json(&value, "receipt result"))
            .transpose()?,
        error: object
            .remove("error")
            .map(|value| to_json(&value, "receipt error"))
            .transpose()?,
        completed_at: object
            .remove("completed_at")
            .and_then(|value| value.as_str().map(str::to_string)),
    })
}

async fn insert_receipt_row(
    tx: &mut Tx<'_>,
    receipt: &ReviewReceiptIr,
    lease: Option<&ReceiptLease>,
) -> RepoResult<()> {
    let columns = receipt_columns(receipt)?;
    sqlx::query!(
        "INSERT INTO memory_receipts \
         (receipt_id, space_id, requester_principal_id, op, idempotency_key, payload_digest, \
          status, result_json, error_json, created_at, completed_at, owner_id, \
          lease_expires_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        &receipt.receipt_id,
        &receipt.space_id,
        &receipt.requester,
        &receipt.operation,
        &receipt.idempotency_key,
        &receipt.payload_digest,
        columns.status,
        &columns.result,
        &columns.error,
        &receipt.created_at,
        &columns.completed_at,
        lease.map(|lease| lease.owner_id.clone()),
        lease.map(|lease| lease.expires_at.clone()),
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| conflict(error, &receipt.receipt_id))?;
    Ok(())
}

// note: remaining row families

struct SpaceRow {
    space_id: String,
    display_name: Option<String>,
    authority_host_id: String,
    authority_generation: i64,
    status: String,
    revision: i64,
    created_at: String,
    pending_receipts: i64,
}

fn space_row(row: SpaceRow) -> RepoResult<MemorySpaceRow> {
    Ok(MemorySpaceRow {
        space_id: SpaceId::new(row.space_id),
        display_name: row.display_name,
        authority_host_id: row.authority_host_id,
        authority_generation: row.authority_generation,
        status: row.status,
        revision: row.revision.into(),
        created_at: row.created_at,
        pending_receipts: u64::try_from(row.pending_receipts)
            .map_err(|_| invalid_row("negative pending_receipts"))?,
    })
}

fn subject_row(row: SubjectRow) -> RepoResult<MemorySubjectRow> {
    let kind = match (row.kind_ns, row.kind_id) {
        (Some(namespace), Some(kind)) => Some((namespace, kind)),
        (None, None) => None,
        _ => return Err(invalid_row("subject kind pair is half set")),
    };
    let fenced: Vec<String> = serde_json::from_str(&row.fenced_contexts_json)
        .map_err(|error| invalid_row(format_args!("fenced_contexts_json: {error}")))?;
    Ok(MemorySubjectRow {
        space_id: SpaceId::new(row.space_id),
        subject_id: SubjectId::new(row.subject_id),
        kind,
        status: row.status,
        fenced_contexts: fenced.into_iter().map(ContextId::new).collect(),
        revision: row.revision.into(),
        created_at: row.created_at,
        extensions: Map::new(),
    })
}

struct SubjectRow {
    space_id: String,
    subject_id: String,
    kind_ns: Option<String>,
    kind_id: Option<String>,
    status: String,
    fenced_contexts_json: String,
    revision: i64,
    created_at: String,
}

/// Positional form of [`SubjectRow`] for the runtime-decoded space-wide scan.
type SubjectColumns = (
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    i64,
    String,
);

fn subject_columns_row(row: SubjectColumns) -> RepoResult<MemorySubjectRow> {
    let (
        space_id,
        subject_id,
        kind_ns,
        kind_id,
        status,
        fenced_contexts_json,
        revision,
        created_at,
    ) = row;
    subject_row(SubjectRow {
        space_id,
        subject_id,
        kind_ns,
        kind_id,
        status,
        fenced_contexts_json,
        revision,
        created_at,
    })
}

/// Positional form of [`MemoryExportRow`] for the runtime-decoded header lookup.
type ExportColumns = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    i64,
    String,
    i64,
    String,
    String,
    String,
);

fn export_row(row: ExportColumns) -> RepoResult<MemoryExportRow> {
    let (
        export_id,
        space_id,
        requester,
        subject_id,
        selection,
        include_tombstoned,
        bundle_digest,
        chunk_count,
        created_at,
        expires_at,
        state,
    ) = row;
    Ok(MemoryExportRow {
        export_id,
        space_id,
        requester,
        subject_id,
        selection,
        include_tombstoned: include_tombstoned != 0,
        bundle_digest,
        chunk_count: u64::try_from(chunk_count)
            .map_err(|_| invalid_row("negative export chunk_count"))?,
        created_at,
        expires_at,
        expired: state == "expired",
    })
}

/// Positional form of [`MemoryImportRow`] for the runtime-decoded staging lookup.
type ImportColumns = (
    String,
    String,
    String,
    String,
    String,
    i64,
    i64,
    String,
    String,
    String,
    Option<String>,
    String,
    String,
    String,
);

fn import_row(row: ImportColumns) -> RepoResult<MemoryImportRow> {
    let (
        import_id,
        space_id,
        requester,
        source_export_id,
        bundle_digest,
        expected_count,
        next_ordinal,
        conflict_policy,
        adopt_grants_json,
        reference_translations_json,
        staged_payload_json,
        state,
        created_at,
        expires_at,
    ) = row;
    Ok(MemoryImportRow {
        import_id,
        space_id,
        requester,
        source_export_id,
        bundle_digest,
        expected_count: u64::try_from(expected_count)
            .map_err(|_| invalid_row("negative import expected_count"))?,
        next_ordinal: u64::try_from(next_ordinal)
            .map_err(|_| invalid_row("negative import next_ordinal"))?,
        conflict_policy,
        adopt_grants: from_json(&adopt_grants_json, "import adopt_grants")?,
        reference_translations: from_json(
            &reference_translations_json,
            "import reference_translations",
        )?,
        staged_payload: staged_payload_json
            .map(|raw| from_json(&raw, "import staged payload"))
            .transpose()?,
        state,
        created_at,
        expires_at,
    })
}

fn grant_row(row: GrantRow) -> RepoResult<MemoryGrantRow> {
    let capabilities: Vec<String> = serde_json::from_str(&row.capabilities_json)
        .map_err(|error| invalid_row(format_args!("capabilities_json: {error}")))?;
    let contexts: Vec<String> = serde_json::from_str(&row.context_ids_json)
        .map_err(|error| invalid_row(format_args!("context_ids_json: {error}")))?;
    Ok(MemoryGrantRow {
        grant_id: row.grant_id,
        space_id: SpaceId::new(row.space_id),
        subject_id: row.subject_id.map(SubjectId::new),
        principal_id: CallerId::new(row.principal_id),
        capabilities,
        context_ids: contexts.into_iter().map(ContextId::new).collect(),
        shared_access: row.shared_access != 0,
        expires_at: row.expires_at,
        revoked_at: row.revoked_at,
        revision: row.revision.into(),
        created_at: row.created_at,
        extensions: Map::new(),
    })
}

struct GrantRow {
    grant_id: String,
    space_id: String,
    subject_id: Option<String>,
    principal_id: String,
    capabilities_json: String,
    context_ids_json: String,
    shared_access: i64,
    expires_at: Option<String>,
    revoked_at: Option<String>,
    revision: i64,
    created_at: String,
}

/// A `memory_profiles` row; `list_profiles` decodes through the macro's
/// unchecked UTF-8 contract like every other compile-checked path.
struct ProfileRow {
    space_id: String,
    subject_id: String,
    profile_kind: String,
    document_json: String,
    updated_by: String,
    revision: i64,
    created_at: String,
    updated_at: String,
}

fn profile_ir(row: ProfileRow) -> RepoResult<MemoryProfileRow> {
    let document = from_json(&row.document_json, "profile document")?;
    Ok(MemoryProfileRow {
        space_id: SpaceId::new(row.space_id),
        subject_id: SubjectId::new(row.subject_id),
        profile_kind: row.profile_kind,
        document,
        updated_by: CallerId::new(row.updated_by),
        revision: row.revision.into(),
        created_at: row.created_at,
        updated_at: row.updated_at,
        extensions: Map::new(),
    })
}

/// The admitted-placement predicate as fixed flags plus the context id list
/// serialized for `json_each`: `(all, shared_only, contexts_json)` — an empty
/// list matches no context, so `SubjectAll` and `SharedOnly` carry `"[]"`.
fn placement_predicate(placement: &PlacementSelection) -> RepoResult<(bool, bool, String)> {
    match placement {
        PlacementSelection::SubjectAll => Ok((true, false, "[]".to_string())),
        PlacementSelection::SharedOnly => Ok((false, true, "[]".to_string())),
        PlacementSelection::SelectedContexts {
            context_ids,
            include_shared,
        } => Ok((
            false,
            *include_shared,
            to_json(&json!(context_ids), "placement contexts")?,
        )),
    }
}

fn reflection_row(row: ReflectionRow) -> RepoResult<MemoryReflectionRow> {
    // why: a stored JSON null reads as absent — a persisted `null` filter is not a
    // malformed filter and must not poison the row.
    let filter = row
        .filter_json
        .map(|raw| from_json(&raw, "reflection filter"))
        .transpose()?
        .filter(|value| !value.is_null());
    let cognition_filter = row
        .cognition_filter_json
        .map(|raw| from_json(&raw, "reflection cognition filter"))
        .transpose()?;
    let extensions = match from_json(&row.extensions_json, "reflection extensions")? {
        Value::Object(map) => map,
        _ => return Err(invalid_row("reflection extensions is not an object")),
    };
    let cost = match (row.cost_unit_kind, row.cost_units) {
        (Some(unit_kind), Some(units)) => Some((unit_kind, units)),
        (None, None) => None,
        _ => return Err(invalid_row("reflection cost pair is half set")),
    };
    Ok(MemoryReflectionRow {
        seq: row.seq,
        reflection_id: row.reflection_id,
        space_id: SpaceId::new(row.space_id),
        subject_id: SubjectId::new(row.subject_id),
        requester: CallerId::new(row.requester_principal_id),
        selection: from_json(&row.selection_json, "reflection selection")?,
        filter,
        cognition_filter,
        basis: from_json(&row.basis_json, "reflection basis")?,
        engine_policy_version: row.engine_policy_version,
        model: row.model,
        schema_version: row.schema_version,
        revision: row.revision.into(),
        extensions,
        created_at: row.created_at,
        body_text: row.body_text,
        body_format: row.body_format,
        cost,
        stale_cache: row.stale_cache != 0,
    })
}

struct ReflectionRow {
    seq: i64,
    reflection_id: String,
    space_id: String,
    subject_id: String,
    requester_principal_id: String,
    selection_json: String,
    filter_json: Option<String>,
    cognition_filter_json: Option<String>,
    basis_json: String,
    engine_policy_version: String,
    model: Option<String>,
    schema_version: i64,
    revision: i64,
    extensions_json: String,
    created_at: String,
    body_text: String,
    body_format: String,
    cost_unit_kind: Option<String>,
    cost_units: Option<f64>,
    stale_cache: i64,
}

/// Parses a persisted reflection selection strictly: unknown modes and malformed members
/// are `InvalidRow` rather than silently nonmatching or widened.
fn pinned_selection(selection: &Value) -> RepoResult<PlacementSelection> {
    match selection.get("mode").and_then(Value::as_str) {
        Some("subject_all") => Ok(PlacementSelection::SubjectAll),
        Some("shared_only") => Ok(PlacementSelection::SharedOnly),
        Some("selected_contexts") => {
            let ids = selection
                .get("context_ids")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid_row("selected_contexts misses context_ids array"))?
                .iter()
                .map(|id| {
                    id.as_str()
                        .map(ContextId::new)
                        .ok_or_else(|| invalid_row("selected_contexts entry is not a string"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let include_shared = match selection.get("include_shared") {
                None => false,
                Some(value) => value
                    .as_bool()
                    .ok_or_else(|| invalid_row("selected_contexts include_shared is not a bool"))?,
            };
            Ok(PlacementSelection::SelectedContexts {
                context_ids: ids,
                include_shared,
            })
        }
        other => Err(invalid_row(format_args!(
            "unknown reflection selection mode {other:?}"
        ))),
    }
}

/// §7.26 scope containment: does `requested` cover the reflection's pinned selection.
fn selection_covers(pinned: &PlacementSelection, requested: &PlacementSelection) -> bool {
    match (pinned, requested) {
        (_, PlacementSelection::SubjectAll)
        | (PlacementSelection::SharedOnly, PlacementSelection::SharedOnly) => true,
        (
            PlacementSelection::SharedOnly,
            PlacementSelection::SelectedContexts { include_shared, .. },
        ) => *include_shared,
        (
            PlacementSelection::SelectedContexts {
                context_ids,
                include_shared,
            },
            PlacementSelection::SelectedContexts {
                context_ids: requested_ids,
                include_shared: requested_shared,
            },
        ) => {
            context_ids.iter().all(|id| requested_ids.contains(id))
                && (!*include_shared || *requested_shared)
        }
        _ => false,
    }
}

fn journal_row(row: JournalRow) -> RepoResult<MemoryJournalRow> {
    let targets: Vec<Value> = serde_json::from_str(&row.targets_json)
        .map_err(|error| invalid_row(format_args!("targets_json: {error}")))?;
    let result = row
        .result_json
        .map(|raw| from_json(&raw, "journal result"))
        .transpose()?;
    Ok(MemoryJournalRow {
        seq: row.seq,
        receipt_id: row.receipt_id,
        space_id: SpaceId::new(row.space_id),
        subject_id: SubjectId::new(row.subject_id),
        requester: CallerId::new(row.requester_principal_id),
        operation: row.op,
        idempotency_key: row.idempotency_key,
        state: row.state,
        targets,
        result,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

struct JournalRow {
    seq: i64,
    receipt_id: String,
    space_id: String,
    subject_id: String,
    requester_principal_id: String,
    op: String,
    idempotency_key: Option<String>,
    state: String,
    targets_json: String,
    result_json: Option<String>,
    created_at: String,
    updated_at: String,
}

impl<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> for JournalRow {
    fn from_row(row: &'r sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        Ok(Self {
            seq: row.try_get("seq")?,
            receipt_id: row.try_get("receipt_id")?,
            space_id: row.try_get("space_id")?,
            subject_id: row.try_get("subject_id")?,
            requester_principal_id: row.try_get("requester_principal_id")?,
            op: row.try_get("op")?,
            idempotency_key: row.try_get("idempotency_key")?,
            state: row.try_get("state")?,
            targets_json: row.try_get("targets_json")?,
            result_json: row.try_get("result_json")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

/// One content-free `memory_receipts` projection for a whole-space export summary:
/// no result, error, lease or raw idempotency key is selected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryReceiptSummary {
    pub receipt_id: String,
    pub requester: String,
    pub operation: String,
    pub payload_digest: String,
    pub status: String,
    pub created_at: String,
}

/// One stored export header: the metadata a page read authorizes on, plus the
/// fixed expiration and artifact counts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryExportRow {
    pub export_id: String,
    pub space_id: String,
    pub requester: String,
    /// `None` for a whole-space export.
    pub subject_id: Option<String>,
    /// Canonical `ReadSelection` JSON; `None` is the subject scope's default.
    pub selection: Option<String>,
    pub include_tombstoned: bool,
    pub bundle_digest: String,
    pub chunk_count: u64,
    pub created_at: String,
    pub expires_at: String,
    /// Reclaimed: the payload is gone and a page read answers `EXPORT_EXPIRED`.
    pub expired: bool,
}

/// One staged import: the immutable header facts fixed by ordinal zero, the
/// progress the next accepted chunk advances, and the expiry that is never reset.
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryImportRow {
    pub import_id: String,
    pub space_id: String,
    pub requester: String,
    pub source_export_id: String,
    pub bundle_digest: String,
    pub expected_count: u64,
    pub next_ordinal: u64,
    pub conflict_policy: String,
    pub adopt_grants: Value,
    /// Ordinal-zero `reference_translations`; applied on activation, never on staging.
    pub reference_translations: Value,
    /// The translated rows final verification froze, present only for `staged`.
    pub staged_payload: Option<Value>,
    pub state: String,
    pub created_at: String,
    pub expires_at: String,
}

/// The borrowed-transaction repository. Zero-sized: the caller's transaction is the seam.
pub struct SqliteMemoryRepository;

impl SqliteMemoryRepository {
    /// The serving path's grant read: one principal's existing-space grants in
    /// `(space_id, grant_id)` order, decoded one row at a time, so peak memory is
    /// a row plus SQLx's delivery buffer rather than the whole grant history.
    pub async fn scan_principal_grants(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        principal: &str,
        mut visit: impl FnMut(MemoryGrantRow) + Send,
    ) -> RepoResult<()> {
        let mut rows = sqlx::query_as!(
            GrantRow,
            // why: the existing-space filter keeps an orphan grant (no space row,
            // unconstrained payload) out of the decode: no listing can show it.
            // `EXISTS` rather than `IN`: SQLite then reads the index once over
            // the principal and probes one space row per grant, instead of
            // probing the index once per stored space.
            "SELECT grant_id, space_id, subject_id, principal_id, \
             capabilities_json, context_ids_json, shared_access, expires_at, revoked_at, \
             revision, created_at FROM memory_grants \
             WHERE principal_id = ? \
             AND EXISTS (SELECT 1 FROM memory_spaces \
                         WHERE memory_spaces.space_id = memory_grants.space_id) \
             ORDER BY space_id, grant_id",
            principal,
        )
        .fetch(&mut **tx);
        while let Some(row) = poll_fn(|cx| rows.as_mut().poll_next(cx)).await {
            visit(grant_row(row.map_err(decode_row)?)?);
        }
        Ok(())
    }

    /// `(grant_id, revision)` for every grant of `principal` in the given spaces,
    /// expired and revoked included: the cursor marker invalidates on any
    /// visible-space grant change without the caller retaining grant rows.
    pub async fn principal_grant_revisions(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        principal: &str,
        visible_space_ids: &[&str],
    ) -> RepoResult<Vec<(String, i64)>> {
        let spaces = serde_json::to_string(visible_space_ids)
            .map_err(|error| invalid_row(format_args!("visible space ids: {error}")))?;
        sqlx::query_as::<_, (String, i64)>(
            // why: `+space_id` keeps the IN list off the index, so SQLite reads the
            // index once over the principal and tests the visible set there,
            // instead of probing the index once per visible space.
            "SELECT grant_id, revision FROM memory_grants \
             WHERE principal_id = ? AND +space_id IN (SELECT value FROM json_each(?)) \
             ORDER BY space_id, grant_id",
        )
        .bind(principal)
        .bind(spaces)
        .fetch_all(&mut **tx)
        .await
        .map_err(decode_row)
    }

    /// The journal row one receipt names, in whatever space it carries: the
    /// receipt id is globally unique, so the row under it is a fact the caller
    /// validates rather than filters away as absence.
    pub async fn journal_by_receipt(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        receipt_id: &str,
    ) -> RepoResult<Option<MemoryJournalRow>> {
        // why: runtime-checked rather than the compile-checked macro — this lookup
        // owns no offline query cache entry.
        sqlx::query_as::<_, JournalRow>(
            "SELECT seq, receipt_id, space_id, subject_id, requester_principal_id, \
             op, idempotency_key, state, targets_json, result_json, created_at, updated_at \
             FROM memory_journal WHERE receipt_id = ?",
        )
        .bind(receipt_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(decode_row)?
        .map(journal_row)
        .transpose()
    }

    /// Every subject row of one space, in stable id order.
    pub async fn list_subjects(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        space: &SpaceId,
    ) -> RepoResult<Vec<MemorySubjectRow>> {
        let rows = sqlx::query_as::<_, SubjectColumns>(
            "SELECT space_id, subject_id, kind_ns, kind_id, status, fenced_contexts_json, \
             revision, created_at FROM memory_subjects WHERE space_id = ? ORDER BY subject_id",
        )
        .bind(space.as_str())
        .fetch_all(&mut **tx)
        .await
        .map_err(decode_row)?;
        rows.into_iter().map(subject_columns_row).collect()
    }

    /// The space's receipt rows projected content-free, in stable id order.
    pub async fn receipt_summaries(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        space: &SpaceId,
    ) -> RepoResult<Vec<MemoryReceiptSummary>> {
        let rows = sqlx::query_as::<_, (String, String, String, String, String, String)>(
            "SELECT receipt_id, requester_principal_id, op, payload_digest, status, created_at \
             FROM memory_receipts WHERE space_id = ? ORDER BY receipt_id",
        )
        .bind(space.as_str())
        .fetch_all(&mut **tx)
        .await
        .map_err(decode_row)?;
        Ok(rows
            .into_iter()
            .map(
                |(receipt_id, requester, operation, payload_digest, status, created_at)| {
                    MemoryReceiptSummary {
                        receipt_id,
                        requester,
                        operation,
                        payload_digest,
                        status,
                        created_at,
                    }
                },
            )
            .collect())
    }

    /// One export header of one space, `Ok(None)` when the store never held it.
    pub async fn export(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        space: &SpaceId,
        export_id: &str,
    ) -> RepoResult<Option<MemoryExportRow>> {
        sqlx::query_as::<_, ExportColumns>(
            "SELECT export_id, space_id, requester_principal_id, subject_id, selection_json, \
             include_tombstoned, bundle_digest, chunk_count, created_at, expires_at, state \
             FROM memory_exports WHERE space_id = ? AND export_id = ?",
        )
        .bind(space.as_str())
        .bind(export_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(decode_row)?
        .map(export_row)
        .transpose()
    }

    /// One stored wire chunk of an export, exactly as it was published.
    pub async fn export_chunk(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        export_id: &str,
        ordinal: u64,
    ) -> RepoResult<Option<String>> {
        let ordinal =
            i64::try_from(ordinal).map_err(|_| invalid_row("chunk ordinal is out of range"))?;
        sqlx::query_scalar::<_, String>(
            "SELECT payload FROM memory_export_chunks WHERE export_id = ? AND ordinal = ?",
        )
        .bind(export_id)
        .bind(ordinal)
        .fetch_optional(&mut **tx)
        .await
        .map_err(decode_row)
    }

    /// Insert-only export header; the artifact's chunks follow under the same id.
    pub async fn insert_export(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        row: &MemoryExportRow,
    ) -> RepoResult<()> {
        let chunk_count = i64::try_from(row.chunk_count)
            .map_err(|_| invalid_row("export chunk_count is out of range"))?;
        sqlx::query(
            "INSERT INTO memory_exports \
             (export_id, space_id, requester_principal_id, subject_id, selection_json, \
              include_tombstoned, bundle_digest, chunk_count, created_at, expires_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&row.export_id)
        .bind(&row.space_id)
        .bind(&row.requester)
        .bind(&row.subject_id)
        .bind(&row.selection)
        .bind(i64::from(row.include_tombstoned))
        .bind(&row.bundle_digest)
        .bind(chunk_count)
        .bind(&row.created_at)
        .bind(&row.expires_at)
        .execute(&mut **tx)
        .await
        .map_err(|error| conflict(error, &row.export_id))?;
        Ok(())
    }

    /// Insert-only export chunk; one row per ordinal, never rewritten.
    pub async fn insert_export_chunk(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        export_id: &str,
        ordinal: u64,
        payload: &str,
    ) -> RepoResult<()> {
        let ordinal =
            i64::try_from(ordinal).map_err(|_| invalid_row("chunk ordinal is out of range"))?;
        sqlx::query(
            "INSERT INTO memory_export_chunks (export_id, ordinal, payload) VALUES (?, ?, ?)",
        )
        .bind(export_id)
        .bind(ordinal)
        .bind(payload)
        .execute(&mut **tx)
        .await
        .map_err(|error| conflict(error, format_args!("{export_id}.{ordinal}")))?;
        Ok(())
    }

    /// The one import holding a target space, requester and source export: the live one
    /// if there is one, else the latest reclaimed one. The partial index makes a live
    /// import single-valued, so a second idempotency key cannot open a second import;
    /// an `expired` row only names its own death, so a late chunk is refused with its
    /// expiry while a fresh header may still start the sequence again.
    pub async fn import_by_source(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        space: &SpaceId,
        requester: &str,
        source_export_id: &str,
    ) -> RepoResult<Option<MemoryImportRow>> {
        sqlx::query_as::<_, ImportColumns>(
            "SELECT import_id, space_id, requester_principal_id, source_export_id, \
             bundle_digest, expected_count, next_ordinal, conflict_policy, adopt_grants_json, \
             reference_translations_json, staged_payload_json, state, created_at, expires_at \
             FROM memory_imports \
             WHERE space_id = ? AND requester_principal_id = ? AND source_export_id = ? \
             AND state IN ('staging', 'staged', 'expired') \
             ORDER BY state = 'expired', created_at DESC",
        )
        .bind(space.as_str())
        .bind(requester)
        .bind(source_export_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(decode_row)?
        .map(import_row)
        .transpose()
    }

    /// Insert-only import header; ordinal zero owns it whole.
    pub async fn insert_import(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        row: &MemoryImportRow,
    ) -> RepoResult<()> {
        let expected_count = i64::try_from(row.expected_count)
            .map_err(|_| invalid_row("import expected_count is out of range"))?;
        let next_ordinal = i64::try_from(row.next_ordinal)
            .map_err(|_| invalid_row("import next_ordinal is out of range"))?;
        let adopt_grants = to_json(&row.adopt_grants, "import adopt_grants")?;
        let reference_translations =
            to_json(&row.reference_translations, "import reference_translations")?;
        sqlx::query(
            "INSERT INTO memory_imports \
             (import_id, space_id, requester_principal_id, source_export_id, bundle_digest, \
              expected_count, next_ordinal, conflict_policy, adopt_grants_json, \
              reference_translations_json, state, created_at, expires_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&row.import_id)
        .bind(&row.space_id)
        .bind(&row.requester)
        .bind(&row.source_export_id)
        .bind(&row.bundle_digest)
        .bind(expected_count)
        .bind(next_ordinal)
        .bind(&row.conflict_policy)
        .bind(adopt_grants)
        .bind(reference_translations)
        .bind(&row.state)
        .bind(&row.created_at)
        .bind(&row.expires_at)
        .execute(&mut **tx)
        .await
        .map_err(|error| conflict(error, &row.import_id))?;
        Ok(())
    }

    /// One stored staged chunk of an import, exactly as it arrived.
    pub async fn import_chunk(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        import_id: &str,
        ordinal: u64,
    ) -> RepoResult<Option<String>> {
        let ordinal =
            i64::try_from(ordinal).map_err(|_| invalid_row("chunk ordinal is out of range"))?;
        sqlx::query_scalar::<_, String>(
            "SELECT payload FROM memory_import_chunks WHERE import_id = ? AND ordinal = ?",
        )
        .bind(import_id)
        .bind(ordinal)
        .fetch_optional(&mut **tx)
        .await
        .map_err(decode_row)
    }

    /// Insert-only staged chunk; one row per ordinal, never rewritten.
    pub async fn insert_import_chunk(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        import_id: &str,
        ordinal: u64,
        payload: &str,
    ) -> RepoResult<()> {
        let ordinal =
            i64::try_from(ordinal).map_err(|_| invalid_row("chunk ordinal is out of range"))?;
        sqlx::query(
            "INSERT INTO memory_import_chunks (import_id, ordinal, payload) VALUES (?, ?, ?)",
        )
        .bind(import_id)
        .bind(ordinal)
        .bind(payload)
        .execute(&mut **tx)
        .await
        .map_err(|error| conflict(error, format_args!("{import_id}.{ordinal}")))?;
        Ok(())
    }

    /// Advance one staging import to its next ordinal. Only a non-final chunk calls
    /// this; the final chunk freezes the import through `finish_import`.
    pub async fn advance_import(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        import_id: &str,
        next_ordinal: u64,
    ) -> RepoResult<()> {
        let next_ordinal = i64::try_from(next_ordinal)
            .map_err(|_| invalid_row("import next_ordinal is out of range"))?;
        sqlx::query(
            "UPDATE memory_imports SET next_ordinal = ? \
             WHERE import_id = ? AND state = 'staging'",
        )
        .bind(next_ordinal)
        .bind(import_id)
        .execute(&mut **tx)
        .await
        .map_err(decode_row)?;
        Ok(())
    }

    /// One import header of one space by its id, whatever state it holds.
    pub async fn import(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        space: &SpaceId,
        import_id: &str,
    ) -> RepoResult<Option<MemoryImportRow>> {
        sqlx::query_as::<_, ImportColumns>(
            "SELECT import_id, space_id, requester_principal_id, source_export_id, \
             bundle_digest, expected_count, next_ordinal, conflict_policy, adopt_grants_json, \
             reference_translations_json, staged_payload_json, state, created_at, expires_at \
             FROM memory_imports \
             WHERE space_id = ? AND import_id = ?",
        )
        .bind(space.as_str())
        .bind(import_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(decode_row)?
        .map(import_row)
        .transpose()
    }

    /// Freeze the translated rows final verification built: the import leaves
    /// `staging` for `staged`, carrying the payload activation applies and the report
    /// that fixes the identity mapping. `false` means the row was not mid-staging.
    pub async fn finish_import(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        import_id: &str,
        staged_payload: &str,
        staged_report: &str,
    ) -> RepoResult<bool> {
        let updated = sqlx::query(
            "UPDATE memory_imports \
             SET state = 'staged', next_ordinal = expected_count, staged_payload_json = ?, \
             staged_report_json = ? \
             WHERE import_id = ? AND state = 'staging'",
        )
        .bind(staged_payload)
        .bind(staged_report)
        .bind(import_id)
        .execute(&mut **tx)
        .await
        .map_err(decode_row)?;
        Ok(updated.rows_affected() == 1)
    }

    /// One visibility flip: a `staged` import becomes `activated`, carrying the
    /// retained source provenance; the content-bearing payload it applied is released.
    /// `false` means the row had already left `staged`, so nothing is applied twice.
    pub async fn mark_import_activated(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        import_id: &str,
        activated_at: &str,
        provenance: &str,
    ) -> RepoResult<bool> {
        let updated = sqlx::query(
            "UPDATE memory_imports \
             SET state = 'activated', activated_at = ?, staged_payload_json = NULL, \
             provenance_json = ? \
             WHERE import_id = ? AND state = 'staged'",
        )
        .bind(activated_at)
        .bind(provenance)
        .bind(import_id)
        .execute(&mut **tx)
        .await
        .map_err(decode_row)?;
        Ok(updated.rows_affected() == 1)
    }

    /// Reclaim an import past its fixed expiry: content-free terminal state, with the
    /// content-bearing payload released, so its source-export key stops blocking a
    /// fresh import. `false` means it had already left the live states.
    pub async fn expire_import(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        import_id: &str,
    ) -> RepoResult<bool> {
        let updated = sqlx::query(
            "UPDATE memory_imports \
             SET state = 'expired', staged_payload_json = NULL, staged_report_json = NULL, \
             provenance_json = NULL \
             WHERE import_id = ? AND state IN ('staging', 'staged')",
        )
        .bind(import_id)
        .execute(&mut **tx)
        .await
        .map_err(decode_row)?;
        Ok(updated.rows_affected() == 1)
    }

    /// Reclaim every live import of one space past its fixed expiry: a content-free terminal
    /// state each, payload released, chunks deleted — so an abandoned import stops holding
    /// storage once any transfer write of that space succeeds, not only when its own source
    /// export is staged again.
    pub async fn expire_imports(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        space: &SpaceId,
        now: &str,
    ) -> RepoResult<()> {
        sqlx::query(
            "UPDATE memory_imports \
             SET state = 'expired', staged_payload_json = NULL, staged_report_json = NULL, \
             provenance_json = NULL \
             WHERE space_id = ? AND state IN ('staging', 'staged') AND expires_at <= ?",
        )
        .bind(space.as_str())
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(decode_row)?;
        sqlx::query(
            "DELETE FROM memory_import_chunks \
             WHERE import_id IN (SELECT import_id FROM memory_imports \
             WHERE space_id = ? AND state = 'expired')",
        )
        .bind(space.as_str())
        .execute(&mut **tx)
        .await
        .map_err(decode_row)?;
        Ok(())
    }

    /// Reclaim every export of one space past its fixed retention: the header is marked
    /// `expired` (so a later read answers `EXPORT_EXPIRED` from that state, not from a clock
    /// that may have rolled back), and the chunk payloads are deleted once any transfer write
    /// of that space succeeds.
    pub async fn expire_exports(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        space: &SpaceId,
        now: &str,
    ) -> RepoResult<()> {
        sqlx::query(
            "DELETE FROM memory_export_chunks \
             WHERE export_id IN (SELECT export_id FROM memory_exports \
             WHERE space_id = ? AND expires_at <= ?)",
        )
        .bind(space.as_str())
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(decode_row)?;
        sqlx::query(
            "UPDATE memory_exports SET state = 'expired' \
             WHERE space_id = ? AND state = 'live' AND expires_at <= ?",
        )
        .bind(space.as_str())
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(decode_row)?;
        Ok(())
    }

    /// Pre-activation rollback: only an import that never activated can be discarded,
    /// and the content-bearing payload it never applied is released with it.
    pub async fn mark_import_discarded(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        import_id: &str,
    ) -> RepoResult<bool> {
        let updated = sqlx::query(
            "UPDATE memory_imports \
             SET state = 'discarded', staged_payload_json = NULL, staged_report_json = NULL, \
             provenance_json = NULL \
             WHERE import_id = ? AND state IN ('staging', 'staged', 'expired')",
        )
        .bind(import_id)
        .execute(&mut **tx)
        .await
        .map_err(decode_row)?;
        Ok(updated.rows_affected() == 1)
    }

    /// Release every staged chunk of one import; activation and discard both own this.
    pub async fn delete_import_chunks(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        import_id: &str,
    ) -> RepoResult<()> {
        sqlx::query("DELETE FROM memory_import_chunks WHERE import_id = ?")
            .bind(import_id)
            .execute(&mut **tx)
            .await
            .map_err(decode_row)?;
        Ok(())
    }
}

#[async_trait]
impl MemoryRepository for SqliteMemoryRepository {
    type Tx<'a> = sqlx::Transaction<'a, Sqlite>;
    type Error = SqliteRepositoryError;

    async fn candidate_head(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> RepoResult<Option<MemoryCandidateHead>> {
        candidate_head_by_key(tx, space, id).await
    }

    async fn candidate(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> RepoResult<Option<MemoryCandidateIr>> {
        candidate_by_key(tx, space, id).await
    }

    async fn insert_candidate(
        &self,
        tx: &mut Self::Tx<'_>,
        candidate: &MemoryCandidateIr,
    ) -> RepoResult<()> {
        insert_candidate_row(tx, candidate).await
    }

    async fn record(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> RepoResult<Option<MemoryRecordIr>> {
        record_by_key(tx, space, id).await
    }

    async fn record_head(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> RepoResult<Option<MemoryRecordHead>> {
        record_head_by_key(tx, space, id).await
    }

    async fn review_receipt(
        &self,
        tx: &mut Self::Tx<'_>,
        key: &ReceiptKey,
    ) -> RepoResult<Option<ReceiptRead>> {
        sqlx::query_as!(
            ReceiptRow,
            "SELECT receipt_id, space_id, requester_principal_id, op, \
             idempotency_key, payload_digest, status, result_json, error_json, created_at, \
             completed_at FROM memory_receipts \
             WHERE space_id = ? AND requester_principal_id = ? AND op = ? \
             AND idempotency_key = ?",
            key.space.as_str(),
            key.requester.as_str(),
            &key.operation,
            &key.idempotency_key,
        )
        .fetch_optional(&mut **tx)
        .await
        .map_err(decode_row)?
        .map(receipt_read)
        .transpose()
    }

    async fn insert_review(
        &self,
        tx: &mut Self::Tx<'_>,
        record: &MemoryRecordIr,
        receipt: &ReviewReceiptIr,
    ) -> RepoResult<()> {
        insert_record_row(tx, record).await?;
        insert_receipt_row(tx, receipt, None).await
    }

    async fn recall(
        &self,
        tx: &mut Self::Tx<'_>,
        query: &RecallQuery,
    ) -> RepoResult<Vec<MemoryRecordIr>> {
        // why: `context: None` admits shared rows and every context; `Some` admits only rows
        // placed in that context (the legacy `context_id` predicate, restated over the
        // Nexus placement columns).
        sqlx::query_as!(
            RecordRow,
            "SELECT record_id, space_id, subject_id, kind, content_json, \
             scope_visibility, scope_context_id, provenance_json, import_ref_json, occurred_at, \
             tombstone_json, revision, created_at, updated_at FROM memory_records \
             WHERE space_id = ? AND subject_id = ? AND tombstone_json IS NULL \
             AND (? IS NULL OR scope_context_id = ?) \
             ORDER BY created_at DESC, record_id",
            query.space.as_str(),
            query.subject.as_str(),
            query
                .context
                .as_ref()
                .map(|context| context.as_str().to_string()),
            query
                .context
                .as_ref()
                .map(|context| context.as_str().to_string()),
        )
        .fetch_all(&mut **tx)
        .await
        .map_err(decode_row)?
        .into_iter()
        .map(record_ir)
        .collect()
    }

    async fn space(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
    ) -> RepoResult<Option<MemorySpaceRow>> {
        let row = sqlx::query_as!(
            SpaceRow,
            "SELECT s.space_id, s.display_name, s.authority_host_id, \
             s.authority_generation, s.status, s.revision, s.created_at, \
             (SELECT COUNT(*) FROM memory_receipts r WHERE r.space_id = s.space_id \
              AND r.status = 'pending') AS pending_receipts \
             FROM memory_spaces s WHERE s.space_id = ?",
            space.as_str(),
        )
        .fetch_optional(&mut **tx)
        .await
        .map_err(decode_row)?;
        Ok(row.map(space_row).transpose()?)
    }

    async fn list_spaces(&self, tx: &mut Self::Tx<'_>) -> RepoResult<Vec<MemorySpaceRow>> {
        sqlx::query_as!(
            SpaceRow,
            "SELECT s.space_id, s.display_name, s.authority_host_id, \
             s.authority_generation, s.status, s.revision, s.created_at, \
             (SELECT COUNT(*) FROM memory_receipts r WHERE r.space_id = s.space_id \
              AND r.status = 'pending') AS pending_receipts \
             FROM memory_spaces s ORDER BY s.space_id",
        )
        .fetch_all(&mut **tx)
        .await
        .map_err(decode_row)
        .and_then(|rows| rows.into_iter().map(space_row).collect())
    }

    async fn update_space(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemorySpaceRow,
        observed_revision: &Number,
    ) -> RepoResult<bool> {
        let updated = sqlx::query!(
            "UPDATE memory_spaces SET display_name = ?, authority_host_id = ?, \
             authority_generation = ?, status = ?, revision = ? \
             WHERE space_id = ? AND revision = ?",
            &row.display_name,
            &row.authority_host_id,
            row.authority_generation,
            &row.status,
            revision_i64(&row.revision)?,
            row.space_id.as_str(),
            revision_i64(observed_revision)?,
        )
        .execute(&mut **tx)
        .await?;
        Ok(updated.rows_affected() == 1)
    }

    async fn subject(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> RepoResult<Option<MemorySubjectRow>> {
        sqlx::query_as!(
            SubjectRow,
            "SELECT space_id, subject_id, kind_ns, kind_id, status, \
             fenced_contexts_json, revision, created_at FROM memory_subjects \
             WHERE space_id = ? AND subject_id = ?",
            space.as_str(),
            id,
        )
        .fetch_optional(&mut **tx)
        .await
        .map_err(decode_row)?
        .map(subject_row)
        .transpose()
    }

    async fn subject_by_kind(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        namespace: &str,
        kind: &str,
    ) -> RepoResult<Option<MemorySubjectRow>> {
        sqlx::query_as!(
            SubjectRow,
            "SELECT space_id, subject_id, kind_ns, kind_id, status, \
             fenced_contexts_json, revision, created_at FROM memory_subjects \
             WHERE space_id = ? AND kind_ns = ? AND kind_id = ? \
             ORDER BY subject_id LIMIT 1",
            space.as_str(),
            namespace,
            kind,
        )
        .fetch_optional(&mut **tx)
        .await
        .map_err(decode_row)?
        .map(subject_row)
        .transpose()
    }

    async fn insert_subject(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemorySubjectRow,
    ) -> RepoResult<()> {
        let (kind_ns, kind_id) = match &row.kind {
            Some((namespace, kind)) => (Some(namespace.clone()), Some(kind.clone())),
            None => (None, None),
        };
        let fenced = serde_json::to_string(
            &row.fenced_contexts
                .iter()
                .map(|context| context.as_str().to_string())
                .collect::<Vec<_>>(),
        )
        .map_err(|error| invalid_row(format_args!("fenced_contexts: {error}")))?;
        sqlx::query!(
            "INSERT INTO memory_subjects \
             (space_id, subject_id, kind_ns, kind_id, status, fenced_contexts_json, \
              revision, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            row.space_id.as_str(),
            row.subject_id.as_str(),
            kind_ns,
            kind_id,
            &row.status,
            &fenced,
            revision_i64(&row.revision)?,
            &row.created_at,
        )
        .execute(&mut **tx)
        .await
        .map_err(|error| conflict(error, format_args!("{}.{}", row.space_id, row.subject_id)))?;
        Ok(())
    }

    async fn update_subject(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemorySubjectRow,
        observed_revision: &Number,
    ) -> RepoResult<bool> {
        let (kind_ns, kind_id) = match &row.kind {
            Some((namespace, kind)) => (Some(namespace.clone()), Some(kind.clone())),
            None => (None, None),
        };
        let fenced = serde_json::to_string(
            &row.fenced_contexts
                .iter()
                .map(|context| context.as_str().to_string())
                .collect::<Vec<_>>(),
        )
        .map_err(|error| invalid_row(format_args!("fenced_contexts: {error}")))?;
        let updated = sqlx::query!(
            "UPDATE memory_subjects SET kind_ns = ?, kind_id = ?, status = ?, \
             fenced_contexts_json = ?, revision = ? \
             WHERE space_id = ? AND subject_id = ? AND revision = ?",
            kind_ns,
            kind_id,
            &row.status,
            &fenced,
            revision_i64(&row.revision)?,
            row.space_id.as_str(),
            row.subject_id.as_str(),
            revision_i64(observed_revision)?,
        )
        .execute(&mut **tx)
        .await?;
        Ok(updated.rows_affected() == 1)
    }

    async fn list_grants(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        principal_filter: Option<&str>,
    ) -> RepoResult<Vec<MemoryGrantRow>> {
        sqlx::query_as!(
            GrantRow,
            "SELECT grant_id, space_id, subject_id, principal_id, \
             capabilities_json, context_ids_json, shared_access, expires_at, revoked_at, \
             revision, created_at FROM memory_grants \
             WHERE space_id = ? AND (? IS NULL OR principal_id = ?) ORDER BY grant_id",
            space.as_str(),
            principal_filter,
            principal_filter,
        )
        .fetch_all(&mut **tx)
        .await
        .map_err(decode_row)?
        .into_iter()
        .map(grant_row)
        .collect()
    }

    async fn principal_grants(
        &self,
        tx: &mut Self::Tx<'_>,
        principal: &str,
    ) -> RepoResult<Vec<MemoryGrantRow>> {
        let mut grants = Vec::new();
        self.scan_principal_grants(tx, principal, |grant| grants.push(grant))
            .await?;
        Ok(grants)
    }

    async fn grant(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> RepoResult<Option<MemoryGrantRow>> {
        sqlx::query_as!(
            GrantRow,
            "SELECT grant_id, space_id, subject_id, principal_id, \
             capabilities_json, context_ids_json, shared_access, expires_at, revoked_at, \
             revision, created_at FROM memory_grants \
             WHERE space_id = ? AND grant_id = ?",
            space.as_str(),
            id,
        )
        .fetch_optional(&mut **tx)
        .await
        .map_err(decode_row)?
        .map(grant_row)
        .transpose()
    }

    async fn insert_grant(&self, tx: &mut Self::Tx<'_>, row: &MemoryGrantRow) -> RepoResult<()> {
        let capabilities = serde_json::to_string(&row.capabilities)
            .map_err(|error| invalid_row(format_args!("capabilities: {error}")))?;
        let contexts = serde_json::to_string(
            &row.context_ids
                .iter()
                .map(|context| context.as_str().to_string())
                .collect::<Vec<_>>(),
        )
        .map_err(|error| invalid_row(format_args!("context_ids: {error}")))?;
        sqlx::query!(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, \
              context_ids_json, shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            &row.grant_id,
            row.space_id.as_str(),
            row.subject_id.as_ref().map(|id| id.as_str().to_string()),
            row.principal_id.as_str(),
            &capabilities,
            &contexts,
            i64::from(row.shared_access),
            &row.expires_at,
            &row.revoked_at,
            revision_i64(&row.revision)?,
            &row.created_at,
        )
        .execute(&mut **tx)
        .await
        .map_err(|error| conflict(error, &row.grant_id))?;
        Ok(())
    }

    async fn update_grant(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryGrantRow,
        observed_revision: &Number,
    ) -> RepoResult<bool> {
        let capabilities = serde_json::to_string(&row.capabilities)
            .map_err(|error| invalid_row(format_args!("capabilities: {error}")))?;
        let contexts = serde_json::to_string(
            &row.context_ids
                .iter()
                .map(|context| context.as_str().to_string())
                .collect::<Vec<_>>(),
        )
        .map_err(|error| invalid_row(format_args!("context_ids: {error}")))?;
        let updated = sqlx::query!(
            "UPDATE memory_grants SET subject_id = ?, principal_id = ?, capabilities_json = ?, \
             context_ids_json = ?, shared_access = ?, expires_at = ?, revoked_at = ?, \
             revision = ? \
             WHERE grant_id = ? AND space_id = ? AND revision = ?",
            row.subject_id.as_ref().map(|id| id.as_str().to_string()),
            row.principal_id.as_str(),
            &capabilities,
            &contexts,
            i64::from(row.shared_access),
            &row.expires_at,
            &row.revoked_at,
            revision_i64(&row.revision)?,
            &row.grant_id,
            row.space_id.as_str(),
            revision_i64(observed_revision)?,
        )
        .execute(&mut **tx)
        .await?;
        Ok(updated.rows_affected() == 1)
    }

    async fn list_candidates(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        subject: &str,
        selection: &CandidateSelection,
    ) -> RepoResult<Vec<MemoryCandidateIr>> {
        let (all, shared_only, contexts) = placement_predicate(&selection.placement)?;
        // why: one fixed statement covers the admitted-placement superset plus the
        // optional status: the flags and the json_each list are parameters, so
        // prepare checks this read like every point query.
        sqlx::query_as!(
            CandidateRow,
            "SELECT candidate_id, space_id, subject_id, capture_visibility, \
             capture_context_id, kind, content_json, provenance_json, classification_json, status, \
             consumed_by_json, content_digest, occurred_at, expires_at, revision, created_at \
             FROM memory_candidates \
             WHERE space_id = ? AND subject_id = ? \
             AND (? OR (? AND capture_visibility = 'shared') \
                  OR capture_context_id IN (SELECT value FROM json_each(?))) \
             AND (? IS NULL OR status = ?) \
             ORDER BY created_at, candidate_id",
            space.as_str(),
            subject,
            all,
            shared_only,
            contexts,
            selection.status,
            selection.status,
        )
        .fetch_all(&mut **tx)
        .await
        .map_err(decode_row)?
        .into_iter()
        .map(candidate_ir)
        .collect()
    }

    async fn update_candidate(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryCandidateIr,
        observed_revision: &Number,
    ) -> RepoResult<bool> {
        let columns = candidate_columns(row)?;
        // why: a consume without `proposed_kind` keeps the stored kind (C-2 policy in
        // `insert_candidate_row`); a miss means the guarded update cannot match.
        let kind = match &columns.kind {
            Some(kind) => kind.clone(),
            None => {
                let Some(stored) = sqlx::query_scalar!(
                    "SELECT kind FROM memory_candidates WHERE space_id = ? AND candidate_id = ?",
                    &row.space_id,
                    &row.candidate_id,
                )
                .fetch_optional(&mut **tx)
                .await?
                else {
                    return Ok(false);
                };
                stored
            }
        };
        let (visibility, context_id) = placement_columns(&row.capture_scope);
        let updated = sqlx::query!(
            "UPDATE memory_candidates SET subject_id = ?, capture_visibility = ?, \
             capture_context_id = ?, kind = ?, content_json = ?, provenance_json = ?, \
             classification_json = ?, status = ?, consumed_by_json = ?, content_digest = ?, \
             occurred_at = ?, expires_at = ?, revision = ? \
             WHERE space_id = ? AND candidate_id = ? AND revision = ?",
            &row.subject_id,
            visibility,
            &context_id,
            &kind,
            &columns.content,
            &columns.provenance,
            &columns.classification,
            columns.status,
            &columns.consumed_by,
            &columns.content_digest,
            &row.occurred_at,
            &columns.expires_at,
            revision_i64(&row.revision)?,
            &row.space_id,
            &row.candidate_id,
            revision_i64(observed_revision)?,
        )
        .execute(&mut **tx)
        .await?;
        Ok(updated.rows_affected() == 1)
    }

    async fn list_records(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        subject: &str,
        selection: &RecordSelection,
    ) -> RepoResult<Vec<MemoryRecordIr>> {
        let (all, shared_only, contexts) = placement_predicate(&selection.placement)?;
        let kinds = to_json(&json!(selection.kinds), "record kinds")?;
        let provenance = to_json(&json!(selection.provenance_filter), "provenance filter")?;
        // why: the variable-width kind/provenance lists arrive as json_each
        // parameters instead of spliced placeholders: an empty kind list
        // selects every kind, an absent provenance filter selects every row
        // and an empty one selects none — the same predicate, now checked.
        sqlx::query_as!(
            RecordRow,
            "SELECT record_id, space_id, subject_id, kind, content_json, \
             scope_visibility, scope_context_id, provenance_json, import_ref_json, occurred_at, \
             tombstone_json, revision, created_at, updated_at \
             FROM memory_records \
             WHERE space_id = ? AND subject_id = ? \
             AND (? OR tombstone_json IS NULL) \
             AND (json_array_length(?) = 0 \
                  OR kind IN (SELECT value FROM json_each(?))) \
             AND (? OR (? AND scope_visibility = 'shared') \
                  OR scope_context_id IN (SELECT value FROM json_each(?))) \
             AND (? OR EXISTS (SELECT 1 FROM \
                  json_each(json_extract(provenance_json, '$.source_context_ids')) AS source \
                  JOIN json_each(?) AS f ON f.value = source.value)) \
             ORDER BY created_at DESC, record_id",
            space.as_str(),
            subject,
            selection.include_tombstoned,
            kinds,
            kinds,
            all,
            shared_only,
            contexts,
            selection.provenance_filter.is_none(),
            provenance,
        )
        .fetch_all(&mut **tx)
        .await
        .map_err(decode_row)?
        .into_iter()
        .map(record_ir)
        .collect()
    }

    async fn insert_record(&self, tx: &mut Self::Tx<'_>, row: &MemoryRecordIr) -> RepoResult<()> {
        insert_record_row(tx, row).await
    }

    async fn update_record(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryRecordIr,
        observed_revision: &Number,
    ) -> RepoResult<bool> {
        let columns = record_columns(row)?;
        let (visibility, context_id) = placement_columns(&row.scope);
        let updated = sqlx::query!(
            "UPDATE memory_records SET subject_id = ?, kind = ?, content_json = ?, \
             scope_visibility = ?, scope_context_id = ?, provenance_json = ?, \
             import_ref_json = ?, occurred_at = ?, tombstone_json = ?, updated_at = ?, \
             revision = ? \
             WHERE space_id = ? AND record_id = ? AND revision = ?",
            &row.subject_id,
            &row.kind,
            &columns.content,
            visibility,
            &context_id,
            &columns.provenance,
            &columns.import_ref,
            &row.occurred_at,
            &columns.tombstone,
            &row.updated_at,
            revision_i64(&row.revision)?,
            &row.space_id,
            &row.record_id,
            revision_i64(observed_revision)?,
        )
        .execute(&mut **tx)
        .await?;
        Ok(updated.rows_affected() == 1)
    }

    async fn receipt(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> RepoResult<Option<ReceiptRead>> {
        sqlx::query_as!(
            ReceiptRow,
            "SELECT receipt_id, space_id, requester_principal_id, op, \
             idempotency_key, payload_digest, status, result_json, error_json, created_at, \
             completed_at FROM memory_receipts \
             WHERE space_id = ? AND receipt_id = ?",
            space.as_str(),
            id,
        )
        .fetch_optional(&mut **tx)
        .await
        .map_err(decode_row)?
        .map(receipt_read)
        .transpose()
    }

    async fn insert_receipt(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &ReviewReceiptIr,
        lease: Option<&ReceiptLease>,
    ) -> RepoResult<()> {
        insert_receipt_row(tx, row, lease).await
    }

    async fn update_receipt(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &ReviewReceiptIr,
        observed_state: &str,
        observed_owner: Option<&str>,
    ) -> RepoResult<bool> {
        let columns = receipt_columns(row)?;
        // invariant: owner matching is exact — only the persisted lease owner settles a pending row,
        // and a `NULL` observed owner matches an unleased (`NULL`) owner exactly.
        let updated = sqlx::query!(
            "UPDATE memory_receipts SET status = ?, result_json = ?, error_json = ?, \
             completed_at = ? \
             WHERE receipt_id = ? AND space_id = ? AND status = ? AND owner_id IS ?",
            columns.status,
            &columns.result,
            &columns.error,
            &columns.completed_at,
            &row.receipt_id,
            &row.space_id,
            observed_state,
            observed_owner,
        )
        .execute(&mut **tx)
        .await?;
        Ok(updated.rows_affected() == 1)
    }

    async fn claim_receipt_lease(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
        owner_id: &str,
        lease_seconds: i64,
    ) -> RepoResult<bool> {
        // contract §7.0.3 lease takeover — the claim stamps a fresh owner and expiry, guarded by the
        // persisted lease instant already having passed, so a lost claim races a live or
        // re-leased row rather than overwriting it. The expiry is measured from the claim
        // instant — never the request instant — so the stamped lease always outlives this
        // transaction's own probe. Stamps keep the same `%Y-%m-%dT%H:%M:%S%.3fZ` spelling
        // `policy::timestamp` writes, so text order is instant order.
        let now = Utc::now();
        let expires_at = (now + chrono::Duration::seconds(lease_seconds))
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string();
        let now = now.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        let claimed = sqlx::query!(
            "UPDATE memory_receipts SET owner_id = ?, lease_expires_at = ? \
             WHERE receipt_id = ? AND space_id = ? AND status = 'pending' \
             AND lease_expires_at <= ?",
            owner_id,
            &expires_at,
            id,
            space.as_str(),
            &now,
        )
        .execute(&mut **tx)
        .await?;
        Ok(claimed.rows_affected() == 1)
    }

    async fn receipt_lease(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> RepoResult<Option<String>> {
        sqlx::query_scalar!(
            "SELECT lease_expires_at FROM memory_receipts \
             WHERE space_id = ? AND receipt_id = ?",
            space.as_str(),
            id,
        )
        .fetch_optional(&mut **tx)
        .await
        .map(Option::flatten)
        .map_err(Into::into)
    }

    async fn list_profiles(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        subject: &str,
        kinds: &[String],
    ) -> RepoResult<Vec<MemoryProfileRow>> {
        let kinds = to_json(&json!(kinds), "profile kinds")?;
        sqlx::query_as!(
            ProfileRow,
            "SELECT space_id, subject_id, profile_kind, document_json, updated_by, \
             revision, created_at, updated_at FROM memory_profiles \
             WHERE space_id = ? AND subject_id = ? \
             AND (json_array_length(?) = 0 \
                  OR profile_kind IN (SELECT value FROM json_each(?))) \
             ORDER BY profile_kind",
            space.as_str(),
            subject,
            kinds,
            kinds,
        )
        .fetch_all(&mut **tx)
        .await
        .map_err(decode_row)?
        .into_iter()
        .map(profile_ir)
        .collect()
    }

    async fn insert_profile(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryProfileRow,
    ) -> RepoResult<()> {
        sqlx::query!(
            "INSERT INTO memory_profiles \
             (space_id, subject_id, profile_kind, document_json, updated_by, revision, \
              created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            row.space_id.as_str(),
            row.subject_id.as_str(),
            &row.profile_kind,
            to_json(&row.document, "profile document")?,
            row.updated_by.as_str(),
            revision_i64(&row.revision)?,
            &row.created_at,
            &row.updated_at,
        )
        .execute(&mut **tx)
        .await
        .map_err(|error| {
            conflict(
                error,
                format_args!("{}.{}.{}", row.space_id, row.subject_id, row.profile_kind),
            )
        })?;
        Ok(())
    }

    async fn update_profile(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryProfileRow,
        observed_revision: &Number,
    ) -> RepoResult<bool> {
        let updated = sqlx::query!(
            "UPDATE memory_profiles SET document_json = ?, updated_by = ?, updated_at = ?, \
             revision = ? \
             WHERE space_id = ? AND subject_id = ? AND profile_kind = ? AND revision = ?",
            to_json(&row.document, "profile document")?,
            row.updated_by.as_str(),
            &row.updated_at,
            revision_i64(&row.revision)?,
            row.space_id.as_str(),
            row.subject_id.as_str(),
            &row.profile_kind,
            revision_i64(observed_revision)?,
        )
        .execute(&mut **tx)
        .await?;
        Ok(updated.rows_affected() == 1)
    }

    async fn list_reflections(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        subject: &str,
        selection: Option<&PlacementSelection>,
    ) -> RepoResult<Vec<MemoryReflectionRow>> {
        let rows = sqlx::query_as!(
            ReflectionRow,
            "SELECT seq, reflection_id, space_id, subject_id, \
             requester_principal_id, selection_json, filter_json, cognition_filter_json, \
             basis_json, engine_policy_version, model, schema_version, revision, \
             extensions_json, created_at, body_text, body_format, cost_unit_kind, cost_units, \
             stale_cache FROM memory_reflections \
             WHERE space_id = ? AND subject_id = ? ORDER BY seq DESC",
            space.as_str(),
            subject,
        )
        .fetch_all(&mut **tx)
        .await
        .map_err(decode_row)?;
        let mut reflections = Vec::with_capacity(rows.len());
        for row in rows {
            let reflection = reflection_row(row)?;
            // invariant: the pinned selection is validated on every read, filter or not.
            let pinned = pinned_selection(&reflection.selection)?;
            if let Some(scope) = selection {
                if !selection_covers(&pinned, scope) {
                    continue;
                }
            }
            reflections.push(reflection);
        }
        Ok(reflections)
    }

    async fn insert_reflection(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryReflectionRow,
    ) -> RepoResult<()> {
        let (cost_unit_kind, cost_units) = match &row.cost {
            Some((unit_kind, units)) => (Some(unit_kind.clone()), Some(*units)),
            None => (None, None),
        };
        sqlx::query!(
            "INSERT INTO memory_reflections \
             (seq, reflection_id, space_id, subject_id, requester_principal_id, \
              selection_json, filter_json, cognition_filter_json, basis_json, \
              engine_policy_version, model, schema_version, revision, extensions_json, \
              created_at, body_text, body_format, cost_unit_kind, cost_units, stale_cache) \
             VALUES (\
               (SELECT COALESCE(MAX(seq), 0) + 1 FROM memory_reflections), \
               ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            &row.reflection_id,
            row.space_id.as_str(),
            row.subject_id.as_str(),
            row.requester.as_str(),
            to_json(&row.selection, "reflection selection")?,
            row.filter
                .as_ref()
                .map(|value| to_json(value, "reflection filter"))
                .transpose()?,
            row.cognition_filter
                .as_ref()
                .map(|value| to_json(value, "reflection cognition filter"))
                .transpose()?,
            to_json(&row.basis, "reflection basis")?,
            &row.engine_policy_version,
            &row.model,
            row.schema_version,
            revision_i64(&row.revision)?,
            to_json(
                &Value::Object(row.extensions.clone()),
                "reflection extensions",
            )?,
            &row.created_at,
            &row.body_text,
            &row.body_format,
            cost_unit_kind,
            cost_units,
            i64::from(row.stale_cache),
        )
        .execute(&mut **tx)
        .await
        .map_err(|error| conflict(error, &row.reflection_id))?;
        Ok(())
    }

    async fn journal_intent(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        receipt_id: &str,
    ) -> RepoResult<Option<MemoryJournalRow>> {
        sqlx::query_as!(
            JournalRow,
            "SELECT seq AS \"seq!\", receipt_id, space_id, subject_id, requester_principal_id, \
             op, idempotency_key, state, targets_json, result_json, created_at, updated_at \
             FROM memory_journal WHERE space_id = ? AND receipt_id = ?",
            space.as_str(),
            receipt_id,
        )
        .fetch_optional(&mut **tx)
        .await
        .map_err(decode_row)?
        .map(journal_row)
        .transpose()
    }

    async fn has_unsettled_intent(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        subject: &SubjectId,
    ) -> RepoResult<bool> {
        // why: existence only — the literal `state <> 'applied'` predicate matches the
        // partial index `idx_memory_journal_unsettled` and no payload column is
        // selected, so an unreadable `targets_json`/`result_json` cannot mask
        // the withhold.
        Ok(sqlx::query_scalar!(
            "SELECT 1 FROM memory_journal \
             WHERE space_id = ? AND subject_id = ? AND state <> 'applied' \
             LIMIT 1",
            space.as_str(),
            subject.as_str(),
        )
        .fetch_optional(&mut **tx)
        .await
        .map_err(decode_row)?
        .is_some())
    }

    async fn insert_journal_intent(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryJournalRow,
    ) -> RepoResult<()> {
        sqlx::query!(
            "INSERT INTO memory_journal \
             (seq, receipt_id, space_id, subject_id, requester_principal_id, op, \
              idempotency_key, state, targets_json, result_json, created_at, updated_at) \
             VALUES ((SELECT COALESCE(MAX(seq), 0) + 1 FROM memory_journal), \
                     ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            &row.receipt_id,
            row.space_id.as_str(),
            row.subject_id.as_str(),
            row.requester.as_str(),
            &row.operation,
            &row.idempotency_key,
            &row.state,
            to_json(&Value::Array(row.targets.clone()), "journal targets")?,
            row.result
                .as_ref()
                .map(|value| to_json(value, "journal result"))
                .transpose()?,
            &row.created_at,
            &row.updated_at,
        )
        .execute(&mut **tx)
        .await
        .map_err(|error| conflict(error, &row.receipt_id))?;
        Ok(())
    }

    async fn update_journal_intent(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryJournalRow,
        observed_state: &str,
    ) -> RepoResult<bool> {
        let updated = sqlx::query!(
            "UPDATE memory_journal SET state = ?, targets_json = ?, result_json = ?, \
             updated_at = ? \
             WHERE space_id = ? AND receipt_id = ? AND state = ?",
            &row.state,
            to_json(&Value::Array(row.targets.clone()), "journal targets")?,
            row.result
                .as_ref()
                .map(|value| to_json(value, "journal result"))
                .transpose()?,
            &row.updated_at,
            row.space_id.as_str(),
            &row.receipt_id,
            observed_state,
        )
        .execute(&mut **tx)
        .await?;
        Ok(updated.rows_affected() == 1)
    }

    async fn provider_meta(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        key: &str,
    ) -> RepoResult<Option<MemoryProviderMetaRow>> {
        // why: keys are caller-composed and already carry their space; `space` scopes the
        // caller's intent, the table keys on `key` alone.
        let _ = space;
        sqlx::query_as!(
            MemoryProviderMetaRow,
            "SELECT key, value, updated_at FROM memory_provider_meta WHERE key = ?",
            key,
        )
        .fetch_optional(&mut **tx)
        .await
        .map_err(decode_row)
    }

    async fn put_provider_meta(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryProviderMetaRow,
    ) -> RepoResult<()> {
        sqlx::query!(
            "INSERT INTO memory_provider_meta (key, value, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, \
             updated_at = excluded.updated_at",
            &row.key,
            &row.value,
            &row.updated_at,
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn delete_provider_meta(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        key: &str,
    ) -> RepoResult<bool> {
        let _ = space;
        let deleted = sqlx::query!("DELETE FROM memory_provider_meta WHERE key = ?", key,)
            .execute(&mut **tx)
            .await?;
        Ok(deleted.rows_affected() == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIME: &str = "2026-11-20T09:20:01.123Z";

    fn pending_row(content_json: Option<&str>) -> CandidateRow {
        CandidateRow {
            candidate_id: "cand_1".into(),
            space_id: "spc_workshop".into(),
            subject_id: "sub_mira".into(),
            capture_visibility: "shared".into(),
            capture_context_id: None,
            kind: "local_note".into(),
            content_json: content_json.map(str::to_string),
            provenance_json: "{}".into(),
            classification_json: "{}".into(),
            status: "pending".into(),
            consumed_by_json: None,
            content_digest: None,
            occurred_at: None,
            expires_at: None,
            revision: 1,
            created_at: TIME.into(),
        }
    }

    // invariant: BUG-02 — the reader-side contract is that a pending row whose `content_json` is SQL
    // NULL (or JSON null) cannot decode; the writer guard that mirrors it is defensive.
    #[test]
    fn pending_candidate_content_refusal_is_defensive_reader_contract() {
        for content in [None, Some("null")] {
            assert!(matches!(
                candidate_ir(pending_row(content)),
                Err(SqliteRepositoryError::InvalidRow(_))
            ));
        }
    }

    // why: a count that cannot be a `u64` is a corrupt row, not a saturated one.
    #[test]
    fn space_row_rejects_negative_pending_receipts() {
        let row = SpaceRow {
            space_id: "spc_workshop".into(),
            display_name: None,
            authority_host_id: "host-1".into(),
            authority_generation: 1,
            status: "active".into(),
            revision: 1,
            created_at: TIME.into(),
            pending_receipts: -1,
        };
        assert!(matches!(
            space_row(row),
            Err(SqliteRepositoryError::InvalidRow(_))
        ));
    }
}
