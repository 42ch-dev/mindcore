//! `MemoryRepository` over a caller-owned transaction against the Nexus-shaped memory schema.
//!
//! Every statement reborrows the caller's `&mut Transaction`; this module never acquires a
//! connection, opens a pool, or begins/commits/rolls back a transaction. Rows decode through
//! the wire codec's JSON shapes, so invalid persisted payloads surface as
//! [`SqliteRepositoryError::InvalidRow`], and `extensions` on candidates, records and
//! receipts is reconstructed empty (the pinned schema has no column for it; see
//! `task-1-schema-map.md` §3 D-1).

use async_trait::async_trait;
use serde_json::{json, Map, Number, Value};
use sqlx::sqlite::SqliteRow;
use sqlx::{AssertSqlSafe, Row, Sqlite};

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::{ContextId, SpaceId, SubjectId};
use mindcore_base::ir::wire::{
    memory_candidate_from_wire, memory_candidate_to_wire, memory_record_from_wire,
    memory_record_to_wire, review_receipt_from_wire, review_receipt_to_wire, WireVersion,
};
use mindcore_base::ir::{
    CandidateSelection, CandidateState, MemoryCandidateIr, MemoryGrantRow, MemoryJournalRow,
    MemoryProfileRow, MemoryProviderMetaRow, MemoryRecordIr, MemoryReflectionRow, MemoryRepository,
    MemorySpaceRow, MemorySubjectRow, Placement, PlacementSelection, ReceiptKey, ReceiptLease,
    ReceiptRead, RecordSelection, ReviewReceiptIr,
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

/// Reads one persisted column, mapping a missing column or decode/type failure to
/// `InvalidRow` instead of the panicking `Row::get`.
fn column<'a, T>(row: &'a SqliteRow, name: &'static str) -> RepoResult<T>
where
    T: sqlx::Decode<'a, Sqlite> + sqlx::Type<Sqlite>,
{
    row.try_get::<T, _>(name)
        .map_err(|error| invalid_row(format_args!("column {name}: {error}")))
}

// ── Candidate ───────────────────────────────────────────────────────────────

const CANDIDATE_COLUMNS: &str = "candidate_id, space_id, subject_id, capture_visibility, \
     capture_context_id, kind, content_json, provenance_json, classification_json, status, \
     consumed_by_json, content_digest, occurred_at, expires_at, revision, created_at";

fn candidate_ir(row: &SqliteRow) -> RepoResult<MemoryCandidateIr> {
    let status: String = column(row, "status")?;
    let mut proposed = Map::new();
    // A consumed row with an empty stored kind has no representable proposal;
    // the consumed codec accepts `proposed` absent.
    let kind: String = column(row, "kind")?;
    if !kind.is_empty() {
        proposed.insert("kind".into(), kind.into());
    }
    let mut wire = Map::new();
    wire.insert("schema_version".into(), 1.into());
    wire.insert(
        "candidate_id".into(),
        column::<String>(row, "candidate_id")?.into(),
    );
    wire.insert("space_id".into(), column::<String>(row, "space_id")?.into());
    wire.insert(
        "subject_id".into(),
        column::<String>(row, "subject_id")?.into(),
    );
    wire.insert("capture_scope".into(), placement_wire(row)?);
    wire.insert(
        "classification".into(),
        from_json(
            &column::<String>(row, "classification_json")?,
            "classification",
        )?,
    );
    wire.insert(
        "provenance".into(),
        from_json(&column::<String>(row, "provenance_json")?, "provenance")?,
    );
    wire.insert(
        "created_at".into(),
        column::<String>(row, "created_at")?.into(),
    );
    wire.insert("revision".into(), column::<i64>(row, "revision")?.into());
    wire.insert("extensions".into(), Map::new().into());
    wire.insert("status".into(), status.clone().into());
    if let Some(occurred_at) = column::<Option<String>>(row, "occurred_at")? {
        wire.insert("occurred_at".into(), occurred_at.into());
    }
    match status.as_str() {
        "pending" => {
            let content = column::<Option<String>>(row, "content_json")?
                .ok_or_else(|| invalid_row("pending candidate has no content_json"))
                .and_then(|raw| from_json(&raw, "candidate content"))?;
            proposed.insert("content".into(), content);
            if let Some(expires_at) = column::<Option<String>>(row, "expires_at")? {
                wire.insert("expires_at".into(), expires_at.into());
            }
        }
        "consumed" => {
            let raw = column::<Option<String>>(row, "consumed_by_json")?;
            wire.insert(
                "consumed_by".into(),
                from_json(
                    &raw.ok_or_else(|| invalid_row("consumed candidate has no consumed_by"))?,
                    "candidate outcome",
                )?,
            );
            wire.insert(
                "content_digest".into(),
                column::<Option<String>>(row, "content_digest")?
                    .ok_or_else(|| invalid_row("consumed candidate has no content_digest"))?
                    .into(),
            );
        }
        other => return Err(invalid_row(format_args!("candidate status {other}"))),
    }
    if !proposed.is_empty() {
        wire.insert("proposed".into(), proposed.into());
    }
    let (_, ir) = memory_candidate_from_wire(wire.into()).map_err(invalid_row)?;
    Ok(ir)
}

fn placement_wire(row: &SqliteRow) -> RepoResult<Value> {
    let visibility: String = column(row, "capture_visibility")?;
    match visibility.as_str() {
        "shared" => Ok(json!({"visibility": "shared"})),
        "context" => Ok(json!({
            "visibility": "context",
            "context_id": column::<Option<String>>(row, "capture_context_id")?
                .ok_or_else(|| invalid_row("context placement has no context id"))?,
        })),
        other => Err(invalid_row(format_args!("capture visibility {other}"))),
    }
}

struct CandidateColumns {
    // `None` when the row has no representable kind (a consumed candidate whose
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
                    &content.unwrap_or(Value::Null),
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

async fn candidate_by_key(
    tx: &mut Tx<'_>,
    space: &SpaceId,
    id: &str,
) -> RepoResult<Option<MemoryCandidateIr>> {
    let sql = AssertSqlSafe(format!(
        "SELECT {CANDIDATE_COLUMNS} FROM memory_candidates \
         WHERE space_id = ? AND candidate_id = ?"
    ));
    sqlx::query(sql)
        .bind(space.as_str())
        .bind(id)
        .fetch_optional(&mut **tx)
        .await?
        .map(|row| candidate_ir(&row))
        .transpose()
}

async fn insert_candidate_row(tx: &mut Tx<'_>, candidate: &MemoryCandidateIr) -> RepoResult<()> {
    let columns = candidate_columns(candidate)?;
    // C-2 policy: the `kind` column is `NOT NULL` and must decode back to a non-empty
    // `proposed.kind`. A consume update without `proposed_kind` therefore preserves the
    // stored kind; a fresh row has nothing to preserve and is rejected instead of
    // writing an empty string it could never read back.
    let kind = columns
        .kind
        .clone()
        .ok_or_else(|| invalid_row("candidate has no kind to persist"))?;
    let (visibility, context_id) = placement_columns(&candidate.capture_scope);
    sqlx::query(
        "INSERT INTO memory_candidates \
         (candidate_id, space_id, subject_id, capture_visibility, capture_context_id, kind, \
          content_json, provenance_json, classification_json, status, consumed_by_json, \
          content_digest, occurred_at, expires_at, revision, created_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&candidate.candidate_id)
    .bind(&candidate.space_id)
    .bind(&candidate.subject_id)
    .bind(visibility)
    .bind(context_id)
    .bind(&kind)
    .bind(&columns.content)
    .bind(&columns.provenance)
    .bind(&columns.classification)
    .bind(columns.status)
    .bind(&columns.consumed_by)
    .bind(&columns.content_digest)
    .bind(&candidate.occurred_at)
    .bind(&columns.expires_at)
    .bind(revision_i64(&candidate.revision)?)
    .bind(&candidate.created_at)
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

// ── Record ──────────────────────────────────────────────────────────────────

const RECORD_COLUMNS: &str = "record_id, space_id, subject_id, kind, content_json, \
     scope_visibility, scope_context_id, provenance_json, import_ref_json, occurred_at, \
     tombstone_json, revision, created_at, updated_at";

fn record_ir(row: &SqliteRow) -> RepoResult<MemoryRecordIr> {
    let mut scope = Map::new();
    let visibility: String = column(row, "scope_visibility")?;
    match visibility.as_str() {
        "shared" => scope.insert("visibility".into(), "shared".into()),
        "context" => {
            scope.insert("visibility".into(), "context".into());
            scope.insert(
                "context_id".into(),
                column::<Option<String>>(row, "scope_context_id")?
                    .ok_or_else(|| invalid_row("context scope has no context id"))?
                    .into(),
            )
        }
        other => return Err(invalid_row(format_args!("scope visibility {other}"))),
    };
    let mut wire = Map::new();
    wire.insert("schema_version".into(), 1.into());
    wire.insert(
        "record_id".into(),
        column::<String>(row, "record_id")?.into(),
    );
    wire.insert("space_id".into(), column::<String>(row, "space_id")?.into());
    wire.insert(
        "subject_id".into(),
        column::<String>(row, "subject_id")?.into(),
    );
    wire.insert("kind".into(), column::<String>(row, "kind")?.into());
    wire.insert("scope".into(), scope.into());
    wire.insert(
        "content".into(),
        from_json(&column::<String>(row, "content_json")?, "record content")?,
    );
    wire.insert(
        "provenance".into(),
        from_json(
            &column::<String>(row, "provenance_json")?,
            "record provenance",
        )?,
    );
    // Pinned projection: the tombstone marks the row dead; a surviving
    // `import_ref_json` (the native delete leaves it) is dropped, and an explicit
    // JSON null `reason` normalizes to the wire's optional-member shape.
    if let Some(raw) = column::<Option<String>>(row, "tombstone_json")? {
        let mut tombstone = from_json(&raw, "record tombstone")?;
        if let Some(object) = tombstone.as_object_mut() {
            if object.get("reason") == Some(&Value::Null) {
                object.remove("reason");
            }
        }
        wire.insert("tombstone".into(), tombstone);
    } else if let Some(import_ref) = column::<Option<String>>(row, "import_ref_json")? {
        wire.insert(
            "import_ref".into(),
            from_json(&import_ref, "record import ref")?,
        );
    }
    if let Some(occurred_at) = column::<Option<String>>(row, "occurred_at")? {
        wire.insert("occurred_at".into(), occurred_at.into());
    }
    wire.insert(
        "created_at".into(),
        column::<String>(row, "created_at")?.into(),
    );
    wire.insert(
        "updated_at".into(),
        column::<String>(row, "updated_at")?.into(),
    );
    wire.insert("revision".into(), column::<i64>(row, "revision")?.into());
    wire.insert("extensions".into(), Map::new().into());
    let (_, ir) = memory_record_from_wire(wire.into()).map_err(invalid_row)?;
    Ok(ir)
}

struct RecordColumns {
    content: String,
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

async fn record_by_key(
    tx: &mut Tx<'_>,
    space: &SpaceId,
    id: &str,
) -> RepoResult<Option<MemoryRecordIr>> {
    let sql = AssertSqlSafe(format!(
        "SELECT {RECORD_COLUMNS} FROM memory_records \
         WHERE space_id = ? AND record_id = ?"
    ));
    sqlx::query(sql)
        .bind(space.as_str())
        .bind(id)
        .fetch_optional(&mut **tx)
        .await?
        .map(|row| record_ir(&row))
        .transpose()
}

async fn insert_record_row(tx: &mut Tx<'_>, record: &MemoryRecordIr) -> RepoResult<()> {
    let columns = record_columns(record)?;
    let (visibility, context_id) = placement_columns(&record.scope);
    sqlx::query(
        "INSERT INTO memory_records \
         (record_id, space_id, subject_id, kind, content_json, scope_visibility, \
          scope_context_id, provenance_json, import_ref_json, occurred_at, tombstone_json, \
          revision, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&record.record_id)
    .bind(&record.space_id)
    .bind(&record.subject_id)
    .bind(&record.kind)
    .bind(&columns.content)
    .bind(visibility)
    .bind(context_id)
    .bind(to_json(
        &memory_record_to_wire(version(), record.clone())
            .get("provenance")
            .cloned()
            .unwrap_or_default(),
        "record provenance",
    )?)
    .bind(&columns.import_ref)
    .bind(&record.occurred_at)
    .bind(&columns.tombstone)
    .bind(revision_i64(&record.revision)?)
    .bind(&record.created_at)
    .bind(&record.updated_at)
    .execute(&mut **tx)
    .await
    .map_err(|error| conflict(error, &record.record_id))?;
    Ok(())
}

// ── Receipt ─────────────────────────────────────────────────────────────────

const RECEIPT_COLUMNS: &str = "receipt_id, space_id, requester_principal_id, op, \
     idempotency_key, payload_digest, status, result_json, error_json, created_at, \
     completed_at, owner_id, lease_expires_at";

fn receipt_wire(row: &SqliteRow) -> RepoResult<Value> {
    let mut wire = Map::new();
    wire.insert("schema_version".into(), 1.into());
    wire.insert(
        "receipt_id".into(),
        column::<String>(row, "receipt_id")?.into(),
    );
    wire.insert("space_id".into(), column::<String>(row, "space_id")?.into());
    wire.insert(
        "requester_principal_id".into(),
        column::<String>(row, "requester_principal_id")?.into(),
    );
    wire.insert("op".into(), column::<String>(row, "op")?.into());
    if let Some(key) = column::<Option<String>>(row, "idempotency_key")? {
        wire.insert("idempotency_key".into(), key.into());
    }
    wire.insert(
        "payload_digest".into(),
        column::<String>(row, "payload_digest")?.into(),
    );
    wire.insert(
        "created_at".into(),
        column::<String>(row, "created_at")?.into(),
    );
    wire.insert("extensions".into(), Map::new().into());
    let status: String = column(row, "status")?;
    wire.insert("status".into(), status.into());
    if let Some(completed_at) = column::<Option<String>>(row, "completed_at")? {
        wire.insert("completed_at".into(), completed_at.into());
    }
    if let Some(result) = column::<Option<String>>(row, "result_json")? {
        wire.insert("result".into(), from_json(&result, "receipt result")?);
    }
    if let Some(error) = column::<Option<String>>(row, "error_json")? {
        wire.insert("error".into(), from_json(&error, "receipt error")?);
    }
    Ok(wire.into())
}

/// Decodes one receipt row: a settled row whose payload columns were reclaimed by
/// retention reports `Evicted` (digest + last status survive), anything else must
/// decode as a full retained receipt.
fn receipt_read(row: &SqliteRow) -> RepoResult<ReceiptRead> {
    let evicted = column::<Option<String>>(row, "result_json")?.is_none()
        && column::<Option<String>>(row, "error_json")?.is_none()
        && column::<String>(row, "status")? != "pending";
    if evicted {
        return Ok(ReceiptRead::Evicted {
            requester: column(row, "requester_principal_id")?,
            operation: column(row, "op")?,
            status: column(row, "status")?,
            payload_digest: column(row, "payload_digest")?,
        });
    }
    let wire = receipt_wire(row)?;
    let (_, ir) = review_receipt_from_wire(wire).map_err(invalid_row)?;
    Ok(ReceiptRead::Retained(ir))
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
    sqlx::query(
        "INSERT INTO memory_receipts \
         (receipt_id, space_id, requester_principal_id, op, idempotency_key, payload_digest, \
          status, result_json, error_json, created_at, completed_at, owner_id, \
          lease_expires_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&receipt.receipt_id)
    .bind(&receipt.space_id)
    .bind(&receipt.requester)
    .bind(&receipt.operation)
    .bind(&receipt.idempotency_key)
    .bind(&receipt.payload_digest)
    .bind(columns.status)
    .bind(&columns.result)
    .bind(&columns.error)
    .bind(&receipt.created_at)
    .bind(&columns.completed_at)
    .bind(lease.map(|lease| lease.owner_id.clone()))
    .bind(lease.map(|lease| lease.expires_at.clone()))
    .execute(&mut **tx)
    .await
    .map_err(|error| conflict(error, &receipt.receipt_id))?;
    Ok(())
}

// ── Row families ────────────────────────────────────────────────────────────

fn space_row(row: &SqliteRow) -> RepoResult<MemorySpaceRow> {
    Ok(MemorySpaceRow {
        space_id: SpaceId::new(column::<String>(row, "space_id")?),
        display_name: column(row, "display_name")?,
        authority_host_id: column(row, "authority_host_id")?,
        authority_generation: column(row, "authority_generation")?,
        status: column(row, "status")?,
        revision: column::<i64>(row, "revision")?.into(),
        created_at: column(row, "created_at")?,
        pending_receipts: column::<i64>(row, "pending_receipts")?
            .try_into()
            .unwrap_or(u64::MAX),
    })
}

const SPACE_COLUMNS: &str = "s.space_id, s.display_name, s.authority_host_id, \
     s.authority_generation, s.status, s.revision, s.created_at, \
     (SELECT COUNT(*) FROM memory_receipts r WHERE r.space_id = s.space_id \
      AND r.status = 'pending') AS pending_receipts";

fn subject_row(row: &SqliteRow) -> RepoResult<MemorySubjectRow> {
    let kind = match (
        column::<Option<String>>(row, "kind_ns")?,
        column::<Option<String>>(row, "kind_id")?,
    ) {
        (Some(namespace), Some(kind)) => Some((namespace, kind)),
        (None, None) => None,
        _ => return Err(invalid_row("subject kind pair is half set")),
    };
    let fenced: Vec<String> = serde_json::from_str(&column::<String>(row, "fenced_contexts_json")?)
        .map_err(|error| invalid_row(format_args!("fenced_contexts_json: {error}")))?;
    Ok(MemorySubjectRow {
        space_id: SpaceId::new(column::<String>(row, "space_id")?),
        subject_id: SubjectId::new(column::<String>(row, "subject_id")?),
        kind,
        status: column(row, "status")?,
        fenced_contexts: fenced.into_iter().map(ContextId::new).collect(),
        revision: column::<i64>(row, "revision")?.into(),
        created_at: column(row, "created_at")?,
        extensions: Map::new(),
    })
}

const SUBJECT_COLUMNS: &str = "space_id, subject_id, kind_ns, kind_id, status, \
     fenced_contexts_json, revision, created_at";

fn grant_row(row: &SqliteRow) -> RepoResult<MemoryGrantRow> {
    let capabilities: Vec<String> =
        serde_json::from_str(&column::<String>(row, "capabilities_json")?)
            .map_err(|error| invalid_row(format_args!("capabilities_json: {error}")))?;
    let contexts: Vec<String> =
        serde_json::from_str(&column::<String>(row, "context_ids_json")?)
            .map_err(|error| invalid_row(format_args!("context_ids_json: {error}")))?;
    Ok(MemoryGrantRow {
        grant_id: column(row, "grant_id")?,
        space_id: SpaceId::new(column::<String>(row, "space_id")?),
        subject_id: column::<Option<String>>(row, "subject_id")?.map(SubjectId::new),
        principal_id: CallerId::new(column::<String>(row, "principal_id")?),
        capabilities,
        context_ids: contexts.into_iter().map(ContextId::new).collect(),
        shared_access: column::<i64>(row, "shared_access")? != 0,
        expires_at: column(row, "expires_at")?,
        revoked_at: column(row, "revoked_at")?,
        revision: column::<i64>(row, "revision")?.into(),
        created_at: column(row, "created_at")?,
        extensions: Map::new(),
    })
}

const GRANT_COLUMNS: &str = "grant_id, space_id, subject_id, principal_id, \
     capabilities_json, context_ids_json, shared_access, expires_at, revoked_at, \
     revision, created_at";

fn profile_row(row: &SqliteRow) -> RepoResult<MemoryProfileRow> {
    Ok(MemoryProfileRow {
        space_id: SpaceId::new(column::<String>(row, "space_id")?),
        subject_id: SubjectId::new(column::<String>(row, "subject_id")?),
        profile_kind: column(row, "profile_kind")?,
        document: from_json(&column::<String>(row, "document_json")?, "profile document")?,
        updated_by: CallerId::new(column::<String>(row, "updated_by")?),
        revision: column::<i64>(row, "revision")?.into(),
        created_at: column(row, "created_at")?,
        updated_at: column(row, "updated_at")?,
        extensions: Map::new(),
    })
}

const PROFILE_COLUMNS: &str = "space_id, subject_id, profile_kind, document_json, updated_by, \
     revision, created_at, updated_at";

fn reflection_row(row: &SqliteRow) -> RepoResult<MemoryReflectionRow> {
    let filter = column::<Option<String>>(row, "filter_json")?
        .map(|raw| from_json(&raw, "reflection filter"))
        .transpose()?;
    let cognition_filter = column::<Option<String>>(row, "cognition_filter_json")?
        .map(|raw| from_json(&raw, "reflection cognition filter"))
        .transpose()?;
    let extensions = match from_json(
        &column::<String>(row, "extensions_json")?,
        "reflection extensions",
    )? {
        Value::Object(map) => map,
        _ => return Err(invalid_row("reflection extensions is not an object")),
    };
    let cost = match (
        column::<Option<String>>(row, "cost_unit_kind")?,
        column::<Option<f64>>(row, "cost_units")?,
    ) {
        (Some(unit_kind), Some(units)) => Some((unit_kind, units)),
        (None, None) => None,
        _ => return Err(invalid_row("reflection cost pair is half set")),
    };
    Ok(MemoryReflectionRow {
        seq: column(row, "seq")?,
        reflection_id: column(row, "reflection_id")?,
        space_id: SpaceId::new(column::<String>(row, "space_id")?),
        subject_id: SubjectId::new(column::<String>(row, "subject_id")?),
        requester: CallerId::new(column::<String>(row, "requester_principal_id")?),
        selection: from_json(
            &column::<String>(row, "selection_json")?,
            "reflection selection",
        )?,
        filter,
        cognition_filter,
        basis: from_json(&column::<String>(row, "basis_json")?, "reflection basis")?,
        engine_policy_version: column(row, "engine_policy_version")?,
        model: column(row, "model")?,
        schema_version: column(row, "schema_version")?,
        revision: column::<i64>(row, "revision")?.into(),
        extensions,
        created_at: column(row, "created_at")?,
        body_text: column(row, "body_text")?,
        body_format: column(row, "body_format")?,
        cost,
        stale_cache: column::<i64>(row, "stale_cache")? != 0,
    })
}

const REFLECTION_COLUMNS: &str = "seq, reflection_id, space_id, subject_id, \
     requester_principal_id, selection_json, filter_json, cognition_filter_json, basis_json, \
     engine_policy_version, model, schema_version, revision, extensions_json, created_at, \
     body_text, body_format, cost_unit_kind, cost_units, stale_cache";

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
            let include_shared = selection
                .get("include_shared")
                .and_then(Value::as_bool)
                .ok_or_else(|| invalid_row("selected_contexts misses include_shared bool"))?;
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

fn journal_row(row: &SqliteRow) -> RepoResult<MemoryJournalRow> {
    let targets: Vec<Value> = serde_json::from_str(&column::<String>(row, "targets_json")?)
        .map_err(|error| invalid_row(format_args!("targets_json: {error}")))?;
    let result = column::<Option<String>>(row, "result_json")?
        .map(|raw| from_json(&raw, "journal result"))
        .transpose()?;
    Ok(MemoryJournalRow {
        seq: column(row, "seq")?,
        receipt_id: column(row, "receipt_id")?,
        space_id: SpaceId::new(column::<String>(row, "space_id")?),
        subject_id: SubjectId::new(column::<String>(row, "subject_id")?),
        requester: CallerId::new(column::<String>(row, "requester_principal_id")?),
        operation: column(row, "op")?,
        idempotency_key: column(row, "idempotency_key")?,
        state: column(row, "state")?,
        targets,
        result,
        created_at: column(row, "created_at")?,
        updated_at: column(row, "updated_at")?,
    })
}

const JOURNAL_COLUMNS: &str = "seq, receipt_id, space_id, subject_id, requester_principal_id, \
     op, idempotency_key, state, targets_json, result_json, created_at, updated_at";

// ── Selection predicate ─────────────────────────────────────────────────────

/// The SQL predicate of a record page plus its binds, in placeholder order; mirrors the
/// pinned schema's admitted-placement filter before any paging.
fn record_predicate(
    space: &SpaceId,
    subject: &str,
    selection: &RecordSelection,
) -> (String, Vec<String>) {
    let mut sql = String::from(" WHERE space_id = ? AND subject_id = ?");
    let mut binds = vec![space.as_str().to_string(), subject.to_string()];
    if !selection.include_tombstoned {
        sql.push_str(" AND tombstone_json IS NULL");
    }
    if !selection.kinds.is_empty() {
        sql.push_str(" AND kind IN (");
        placeholders(&mut sql, selection.kinds.len());
        binds.extend(selection.kinds.iter().cloned());
        sql.push(')');
    }
    placement_clause(
        &mut sql,
        &mut binds,
        &selection.placement,
        "scope_visibility",
        "scope_context_id",
    );
    if let Some(filter) = &selection.provenance_filter {
        if filter.is_empty() {
            sql.push_str(" AND 0");
        } else {
            sql.push_str(
                " AND EXISTS (SELECT 1 FROM \
                 json_each(json_extract(provenance_json, '$.source_context_ids')) AS source \
                 WHERE source.value IN (",
            );
            placeholders(&mut sql, filter.len());
            binds.extend(filter.iter().cloned());
            sql.push_str("))");
        }
    }
    (sql, binds)
}

/// The shared admitted-placement SQL fragment plus its binds, appended to `sql`/`binds`.
/// `visibility_column`/`context_column` name the row family's placement columns.
fn placement_clause(
    sql: &mut String,
    binds: &mut Vec<String>,
    placement: &PlacementSelection,
    visibility_column: &str,
    context_column: &str,
) {
    match placement {
        PlacementSelection::SubjectAll => {}
        PlacementSelection::SharedOnly => {
            sql.push_str(&format!(" AND {visibility_column} = 'shared'"));
        }
        PlacementSelection::SelectedContexts {
            context_ids,
            include_shared,
        } => {
            if context_ids.is_empty() && !include_shared {
                sql.push_str(" AND 0");
            } else if context_ids.is_empty() {
                sql.push_str(&format!(" AND {visibility_column} = 'shared'"));
            } else {
                sql.push_str(" AND (");
                if *include_shared {
                    sql.push_str(&format!("{visibility_column} = 'shared' OR "));
                }
                sql.push_str(&format!("{context_column} IN ("));
                placeholders(sql, context_ids.len());
                binds.extend(context_ids.iter().map(|id| id.as_str().to_string()));
                sql.push_str("))");
            }
        }
    }
}

fn placeholders(sql: &mut String, count: usize) {
    for index in 0..count {
        if index > 0 {
            sql.push(',');
        }
        sql.push('?');
    }
}

/// The borrowed-transaction repository. Zero-sized: the caller's transaction is the seam.
pub struct SqliteMemoryRepository;

#[async_trait]
impl MemoryRepository for SqliteMemoryRepository {
    type Tx<'a> = sqlx::Transaction<'a, Sqlite>;
    type Error = SqliteRepositoryError;

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

    async fn review_receipt(
        &self,
        tx: &mut Self::Tx<'_>,
        key: &ReceiptKey,
    ) -> RepoResult<Option<ReceiptRead>> {
        let sql = AssertSqlSafe(format!(
            "SELECT {RECEIPT_COLUMNS} FROM memory_receipts \
             WHERE space_id = ? AND requester_principal_id = ? AND op = ? \
             AND idempotency_key = ?"
        ));
        sqlx::query(sql)
            .bind(key.space.as_str())
            .bind(key.requester.as_str())
            .bind(&key.operation)
            .bind(&key.idempotency_key)
            .fetch_optional(&mut **tx)
            .await?
            .map(|row| receipt_read(&row))
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
        // `context: None` admits shared rows and every context; `Some` admits only rows
        // placed in that context (the legacy `context_id` predicate, restated over the
        // Nexus placement columns).
        let sql = AssertSqlSafe(format!(
            "SELECT {RECORD_COLUMNS} FROM memory_records \
             WHERE space_id = ? AND subject_id = ? AND tombstone_json IS NULL \
             AND (? IS NULL OR scope_context_id = ?) \
             ORDER BY created_at DESC, record_id"
        ));
        sqlx::query(sql)
            .bind(query.space.as_str())
            .bind(query.subject.as_str())
            .bind(
                query
                    .context
                    .as_ref()
                    .map(|context| context.as_str().to_string()),
            )
            .bind(
                query
                    .context
                    .as_ref()
                    .map(|context| context.as_str().to_string()),
            )
            .fetch_all(&mut **tx)
            .await?
            .iter()
            .map(record_ir)
            .collect()
    }

    async fn space(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
    ) -> RepoResult<Option<MemorySpaceRow>> {
        let sql = AssertSqlSafe(format!(
            "SELECT {SPACE_COLUMNS} FROM memory_spaces s WHERE s.space_id = ?"
        ));
        sqlx::query(sql)
            .bind(space.as_str())
            .fetch_optional(&mut **tx)
            .await?
            .map(|row| space_row(&row))
            .transpose()
    }

    async fn list_spaces(&self, tx: &mut Self::Tx<'_>) -> RepoResult<Vec<MemorySpaceRow>> {
        let sql = AssertSqlSafe(format!(
            "SELECT {SPACE_COLUMNS} FROM memory_spaces s ORDER BY s.space_id"
        ));
        sqlx::query(sql)
            .fetch_all(&mut **tx)
            .await?
            .iter()
            .map(space_row)
            .collect()
    }

    async fn update_space(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemorySpaceRow,
        observed_revision: &Number,
    ) -> RepoResult<bool> {
        let updated = sqlx::query(
            "UPDATE memory_spaces SET display_name = ?, authority_host_id = ?, \
             authority_generation = ?, status = ?, revision = ? \
             WHERE space_id = ? AND revision = ?",
        )
        .bind(&row.display_name)
        .bind(&row.authority_host_id)
        .bind(row.authority_generation)
        .bind(&row.status)
        .bind(revision_i64(&row.revision)?)
        .bind(row.space_id.as_str())
        .bind(revision_i64(observed_revision)?)
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
        let sql = AssertSqlSafe(format!(
            "SELECT {SUBJECT_COLUMNS} FROM memory_subjects \
             WHERE space_id = ? AND subject_id = ?"
        ));
        sqlx::query(sql)
            .bind(space.as_str())
            .bind(id)
            .fetch_optional(&mut **tx)
            .await?
            .map(|row| subject_row(&row))
            .transpose()
    }

    async fn subject_by_kind(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        namespace: &str,
        kind: &str,
    ) -> RepoResult<Option<MemorySubjectRow>> {
        let sql = AssertSqlSafe(format!(
            "SELECT {SUBJECT_COLUMNS} FROM memory_subjects \
             WHERE space_id = ? AND kind_ns = ? AND kind_id = ? \
             ORDER BY subject_id LIMIT 1"
        ));
        sqlx::query(sql)
            .bind(space.as_str())
            .bind(namespace)
            .bind(kind)
            .fetch_optional(&mut **tx)
            .await?
            .map(|row| subject_row(&row))
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
        sqlx::query(
            "INSERT INTO memory_subjects \
             (space_id, subject_id, kind_ns, kind_id, status, fenced_contexts_json, \
              revision, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(row.space_id.as_str())
        .bind(row.subject_id.as_str())
        .bind(kind_ns)
        .bind(kind_id)
        .bind(&row.status)
        .bind(&fenced)
        .bind(revision_i64(&row.revision)?)
        .bind(&row.created_at)
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
        let updated = sqlx::query(
            "UPDATE memory_subjects SET kind_ns = ?, kind_id = ?, status = ?, \
             fenced_contexts_json = ?, revision = ? \
             WHERE space_id = ? AND subject_id = ? AND revision = ?",
        )
        .bind(kind_ns)
        .bind(kind_id)
        .bind(&row.status)
        .bind(&fenced)
        .bind(revision_i64(&row.revision)?)
        .bind(row.space_id.as_str())
        .bind(row.subject_id.as_str())
        .bind(revision_i64(observed_revision)?)
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
        let sql = AssertSqlSafe(format!(
            "SELECT {GRANT_COLUMNS} FROM memory_grants \
             WHERE space_id = ? AND (? IS NULL OR principal_id = ?) ORDER BY grant_id"
        ));
        sqlx::query(sql)
            .bind(space.as_str())
            .bind(principal_filter)
            .bind(principal_filter)
            .fetch_all(&mut **tx)
            .await?
            .iter()
            .map(grant_row)
            .collect()
    }

    async fn grant(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        id: &str,
    ) -> RepoResult<Option<MemoryGrantRow>> {
        let sql = AssertSqlSafe(format!(
            "SELECT {GRANT_COLUMNS} FROM memory_grants \
             WHERE space_id = ? AND grant_id = ?"
        ));
        sqlx::query(sql)
            .bind(space.as_str())
            .bind(id)
            .fetch_optional(&mut **tx)
            .await?
            .map(|row| grant_row(&row))
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
        sqlx::query(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, \
              context_ids_json, shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&row.grant_id)
        .bind(row.space_id.as_str())
        .bind(row.subject_id.as_ref().map(|id| id.as_str().to_string()))
        .bind(row.principal_id.as_str())
        .bind(&capabilities)
        .bind(&contexts)
        .bind(i64::from(row.shared_access))
        .bind(&row.expires_at)
        .bind(&row.revoked_at)
        .bind(revision_i64(&row.revision)?)
        .bind(&row.created_at)
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
        let updated = sqlx::query(
            "UPDATE memory_grants SET subject_id = ?, principal_id = ?, capabilities_json = ?, \
             context_ids_json = ?, shared_access = ?, expires_at = ?, revoked_at = ?, \
             revision = ? \
             WHERE grant_id = ? AND space_id = ? AND revision = ?",
        )
        .bind(row.subject_id.as_ref().map(|id| id.as_str().to_string()))
        .bind(row.principal_id.as_str())
        .bind(&capabilities)
        .bind(&contexts)
        .bind(i64::from(row.shared_access))
        .bind(&row.expires_at)
        .bind(&row.revoked_at)
        .bind(revision_i64(&row.revision)?)
        .bind(&row.grant_id)
        .bind(row.space_id.as_str())
        .bind(revision_i64(observed_revision)?)
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
        let mut where_sql = String::from(" WHERE space_id = ? AND subject_id = ?");
        let mut binds = vec![space.as_str().to_string(), subject.to_string()];
        placement_clause(
            &mut where_sql,
            &mut binds,
            &selection.placement,
            "capture_visibility",
            "capture_context_id",
        );
        if let Some(status) = &selection.status {
            where_sql.push_str(" AND status = ?");
            binds.push(status.clone());
        }
        let sql = AssertSqlSafe(format!(
            "SELECT {CANDIDATE_COLUMNS} FROM memory_candidates{where_sql} \
             ORDER BY created_at, candidate_id"
        ));
        let mut query = sqlx::query(sql);
        for bind in &binds {
            query = query.bind(bind);
        }
        query
            .fetch_all(&mut **tx)
            .await?
            .iter()
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
        // A consume without `proposed_kind` keeps the stored kind (C-2 policy in
        // `insert_candidate_row`); a miss means the guarded update cannot match.
        let kind = match &columns.kind {
            Some(kind) => kind.clone(),
            None => {
                let Some(stored) = sqlx::query_scalar::<_, String>(
                    "SELECT kind FROM memory_candidates WHERE space_id = ? AND candidate_id = ?",
                )
                .bind(&row.space_id)
                .bind(&row.candidate_id)
                .fetch_optional(&mut **tx)
                .await?
                else {
                    return Ok(false);
                };
                stored
            }
        };
        let (visibility, context_id) = placement_columns(&row.capture_scope);
        let updated = sqlx::query(
            "UPDATE memory_candidates SET subject_id = ?, capture_visibility = ?, \
             capture_context_id = ?, kind = ?, content_json = ?, provenance_json = ?, \
             classification_json = ?, status = ?, consumed_by_json = ?, content_digest = ?, \
             occurred_at = ?, expires_at = ?, revision = ? \
             WHERE space_id = ? AND candidate_id = ? AND revision = ?",
        )
        .bind(&row.subject_id)
        .bind(visibility)
        .bind(&context_id)
        .bind(&kind)
        .bind(&columns.content)
        .bind(&columns.provenance)
        .bind(&columns.classification)
        .bind(columns.status)
        .bind(&columns.consumed_by)
        .bind(&columns.content_digest)
        .bind(&row.occurred_at)
        .bind(&columns.expires_at)
        .bind(revision_i64(&row.revision)?)
        .bind(&row.space_id)
        .bind(&row.candidate_id)
        .bind(revision_i64(observed_revision)?)
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
        let (where_sql, binds) = record_predicate(space, subject, selection);
        let sql = AssertSqlSafe(format!(
            "SELECT {RECORD_COLUMNS} FROM memory_records{where_sql} \
             ORDER BY created_at DESC, record_id"
        ));
        let mut query = sqlx::query(sql);
        for bind in &binds {
            query = query.bind(bind);
        }
        query
            .fetch_all(&mut **tx)
            .await?
            .iter()
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
        let mut wire = memory_record_to_wire(version(), row.clone());
        let provenance = wire
            .as_object_mut()
            .and_then(|wire| wire.remove("provenance"))
            .unwrap_or_default();
        let updated = sqlx::query(
            "UPDATE memory_records SET subject_id = ?, kind = ?, content_json = ?, \
             scope_visibility = ?, scope_context_id = ?, provenance_json = ?, \
             import_ref_json = ?, occurred_at = ?, tombstone_json = ?, updated_at = ?, \
             revision = ? \
             WHERE space_id = ? AND record_id = ? AND revision = ?",
        )
        .bind(&row.subject_id)
        .bind(&row.kind)
        .bind(&columns.content)
        .bind(visibility)
        .bind(&context_id)
        .bind(to_json(&provenance, "record provenance")?)
        .bind(&columns.import_ref)
        .bind(&row.occurred_at)
        .bind(&columns.tombstone)
        .bind(&row.updated_at)
        .bind(revision_i64(&row.revision)?)
        .bind(&row.space_id)
        .bind(&row.record_id)
        .bind(revision_i64(observed_revision)?)
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
        let sql = AssertSqlSafe(format!(
            "SELECT {RECEIPT_COLUMNS} FROM memory_receipts \
             WHERE space_id = ? AND receipt_id = ?"
        ));
        let Some(row) = sqlx::query(sql)
            .bind(space.as_str())
            .bind(id)
            .fetch_optional(&mut **tx)
            .await?
        else {
            return Ok(None);
        };
        receipt_read(&row).map(Some)
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
        let updated = sqlx::query(
            "UPDATE memory_receipts SET status = ?, result_json = ?, error_json = ?, \
             completed_at = ? \
             WHERE receipt_id = ? AND space_id = ? AND status = ? AND owner_id IS ?",
        )
        .bind(columns.status)
        .bind(&columns.result)
        .bind(&columns.error)
        .bind(&columns.completed_at)
        .bind(&row.receipt_id)
        .bind(&row.space_id)
        .bind(observed_state)
        .bind(observed_owner)
        .execute(&mut **tx)
        .await?;
        Ok(updated.rows_affected() == 1)
    }

    async fn list_profiles(
        &self,
        tx: &mut Self::Tx<'_>,
        space: &SpaceId,
        subject: &str,
        kinds: &[String],
    ) -> RepoResult<Vec<MemoryProfileRow>> {
        let mut where_sql = String::from(" WHERE space_id = ? AND subject_id = ?");
        let mut binds = vec![space.as_str().to_string(), subject.to_string()];
        if !kinds.is_empty() {
            where_sql.push_str(" AND profile_kind IN (");
            placeholders(&mut where_sql, kinds.len());
            binds.extend(kinds.iter().cloned());
            where_sql.push(')');
        }
        let sql = AssertSqlSafe(format!(
            "SELECT {PROFILE_COLUMNS} FROM memory_profiles{where_sql} \
             ORDER BY profile_kind"
        ));
        let mut query = sqlx::query(sql);
        for bind in &binds {
            query = query.bind(bind);
        }
        query
            .fetch_all(&mut **tx)
            .await?
            .iter()
            .map(profile_row)
            .collect()
    }

    async fn insert_profile(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryProfileRow,
    ) -> RepoResult<()> {
        sqlx::query(
            "INSERT INTO memory_profiles \
             (space_id, subject_id, profile_kind, document_json, updated_by, revision, \
              created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(row.space_id.as_str())
        .bind(row.subject_id.as_str())
        .bind(&row.profile_kind)
        .bind(to_json(&row.document, "profile document")?)
        .bind(row.updated_by.as_str())
        .bind(revision_i64(&row.revision)?)
        .bind(&row.created_at)
        .bind(&row.updated_at)
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
        let updated = sqlx::query(
            "UPDATE memory_profiles SET document_json = ?, updated_by = ?, updated_at = ?, \
             revision = ? \
             WHERE space_id = ? AND subject_id = ? AND profile_kind = ? AND revision = ?",
        )
        .bind(to_json(&row.document, "profile document")?)
        .bind(row.updated_by.as_str())
        .bind(&row.updated_at)
        .bind(revision_i64(&row.revision)?)
        .bind(row.space_id.as_str())
        .bind(row.subject_id.as_str())
        .bind(&row.profile_kind)
        .bind(revision_i64(observed_revision)?)
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
        let sql = AssertSqlSafe(format!(
            "SELECT {REFLECTION_COLUMNS} FROM memory_reflections \
             WHERE space_id = ? AND subject_id = ? ORDER BY seq DESC"
        ));
        let rows = sqlx::query(sql)
            .bind(space.as_str())
            .bind(subject)
            .fetch_all(&mut **tx)
            .await?;
        let mut reflections = Vec::with_capacity(rows.len());
        for row in &rows {
            let reflection = reflection_row(row)?;
            // The pinned selection is validated on every read, filter or not.
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
        sqlx::query(
            "INSERT INTO memory_reflections \
             (seq, reflection_id, space_id, subject_id, requester_principal_id, \
              selection_json, filter_json, cognition_filter_json, basis_json, \
              engine_policy_version, model, schema_version, revision, extensions_json, \
              created_at, body_text, body_format, cost_unit_kind, cost_units, stale_cache) \
             VALUES (\
               (SELECT COALESCE(MAX(seq), 0) + 1 FROM memory_reflections), \
               ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&row.reflection_id)
        .bind(row.space_id.as_str())
        .bind(row.subject_id.as_str())
        .bind(row.requester.as_str())
        .bind(to_json(&row.selection, "reflection selection")?)
        .bind(
            row.filter
                .as_ref()
                .map(|value| to_json(value, "reflection filter"))
                .transpose()?,
        )
        .bind(
            row.cognition_filter
                .as_ref()
                .map(|value| to_json(value, "reflection cognition filter"))
                .transpose()?,
        )
        .bind(to_json(&row.basis, "reflection basis")?)
        .bind(&row.engine_policy_version)
        .bind(&row.model)
        .bind(row.schema_version)
        .bind(revision_i64(&row.revision)?)
        .bind(to_json(
            &Value::Object(row.extensions.clone()),
            "reflection extensions",
        )?)
        .bind(&row.created_at)
        .bind(&row.body_text)
        .bind(&row.body_format)
        .bind(cost_unit_kind)
        .bind(cost_units)
        .bind(i64::from(row.stale_cache))
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
        let sql = AssertSqlSafe(format!(
            "SELECT {JOURNAL_COLUMNS} FROM memory_journal \
             WHERE space_id = ? AND receipt_id = ?"
        ));
        sqlx::query(sql)
            .bind(space.as_str())
            .bind(receipt_id)
            .fetch_optional(&mut **tx)
            .await?
            .map(|row| journal_row(&row))
            .transpose()
    }

    async fn insert_journal_intent(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryJournalRow,
    ) -> RepoResult<()> {
        sqlx::query(
            "INSERT INTO memory_journal \
             (seq, receipt_id, space_id, subject_id, requester_principal_id, op, \
              idempotency_key, state, targets_json, result_json, created_at, updated_at) \
             VALUES ((SELECT COALESCE(MAX(seq), 0) + 1 FROM memory_journal), \
                     ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&row.receipt_id)
        .bind(row.space_id.as_str())
        .bind(row.subject_id.as_str())
        .bind(row.requester.as_str())
        .bind(&row.operation)
        .bind(&row.idempotency_key)
        .bind(&row.state)
        .bind(to_json(
            &Value::Array(row.targets.clone()),
            "journal targets",
        )?)
        .bind(
            row.result
                .as_ref()
                .map(|value| to_json(value, "journal result"))
                .transpose()?,
        )
        .bind(&row.created_at)
        .bind(&row.updated_at)
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
        let updated = sqlx::query(
            "UPDATE memory_journal SET state = ?, targets_json = ?, result_json = ?, \
             updated_at = ? \
             WHERE space_id = ? AND receipt_id = ? AND state = ?",
        )
        .bind(&row.state)
        .bind(to_json(
            &Value::Array(row.targets.clone()),
            "journal targets",
        )?)
        .bind(
            row.result
                .as_ref()
                .map(|value| to_json(value, "journal result"))
                .transpose()?,
        )
        .bind(&row.updated_at)
        .bind(row.space_id.as_str())
        .bind(&row.receipt_id)
        .bind(observed_state)
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
        // Keys are caller-composed and already carry their space; `space` scopes the
        // caller's intent, the table keys on `key` alone.
        let _ = space;
        sqlx::query_as::<_, (String, String, String)>(
            "SELECT key, value, updated_at FROM memory_provider_meta WHERE key = ?",
        )
        .bind(key)
        .fetch_optional(&mut **tx)
        .await
        .map(|row| {
            row.map(|(key, value, updated_at)| MemoryProviderMetaRow {
                key,
                value,
                updated_at,
            })
        })
        .map_err(Into::into)
    }

    async fn put_provider_meta(
        &self,
        tx: &mut Self::Tx<'_>,
        row: &MemoryProviderMetaRow,
    ) -> RepoResult<()> {
        sqlx::query(
            "INSERT INTO memory_provider_meta (key, value, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, \
             updated_at = excluded.updated_at",
        )
        .bind(&row.key)
        .bind(&row.value)
        .bind(&row.updated_at)
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
        let deleted = sqlx::query("DELETE FROM memory_provider_meta WHERE key = ?")
            .bind(key)
            .execute(&mut **tx)
            .await?;
        Ok(deleted.rows_affected() == 1)
    }
}
