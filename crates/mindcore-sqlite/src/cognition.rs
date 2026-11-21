//! Carrier reads and writes over one scoped knowledge collection.

use serde_json::Value;
use spoke_operations::{spoke_reject, KnowledgeEntryPort, SpokeRejectCode, SpokeResult};
use spoke_schemas::KnowledgeEntry;
use sqlx::{Executor, Sqlite, Transaction};

use crate::knowledge::{internal, ScopedKnowledgeStore};

/// Reads one carrier from the port's fixed collection.
pub async fn read_carrier(
    store: &ScopedKnowledgeStore,
    entry_id: &str,
) -> SpokeResult<KnowledgeEntry> {
    store.get_knowledge_entry(entry_id).await
}

/// Writes a carrier against the revision the caller derived it from.
///
/// Validation lives in the port's `put`, the single persistence boundary, so this stays a thin
/// revision-guarded write.
pub async fn write_carrier(
    store: &ScopedKnowledgeStore,
    entry: KnowledgeEntry,
    expected_revision: u64,
) -> SpokeResult<KnowledgeEntry> {
    store
        .put_knowledge_entry(entry, Some(expected_revision))
        .await
}

/// Appends one prepared record row at `append_seq` through the caller's transaction.
///
/// Crate-private raw row writer: `append_cognition` and the port's full put
/// (`write_entry`) are its only callers, and they own every invariant this helper
/// does not check — the record's wire shape, its holder identity, the carrier
/// revision, the head pointer and the envelope. `append_prior_shape` is `Some` only for a
/// dedicated append and lets the preimage trigger recognize (and skip) it. A duplicate scoped
/// `record_id` is invalid input, not a storage failure.
pub(crate) async fn insert_record(
    tx: &mut Transaction<'_, Sqlite>,
    collection: &str,
    entry_id: &str,
    append_seq: i64,
    record: &Value,
    append_prior_shape: Option<i64>,
) -> SpokeResult<()> {
    let Some(record_id) = record.get("record_id").and_then(Value::as_str) else {
        return spoke_reject(
            SpokeRejectCode::InvalidInput,
            "cognition record has no usable record_id",
            None,
        );
    };
    let payload = match serde_json::to_string(record) {
        Ok(payload) => payload,
        Err(error) => {
            return internal(format!("cognition record could not be serialized: {error}"))
        }
    };
    let written = sqlx::query(
        "INSERT INTO cognition_records \
         (collection, entry_id, append_seq, record_id, payload, append_prior_shape) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(collection)
    .bind(entry_id)
    .bind(append_seq)
    .bind(record_id)
    .bind(&payload)
    .bind(append_prior_shape)
    .execute(&mut **tx)
    .await;
    match written {
        Ok(_) => SpokeResult::Ok(()),
        Err(error) if is_unique_violation(&error) => spoke_reject(
            SpokeRejectCode::InvalidInput,
            format!("record_id {record_id} already exists in the carrier"),
            None,
        ),
        Err(error) => internal(format!("cognition record could not be written: {error}")),
    }
}

/// Every record of one holder in storage order: `append_seq` ASC, the carrier order the derived
/// view reads, independent of the records' own `order` field.
pub async fn read_records<'e, E>(
    executor: E,
    collection: &str,
    entry_id: &str,
) -> SpokeResult<Vec<Value>>
where
    E: Executor<'e, Database = Sqlite>,
{
    ordered_records(executor, collection, entry_id, None).await
}

/// The holder's records up to and including `head_seq` in storage order: the historical
/// projection a replay hydrates, so a record appended after that head is never read.
pub(crate) async fn read_records_through<'e, E>(
    executor: E,
    collection: &str,
    entry_id: &str,
    head_seq: i64,
) -> SpokeResult<Vec<Value>>
where
    E: Executor<'e, Database = Sqlite>,
{
    ordered_records(executor, collection, entry_id, Some(head_seq)).await
}

// One decoder and one refusal path for both record reads: the bound differs, the storage order,
// the JSON shape and the failure message do not.
async fn ordered_records<'e, E>(
    executor: E,
    collection: &str,
    entry_id: &str,
    through: Option<i64>,
) -> SpokeResult<Vec<Value>>
where
    E: Executor<'e, Database = Sqlite>,
{
    let payloads: Result<Vec<String>, sqlx::Error> = match through {
        Some(head_seq) => {
            sqlx::query_scalar(
                "SELECT payload FROM cognition_records \
                 WHERE collection = ? AND entry_id = ? AND append_seq <= ? \
                 ORDER BY append_seq ASC",
            )
            .bind(collection)
            .bind(entry_id)
            .bind(head_seq)
            .fetch_all(executor)
            .await
        }
        None => {
            sqlx::query_scalar(
                "SELECT payload FROM cognition_records \
                 WHERE collection = ? AND entry_id = ? ORDER BY append_seq ASC",
            )
            .bind(collection)
            .bind(entry_id)
            .fetch_all(executor)
            .await
        }
    };
    let payloads = match payloads {
        Ok(payloads) => payloads,
        Err(error) => return internal(format!("cognition records could not be listed: {error}")),
    };
    let mut records = Vec::with_capacity(payloads.len());
    for payload in &payloads {
        match serde_json::from_str(payload) {
            Ok(record) => records.push(record),
            Err(error) => {
                return internal(format!("stored cognition record is not JSON: {error}"));
            }
        }
    }
    SpokeResult::Ok(records)
}

/// Whether the `gap` head positions a replay crosses are exactly the rows dedicated appends wrote:
/// the classified rows at `after + 1 ..= after + gap`, so no position is missing and none was
/// committed by a full put. A full put stamps `NULL` and saves its own preimage, so it can never
/// leave a gap behind.
pub(crate) async fn gap_is_dedicated_appends<'e, E>(
    executor: E,
    collection: &str,
    entry_id: &str,
    after: i64,
    gap: i64,
) -> SpokeResult<bool>
where
    E: Executor<'e, Database = Sqlite>,
{
    // Positions, not a count: `append_seq` is a non-`STRICT` `INTEGER`, so a fractional value is
    // storable and a count would let it stand in for the position it is not.
    let rows: Result<Vec<(i64, Option<i64>)>, sqlx::Error> = sqlx::query_as(
        "SELECT CAST(append_seq AS INTEGER), append_prior_shape FROM cognition_records \
         WHERE collection = ? AND entry_id = ? AND append_seq > ? AND append_seq <= ? \
           AND append_seq = CAST(append_seq AS INTEGER) \
         ORDER BY append_seq ASC",
    )
    .bind(collection)
    .bind(entry_id)
    .bind(after)
    .bind(after + gap)
    .fetch_all(executor)
    .await;
    match rows {
        Ok(rows) => SpokeResult::Ok(
            rows.len() as i64 == gap
                && rows
                    .iter()
                    .zip(after + 1..=after + gap)
                    .all(|((position, shape), expected)| *position == expected && shape.is_some()),
        ),
        Err(error) => internal(format!("cognition history gap could not be read: {error}")),
    }
}

/// What the append at `append_seq` replaced in the carrier envelope: `None` when that row has no
/// classification (a full-put record, or no such row), which a replay must treat as missing
/// history rather than as an empty shape.
pub(crate) async fn read_append_prior_shape<'e, E>(
    executor: E,
    collection: &str,
    entry_id: &str,
    append_seq: i64,
) -> SpokeResult<Option<i64>>
where
    E: Executor<'e, Database = Sqlite>,
{
    let shape: Result<Option<Option<i64>>, sqlx::Error> = sqlx::query_scalar(
        "SELECT append_prior_shape FROM cognition_records \
         WHERE collection = ? AND entry_id = ? AND append_seq = ?",
    )
    .bind(collection)
    .bind(entry_id)
    .bind(append_seq)
    .fetch_optional(executor)
    .await;
    match shape {
        Ok(Some(shape)) => SpokeResult::Ok(shape),
        Ok(None) => SpokeResult::Ok(None),
        Err(error) => internal(format!("cognition record shape could not be read: {error}")),
    }
}

/// One record addressed by its scoped `record_id`, independent of history size.
pub async fn read_record<'e, E>(
    executor: E,
    collection: &str,
    entry_id: &str,
    record_id: &str,
) -> SpokeResult<Option<Value>>
where
    E: Executor<'e, Database = Sqlite>,
{
    let payload: Result<Option<String>, sqlx::Error> = sqlx::query_scalar(
        "SELECT payload FROM cognition_records \
         WHERE collection = ? AND entry_id = ? AND record_id = ?",
    )
    .bind(collection)
    .bind(entry_id)
    .bind(record_id)
    .fetch_optional(executor)
    .await;
    match payload {
        Ok(Some(payload)) => match serde_json::from_str(&payload) {
            Ok(record) => SpokeResult::Ok(Some(record)),
            Err(error) => internal(format!("stored cognition record is not JSON: {error}")),
        },
        Ok(None) => SpokeResult::Ok(None),
        Err(error) => internal(format!("cognition record could not be read: {error}")),
    }
}

/// The holder's current judgment: the record row its head points at, one indexed
/// lookup over two primary keys regardless of history size; none while it holds
/// no records.
pub async fn read_current_record<'e, E>(
    executor: E,
    collection: &str,
    entry_id: &str,
) -> SpokeResult<Option<Value>>
where
    E: Executor<'e, Database = Sqlite>,
{
    let payload: Result<Option<String>, sqlx::Error> = sqlx::query_scalar(
        "SELECT r.payload FROM cognition_records AS r \
         JOIN knowledge_entries AS k \
           ON k.collection = r.collection AND k.entry_id = r.entry_id \
          AND k.cognition_head_seq = r.append_seq \
         WHERE k.collection = ? AND k.entry_id = ?",
    )
    .bind(collection)
    .bind(entry_id)
    .fetch_optional(executor)
    .await;
    match payload {
        Ok(Some(payload)) => match serde_json::from_str(&payload) {
            Ok(record) => SpokeResult::Ok(Some(record)),
            Err(error) => internal(format!("stored cognition record is not JSON: {error}")),
        },
        Ok(None) => SpokeResult::Ok(None),
        Err(error) => internal(format!("cognition record could not be read: {error}")),
    }
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}
