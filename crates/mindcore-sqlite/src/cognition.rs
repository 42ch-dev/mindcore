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
/// The caller owns the head pointer and the carrier revision, so the row commits together with
/// them or not at all. A duplicate scoped `record_id` is invalid input, not a storage failure.
pub async fn insert_record(
    tx: &mut Transaction<'_, Sqlite>,
    collection: &str,
    entry_id: &str,
    append_seq: i64,
    record: &Value,
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
        "INSERT INTO cognition_records (collection, entry_id, append_seq, record_id, payload)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(collection)
    .bind(entry_id)
    .bind(append_seq)
    .bind(record_id)
    .bind(&payload)
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
    let payloads: Result<Vec<String>, sqlx::Error> = sqlx::query_scalar(
        "SELECT payload FROM cognition_records \
         WHERE collection = ? AND entry_id = ? ORDER BY append_seq ASC",
    )
    .bind(collection)
    .bind(entry_id)
    .fetch_all(executor)
    .await;
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
