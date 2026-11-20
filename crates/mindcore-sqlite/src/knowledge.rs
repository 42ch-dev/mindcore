//! `KnowledgeEntryPort` adapter: one fixed collection over the store's shared pool.

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value};
use sqlx::{Executor, Sqlite, SqlitePool, Transaction};

use mindcore_base::cognition::carrier_record_violation;
use mindcore_base::knowledge::KnowledgeScope;
use spoke_operations::{
    spoke_reject, validate_cognition_module, KnowledgeEntryPort, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::knowledge_entry::{KnowledgeEntryModulesKey, KnowledgeEntryModulesValue};
use spoke_schemas::KnowledgeEntry;

use crate::store::MindStore;

/// A knowledge port bound to one collection.
///
/// The scope is fixed at construction and never changes: every statement filters on it, so the port
/// cannot read or write another collection's entries. `owner` and extension metadata travel inside
/// the payload and never select rows.
///
/// `put` persists the submitted payload unchanged except for the assigned `revision` (`1` on create,
/// stored + 1 on update) and, on update, the stored row's `created_at`, which the payload repeats so
/// payload and column agree; the SQL timestamp columns carry the store's own write bookkeeping.
/// A present `modules.cognition` is gated here, so no caller can commit a module that the next
/// derivation would reject.
pub struct ScopedKnowledgeStore {
    pool: SqlitePool,
    scope: KnowledgeScope,
}

impl ScopedKnowledgeStore {
    pub fn new(pool: SqlitePool, scope: KnowledgeScope) -> Self {
        Self { pool, scope }
    }

    pub fn scope(&self) -> &KnowledgeScope {
        &self.scope
    }

    /// Every entry of this collection, ordered by `entry_id` ASC.
    pub async fn list_entries(&self) -> SpokeResult<Vec<KnowledgeEntry>> {
        entries(&self.pool, &self.scope.collection).await
    }
}

/// Every entry of `collection`, ordered by `entry_id` ASC, read through the
/// caller's transaction so a workflow reads one whole snapshot at one point.
pub async fn list_entries_in(
    tx: &mut Transaction<'_, Sqlite>,
    collection: &str,
) -> SpokeResult<Vec<KnowledgeEntry>> {
    entries(&mut **tx, collection).await
}

async fn entries<'e, E>(executor: E, collection: &str) -> SpokeResult<Vec<KnowledgeEntry>>
where
    E: Executor<'e, Database = Sqlite>,
{
    let payloads: Result<Vec<String>, sqlx::Error> = sqlx::query_scalar(
        "SELECT payload FROM knowledge_entries WHERE collection = ? ORDER BY entry_id ASC",
    )
    .bind(collection)
    .fetch_all(executor)
    .await;
    let payloads = match payloads {
        Ok(payloads) => payloads,
        Err(error) => return internal(format!("knowledge entries could not be listed: {error}")),
    };
    let mut stored = Vec::with_capacity(payloads.len());
    for payload in &payloads {
        match decode(payload) {
            SpokeResult::Ok(entry) => stored.push(entry),
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        }
    }
    SpokeResult::Ok(stored)
}

impl MindStore {
    /// A port for `scope`, sharing this store's single pool.
    pub fn knowledge(&self, scope: KnowledgeScope) -> ScopedKnowledgeStore {
        ScopedKnowledgeStore::new(self.pool.clone(), scope)
    }
}

fn internal<T>(message: impl Into<String>) -> SpokeResult<T> {
    spoke_reject(SpokeRejectCode::InternalError, message, None)
}

fn decode(payload: &str) -> SpokeResult<KnowledgeEntry> {
    match serde_json::from_str(payload) {
        Ok(entry) => SpokeResult::Ok(entry),
        Err(error) => internal(format!("stored knowledge payload is not an entry: {error}")),
    }
}

fn encode(entry: &KnowledgeEntry) -> SpokeResult<String> {
    match serde_json::to_string(entry) {
        Ok(payload) => SpokeResult::Ok(payload),
        Err(error) => internal(format!("knowledge entry could not be serialized: {error}")),
    }
}

// Fixed-width fractional seconds keep TEXT ordering identical to instant ordering.
fn encode_timestamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn cognition_module(entry: &KnowledgeEntry) -> Option<Value> {
    let key: KnowledgeEntryModulesKey = "cognition".parse().expect("valid module namespace");
    match entry.modules.get(&key)? {
        KnowledgeEntryModulesValue::Object(module) => Some(Value::Object(module.clone())),
        KnowledgeEntryModulesValue::Array(items) => Some(Value::Array(items.clone())),
    }
}

fn stale<T>(
    collection: &str,
    entry_id: &str,
    expected: Option<u64>,
    stored: Option<u64>,
) -> SpokeResult<T> {
    let mut details = Map::new();
    details.insert("expected_base_revision".to_string(), Value::from(expected));
    details.insert("stored_revision".to_string(), Value::from(stored));
    spoke_reject(
        SpokeRejectCode::StoredRevisionStale,
        format!(
            "knowledge entry {entry_id} in collection {collection} does not hold the expected revision"
        ),
        Some(details),
    )
}

/// One entry of `collection`, read through the caller's executor: the scoped
/// port and the workflow binding share this read, and a settlement reads it at
/// the same point as the write it guards.
pub async fn read_entry<'e, E>(
    executor: E,
    collection: &str,
    entry_id: &str,
) -> SpokeResult<KnowledgeEntry>
where
    E: Executor<'e, Database = Sqlite>,
{
    let payload: Result<Option<String>, sqlx::Error> = sqlx::query_scalar(
        "SELECT payload FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
    )
    .bind(collection)
    .bind(entry_id)
    .fetch_optional(executor)
    .await;
    match payload {
        Ok(Some(payload)) => decode(&payload),
        Ok(None) => spoke_reject(
            SpokeRejectCode::KnowledgeEntryNotFound,
            format!("no knowledge entry {entry_id} in collection {collection}"),
            None,
        ),
        Err(error) => internal(format!("knowledge entry could not be read: {error}")),
    }
}

/// Write one entry of `collection` against `expected_base_revision` through the
/// caller's transaction, assigning `1` on create and `stored + 1` on update.
///
/// An update also restores the stored row's `created_at` into the payload: the
/// SQL column keeps the original creation instant across an update, so the
/// payload must not carry a fresher one.
///
/// The caller holds the write reservation, so the revision comparison and the
/// write cannot interleave with another handle on the same file.
pub async fn write_entry(
    tx: &mut Transaction<'_, Sqlite>,
    collection: &str,
    mut entry: KnowledgeEntry,
    expected_base_revision: Option<u64>,
) -> SpokeResult<KnowledgeEntry> {
    // Single persistence boundary: gate the carrier module before the revision read or any row
    // write, so a refusal leaves the stored row untouched.
    if let Some(module) = cognition_module(&entry) {
        if let SpokeResult::Reject(reject) = validate_cognition_module(&module) {
            return SpokeResult::Reject(reject);
        }
        // The module validator carries no entry identity; the carrier contract also requires
        // every stored record to be held by this entry.
        if let Some(records) = module.get("records").and_then(Value::as_array) {
            for record in records {
                if let Some(violation) = carrier_record_violation(record, &entry.entry_id) {
                    return spoke_reject(SpokeRejectCode::InvalidInput, violation, None);
                }
            }
        }
    }

    let stored: Result<Option<(i64, String)>, sqlx::Error> = sqlx::query_as(
        "SELECT revision, created_at FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
    )
    .bind(collection)
    .bind(&entry.entry_id)
    .fetch_optional(&mut **tx)
    .await;
    let stored = match stored {
        Ok(stored) => stored,
        Err(error) => return internal(format!("stored revision could not be read: {error}")),
    };
    let stored_created_at = stored.as_ref().map(|(_, created_at)| created_at.clone());
    let stored = stored.map(|(revision, _)| revision as u64);

    let revision = match expected_base_revision {
        None => match stored {
            None => 1,
            Some(_) => {
                return spoke_reject(
                    SpokeRejectCode::KnowledgeEntryAlreadyExists,
                    format!(
                        "knowledge entry {} already exists in collection {collection}",
                        entry.entry_id
                    ),
                    None,
                )
            }
        },
        Some(expected) => match stored {
            Some(stored) if stored == expected => expected + 1,
            stored => return stale(collection, &entry.entry_id, Some(expected), stored),
        },
    };

    // Store bookkeeping stays in the SQL timestamp columns; the payload keeps
    // the caller's, except the creation instant an update never moves.
    let now = Utc::now();
    entry.revision = Some(revision);
    if let Some(stored_created_at) = stored_created_at {
        let created_at = match DateTime::parse_from_rfc3339(&stored_created_at) {
            Ok(created_at) => created_at.with_timezone(&Utc),
            Err(error) => {
                return internal(format!(
                    "stored created_at of knowledge entry {} is not RFC 3339: {error}",
                    entry.entry_id
                ))
            }
        };
        // The row column is not rewritten by an update, so the payload carries
        // the stored instant: payload and column never diverge on it.
        entry.created_at = Some(created_at);
    }

    let payload = match encode(&entry) {
        SpokeResult::Ok(payload) => payload,
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    let created_at = encode_timestamp(entry.created_at.unwrap_or(now));
    let updated_at = encode_timestamp(now);
    let written: Result<(), sqlx::Error> = match stored {
        Some(_) => sqlx::query(
            "UPDATE knowledge_entries SET revision = ?, payload = ?, updated_at = ?
             WHERE collection = ? AND entry_id = ?",
        )
        .bind(revision as i64)
        .bind(&payload)
        .bind(&updated_at)
        .bind(collection)
        .bind(&entry.entry_id)
        .execute(&mut **tx)
        .await
        .map(|_| ()),
        None => sqlx::query(
            "INSERT INTO knowledge_entries (collection, entry_id, revision, payload, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(collection)
        .bind(&entry.entry_id)
        .bind(revision as i64)
        .bind(&payload)
        .bind(&created_at)
        .bind(&updated_at)
        .execute(&mut **tx)
        .await
        .map(|_| ()),
    };
    if let Err(error) = written {
        return internal(format!("knowledge entry could not be written: {error}"));
    }
    SpokeResult::Ok(entry)
}

#[async_trait]
impl KnowledgeEntryPort for ScopedKnowledgeStore {
    async fn get_knowledge_entry(&self, entry_id: &str) -> SpokeResult<KnowledgeEntry> {
        read_entry(&self.pool, &self.scope.collection, entry_id).await
    }

    async fn put_knowledge_entry(
        &self,
        entry: KnowledgeEntry,
        expected_base_revision: Option<u64>,
    ) -> SpokeResult<KnowledgeEntry> {
        // Hold SQLite's write reservation across the read and the write: the revision comparison and
        // the put must not interleave with another handle on the same file.
        let mut transaction = match self.pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(transaction) => transaction,
            Err(error) => return internal(format!("knowledge write could not begin: {error}")),
        };
        let written = write_entry(
            &mut transaction,
            &self.scope.collection,
            entry,
            expected_base_revision,
        )
        .await;
        let entry = match written {
            SpokeResult::Ok(entry) => entry,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        if let Err(error) = transaction.commit().await {
            return internal(format!("knowledge write could not commit: {error}"));
        }
        SpokeResult::Ok(entry)
    }
}
