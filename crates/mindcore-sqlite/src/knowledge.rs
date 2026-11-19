//! `KnowledgeEntryPort` adapter: one fixed collection over the store's shared pool.

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value};
use sqlx::SqlitePool;

use mindcore_core::knowledge::KnowledgeScope;
use spoke_operations::{spoke_reject, KnowledgeEntryPort, SpokeRejectCode, SpokeResult};
use spoke_schemas::KnowledgeEntry;

use crate::store::MindStore;

/// A knowledge port bound to one collection.
///
/// The scope is fixed at construction and never changes: every statement filters on it, so the port
/// cannot read or write another collection's entries. `owner` and extension metadata travel inside
/// the payload and never select rows.
///
/// `put` assigns the revision (`1` on create, stored + 1 on update), stamps `updated_at` with the
/// write instant, and stamps an absent `created_at` on create. The SQL columns keep the store's own
/// timestamps; a payload without `created_at` therefore reads back without one.
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
        let payloads: Result<Vec<String>, sqlx::Error> = sqlx::query_scalar(
            "SELECT payload FROM knowledge_entries WHERE collection = ? ORDER BY entry_id ASC",
        )
        .bind(&self.scope.collection)
        .fetch_all(&self.pool)
        .await;
        let payloads = match payloads {
            Ok(payloads) => payloads,
            Err(error) => {
                return internal(format!("knowledge entries could not be listed: {error}"))
            }
        };
        let mut entries = Vec::with_capacity(payloads.len());
        for payload in &payloads {
            match decode(payload) {
                SpokeResult::Ok(entry) => entries.push(entry),
                SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
            }
        }
        SpokeResult::Ok(entries)
    }
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

#[async_trait]
impl KnowledgeEntryPort for ScopedKnowledgeStore {
    async fn get_knowledge_entry(&self, entry_id: &str) -> SpokeResult<KnowledgeEntry> {
        let payload: Result<Option<String>, sqlx::Error> = sqlx::query_scalar(
            "SELECT payload FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
        )
        .bind(&self.scope.collection)
        .bind(entry_id)
        .fetch_optional(&self.pool)
        .await;
        let payload = match payload {
            Ok(Some(payload)) => payload,
            Ok(None) => {
                return spoke_reject(
                    SpokeRejectCode::KnowledgeEntryNotFound,
                    format!(
                        "no knowledge entry {entry_id} in collection {}",
                        self.scope.collection
                    ),
                    None,
                )
            }
            Err(error) => return internal(format!("knowledge entry could not be read: {error}")),
        };
        decode(&payload)
    }

    async fn put_knowledge_entry(
        &self,
        mut entry: KnowledgeEntry,
        expected_base_revision: Option<u64>,
    ) -> SpokeResult<KnowledgeEntry> {
        let collection = self.scope.collection.as_str();
        // Hold SQLite's write reservation across the read and the write: the revision comparison and
        // the put must not interleave with another handle on the same file.
        let mut transaction = match self.pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(transaction) => transaction,
            Err(error) => return internal(format!("knowledge write could not begin: {error}")),
        };

        let stored: Result<Option<i64>, sqlx::Error> = sqlx::query_scalar(
            "SELECT revision FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
        )
        .bind(collection)
        .bind(&entry.entry_id)
        .fetch_optional(&mut *transaction)
        .await;
        let stored = match stored {
            Ok(stored) => stored.map(|revision| revision as u64),
            Err(error) => return internal(format!("stored revision could not be read: {error}")),
        };

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

        let now = Utc::now();
        if stored.is_none() && entry.created_at.is_none() {
            entry.created_at = Some(now);
        }
        entry.updated_at = Some(now.into());
        entry.revision = Some(revision);

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
            .execute(&mut *transaction)
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
            .execute(&mut *transaction)
            .await
            .map(|_| ()),
        };
        if let Err(error) = written {
            return internal(format!("knowledge entry could not be written: {error}"));
        }

        if let Err(error) = transaction.commit().await {
            return internal(format!("knowledge write could not commit: {error}"));
        }
        SpokeResult::Ok(entry)
    }
}
