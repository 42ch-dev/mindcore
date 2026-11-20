//! `KnowledgeEntryPort` adapter: one fixed collection over the store's shared pool.

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value};
use sqlx::{Executor, Sqlite, SqliteConnection, SqlitePool, Transaction};

use mindcore_base::cognition::carrier_record_violation;
use mindcore_base::knowledge::KnowledgeScope;
use spoke_operations::{
    spoke_reject, validate_cognition_module, validate_cognition_record, KnowledgeEntryPort,
    SpokeRejectCode, SpokeResult,
};
use spoke_schemas::knowledge_entry::{KnowledgeEntryModulesKey, KnowledgeEntryModulesValue};
use spoke_schemas::KnowledgeEntry;

use crate::cognition::{insert_record, read_records};
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
/// Cognition records are the exception to "unchanged": they are stored as their own rows and the
/// payload keeps the module envelope without the inline array. A present `modules.cognition` is
/// gated here, so no caller can commit a module that the next derivation would reject.
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
        let mut connection = match self.pool.acquire().await {
            Ok(connection) => connection,
            Err(error) => {
                return internal(format!("knowledge entries could not be listed: {error}"))
            }
        };
        entries(&mut connection, &self.scope.collection).await
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

async fn entries(
    connection: &mut SqliteConnection,
    collection: &str,
) -> SpokeResult<Vec<KnowledgeEntry>> {
    let payloads: Result<Vec<String>, sqlx::Error> = sqlx::query_scalar(
        "SELECT payload FROM knowledge_entries WHERE collection = ? ORDER BY entry_id ASC",
    )
    .bind(collection)
    .fetch_all(&mut *connection)
    .await;
    let payloads = match payloads {
        Ok(payloads) => payloads,
        Err(error) => return internal(format!("knowledge entries could not be listed: {error}")),
    };
    let mut stored = Vec::with_capacity(payloads.len());
    for payload in &payloads {
        let mut entry = match decode(payload) {
            SpokeResult::Ok(entry) => entry,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        if let SpokeResult::Reject(reject) =
            hydrate_cognition_records(connection, collection, &mut entry).await
        {
            return SpokeResult::Reject(reject);
        }
        stored.push(entry);
    }
    SpokeResult::Ok(stored)
}

impl MindStore {
    /// A port for `scope`, sharing this store's single pool.
    pub fn knowledge(&self, scope: KnowledgeScope) -> ScopedKnowledgeStore {
        ScopedKnowledgeStore::new(self.pool.clone(), scope)
    }
}

pub(crate) fn internal<T>(message: impl Into<String>) -> SpokeResult<T> {
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

fn module_key() -> KnowledgeEntryModulesKey {
    "cognition".parse().expect("valid module namespace")
}

fn cognition_module(entry: &KnowledgeEntry) -> Option<Value> {
    match entry.modules.get(&module_key())? {
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

/// One entry of `collection`, read through the caller's connection: the scoped
/// port and the workflow binding share this read, and a settlement reads it at
/// the same point as the write it guards. The cognition module's records are
/// projected from the holder's record rows in append order.
pub async fn read_entry(
    connection: &mut SqliteConnection,
    collection: &str,
    entry_id: &str,
) -> SpokeResult<KnowledgeEntry> {
    let payload: Result<Option<String>, sqlx::Error> = sqlx::query_scalar(
        "SELECT payload FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
    )
    .bind(collection)
    .bind(entry_id)
    .fetch_optional(&mut *connection)
    .await;
    match payload {
        Ok(Some(payload)) => {
            let mut entry = match decode(&payload) {
                SpokeResult::Ok(entry) => entry,
                SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
            };
            if let SpokeResult::Reject(reject) =
                hydrate_cognition_records(connection, collection, &mut entry).await
            {
                return SpokeResult::Reject(reject);
            }
            SpokeResult::Ok(entry)
        }
        Ok(None) => spoke_reject(
            SpokeRejectCode::KnowledgeEntryNotFound,
            format!("no knowledge entry {entry_id} in collection {collection}"),
            None,
        ),
        Err(error) => internal(format!("knowledge entry could not be read: {error}")),
    }
}

/// Projects `modules.cognition.records` from the holder's record rows: the rows are
/// the storage authority and the payload keeps only the module envelope. A present
/// `records` member stays present (empty while there are no rows); an absent module
/// or member stays absent.
async fn hydrate_cognition_records(
    connection: &mut SqliteConnection,
    collection: &str,
    entry: &mut KnowledgeEntry,
) -> SpokeResult<()> {
    let key = module_key();
    let carries_records = matches!(
        entry.modules.get(&key),
        Some(KnowledgeEntryModulesValue::Object(module)) if module.contains_key("records")
    );
    if !carries_records {
        return SpokeResult::Ok(());
    }
    let records = match read_records(&mut *connection, collection, &entry.entry_id).await {
        SpokeResult::Ok(records) => records,
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    if let Some(KnowledgeEntryModulesValue::Object(module)) = entry.modules.get_mut(&key) {
        module.insert("records".to_string(), Value::Array(records));
    }
    SpokeResult::Ok(())
}

/// One holder's assigned revision and current-judgment pointer, read without hydrating its
/// payload or history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CarrierHeader {
    pub revision: u64,
    pub head_seq: Option<i64>,
}

/// One holder's header, read through the caller's executor: the append path compares the
/// revision and takes the next `append_seq` from here instead of reading the carrier history.
pub async fn read_carrier_header<'e, E>(
    executor: E,
    collection: &str,
    entry_id: &str,
) -> SpokeResult<CarrierHeader>
where
    E: Executor<'e, Database = Sqlite>,
{
    let header: Result<Option<(i64, Option<i64>)>, sqlx::Error> = sqlx::query_as(
        "SELECT revision, cognition_head_seq FROM knowledge_entries \
         WHERE collection = ? AND entry_id = ?",
    )
    .bind(collection)
    .bind(entry_id)
    .fetch_optional(executor)
    .await;
    match header {
        Ok(Some((revision, head_seq))) => SpokeResult::Ok(CarrierHeader {
            revision: revision as u64,
            head_seq,
        }),
        Ok(None) => spoke_reject(
            SpokeRejectCode::KnowledgeEntryNotFound,
            format!("no knowledge entry {entry_id} in collection {collection}"),
            None,
        ),
        Err(error) => internal(format!("carrier header could not be read: {error}")),
    }
}

/// Write one entry of `collection` against `expected_base_revision` through the
/// caller's transaction, assigning `1` on create and `stored + 1` on update.
///
/// The submitted cognition records are stored as their own rows: on create all of them, on
/// update only the suffix behind the immutable committed prefix — dropping, reordering or
/// rewriting a committed record is invalid input. The stored payload keeps the module
/// envelope without the inline array.
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
    let submitted_records = match entry.modules.get(&module_key()) {
        Some(KnowledgeEntryModulesValue::Object(module)) => module
            .get("records")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        _ => Vec::new(),
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

    // Record rows are immutable once committed: an update must carry every committed record
    // unchanged and may only append its validated suffix. Reading the rows is fine here — a full
    // DTO put processes its full payload anyway; only the dedicated append and current lookup are
    // bounded independently of history.
    let committed = match stored {
        Some(_) => match read_records(&mut **tx, collection, &entry.entry_id).await {
            SpokeResult::Ok(records) => records,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        },
        None => Vec::new(),
    };
    if submitted_records.len() < committed.len() {
        return spoke_reject(
            SpokeRejectCode::InvalidInput,
            format!(
                "knowledge entry {} drops {} committed cognition record(s)",
                entry.entry_id,
                committed.len() - submitted_records.len()
            ),
            None,
        );
    }
    for (index, (committed_record, submitted)) in
        committed.iter().zip(submitted_records.iter()).enumerate()
    {
        if committed_record != submitted {
            return spoke_reject(
                SpokeRejectCode::InvalidInput,
                format!(
                    "knowledge entry {} rewrites committed cognition record at position {index}",
                    entry.entry_id
                ),
                None,
            );
        }
    }

    // The stored payload keeps the cognition envelope only: the record rows carry the values,
    // and the empty `records` key preserves the member's presence.
    let mut stored_entry = entry.clone();
    strip_records(&mut stored_entry);
    let payload = match encode(&stored_entry) {
        SpokeResult::Ok(payload) => payload,
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    let created_at = encode_timestamp(entry.created_at.unwrap_or(now));
    let updated_at = encode_timestamp(now);
    let head = (!submitted_records.is_empty()).then_some(submitted_records.len() as i64);
    let suffix = &submitted_records[committed.len()..];

    if stored.is_none() {
        // The carrier row exists before the record rows that reference it; the head foreign
        // key can only be set once those rows exist.
        let written = sqlx::query(
            "INSERT INTO knowledge_entries \
             (collection, entry_id, revision, cognition_head_seq, payload, created_at, updated_at)
             VALUES (?, ?, ?, NULL, ?, ?, ?)",
        )
        .bind(collection)
        .bind(&entry.entry_id)
        .bind(revision as i64)
        .bind(&payload)
        .bind(&created_at)
        .bind(&updated_at)
        .execute(&mut **tx)
        .await;
        if let Err(error) = written {
            return internal(format!("knowledge entry could not be written: {error}"));
        }
    }
    for (offset, record) in suffix.iter().enumerate() {
        if let SpokeResult::Reject(reject) = insert_record(
            tx,
            collection,
            &entry.entry_id,
            committed.len() as i64 + offset as i64 + 1,
            record,
        )
        .await
        {
            return SpokeResult::Reject(reject);
        }
    }
    let written: Result<(), sqlx::Error> = match stored {
        Some(_) => sqlx::query(
            "UPDATE knowledge_entries \
             SET revision = ?, cognition_head_seq = ?, payload = ?, updated_at = ? \
             WHERE collection = ? AND entry_id = ?",
        )
        .bind(revision as i64)
        .bind(head)
        .bind(&payload)
        .bind(&updated_at)
        .bind(collection)
        .bind(&entry.entry_id)
        .execute(&mut **tx)
        .await
        .map(|_| ()),
        None => match head {
            None => Ok(()),
            Some(head) => sqlx::query(
                "UPDATE knowledge_entries SET cognition_head_seq = ? \
                 WHERE collection = ? AND entry_id = ?",
            )
            .bind(head)
            .bind(collection)
            .bind(&entry.entry_id)
            .execute(&mut **tx)
            .await
            .map(|_| ()),
        },
    };
    if let Err(error) = written {
        return internal(format!("knowledge entry could not be written: {error}"));
    }
    SpokeResult::Ok(entry)
}

/// Drops the inline records array from the stored payload's cognition module: the record rows
/// carry the values, the empty `records` key preserves the member's presence.
fn strip_records(entry: &mut KnowledgeEntry) {
    let key = module_key();
    if let Some(KnowledgeEntryModulesValue::Object(module)) = entry.modules.get_mut(&key) {
        if module.contains_key("records") {
            module.insert("records".to_string(), Value::Array(Vec::new()));
        }
    }
}

/// Append one validated cognition record inside the caller's transaction: compare the
/// carrier revision without reading history, insert the record row at `head + 1` and
/// advance the head and the outer revision together. Returns the assigned revision.
///
/// A stale expected revision is the generic stale rejection — `commit_cognition` rechecks
/// the revision first and reports the retryable conflict. A duplicate scoped `record_id`
/// is invalid input, not a storage failure. Rollback of the caller's transaction leaves
/// every row and the revision unchanged.
pub async fn append_cognition(
    tx: &mut Transaction<'_, Sqlite>,
    collection: &str,
    entry_id: &str,
    expected_revision: u64,
    record: &Value,
) -> SpokeResult<u64> {
    let header: Result<Option<(i64, Option<i64>, String)>, sqlx::Error> = sqlx::query_as(
        "SELECT revision, cognition_head_seq, payload FROM knowledge_entries \
         WHERE collection = ? AND entry_id = ?",
    )
    .bind(collection)
    .bind(entry_id)
    .fetch_optional(&mut **tx)
    .await;
    let (stored_revision, head_seq, payload) = match header {
        Ok(Some(header)) => header,
        Ok(None) => {
            return spoke_reject(
                SpokeRejectCode::KnowledgeEntryNotFound,
                format!("no knowledge entry {entry_id} in collection {collection}"),
                None,
            )
        }
        Err(error) => return internal(format!("carrier header could not be read: {error}")),
    };
    let stored_revision = stored_revision as u64;
    if stored_revision != expected_revision {
        return stale(
            collection,
            entry_id,
            Some(expected_revision),
            Some(stored_revision),
        );
    }
    // The record rules are the carrier contract's, not this adapter's: the same gate a full
    // put applies before any row write.
    if let SpokeResult::Reject(reject) = validate_cognition_record(record) {
        return SpokeResult::Reject(reject);
    }
    if let Some(violation) = carrier_record_violation(record, entry_id) {
        return spoke_reject(SpokeRejectCode::InvalidInput, violation, None);
    }
    let append_seq = head_seq.map_or(1, |head| head + 1);
    if let SpokeResult::Reject(reject) =
        insert_record(tx, collection, entry_id, append_seq, record).await
    {
        return SpokeResult::Reject(reject);
    }

    // Re-encode the carrier envelope with the advanced revision: records live in their own
    // rows, so this touches the carrier row only — never an earlier record.
    let mut entry = match decode(&payload) {
        SpokeResult::Ok(entry) => entry,
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    entry.revision = Some(stored_revision + 1);
    if !entry.modules.contains_key(&module_key()) {
        // First record of a carrier without a module: seed the same envelope a full put
        // would have committed.
        entry.modules.insert(
            module_key(),
            KnowledgeEntryModulesValue::Object(Map::from_iter([
                ("records".to_string(), Value::Array(Vec::new())),
                ("world_facts".to_string(), Value::Array(Vec::new())),
                ("extensions".to_string(), Value::Object(Map::new())),
            ])),
        );
    }
    strip_records(&mut entry);
    let payload = match encode(&entry) {
        SpokeResult::Ok(payload) => payload,
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    let written = sqlx::query(
        "UPDATE knowledge_entries \
         SET revision = ?, cognition_head_seq = ?, payload = ?, updated_at = ? \
         WHERE collection = ? AND entry_id = ?",
    )
    .bind((stored_revision + 1) as i64)
    .bind(append_seq)
    .bind(&payload)
    .bind(encode_timestamp(Utc::now()))
    .bind(collection)
    .bind(entry_id)
    .execute(&mut **tx)
    .await;
    if let Err(error) = written {
        return internal(format!("knowledge entry could not be written: {error}"));
    }
    SpokeResult::Ok(stored_revision + 1)
}

#[async_trait]
impl KnowledgeEntryPort for ScopedKnowledgeStore {
    async fn get_knowledge_entry(&self, entry_id: &str) -> SpokeResult<KnowledgeEntry> {
        let mut connection = match self.pool.acquire().await {
            Ok(connection) => connection,
            Err(error) => return internal(format!("knowledge entry could not be read: {error}")),
        };
        read_entry(&mut connection, &self.scope.collection, entry_id).await
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
