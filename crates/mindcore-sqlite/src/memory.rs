//! `MemoryStore` adapter: statements over the shared pool, SQLx errors mapped to `MemoryError::Store`.

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::{FromRow, Sqlite, SqlitePool};

use mindcore_base::ids::{ContextId, SpaceId, SubjectId};
use mindcore_base::memory::{MemoryCandidate, MemoryError, MemoryRecord, MemoryStore, RecallQuery};

use crate::store::MindStore;

fn store_error(error: sqlx::Error) -> MemoryError {
    MemoryError::Store(Box::new(error))
}

// Fixed-width fractional seconds keep TEXT ordering identical to instant ordering.
fn encode_timestamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn decode_timestamp(text: &str) -> Result<DateTime<Utc>, MemoryError> {
    DateTime::parse_from_rfc3339(text)
        .map(|at| at.with_timezone(&Utc))
        .map_err(|error| MemoryError::Store(Box::new(error)))
}

// Chrono writes years outside 0000..=9999 with a signed expanded year ("+10000-…") that the strict
// parser cannot read, so prove the encoded text decodes back before a row claims the candidate id.
fn encode_readable_timestamp(at: DateTime<Utc>) -> Result<String, MemoryError> {
    let text = encode_timestamp(at);
    decode_timestamp(&text).map_err(|_| MemoryError::UnrepresentableTimestamp)?;
    Ok(text)
}

#[derive(FromRow)]
struct CandidateRow {
    candidate_id: String,
    space_id: String,
    subject_id: String,
    context_id: String,
    content: String,
    observed_at: String,
}

impl CandidateRow {
    fn into_candidate(self) -> Result<MemoryCandidate, MemoryError> {
        Ok(MemoryCandidate {
            candidate_id: self.candidate_id,
            space: SpaceId::new(self.space_id),
            subject: SubjectId::new(self.subject_id),
            context: ContextId::new(self.context_id),
            content: self.content,
            observed_at: decode_timestamp(&self.observed_at)?,
        })
    }
}

#[derive(FromRow)]
struct RecordRow {
    record_id: String,
    candidate_id: String,
    space_id: String,
    subject_id: String,
    context_id: String,
    content: String,
    recorded_at: String,
}

impl RecordRow {
    fn into_record(self) -> Result<MemoryRecord, MemoryError> {
        Ok(MemoryRecord {
            record_id: self.record_id,
            candidate_id: self.candidate_id,
            space: SpaceId::new(self.space_id),
            subject: SubjectId::new(self.subject_id),
            context: ContextId::new(self.context_id),
            content: self.content,
            recorded_at: decode_timestamp(&self.recorded_at)?,
        })
    }
}

async fn candidate_by_id<'e, E>(
    executor: E,
    candidate_id: &str,
) -> Result<Option<MemoryCandidate>, MemoryError>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    sqlx::query_as::<_, CandidateRow>(
        "SELECT candidate_id, space_id, subject_id, context_id, content, observed_at
         FROM memory_candidates WHERE candidate_id = ?",
    )
    .bind(candidate_id)
    .fetch_optional(executor)
    .await
    .map_err(store_error)?
    .map(CandidateRow::into_candidate)
    .transpose()
}

async fn record_by_id<'e, E>(
    executor: E,
    record_id: &str,
) -> Result<Option<MemoryRecord>, MemoryError>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    sqlx::query_as::<_, RecordRow>(
        "SELECT record_id, candidate_id, space_id, subject_id, context_id, content, recorded_at
         FROM memory_records WHERE record_id = ?",
    )
    .bind(record_id)
    .fetch_optional(executor)
    .await
    .map_err(store_error)?
    .map(RecordRow::into_record)
    .transpose()
}

async fn capture_candidate(
    pool: &SqlitePool,
    candidate: MemoryCandidate,
) -> Result<(), MemoryError> {
    let observed_at = encode_readable_timestamp(candidate.observed_at)?;
    let inserted = sqlx::query(
        "INSERT INTO memory_candidates (candidate_id, space_id, subject_id, context_id, content, observed_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(candidate_id) DO NOTHING",
    )
    .bind(&candidate.candidate_id)
    .bind(candidate.space.as_str())
    .bind(candidate.subject.as_str())
    .bind(candidate.context.as_str())
    .bind(&candidate.content)
    .bind(observed_at)
    .execute(pool)
    .await
    .map_err(store_error)?
    .rows_affected();

    if inserted == 1 {
        return Ok(());
    }

    let existing = candidate_by_id(pool, &candidate.candidate_id)
        .await?
        .ok_or_else(|| MemoryError::CandidateNotFound(candidate.candidate_id.clone()))?;
    if existing == candidate {
        Ok(())
    } else {
        Err(MemoryError::IdempotencyConflict)
    }
}

async fn retain_candidate(
    pool: &SqlitePool,
    candidate_id: &str,
    request_id: &str,
) -> Result<MemoryRecord, MemoryError> {
    // Take the write reservation up front: two handles on one file must not both read "no receipt"
    // and then lose the lock upgrade. Losers wait on busy_timeout and see the committed winner.
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(store_error)?;

    let receipt: Option<(String, String)> =
        sqlx::query_as("SELECT candidate_id, record_id FROM review_receipts WHERE request_id = ?")
            .bind(request_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(store_error)?;

    if let Some((reviewed_candidate_id, record_id)) = receipt {
        if reviewed_candidate_id != candidate_id {
            return Err(MemoryError::IdempotencyConflict);
        }
        let record = record_by_id(&mut *transaction, &record_id)
            .await?
            .ok_or_else(|| {
                MemoryError::Store(Box::new(std::io::Error::other(format!(
                    "receipt {request_id} points at missing record {record_id}"
                ))))
            })?;
        transaction.commit().await.map_err(store_error)?;
        return Ok(record);
    }

    let candidate = candidate_by_id(&mut *transaction, candidate_id)
        .await?
        .ok_or_else(|| MemoryError::CandidateNotFound(candidate_id.to_string()))?;

    let already_retained: Option<String> =
        sqlx::query_scalar("SELECT record_id FROM memory_records WHERE candidate_id = ?")
            .bind(candidate_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(store_error)?;
    if already_retained.is_some() {
        return Err(MemoryError::CandidateAlreadyReviewed);
    }

    let MemoryCandidate {
        candidate_id: captured_id,
        space,
        subject,
        context,
        content,
        ..
    } = candidate;
    let record = MemoryRecord {
        record_id: format!("rec_{captured_id}"),
        candidate_id: captured_id,
        space,
        subject,
        context,
        content,
        recorded_at: Utc::now(),
    };

    sqlx::query(
        "INSERT INTO memory_records (record_id, candidate_id, space_id, subject_id, context_id, content, recorded_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&record.record_id)
    .bind(&record.candidate_id)
    .bind(record.space.as_str())
    .bind(record.subject.as_str())
    .bind(record.context.as_str())
    .bind(&record.content)
    .bind(encode_timestamp(record.recorded_at))
    .execute(&mut *transaction)
    .await
    .map_err(store_error)?;

    sqlx::query(
        "INSERT INTO review_receipts (request_id, candidate_id, record_id) VALUES (?, ?, ?)",
    )
    .bind(request_id)
    .bind(&record.candidate_id)
    .bind(&record.record_id)
    .execute(&mut *transaction)
    .await
    .map_err(store_error)?;

    transaction.commit().await.map_err(store_error)?;
    Ok(record)
}

async fn recall_records(
    pool: &SqlitePool,
    query: &RecallQuery,
) -> Result<Vec<MemoryRecord>, MemoryError> {
    let context_id = query.context.as_ref().map(ContextId::as_str);
    let rows = sqlx::query_as::<_, RecordRow>(
        "SELECT record_id, candidate_id, space_id, subject_id, context_id, content, recorded_at
         FROM memory_records
         WHERE space_id = ? AND subject_id = ? AND (? IS NULL OR context_id = ?)
         ORDER BY recorded_at ASC, record_id ASC",
    )
    .bind(query.space.as_str())
    .bind(query.subject.as_str())
    .bind(context_id)
    .bind(context_id)
    .fetch_all(pool)
    .await
    .map_err(store_error)?;
    rows.into_iter().map(RecordRow::into_record).collect()
}

#[async_trait]
impl MemoryStore for MindStore {
    async fn capture(&self, candidate: MemoryCandidate) -> Result<(), MemoryError> {
        capture_candidate(&self.pool, candidate).await
    }

    async fn candidate(&self, candidate_id: &str) -> Result<Option<MemoryCandidate>, MemoryError> {
        candidate_by_id(&self.pool, candidate_id).await
    }

    async fn retain(
        &self,
        candidate_id: &str,
        request_id: &str,
    ) -> Result<MemoryRecord, MemoryError> {
        retain_candidate(&self.pool, candidate_id, request_id).await
    }

    async fn recall(&self, query: &RecallQuery) -> Result<Vec<MemoryRecord>, MemoryError> {
        recall_records(&self.pool, query).await
    }
}
