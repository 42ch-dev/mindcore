//! The workflow binding: `memory.recall` plus the scoped knowledge read on the
//! caller's own transaction, projected onto the SPI's DTO-only seam.
//!
//! The adapter never opens or commits a transaction across provider work: one
//! call reads and returns, so an embedder, reasoner or policy hook runs with no
//! transaction open and the workflow can revalidate afterwards.

use async_trait::async_trait;
use serde_json::{Map, Value};
use spoke_operations::{SpokeReject, SpokeRejectCode, SpokeResult};
use spoke_schemas::{MemoryRecallRequest, Principal, RetainedRecord};
use sqlx::SqlitePool;

use mindcore_base::knowledge::KnowledgeScope;
use mindcore_spi::{RecallSnapshot, SpiError, WorkflowBinding};
use mindcore_sqlite::list_entries_in;

use crate::{MemoryAdapter, MemoryStage};

/// The in-process workflow binding over one SQLite pool.
///
/// `knowledge` fixes the one collection a caller may read: without it the
/// binding serves no knowledge at all, and the memory grant never widens the
/// knowledge slot because this path never reads a memory grant.
pub struct MemoryWorkflowBinding {
    adapter: MemoryAdapter,
    pool: SqlitePool,
    knowledge: Option<KnowledgeScope>,
}

impl MemoryWorkflowBinding {
    /// Bind to `pool`; `reflections_enabled` matches the adapter instance the
    /// host serves.
    #[must_use]
    pub fn new(pool: SqlitePool, reflections_enabled: bool) -> Self {
        Self {
            adapter: MemoryAdapter::new(reflections_enabled),
            pool,
            knowledge: None,
        }
    }

    /// Fill the pack's knowledge slot from `scope`'s collection.
    #[must_use]
    pub fn with_knowledge_collection(mut self, scope: KnowledgeScope) -> Self {
        self.knowledge = Some(scope);
        self
    }
}

#[async_trait]
impl WorkflowBinding for MemoryWorkflowBinding {
    async fn recall_snapshot(
        &self,
        principal: &Principal,
        request: &MemoryRecallRequest,
    ) -> Result<RecallSnapshot, SpiError> {
        let raw = serde_json::to_value(request)
            .map_err(|error| internal(format!("recall request could not be rendered: {error}")))?;
        let mut transaction =
            self.pool.begin().await.map_err(|error| {
                internal(format!("recall transaction could not begin: {error}"))
            })?;
        let stage = match self.adapter.recall(&mut transaction, principal, raw).await {
            SpokeResult::Ok(stage) => stage,
            SpokeResult::Reject(reject) => return Err(SpiError::Settlement(reject)),
        };
        let records = match stage {
            MemoryStage::Ready(body) => decode_records(body)?,
            other => {
                return Err(internal(format!(
                    "memory.recall returned an unserveable stage: {other:?}"
                )))
            }
        };
        let knowledge = match &self.knowledge {
            None => Vec::new(),
            Some(scope) => match list_entries_in(&mut transaction, &scope.collection).await {
                SpokeResult::Ok(entries) => entries,
                SpokeResult::Reject(reject) => return Err(SpiError::Settlement(reject)),
            },
        };
        transaction
            .commit()
            .await
            .map_err(|error| internal(format!("recall transaction could not commit: {error}")))?;
        Ok(RecallSnapshot { records, knowledge })
    }
}

fn decode_records(body: Value) -> Result<Vec<RetainedRecord>, SpiError> {
    let Value::Object(mut body) = body else {
        return Err(internal(
            "memory.recall ready stage is not a response object".to_string(),
        ));
    };
    let records = body
        .remove("records")
        .ok_or_else(|| internal("memory.recall ready stage carries no records".to_string()))?;
    serde_json::from_value(records)
        .map_err(|error| internal(format!("memory.recall records do not decode: {error}")))
}

fn internal(message: String) -> SpiError {
    let mut details = Map::new();
    details.insert("retry_class".to_string(), Value::String("retryable".into()));
    SpiError::Settlement(SpokeReject {
        code: SpokeRejectCode::InternalError,
        message,
        details: Some(details),
    })
}
