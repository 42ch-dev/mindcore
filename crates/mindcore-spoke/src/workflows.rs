//! The workflow binding: `memory.recall` plus the admitted knowledge read on the
//! caller's own transaction, projected onto the SPI's DTO-only seam.
//!
//! The adapter never opens or commits a transaction across provider work: one
//! call reads and returns, so an embedder, reasoner or policy hook runs with no
//! transaction open and the workflow can revalidate afterwards.

use async_trait::async_trait;
use chrono::Utc;
use serde_json::{Map, Value};
use spoke_operations::{GrantRequirement, SpokeReject, SpokeRejectCode, SpokeResult};
use spoke_schemas::{KnowledgeEntry, MemoryRecallRequest, Principal, RetainedRecord};
use sqlx::SqlitePool;

use mindcore_base::ids::SpaceId;
use mindcore_base::knowledge::KnowledgeScope;
use mindcore_spi::{RecallSnapshot, SpiError, WorkflowBinding};
use mindcore_sqlite::{list_entries_in, SqliteMemoryRepository};

use crate::admission;
use crate::reject::Reject;
use crate::{principal_id, MemoryAdapter, MemoryStage, Tx};

/// The knowledge-read capability. A grant carrying it admits its principal to
/// knowledge reads in that grant's space; the open capability vocabulary names
/// no knowledge capability, and no memory capability (`recall`, `records_read`)
/// stands in for it.
const KNOWLEDGE_READ: &str = "knowledge_read";

/// One collection together with the caller it is admitted to.
struct AdmittedCollection {
    caller: String,
    scope: KnowledgeScope,
}

/// The in-process workflow binding over one SQLite pool.
///
/// `knowledge` fixes the one collection this binding serves and its one caller:
/// without it the binding serves no knowledge at all, and a collection bound to
/// another caller is refused rather than read.
pub struct MemoryWorkflowBinding {
    adapter: MemoryAdapter,
    pool: SqlitePool,
    knowledge: Option<AdmittedCollection>,
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

    /// Fill the pack's knowledge slot from `scope`, admitted to `caller` only.
    ///
    /// The collection is one caller's knowledge scope: every read needs that
    /// caller to match and to hold a live [`KNOWLEDGE_READ`] grant in the
    /// request's space, so another principal's memory grant never reaches it.
    #[must_use]
    pub fn with_knowledge_collection(
        mut self,
        caller: impl Into<String>,
        scope: KnowledgeScope,
    ) -> Self {
        self.knowledge = Some(AdmittedCollection {
            caller: caller.into(),
            scope,
        });
        self
    }

    /// One knowledge read under the caller's own admission.
    ///
    /// Caller identity, live grant and collection are re-derived on every read,
    /// so the revalidation read after provider work runs the same admission and
    /// a revoked knowledge grant refuses the pack.
    async fn knowledge_slot(
        &self,
        tx: &mut Tx<'_>,
        principal: &Principal,
        request: &MemoryRecallRequest,
    ) -> Result<Vec<KnowledgeEntry>, SpiError> {
        let Some(admitted) = &self.knowledge else {
            return Ok(Vec::new());
        };
        let caller = principal_id(principal);
        if caller != admitted.caller.as_str() {
            return Err(not_authorized(format!(
                "principal {caller} is not admitted to knowledge collection {}",
                admitted.scope.collection
            )));
        }
        let space = SpaceId::new(request.space_id.to_string());
        let rows = admission::grants(&SqliteMemoryRepository, tx, &space)
            .await
            .map_err(|reject| SpiError::Settlement(reject.into_spoke()))?;
        let grants = admission::wire_grants(&rows)
            .map_err(|reject| SpiError::Settlement(reject.into_spoke()))?;
        let requirement = GrantRequirement {
            capability: KNOWLEDGE_READ,
            subject_id: None,
            context_ids: Vec::new(),
            reads_shared: false,
            subject_all: false,
        };
        if !admission::covered(&grants, caller, space.as_str(), Utc::now(), &requirement) {
            return Err(not_authorized(format!(
                "principal {caller} holds no live {KNOWLEDGE_READ} grant in space {}",
                space.as_str()
            )));
        }
        match list_entries_in(tx, &admitted.scope.collection).await {
            SpokeResult::Ok(entries) => Ok(entries),
            SpokeResult::Reject(reject) => Err(SpiError::Settlement(reject)),
        }
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
        let knowledge = self
            .knowledge_slot(&mut transaction, principal, request)
            .await?;
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

fn not_authorized(message: String) -> SpiError {
    SpiError::Settlement(Reject::not_authorized(message).into_spoke())
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
