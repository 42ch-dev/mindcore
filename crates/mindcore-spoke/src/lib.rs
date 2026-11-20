//! SPOKE 0.16.1 memory protocol adapter over the borrowed-transaction
//! [`MemoryRepository`](mindcore_base::ir::MemoryRepository).
//!
//! The adapter owns every protocol rule — wire validation, the wire↔IR
//! boundary, reject mapping, admission, replay and staging — and delegates
//! storage only. It never opens, commits or rolls back a transaction: a
//! successful stage is the caller's to commit, and a [`MemoryStage`] value
//! (never wire data) says whether that stage is durable as-is or still owes a
//! publication phase.

mod admission;
mod content;
mod control;
mod effects;
mod policy;
mod project;
mod reflection;
mod reject;
mod wire;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde_json::Value;
use spoke_operations::{SpokeRejectCode, SpokeResult};
use spoke_schemas::Principal;
use sqlx::{Sqlite, Transaction};

use mindcore_base::ids::SpaceId;
use mindcore_base::ir::{ReceiptFailure, ReceiptState, ReviewReceiptIr};
use mindcore_sqlite::SqliteMemoryRepository;

use crate::admission::Op;
use crate::reject::{AResult, Reject};

/// The caller-owned transaction every entry point reads and writes through.
pub type Tx<'a> = Transaction<'a, Sqlite>;

/// The outcome of one operation, held inside the caller's transaction.
#[derive(Debug)]
pub enum MemoryStage {
    /// The pinned success response; the caller may commit and return it.
    Ready(Value),
    /// A journaled effect is staged: pending receipt, durable intent and the
    /// file targets the caller must publish before settling the intent.
    JournalPending { space: SpaceId, receipt_id: String },
    /// A Reflection generation is leased: the caller commits transaction 1,
    /// composes the result outside it, then completes in transaction 2.
    ReflectionPending(ReflectionWork),
}

/// Everything the second Reflection transaction needs after transaction 1
/// committed: the leased pending receipt and the basis it was pinned to.
#[derive(Debug, Clone)]
pub struct ReflectionWork {
    pub space: SpaceId,
    pub receipt_id: String,
    /// The lease owner stamped on the pending receipt (§7.0.3).
    pub owner_id: String,
    pub subject_id: String,
    pub selection: Value,
    pub filter: Option<Value>,
    pub basis: Value,
    pub engine_policy_version: &'static str,
}

/// The in-process SPOKE memory adapter.
pub struct MemoryAdapter {
    repository: SqliteMemoryRepository,
    reflections: bool,
}

impl MemoryAdapter {
    /// `reflections_enabled` is the instance's own policy switch: it decides
    /// both what `describe` advertises and whether §7.25–§7.27 are served.
    #[must_use]
    pub fn new(reflections_enabled: bool) -> Self {
        Self {
            repository: SqliteMemoryRepository,
            reflections: reflections_enabled,
        }
    }

    #[must_use]
    pub fn reflections_enabled(&self) -> bool {
        self.reflections
    }

    /// Validate and validate-again one rebuilt success body against its pinned
    /// response type.
    pub(crate) fn ready<T: DeserializeOwned>(body: Value) -> AResult<MemoryStage> {
        Ok(MemoryStage::Ready(project::checked::<T>(body)?))
    }

    /// Validate one already-built success body against an op-specific response
    /// type; a staged result passes through untouched.
    pub(crate) fn recheck<T: DeserializeOwned>(stage: MemoryStage) -> AResult<MemoryStage> {
        match stage {
            MemoryStage::Ready(body) => Self::ready::<T>(body),
            stage => Ok(stage),
        }
    }

    /// §7.0.5: a receipt the dedup namespace holds resolves the key. A
    /// committed replay is a published success response, so it passes the
    /// operation's pinned response type before it can leave the adapter.
    pub(crate) fn replay<T: DeserializeOwned>(receipt: &ReviewReceiptIr) -> AResult<MemoryStage> {
        match &receipt.state {
            ReceiptState::Committed { .. } => Self::ready::<T>(project::receipt_body(receipt)),
            ReceiptState::Failed { error, .. } => Err(recorded_failure(error)),
            ReceiptState::Pending => Err(Reject::uncertain(format!(
                "receipt {} is still pending",
                receipt.receipt_id
            ))),
        }
    }

    /// Project one op's request facts; the digest binds the original request.
    pub(crate) fn op<'a>(
        space: &str,
        principal: &'a str,
        operation: &'static str,
        idempotency_key: Option<&str>,
        request: &Value,
    ) -> AResult<Op<'a>> {
        Ok(Op {
            space: SpaceId::new(space),
            requester: principal,
            operation,
            idempotency_key: idempotency_key.map(str::to_string),
            payload_digest: policy::payload_digest(request)?,
            now: chrono::Utc::now(),
        })
    }

    /// An optional op this build does not serve (§4.1): no storage, no receipt.
    fn refuse(op: &'static str) -> SpokeResult<MemoryStage> {
        SpokeResult::Reject(Reject::unsupported(op).into_spoke())
    }

    // ── O2: transfer is not served by this build ──────────────────────────

    pub async fn create_export(
        &self,
        _tx: &mut Tx<'_>,
        _principal: &Principal,
        _request: Value,
    ) -> SpokeResult<MemoryStage> {
        Self::refuse("memory.exports.create")
    }

    pub async fn get_export(
        &self,
        _tx: &mut Tx<'_>,
        _principal: &Principal,
        _request: Value,
    ) -> SpokeResult<MemoryStage> {
        Self::refuse("memory.exports.get")
    }

    pub async fn stage_import(
        &self,
        _tx: &mut Tx<'_>,
        _principal: &Principal,
        _request: Value,
    ) -> SpokeResult<MemoryStage> {
        Self::refuse("memory.imports.stage")
    }

    pub async fn activate_import(
        &self,
        _tx: &mut Tx<'_>,
        _principal: &Principal,
        _request: Value,
    ) -> SpokeResult<MemoryStage> {
        Self::refuse("memory.imports.activate")
    }

    pub async fn discard_import(
        &self,
        _tx: &mut Tx<'_>,
        _principal: &Principal,
        _request: Value,
    ) -> SpokeResult<MemoryStage> {
        Self::refuse("memory.imports.discard")
    }
}

/// The authenticated principal the caller supplied; never payload-asserted.
pub(crate) fn principal_id(principal: &Principal) -> &str {
    principal.principal_id.as_str()
}

fn recorded_failure(failure: &ReceiptFailure) -> Reject {
    let code =
        SpokeRejectCode::try_from_str(&failure.code).unwrap_or(SpokeRejectCode::InternalError);
    let class = failure
        .details
        .get("retry_class")
        .and_then(Value::as_str)
        .unwrap_or("terminal")
        .to_string();
    let mut reject = Reject::terminal(code, failure.message.clone());
    reject
        .details
        .insert("retry_class".to_string(), Value::String(class));
    for (key, value) in &failure.details {
        if key != "retry_class" {
            reject.details.insert(key.clone(), value.clone());
        }
    }
    reject
}

/// A fresh identity for one stored row. The counter keeps it unique within the
/// process and the nanosecond stamp across processes.
pub(crate) fn mint(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default();
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{nanos:x}{count:x}")
}

/// One stored row's revision bumped by one.
pub(crate) fn bump(revision: &serde_json::Number) -> serde_json::Number {
    serde_json::Number::from(revision.as_u64().unwrap_or(0).saturating_add(1))
}
