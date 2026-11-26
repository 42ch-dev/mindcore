//! The Mind handle: the in-process facade over one Mind. `open` is one registry read —
//! `minds::get` under the caller's live grants — and the registry's own refusal answers as it
//! is. The handle keeps the row that read answers and nothing about grants: it caches no
//! grant and acts on no status from the row, so a frozen Mind opens and every later operation
//! re-reads the live state on its own transaction.
//!
//! The memory operations are composition, not a second write path: `capture` and `recall`
//! hand the adapter one transaction each with the Mind's id as `space_id`, and `review` runs
//! the staged-settlement fold — stage inside one transaction, publish before that staging is
//! durable so a refused publication leaves no record, then settle the receipt on a second
//! transaction. No method takes a collection, an owner or a viewpoint, and none chooses a
//! subject: the caller names it every time.
//!
//! The knowledge operations compose the scoped store the same way: each admits the caller under
//! `MemoryAdapter::require_space_grants` on its own transaction — space-wide `knowledge_read` for
//! the reads, `knowledge_write` for the put — then reads or writes through the store's
//! borrowed-transaction entry points on that same transaction, against the collection the row
//! resolved at open. Revisions and refusals are the store's own.

use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::{MindId, SpaceId};
use mindcore_base::ir::{MemoryRepository, ReceiptKey, ReceiptRead};
use mindcore_spi::{
    DeliberateOutcome, DeliberateRequest, MindWorkflows, SpiError, SpiMindWorkflows,
};
use mindcore_spoke::{Cognition, MemoryAdapter, MemoryStage, MemoryWorkflowBinding};
use mindcore_sqlite::minds::Mind as MindRow;
use mindcore_sqlite::{
    list_entries_in, read_entry, read_entry_at_revision, write_entry, MindStore,
    SqliteMemoryRepository,
};
use spoke_operations::{SpokeReject, SpokeRejectCode, SpokeResult};
use spoke_schemas::data::cognition::{CognitionReference, CognitionTarget};
use spoke_schemas::memory_candidates_review_request::Decision;
use spoke_schemas::{
    KnowledgeEntry, MemoryCandidatesReviewResponse, MemoryCaptureResponse, MemoryRecallResponse,
    Principal,
};
use sqlx::SqlitePool;
use std::sync::Arc;

use crate::tools::workflow_reject;

/// The host's journal publisher: the handle never invents one.
type JournalPublisher = dyn Fn(&[Value]) -> Result<(), SpiError> + Send + Sync;

/// A caller's handle to one Mind: the pool, the caller and the row `minds::get` answered at
/// open.
///
/// The handle is not a grant — it confers no access beyond what the caller's live grants admit
/// on every operation — and its row is a snapshot: the status it carries is the one at open,
/// never re-read here.
pub struct Mind {
    pool: SqlitePool,
    caller: CallerId,
    row: MindRow,
    adapter: MemoryAdapter,
    publisher: Option<Arc<JournalPublisher>>,
    providers: crate::MindProviders,
}

/// One memory capture in plain arguments; the handle renders the wire body with the Mind's
/// id as `space_id`.
pub struct Capture<'a> {
    /// The subject this capture belongs to; the caller's, never defaulted.
    pub subject: &'a str,
    /// The context the capture is placed in, or `None` for the shared placement.
    pub context: Option<&'a str>,
    /// The content subtree of the SPOKE capture request (`text` plus optional members).
    pub content: &'a Value,
    pub occurred_at: Option<DateTime<Utc>>,
    pub kind_hint: Option<&'a str>,
    pub idempotency_key: &'a str,
}

/// One memory recall in plain arguments; the handle renders the wire body with the Mind's
/// id as `space_id`.
pub struct Recall<'a> {
    /// The subject whose records are read; the caller's, never defaulted.
    pub subject: &'a str,
    /// The §6.12 read selection member (`{"mode": ...}`), exactly as the wire carries it.
    pub selection: &'a Value,
    pub max_items: Option<u64>,
    pub max_bytes: Option<u64>,
    pub query: Option<&'a str>,
    pub cursor: Option<&'a str>,
}

/// One deliberation in plain arguments: a target and the evidence the verdict
/// may rest on. There is no subject: the facade's `deliberate` is space-level,
/// and the request carries the Mind's id as its assessment context.
pub struct Deliberation<'a> {
    /// The cognition target being assessed.
    pub target: &'a CognitionTarget,
    /// Evidence admitted for this deliberation.
    pub evidence: &'a [mindcore_spi::Evidence<'a>],
    /// Elapsed-time budget for the whole call.
    pub timeout: std::time::Duration,
}

fn reject<T>(code: SpokeRejectCode, message: impl Into<String>, class: &str) -> SpokeResult<T> {
    let mut details = Map::new();
    details.insert("retry_class".to_string(), Value::String(class.to_string()));
    SpokeResult::Reject(SpokeReject {
        code,
        message: message.into(),
        details: Some(details),
    })
}

/// A failure before anything can have committed: the caller may retry the same request.
fn retryable_internal<T>(message: impl Into<String>) -> SpokeResult<T> {
    reject(SpokeRejectCode::InternalError, message, "retryable")
}

/// A failure after the staging commit: rows may be durable, so the outcome is unknown and
/// the caller resolves it instead of retrying blind.
fn uncertain<T>(message: impl Into<String>) -> SpokeResult<T> {
    reject(SpokeRejectCode::OutcomeUncertain, message, "uncertain")
}

/// The authenticated caller rendered as the adapter's wire `Principal`; the id is the
/// authenticated identity the embedding host supplied at open, never payload-asserted.
fn principal(caller: &CallerId) -> Principal {
    serde_json::from_value(json!({
        "principal_id": caller.as_str(),
        "extensions": {},
    }))
    .expect("a caller id is a valid principal")
}

/// The ready body of one adapter operation, decoded into its pinned response type. Any other
/// stage here is a composition defect: these operations never journal.
fn ready<T: serde::de::DeserializeOwned>(stage: SpokeResult<MemoryStage>) -> SpokeResult<T> {
    match stage {
        SpokeResult::Ok(MemoryStage::Ready(body)) => match serde_json::from_value(body) {
            Ok(response) => SpokeResult::Ok(response),
            Err(error) => retryable_internal(format!(
                "the adapter's response does not match its schema: {error}"
            )),
        },
        SpokeResult::Ok(stage) => retryable_internal(format!(
            "the adapter answered an unexpected stage: {stage:?}"
        )),
        SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
    }
}

impl Mind {
    /// Open the Mind `mind_id` for `caller`: one call to `minds::get` under the caller's live
    /// grants. A Mind the caller cannot see answers the registry's own `NotFound`; a frozen
    /// Mind opens.
    pub async fn open(store: &MindStore, caller: &CallerId, mind_id: &MindId) -> SpokeResult<Self> {
        match crate::minds::get(store, caller, mind_id).await {
            SpokeResult::Ok(row) => SpokeResult::Ok(Mind {
                pool: store.pool(),
                caller: caller.clone(),
                row,
                adapter: MemoryAdapter::new(false),
                publisher: None,
                providers: crate::MindProviders::default(),
            }),
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        }
    }

    /// Publish journaled file targets into the host's medium.
    ///
    /// The closure returns `Ok(())` only after every target's exact bytes are durable. It runs
    /// while the handle holds the store's one connection, so it must not touch the store.
    /// `review` without a publisher is refused before anything is staged.
    #[must_use]
    pub fn with_journal_publisher(
        mut self,
        publish: impl Fn(&[Value]) -> Result<(), SpiError> + Send + Sync + 'static,
    ) -> Self {
        self.publisher = Some(Arc::new(publish));
        self
    }

    /// The providers `deliberate` reasons with: the host's own embedder, reasoner,
    /// settlement policy and lens. The handle never invents them; `deliberate`
    /// without a reasoner is refused with the workflow's own unavailability.
    #[must_use]
    pub fn with_providers(mut self, providers: crate::MindProviders) -> Self {
        self.providers = providers;
        self
    }

    /// The entry in the Mind's collection that holds its current judgment, as the row named it
    /// at open.
    pub fn carrier_entry_id(&self) -> &str {
        &self.row.carrier_entry_id
    }

    /// The registry row `open` resolved, as it was at open.
    pub fn row(&self) -> &MindRow {
        &self.row
    }

    /// Capture one memory into this Mind: the adapter's `capture` over one immediate-write
    /// transaction, with the Mind's id as `space_id`.
    pub async fn capture(&self, capture: &Capture<'_>) -> SpokeResult<MemoryCaptureResponse> {
        let mut request = json!({
            "space_id": self.row.id.as_str(),
            "subject_id": capture.subject,
            "capture_scope": match capture.context {
                Some(context) => json!({ "visibility": "context", "context_id": context }),
                None => json!({ "visibility": "shared" }),
            },
            "content": capture.content,
            "idempotency_key": capture.idempotency_key,
        });
        let object = request.as_object_mut().expect("the request is an object");
        if let Some(kind_hint) = capture.kind_hint {
            object.insert("kind_hint".to_string(), json!(kind_hint));
        }
        if let Some(occurred_at) = capture.occurred_at {
            object.insert("occurred_at".to_string(), json!(occurred_at));
        }

        let mut transaction = match self.pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(transaction) => transaction,
            Err(error) => {
                return retryable_internal(format!("capture transaction could not begin: {error}"))
            }
        };
        let stage = self
            .adapter
            .capture(&mut transaction, &principal(&self.caller), request)
            .await;
        if stage.is_reject() {
            drop(transaction);
            return ready(stage);
        }
        if let Err(error) = transaction.commit().await {
            return uncertain(format!("capture committed an unknown outcome: {error}"));
        }
        ready(stage)
    }

    /// Recall this Mind's live records: the adapter's `recall` over one read transaction,
    /// rolled back after the read.
    pub async fn recall(&self, recall: &Recall<'_>) -> SpokeResult<MemoryRecallResponse> {
        let mut budget = Map::new();
        if let Some(max_items) = recall.max_items {
            budget.insert("max_items".to_string(), json!(max_items));
        }
        if let Some(max_bytes) = recall.max_bytes {
            budget.insert("max_bytes".to_string(), json!(max_bytes));
        }
        let mut request = json!({
            "space_id": self.row.id.as_str(),
            "subject_id": recall.subject,
            "selection": recall.selection,
            "budget": Value::Object(budget),
        });
        let object = request.as_object_mut().expect("the request is an object");
        if let Some(query) = recall.query {
            object.insert("query".to_string(), json!(query));
        }
        if let Some(cursor) = recall.cursor {
            object.insert("cursor".to_string(), json!(cursor));
        }

        let mut transaction = match self.pool.begin().await {
            Ok(transaction) => transaction,
            Err(error) => {
                return retryable_internal(format!("recall transaction could not begin: {error}"))
            }
        };
        let stage = self
            .adapter
            .recall(&mut transaction, &principal(&self.caller), request)
            .await;
        drop(transaction);
        ready(stage)
    }

    /// Read one entry of this Mind's collection: `knowledge_read` admitted on one read
    /// transaction, then the store's `read_entry` — or `read_entry_at_revision` when a
    /// revision is named — on it, rolled back after the read.
    pub async fn knowledge_get(
        &self,
        entry_id: &str,
        revision: Option<u64>,
    ) -> SpokeResult<KnowledgeEntry> {
        let mut transaction = match self.pool.begin().await {
            Ok(transaction) => transaction,
            Err(error) => {
                return retryable_internal(format!(
                    "knowledge read transaction could not begin: {error}"
                ))
            }
        };
        if let Err(error) = self
            .adapter
            .require_space_grants(
                &mut transaction,
                &principal(&self.caller),
                self.row.id.as_str(),
                &["knowledge_read"],
            )
            .await
        {
            drop(transaction);
            return match error {
                SpiError::Settlement(reject) => SpokeResult::Reject(reject),
                other => retryable_internal(format!("knowledge admission failed: {other}")),
            };
        }
        let result = match revision {
            Some(revision) => {
                read_entry_at_revision(
                    &mut transaction,
                    &self.row.knowledge.collection,
                    entry_id,
                    revision,
                )
                .await
            }
            None => read_entry(&mut transaction, &self.row.knowledge.collection, entry_id).await,
        };
        drop(transaction);
        result
    }

    /// Write one entry of this Mind's collection: `knowledge_write` admitted on one
    /// immediate-write transaction, then the store's `write_entry` on it, committed. The
    /// revision check and every refusal are the store's own.
    pub async fn knowledge_put(
        &self,
        entry: KnowledgeEntry,
        expected_base_revision: Option<u64>,
    ) -> SpokeResult<KnowledgeEntry> {
        let mut transaction = match self.pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(transaction) => transaction,
            Err(error) => {
                return retryable_internal(format!(
                    "knowledge write transaction could not begin: {error}"
                ))
            }
        };
        if let Err(error) = self
            .adapter
            .require_space_grants(
                &mut transaction,
                &principal(&self.caller),
                self.row.id.as_str(),
                &["knowledge_write"],
            )
            .await
        {
            drop(transaction);
            return match error {
                SpiError::Settlement(reject) => SpokeResult::Reject(reject),
                other => retryable_internal(format!("knowledge admission failed: {other}")),
            };
        }
        let written = write_entry(
            &mut transaction,
            &self.row.knowledge.collection,
            entry,
            expected_base_revision,
        )
        .await;
        if written.is_reject() {
            drop(transaction);
            return written;
        }
        if let Err(error) = transaction.commit().await {
            return uncertain(format!(
                "knowledge write committed an unknown outcome: {error}"
            ));
        }
        written
    }

    /// Every entry of this Mind's collection: `knowledge_read` admitted on one read
    /// transaction, then the store's `list_entries_in` on it, rolled back after the read.
    pub async fn knowledge_list(&self) -> SpokeResult<Vec<KnowledgeEntry>> {
        let mut transaction = match self.pool.begin().await {
            Ok(transaction) => transaction,
            Err(error) => {
                return retryable_internal(format!(
                    "knowledge list transaction could not begin: {error}"
                ))
            }
        };
        if let Err(error) = self
            .adapter
            .require_space_grants(
                &mut transaction,
                &principal(&self.caller),
                self.row.id.as_str(),
                &["knowledge_read"],
            )
            .await
        {
            drop(transaction);
            return match error {
                SpiError::Settlement(reject) => SpokeResult::Reject(reject),
                other => retryable_internal(format!("knowledge admission failed: {other}")),
            };
        }
        let entries = list_entries_in(&mut transaction, &self.row.knowledge.collection).await;
        drop(transaction);
        entries
    }

    /// Review one pending candidate and answer the settled receipt, not a `MemoryStage`.
    ///
    /// The fold stages the whole decision inside one immediate-write transaction, reads the
    /// staged intent on it and publishes the owed targets before the staging is durable: a
    /// refused publication rolls everything back — no record, no intent, a pending candidate —
    /// and the same idempotency key can be repeated. Only after a successful publication does
    /// the staging commit; settlement then runs on a second transaction, and any failure from
    /// there answers `OUTCOME_UNCERTAIN` because durable work may be outstanding.
    pub async fn review(
        &self,
        candidate_id: &str,
        decision: &Decision,
        idempotency_key: &str,
    ) -> SpokeResult<MemoryCandidatesReviewResponse> {
        let publisher = match self.publisher.as_ref() {
            Some(publisher) => publisher,
            None => {
                return reject(
                    SpokeRejectCode::InvalidInput,
                    "this Mind handle has no journal publisher, so it cannot settle a review",
                    "terminal",
                )
            }
        };
        let space = SpaceId::new(self.row.id.as_str());
        let caller = principal(&self.caller);
        let mut transaction = match self.pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(transaction) => transaction,
            Err(error) => {
                return retryable_internal(format!(
                    "review staging transaction could not begin: {error}"
                ))
            }
        };

        // 1. Read the candidate's consumption status and revision as metadata. The answer
        //    decides how the revision is established:
        //    - A pending (or absent) candidate is fresh work: the facade reads it through the
        //      adapter's `get_candidate`, whose withhold refuses while any intent of the
        //      candidate's subject is unsettled — the refusal fresh reviews answered before
        //      this facade ever probed receipts.
        //    - A consumed candidate can only be a replay (settled receipt) or a retry of a
        //      still-pending receipt, and a receipt is not subject content: the content
        //      withhold must not gate it, so the revision is derived as metadata — exactly one
        //      less than the consumed revision, because consumption is a candidate's only
        //      revision bump and that is the only revision any recorded review payload for
        //      this candidate can carry.
        //    Admission itself does not move: `review_candidate` authorizes the write, probes
        //    the receipt first and applies every fresh-work gate on its own. A same-key replay
        //    is digest-identical, so either path can only rebuild the original
        //    `expected_revision` — a wrong value can never reach a receipt.
        let status: Option<(i64, String)> = match sqlx::query_as(
            "SELECT revision, status FROM memory_candidates WHERE space_id = ? AND candidate_id = ?",
        )
        .bind(space.as_str())
        .bind(candidate_id)
        .fetch_optional(&mut *transaction)
        .await
        {
            Ok(status) => status,
            Err(error) => {
                drop(transaction);
                return retryable_internal(format!("the candidate row could not be read: {error}"));
            }
        };
        let consumed = matches!(status, Some((_, ref status)) if status == "consumed");
        let observed_revision = if consumed {
            status.map(|(revision, _)| u64::try_from(revision).unwrap_or(0))
        } else {
            // Fresh work: the adapter's own content read, withhold included. A refusal is the
            // adapter's own and rolls the transaction back.
            let observed = self
                .adapter
                .get_candidate(
                    &mut transaction,
                    &caller,
                    json!({ "space_id": space.as_str(), "candidate_id": candidate_id }),
                )
                .await;
            match observed {
                SpokeResult::Ok(MemoryStage::Ready(body)) => {
                    let revision = body
                        .get("candidate")
                        .and_then(|candidate| candidate.get("revision"))
                        .and_then(Value::as_u64);
                    match revision {
                        Some(revision) => Some(revision),
                        None => {
                            drop(transaction);
                            return retryable_internal(
                                "the candidate body carries no integer revision",
                            );
                        }
                    }
                }
                SpokeResult::Ok(stage) => {
                    drop(transaction);
                    return retryable_internal(format!(
                        "candidate read answered an unexpected stage: {stage:?}"
                    ));
                }
                SpokeResult::Reject(reject) => {
                    drop(transaction);
                    return SpokeResult::Reject(reject);
                }
            }
        };
        // No row at all: revision 1, the candidate's fresh-work value —
        // `review_candidate` answers its own `NotFound` for a candidate that never existed.
        let revision = observed_revision.unwrap_or(1);
        // 2. Stage on the same transaction. The adapter's dedup probe keys on
        //    (space, requester, operation, idempotency_key) and digests the whole payload, so
        //    a same-key replay must rebuild a byte-identical request to reach the recorded
        //    receipt. A consumed candidate's pre-consumption revision is exactly one less —
        //    consumption is a candidate's only revision bump — and that is the only revision
        //    any recorded review payload for this candidate can carry; a still-pending
        //    candidate (including one imported at a non-1 revision) stages at its own live
        //    revision. A different decision under one key still changes the digest and stays
        //    an `IdempotencyConflict`.
        let expected_revision = if consumed {
            revision.saturating_sub(1).max(1)
        } else {
            revision
        };
        let decision = match serde_json::to_value(decision) {
            Ok(decision) => decision,
            Err(error) => {
                drop(transaction);
                return retryable_internal(format!("the decision could not be rendered: {error}"));
            }
        };
        let stage = self
            .adapter
            .review_candidate(
                &mut transaction,
                &caller,
                json!({
                    "space_id": space.as_str(),
                    "candidate_id": candidate_id,
                    "decision": decision,
                    "expected_revision": expected_revision,
                    "idempotency_key": idempotency_key,
                }),
            )
            .await;
        enum Staging {
            // The fold staged a fresh decision on this transaction: it owns the staging and
            // must commit it after publication.
            Fresh { receipt_id: String },
            // A repeat met its own still-pending receipt: the staging is already durable, the
            // transaction is read-only for what follows, and the fold continues to publication
            // and settlement rather than answering the uncertain outcome again.
            PendingRetry { receipt_id: String },
        }
        let staging = match stage {
            SpokeResult::Ok(MemoryStage::JournalPending { receipt_id, .. }) => {
                Staging::Fresh { receipt_id }
            }
            SpokeResult::Ok(MemoryStage::Ready(body)) => {
                drop(transaction);
                return match serde_json::from_value(body) {
                    Ok(response) => SpokeResult::Ok(response),
                    Err(error) => retryable_internal(format!(
                        "the replayed receipt does not match its schema: {error}"
                    )),
                };
            }
            SpokeResult::Ok(stage) => {
                drop(transaction);
                return retryable_internal(format!(
                    "review answered an unexpected stage: {stage:?}"
                ));
            }
            SpokeResult::Reject(reject) if reject.code == SpokeRejectCode::OutcomeUncertain => {
                // The only uncertain answer `review_candidate` can give is its own replay
                // probe finding this key's receipt still pending. That is the recovery path:
                // resolve the key to the receipt and continue the fold — publish, then
                // settle — instead of answering the uncertain outcome forever.
                let key = ReceiptKey {
                    space: space.clone(),
                    requester: mindcore_base::boundary::CallerId::new(self.caller.as_str()),
                    operation: "memory.candidates.review".to_string(),
                    idempotency_key: idempotency_key.to_string(),
                };
                let receipt = match SqliteMemoryRepository
                    .review_receipt(&mut transaction, &key)
                    .await
                {
                    Ok(Some(ReceiptRead::Retained(receipt))) => receipt,
                    _other => {
                        drop(transaction);
                        return SpokeResult::Reject(reject);
                    }
                };
                Staging::PendingRetry {
                    receipt_id: receipt.receipt_id.clone(),
                }
            }
            SpokeResult::Reject(reject) => {
                drop(transaction);
                return SpokeResult::Reject(reject);
            }
        };
        let (receipt_id, fresh) = match staging {
            Staging::Fresh { receipt_id } => (receipt_id, true),
            Staging::PendingRetry { receipt_id } => (receipt_id, false),
        };

        // 3. Read the staged intent on the same transaction for the targets it owes.
        let intent = match SqliteMemoryRepository
            .journal_intent(&mut transaction, &space, &receipt_id)
            .await
        {
            Ok(Some(intent)) => intent,
            Ok(None) => {
                drop(transaction);
                return retryable_internal(format!("receipt {receipt_id} has no journaled intent"));
            }
            Err(_) => {
                drop(transaction);
                return retryable_internal(format!(
                    "journal intent {receipt_id} could not be read"
                ));
            }
        };

        // 4. Publish before the staging is durable, off the async worker: the publisher writes
        //    and fsyncs synchronously, so it runs on the blocking pool while the handle's task
        //    awaits it — unrelated executor work stays schedulable for the whole flush. The
        //    transaction itself moves into the blocking task and comes back with the result,
        //    so a caller that drops the review future can never release the writer before the
        //    started publication ends: a same-key retry's own transaction waits for the first
        //    one's rollback, and the two attempts can never publish side by side. Nothing
        //    commits before the publisher has confirmed, a refusal rolls everything back, and
        //    the publisher's own `SpiError::Settlement` refusal travels unchanged.
        let publish_targets = intent.targets.clone();
        let publish = {
            let publisher = Arc::clone(publisher);
            tokio::task::spawn_blocking(move || {
                let result = publisher(&publish_targets);
                (result, transaction)
            })
        };
        let (published, back) = match publish.await {
            Ok(pair) => pair,
            Err(error) => {
                return retryable_internal(format!("the publication task failed to join: {error}"));
            }
        };
        let transaction = back;
        if let Err(error) = published {
            drop(transaction);
            return match error {
                SpiError::Settlement(reject) => SpokeResult::Reject(reject),
                other => retryable_internal(format!("publication failed: {other}")),
            };
        }

        // 5. Commit the staging — a fresh fold's own; a pending retry's is already durable, so
        // its transaction is only dropped — then settle the receipt on a second
        // immediate-write transaction. A failure from here leaves a pending intent and answers
        // `OUTCOME_UNCERTAIN`, as the served surface does, and a same-key repeat resumes at
        // this settlement leg rather than re-staging.
        if fresh {
            if let Err(error) = transaction.commit().await {
                return uncertain(format!(
                    "review staging for receipt {receipt_id} committed an unknown outcome: {error}"
                ));
            }
        } else {
            drop(transaction);
        }
        let mut settlement = match self.pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(transaction) => transaction,
            Err(error) => {
                return uncertain(format!(
                    "settlement transaction for receipt {receipt_id} could not begin: {error}"
                ))
            }
        };
        let settled = self
            .adapter
            .settle_journal(&mut settlement, &space, &receipt_id)
            .await;
        let body = match settled {
            SpokeResult::Ok(body) => body,
            SpokeResult::Reject(reject) => {
                drop(settlement);
                return uncertain(format!(
                    "settlement of receipt {receipt_id} failed after it was staged: {}: {}",
                    reject.code, reject.message
                ));
            }
        };
        if let Err(error) = settlement.commit().await {
            return uncertain(format!(
                "settlement of receipt {receipt_id} committed an unknown outcome: {error}"
            ));
        }
        match serde_json::from_value(body) {
            Ok(response) => SpokeResult::Ok(response),
            Err(error) => uncertain(format!(
                "the settled receipt {receipt_id} does not match its schema: {error}"
            )),
        }
    }

    /// Read this Mind's judgment over one read transaction, rolled back after the read. The
    /// collection and carrier entry the transaction reads are the row's own — resolved from the
    /// admitted Mind at `open`, never from the caller — so a space-wide `knowledge_read` grant
    /// authorizes exactly the Mind that is read. A fresh carrier answers `None`; after one
    /// record, the judgment's records and the derived `MindState`. The record list is the one
    /// `derive_mind_state` already owns: the validated view is the single extraction, projected
    /// once into the wire state and moved into the answer.
    pub async fn cognition_read(&self) -> SpokeResult<Option<Cognition>> {
        let mut transaction = match self.pool.begin().await {
            Ok(transaction) => transaction,
            Err(error) => {
                return retryable_internal(format!(
                    "cognition read transaction could not begin: {error}"
                ))
            }
        };
        let admission = self
            .adapter
            .require_space_grants(
                &mut transaction,
                &principal(&self.caller),
                self.row.id.as_str(),
                &["knowledge_read"],
            )
            .await;
        if let Err(error) = admission {
            drop(transaction);
            return SpokeResult::Reject(workflow_reject(error));
        }
        let carrier = match read_entry(
            &mut transaction,
            &self.row.knowledge.collection,
            &self.row.carrier_entry_id,
        )
        .await
        {
            SpokeResult::Ok(carrier) => carrier,
            SpokeResult::Reject(reject) => {
                drop(transaction);
                return SpokeResult::Reject(reject);
            }
        };
        drop(transaction);
        let revision = match carrier.revision {
            Some(revision) => revision,
            None => {
                return reject(
                    SpokeRejectCode::InternalError,
                    format!(
                        "carrier {} holds no assigned revision",
                        self.row.carrier_entry_id
                    ),
                    "terminal",
                )
            }
        };
        let view = match mindcore_base::cognition::derive_mind_state(&carrier) {
            Ok(view) => view,
            Err(mindcore_base::cognition::MindError::NoRecords) => return SpokeResult::Ok(None),
            Err(error) => {
                return reject(
                    SpokeRejectCode::InternalError,
                    format!(
                        "carrier {} cannot be derived: {error}",
                        self.row.carrier_entry_id
                    ),
                    "terminal",
                )
            }
        };
        let mind_state =
            match mindcore_base::cognition::mind_state_value(&view, self.row.id.as_str()) {
                Ok(mind_state) => mind_state,
                Err(error) => {
                    return reject(
                        SpokeRejectCode::InternalError,
                        format!(
                            "the mind state of {} cannot be projected: {error}",
                            self.row.carrier_entry_id
                        ),
                        "terminal",
                    )
                }
            };
        SpokeResult::Ok(Some(Cognition {
            carrier_revision: revision,
            records: view.records,
            mind_state,
        }))
    }

    /// Deliberate one target over the evidence: `SpiMindWorkflows::deliberate`
    /// with the Mind's id as `space_id`, no subject, the carrier the row names
    /// as holder and the Mind's id as the assessment context. The space-level
    /// revalidation and unsettled-intent checks stand where the subject-level
    /// pair would; no path chooses a subject.
    pub async fn deliberate(
        &self,
        deliberation: &Deliberation<'_>,
    ) -> SpokeResult<DeliberateOutcome> {
        let holder: CognitionReference = match serde_json::from_value(json!({
            "kind": "knowledge_entry",
            "namespace": "mindcore",
            "id": self.row.carrier_entry_id.as_str(),
        })) {
            Ok(holder) => holder,
            Err(error) => {
                return retryable_internal(format!(
                    "the carrier reference does not decode: {error}"
                ))
            }
        };
        let binding = MemoryWorkflowBinding::new(self.pool.clone(), false).with_carrier_collection(
            self.row.id.as_str(),
            &self.row.knowledge.collection,
            &self.row.carrier_entry_id,
        );
        let workflows = SpiMindWorkflows::new(binding);
        let request = DeliberateRequest {
            space_id: self.row.id.as_str(),
            subject_id: None,
            assessment_context_id: Some(self.row.id.as_str()),
            holder: &holder,
            target: deliberation.target,
            evidence: deliberation.evidence,
            timeout: deliberation.timeout,
        };
        let caps = self.providers.capabilities();
        let outcome = MindWorkflows::deliberate(
            &workflows,
            &principal(&self.caller),
            request,
            self.providers.lens.as_deref(),
            &caps,
        )
        .await;
        match outcome {
            Ok(outcome) => SpokeResult::Ok(outcome),
            Err(error) => SpokeResult::Reject(workflow_reject(error)),
        }
    }
}
