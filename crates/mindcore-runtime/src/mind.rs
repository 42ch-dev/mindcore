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

use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::{MindId, SpaceId};
use mindcore_base::ir::MemoryRepository;
use mindcore_spi::SpiError;
use mindcore_spoke::{MemoryAdapter, MemoryStage};
use mindcore_sqlite::minds::Mind as MindRow;
use mindcore_sqlite::{MindStore, SqliteMemoryRepository};
use spoke_operations::{SpokeReject, SpokeRejectCode, SpokeResult};
use spoke_schemas::memory_candidates_review_request::Decision;
use spoke_schemas::{
    MemoryCandidatesReviewResponse, MemoryCaptureResponse, MemoryRecallResponse, Principal,
};
use sqlx::SqlitePool;

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
    publisher: Option<Box<JournalPublisher>>,
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
        self.publisher = Some(Box::new(publish));
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
        let publisher = match self.publisher.as_deref() {
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

        // 1–2. Read the candidate's revision, then stage the review on the same transaction.
        // A replay of a receipt already settled answers `Ready` and is returned as it is.
        let candidate = self
            .adapter
            .get_candidate(
                &mut transaction,
                &caller,
                json!({ "space_id": space.as_str(), "candidate_id": candidate_id }),
            )
            .await;
        let revision = match candidate {
            SpokeResult::Ok(MemoryStage::Ready(body)) => {
                match body.pointer("/candidate/revision").and_then(Value::as_u64) {
                    Some(revision) => revision,
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
                    "expected_revision": revision,
                    "idempotency_key": idempotency_key,
                }),
            )
            .await;
        let receipt_id = match stage {
            SpokeResult::Ok(MemoryStage::JournalPending { receipt_id, .. }) => receipt_id,
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
            SpokeResult::Reject(reject) => {
                drop(transaction);
                return SpokeResult::Reject(reject);
            }
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

        // 4. Publish before the staging is durable. A refusal rolls the transaction back:
        // no record row, no intent and a pending candidate are left, and the publisher's own
        // `SpiError::Settlement` refusal travels unchanged.
        if let Err(error) = publisher(&intent.targets) {
            drop(transaction);
            return match error {
                SpiError::Settlement(reject) => SpokeResult::Reject(reject),
                other => retryable_internal(format!("publication failed: {other}")),
            };
        }

        // 5. Commit the staging, then settle the receipt on a second immediate-write
        // transaction. A failure from here leaves a pending intent and answers
        // `OUTCOME_UNCERTAIN`, as the served surface does.
        if let Err(error) = transaction.commit().await {
            return uncertain(format!(
                "review staging for receipt {receipt_id} committed an unknown outcome: {error}"
            ));
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
}
