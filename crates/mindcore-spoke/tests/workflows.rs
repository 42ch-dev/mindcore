//! The workflow binding against a real SQLite fixture: what one recall page
//! costs, which knowledge collection fills the second slot, and the reread
//! that keeps a snapshot moved during provider work out of a released pack.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Connection, SqliteConnection, SqlitePool};
use tempfile::TempDir;

use mindcore_base::knowledge::KnowledgeScope;
use mindcore_spi::{
    AbsorbCandidateOutcome, CognitiveTaskKind, Embedder, EmbeddingIdentity, MindCapabilities,
    PolicyDecision, PolicyHook, Reasoner, SettlementDraft, SpiError, SpiMindWorkflows,
    StructuredReasonRequest,
};
use mindcore_spoke::MemoryWorkflowBinding;
use mindcore_sqlite::SERVED_KNOWLEDGE_LIST_LIMIT;
use spoke_operations::SpokeRejectCode;
use spoke_schemas::{MemoryCaptureRequest, MemoryRecallRequest, Principal};

const DDL: &str = include_str!("../../mindcore-sqlite/tests/fixtures/settlement-prepare.sql");
/// The optional vector cache, exactly as the shipped migration creates it.
const VECTORS_DDL: &str =
    include_str!("../../mindcore-sqlite/migrations/0002_memory_embedding_vectors.sql");
/// The knowledge tables of `migrations/0001_init.sql`; that migration's legacy
/// memory tables have a different shape than the protocol fixture above.
const KNOWLEDGE_DDL: &str = "
CREATE TABLE IF NOT EXISTS knowledge_entries (
  collection   TEXT NOT NULL,
  entry_id     TEXT NOT NULL,
  revision     INTEGER NOT NULL,
  payload      TEXT NOT NULL,
  cognition_head_seq INTEGER,
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL,
  PRIMARY KEY (collection, entry_id),
  FOREIGN KEY (collection, entry_id, cognition_head_seq)
    REFERENCES cognition_records (collection, entry_id, append_seq)
);
CREATE TABLE IF NOT EXISTS cognition_records (
  collection TEXT NOT NULL,
  entry_id   TEXT NOT NULL,
  append_seq INTEGER NOT NULL CHECK (append_seq > 0),
  record_id  TEXT NOT NULL,
  payload    TEXT NOT NULL,
  PRIMARY KEY (collection, entry_id, append_seq),
  UNIQUE (collection, entry_id, record_id),
  FOREIGN KEY (collection, entry_id) REFERENCES knowledge_entries (collection, entry_id)
);";
const SPACE: &str = "spc_1";
const SUBJECT: &str = "subj_1";
const PRINCIPAL: &str = "prn_1";
const NOW: &str = "2026-11-20T09:00:00.000Z";

/// What the embedder does to the store while it runs — outside every
/// transaction the workflow holds.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SideEffect {
    None,
    RevokeGrant,
    RevokeKnowledgeGrant,
    BumpRevision,
    PendingIntent,
    /// Revokes the recall grant once the record batch — the second provider
    /// call of a cold recall — runs.
    RevokeGrantOnRecordBatch,
    /// Moves every record and poisons one record's new-revision row, so a
    /// lookup of the moved page would fail to decode.
    MovePageAndPoisonVector,
}

/// Revoke the fixture principal's recall grant.
async fn revoke_recall_grant(pool: &SqlitePool) {
    sqlx::query("UPDATE memory_grants SET revoked_at = ? WHERE principal_id = ?")
        .bind(NOW)
        .bind(PRINCIPAL)
        .execute(pool)
        .await
        .expect("revoke the grant");
}

/// Withhold the subject's content: a pending journal intent on it.
async fn withhold_subject(pool: &SqlitePool) {
    sqlx::query(
        "INSERT INTO memory_journal \
         (seq, receipt_id, space_id, subject_id, requester_principal_id, op, \
          idempotency_key, state, targets_json, result_json, created_at, updated_at) \
         VALUES ((SELECT COALESCE(MAX(seq), 0) + 1 FROM memory_journal), 'rct_pending', \
                 ?, ?, ?, 'memory.records.update', 'key', 'pending', '[]', NULL, ?, ?)",
    )
    .bind(SPACE)
    .bind(SUBJECT)
    .bind(PRINCIPAL)
    .bind(NOW)
    .bind(NOW)
    .execute(pool)
    .await
    .expect("stage a pending intent");
}

struct BoundaryEmbedder {
    identity: EmbeddingIdentity,
    side_effect: SideEffect,
    pool: SqlitePool,
    /// Every text this embedder was asked for, in call order.
    embedded: Arc<Mutex<Vec<String>>>,
    /// How many provider calls ran, so a side effect can wait for the record
    /// batch instead of firing on the query.
    calls: AtomicUsize,
}

impl BoundaryEmbedder {
    fn embedded(&self) -> Vec<String> {
        self.embedded.lock().expect("embedded texts").clone()
    }
}

#[async_trait]
impl Embedder for BoundaryEmbedder {
    fn identity(&self) -> Option<&EmbeddingIdentity> {
        Some(&self.identity)
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, SpiError> {
        self.embedded
            .lock()
            .expect("embedded texts")
            .extend(texts.iter().cloned());
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        match self.side_effect {
            SideEffect::None => {}
            SideEffect::RevokeGrant => revoke_recall_grant(&self.pool).await,
            // A cold recall embeds the query first and its record batch second,
            // so the second call is where the grant is revoked.
            SideEffect::RevokeGrantOnRecordBatch if call == 1 => {
                revoke_recall_grant(&self.pool).await
            }
            SideEffect::RevokeGrantOnRecordBatch => {}
            SideEffect::RevokeKnowledgeGrant => {
                // Only the knowledge admission is revoked; the recall grant stays live.
                sqlx::query("UPDATE memory_grants SET revoked_at = ? WHERE grant_id = ?")
                    .bind(NOW)
                    .bind("grt_knowledge")
                    .execute(&self.pool)
                    .await
                    .expect("revoke the knowledge grant");
            }
            SideEffect::BumpRevision => {
                sqlx::query("UPDATE memory_records SET revision = revision + 1")
                    .execute(&self.pool)
                    .await
                    .expect("bump the record revision");
            }
            SideEffect::MovePageAndPoisonVector => {
                sqlx::query("UPDATE memory_records SET revision = revision + 1")
                    .execute(&self.pool)
                    .await
                    .expect("bump the record revision");
                // The moved page's row carries the new revision but cannot be
                // decoded: a probe of the new revision is a storage error.
                sqlx::query(
                    "UPDATE memory_embedding_vectors \
                     SET record_revision = record_revision + 1, vector = X'0000C07F00000000' \
                     WHERE record_id = 'rec_c'",
                )
                .execute(&self.pool)
                .await
                .expect("poison the moved record's row");
            }
            SideEffect::PendingIntent => withhold_subject(&self.pool).await,
        }
        Ok(texts.iter().map(|_| vec![1.0, 0.0]).collect())
    }
}

struct Harness {
    _dir: TempDir,
    root: PathBuf,
    pool: SqlitePool,
}

impl Harness {
    async fn open() -> Self {
        let dir = TempDir::new().expect("temporary directory");
        let root = dir.path().to_path_buf();
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(dir.path().join("workflows.db"))
                    .create_if_missing(true)
                    .foreign_keys(false),
            )
            .await
            .expect("open pool");
        sqlx::raw_sql(DDL).execute(&pool).await.expect("apply DDL");
        sqlx::raw_sql(VECTORS_DDL)
            .execute(&pool)
            .await
            .expect("apply the vector DDL");
        sqlx::raw_sql(KNOWLEDGE_DDL)
            .execute(&pool)
            .await
            .expect("apply knowledge DDL");
        sqlx::query(
            "INSERT INTO memory_spaces \
             (space_id, display_name, authority_host_id, authority_generation, status, revision, \
              created_at) \
             VALUES (?, 'space', 'host_1', 1, 'active', 1, ?)",
        )
        .bind(SPACE)
        .bind(NOW)
        .execute(&pool)
        .await
        .expect("seed space");
        sqlx::query(
            "INSERT INTO memory_subjects \
             (space_id, subject_id, kind_ns, kind_id, status, fenced_contexts_json, revision, \
              created_at) \
             VALUES (?, ?, NULL, NULL, 'active', '[]', 1, ?)",
        )
        .bind(SPACE)
        .bind(SUBJECT)
        .bind(NOW)
        .execute(&pool)
        .await
        .expect("seed subject");
        Self {
            _dir: dir,
            root,
            pool,
        }
    }

    async fn grant(&self, capabilities: &[&str]) {
        sqlx::query(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
              shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES ('grt_recall', ?, NULL, ?, ?, '[]', 0, NULL, NULL, 1, ?)",
        )
        .bind(SPACE)
        .bind(PRINCIPAL)
        .bind(serde_json::to_string(capabilities).expect("capabilities"))
        .bind(NOW)
        .execute(&self.pool)
        .await
        .expect("seed grant");
    }

    /// The principal's knowledge admission: a space-wide `knowledge_read` grant.
    async fn knowledge_grant(&self, principal: &str) {
        sqlx::query(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
              shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES ('grt_knowledge', ?, NULL, ?, '[\"knowledge_read\"]', '[]', 0, NULL, NULL, 1, ?)",
        )
        .bind(SPACE)
        .bind(principal)
        .bind(NOW)
        .execute(&self.pool)
        .await
        .expect("seed knowledge grant");
    }

    /// The carrier-write authority: a space-wide `knowledge_write` grant,
    /// separate from the read admission it never substitutes for.
    async fn carrier_grant(&self, principal: &str) {
        sqlx::query(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
              shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES ('grt_carrier', ?, NULL, ?, '[\"knowledge_write\"]', '[]', 0, NULL, NULL, 1, ?)",
        )
        .bind(SPACE)
        .bind(principal)
        .bind(NOW)
        .execute(&self.pool)
        .await
        .expect("seed carrier grant");
    }

    async fn record(&self, record_id: &str, text: &str, created_at: &str) {
        sqlx::query(
            "INSERT INTO memory_records \
             (record_id, space_id, subject_id, kind, content_json, scope_visibility, \
              scope_context_id, provenance_json, import_ref_json, occurred_at, tombstone_json, \
              revision, created_at, updated_at) \
             VALUES (?, ?, ?, 'fact', ?, 'shared', NULL, ?, NULL, NULL, NULL, 1, ?, ?)",
        )
        .bind(record_id)
        .bind(SPACE)
        .bind(SUBJECT)
        .bind(json!({ "text": text }).to_string())
        .bind(
            json!({
                "source_context_ids": ["ctx_a"],
                "anchors": [],
                "captured_by": "adapter",
                "captured_at": NOW
            })
            .to_string(),
        )
        .bind(created_at)
        .bind(created_at)
        .execute(&self.pool)
        .await
        .expect("seed record");
    }

    async fn entry(&self, collection: &str, entry_id: &str, summary: &str) {
        let payload = json!({
            "schema_version": 1,
            "entry_id": entry_id,
            "entry_type": "location",
            "canonical_name": entry_id,
            "status": "confirmed",
            "body": { "summary": summary },
            "revision": 1,
            "extensions": {}
        })
        .to_string();
        sqlx::query(
            "INSERT INTO knowledge_entries \
             (collection, entry_id, revision, payload, created_at, updated_at) \
             VALUES (?, ?, 1, ?, ?, ?)",
        )
        .bind(collection)
        .bind(entry_id)
        .bind(payload)
        .bind(NOW)
        .bind(NOW)
        .execute(&self.pool)
        .await
        .expect("seed knowledge entry");
    }

    fn embedder(&self, side_effect: SideEffect) -> BoundaryEmbedder {
        BoundaryEmbedder {
            identity: EmbeddingIdentity {
                provider_id: "local".to_string(),
                model_id: "axes".to_string(),
                model_version: "1".to_string(),
                dim: 2,
            },
            side_effect,
            pool: self.pool.clone(),
            embedded: Arc::new(Mutex::new(Vec::new())),
            calls: AtomicUsize::new(0),
        }
    }

    fn binding(
        &self,
        caller: &str,
        side_effect: SideEffect,
    ) -> (SpiMindWorkflows, BoundaryEmbedder) {
        let binding = MemoryWorkflowBinding::new(self.pool.clone(), true)
            .with_knowledge_collection(caller, KnowledgeScope::new("wren"));
        (SpiMindWorkflows::new(binding), self.embedder(side_effect))
    }

    /// An absorb-capable engine whose publisher writes journal targets under
    /// the fixture root, exactly as a host's file medium would.
    fn absorb_binding(&self) -> SpiMindWorkflows {
        let root = self.root.clone();
        SpiMindWorkflows::new(
            MemoryWorkflowBinding::new(self.pool.clone(), true).with_publisher(move |targets| {
                for target in targets {
                    let path = root.join(target["path"].as_str().expect("target path"));
                    if let Some(parent) = path.parent() {
                        std::fs::create_dir_all(parent).map_err(publish_error)?;
                    }
                    std::fs::write(&path, target["content"].as_str().expect("content"))
                        .map_err(publish_error)?;
                }
                Ok(())
            }),
        )
    }

    async fn count_from(&self, from: &str) -> i64 {
        let sql = format!("SELECT COUNT(*) FROM {from}");
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql))
            .fetch_one(&self.pool)
            .await
            .expect("count")
    }

    async fn candidate_status(&self, candidate_id: &str) -> String {
        sqlx::query_scalar("SELECT status FROM memory_candidates WHERE candidate_id = ?")
            .bind(candidate_id)
            .fetch_one(&self.pool)
            .await
            .expect("candidate status")
    }

    async fn candidate_text(&self, candidate_id: &str) -> Option<String> {
        let raw: Option<String> =
            sqlx::query_scalar("SELECT content_json FROM memory_candidates WHERE candidate_id = ?")
                .bind(candidate_id)
                .fetch_one(&self.pool)
                .await
                .expect("candidate content");
        raw.map(|raw| {
            serde_json::from_str::<Value>(&raw).expect("candidate content")["text"]
                .as_str()
                .expect("candidate text")
                .to_string()
        })
    }

    async fn stored_record(&self, record_id: &str) -> Option<(i64, String)> {
        let row: Option<(i64, String)> =
            sqlx::query_as("SELECT revision, content_json FROM memory_records WHERE record_id = ?")
                .bind(record_id)
                .fetch_optional(&self.pool)
                .await
                .expect("stored record");
        row.map(|(revision, content)| {
            let content = serde_json::from_str::<Value>(&content).expect("record content");
            (
                revision,
                content["text"].as_str().expect("record text").to_string(),
            )
        })
    }

    async fn vector_rows(&self) -> Vec<(String, String, i64, i64, Vec<u8>)> {
        sqlx::query_as(
            "SELECT record_id, identity_hash, record_revision, dim, vector \
             FROM memory_embedding_vectors ORDER BY record_id",
        )
        .fetch_all(&self.pool)
        .await
        .expect("vector rows")
    }
}

fn publish_error(error: std::io::Error) -> SpiError {
    SpiError::InvalidInput(format!("publication failed: {error}"))
}

fn retry_class(reject: &spoke_operations::SpokeReject) -> Option<&str> {
    reject
        .details
        .as_ref()
        .and_then(|details| details.get("retry_class"))
        .and_then(Value::as_str)
}

fn capture(key: &str, text: &str) -> MemoryCaptureRequest {
    serde_json::from_value(json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "capture_scope": { "visibility": "shared" },
        "content": { "text": text },
        "idempotency_key": key,
    }))
    .expect("capture request")
}

/// What the reasoner does to the store while it runs — outside every
/// transaction the workflow holds.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReasonerEffect {
    None,
    /// `None` bumps every record, `Some(id)` only that one.
    BumpRecordRevision(Option<&'static str>),
    RevokeGrant,
}

struct FixedReasoner {
    verdict: Value,
    effect: ReasonerEffect,
    pool: SqlitePool,
}

impl FixedReasoner {
    fn new(verdict: Value, pool: &SqlitePool) -> Self {
        Self {
            verdict,
            effect: ReasonerEffect::None,
            pool: pool.clone(),
        }
    }

    fn bumping(mut self) -> Self {
        self.effect = ReasonerEffect::BumpRecordRevision(None);
        self
    }

    fn bumping_only(mut self, record_id: &'static str) -> Self {
        self.effect = ReasonerEffect::BumpRecordRevision(Some(record_id));
        self
    }

    fn revoking(mut self) -> Self {
        self.effect = ReasonerEffect::RevokeGrant;
        self
    }
}

#[async_trait]
impl Reasoner for FixedReasoner {
    async fn reason_structured(
        &self,
        request: StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        assert_eq!(request.task_kind, CognitiveTaskKind::Absorb);
        match self.effect {
            ReasonerEffect::None => {}
            ReasonerEffect::BumpRecordRevision(None) => {
                sqlx::query("UPDATE memory_records SET revision = revision + 1")
                    .execute(&self.pool)
                    .await
                    .expect("bump the record revision");
            }
            ReasonerEffect::BumpRecordRevision(Some(record_id)) => {
                sqlx::query(
                    "UPDATE memory_records SET revision = revision + 1 WHERE record_id = ?",
                )
                .bind(record_id)
                .execute(&self.pool)
                .await
                .expect("bump the cited record revision");
            }
            ReasonerEffect::RevokeGrant => {
                sqlx::query("UPDATE memory_grants SET revoked_at = ? WHERE principal_id = ?")
                    .bind(NOW)
                    .bind(PRINCIPAL)
                    .execute(&self.pool)
                    .await
                    .expect("revoke the grant");
            }
        }
        Ok(self.verdict.clone())
    }
}

struct AutoPolicy;

#[async_trait]
impl PolicyHook for AutoPolicy {
    async fn evaluate_settlement(
        &self,
        _draft: SettlementDraft<'_>,
    ) -> Result<PolicyDecision, SpiError> {
        Ok(PolicyDecision::AutoSettle)
    }
}

/// Refuses every draft, so the workflow drops the candidate.
struct RejectPolicy;

#[async_trait]
impl PolicyHook for RejectPolicy {
    async fn evaluate_settlement(
        &self,
        _draft: SettlementDraft<'_>,
    ) -> Result<PolicyDecision, SpiError> {
        Ok(PolicyDecision::Reject {
            reason: "no automatic settlement for this candidate".to_string(),
        })
    }
}

fn principal() -> Principal {
    serde_json::from_value(json!({ "principal_id": PRINCIPAL, "extensions": {} }))
        .expect("principal")
}

fn request(query: Option<&str>, max_items: u64) -> MemoryRecallRequest {
    let mut value = json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "selection": { "mode": "subject_all" },
        "budget": { "max_items": max_items },
    });
    if let Some(query) = query {
        value["query"] = json!(query);
    }
    serde_json::from_value(value).expect("recall request")
}

fn budget() -> NonZeroUsize {
    NonZeroUsize::new(1_000_000).expect("non-zero budget")
}

fn record_ids(pack: &mindcore_spi::ContextPack) -> Vec<&str> {
    pack.records
        .iter()
        .map(|record| match record {
            spoke_schemas::RetainedRecord::LiveRecord(record) => record.record_id.as_str(),
            spoke_schemas::RetainedRecord::TombstonedRecord(record) => record.record_id.as_str(),
        })
        .collect()
}

#[tokio::test]
async fn one_recall_page_is_packed_whole_with_its_provenance() {
    let harness = Harness::open().await;
    harness.grant(&["recall"]).await;
    harness
        .record(
            "rec_a",
            "the north gate is closed",
            "2026-11-20T09:00:03.000Z",
        )
        .await;
    harness
        .record(
            "rec_b",
            "the market opens at dawn",
            "2026-11-20T09:00:02.000Z",
        )
        .await;
    harness
        .record(
            "rec_c",
            "north gate guards sleep",
            "2026-11-20T09:00:01.000Z",
        )
        .await;
    let workflows = SpiMindWorkflows::new(MemoryWorkflowBinding::new(harness.pool.clone(), true));

    let pack = workflows
        .recall_context(
            &principal(),
            &request(None, 2),
            budget(),
            &MindCapabilities::default(),
        )
        .await
        .expect("pack");

    // The SPOKE budget cut the page to two items; the workflow never asks for
    // a second page.
    assert_eq!(record_ids(&pack), ["rec_a", "rec_b"]);
    let spoke_schemas::RetainedRecord::LiveRecord(first) = &pack.records[0] else {
        panic!("expected a live record");
    };
    assert_eq!(first.space_id.as_str(), SPACE);
    assert_eq!(first.subject_id.as_str(), SUBJECT);
    assert_eq!(first.revision.get(), 1);
    assert_eq!(first.provenance.source_context_ids, ["ctx_a"]);
    assert_eq!(first.provenance.captured_by.as_str(), "adapter");
    assert_eq!(
        serde_json::to_value(&first.provenance).expect("provenance serializes")["captured_at"],
        json!("2026-11-20T09:00:00Z")
    );
    assert!(pack.tokens_used > 0);
    assert!(
        pack.knowledge.is_empty(),
        "no knowledge collection is bound here"
    );
}

#[tokio::test]
async fn knowledge_slot_reads_only_the_bound_collection() {
    let harness = Harness::open().await;
    harness.grant(&["recall"]).await;
    harness.knowledge_grant(PRINCIPAL).await;
    harness
        .record(
            "rec_a",
            "the north gate is closed",
            "2026-11-20T09:00:01.000Z",
        )
        .await;
    harness.entry("wren", "ke_gate", "closed after dusk").await;
    harness.entry("wren", "ke_market", "opens at dawn").await;
    harness
        .entry("wanderer", "ke_elsewhere", "not this holder's")
        .await;
    let (workflows, embedder) = harness.binding(PRINCIPAL, SideEffect::None);

    let pack = workflows
        .recall_context(
            &principal(),
            &request(None, 10),
            budget(),
            &MindCapabilities {
                embedder: Some(&embedder),
                ..MindCapabilities::default()
            },
        )
        .await
        .expect("pack");

    let entries: Vec<&str> = pack
        .knowledge
        .iter()
        .map(|entry| entry.entry_id.as_str())
        .collect();
    assert_eq!(entries, ["ke_gate", "ke_market"]);
    assert_eq!(pack.knowledge[0].revision, Some(1));
    assert_eq!(
        pack.knowledge[0].body.summary.as_deref(),
        Some("closed after dusk")
    );
}

/// The knowledge slot is a served list too: an admitted recall decodes at most the served bound
/// and refuses a larger collection instead of reading it whole.
#[tokio::test]
async fn knowledge_slot_refuses_a_collection_over_the_served_bound() {
    let harness = Harness::open().await;
    harness.grant(&["recall"]).await;
    harness.knowledge_grant(PRINCIPAL).await;
    for index in 0..SERVED_KNOWLEDGE_LIST_LIMIT {
        harness
            .entry("wren", &format!("ke_{index:04}"), "bounded entry")
            .await;
    }
    let (workflows, embedder) = harness.binding(PRINCIPAL, SideEffect::None);
    let capabilities = MindCapabilities {
        embedder: Some(&embedder),
        ..MindCapabilities::default()
    };

    let pack = workflows
        .recall_context(&principal(), &request(None, 10), budget(), &capabilities)
        .await
        .expect("a collection at the bound is served whole");
    assert_eq!(
        pack.knowledge.len(),
        SERVED_KNOWLEDGE_LIST_LIMIT,
        "the slot carries the whole bounded collection"
    );

    harness
        .entry("wren", "ke_over", "one row past the bound")
        .await;
    let error = workflows
        .recall_context(&principal(), &request(None, 10), budget(), &capabilities)
        .await
        .expect_err("an over-bound collection refuses the recall's knowledge slot");
    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::InternalError);
    assert!(
        reject
            .message
            .contains(&SERVED_KNOWLEDGE_LIST_LIMIT.to_string()),
        "the refusal names the bound: {}",
        reject.message
    );
}

#[tokio::test]
async fn revoked_grant_during_provider_work_refuses_the_pack() {
    let harness = Harness::open().await;
    harness.grant(&["recall"]).await;
    harness.knowledge_grant(PRINCIPAL).await;
    harness
        .record(
            "rec_a",
            "the north gate is closed",
            "2026-11-20T09:00:01.000Z",
        )
        .await;
    let (workflows, embedder) = harness.binding(PRINCIPAL, SideEffect::RevokeGrant);

    let error = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &MindCapabilities {
                embedder: Some(&embedder),
                ..MindCapabilities::default()
            },
        )
        .await
        .expect_err("a revoked grant must not release a pack");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
}

#[tokio::test]
async fn pending_intent_during_provider_work_refuses_the_pack() {
    let harness = Harness::open().await;
    harness.grant(&["recall"]).await;
    harness.knowledge_grant(PRINCIPAL).await;
    harness
        .record(
            "rec_a",
            "the north gate is closed",
            "2026-11-20T09:00:01.000Z",
        )
        .await;
    let (workflows, embedder) = harness.binding(PRINCIPAL, SideEffect::PendingIntent);

    let error = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &MindCapabilities {
                embedder: Some(&embedder),
                ..MindCapabilities::default()
            },
        )
        .await
        .expect_err("an unsettled intent must not release a pack");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::InternalError);
    assert_eq!(
        reject
            .details
            .as_ref()
            .and_then(|details| details.get("reason"))
            .and_then(Value::as_str),
        Some("UNRECONCILED")
    );
}

#[tokio::test]
async fn changed_revision_during_provider_work_refuses_the_pack() {
    let harness = Harness::open().await;
    harness.grant(&["recall"]).await;
    harness.knowledge_grant(PRINCIPAL).await;
    harness
        .record(
            "rec_a",
            "the north gate is closed",
            "2026-11-20T09:00:01.000Z",
        )
        .await;
    let (workflows, embedder) = harness.binding(PRINCIPAL, SideEffect::BumpRevision);

    let error = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &MindCapabilities {
                embedder: Some(&embedder),
                ..MindCapabilities::default()
            },
        )
        .await
        .expect_err("a changed revision must not release a pack");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::RevisionConflict);
}

#[tokio::test]
async fn memory_grant_alone_leaves_the_knowledge_slot_empty() {
    let harness = Harness::open().await;
    // `recall` is a memory capability; it admits no knowledge read, so the slot stays empty
    // while the memory pack the grant does authorize is released.
    harness.grant(&["recall"]).await;
    harness
        .record(
            "rec_a",
            "the north gate is closed",
            "2026-11-20T09:00:01.000Z",
        )
        .await;
    harness.entry("wren", "ke_gate", "closed after dusk").await;
    let (workflows, embedder) = harness.binding(PRINCIPAL, SideEffect::None);

    let pack = workflows
        .recall_context(
            &principal(),
            &request(None, 10),
            budget(),
            &MindCapabilities {
                embedder: Some(&embedder),
                ..MindCapabilities::default()
            },
        )
        .await
        .expect("the memory grant still releases its pack");

    assert_eq!(record_ids(&pack), ["rec_a"]);
    assert!(
        pack.knowledge.is_empty(),
        "a memory grant never fills the knowledge slot"
    );
}

#[tokio::test]
async fn another_granted_principal_reads_the_bound_collection() {
    let harness = Harness::open().await;
    // The caller's own admission is complete; the carrier write is bound to another caller.
    harness.grant(&["recall"]).await;
    harness.knowledge_grant(PRINCIPAL).await;
    harness
        .record(
            "rec_a",
            "the north gate is closed",
            "2026-11-20T09:00:01.000Z",
        )
        .await;
    harness.entry("wren", "ke_gate", "closed after dusk").await;
    let (workflows, embedder) = harness.binding("prn_2", SideEffect::None);

    let pack = workflows
        .recall_context(
            &principal(),
            &request(None, 10),
            budget(),
            &MindCapabilities {
                embedder: Some(&embedder),
                ..MindCapabilities::default()
            },
        )
        .await
        .expect("the caller's own knowledge grant admits the slot");

    // The route's caller is prn_2, so the slot proves the read authority is the caller's own
    // grant against the bound collection rather than an identity match with the write caller.
    let entries: Vec<&str> = pack
        .knowledge
        .iter()
        .map(|entry| entry.entry_id.as_str())
        .collect();
    assert_eq!(entries, ["ke_gate"]);
}

/// Recall re-derives the knowledge grant over its own transaction and compares both reads: a
/// revocation during provider work empties the slot, so the pack is retried against a snapshot
/// that no longer includes it — never released against a slot whose authority just vanished.
#[tokio::test]
async fn revoked_knowledge_admission_during_provider_work_is_a_revision_conflict() {
    let harness = Harness::open().await;
    harness.grant(&["recall"]).await;
    harness.knowledge_grant(PRINCIPAL).await;
    harness
        .record(
            "rec_a",
            "the north gate is closed",
            "2026-11-20T09:00:01.000Z",
        )
        .await;
    harness.entry("wren", "ke_gate", "closed after dusk").await;
    let (workflows, embedder) = harness.binding(PRINCIPAL, SideEffect::RevokeKnowledgeGrant);

    let error = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &MindCapabilities {
                embedder: Some(&embedder),
                ..MindCapabilities::default()
            },
        )
        .await
        .expect_err("a revoked knowledge admission must not release the slot it filled");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::RevisionConflict);
    assert_eq!(
        reject
            .details
            .as_ref()
            .and_then(|details| details.get("retry_class"))
            .and_then(Value::as_str),
        Some("retryable"),
        "the retried call reads an empty slot rather than the revoked one"
    );
}

// ---------- absorb over the real adapter ----------

async fn granted_harness() -> Harness {
    let harness = Harness::open().await;
    harness
        .grant(&["capture", "recall", "review", "records_write"])
        .await;
    harness
        .record(
            "rec_gate",
            "the north gate is closed",
            "2026-11-20T09:00:01.000Z",
        )
        .await;
    harness
}

fn promoted(record_ids: &[String]) -> AbsorbCandidateOutcome {
    AbsorbCandidateOutcome::Settled {
        candidate_id: "unused".to_string(),
        record_ids: record_ids.to_vec(),
    }
}

#[tokio::test]
async fn promote_settles_one_record_and_consumes_the_candidate() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let reasoner = FixedReasoner::new(
        json!({ "action": "promote", "evidence_ids": ["rec_gate"] }),
        &harness.pool,
    );
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let outcomes = workflows
        .absorb(
            &principal(),
            &capture("cap_promote", "the north gate is closed at dusk"),
            None,
            &caps,
        )
        .await
        .expect("absorb");

    let [AbsorbCandidateOutcome::Settled {
        candidate_id,
        record_ids,
    }] = outcomes.as_slice()
    else {
        panic!("expected one settled candidate, got {outcomes:?}");
    };
    assert_eq!(record_ids.len(), 1);
    let (revision, text) = harness
        .stored_record(&record_ids[0])
        .await
        .expect("the promoted record");
    assert_eq!(revision, 1);
    assert_eq!(text, "the north gate is closed at dusk");
    assert_eq!(harness.candidate_status(candidate_id).await, "consumed");
    assert_eq!(
        harness
            .count_from("memory_journal WHERE state = 'pending'")
            .await,
        0
    );
    assert!(harness
        .root
        .join(format!("records/{}.md", record_ids[0]))
        .exists());
    // The seeded record is untouched.
    assert_eq!(
        harness
            .stored_record("rec_gate")
            .await
            .expect("seeded record"),
        (1, "the north gate is closed".to_string())
    );
}

#[tokio::test]
async fn update_replaces_the_target_in_place_and_consumes_the_candidate() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let reasoner = FixedReasoner::new(
        json!({
            "action": "update",
            "record_id": "rec_gate",
            "evidence_ids": ["rec_gate"]
        }),
        &harness.pool,
    );
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let outcomes = workflows
        .absorb(
            &principal(),
            &capture("cap_update", "the north gate is closed at dusk"),
            None,
            &caps,
        )
        .await
        .expect("absorb");

    let [AbsorbCandidateOutcome::Settled {
        candidate_id,
        record_ids,
    }] = outcomes.as_slice()
    else {
        panic!("expected one settled candidate, got {outcomes:?}");
    };
    assert_eq!(promoted(record_ids), promoted(&["rec_gate".to_string()]));
    assert_eq!(
        harness
            .stored_record("rec_gate")
            .await
            .expect("updated record"),
        (2, "the north gate is closed at dusk".to_string())
    );
    // One record in place, no second record minted, and the candidate consumed.
    assert_eq!(harness.count_from("memory_records").await, 1);
    assert_eq!(harness.candidate_status(candidate_id).await, "consumed");
    assert_eq!(
        harness
            .count_from("memory_journal WHERE op = 'memory.records.update' AND state = 'applied'")
            .await,
        1
    );
    assert_eq!(
        harness
            .count_from(
                "memory_journal WHERE op = 'memory.candidates.dismiss' AND state = 'applied'"
            )
            .await,
        1
    );
    let published = std::fs::read_to_string(harness.root.join("records/rec_gate.md"))
        .expect("the updated projection is published");
    assert!(published.contains("revision: 2"));
}

#[tokio::test]
async fn dismiss_drops_the_candidate_without_a_record() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let reasoner = FixedReasoner::new(
        json!({
            "action": "dismiss",
            "rationale": "the records already say this",
            "evidence_ids": ["rec_gate"]
        }),
        &harness.pool,
    );
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let outcomes = workflows
        .absorb(
            &principal(),
            &capture("cap_dismiss", "the north gate closes at dusk"),
            None,
            &caps,
        )
        .await
        .expect("absorb");

    let [AbsorbCandidateOutcome::Dropped { candidate_id }] = outcomes.as_slice() else {
        panic!("expected one dropped candidate, got {outcomes:?}");
    };
    assert_eq!(harness.candidate_status(candidate_id).await, "consumed");
    assert_eq!(harness.count_from("memory_records").await, 1);
    assert_eq!(
        harness
            .stored_record("rec_gate")
            .await
            .expect("seeded record"),
        (1, "the north gate is closed".to_string())
    );

    // Replaying the same capture reports the same drop and writes nothing more.
    let replay = workflows
        .absorb(
            &principal(),
            &capture("cap_dismiss", "the north gate closes at dusk"),
            None,
            &caps,
        )
        .await
        .expect("replayed absorb");
    assert_eq!(replay, outcomes);
    assert_eq!(
        harness
            .count_from("memory_receipts WHERE op = 'memory.candidates.review'")
            .await,
        1
    );
}

#[tokio::test]
async fn replay_returns_the_recorded_outcome_without_a_second_write() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let reasoner = FixedReasoner::new(
        json!({ "action": "promote", "evidence_ids": ["rec_gate"] }),
        &harness.pool,
    );
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let first = workflows
        .absorb(
            &principal(),
            &capture("cap_replay", "the north gate is closed at dusk"),
            None,
            &caps,
        )
        .await
        .expect("first absorb");
    let second = workflows
        .absorb(
            &principal(),
            &capture("cap_replay", "the north gate is closed at dusk"),
            None,
            &caps,
        )
        .await
        .expect("replayed absorb");

    assert_eq!(second, first);
    assert_eq!(harness.count_from("memory_records").await, 2);
    assert_eq!(
        harness
            .count_from("memory_receipts WHERE op = 'memory.candidates.review'")
            .await,
        1
    );
}

#[tokio::test]
async fn stale_target_revision_rejects_without_a_second_write() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let reasoner = FixedReasoner::new(
        json!({
            "action": "update",
            "record_id": "rec_gate",
            "evidence_ids": ["rec_gate"]
        }),
        &harness.pool,
    )
    .bumping();
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = workflows
        .absorb(
            &principal(),
            &capture("cap_stale", "the north gate is closed at dusk"),
            None,
            &caps,
        )
        .await
        .expect_err("a stale target revision must not settle");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::RevisionConflict);
    // The reasoner's bump is the only change; the update and the dismissal
    // both rolled back with the staging transaction.
    assert_eq!(
        harness
            .stored_record("rec_gate")
            .await
            .expect("seeded record"),
        (2, "the north gate is closed".to_string())
    );
    assert_eq!(harness.count_from("memory_records").await, 1);
    assert_eq!(
        harness
            .count_from("memory_journal WHERE state = 'pending'")
            .await,
        0
    );
    assert_eq!(
        harness
            .count_from("memory_candidates WHERE status = 'pending'")
            .await,
        1
    );
}

#[tokio::test]
async fn default_policy_leaves_the_candidate_pending() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let reasoner = FixedReasoner::new(
        json!({ "action": "promote", "evidence_ids": ["rec_gate"] }),
        &harness.pool,
    );
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        ..MindCapabilities::default()
    };

    let outcomes = workflows
        .absorb(
            &principal(),
            &capture("cap_review", "the north gate is closed at dusk"),
            None,
            &caps,
        )
        .await
        .expect("absorb");

    let [AbsorbCandidateOutcome::Pending { candidate_id }] = outcomes.as_slice() else {
        panic!("expected one pending candidate, got {outcomes:?}");
    };
    assert_eq!(harness.candidate_status(candidate_id).await, "pending");
    assert_eq!(harness.count_from("memory_records").await, 1);
    assert_eq!(
        harness
            .count_from("memory_receipts WHERE op = 'memory.candidates.review'")
            .await,
        0
    );
    assert_eq!(harness.count_from("memory_journal").await, 0);
}

#[tokio::test]
async fn absent_reasoner_matches_deterministic_capture() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();

    let outcomes = workflows
        .absorb(
            &principal(),
            &capture("cap_deterministic", "the north gate is closed at dusk"),
            None,
            &MindCapabilities::default(),
        )
        .await
        .expect("absorb");

    let [AbsorbCandidateOutcome::Pending { candidate_id }] = outcomes.as_slice() else {
        panic!("expected the capture's own candidate, got {outcomes:?}");
    };
    assert_eq!(harness.candidate_status(candidate_id).await, "pending");
    assert_eq!(
        harness.candidate_text(candidate_id).await.as_deref(),
        Some("the north gate is closed at dusk")
    );
    assert_eq!(harness.count_from("memory_records").await, 1);
    assert_eq!(harness.count_from("memory_journal").await, 0);
}

#[tokio::test]
async fn a_capture_only_principal_absorbs_a_pending_candidate() {
    let harness = Harness::open().await;
    // Capture admission only: reading the candidate back needs `review`, and
    // the evidence page needs `recall`.
    harness.grant(&["capture"]).await;
    let workflows = harness.absorb_binding();

    let outcomes = workflows
        .absorb(
            &principal(),
            &capture("cap_capture_only", "the north gate is closed at dusk"),
            None,
            &MindCapabilities::default(),
        )
        .await
        .expect("a fresh capture reports its own receipt");

    let [AbsorbCandidateOutcome::Pending { candidate_id }] = outcomes.as_slice() else {
        panic!("expected the capture's own candidate, got {outcomes:?}");
    };
    assert_eq!(harness.candidate_status(candidate_id).await, "pending");
    assert_eq!(
        harness
            .count_from("memory_receipts WHERE op != 'memory.capture'")
            .await,
        0,
        "no review was written"
    );
}

#[tokio::test]
async fn fabricated_evidence_leaves_the_captured_candidate_unchanged() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let reasoner = FixedReasoner::new(
        json!({ "action": "promote", "evidence_ids": ["rec_ghost"] }),
        &harness.pool,
    );
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = workflows
        .absorb(
            &principal(),
            &capture("cap_fabricated", "the north gate is closed at dusk"),
            None,
            &caps,
        )
        .await
        .expect_err("a fabricated id must not settle");

    assert!(matches!(error, SpiError::InvalidReasonerOutput(_)));
    assert_eq!(harness.count_from("memory_records").await, 1);
    assert_eq!(harness.count_from("memory_journal").await, 0);
    assert_eq!(
        harness
            .count_from("memory_candidates WHERE status = 'pending' AND revision = 1")
            .await,
        1
    );
    let candidate_id: String =
        sqlx::query_scalar("SELECT candidate_id FROM memory_candidates WHERE status = 'pending'")
            .fetch_one(&harness.pool)
            .await
            .expect("the captured candidate");
    assert_eq!(
        harness.candidate_text(&candidate_id).await.as_deref(),
        Some("the north gate is closed at dusk")
    );
}

#[tokio::test]
async fn replay_of_an_update_reports_the_record_it_settled() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let reasoner = FixedReasoner::new(
        json!({
            "action": "update",
            "record_id": "rec_gate",
            "evidence_ids": ["rec_gate"]
        }),
        &harness.pool,
    );
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };
    let request = capture("cap_update_replay", "the north gate is closed at dusk");

    let first = workflows
        .absorb(&principal(), &request, None, &caps)
        .await
        .expect("first absorb");
    let replay = workflows
        .absorb(&principal(), &request, None, &caps)
        .await
        .expect("replayed absorb");

    // The dismissal consumes the candidate without a record id; the replay
    // reads the update leg's own receipt instead of reporting a drop.
    assert_eq!(replay, first);
    assert!(matches!(
        replay.as_slice(),
        [AbsorbCandidateOutcome::Settled { record_ids, .. }] if record_ids.as_slice() == ["rec_gate"]
    ));
    assert_eq!(harness.count_from("memory_records").await, 1);
    assert_eq!(
        harness
            .count_from("memory_receipts WHERE op = 'memory.records.update'")
            .await,
        1
    );
    assert_eq!(
        harness
            .count_from("memory_journal WHERE state = 'applied'")
            .await,
        2
    );
}

#[tokio::test]
async fn no_reasoner_replay_reports_the_recorded_outcome() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let reasoner = FixedReasoner::new(
        json!({ "action": "promote", "evidence_ids": ["rec_gate"] }),
        &harness.pool,
    );
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };
    let request = capture("cap_replay_no_reasoner", "the north gate is closed at dusk");

    let first = workflows
        .absorb(&principal(), &request, None, &caps)
        .await
        .expect("first absorb");
    let replay = workflows
        .absorb(&principal(), &request, None, &MindCapabilities::default())
        .await
        .expect("replayed absorb");

    assert_eq!(replay, first);
    assert!(matches!(
        replay.as_slice(),
        [AbsorbCandidateOutcome::Settled { .. }]
    ));
    assert_eq!(harness.count_from("memory_records").await, 2);
    assert_eq!(
        harness
            .count_from("memory_receipts WHERE op = 'memory.candidates.review'")
            .await,
        1
    );
}

#[tokio::test]
async fn a_capture_only_replay_of_a_pending_capture_reports_pending() {
    let harness = Harness::open().await;
    // Capture authority only: the caller never held the recall or review grant
    // an absorb decision reads back through.
    harness.grant(&["capture"]).await;
    let workflows = harness.absorb_binding();
    let request = capture("cap_capture_only", "the north gate is closed at dusk");

    let first = workflows
        .absorb(&principal(), &request, None, &MindCapabilities::default())
        .await
        .expect("a capture-only absorb reports its fresh candidate");
    assert!(matches!(
        first.as_slice(),
        [AbsorbCandidateOutcome::Pending { .. }]
    ));

    // The identical retry replays the capture receipt: the recorded run left the
    // candidate pending, and the caller holds the receipt that names it, so the
    // replay reports that state without demanding review or recall grants.
    let replay = workflows
        .absorb(&principal(), &request, None, &MindCapabilities::default())
        .await
        .expect("the capture-only retry must not demand review or recall");
    assert_eq!(replay, first);
    assert_eq!(harness.count_from("memory_candidates").await, 1);
}

#[tokio::test]
async fn a_changed_non_target_citation_refuses_the_settlement() {
    let harness = granted_harness().await;
    harness
        .record(
            "rec_wall",
            "the east wall is guarded",
            "2026-11-20T09:00:02.000Z",
        )
        .await;
    let workflows = harness.absorb_binding();
    let reasoner = FixedReasoner::new(
        json!({
            "action": "update",
            "record_id": "rec_wall",
            "evidence_ids": ["rec_gate", "rec_wall"]
        }),
        &harness.pool,
    )
    .bumping_only("rec_gate");
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = workflows
        .absorb(
            &principal(),
            &capture("cap_cited_changed", "the north gate is closed at dusk"),
            None,
            &caps,
        )
        .await
        .expect_err("a changed citation must not settle");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::RevisionConflict);
    // Neither settlement effect was staged: the target, the candidate and the
    // journal are exactly what the failed run found.
    assert_eq!(
        harness
            .stored_record("rec_wall")
            .await
            .expect("the target record"),
        (1, "the east wall is guarded".to_string())
    );
    assert_eq!(harness.count_from("memory_journal").await, 0);
    assert_eq!(
        harness
            .count_from("memory_candidates WHERE status = 'pending'")
            .await,
        1
    );
}

#[tokio::test]
async fn a_citation_that_loses_its_admission_refuses_the_settlement() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let reasoner = FixedReasoner::new(
        json!({ "action": "promote", "evidence_ids": ["rec_gate"] }),
        &harness.pool,
    )
    .revoking();
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = workflows
        .absorb(
            &principal(),
            &capture("cap_cited_revoked", "the north gate is closed at dusk"),
            None,
            &caps,
        )
        .await
        .expect_err("an unauthorized citation must not settle");

    // §7.17: the revoked record reads as absent rather than admitted.
    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::NotFound);
    assert_eq!(harness.count_from("memory_records").await, 1);
    assert_eq!(harness.count_from("memory_journal").await, 0);
    assert_eq!(
        harness
            .count_from("memory_receipts WHERE op != 'memory.capture'")
            .await,
        0
    );
}

#[tokio::test]
async fn a_binding_without_a_publisher_refuses_before_staging_anything() {
    let harness = granted_harness().await;
    let workflows = SpiMindWorkflows::new(MemoryWorkflowBinding::new(harness.pool.clone(), true));
    let policy = AutoPolicy;

    for (verdict, key, text) in [
        (
            json!({ "action": "promote", "evidence_ids": ["rec_gate"] }),
            "cap_no_publisher",
            "the north gate is closed at dusk",
        ),
        (
            json!({
                "action": "update",
                "record_id": "rec_gate",
                "evidence_ids": ["rec_gate"]
            }),
            "cap_no_publisher_update",
            "the north gate shuts at dusk",
        ),
    ] {
        let reasoner = FixedReasoner::new(verdict, &harness.pool);
        let caps = MindCapabilities {
            reasoner: Some(&reasoner),
            policy_hook: Some(&policy),
            ..MindCapabilities::default()
        };

        let error = workflows
            .absorb(&principal(), &capture(key, text), None, &caps)
            .await
            .expect_err("a binding without a publisher must refuse");

        assert!(matches!(error, SpiError::InvalidInput(_)));
    }

    // The refusal is a normal input error with no write at all.
    assert_eq!(
        harness
            .stored_record("rec_gate")
            .await
            .expect("seeded record"),
        (1, "the north gate is closed".to_string())
    );
    assert_eq!(harness.count_from("memory_records").await, 1);
    assert_eq!(harness.count_from("memory_journal").await, 0);
    assert_eq!(
        harness
            .count_from("memory_receipts WHERE op != 'memory.capture'")
            .await,
        0
    );
    assert_eq!(
        harness
            .count_from("memory_candidates WHERE status = 'pending' AND revision = 1")
            .await,
        2
    );
}

#[tokio::test]
async fn a_publisher_that_fails_after_staging_reports_an_uncertain_outcome() {
    let harness = granted_harness().await;
    // The medium is installed and refuses after the intents were staged.
    let workflows = SpiMindWorkflows::new(
        MemoryWorkflowBinding::new(harness.pool.clone(), true).with_publisher(|_| {
            Err(SpiError::InvalidInput(
                "the medium is read-only".to_string(),
            ))
        }),
    );
    let reasoner = FixedReasoner::new(
        json!({ "action": "promote", "evidence_ids": ["rec_gate"] }),
        &harness.pool,
    );
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = workflows
        .absorb(
            &principal(),
            &capture("cap_publish_fails", "the north gate is closed at dusk"),
            None,
            &caps,
        )
        .await
        .expect_err("a staged publication that failed is not a clean refusal");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::OutcomeUncertain);
    assert_eq!(retry_class(&reject), Some("uncertain"));
    // The staged intent is durable and unsettled: its receipt is the recovery
    // identity, and the cause survives.
    let receipt: String =
        sqlx::query_scalar("SELECT receipt_id FROM memory_journal WHERE state = 'pending'")
            .fetch_one(&harness.pool)
            .await
            .expect("the pending intent");
    assert!(reject.message.contains(&receipt), "{}", reject.message);
    assert!(
        reject.message.contains("the medium is read-only"),
        "{}",
        reject.message
    );
    // The staged promotion is durable and unconfirmed: the seeded record plus
    // the one the pending intent would publish.
    assert_eq!(harness.count_from("memory_records").await, 2);
}

#[tokio::test]
async fn a_settlement_that_cannot_begin_after_staging_reports_an_uncertain_outcome() {
    let harness = granted_harness().await;
    // The publisher closes the pool, so the settlement transaction cannot open
    // after the staging commit.
    let pool = harness.pool.clone();
    let workflows = SpiMindWorkflows::new(
        MemoryWorkflowBinding::new(pool.clone(), true).with_publisher(move |_| {
            // Marking the pool closed is eager; the returned future is dropped.
            let _closing = pool.close();
            Ok(())
        }),
    );
    let reasoner = FixedReasoner::new(
        json!({ "action": "promote", "evidence_ids": ["rec_gate"] }),
        &harness.pool,
    );
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = workflows
        .absorb(
            &principal(),
            &capture("cap_begin_fails", "the north gate is closed at dusk"),
            None,
            &caps,
        )
        .await
        .expect_err("an unopened settlement is not a clean refusal");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::OutcomeUncertain);
    assert_eq!(retry_class(&reject), Some("uncertain"));
    // The staged intent is durable: its receipt is the recovery identity.
    let mut reader = SqliteConnection::connect_with(
        &SqliteConnectOptions::new().filename(harness.root.join("workflows.db")),
    )
    .await
    .expect("read the staged fixture");
    let receipt: String =
        sqlx::query_scalar("SELECT receipt_id FROM memory_journal WHERE state = 'pending'")
            .fetch_one(&mut reader)
            .await
            .expect("the pending intent");
    assert!(reject.message.contains(&receipt), "{}", reject.message);
    assert!(
        reject.message.contains("settlement transaction begin"),
        "{}",
        reject.message
    );
}

#[tokio::test]
async fn a_replay_never_settles_what_a_policy_left_pending() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let request = capture("cap_held", "the north gate is closed at dusk");
    let reasoner = FixedReasoner::new(
        json!({ "action": "promote", "evidence_ids": ["rec_gate"] }),
        &harness.pool,
    );

    // The default policy holds every settlement, so the candidate stays pending.
    let held = workflows
        .absorb(
            &principal(),
            &request,
            None,
            &MindCapabilities {
                reasoner: Some(&reasoner),
                ..MindCapabilities::default()
            },
        )
        .await
        .expect("held absorb");

    // The replay runs under a policy that would settle it, over a reasoner that
    // marks every call by bumping the record it read.
    let bumping = FixedReasoner::new(
        json!({ "action": "promote", "evidence_ids": [] }),
        &harness.pool,
    )
    .bumping();
    let policy = AutoPolicy;
    let replay = workflows
        .absorb(
            &principal(),
            &request,
            None,
            &MindCapabilities {
                reasoner: Some(&bumping),
                policy_hook: Some(&policy),
                ..MindCapabilities::default()
            },
        )
        .await
        .expect("replayed absorb");

    assert_eq!(replay, held);
    assert!(matches!(
        replay.as_slice(),
        [AbsorbCandidateOutcome::Pending { .. }]
    ));
    // The reasoner never ran again: the record it would have bumped and the
    // promotion it would have written are both absent.
    assert_eq!(
        harness
            .stored_record("rec_gate")
            .await
            .expect("seeded record"),
        (1, "the north gate is closed".to_string())
    );
    assert_eq!(harness.count_from("memory_records").await, 1);
    assert_eq!(
        harness
            .count_from("memory_receipts WHERE op = 'memory.candidates.review'")
            .await,
        0
    );
}

#[tokio::test]
async fn a_policy_rejection_revalidates_the_cited_evidence_it_consumes() {
    let harness = granted_harness().await;
    harness
        .record(
            "rec_wall",
            "the east wall is guarded",
            "2026-11-20T09:00:02.000Z",
        )
        .await;
    let workflows = harness.absorb_binding();
    let reasoner = FixedReasoner::new(
        json!({
            "action": "promote",
            "evidence_ids": ["rec_gate", "rec_wall"]
        }),
        &harness.pool,
    )
    .bumping_only("rec_wall");
    let policy = RejectPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = workflows
        .absorb(
            &principal(),
            &capture("cap_reject_stale", "the north gate is closed at dusk"),
            None,
            &caps,
        )
        .await
        .expect_err("a refusal over a changed citation must not consume the candidate");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::RevisionConflict);
    // The drop never reached staging: the candidate is still pending and no
    // review intent was journaled or settled.
    assert_eq!(
        harness
            .count_from("memory_candidates WHERE status = 'pending'")
            .await,
        1
    );
    assert_eq!(harness.count_from("memory_journal").await, 0);
    assert_eq!(
        harness
            .count_from("memory_receipts WHERE op = 'memory.candidates.review'")
            .await,
        0
    );
}

// ── deliberate over the real carrier ─────────────────────────────────

const CARRIER: &str = "ke_wren";

/// What the reasoner does to the store while it deliberates.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DeliberateEffect {
    None,
    /// Move the cited record once: only the first attempt's citation is stale.
    BumpCitedOnce,
    /// Move the cited record on every call: every attempt's citation is stale.
    BumpCitedAlways,
    RevokeKnowledgeGrant,
    /// Revoke the separate carrier-write authority while the reasoner runs.
    RevokeCarrierGrant,
    /// Revoke the subject's recall grant while the reasoner runs.
    RevokeRecallGrant,
    /// Withhold the subject: an unsettled intent appears while the reasoner runs.
    StagePendingIntent,
}

struct DeliberateReasoner {
    verdict: Value,
    effect: DeliberateEffect,
    pool: SqlitePool,
    calls: Arc<AtomicUsize>,
}

impl DeliberateReasoner {
    fn new(verdict: Value, effect: DeliberateEffect, pool: &SqlitePool) -> Self {
        Self {
            verdict,
            effect,
            pool: pool.clone(),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn calls(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.calls)
    }
}

#[async_trait]
impl Reasoner for DeliberateReasoner {
    async fn reason_structured(
        &self,
        request: StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        assert_eq!(request.task_kind, CognitiveTaskKind::Deliberate);
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        match self.effect {
            DeliberateEffect::None => {}
            DeliberateEffect::BumpCitedOnce if call == 0 => {
                sqlx::query("UPDATE memory_records SET revision = revision + 1")
                    .execute(&self.pool)
                    .await
                    .expect("bump the cited record");
            }
            DeliberateEffect::BumpCitedOnce => {}
            DeliberateEffect::BumpCitedAlways => {
                sqlx::query("UPDATE memory_records SET revision = revision + 1")
                    .execute(&self.pool)
                    .await
                    .expect("bump the cited record");
            }
            DeliberateEffect::RevokeKnowledgeGrant => {
                sqlx::query("UPDATE memory_grants SET revoked_at = ? WHERE grant_id = ?")
                    .bind(NOW)
                    .bind("grt_knowledge")
                    .execute(&self.pool)
                    .await
                    .expect("revoke the knowledge grant");
            }
            DeliberateEffect::RevokeCarrierGrant => {
                sqlx::query("UPDATE memory_grants SET revoked_at = ? WHERE grant_id = ?")
                    .bind(NOW)
                    .bind("grt_carrier")
                    .execute(&self.pool)
                    .await
                    .expect("revoke the carrier grant");
            }
            DeliberateEffect::RevokeRecallGrant => {
                sqlx::query("UPDATE memory_grants SET revoked_at = ? WHERE grant_id = ?")
                    .bind(NOW)
                    .bind("grt_recall")
                    .execute(&self.pool)
                    .await
                    .expect("revoke the recall grant");
            }
            DeliberateEffect::StagePendingIntent => {
                sqlx::query(
                    "INSERT INTO memory_journal \
                     (seq, receipt_id, space_id, subject_id, requester_principal_id, op, \
                      idempotency_key, state, targets_json, result_json, created_at, updated_at) \
                     VALUES (1, 'rct_pending', ?, ?, ?, 'memory.records.update', 'key', 'pending', \
                             '[]', NULL, ?, ?)",
                )
                .bind(SPACE)
                .bind(SUBJECT)
                .bind(PRINCIPAL)
                .bind(NOW)
                .bind(NOW)
                .execute(&self.pool)
                .await
                .expect("stage a pending intent");
            }
        }
        Ok(self.verdict.clone())
    }
}

fn cognition_reference(id: &str) -> Value {
    json!({ "kind": "knowledge_entry", "namespace": "mindcore", "id": id })
}

fn promote_verdict() -> Value {
    json!({
        "recommendation": { "kind": "promote", "target_kind": "adr" },
        "rationale": "the gate test passed on the cited record",
        "evidence_ids": ["ev_gate"],
        "record_ids": ["rec_gate"],
        "record": {
            "kind": "belief",
            "order": 1,
            "q": {
                "value": 0.8,
                "assertion": "belief_attribution",
                "method": "two_witness_reports",
                "calibration": "uncalibrated"
            },
            "assessor_role": "holder_self_report",
            "basis": {
                "kind": "evidence",
                "evidence": [{ "kind": "memory_record", "namespace": "mindcore", "id": "rec_gate" }]
            }
        }
    })
}

/// One deliberation over the seeded carrier.
async fn deliberate(
    harness: &Harness,
    caps: &MindCapabilities<'_>,
    timeout: std::time::Duration,
) -> Result<mindcore_spi::DeliberateOutcome, SpiError> {
    deliberate_as(harness, PRINCIPAL, caps, timeout).await
}

/// One deliberation under a binding whose carrier route names `caller`.
async fn deliberate_as(
    harness: &Harness,
    caller: &str,
    caps: &MindCapabilities<'_>,
    timeout: std::time::Duration,
) -> Result<mindcore_spi::DeliberateOutcome, SpiError> {
    let workflows = SpiMindWorkflows::new(
        MemoryWorkflowBinding::new(harness.pool.clone(), true)
            .with_knowledge_collection(caller, KnowledgeScope::new("wren")),
    );
    let holder_value = cognition_reference(CARRIER);
    let holder: spoke_schemas::data::cognition::CognitionReference =
        serde_json::from_value(holder_value).expect("holder");
    let target: spoke_schemas::data::cognition::CognitionTarget = serde_json::from_value(json!({
        "carrier": cognition_reference("ke_shared_gate"),
        "target_id": "prop_gate_closed",
        "kind": "proposition"
    }))
    .expect("target");
    let evidence = [mindcore_spi::Evidence {
        id: "ev_gate",
        text: "the gate test passed on this commit",
    }];
    let request = mindcore_spi::DeliberateRequest {
        space_id: SPACE,
        subject_id: SUBJECT,
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout,
    };
    workflows
        .deliberate(&principal(), request, None, caps)
        .await
}

/// The carrier's stored revision and payload.
async fn stored_carrier(harness: &Harness) -> (i64, Value) {
    let (revision, payload): (i64, String) = sqlx::query_as(
        "SELECT revision, payload FROM knowledge_entries WHERE collection = 'wren' AND entry_id = ?",
    )
    .bind(CARRIER)
    .fetch_one(&harness.pool)
    .await
    .expect("stored carrier");
    (
        revision,
        serde_json::from_str(&payload).expect("carrier payload"),
    )
}

/// The carrier's committed record rows in `append_seq` order: the rows are the
/// storage authority, the payload keeps only the module envelope.
async fn committed_records(harness: &Harness) -> Vec<Value> {
    let payloads: Vec<String> = sqlx::query_scalar(
        "SELECT payload FROM cognition_records \
         WHERE collection = 'wren' AND entry_id = ? ORDER BY append_seq ASC",
    )
    .bind(CARRIER)
    .fetch_all(&harness.pool)
    .await
    .expect("committed records");
    payloads
        .iter()
        .map(|payload| serde_json::from_str(payload).expect("record payload"))
        .collect()
}

/// A harness holding one subject record and the admitted carrier entry.
async fn deliberate_harness() -> Harness {
    let harness = Harness::open().await;
    harness.grant(&["recall"]).await;
    harness.knowledge_grant(PRINCIPAL).await;
    harness.carrier_grant(PRINCIPAL).await;
    harness
        .record(
            "rec_gate",
            "the north gate is closed",
            "2026-11-20T09:00:01.000Z",
        )
        .await;
    harness
        .entry("wren", CARRIER, "the warden's north gate")
        .await;
    harness
}

fn long_timeout() -> std::time::Duration {
    std::time::Duration::from_secs(30)
}

#[tokio::test]
async fn another_principal_cannot_write_the_bound_carrier() {
    // Recall is admitted per caller; the carrier write keeps the one-caller limit.
    let harness = deliberate_harness().await;
    let reasoner =
        DeliberateReasoner::new(promote_verdict(), DeliberateEffect::None, &harness.pool);
    let calls = reasoner.calls();
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = deliberate_as(&harness, "prn_2", &caps, long_timeout())
        .await
        .expect_err("only the route's caller may write its carrier");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the snapshot read refuses before the reasoner runs"
    );
    let (revision, _) = stored_carrier(&harness).await;
    assert_eq!(revision, 1);
}

#[tokio::test]
async fn deliberate_commits_a_cognition_record_onto_the_carrier() {
    let harness = deliberate_harness().await;
    let reasoner =
        DeliberateReasoner::new(promote_verdict(), DeliberateEffect::None, &harness.pool);
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let outcome = deliberate(&harness, &caps, long_timeout())
        .await
        .expect("deliberate");
    let cases = match outcome {
        mindcore_spi::DeliberateOutcome::Settled {
            record,
            mind_state_version,
            ..
        } => (record, mind_state_version),
        other => panic!("expected a settlement, got {other:?}"),
    };
    let (record, mind_state_version) = cases;

    // The settled view descends from the committed carrier, not a draft.
    assert_eq!(mind_state_version, 2);
    let (revision, payload) = stored_carrier(&harness).await;
    assert_eq!(revision, 2);
    // The payload keeps only the module envelope; the committed record lives in its own row.
    assert_eq!(payload["modules"]["cognition"]["records"], json!([]));
    let records = committed_records(&harness).await;
    assert_eq!(records.len(), 1);
    let stored = &records[0];
    assert_eq!(stored["holder"], cognition_reference(CARRIER));
    assert_eq!(stored["target"]["target_id"], "prop_gate_closed");
    assert_eq!(stored["view"], "current");
    assert_eq!(stored["assessor"]["kind"], "principal");
    assert_eq!(
        serde_json::to_value(&record).expect("record serializes"),
        *stored
    );
}

#[tokio::test]
async fn default_policy_holds_the_verdict_without_writing() {
    let harness = deliberate_harness().await;
    let reasoner =
        DeliberateReasoner::new(promote_verdict(), DeliberateEffect::None, &harness.pool);
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        ..MindCapabilities::default()
    };

    let outcome = deliberate(&harness, &caps, long_timeout())
        .await
        .expect("deliberate");

    assert!(matches!(
        outcome,
        mindcore_spi::DeliberateOutcome::NeedsReview { .. }
    ));
    let (revision, payload) = stored_carrier(&harness).await;
    assert_eq!(revision, 1, "the carrier was not written");
    assert!(
        payload.get("modules").is_none(),
        "no cognition record reached the carrier"
    );
}

#[tokio::test]
async fn a_cited_record_that_moved_during_reasoning_is_retried() {
    let harness = deliberate_harness().await;
    let reasoner = DeliberateReasoner::new(
        promote_verdict(),
        DeliberateEffect::BumpCitedOnce,
        &harness.pool,
    );
    let calls = reasoner.calls();
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let outcome = deliberate(&harness, &caps, long_timeout())
        .await
        .expect("the fresh snapshot settles");

    assert!(matches!(
        outcome,
        mindcore_spi::DeliberateOutcome::Settled {
            mind_state_version: 2,
            ..
        }
    ));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "one retry over a fresh snapshot"
    );
    let (revision, _payload) = stored_carrier(&harness).await;
    assert_eq!(revision, 2);
    assert_eq!(committed_records(&harness).await.len(), 1);
}

#[tokio::test]
async fn an_unsettled_citation_conflict_surfaces_without_writing() {
    let harness = deliberate_harness().await;
    let reasoner = DeliberateReasoner::new(
        promote_verdict(),
        DeliberateEffect::BumpCitedAlways,
        &harness.pool,
    );
    let calls = reasoner.calls();
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = deliberate(&harness, &caps, long_timeout())
        .await
        .expect_err("a citation that keeps moving exhausts the retries");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::RevisionConflict);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "one attempt plus two retries"
    );
    let (revision, payload) = stored_carrier(&harness).await;
    assert_eq!(revision, 1);
    assert!(payload.get("modules").is_none());
}

#[tokio::test]
async fn a_revoked_knowledge_grant_refuses_the_carrier_write() {
    let harness = deliberate_harness().await;
    let reasoner = DeliberateReasoner::new(
        promote_verdict(),
        DeliberateEffect::RevokeKnowledgeGrant,
        &harness.pool,
    );
    let calls = reasoner.calls();
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = deliberate(&harness, &caps, long_timeout())
        .await
        .expect_err("a revoked admission must not write the carrier");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a refusal is never retried"
    );
    let (revision, payload) = stored_carrier(&harness).await;
    assert_eq!(revision, 1);
    assert!(payload.get("modules").is_none());
}

#[tokio::test]
async fn a_read_only_caller_refuses_the_carrier_write() {
    // The read admission alone reaches the snapshot; it never authorizes the write.
    let harness = deliberate_harness().await;
    sqlx::query("DELETE FROM memory_grants WHERE grant_id = 'grt_carrier'")
        .execute(&harness.pool)
        .await
        .expect("drop the carrier grant");
    let reasoner =
        DeliberateReasoner::new(promote_verdict(), DeliberateEffect::None, &harness.pool);
    let calls = reasoner.calls();
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = deliberate(&harness, &caps, long_timeout())
        .await
        .expect_err("a read-only caller must not write the carrier");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
    assert!(
        reject.message.contains("knowledge_write"),
        "the refusal names the missing write authority: {reject:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a refusal is never retried"
    );
    let (revision, payload) = stored_carrier(&harness).await;
    assert_eq!(revision, 1);
    assert!(payload.get("modules").is_none());
}

#[tokio::test]
async fn a_revoked_carrier_grant_refuses_the_carrier_write() {
    let harness = deliberate_harness().await;
    let reasoner = DeliberateReasoner::new(
        promote_verdict(),
        DeliberateEffect::RevokeCarrierGrant,
        &harness.pool,
    );
    let calls = reasoner.calls();
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = deliberate(&harness, &caps, long_timeout())
        .await
        .expect_err("a revoked write grant must not write the carrier");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a refusal is never retried"
    );
    let (revision, payload) = stored_carrier(&harness).await;
    assert_eq!(revision, 1);
    assert!(payload.get("modules").is_none());
}

/// A promotion resting on a declared prior: it cites no memory record at all.
fn prior_verdict() -> Value {
    json!({
        "recommendation": { "kind": "promote", "target_kind": "adr" },
        "rationale": "the warden's standing assumption already holds",
        "evidence_ids": [],
        "record_ids": [],
        "record": {
            "kind": "belief",
            "order": 1,
            "q": {
                "value": 0.8,
                "assertion": "belief_attribution",
                "method": "two_witness_reports",
                "calibration": "uncalibrated"
            },
            "assessor_role": "holder_self_report",
            "basis": {
                "kind": "prior",
                "description": "the warden has always held the north gate"
            }
        }
    })
}

#[tokio::test]
async fn a_revoked_subject_grant_refuses_a_write_that_cites_nothing() {
    let harness = deliberate_harness().await;
    let reasoner = DeliberateReasoner::new(
        prior_verdict(),
        DeliberateEffect::RevokeRecallGrant,
        &harness.pool,
    );
    let calls = reasoner.calls();
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = deliberate(&harness, &caps, long_timeout())
        .await
        .expect_err("a revoked subject grant must refuse the write without citations");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
    assert!(
        reject.message.contains("recall"),
        "the refusal names the missing subject admission: {reject:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a refusal is never retried"
    );
    let (revision, payload) = stored_carrier(&harness).await;
    assert_eq!(revision, 1);
    assert!(payload.get("modules").is_none());
}

#[tokio::test]
async fn a_subject_withheld_during_reasoning_refuses_the_carrier_write() {
    let harness = deliberate_harness().await;
    let reasoner = DeliberateReasoner::new(
        promote_verdict(),
        DeliberateEffect::StagePendingIntent,
        &harness.pool,
    );
    let calls = reasoner.calls();
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let error = deliberate(&harness, &caps, long_timeout())
        .await
        .expect_err("a withheld subject must not gain a cognition record");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::InternalError);
    assert_eq!(
        reject
            .details
            .as_ref()
            .and_then(|details| details.get("reason"))
            .and_then(Value::as_str),
        Some("UNRECONCILED")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "withholding is terminal");
    let (revision, payload) = stored_carrier(&harness).await;
    assert_eq!(revision, 1);
    assert!(payload.get("modules").is_none());
}

// ---------- vector warm after settlement ----------

/// One auto-settling promote of `text`, with `embedder` injected when the host
/// supplies one.
async fn promote_with(
    harness: &Harness,
    key: &str,
    text: &str,
    embedder: Option<&dyn Embedder>,
) -> Vec<AbsorbCandidateOutcome> {
    let workflows = harness.absorb_binding();
    let reasoner = FixedReasoner::new(
        json!({ "action": "promote", "evidence_ids": ["rec_gate"] }),
        &harness.pool,
    );
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        embedder,
    };
    workflows
        .absorb(&principal(), &capture(key, text), None, &caps)
        .await
        .expect("absorb")
}

fn warming_identity_hash() -> String {
    match mindcore_sqlite::vectors::identity_hash("local", "axes", "1", 2) {
        spoke_operations::SpokeResult::Ok(hash) => hash,
        spoke_operations::SpokeResult::Reject(reject) => {
            panic!("the warming identity hashes: {}", reject.message)
        }
    }
}

#[tokio::test]
async fn a_settled_capture_warms_one_row_for_the_live_record() {
    let harness = granted_harness().await;
    let embedder = harness.embedder(SideEffect::None);

    let outcomes = promote_with(
        &harness,
        "cap_warm",
        "the north gate is closed at dusk",
        Some(&embedder),
    )
    .await;

    let [AbsorbCandidateOutcome::Settled { record_ids, .. }] = outcomes.as_slice() else {
        panic!("expected one settled candidate, got {outcomes:?}");
    };
    assert_eq!(
        embedder.embedded(),
        vec!["the north gate is closed at dusk".to_string()],
        "the warm embeds the exact live record text"
    );
    let rows = harness.vector_rows().await;
    assert_eq!(rows.len(), 1, "one settled record, one row");
    let (record_id, identity_hash, revision, dim, vector) = &rows[0];
    assert_eq!(record_id, &record_ids[0]);
    assert_eq!(identity_hash, &warming_identity_hash());
    assert_eq!(*revision, 1, "the committed revision is the row's revision");
    assert_eq!(*dim, 2);
    assert_eq!(
        vector,
        &[1.0f32, 0.0]
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect::<Vec<u8>>(),
        "the row carries the provider's little-endian components"
    );
}

#[tokio::test]
async fn a_replayed_capture_warms_nothing() {
    let harness = granted_harness().await;
    let first = promote_with(
        &harness,
        "cap_warm_replay",
        "the north gate is closed at dusk",
        None,
    )
    .await;
    assert!(
        harness.vector_rows().await.is_empty(),
        "no embedder, no row"
    );

    let embedder = harness.embedder(SideEffect::None);
    let replay = promote_with(
        &harness,
        "cap_warm_replay",
        "the north gate is closed at dusk",
        Some(&embedder),
    )
    .await;

    assert_eq!(replay, first, "a replay reports the same settlement");
    assert!(
        embedder.embedded().is_empty(),
        "a replay has nothing fresh to embed"
    );
    assert!(
        harness.vector_rows().await.is_empty(),
        "a replay writes no vector row"
    );
}

#[tokio::test]
async fn an_updated_settlement_warms_the_new_revision() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let reasoner = FixedReasoner::new(
        json!({
            "action": "update",
            "record_id": "rec_gate",
            "evidence_ids": ["rec_gate"]
        }),
        &harness.pool,
    );
    let policy = AutoPolicy;
    let embedder = harness.embedder(SideEffect::None);
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        embedder: Some(&embedder),
    };

    let outcomes = workflows
        .absorb(
            &principal(),
            &capture("cap_warm_update", "the north gate is closed at dusk"),
            None,
            &caps,
        )
        .await
        .expect("absorb");

    let [AbsorbCandidateOutcome::Settled { record_ids, .. }] = outcomes.as_slice() else {
        panic!("expected one settled candidate, got {outcomes:?}");
    };
    assert_eq!(record_ids, &["rec_gate".to_string()]);
    assert_eq!(
        embedder.embedded(),
        vec!["the north gate is closed at dusk".to_string()],
        "the warm embeds the updated live text"
    );
    let rows = harness.vector_rows().await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "rec_gate");
    assert_eq!(rows[0].2, 2, "the row carries the committed revision");
}

#[tokio::test]
async fn a_provider_failure_after_settlement_leaves_a_miss() {
    struct FailingEmbedder {
        identity: EmbeddingIdentity,
    }

    #[async_trait]
    impl Embedder for FailingEmbedder {
        fn identity(&self) -> Option<&EmbeddingIdentity> {
            Some(&self.identity)
        }

        async fn embed_batch(&self, _texts: &[String]) -> Result<Vec<Vec<f32>>, SpiError> {
            Err(SpiError::EmbeddingFailed("provider down".to_string()))
        }
    }

    let harness = granted_harness().await;
    let embedder = FailingEmbedder {
        identity: EmbeddingIdentity {
            provider_id: "local".to_string(),
            model_id: "axes".to_string(),
            model_version: "1".to_string(),
            dim: 2,
        },
    };

    let outcomes = promote_with(
        &harness,
        "cap_warm_fail",
        "the north gate is closed at dusk",
        Some(&embedder),
    )
    .await;

    let [AbsorbCandidateOutcome::Settled {
        candidate_id,
        record_ids,
    }] = outcomes.as_slice()
    else {
        panic!("expected one settled candidate, got {outcomes:?}");
    };
    assert_eq!(
        harness.stored_record(&record_ids[0]).await,
        Some((1, "the north gate is closed at dusk".to_string())),
        "the durable record survives the failed warm"
    );
    assert_eq!(harness.candidate_status(candidate_id).await, "consumed");
    assert!(
        harness.vector_rows().await.is_empty(),
        "a failed warm is a cache miss, not a Memory effect"
    );
}

#[tokio::test]
async fn a_revision_moved_during_the_warm_keeps_no_row() {
    let harness = granted_harness().await;
    let embedder = harness.embedder(SideEffect::BumpRevision);

    let outcomes = promote_with(
        &harness,
        "cap_warm_stale",
        "the north gate is closed at dusk",
        Some(&embedder),
    )
    .await;

    let [AbsorbCandidateOutcome::Settled { record_ids, .. }] = outcomes.as_slice() else {
        panic!("expected one settled candidate, got {outcomes:?}");
    };
    assert_eq!(
        harness
            .stored_record(&record_ids[0])
            .await
            .expect("the promoted record")
            .0,
        2,
        "the provider ran after the committed read and the record moved"
    );
    assert!(
        harness.vector_rows().await.is_empty(),
        "obsolete work cannot overwrite a newer vector"
    );
}

#[tokio::test]
async fn a_subject_withheld_during_the_warm_keeps_no_row() {
    let harness = granted_harness().await;
    // The provider stages an unsettled intent while it runs, so the subject is
    // withheld from the settlement's live read onwards — before the fill.
    let embedder = harness.embedder(SideEffect::PendingIntent);

    let outcomes = promote_with(
        &harness,
        "cap_warm_withheld",
        "the north gate is closed at dusk",
        Some(&embedder),
    )
    .await;

    let [AbsorbCandidateOutcome::Settled {
        candidate_id,
        record_ids,
    }] = outcomes.as_slice()
    else {
        panic!("expected one settled candidate, got {outcomes:?}");
    };
    assert_eq!(
        embedder.embedded(),
        vec!["the north gate is closed at dusk".to_string()],
        "the provider ran after the settlement's live read served the record"
    );
    assert_eq!(
        harness.stored_record(&record_ids[0]).await,
        Some((1, "the north gate is closed at dusk".to_string())),
        "the already-settled Memory effect stays successful"
    );
    assert_eq!(harness.candidate_status(candidate_id).await, "consumed");
    assert!(
        harness.vector_rows().await.is_empty(),
        "a subject withheld before the fill gains no vector row"
    );
}

/// The axis one text sits on: the market side, the gate, or the guards
/// half-way. Equal texts land on the same vector, so a page can tie.
fn axis_vector(text: &str) -> Vec<f32> {
    if text.contains("market") {
        vec![0.0, 1.0]
    } else if text.contains("guards") {
        vec![0.5, 0.5]
    } else {
        vec![1.0, 0.0]
    }
}

/// Embeds by axis and keeps every text it was asked for, so a recall's provider
/// cost is a count rather than an inference.
struct AxisEmbedder {
    identity: EmbeddingIdentity,
    texts: Arc<Mutex<Vec<String>>>,
}

impl AxisEmbedder {
    fn new(model_version: &str) -> Self {
        Self {
            identity: EmbeddingIdentity {
                provider_id: "local".to_string(),
                model_id: "axes".to_string(),
                model_version: model_version.to_string(),
                dim: 2,
            },
            texts: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn embedded(&self) -> Vec<String> {
        self.texts.lock().expect("embedded texts").clone()
    }
}

#[async_trait]
impl Embedder for AxisEmbedder {
    fn identity(&self) -> Option<&EmbeddingIdentity> {
        Some(&self.identity)
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, SpiError> {
        self.texts
            .lock()
            .expect("embedded texts")
            .extend(texts.iter().cloned());
        Ok(texts.iter().map(|text| axis_vector(text)).collect())
    }
}

/// An embedder that names its identity but never serves a vector.
struct UnavailableEmbedder(EmbeddingIdentity);

#[async_trait]
impl Embedder for UnavailableEmbedder {
    fn identity(&self) -> Option<&EmbeddingIdentity> {
        Some(&self.0)
    }

    async fn embed_batch(&self, _texts: &[String]) -> Result<Vec<Vec<f32>>, SpiError> {
        Err(SpiError::EmbeddingsUnavailable)
    }
}

fn axes_identity(model_version: &str) -> EmbeddingIdentity {
    EmbeddingIdentity {
        provider_id: "local".to_string(),
        model_id: "axes".to_string(),
        model_version: model_version.to_string(),
        dim: 2,
    }
}

fn embedding_caps(embedder: &dyn Embedder) -> MindCapabilities<'_> {
    MindCapabilities {
        embedder: Some(embedder),
        ..MindCapabilities::default()
    }
}

/// A recall-authorized page of three records in adapter order (newest first):
/// the market, the closed gate and the sleeping guards.
async fn vector_page() -> (Harness, SpiMindWorkflows) {
    let harness = Harness::open().await;
    harness.grant(&["recall"]).await;
    harness
        .record(
            "rec_b",
            "the market opens at dawn",
            "2026-11-20T09:00:03.000Z",
        )
        .await;
    harness
        .record(
            "rec_a",
            "the north gate is closed",
            "2026-11-20T09:00:02.000Z",
        )
        .await;
    harness
        .record(
            "rec_c",
            "north gate guards sleep",
            "2026-11-20T09:00:01.000Z",
        )
        .await;
    let workflows = SpiMindWorkflows::new(MemoryWorkflowBinding::new(harness.pool.clone(), true));
    (harness, workflows)
}

/// One cold recall that leaves the query slot and every page vector warm.
async fn warm_page(workflows: &SpiMindWorkflows, embedder: &AxisEmbedder) {
    workflows
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &embedding_caps(embedder),
        )
        .await
        .expect("warm recall");
}

#[tokio::test]
async fn an_identical_warm_recall_makes_no_provider_call() {
    let (harness, workflows) = vector_page().await;
    let embedder = AxisEmbedder::new("1");

    let first = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &embedding_caps(&embedder),
        )
        .await
        .expect("pack");
    assert_eq!(
        embedder.embedded(),
        [
            "north gate",
            "the north gate is closed",
            "north gate guards sleep",
            "the market opens at dawn",
        ],
        "a cold recall embeds the query and every page text"
    );
    assert_eq!(record_ids(&first), ["rec_a", "rec_c", "rec_b"]);
    assert_eq!(
        harness.vector_rows().await.len(),
        3,
        "every miss is filled once"
    );

    let second = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &embedding_caps(&embedder),
        )
        .await
        .expect("pack");

    assert_eq!(
        embedder.embedded().len(),
        4,
        "a warm identical recall makes no provider call"
    );
    assert_eq!(record_ids(&second), record_ids(&first));
    assert_eq!(
        harness.vector_rows().await.len(),
        3,
        "a warm page is never refilled"
    );
}

#[tokio::test]
async fn a_new_query_reuses_the_current_record_vectors() {
    let (harness, workflows) = vector_page().await;
    let embedder = AxisEmbedder::new("1");
    warm_page(&workflows, &embedder).await;

    let pack = workflows
        .recall_context(
            &principal(),
            &request(Some("market dawn"), 10),
            budget(),
            &embedding_caps(&embedder),
        )
        .await
        .expect("pack");

    assert_eq!(
        embedder.embedded()[4..],
        ["market dawn"],
        "a new query embeds its own text only"
    );
    assert_eq!(record_ids(&pack), ["rec_b", "rec_c", "rec_a"]);
    assert_eq!(harness.vector_rows().await.len(), 3);
}

#[tokio::test]
async fn a_stale_revision_embeds_only_that_record() {
    let (harness, workflows) = vector_page().await;
    let embedder = AxisEmbedder::new("1");
    warm_page(&workflows, &embedder).await;

    sqlx::query(
        "UPDATE memory_records SET revision = revision + 1, content_json = ? WHERE record_id = ?",
    )
    .bind(json!({ "text": "north gate guards wake" }).to_string())
    .bind("rec_c")
    .execute(&harness.pool)
    .await
    .expect("move rec_c");

    workflows
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &embedding_caps(&embedder),
        )
        .await
        .expect("pack");

    assert_eq!(
        embedder.embedded()[4..],
        ["north gate guards wake"],
        "only the moved revision is embedded"
    );
    let rows = harness.vector_rows().await;
    assert_eq!(rows.len(), 3, "the stale row is replaced, not duplicated");
    let rec_c = rows
        .iter()
        .find(|row| row.0 == "rec_c")
        .expect("the moved record's row");
    assert_eq!(rec_c.2, 2, "the row carries the live revision");
}

#[tokio::test]
async fn a_cold_unavailable_query_leaves_a_pre_populated_table_untouched() {
    let (harness, workflows) = vector_page().await;
    warm_page(&workflows, &AxisEmbedder::new("1")).await;
    let before = harness.vector_rows().await;
    assert_eq!(before.len(), 3);

    let cold = UnavailableEmbedder(axes_identity("1"));
    let pack = workflows
        .recall_context(
            &principal(),
            &request(Some("guards sleeping"), 10),
            budget(),
            &embedding_caps(&cold),
        )
        .await
        .expect("lexical pack");

    // The adapter's own order for that query, read with no embedder at all.
    let lexical = SpiMindWorkflows::new(MemoryWorkflowBinding::new(harness.pool.clone(), true))
        .recall_context(
            &principal(),
            &request(Some("guards sleeping"), 10),
            budget(),
            &MindCapabilities::default(),
        )
        .await
        .expect("lexical pack");
    assert_eq!(record_ids(&pack), record_ids(&lexical), "lexical order");
    assert_eq!(
        harness.vector_rows().await,
        before,
        "an unavailable query writes nothing"
    );
}

#[tokio::test]
async fn an_embedder_free_recall_never_probes_the_vector_table() {
    let (harness, _workflows) = vector_page().await;
    // With no embedder no statement may touch the table, so removing it leaves
    // the recall working.
    sqlx::raw_sql("DROP TABLE memory_embedding_vectors")
        .execute(&harness.pool)
        .await
        .expect("drop the vector table");
    let workflows = SpiMindWorkflows::new(MemoryWorkflowBinding::new(harness.pool.clone(), true));

    let pack = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &MindCapabilities::default(),
        )
        .await
        .expect("lexical pack");

    assert_eq!(
        record_ids(&pack),
        ["rec_a", "rec_c", "rec_b"],
        "adapter order"
    );
}

#[tokio::test]
async fn a_miss_batch_that_goes_unavailable_writes_nothing() {
    struct QueryOnlyEmbedder {
        identity: EmbeddingIdentity,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Embedder for QueryOnlyEmbedder {
        fn identity(&self) -> Option<&EmbeddingIdentity> {
            Some(&self.identity)
        }

        async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, SpiError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(texts.iter().map(|text| axis_vector(text)).collect())
            } else {
                Err(SpiError::EmbeddingsUnavailable)
            }
        }
    }

    let (harness, workflows) = vector_page().await;
    let embedder = QueryOnlyEmbedder {
        identity: axes_identity("1"),
        calls: AtomicUsize::new(0),
    };

    let pack = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &embedding_caps(&embedder),
        )
        .await
        .expect("lexical pack");

    assert_eq!(
        record_ids(&pack),
        ["rec_a", "rec_c", "rec_b"],
        "adapter order"
    );
    assert!(
        harness.vector_rows().await.is_empty(),
        "a record batch that goes unavailable writes nothing"
    );
}

#[tokio::test]
async fn a_withheld_read_with_a_warm_page_probes_nothing() {
    let (harness, workflows) = vector_page().await;
    // Warm both caches: the query slot and every page row.
    warm_page(&workflows, &AxisEmbedder::new("1")).await;
    // The warm recall probed the slot once; a withheld read must not add a
    // probe, which the dropped table below cannot observe.
    let probes = workflows.query_slot_probes();
    assert!(probes > 0, "the warm recall exercised the slot probe");
    // Any vector probe from here is a storage error, so a refusal that still
    // probed could not be reported as the withheld guard.
    sqlx::raw_sql("DROP TABLE memory_embedding_vectors")
        .execute(&harness.pool)
        .await
        .expect("drop the vector table");
    withhold_subject(&harness.pool).await;
    let embedder = AxisEmbedder::new("1");

    let error = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &embedding_caps(&embedder),
        )
        .await
        .expect_err("a withheld read must not release a pack");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::InternalError);
    assert_eq!(
        reject
            .details
            .as_ref()
            .and_then(|details| details.get("reason"))
            .and_then(Value::as_str),
        Some("UNRECONCILED"),
        "the withheld guard, not a storage error"
    );
    assert!(
        embedder.embedded().is_empty(),
        "a warm slot does not let a refused page reach the provider"
    );
    assert_eq!(
        workflows.query_slot_probes(),
        probes,
        "the refusal happens before the in-memory query slot is looked up"
    );
}

#[tokio::test]
async fn a_subject_withheld_after_the_query_call_is_refused_before_any_probe() {
    let (harness, workflows) = vector_page().await;
    // A vector probe from here is a storage error, so the refusal must be the
    // withheld guard rebuilding admission after the query provider call.
    sqlx::raw_sql("DROP TABLE memory_embedding_vectors")
        .execute(&harness.pool)
        .await
        .expect("drop the vector table");
    let embedder = harness.embedder(SideEffect::PendingIntent);

    let error = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &embedding_caps(&embedder),
        )
        .await
        .expect_err("a withheld page must not release a pack");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::InternalError);
    assert_eq!(
        reject
            .details
            .as_ref()
            .and_then(|details| details.get("reason"))
            .and_then(Value::as_str),
        Some("UNRECONCILED"),
        "the second snapshot refused the withheld page"
    );
    assert_eq!(
        embedder.embedded(),
        ["north gate"],
        "the query call ran, then the refusal: no record was probed or embedded"
    );
}

#[tokio::test]
async fn a_refused_recall_keeps_no_query_vector() {
    let (harness, workflows) = vector_page().await;
    // The grant is revoked while the record batch is embedded, so the final
    // reauthorization refuses a recall whose query vector was already
    // validated.
    let embedder = harness.embedder(SideEffect::RevokeGrantOnRecordBatch);

    let error = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &embedding_caps(&embedder),
        )
        .await
        .expect_err("a revoked grant must not release a pack");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
    assert_eq!(
        embedder.embedded(),
        [
            "north gate",
            "the north gate is closed",
            "north gate guards sleep",
            "the market opens at dawn",
        ],
        "the query and the record batch ran, then the refusal"
    );

    sqlx::query("UPDATE memory_grants SET revoked_at = NULL WHERE grant_id = 'grt_recall'")
        .execute(&harness.pool)
        .await
        .expect("restore the grant");
    workflows
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &embedding_caps(&embedder),
        )
        .await
        .expect("the retried recall");

    assert_eq!(
        embedder.embedded()[4],
        "north gate",
        "the refused recall cached no query vector"
    );
}

#[tokio::test]
async fn a_moved_page_conflicts_before_its_stored_vector_is_read() {
    let (harness, workflows) = vector_page().await;
    warm_page(&workflows, &AxisEmbedder::new("1")).await;
    // The query provider call moves every record and poisons the moved
    // record's row at its new revision: a probe would fail to decode.
    let embedder = harness.embedder(SideEffect::MovePageAndPoisonVector);

    let error = workflows
        .recall_context(
            &principal(),
            &request(Some("market dawn"), 10),
            budget(),
            &embedding_caps(&embedder),
        )
        .await
        .expect_err("a moved page must not release a pack");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(
        reject.code,
        SpokeRejectCode::RevisionConflict,
        "the page moved, so the read refuses before decoding the newer row"
    );
}

#[tokio::test]
async fn a_retired_identity_never_serves_another_partition() {
    let (harness, workflows) = vector_page().await;
    let retired_embedder = AxisEmbedder::new("1");
    warm_page(&workflows, &retired_embedder).await;

    // A new serving identity cannot hit the rows another one wrote.
    let other_engine =
        SpiMindWorkflows::new(MemoryWorkflowBinding::new(harness.pool.clone(), true));
    let other = AxisEmbedder::new("2");
    other_engine
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &embedding_caps(&other),
        )
        .await
        .expect("pack");
    assert_eq!(
        other.embedded().len(),
        4,
        "another identity embeds the query and every record"
    );
    assert_eq!(
        harness.vector_rows().await.len(),
        6,
        "both partitions coexist"
    );

    let retired = match mindcore_sqlite::vectors::identity_hash("local", "axes", "1", 2) {
        spoke_operations::SpokeResult::Ok(hash) => hash,
        spoke_operations::SpokeResult::Reject(reject) => {
            panic!("the retired identity hashes: {}", reject.message)
        }
    };
    let deleted =
        match mindcore_sqlite::vectors::delete_identity_vectors(&harness.pool, &retired).await {
            spoke_operations::SpokeResult::Ok(deleted) => deleted,
            spoke_operations::SpokeResult::Reject(reject) => {
                panic!("the retired partition deletes: {}", reject.message)
            }
        };
    assert_eq!(
        deleted, 3,
        "one delete clears exactly the retired partition"
    );
    assert_eq!(harness.vector_rows().await.len(), 3);

    // Retirement also clears the identity's query slot: nothing the retired
    // identity produced may be reused.
    workflows.retire_identity(&axes_identity("1"));
    let before = retired_embedder.embedded().len();
    warm_page(&workflows, &retired_embedder).await;
    assert_eq!(
        retired_embedder.embedded()[before..],
        [
            "north gate",
            "the north gate is closed",
            "north gate guards sleep",
            "the market opens at dawn",
        ],
        "a retired identity re-embeds its query and its missing rows"
    );

    // Retiring one identity leaves another identity's slot and rows warm.
    let before = other.embedded().len();
    other_engine.retire_identity(&axes_identity("1"));
    other_engine
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &embedding_caps(&other),
        )
        .await
        .expect("pack");
    assert_eq!(
        other.embedded().len(),
        before,
        "retiring identity 1 keeps identity 2 warm"
    );
}

#[tokio::test]
async fn a_reloaded_page_ranks_exactly_like_fresh_embeddings() {
    let harness = Harness::open().await;
    harness.grant(&["recall"]).await;
    // rec_a and rec_c share their text, so the page carries a real tie.
    harness
        .record(
            "rec_a",
            "the north gate is closed",
            "2026-11-20T09:00:03.000Z",
        )
        .await;
    harness
        .record(
            "rec_c",
            "the north gate is closed",
            "2026-11-20T09:00:02.000Z",
        )
        .await;
    harness
        .record(
            "rec_d",
            "north gate guards sleep",
            "2026-11-20T09:00:01.000Z",
        )
        .await;
    harness
        .record(
            "rec_b",
            "the market opens at dawn",
            "2026-11-20T09:00:00.000Z",
        )
        .await;
    let workflows = SpiMindWorkflows::new(MemoryWorkflowBinding::new(harness.pool.clone(), true));
    let embedder = AxisEmbedder::new("1");

    let fresh = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &embedding_caps(&embedder),
        )
        .await
        .expect("pack");
    let page = ["rec_a", "rec_c", "rec_d", "rec_b"];
    let texts = [
        "the north gate is closed",
        "the north gate is closed",
        "north gate guards sleep",
        "the market opens at dawn",
    ];
    assert_eq!(
        record_ids(&fresh),
        page,
        "the tied pair keeps adapter order"
    );

    // The components the fill wrote, read back through the store's own codec.
    let rows = harness.vector_rows().await;
    let stored: Vec<Vec<f32>> = page
        .iter()
        .map(|record_id| {
            let row = rows
                .iter()
                .find(|row| row.0 == *record_id)
                .expect("the record's stored vector");
            match mindcore_sqlite::vectors::decode_vector(
                &row.4,
                usize::try_from(row.3).expect("stored dimension"),
            ) {
                spoke_operations::SpokeResult::Ok(vector) => vector,
                spoke_operations::SpokeResult::Reject(reject) => {
                    panic!("the stored vector decodes: {}", reject.message)
                }
            }
        })
        .collect();
    let fresh_vectors: Vec<Vec<f32>> = texts.iter().map(|text| axis_vector(text)).collect();
    let bits = |vector: &Vec<f32>| -> Vec<u32> {
        vector.iter().map(|component| component.to_bits()).collect()
    };
    for (fresh, stored) in fresh_vectors.iter().zip(&stored) {
        assert_eq!(
            bits(fresh),
            bits(stored),
            "the BLOB round-trips every component bit"
        );
    }

    // Scored over the same page, the persisted components give identical
    // per-item bits, and the stable order keeps the tie in adapter order.
    let query_vector = axis_vector("north gate");
    let in_memory =
        mindcore_spi::hybrid_scores(&texts, "north gate", &query_vector, &fresh_vectors);
    let reloaded_scores = mindcore_spi::hybrid_scores(&texts, "north gate", &query_vector, &stored);
    let score_bits =
        |scores: &[f64]| -> Vec<u64> { scores.iter().map(|score| score.to_bits()).collect() };
    assert_eq!(
        score_bits(&in_memory),
        score_bits(&reloaded_scores),
        "a persisted component cannot change a score bit"
    );
    let order = |scores: &[f64]| {
        let mut order: Vec<usize> = (0..scores.len()).collect();
        order.sort_by(|&left, &right| scores[right].total_cmp(&scores[left]));
        order
    };
    assert_eq!(
        order(&reloaded_scores),
        [0, 1, 2, 3],
        "the tied pair keeps adapter order"
    );

    // A second engine starts with a cold query slot: every record vector it
    // scores comes back through the stored BLOB, and it serves exactly the
    // page those scores dictate.
    let reloaded_engine =
        SpiMindWorkflows::new(MemoryWorkflowBinding::new(harness.pool.clone(), true));
    let reloaded = reloaded_engine
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &embedding_caps(&embedder),
        )
        .await
        .expect("pack");

    let expected: Vec<&str> = order(&reloaded_scores)
        .into_iter()
        .map(|index| page[index])
        .collect();
    assert_eq!(
        record_ids(&reloaded),
        expected,
        "the persisted page ranks exactly like the in-memory one"
    );
    assert_eq!(reloaded.tokens_used, fresh.tokens_used);
    assert_eq!(
        embedder.embedded().len(),
        6,
        "only the cold query is embedded for the reloaded page"
    );
}
