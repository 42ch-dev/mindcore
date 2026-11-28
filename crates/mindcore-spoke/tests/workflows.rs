//! The workflow binding against a real SQLite fixture: what one recall page
//! costs, which knowledge collection fills the second slot, and the reread
//! that keeps a snapshot moved during provider work out of a released pack.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Number, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Connection, SqliteConnection, SqlitePool};
use tempfile::TempDir;

use mindcore_base::ids::SpaceId;
use mindcore_base::ir::MemorySpaceRow;
use mindcore_base::knowledge::KnowledgeScope;
use mindcore_spi::{
    AbsorbCandidateOutcome, CognitiveTaskKind, Embedder, EmbeddingIdentity, MindCapabilities,
    PolicyDecision, PolicyHook, Reasoner, SettlementDraft, SpiError, SpiMindWorkflows,
    StructuredReasonRequest, WorkflowBinding,
};
use mindcore_spoke::{MemoryAdapter, MemoryStage, MemoryWorkflowBinding};
use mindcore_sqlite::{
    MindStore, ScopedKnowledgeStore, SqliteMemoryRepository, SERVED_KNOWLEDGE_LIST_LIMIT,
};
use spoke_operations::{SpokeRejectCode, SpokeResult};
use spoke_schemas::{MemoryCaptureRequest, MemoryRecallRequest, Principal};

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
            SideEffect::RevokeGrantOnRecordBatch if call == 1 => {
                revoke_recall_grant(&self.pool).await
            }
            SideEffect::RevokeGrantOnRecordBatch => {}
            SideEffect::RevokeKnowledgeGrant => {
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
        let pool = MindStore::open(&dir.path().join("workflows.db"))
            .await
            .expect("open store")
            .pool();
        let mut tx = pool.begin().await.expect("begin");
        let space = MemorySpaceRow {
            space_id: SpaceId::new(SPACE),
            display_name: Some("space".to_string()),
            authority_host_id: "host_1".to_string(),
            authority_generation: 1,
            status: "active".to_string(),
            revision: Number::from(1),
            created_at: NOW.to_string(),
            pending_receipts: 0,
        };
        SqliteMemoryRepository
            .insert_space(&mut tx, &space)
            .await
            .expect("seed space");
        tx.commit().await.expect("commit space");
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
        sqlx::query(
            "INSERT INTO knowledge_entries \
             (collection, entry_id, revision, payload, created_at, updated_at) \
             VALUES ('wren', 'ke_wren', 1, ?, ?, ?)",
        )
        .bind(
            serde_json::json!({
                "schema_version": 1,
                "entry_id": "ke_wren",
                "entry_type": "character",
                "canonical_name": "ke_wren carrier",
                "status": "confirmed",
                "revision": 1,
                "body": {},
                "extensions": {}
            })
            .to_string(),
        )
        .bind(NOW)
        .bind(NOW)
        .execute(&pool)
        .await
        .expect("seed the carrier entry");
        sqlx::query(
            "INSERT INTO minds (space_id, knowledge_collection, carrier_entry_id) VALUES (?, ?, ?)",
        )
        .bind(SPACE)
        .bind("wren")
        .bind("ke_wren")
        .execute(&pool)
        .await
        .expect("seed the mind registry row");
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
        });
        self.insert_entry(collection, entry_id, payload).await;
    }

    /// An `owner`-private entry: only a read whose admitted viewpoint is `owner` may see it.
    async fn private_entry(&self, collection: &str, entry_id: &str, owner: &str, summary: &str) {
        let payload = json!({
            "schema_version": 1,
            "entry_id": entry_id,
            "entry_type": "location",
            "canonical_name": entry_id,
            "status": "confirmed",
            "body": { "summary": summary },
            "disclosure": "owner-private",
            "owner": owner,
            "revision": 1,
            "extensions": {}
        });
        self.insert_entry(collection, entry_id, payload).await;
    }

    async fn insert_entry(&self, collection: &str, entry_id: &str, payload: Value) {
        sqlx::query(
            "INSERT INTO knowledge_entries \
             (collection, entry_id, revision, payload, created_at, updated_at) \
             VALUES (?, ?, 1, ?, ?, ?)",
        )
        .bind(collection)
        .bind(entry_id)
        .bind(payload.to_string())
        .bind(NOW)
        .bind(NOW)
        .execute(&self.pool)
        .await
        .expect("seed knowledge entry");
    }

    /// Rewrites the carrier entry's payload, revision unchanged: the shape the
    /// disclosure fix reads is the live row's current payload.
    async fn set_carrier_payload(&self, payload: Value) {
        sqlx::query(
            "UPDATE knowledge_entries SET payload = ? \
             WHERE collection = 'wren' AND entry_id = 'ke_wren'",
        )
        .bind(payload.to_string())
        .execute(&self.pool)
        .await
        .expect("rewrite the carrier payload");
    }

    /// One schema-valid belief judgment on the carrier, stored the way a
    /// dedicated append stores it: a `cognition_records` row the read hydrates
    /// into the payload's `records` member.
    async fn judgment(&self, record_id: &str) {
        let reference =
            |kind: &str, id: &str| json!({ "kind": kind, "id": id, "namespace": "mindcore" });
        let record = json!({
            "assessed_at": NOW,
            "assessment_context": { "context_id": "ctx_gate" },
            "assessor": reference("knowledge_entry", "ke_wren"),
            "assessor_role": "holder_self_report",
            "basis": {
                "acquisition": "Perception",
                "evidence": [reference("memory_record", "rec_gate")],
                "kind": "evidence",
            },
            "effective_at": NOW,
            "extensions": {},
            "holder": reference("knowledge_entry", "ke_wren"),
            "kind": "belief",
            "order": 1,
            "q": {
                "assertion": "belief_attribution",
                "calibration": "uncalibrated",
                "method": "single_self_report",
                "value": 0.4,
            },
            "record_id": record_id,
            "target": {
                "carrier": reference("knowledge_entry", "ke_wren"),
                "kind": "proposition",
                "target_id": "prop_gate_closed",
            },
            "truth": "True",
            "view": "current",
        });
        sqlx::query(
            "INSERT INTO cognition_records \
             (collection, entry_id, append_seq, record_id, payload, append_prior_shape) \
             VALUES ('wren', 'ke_wren', 1, ?, ?, 0)",
        )
        .bind(record_id)
        .bind(record.to_string())
        .execute(&self.pool)
        .await
        .expect("seed the judgment record");
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

    fn binding(&self, side_effect: SideEffect) -> (SpiMindWorkflows, BoundaryEmbedder) {
        let binding = MemoryWorkflowBinding::new(self.pool.clone(), true);
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

    /// The result of the applied absorb-decision marker this store holds, when
    /// one absorb run recorded a Pending decision.
    async fn decision_marker(&self) -> Option<Value> {
        let raw: Option<String> = sqlx::query_scalar(
            "SELECT result_json FROM memory_journal \
             WHERE op = 'mindcore.absorb.decision' AND state = 'applied'",
        )
        .fetch_optional(&self.pool)
        .await
        .expect("absorb decision marker");
        raw.map(|raw| serde_json::from_str(&raw).expect("absorb decision result"))
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

/// Resolve `candidate_id` outside absorb: the stand-alone adapter dismisses it
/// and settles that intent on its own transaction, exactly as a host would.
async fn resolve_externally(harness: &Harness, candidate_id: &str) {
    let adapter = MemoryAdapter::new(true);
    let mut transaction = harness
        .pool
        .begin()
        .await
        .expect("external settlement transaction");
    let staged = adapter
        .dismiss_candidate(
            &mut transaction,
            &principal(),
            json!({
                "space_id": SPACE,
                "candidate_id": candidate_id,
                "expected_revision": 1,
                "idempotency_key": format!("external_{candidate_id}"),
            }),
        )
        .await;
    let (space, receipt_id) = match staged {
        SpokeResult::Ok(MemoryStage::JournalPending { space, receipt_id }) => (space, receipt_id),
        other => panic!("the external dismissal must stage its intent, got {other:?}"),
    };
    let settled = adapter
        .settle_journal(&mut transaction, &space, &receipt_id)
        .await;
    assert!(
        matches!(settled, SpokeResult::Ok(_)),
        "the external dismissal must settle, got {settled:?}"
    );
    transaction
        .commit()
        .await
        .expect("external settlement commit");
}

/// Rewrite the recorded absorb decision, the way a corrupt row would carry it.
async fn rewrite_decision(harness: &Harness, decision: Value) {
    sqlx::query("UPDATE memory_journal SET result_json = ? WHERE op = 'mindcore.absorb.decision'")
        .bind(decision.to_string())
        .execute(&harness.pool)
        .await
        .expect("rewrite the absorb decision");
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
    let (workflows, embedder) = harness.binding(SideEffect::None);

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
    assert_eq!(entries, ["ke_gate", "ke_market", "ke_wren"]);
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
    // The carrier entry is the collection's first live row, so exactly-at-the-bound means
    // SERVED_KNOWLEDGE_LIST_LIMIT - 1 seeded rows beside it.
    for index in 0..(SERVED_KNOWLEDGE_LIST_LIMIT - 1) {
        harness
            .entry("wren", &format!("ke_{index:04}"), "bounded entry")
            .await;
    }
    let (workflows, embedder) = harness.binding(SideEffect::None);
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

/// The knowledge slot applies the served list's disclosure predicate. The binding
/// carries no admitted viewpoint, so an `owner`-private entry must never reach the
/// pack, however large the recall's budget.
#[tokio::test]
async fn knowledge_slot_hides_owner_private_entries() {
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
    harness
        .private_entry("wren", "ke_mira_diary", "ke_mira", "mira's private notes")
        .await;
    let (workflows, embedder) = harness.binding(SideEffect::None);

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
    assert_eq!(
        entries,
        ["ke_gate", "ke_wren"],
        "an owner-private entry must not reach a pack with no admitted viewpoint"
    );
}

/// A superseded entry is not current: the pack's knowledge slot omits it, while an owner-private
/// entry stays withheld from the viewpoint-free read.
#[tokio::test]
async fn knowledge_slot_omits_a_superseded_entry() {
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
        .private_entry("wren", "ke_mira_diary", "ke_mira", "mira's private notes")
        .await;
    let wren = ScopedKnowledgeStore::new(harness.pool.clone(), KnowledgeScope::new("wren"));
    match wren.supersede_entry("ke_market", 1, "ke_gate").await {
        SpokeResult::Ok(revision) => assert_eq!(revision, 2),
        SpokeResult::Reject(reject) => panic!("marking the slot entry failed: {reject:?}"),
    }
    let (workflows, embedder) = harness.binding(SideEffect::None);

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
    assert_eq!(
        entries,
        ["ke_gate", "ke_wren"],
        "a superseded entry never reaches the pack, and the private one stays withheld"
    );
}

/// `read_cognition` applies the knowledge slot's disclosure predicate, viewpoint-free: an
/// `owner`-private carrier's judgments answer the empty view a Mind that has judged nothing
/// answers — no records and a null `MindState` derived from the filtered set — while a
/// space-visible carrier still serves its record and derived state to the same
/// `knowledge_read` peer.
#[tokio::test]
async fn cognition_read_hides_an_owner_private_carrier_from_a_viewpoint_free_peer() {
    let harness = Harness::open().await;
    harness.grant(&["knowledge_read"]).await;
    // One judgment on the carrier, stored the way a dedicated append stores it: a
    // `cognition_records` row the read hydrates into the payload's module.
    let module = json!({
        "records": [],
        "world_facts": [],
        "extensions": {},
    });
    let private_payload = json!({
        "schema_version": 1,
        "entry_id": "ke_wren",
        "entry_type": "character",
        "canonical_name": "ke_wren carrier",
        "status": "confirmed",
        "revision": 1,
        "body": {},
        "disclosure": "owner-private",
        "owner": "prn_owner",
        "modules": { "cognition": module },
        "extensions": {},
    });
    harness.set_carrier_payload(private_payload).await;
    harness.judgment("rec_judgment").await;
    let mut tx = harness.pool.begin().await.expect("begin hidden read");
    let hidden = MemoryAdapter::read_cognition(&mut tx, &principal(), SPACE)
        .await
        .expect("the hidden carrier read");
    assert_eq!(hidden.carrier_revision, 1);
    assert_eq!(
        hidden.records,
        Vec::<Value>::new(),
        "an owner-private carrier serves no records to a viewpoint-free peer"
    );
    assert_eq!(
        hidden.mind_state,
        Value::Null,
        "the derived state follows the filtered set: {hidden:?}"
    );
    drop(tx);

    let visible_payload = json!({
        "schema_version": 1,
        "entry_id": "ke_wren",
        "entry_type": "character",
        "canonical_name": "ke_wren carrier",
        "status": "confirmed",
        "revision": 1,
        "body": {},
        "modules": { "cognition": module },
        "extensions": {},
    });
    harness.set_carrier_payload(visible_payload).await;
    let mut tx = harness.pool.begin().await.expect("begin visible read");
    let visible = MemoryAdapter::read_cognition(&mut tx, &principal(), SPACE)
        .await
        .expect("the space-visible carrier read");
    assert_eq!(visible.carrier_revision, 1);
    let records = &visible.records;
    assert_eq!(
        records.len(),
        1,
        "the judgment serves once visible: {visible:?}"
    );
    assert_eq!(records[0]["record_id"], "rec_judgment");
    assert!(
        visible.mind_state.is_object(),
        "the derived state serves beside the record"
    );
    assert_eq!(visible.mind_state["mind_state_id"], SPACE);
    assert_eq!(visible.mind_state["holder_entry_id"], "ke_wren");
}

/// Tombstoning a record prunes its stored vectors in the tombstone's own transaction:
/// a row the store can never serve again must not outlive the live record.
#[tokio::test]
async fn tombstoning_a_record_prunes_its_vector_rows() {
    use mindcore_spoke::{MemoryAdapter, MemoryStage};
    use spoke_operations::SpokeResult;

    let harness = Harness::open().await;
    harness.grant(&["recall", "records_write"]).await;
    harness
        .record(
            "rec_a",
            "the north gate is closed",
            "2026-11-20T09:00:01.000Z",
        )
        .await;
    let (workflows, embedder) = harness.binding(SideEffect::None);
    workflows
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
        .expect("recall warms the vector");
    assert_eq!(
        harness.vector_rows().await.len(),
        1,
        "the recall fills the live record's vector"
    );

    let adapter = MemoryAdapter::new(false);
    let mut tx = harness.pool.begin().await.expect("begin delete");
    let staged = adapter
        .delete_record(
            &mut tx,
            &principal(),
            json!({
                "space_id": SPACE,
                "record_id": "rec_a",
                "expected_revision": 1,
                "idempotency_key": "del-v1"
            }),
        )
        .await;
    assert!(
        matches!(staged, SpokeResult::Ok(MemoryStage::JournalPending { .. })),
        "the delete stages its journaled tombstone: {staged:?}"
    );
    tx.commit().await.expect("commit the tombstone");

    assert!(
        harness.vector_rows().await.is_empty(),
        "a tombstone keeps no vector row"
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
    let (workflows, embedder) = harness.binding(SideEffect::RevokeGrant);

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
    let (workflows, embedder) = harness.binding(SideEffect::PendingIntent);

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
    let (workflows, embedder) = harness.binding(SideEffect::BumpRevision);

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
    harness.grant(&["recall"]).await;
    harness
        .record(
            "rec_a",
            "the north gate is closed",
            "2026-11-20T09:00:01.000Z",
        )
        .await;
    harness.entry("wren", "ke_gate", "closed after dusk").await;
    let (workflows, embedder) = harness.binding(SideEffect::None);

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

/// Under per-Mind routing the slot empties by grant alone: a caller holding its own live
/// `knowledge_read` grant but no registry row of its own reads its records with an empty slot,
/// and the recall is never refused for it.
#[tokio::test]
async fn a_caller_without_knowledge_read_recalls_with_an_empty_slot() {
    let harness = Harness::open().await;
    harness.grant(&["recall"]).await;
    harness
        .record(
            "rec_a",
            "the north gate is closed",
            "2026-11-20T09:00:01.000Z",
        )
        .await;
    harness.entry("wren", "ke_gate", "closed after dusk").await;
    let (workflows, embedder) = harness.binding(SideEffect::None);

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
        .expect("a reader without a knowledge grant still reads its memory page");

    assert_eq!(record_ids(&pack), ["rec_a"]);
    assert!(
        pack.knowledge.is_empty(),
        "no knowledge grant means an empty slot: {:?}",
        pack.knowledge
    );
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
    let (workflows, embedder) = harness.binding(SideEffect::RevokeKnowledgeGrant);

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
    assert_eq!(
        harness
            .count_from("memory_journal WHERE op <> 'mindcore.absorb.decision'")
            .await,
        0
    );
    assert_eq!(
        harness
            .count_from(
                "memory_journal WHERE op = 'mindcore.absorb.decision' \
                 AND state = 'applied' AND targets_json = '[]'"
            )
            .await,
        1
    );
    assert_eq!(
        harness.decision_marker().await,
        Some(json!({ "pending_candidate_ids": [candidate_id] }))
    );
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
    assert_eq!(
        harness.decision_marker().await,
        Some(json!({ "pending_candidate_ids": [candidate_id] })),
        "a no-reasoner Pending run records its decision"
    );
}

#[tokio::test]
async fn a_capture_only_principal_absorbs_a_pending_candidate() {
    let harness = Harness::open().await;
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

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
    assert!(reject.details.is_none(), "{reject:?}");
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
    assert_eq!(harness.count_from("memory_records").await, 2);
}

#[tokio::test]
async fn a_settlement_that_cannot_begin_after_staging_reports_an_uncertain_outcome() {
    let harness = granted_harness().await;
    let pool = harness.pool.clone();
    let workflows = SpiMindWorkflows::new(
        MemoryWorkflowBinding::new(pool.clone(), true).with_publisher(move |_| {
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
async fn a_held_then_externally_resolved_capture_replays_as_pending() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let request = capture("cap_held_resolved", "the north gate is closed at dusk");
    let reasoner = FixedReasoner::new(
        json!({ "action": "promote", "evidence_ids": ["rec_gate"] }),
        &harness.pool,
    );

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
    let [AbsorbCandidateOutcome::Pending { candidate_id }] = held.as_slice() else {
        panic!("expected one held candidate, got {held:?}");
    };
    assert_eq!(
        harness.decision_marker().await,
        Some(json!({ "pending_candidate_ids": [candidate_id] })),
        "the held decision is recorded once"
    );

    resolve_externally(&harness, candidate_id).await;
    assert_eq!(harness.candidate_status(candidate_id).await, "consumed");

    let reopened = harness.absorb_binding();
    let bumping = FixedReasoner::new(
        json!({ "action": "promote", "evidence_ids": [] }),
        &harness.pool,
    )
    .bumping();
    let policy = AutoPolicy;
    for _ in 0..2 {
        let replay = reopened
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
        assert_eq!(replay, held, "the replay reports the original decision");
    }

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
    assert_eq!(
        harness
            .count_from("memory_journal WHERE op = 'mindcore.absorb.decision'")
            .await,
        1
    );
    assert_eq!(
        harness
            .count_from("memory_journal WHERE op = 'memory.candidates.dismiss'")
            .await,
        1
    );
    assert_eq!(
        harness
            .count_from("memory_candidates WHERE status = 'consumed'")
            .await,
        1
    );
    assert_eq!(
        harness.decision_marker().await,
        Some(json!({ "pending_candidate_ids": [candidate_id] }))
    );
}

#[tokio::test]
async fn a_marker_under_another_subject_fails_the_replay_closed() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let request = capture("cap_marker_subject", "the north gate is closed at dusk");

    let held = workflows
        .absorb(&principal(), &request, None, &MindCapabilities::default())
        .await
        .expect("held absorb");
    assert!(matches!(
        held.as_slice(),
        [AbsorbCandidateOutcome::Pending { .. }]
    ));

    sqlx::query(
        "UPDATE memory_journal SET subject_id = 'subj_other' \
         WHERE op = 'mindcore.absorb.decision'",
    )
    .execute(&harness.pool)
    .await
    .expect("rewrite the marker subject");

    let error = workflows
        .absorb(&principal(), &request, None, &MindCapabilities::default())
        .await
        .expect_err("a marker for another subject must not replay as Pending");
    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::InternalError);
}

#[tokio::test]
async fn a_repeated_recorded_decision_fails_the_replay_closed() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let request = capture("cap_marker_repeated", "the north gate is closed at dusk");

    let held = workflows
        .absorb(&principal(), &request, None, &MindCapabilities::default())
        .await
        .expect("held absorb");
    let [AbsorbCandidateOutcome::Pending { candidate_id }] = held.as_slice() else {
        panic!("expected one held candidate, got {held:?}");
    };

    rewrite_decision(
        &harness,
        json!({ "pending_candidate_ids": [candidate_id, candidate_id] }),
    )
    .await;
    let error = workflows
        .absorb(&principal(), &request, None, &MindCapabilities::default())
        .await
        .expect_err("a repeated decision must not replay as Pending");
    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::InternalError);
}

#[tokio::test]
async fn an_empty_recorded_decision_fails_the_replay_closed() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let request = capture("cap_marker_empty", "the north gate is closed at dusk");

    let held = workflows
        .absorb(&principal(), &request, None, &MindCapabilities::default())
        .await
        .expect("held absorb");
    let [AbsorbCandidateOutcome::Pending { candidate_id }] = held.as_slice() else {
        panic!("expected one held candidate, got {held:?}");
    };

    resolve_externally(&harness, candidate_id).await;
    rewrite_decision(&harness, json!({ "pending_candidate_ids": [] })).await;
    let error = workflows
        .absorb(&principal(), &request, None, &MindCapabilities::default())
        .await
        .expect_err("an empty decision must not project the external resolution");
    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::InternalError);
}

#[tokio::test]
async fn a_marker_under_another_space_fails_the_replay_closed() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let request = capture("cap_marker_space", "the north gate is closed at dusk");

    let held = workflows
        .absorb(&principal(), &request, None, &MindCapabilities::default())
        .await
        .expect("held absorb");
    let [AbsorbCandidateOutcome::Pending { candidate_id }] = held.as_slice() else {
        panic!("expected one held candidate, got {held:?}");
    };
    resolve_externally(&harness, candidate_id).await;

    sqlx::query(
        "UPDATE memory_journal SET space_id = 'spc_elsewhere' \
         WHERE op = 'mindcore.absorb.decision'",
    )
    .execute(&harness.pool)
    .await
    .expect("rewrite the marker space");

    let error = workflows
        .absorb(&principal(), &request, None, &MindCapabilities::default())
        .await
        .expect_err("a marker for another space must not project the resolution");
    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::InternalError);
}

#[tokio::test]
async fn an_unexpected_decision_member_fails_the_replay_closed() {
    let harness = granted_harness().await;
    let workflows = harness.absorb_binding();
    let request = capture("cap_marker_member", "the north gate is closed at dusk");

    let held = workflows
        .absorb(&principal(), &request, None, &MindCapabilities::default())
        .await
        .expect("held absorb");
    let [AbsorbCandidateOutcome::Pending { candidate_id }] = held.as_slice() else {
        panic!("expected one held candidate, got {held:?}");
    };

    rewrite_decision(
        &harness,
        json!({ "pending_candidate_ids": [candidate_id], "unexpected": true }),
    )
    .await;
    let error = workflows
        .absorb(&principal(), &request, None, &MindCapabilities::default())
        .await
        .expect_err("a decision carrying an unexpected member must not replay as Pending");
    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::InternalError);
}

#[tokio::test]
async fn a_direct_capture_replay_projects_the_external_resolution() {
    let harness = granted_harness().await;
    let request = capture("cap_external_only", "the north gate is closed at dusk");
    let adapter = MemoryAdapter::new(true);
    let mut transaction = harness.pool.begin().await.expect("capture transaction");
    let staged = adapter
        .capture(
            &mut transaction,
            &principal(),
            serde_json::to_value(&request).expect("capture request"),
        )
        .await;
    let body = match staged {
        SpokeResult::Ok(MemoryStage::Ready(body)) => body,
        other => panic!("a fresh adapter capture is ready, got {other:?}"),
    };
    let candidate_id = body["result"]["candidate_ids"][0]
        .as_str()
        .expect("candidate id")
        .to_string();
    transaction.commit().await.expect("capture commit");
    resolve_externally(&harness, &candidate_id).await;
    assert_eq!(harness.candidate_status(&candidate_id).await, "consumed");

    let workflows = harness.absorb_binding();
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
    assert!(matches!(
        replay.as_slice(),
        [AbsorbCandidateOutcome::Dropped { .. }]
    ));
    assert_eq!(harness.decision_marker().await, None);
    assert_eq!(
        harness
            .stored_record("rec_gate")
            .await
            .expect("seeded record"),
        (1, "the north gate is closed".to_string())
    );
    assert_eq!(harness.count_from("memory_records").await, 1);
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
    deliberate_as(harness, PRINCIPAL, caps, timeout, None).await
}

/// One deliberation under a binding whose carrier route names `caller`.
async fn deliberate_as(
    harness: &Harness,
    caller: &str,
    caps: &MindCapabilities<'_>,
    timeout: std::time::Duration,
    assessment_context_id: Option<&str>,
) -> Result<mindcore_spi::DeliberateOutcome, SpiError> {
    let workflows = SpiMindWorkflows::new(MemoryWorkflowBinding::new(harness.pool.clone(), true));
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
        subject_id: Some(SUBJECT),
        assessment_context_id,
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout,
    };
    let caller_principal: Principal =
        serde_json::from_value(json!({ "principal_id": caller, "extensions": {} }))
            .expect("caller principal");
    workflows
        .deliberate(&caller_principal, request, None, caps)
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
}

fn long_timeout() -> std::time::Duration {
    std::time::Duration::from_secs(30)
}

/// A principal holding no grants in the Mind's space cannot write its carrier: routing keys to
/// the space id, so admission inside the space is the whole boundary and a stranger never reads
/// the snapshot before the refusal.
#[tokio::test]
async fn a_principal_without_grants_cannot_write_the_carrier() {
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

    let error = deliberate_as(&harness, "prn_2", &caps, long_timeout(), None)
        .await
        .expect_err("a stranger in the space may not write its carrier");

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

    assert_eq!(mind_state_version, 2);
    let (revision, payload) = stored_carrier(&harness).await;
    assert_eq!(revision, 2);
    assert_eq!(payload["modules"]["cognition"]["records"], json!([]));
    let records = committed_records(&harness).await;
    assert_eq!(records.len(), 1);
    let stored = &records[0];
    assert_eq!(stored["holder"], cognition_reference(CARRIER));
    assert_eq!(stored["target"]["target_id"], "prop_gate_closed");
    assert_eq!(stored["view"], "current");
    assert_eq!(stored["assessor"]["kind"], "principal");
    assert_eq!(stored["assessment_context"]["context_id"], SUBJECT);
    assert_eq!(
        serde_json::to_value(&record).expect("record serializes"),
        *stored
    );
}

#[tokio::test]
async fn an_explicit_assessment_context_id_reaches_the_stored_record() {
    let harness = deliberate_harness().await;
    let reasoner =
        DeliberateReasoner::new(promote_verdict(), DeliberateEffect::None, &harness.pool);
    let policy = AutoPolicy;
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        policy_hook: Some(&policy),
        ..MindCapabilities::default()
    };

    let outcome = deliberate_as(
        &harness,
        PRINCIPAL,
        &caps,
        long_timeout(),
        Some("ctx_gate_audit"),
    )
    .await
    .expect("deliberate");

    assert!(matches!(
        outcome,
        mindcore_spi::DeliberateOutcome::Settled { .. }
    ));
    let records = committed_records(&harness).await;
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0]["assessment_context"]["context_id"],
        "ctx_gate_audit"
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
    warm_page(&workflows, &AxisEmbedder::new("1")).await;
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
}

#[tokio::test]
async fn a_subject_withheld_after_the_query_call_is_refused_before_any_probe() {
    let (harness, workflows) = vector_page().await;
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
async fn a_release_that_cannot_reserve_its_write_serves_nothing() {
    let (harness, _) = vector_page().await;
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(harness.root.join("workflows.db"))
                .busy_timeout(Duration::ZERO),
        )
        .await
        .expect("open zero-wait pool");
    let workflows = SpiMindWorkflows::new(MemoryWorkflowBinding::new(pool, true));

    let holder = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(harness.root.join("workflows.db"))
                .busy_timeout(Duration::from_secs(5)),
        )
        .await
        .expect("open the second writer");
    let mut reservation = holder
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("hold the write reservation");
    sqlx::query("UPDATE memory_grants SET revoked_at = ? WHERE grant_id = 'grt_recall'")
        .bind(NOW)
        .execute(&mut *reservation)
        .await
        .expect("stage the revocation");

    let embedder = AxisEmbedder::new("1");
    let error = workflows
        .recall_context(
            &principal(),
            &request(Some("north gate"), 10),
            budget(),
            &embedding_caps(&embedder),
        )
        .await
        .expect_err("a release that cannot reserve its write must not serve the page");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::InternalError);
    assert!(
        reject
            .message
            .contains("recall release transaction could not begin"),
        "the reservation must be taken at begin, before the reauthorization read: {}",
        reject.message
    );
    assert!(
        reject.message.contains("locked"),
        "the begin failure must carry SQLite's lock condition: {}",
        reject.message
    );

    reservation
        .rollback()
        .await
        .expect("release the write reservation");
    holder.close().await;

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

    assert_eq!(
        record_ids(&reloaded),
        page,
        "the persisted page ranks exactly like the in-memory one"
    );
    assert_eq!(reloaded.tokens_used, fresh.tokens_used);
    assert_eq!(
        embedder.embedded().len(),
        6,
        "only the cold query is embedded for the reloaded page"
    );
}

/// The outer guard over one settle: a regression fails the run instead of
/// hanging it.
const CANCELLATION_GUARD: Duration = Duration::from_secs(5);

/// A binding over `pool` routed like `deliberate_as` routes it.
fn settle_binding(pool: &SqlitePool) -> MemoryWorkflowBinding {
    MemoryWorkflowBinding::new(pool.clone(), true)
}

fn settle_holder() -> spoke_schemas::data::cognition::CognitionReference {
    serde_json::from_value(cognition_reference(CARRIER)).expect("holder")
}

/// One valid current-view record for the seeded carrier; it cites nothing, so
/// the commit revalidates no memory record.
fn settle_record(record_id: &str) -> Value {
    json!({
        "kind": "belief",
        "order": 1,
        "record_id": record_id,
        "holder": cognition_reference(CARRIER),
        "target": {
            "carrier": cognition_reference("ke_shared_gate"),
            "target_id": "prop_gate_closed",
            "kind": "proposition",
        },
        "assessor": cognition_reference(CARRIER),
        "assessor_role": "holder_self_report",
        "basis": {
            "kind": "evidence",
            "evidence": [{
                "kind": "memory_record",
                "namespace": "mindcore",
                "id": "rec_gate",
            }],
        },
        "assessment_context": { "context_id": SUBJECT },
        "effective_at": "2026-11-20T09:00:00Z",
        "assessed_at": "2026-11-20T09:00:00Z",
        "view": "current",
        "q": {
            "value": 0.8,
            "assertion": "belief_attribution",
            "method": "two_witness_reports",
            "calibration": "uncalibrated",
        },
        "extensions": {},
    })
}

/// The production pool holds one connection, so a settle waiting behind any
/// other statement waits forever: its deadline must end that wait without
/// opening a transaction.
#[tokio::test]
async fn a_settle_refuses_at_its_deadline_while_the_only_connection_is_held() {
    let harness = deliberate_harness().await;
    let binding = settle_binding(&harness.pool);
    let hold = harness
        .pool
        .acquire()
        .await
        .expect("hold the only connection");
    let deadline = std::time::Instant::now() + Duration::from_millis(50);

    let outcome = tokio::time::timeout(
        CANCELLATION_GUARD,
        binding.commit_cognition(
            &principal(),
            SPACE,
            Some(SUBJECT),
            &settle_holder(),
            1,
            settle_record("cog_refused"),
            &[],
            deadline,
        ),
    )
    .await
    .expect("a held connection must not block the settle past its guard");
    let error = outcome.expect_err("an expired acquire refuses the settle");
    assert!(matches!(error, SpiError::DeadlineExceeded), "got {error:?}");

    drop(hold);
    let (revision, _) = stored_carrier(&harness).await;
    assert_eq!(revision, 1, "a refused settle moves no carrier");
    assert_eq!(
        harness.count_from("cognition_records").await,
        0,
        "a refused settle appends no record"
    );
}

/// An acquire that finds a free connection can complete with the deadline
/// already past: the re-check before `BEGIN IMMEDIATE` refuses it.
///
/// The pool pins `test_before_acquire(false)` so the acquire completes on its
/// first poll instead of refusing at the ping, and `before_acquire` records
/// that the hand-over happened. That record is the proof this test is about
/// the re-check and not the acquire's own bound: the acquire ran far enough to
/// give a connection out, so the elapsed deadline can only refuse it later.
#[tokio::test]
async fn a_settle_with_a_passed_deadline_opens_no_transaction_on_a_free_connection() {
    let harness = deliberate_harness().await;
    let handed_out = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let acquired = handed_out.clone();
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .test_before_acquire(false)
        .before_acquire(move |_connection, _metadata| {
            acquired.store(true, Ordering::SeqCst);
            Box::pin(async { Ok(true) })
        })
        .connect_with(
            SqliteConnectOptions::new()
                .filename(harness.root.join("workflows.db"))
                .foreign_keys(true),
        )
        .await
        .expect("open the free-connection pool");
    let binding = settle_binding(&pool);
    // `connect_with` releases one checked connection, so the acquire below
    // takes an idle connection on its first poll.
    let deadline = std::time::Instant::now() - Duration::from_millis(1);

    let outcome = tokio::time::timeout(
        CANCELLATION_GUARD,
        binding.commit_cognition(
            &principal(),
            SPACE,
            Some(SUBJECT),
            &settle_holder(),
            1,
            settle_record("cog_expired"),
            &[],
            deadline,
        ),
    )
    .await
    .expect("a free connection must not block the settle past its guard");
    let error = outcome.expect_err("a passed deadline refuses the settle");
    assert!(matches!(error, SpiError::DeadlineExceeded), "got {error:?}");
    assert!(
        handed_out.load(Ordering::SeqCst),
        "the acquire handed out a connection, so the elapsed deadline was refused by the re-check"
    );

    let (revision, _) = stored_carrier(&harness).await;
    assert_eq!(revision, 1, "a refused settle moves no carrier");
    assert_eq!(
        harness.count_from("cognition_records").await,
        0,
        "a refused settle appends no record"
    );
}

/// The control: the same call on a free connection with time left commits the
/// next carrier revision.
#[tokio::test]
async fn a_settle_commits_on_a_free_connection_within_its_deadline() {
    let harness = deliberate_harness().await;
    let binding = settle_binding(&harness.pool);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);

    let version = tokio::time::timeout(
        CANCELLATION_GUARD,
        binding.commit_cognition(
            &principal(),
            SPACE,
            Some(SUBJECT),
            &settle_holder(),
            1,
            settle_record("cog_settled"),
            &[],
            deadline,
        ),
    )
    .await
    .expect("a free connection must not block the settle past its guard")
    .expect("a settle within its deadline commits");

    assert_eq!(version, 2, "the carrier advances one revision");
    let (revision, payload) = stored_carrier(&harness).await;
    assert_eq!(revision, 2);
    assert_eq!(payload["modules"]["cognition"]["records"], json!([]));
    let records = committed_records(&harness).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["record_id"], "cog_settled");
}

/// The bounded acquire waits rather than failing fast: a connection held only
/// briefly, released well before the deadline, still lets the settle commit.
#[tokio::test]
async fn a_settle_waits_out_a_short_hold_until_its_deadline() {
    /// How long the only connection stays held: short against the deadline,
    /// long against any immediate refusal.
    const HOLD: Duration = Duration::from_millis(150);

    let harness = deliberate_harness().await;
    let binding = settle_binding(&harness.pool);
    let hold = harness
        .pool
        .acquire()
        .await
        .expect("hold the only connection");
    let start = std::time::Instant::now();
    let deadline = start + Duration::from_secs(30);
    let release = tokio::spawn(async move {
        tokio::time::sleep(HOLD).await;
        drop(hold);
    });

    let version = tokio::time::timeout(
        CANCELLATION_GUARD,
        binding.commit_cognition(
            &principal(),
            SPACE,
            Some(SUBJECT),
            &settle_holder(),
            1,
            settle_record("cog_waited"),
            &[],
            deadline,
        ),
    )
    .await
    .expect("the settle must not outlast its guard")
    .expect("a connection released before the deadline lets the settle commit");
    release.await.expect("the release task");

    assert!(
        start.elapsed() >= HOLD,
        "the settle waited for the held connection instead of refusing at once"
    );
    assert_eq!(version, 2, "the carrier advances one revision");
    let records = committed_records(&harness).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["record_id"], "cog_waited");
}

/// A bound binding serves exactly the carrier its row names: a holder naming
/// any other entry of the same collection is refused at both boundaries, so a
/// caller cannot read or append cognition beside the Mind's own carrier.
#[tokio::test]
async fn a_bound_carrier_refuses_a_holder_that_names_another_entry() {
    let harness = deliberate_harness().await;
    let binding = settle_binding(&harness.pool).with_carrier_collection(SPACE, "wren", CARRIER);
    let other: spoke_schemas::data::cognition::CognitionReference =
        serde_json::from_value(cognition_reference("ke_not_the_carrier")).expect("other holder");

    let refused = binding
        .deliberate_snapshot(&principal(), SPACE, Some(SUBJECT), &other, 32)
        .await
        .expect_err("a foreign holder is refused before any carrier read");
    assert!(
        matches!(refused, SpiError::InvalidInput(_)),
        "the binding's identity is enforced, got {refused:?}"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let refused = binding
        .commit_cognition(
            &principal(),
            SPACE,
            Some(SUBJECT),
            &other,
            1,
            settle_record("cog_foreign"),
            &[],
            deadline,
        )
        .await
        .expect_err("a foreign holder is refused before any write");
    assert!(
        matches!(refused, SpiError::InvalidInput(_)),
        "the commit refuses the foreign holder too, got {refused:?}"
    );
    assert_eq!(
        harness.count_from("cognition_records").await,
        0,
        "the refused commit appended nothing"
    );

    let snapshot = binding
        .deliberate_snapshot(&principal(), SPACE, Some(SUBJECT), &settle_holder(), 32)
        .await
        .expect("the bound carrier is the one this binding serves");
    assert_eq!(snapshot.carrier_revision, 1);
}

/// A request naming neither a subject nor an assessment context is the caller's
/// invalid input: the check runs before the snapshot and the reasoner, so even a
/// verdict that would write nothing cannot make it succeed.
#[tokio::test]
async fn a_request_naming_neither_subject_nor_context_is_refused_before_the_verdict() {
    struct NoRecordReasoner(Arc<AtomicUsize>);

    #[async_trait]
    impl Reasoner for NoRecordReasoner {
        async fn reason_structured(
            &self,
            _request: StructuredReasonRequest<'_>,
        ) -> Result<Value, SpiError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(json!({
                "recommendation": { "kind": "none" },
                "rationale": "nothing worth recording",
                "evidence_ids": [],
                "record_ids": [],
            }))
        }
    }

    let harness = deliberate_harness().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let reasoner = NoRecordReasoner(Arc::clone(&calls));
    let caps = MindCapabilities {
        reasoner: Some(&reasoner),
        ..MindCapabilities::default()
    };
    let workflows = SpiMindWorkflows::new(MemoryWorkflowBinding::new(harness.pool.clone(), true));
    let holder: spoke_schemas::data::cognition::CognitionReference =
        serde_json::from_value(cognition_reference(CARRIER)).expect("holder");
    let target: spoke_schemas::data::cognition::CognitionTarget = serde_json::from_value(json!({
        "carrier": cognition_reference(CARRIER),
        "target_id": "prop_gate_closed",
        "kind": "proposition",
    }))
    .expect("target");
    let evidence = [mindcore_spi::Evidence {
        id: "ev_gate",
        text: "the gate test passed on this commit",
    }];
    let request = mindcore_spi::DeliberateRequest {
        space_id: SPACE,
        subject_id: None,
        assessment_context_id: None,
        holder: &holder,
        target: &target,
        evidence: &evidence,
        timeout: Duration::from_secs(5),
    };

    let refused = workflows
        .deliberate(&principal(), request, None, &caps)
        .await
        .expect_err("a request with neither subject nor context is refused");
    assert!(
        matches!(refused, SpiError::InvalidInput(_)),
        "the refusal is the caller's invalid input, got {refused:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "no reasoner work ran before the request was refused"
    );
    assert_eq!(
        harness.count_from("cognition_records").await,
        0,
        "the refused request wrote nothing"
    );
}
