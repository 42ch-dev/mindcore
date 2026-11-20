//! The workflow binding against a real SQLite fixture: what one recall page
//! costs, which knowledge collection fills the second slot, and the reread
//! that keeps a snapshot moved during provider work out of a released pack.

use std::num::NonZeroUsize;

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use tempfile::TempDir;

use mindcore_base::knowledge::KnowledgeScope;
use mindcore_spi::{Embedder, EmbeddingIdentity, MindCapabilities, SpiError, SpiMindWorkflows};
use mindcore_spoke::MemoryWorkflowBinding;
use spoke_operations::SpokeRejectCode;
use spoke_schemas::{MemoryRecallRequest, Principal};

const DDL: &str = include_str!("../../mindcore-sqlite/tests/fixtures/settlement-prepare.sql");
/// The knowledge table of `migrations/0001_init.sql`; that migration's legacy
/// memory tables have a different shape than the protocol fixture above.
const KNOWLEDGE_DDL: &str = "
CREATE TABLE IF NOT EXISTS knowledge_entries (
  collection   TEXT NOT NULL,
  entry_id     TEXT NOT NULL,
  revision     INTEGER NOT NULL,
  payload      TEXT NOT NULL,
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL,
  PRIMARY KEY (collection, entry_id)
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
}

struct BoundaryEmbedder {
    identity: EmbeddingIdentity,
    side_effect: SideEffect,
    pool: SqlitePool,
}

#[async_trait]
impl Embedder for BoundaryEmbedder {
    fn identity(&self) -> Option<&EmbeddingIdentity> {
        Some(&self.identity)
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, SpiError> {
        match self.side_effect {
            SideEffect::None => {}
            SideEffect::RevokeGrant => {
                sqlx::query("UPDATE memory_grants SET revoked_at = ? WHERE principal_id = ?")
                    .bind(NOW)
                    .bind(PRINCIPAL)
                    .execute(&self.pool)
                    .await
                    .expect("revoke the grant");
            }
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
            SideEffect::PendingIntent => {
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
        Ok(texts.iter().map(|_| vec![1.0, 0.0]).collect())
    }
}

struct Harness {
    _dir: TempDir,
    pool: SqlitePool,
}

impl Harness {
    async fn open() -> Self {
        let dir = TempDir::new().expect("temporary directory");
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
        Self { _dir: dir, pool }
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

    fn binding(
        &self,
        caller: &str,
        side_effect: SideEffect,
    ) -> (SpiMindWorkflows, BoundaryEmbedder) {
        let binding = MemoryWorkflowBinding::new(self.pool.clone(), true)
            .with_knowledge_collection(caller, KnowledgeScope::new("wren"));
        let embedder = BoundaryEmbedder {
            identity: EmbeddingIdentity {
                provider_id: "local".to_string(),
                model_id: "axes".to_string(),
                model_version: "1".to_string(),
                dim: 2,
            },
            side_effect,
            pool: self.pool.clone(),
        };
        (SpiMindWorkflows::new(binding), embedder)
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
async fn memory_grant_alone_does_not_admit_knowledge() {
    let harness = Harness::open().await;
    // `recall` is a memory capability; it admits no knowledge read.
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

    let error = workflows
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
        .expect_err("a memory grant must not release the bound collection");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
}

#[tokio::test]
async fn another_principal_is_refused_the_bound_collection() {
    let harness = Harness::open().await;
    // The caller's own admission is complete — only the bound caller differs.
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

    let error = workflows
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
        .expect_err("another caller's collection must not be served");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
}

#[tokio::test]
async fn revoked_knowledge_admission_during_provider_work_refuses_the_pack() {
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
        .expect_err("a revoked knowledge admission must not release a pack");

    let SpiError::Settlement(reject) = error else {
        panic!("expected a settlement refusal, got {error:?}");
    };
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
}
