//! The adapter's protocol surface against a caller-held SQLite transaction:
//! wire→IR→wire round-trips, the exact reject boundary, and the staging split
//! between what the adapter commits and what only the caller may commit.

use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Sqlite, SqlitePool, Transaction};
use tempfile::TempDir;

use mindcore_spoke::{MemoryAdapter, MemoryStage};
use spoke_operations::{SpokeReject, SpokeRejectCode, SpokeResult};
use spoke_schemas::Principal;

const DDL: &str = include_str!("../../mindcore-sqlite/tests/fixtures/settlement-prepare.sql");
const SPACE: &str = "sp_1";
const SUBJECT: &str = "s_1";
const PRINCIPAL: &str = "p_1";
const STRANGER: &str = "p_2";
const NOW: &str = "2026-11-20T09:20:01.123Z";

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
                    .filename(dir.path().join("memory.db"))
                    .create_if_missing(true)
                    .foreign_keys(false),
            )
            .await
            .expect("open pool");
        sqlx::raw_sql(DDL).execute(&pool).await.expect("apply DDL");
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

    async fn begin(&self) -> Transaction<'_, Sqlite> {
        self.pool.begin().await.expect("begin")
    }

    async fn seed_grant(&self, principal: &str, capabilities: &[&str]) {
        sqlx::query(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
              shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES (?, ?, NULL, ?, ?, '[]', 0, NULL, NULL, 1, ?)",
        )
        .bind(format!("grt_{principal}"))
        .bind(SPACE)
        .bind(principal)
        .bind(serde_json::to_string(capabilities).expect("capabilities"))
        .bind(NOW)
        .execute(&self.pool)
        .await
        .expect("seed grant");
    }

    async fn count(&self, tx: &mut Transaction<'_, Sqlite>, table: &str) -> i64 {
        let sql = format!("SELECT COUNT(*) FROM {table}");
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql))
            .fetch_one(&mut **tx)
            .await
            .expect("count")
    }

    /// Count after the caller's transaction ended, when the only connection is
    /// free again.
    async fn count_pool(&self, table: &str) -> i64 {
        let sql = format!("SELECT COUNT(*) FROM {table}");
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql))
            .fetch_one(&self.pool)
            .await
            .expect("count")
    }
}

fn principal(id: &str) -> Principal {
    serde_json::from_value(json!({
        "principal_id": id,
        "extensions": {},
    }))
    .expect("principal")
}

fn reject(result: &SpokeResult<MemoryStage>) -> &SpokeReject {
    match result {
        SpokeResult::Reject(reject) => reject,
        SpokeResult::Ok(_) => panic!("expected a reject, got {result:?}"),
    }
}

fn retry_class(reject: &SpokeReject) -> &str {
    reject
        .details
        .as_ref()
        .and_then(|details| details.get("retry_class"))
        .and_then(Value::as_str)
        .expect("every reject names its retry_class")
}

fn ready(result: SpokeResult<MemoryStage>) -> Value {
    match result {
        SpokeResult::Ok(MemoryStage::Ready(body)) => body,
        other => panic!("expected a ready stage, got {other:?}"),
    }
}

/// §7.1: `describe` derives its surface from the instance's own policy.
#[tokio::test]
async fn describe_derives_the_served_surface() {
    let harness = Harness::open().await;
    let mut tx = harness.begin().await;
    let request = json!({});

    let with_reflection = MemoryAdapter::new(true);
    let body = ready(
        with_reflection
            .describe(&mut tx, &principal(PRINCIPAL), request.clone())
            .await,
    );
    assert_eq!(body["memory_protocol_version"], 1);
    assert_eq!(
        body["capabilities"],
        json!(["spoke-memory", "spoke-memory-reflection"])
    );
    assert_eq!(body["engine_policy_version"], "mindcore-local-memory-v1");
    assert_eq!(body["required_profile_kinds"], json!(["personality"]));
    assert_eq!(body["limits"]["max_page_size"], 100);

    let without_reflection = MemoryAdapter::new(false);
    let body = ready(
        without_reflection
            .describe(&mut tx, &principal(PRINCIPAL), request)
            .await,
    );
    assert_eq!(body["capabilities"], json!(["spoke-memory"]));
}

/// §7.11: the capture commits its receipt in the caller's transaction, and the
/// stored candidate keeps the request's content subtree byte for byte.
#[tokio::test]
async fn capture_commits_a_receipt_and_preserves_the_content_subtree() {
    let harness = Harness::open().await;
    harness
        .seed_grant(
            PRINCIPAL,
            &["capture", "review", "records_read", "records_write"],
        )
        .await;
    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;
    let request = json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "capture_scope": { "visibility": "shared" },
        "content": {
            "text": "a capture with enough words to be promoted by provider policy",
            "structured": { "k": [1, 2] }
        },
        "idempotency_key": "cap-1"
    });
    let body = ready(
        adapter
            .capture(&mut tx, &principal(PRINCIPAL), request)
            .await,
    );
    assert_eq!(body["status"], "committed");
    assert_eq!(body["op"], "memory.capture");
    assert_eq!(body["result"]["classified"], 1);
    let candidate_id = body["result"]["candidate_ids"][0]
        .as_str()
        .expect("candidate id")
        .to_string();
    tx.commit().await.expect("commit");

    let content: String = sqlx::query_scalar(
        "SELECT content_json FROM memory_candidates WHERE space_id = ? AND candidate_id = ?",
    )
    .bind(SPACE)
    .bind(&candidate_id)
    .fetch_one(&harness.pool)
    .await
    .expect("stored candidate");
    assert_eq!(
        serde_json::from_str::<Value>(&content).expect("content json"),
        json!({
            "text": "a capture with enough words to be promoted by provider policy",
            "structured": { "k": [1, 2] }
        })
    );
}

/// §7.17: a stored record reads back as the IR projection of its own wire body.
#[tokio::test]
async fn get_record_round_trips_wire_through_ir() {
    let harness = Harness::open().await;
    harness
        .seed_grant(PRINCIPAL, &["records_read", "records_write"])
        .await;
    let stored = json!({
        "record_id": "rec_1",
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "kind": "fact",
        "content": { "text": "an authored record", "structured": { "n": 7 } },
        "scope": { "visibility": "shared" },
        "provenance": {
            "source_context_ids": [],
            "anchors": [],
            "captured_by": "adapter",
            "captured_at": NOW
        },
        "created_at": NOW,
        "updated_at": NOW,
        "revision": 1,
        "extensions": {}
    });
    sqlx::query(
        "INSERT INTO memory_records \
         (record_id, space_id, subject_id, kind, content_json, scope_visibility, \
          scope_context_id, provenance_json, import_ref_json, occurred_at, tombstone_json, \
          revision, created_at, updated_at) \
         VALUES ('rec_1', ?, ?, 'fact', ?, 'shared', NULL, ?, NULL, NULL, NULL, 1, ?, ?)",
    )
    .bind(SPACE)
    .bind(SUBJECT)
    .bind(serde_json::to_string(&stored["content"]).expect("content"))
    .bind(serde_json::to_string(&stored["provenance"]).expect("provenance"))
    .bind(NOW)
    .bind(NOW)
    .execute(&harness.pool)
    .await
    .expect("seed record");

    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;
    let body = ready(
        adapter
            .get_record(
                &mut tx,
                &principal(PRINCIPAL),
                json!({ "space_id": SPACE, "record_id": "rec_1" }),
            )
            .await,
    );
    let record = &body["record"];
    assert_eq!(record["record_id"], "rec_1");
    assert_eq!(record["content"], stored["content"]);
    assert_eq!(record["scope"], stored["scope"]);
    assert_eq!(record["revision"], 1);
    // The projection is a valid protocol body: it decodes through the codec.
    mindcore_base::ir::wire::memory_record_from_wire(record.clone())
        .expect("the rebuilt body decodes to IR");
    tx.rollback().await.expect("rollback");
}

/// §6.5: no coverage is `NOT_AUTHORIZED`/terminal and leaves no effect at all.
#[tokio::test]
async fn unauthorized_capture_rejects_without_an_effect() {
    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["capture"]).await;
    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;
    let request = json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "capture_scope": { "visibility": "shared" },
        "content": { "text": "a stranger capture attempt" },
        "idempotency_key": "cap-x"
    });
    let result = adapter
        .capture(&mut tx, &principal(STRANGER), request)
        .await;
    let reject = reject(&result);
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
    assert_eq!(retry_class(reject), "terminal");
    assert_eq!(harness.count(&mut tx, "memory_candidates").await, 0);
    assert_eq!(harness.count(&mut tx, "memory_receipts").await, 0);
    tx.rollback().await.expect("rollback");
}

/// §7.0.4: a stale expected revision is `REVISION_CONFLICT`/terminal and writes
/// nothing, so the caller's rollback is always safe.
#[tokio::test]
async fn stale_space_revision_rejects_and_writes_nothing() {
    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["admin"]).await;
    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;
    let result = adapter
        .freeze_space(
            &mut tx,
            &principal(PRINCIPAL),
            json!({
                "space_id": SPACE,
                "expected_revision": 9,
                "idempotency_key": "freeze-1"
            }),
        )
        .await;
    let reject = reject(&result);
    assert_eq!(reject.code, SpokeRejectCode::RevisionConflict);
    assert_eq!(retry_class(reject), "terminal");
    assert_eq!(harness.count(&mut tx, "memory_receipts").await, 0);
    let status: String = sqlx::query_scalar("SELECT status FROM memory_spaces WHERE space_id = ?")
        .bind(SPACE)
        .fetch_one(&mut *tx)
        .await
        .expect("space");
    assert_eq!(status, "active");
    tx.rollback().await.expect("rollback");
}

/// §7.0.5: the same key with a different payload is `IDEMPOTENCY_CONFLICT`.
#[tokio::test]
async fn same_key_with_a_different_payload_rejects_as_idempotency_conflict() {
    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["capture"]).await;
    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;
    let first = json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "capture_scope": { "visibility": "shared" },
        "content": { "text": "the first captured payload" },
        "idempotency_key": "cap-dup"
    });
    let mut second = first.clone();
    second["content"] = json!({ "text": "a different captured payload" });
    let _ = ready(adapter.capture(&mut tx, &principal(PRINCIPAL), first).await);
    let result = adapter
        .capture(&mut tx, &principal(PRINCIPAL), second)
        .await;
    let reject = reject(&result);
    assert_eq!(reject.code, SpokeRejectCode::IdempotencyConflict);
    assert_eq!(retry_class(reject), "terminal");
    tx.rollback().await.expect("rollback");
}

/// O1/O2: a family this build does not serve refuses before touching storage.
#[tokio::test]
async fn withheld_families_refuse_before_storage() {
    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["admin"]).await;
    let adapter = MemoryAdapter::new(false);
    let mut tx = harness.begin().await;

    let state = adapter
        .get_reflection_state(
            &mut tx,
            &principal(PRINCIPAL),
            json!({ "space_id": SPACE, "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" } }),
        )
        .await;
    let refusal = reject(&state);
    assert_eq!(refusal.code, SpokeRejectCode::CapabilityPortMissing);
    assert_eq!(retry_class(refusal), "terminal");
    assert_eq!(
        refusal.details.as_ref().expect("details")["op"],
        "memory.reflections.state"
    );

    let export = adapter
        .create_export(&mut tx, &principal(PRINCIPAL), json!({ "space_id": SPACE }))
        .await;
    let refusal = reject(&export);
    assert_eq!(refusal.code, SpokeRejectCode::CapabilityPortMissing);
    assert_eq!(
        refusal.details.as_ref().expect("details")["op"],
        "memory.exports.create"
    );
    assert_eq!(harness.count(&mut tx, "memory_receipts").await, 0);
    tx.rollback().await.expect("rollback");
}

/// §5.2: a journaled mutation stages the effect, the pending receipt and the
/// intent inside the caller's transaction — and the caller's rollback erases
/// all three, because the adapter never commits.
#[tokio::test]
async fn journaled_mutation_stages_and_the_caller_rollback_erases_it() {
    let harness = Harness::open().await;
    harness
        .seed_grant(PRINCIPAL, &["records_write", "records_read"])
        .await;
    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;
    let stage = adapter
        .create_record(
            &mut tx,
            &principal(PRINCIPAL),
            json!({
                "space_id": SPACE,
                "subject_id": SUBJECT,
                "kind": "fact",
                "scope": { "visibility": "shared" },
                "content": { "text": "an authored record body" },
                "idempotency_key": "record-1"
            }),
        )
        .await;
    let (space, receipt_id) = match stage {
        SpokeResult::Ok(MemoryStage::JournalPending { space, receipt_id }) => (space, receipt_id),
        other => panic!("expected a journal pending stage, got {other:?}"),
    };
    assert_eq!(space.as_str(), SPACE);

    // Staged, inside the caller's transaction.
    assert_eq!(harness.count(&mut tx, "memory_records").await, 1);
    let (status, result_json): (String, Option<String>) =
        sqlx::query_as("SELECT status, result_json FROM memory_receipts WHERE receipt_id = ?")
            .bind(&receipt_id)
            .fetch_one(&mut *tx)
            .await
            .expect("staged receipt");
    assert_eq!(status, "pending");
    assert!(result_json.is_none());

    let (state, targets_json): (String, String) =
        sqlx::query_as("SELECT state, targets_json FROM memory_journal WHERE receipt_id = ?")
            .bind(&receipt_id)
            .fetch_one(&mut *tx)
            .await
            .expect("staged intent");
    assert_eq!(state, "pending");
    let targets: Value = serde_json::from_str(&targets_json).expect("targets");
    assert_eq!(targets.as_array().expect("targets array").len(), 1);
    let path = targets[0]["path"].as_str().expect("path");
    assert!(path.starts_with("records/"), "{path}");
    assert!(path.ends_with(".md"), "{path}");
    assert!(targets[0]["digest"].is_string());
    assert!(targets[0]["content"]
        .as_str()
        .expect("content")
        .contains("record_id:"));

    // No record survives the caller's rollback: the adapter committed nothing.
    tx.rollback().await.expect("rollback");
    assert_eq!(harness.count_pool("memory_records").await, 0);
    assert_eq!(harness.count_pool("memory_receipts").await, 0);
    assert_eq!(harness.count_pool("memory_journal").await, 0);
}

/// The public surface compiles with the assignment's exact signature shape.
#[allow(dead_code)]
async fn every_named_operation(
    adapter: &MemoryAdapter,
    tx: &mut Transaction<'_, Sqlite>,
    principal: &Principal,
    request: Value,
) {
    let _: SpokeResult<MemoryStage> = adapter.describe(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.list_spaces(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.freeze_space(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.unfreeze_space(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.ensure_subject(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.get_subject(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.fence_subject(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.grant(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.list_grants(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.revoke_grant(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.capture(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter
        .list_candidates(tx, principal, request.clone())
        .await;
    let _: SpokeResult<MemoryStage> = adapter.get_candidate(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter
        .review_candidate(tx, principal, request.clone())
        .await;
    let _: SpokeResult<MemoryStage> = adapter
        .dismiss_candidate(tx, principal, request.clone())
        .await;
    let _: SpokeResult<MemoryStage> = adapter.create_record(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.get_record(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.list_records(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.update_record(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.promote_record(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.delete_record(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.recall(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.update_profile(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.get_profile(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.get_receipt(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter
        .get_reflection_state(tx, principal, request.clone())
        .await;
    let _: SpokeResult<MemoryStage> = adapter
        .generate_reflection(tx, principal, request.clone())
        .await;
    let _: SpokeResult<MemoryStage> = adapter
        .list_reflections(tx, principal, request.clone())
        .await;
    let _: SpokeResult<MemoryStage> = adapter.create_export(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.get_export(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter.stage_import(tx, principal, request.clone()).await;
    let _: SpokeResult<MemoryStage> = adapter
        .activate_import(tx, principal, request.clone())
        .await;
    let _: SpokeResult<MemoryStage> = adapter.discard_import(tx, principal, request).await;
}
