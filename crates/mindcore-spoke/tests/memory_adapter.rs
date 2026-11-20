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
        self.seed_grant_with_expiry(principal, capabilities, None)
            .await;
    }

    async fn seed_grant_with_expiry(
        &self,
        principal: &str,
        capabilities: &[&str],
        expires_at: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
              shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES (?, ?, NULL, ?, ?, '[]', 0, ?, NULL, 1, ?)",
        )
        .bind(format!("grt_{principal}"))
        .bind(SPACE)
        .bind(principal)
        .bind(serde_json::to_string(capabilities).expect("capabilities"))
        .bind(expires_at)
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

/// §7.11/F1: the adapter keeps the caller's original subtrees — member
/// presence, anchor extensions, offset timestamps and id order — instead of
/// the DTO's normalized re-serialization.
#[tokio::test]
async fn capture_preserves_original_member_presence() {
    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["capture"]).await;
    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;
    let occurred_at = "2026-11-20T09:20:01.123456+02:00";
    let request = json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "capture_scope": { "visibility": "shared" },
        "content": { "text": "present-but-null structured stays present", "structured": null },
        "source_context_ids": ["ctx_b", "ctx_a", "ctx_a"],
        "anchors": [{
            "schema_version": 1,
            "source_id": "doc-1",
            "extensions": { "mindcore_round": { "kept": true } }
        }],
        "occurred_at": occurred_at,
        "idempotency_key": "cap-presence"
    });
    let body = ready(
        adapter
            .capture(&mut tx, &principal(PRINCIPAL), request)
            .await,
    );
    let candidate_id = body["result"]["candidate_ids"][0]
        .as_str()
        .expect("candidate id")
        .to_string();

    let (content, provenance, stored_occurred_at): (String, String, Option<String>) =
        sqlx::query_as(
            "SELECT content_json, provenance_json, occurred_at FROM memory_candidates \
         WHERE space_id = ? AND candidate_id = ?",
        )
        .bind(SPACE)
        .bind(&candidate_id)
        .fetch_one(&mut *tx)
        .await
        .expect("stored candidate");

    let content: Value = serde_json::from_str(&content).expect("content json");
    assert_eq!(content["text"], "present-but-null structured stays present");
    assert!(
        content.get("structured").is_some(),
        "a present null structured member must survive: {content}"
    );
    assert_eq!(content["structured"], Value::Null);

    let provenance: Value = serde_json::from_str(&provenance).expect("provenance json");
    assert_eq!(
        provenance["source_context_ids"],
        json!(["ctx_b", "ctx_a", "ctx_a"])
    );
    assert_eq!(
        provenance["anchors"],
        json!([{
            "schema_version": 1,
            "source_id": "doc-1",
            "extensions": { "mindcore_round": { "kept": true } }
        }])
    );
    assert_eq!(stored_occurred_at.as_deref(), Some(occurred_at));
    tx.rollback().await.expect("rollback");
}

/// §7.28/Y04: an expired-but-unrevoked grant is not current participation, so
/// neither an existing nor an absent receipt id is disclosed.
#[tokio::test]
async fn expired_grant_is_not_participation() {
    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["capture"]).await;
    harness
        .seed_grant_with_expiry(STRANGER, &["admin"], Some("2020-01-01T00:00:00.000Z"))
        .await;
    let adapter = MemoryAdapter::new(true);

    let mut tx = harness.begin().await;
    let body = ready(
        adapter
            .capture(
                &mut tx,
                &principal(PRINCIPAL),
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "capture_scope": { "visibility": "shared" },
                    "content": { "text": "an existing receipt to probe" },
                    "idempotency_key": "cap-y04"
                }),
            )
            .await,
    );
    let receipt_id = body["receipt_id"].as_str().expect("receipt id").to_string();

    let existing = adapter
        .get_receipt(
            &mut tx,
            &principal(STRANGER),
            json!({ "space_id": SPACE, "receipt_id": receipt_id }),
        )
        .await;
    let unknown = adapter
        .get_receipt(
            &mut tx,
            &principal(STRANGER),
            json!({ "space_id": SPACE, "receipt_id": "rct_absent" }),
        )
        .await;
    let existing = reject(&existing);
    let unknown = reject(&unknown);
    assert_eq!(existing.code, SpokeRejectCode::NotAuthorized);
    assert_eq!(retry_class(existing), "terminal");
    assert_eq!(existing.code, unknown.code);
    assert_eq!(existing.message, unknown.message);
    tx.rollback().await.expect("rollback");
}

/// §7.27/F3: an empty required profile document is not completeness.
#[tokio::test]
async fn empty_required_profile_is_incomplete() {
    let harness = Harness::open().await;
    harness
        .seed_grant(PRINCIPAL, &["reflect", "records_read", "records_write"])
        .await;
    sqlx::query(
        "INSERT INTO memory_records \
         (record_id, space_id, subject_id, kind, content_json, scope_visibility, \
          scope_context_id, provenance_json, import_ref_json, occurred_at, tombstone_json, \
          revision, created_at, updated_at) \
         VALUES ('rec_1', ?, ?, 'fact', '{\"text\":\"a live basis record\"}', 'shared', NULL, \
                 '{\"source_context_ids\":[],\"anchors\":[],\"captured_by\":\"adapter\",\
                   \"captured_at\":\"2026-11-20T09:20:01.123Z\"}', \
                 NULL, NULL, NULL, 1, ?, ?)",
    )
    .bind(SPACE)
    .bind(SUBJECT)
    .bind(NOW)
    .bind(NOW)
    .execute(&harness.pool)
    .await
    .expect("seed record");
    sqlx::query(
        "INSERT INTO memory_profiles \
         (space_id, subject_id, profile_kind, document_json, updated_by, revision, created_at, \
          updated_at) VALUES (?, ?, 'personality', ?, 'adapter', 1, ?, ?)",
    )
    .bind(SPACE)
    .bind(SUBJECT)
    .bind(r#"{"body":"   ","format":"markdown"}"#)
    .bind(NOW)
    .bind(NOW)
    .execute(&harness.pool)
    .await
    .expect("seed empty profile");

    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;
    let state = ready(
        adapter
            .get_reflection_state(
                &mut tx,
                &principal(PRINCIPAL),
                json!({ "space_id": SPACE, "subject_id": SUBJECT,
                        "selection": { "mode": "subject_all" } }),
            )
            .await,
    );
    assert_eq!(
        state["missing_required_profile_kinds"],
        json!(["personality"])
    );
    let digest = state["state_digest"].as_str().expect("digest").to_string();

    let refused = adapter
        .generate_reflection(
            &mut tx,
            &principal(PRINCIPAL),
            json!({ "space_id": SPACE, "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" },
                    "expected_state_digest": digest,
                    "idempotency_key": "gen-empty" }),
        )
        .await;
    let refusal = reject(&refused);
    assert_eq!(refusal.code, SpokeRejectCode::ProfileIncomplete);
    assert_eq!(
        refusal.details.as_ref().expect("details")["missing"],
        json!(["personality"])
    );
    assert_eq!(harness.count(&mut tx, "memory_receipts").await, 0);

    // With authored content the same basis generates: the gate refused the
    // empty document, not the subject.
    sqlx::query("UPDATE memory_profiles SET document_json = ? WHERE space_id = ? AND subject_id = ? AND profile_kind = 'personality'")
        .bind(r#"{"body":"a real personality","format":"markdown"}"#)
        .bind(SPACE)
        .bind(SUBJECT)
        .execute(&mut *tx)
        .await
        .expect("author the profile");
    let staged = adapter
        .generate_reflection(
            &mut tx,
            &principal(PRINCIPAL),
            json!({ "space_id": SPACE, "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" },
                    "expected_state_digest": digest,
                    "idempotency_key": "gen-authored" }),
        )
        .await;
    match staged {
        SpokeResult::Ok(MemoryStage::ReflectionPending(work)) => {
            assert_eq!(work.subject_id, SUBJECT);
            assert_eq!(work.engine_policy_version, "mindcore-local-memory-v1");
            assert!(work.basis["state_digest"].is_string());
        }
        other => panic!("expected a reflection pending stage, got {other:?}"),
    }
    tx.rollback().await.expect("rollback");
}

/// §7.24/F4: a supplied expectation for an absent profile is a revision
/// conflict before any write, so nothing is staged.
#[tokio::test]
async fn absent_profile_with_expected_revision_refuses() {
    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["profile_write"]).await;
    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;
    let result = adapter
        .update_profile(
            &mut tx,
            &principal(PRINCIPAL),
            json!({
                "space_id": SPACE,
                "subject_id": SUBJECT,
                "profile_kind": "personality",
                "document": { "body": "an authored personality", "format": "markdown" },
                "expected_revision": 5,
                "idempotency_key": "profile-cas"
            }),
        )
        .await;
    let refusal = reject(&result);
    assert_eq!(refusal.code, SpokeRejectCode::RevisionConflict);
    assert_eq!(retry_class(refusal), "terminal");
    assert_eq!(harness.count(&mut tx, "memory_profiles").await, 0);
    assert_eq!(harness.count(&mut tx, "memory_receipts").await, 0);
    assert_eq!(harness.count(&mut tx, "memory_journal").await, 0);
    tx.rollback().await.expect("rollback");
}

/// §7.21/F5: a promotion into a fenced context is refused and stages nothing.
#[tokio::test]
async fn promotion_into_fenced_context_refuses() {
    let harness = Harness::open().await;
    harness
        .seed_grant(PRINCIPAL, &["promote", "records_write"])
        .await;
    sqlx::query("UPDATE memory_subjects SET fenced_contexts_json = '[\"c1\"]' WHERE space_id = ? AND subject_id = ?")
        .bind(SPACE)
        .bind(SUBJECT)
        .execute(&harness.pool)
        .await
        .expect("fence context");
    sqlx::query(
        "INSERT INTO memory_records \
         (record_id, space_id, subject_id, kind, content_json, scope_visibility, \
          scope_context_id, provenance_json, import_ref_json, occurred_at, tombstone_json, \
          revision, created_at, updated_at) \
         VALUES ('rec_1', ?, ?, 'fact', '{\"text\":\"a shared record\"}', 'shared', NULL, \
                 '{\"source_context_ids\":[],\"anchors\":[],\"captured_by\":\"adapter\",\
                   \"captured_at\":\"2026-11-20T09:20:01.123Z\"}', \
                 NULL, NULL, NULL, 1, ?, ?)",
    )
    .bind(SPACE)
    .bind(SUBJECT)
    .bind(NOW)
    .bind(NOW)
    .execute(&harness.pool)
    .await
    .expect("seed record");

    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;
    let result = adapter
        .promote_record(
            &mut tx,
            &principal(PRINCIPAL),
            json!({
                "space_id": SPACE,
                "record_id": "rec_1",
                "expected_revision": 1,
                "target_scope": { "visibility": "context", "context_id": "c1" },
                "idempotency_key": "promote-fenced"
            }),
        )
        .await;
    let refusal = reject(&result);
    assert_eq!(refusal.code, SpokeRejectCode::SubjectFenced);
    assert_eq!(retry_class(refusal), "terminal");
    assert_eq!(harness.count(&mut tx, "memory_receipts").await, 0);
    assert_eq!(harness.count(&mut tx, "memory_journal").await, 0);
    let (visibility, revision): (String, i64) = sqlx::query_as(
        "SELECT scope_visibility, revision FROM memory_records WHERE record_id = 'rec_1'",
    )
    .fetch_one(&mut *tx)
    .await
    .expect("record");
    assert_eq!((visibility.as_str(), revision), ("shared", 1));
    tx.rollback().await.expect("rollback");
}

/// §7.0.5/F6: a committed replay passes the operation's pinned response type;
/// a tampered stored result is an internal error, never a wire success.
#[tokio::test]
async fn replayed_receipt_with_tampered_result_rejects() {
    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["capture"]).await;
    let adapter = MemoryAdapter::new(true);
    let request = json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "capture_scope": { "visibility": "shared" },
        "content": { "text": "a capture whose receipt result will be tampered with" },
        "idempotency_key": "cap-tamper"
    });

    let mut tx = harness.begin().await;
    let _ = ready(
        adapter
            .capture(&mut tx, &principal(PRINCIPAL), request.clone())
            .await,
    );
    tx.commit().await.expect("commit");

    sqlx::query("UPDATE memory_receipts SET result_json = ? WHERE idempotency_key = ?")
        .bind(r#"{"candidate_ids":"not-an-array","classified":1}"#)
        .bind("cap-tamper")
        .execute(&harness.pool)
        .await
        .expect("tamper");

    let mut tx = harness.begin().await;
    let replay = adapter
        .capture(&mut tx, &principal(PRINCIPAL), request)
        .await;
    let refusal = reject(&replay);
    assert_eq!(refusal.code, SpokeRejectCode::InternalError);
    assert_eq!(retry_class(refusal), "terminal");
    assert_eq!(harness.count(&mut tx, "memory_candidates").await, 1);
    assert_eq!(harness.count(&mut tx, "memory_receipts").await, 1);
    tx.rollback().await.expect("rollback");
}

/// §6.5/F2-residual: expiry is an instant. A `+hh:mm` expiry whose text sorts
/// after `now` is still expired, and a `-hh:mm` expiry whose text sorts before
/// `now` is still live.
#[tokio::test]
async fn grant_expiry_is_compared_as_an_instant() {
    use chrono::{Duration, FixedOffset, SecondsFormat, Utc};

    fn offset_stamp(minutes_from_now: i64, offset_hours: i32) -> String {
        let at = Utc::now() + Duration::minutes(minutes_from_now);
        let offset = FixedOffset::east_opt(offset_hours * 3600).expect("fixed offset");
        at.with_timezone(&offset)
            .to_rfc3339_opts(SecondsFormat::Millis, false)
    }

    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["capture"]).await;
    // Text sorts after `now` (local wall clock is ~10h ahead) but the instant
    // is two hours in the past.
    let expired = offset_stamp(-120, 12);
    // Text sorts before `now` (local wall clock is ~10h behind) but the instant
    // is two hours in the future.
    let live = offset_stamp(120, -12);
    harness
        .seed_grant_with_expiry(STRANGER, &["admin"], Some(expired.as_str()))
        .await;
    harness
        .seed_grant_with_expiry("p_offset", &["capture"], Some(live.as_str()))
        .await;

    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;
    let body = ready(
        adapter
            .capture(
                &mut tx,
                &principal(PRINCIPAL),
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "capture_scope": { "visibility": "shared" },
                    "content": { "text": "a receipt under an instant-aware check" },
                    "idempotency_key": "cap-instant"
                }),
            )
            .await,
    );
    let receipt_id = body["receipt_id"].as_str().expect("receipt id").to_string();

    // Expired by instant: no participation, and an admin grant cannot rescue it.
    let expired_probe = adapter
        .get_receipt(
            &mut tx,
            &principal(STRANGER),
            json!({ "space_id": SPACE, "receipt_id": receipt_id }),
        )
        .await;
    let refusal = reject(&expired_probe);
    assert_eq!(refusal.code, SpokeRejectCode::NotAuthorized);
    assert_eq!(retry_class(refusal), "terminal");

    // Live by instant: the offset grant participates and reads its own receipt.
    let own = ready(
        adapter
            .capture(
                &mut tx,
                &principal("p_offset"),
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "capture_scope": { "visibility": "shared" },
                    "content": { "text": "an offset caller's own capture" },
                    "idempotency_key": "cap-offset"
                }),
            )
            .await,
    );
    let own_receipt = own["receipt_id"].as_str().expect("receipt id").to_string();
    let read = adapter
        .get_receipt(
            &mut tx,
            &principal("p_offset"),
            json!({ "space_id": SPACE, "receipt_id": own_receipt }),
        )
        .await;
    assert!(
        read.is_ok(),
        "a live offset grant must participate: {read:?}"
    );
    tx.rollback().await.expect("rollback");
}

/// §6.1/F1-residual: the idempotency digest covers the original request, so two
/// inputs the DTO would normalize to one shape are different payloads.
#[tokio::test]
async fn digest_binds_the_original_request_not_the_normalized_dto() {
    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["capture"]).await;
    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;

    let with_null = json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "capture_scope": { "visibility": "shared" },
        "content": { "text": "the same text either way", "structured": null },
        "idempotency_key": "cap-digest"
    });
    let without_null = json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "capture_scope": { "visibility": "shared" },
        "content": { "text": "the same text either way" },
        "idempotency_key": "cap-digest"
    });

    let first = ready(
        adapter
            .capture(&mut tx, &principal(PRINCIPAL), with_null.clone())
            .await,
    );
    let first_receipt = first["receipt_id"]
        .as_str()
        .expect("receipt id")
        .to_string();

    // Only a normalized member differs, but the original bytes differ: the key
    // names a different payload and must not replay the earlier receipt.
    let collision = adapter
        .capture(&mut tx, &principal(PRINCIPAL), without_null.clone())
        .await;
    let refusal = reject(&collision);
    assert_eq!(refusal.code, SpokeRejectCode::IdempotencyConflict);
    assert_eq!(retry_class(refusal), "terminal");

    // The identical original request still replays its receipt.
    let replay = ready(
        adapter
            .capture(&mut tx, &principal(PRINCIPAL), with_null)
            .await,
    );
    assert_eq!(
        replay["receipt_id"].as_str().expect("receipt id"),
        first_receipt
    );
    assert_eq!(harness.count(&mut tx, "memory_candidates").await, 1);
    assert_eq!(harness.count(&mut tx, "memory_receipts").await, 1);
    tx.rollback().await.expect("rollback");
}

/// §6.5/§7.6/§7.23: a revoked-but-unexpired grant is not live, so subject and
/// profile reads refuse instead of disclosing; a live grant still passes.
#[tokio::test]
async fn revoked_grant_denies_subject_and_profile_reads() {
    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["records_read"]).await;
    harness.seed_grant("p_revoked", &["admin"]).await;
    sqlx::query("UPDATE memory_grants SET revoked_at = ? WHERE principal_id = 'p_revoked'")
        .bind(NOW)
        .execute(&harness.pool)
        .await
        .expect("revoke the grant");

    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;

    let subject = adapter
        .get_subject(
            &mut tx,
            &principal("p_revoked"),
            json!({ "space_id": SPACE, "subject_id": SUBJECT }),
        )
        .await;
    let refusal = reject(&subject);
    assert_eq!(refusal.code, SpokeRejectCode::NotAuthorized);
    assert_eq!(retry_class(refusal), "terminal");

    let profile = adapter
        .get_profile(
            &mut tx,
            &principal("p_revoked"),
            json!({ "space_id": SPACE, "subject_id": SUBJECT, "profile_kinds": ["personality"] }),
        )
        .await;
    let refusal = reject(&profile);
    assert_eq!(refusal.code, SpokeRejectCode::NotAuthorized);
    assert_eq!(retry_class(refusal), "terminal");

    // Unrevoked and unexpired: both reads proceed (the profile read is
    // honestly empty rather than refused).
    let subject = ready(
        adapter
            .get_subject(
                &mut tx,
                &principal(PRINCIPAL),
                json!({ "space_id": SPACE, "subject_id": SUBJECT }),
            )
            .await,
    );
    assert_eq!(subject["subject"]["subject_id"], SUBJECT);
    let profiles = ready(
        adapter
            .get_profile(
                &mut tx,
                &principal(PRINCIPAL),
                json!({ "space_id": SPACE, "subject_id": SUBJECT, "profile_kinds": ["personality"] }),
            )
            .await,
    );
    assert_eq!(profiles["profiles"], json!([]));
    tx.rollback().await.expect("rollback");
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
