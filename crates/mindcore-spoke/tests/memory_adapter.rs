//! The adapter's protocol surface against a caller-held SQLite transaction:
//! wire→IR→wire round-trips, the exact reject boundary, and the staging split
//! between what the adapter commits and what only the caller may commit.

use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Sqlite, SqlitePool, Transaction};
use tempfile::TempDir;

use mindcore_base::ids::SpaceId;
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
        sqlx::query!(
            "INSERT INTO memory_spaces \
             (space_id, display_name, authority_host_id, authority_generation, status, revision, \
              created_at) \
             VALUES (?, 'space', 'host_1', 1, 'active', 1, ?)",
            SPACE,
            NOW,
        )
        .execute(&pool)
        .await
        .expect("seed space");
        sqlx::query!(
            "INSERT INTO memory_subjects \
             (space_id, subject_id, kind_ns, kind_id, status, fenced_contexts_json, revision, \
              created_at) \
             VALUES (?, ?, NULL, NULL, 'active', '[]', 1, ?)",
            SPACE,
            SUBJECT,
            NOW,
        )
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
        sqlx::query!(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
              shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES (?, ?, NULL, ?, ?, '[]', 0, ?, NULL, 1, ?)",
            format!("grt_{principal}"),
            SPACE,
            principal,
            serde_json::to_string(capabilities).expect("capabilities"),
            expires_at,
            NOW,
        )
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

    /// The durable publication targets one journaled intent still owes.
    async fn targets(&self, tx: &mut Transaction<'_, Sqlite>, receipt_id: &str) -> Vec<Value> {
        let raw: String = sqlx::query_scalar!(
            "SELECT targets_json FROM memory_journal WHERE receipt_id = ?",
            receipt_id,
        )
        .fetch_one(&mut **tx)
        .await
        .expect("intent");
        serde_json::from_str(&raw).expect("targets json")
    }

    /// Publication confirmation: every owed target's landed bytes equal the
    /// content the intent declared. The caller settles only when this holds.
    async fn confirm(&self, targets: &[Value]) -> bool {
        let root = self._dir.path();
        targets.iter().all(|target| {
            std::fs::read_to_string(root.join(target["path"].as_str().expect("target path")))
                .map(|landed| landed == target["content"].as_str().expect("content"))
                .unwrap_or(false)
        })
    }

    /// The caller's publication phase: write every owed file target under the
    /// scratch root, then confirm each landed byte-for-byte.
    async fn publish(&self, targets: &[Value]) -> bool {
        let root = self._dir.path();
        for target in targets {
            let path = root.join(target["path"].as_str().expect("target path"));
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("target directory");
            }
            std::fs::write(&path, target["content"].as_str().expect("content"))
                .expect("write target");
        }
        self.confirm(targets).await
    }
}

fn principal(id: &str) -> Principal {
    serde_json::from_value(json!({
        "principal_id": id,
        "extensions": {},
    }))
    .expect("principal")
}

fn reject<T: std::fmt::Debug>(result: &SpokeResult<T>) -> &SpokeReject {
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

    let content: String = sqlx::query_scalar!(
        "SELECT content_json AS \"content_json!\" \
         FROM memory_candidates WHERE space_id = ? AND candidate_id = ?",
        SPACE,
        &candidate_id,
    )
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
    sqlx::query!(
        "INSERT INTO memory_records \
         (record_id, space_id, subject_id, kind, content_json, scope_visibility, \
          scope_context_id, provenance_json, import_ref_json, occurred_at, tombstone_json, \
          revision, created_at, updated_at) \
         VALUES ('rec_1', ?, ?, 'fact', ?, 'shared', NULL, ?, NULL, NULL, NULL, 1, ?, ?)",
        SPACE,
        SUBJECT,
        serde_json::to_string(&stored["content"]).expect("content"),
        serde_json::to_string(&stored["provenance"]).expect("provenance"),
        NOW,
        NOW,
    )
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
    let status: String =
        sqlx::query_scalar!("SELECT status FROM memory_spaces WHERE space_id = ?", SPACE)
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
    let receipt = sqlx::query!(
        "SELECT status, result_json FROM memory_receipts WHERE receipt_id = ?",
        &receipt_id,
    )
    .fetch_one(&mut *tx)
    .await
    .expect("staged receipt");
    let (status, result_json) = (receipt.status, receipt.result_json);
    assert_eq!(status, "pending");
    assert!(result_json.is_none());

    let intent = sqlx::query!(
        "SELECT state, targets_json FROM memory_journal WHERE receipt_id = ?",
        &receipt_id,
    )
    .fetch_one(&mut *tx)
    .await
    .expect("staged intent");
    let (state, targets_json) = (intent.state, intent.targets_json);
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

    let stored = sqlx::query!(
        "SELECT content_json AS \"content_json!\", provenance_json, occurred_at \
         FROM memory_candidates WHERE space_id = ? AND candidate_id = ?",
        SPACE,
        &candidate_id,
    )
    .fetch_one(&mut *tx)
    .await
    .expect("stored candidate");
    let (content, provenance, stored_occurred_at) = (
        stored.content_json,
        stored.provenance_json,
        stored.occurred_at,
    );

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
    sqlx::query!(
        "INSERT INTO memory_records \
         (record_id, space_id, subject_id, kind, content_json, scope_visibility, \
          scope_context_id, provenance_json, import_ref_json, occurred_at, tombstone_json, \
          revision, created_at, updated_at) \
         VALUES ('rec_1', ?, ?, 'fact', '{\"text\":\"a live basis record\"}', 'shared', NULL, \
                 '{\"source_context_ids\":[],\"anchors\":[],\"captured_by\":\"adapter\",\
                   \"captured_at\":\"2026-11-20T09:20:01.123Z\"}', \
                 NULL, NULL, NULL, 1, ?, ?)",
        SPACE,
        SUBJECT,
        NOW,
        NOW,
    )
    .execute(&harness.pool)
    .await
    .expect("seed record");
    sqlx::query!(
        "INSERT INTO memory_profiles \
         (space_id, subject_id, profile_kind, document_json, updated_by, revision, created_at, \
          updated_at) VALUES (?, ?, 'personality', ?, 'adapter', 1, ?, ?)",
        SPACE,
        SUBJECT,
        r#"{"body":"   ","format":"markdown"}"#,
        NOW,
        NOW,
    )
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
    sqlx::query!(
        "UPDATE memory_profiles SET document_json = ? \
         WHERE space_id = ? AND subject_id = ? AND profile_kind = 'personality'",
        r#"{"body":"a real personality","format":"markdown"}"#,
        SPACE,
        SUBJECT,
    )
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
    sqlx::query!(
        "UPDATE memory_subjects SET fenced_contexts_json = '[\"c1\"]' \
         WHERE space_id = ? AND subject_id = ?",
        SPACE,
        SUBJECT,
    )
    .execute(&harness.pool)
    .await
    .expect("fence context");
    sqlx::query!(
        "INSERT INTO memory_records \
         (record_id, space_id, subject_id, kind, content_json, scope_visibility, \
          scope_context_id, provenance_json, import_ref_json, occurred_at, tombstone_json, \
          revision, created_at, updated_at) \
         VALUES ('rec_1', ?, ?, 'fact', '{\"text\":\"a shared record\"}', 'shared', NULL, \
                 '{\"source_context_ids\":[],\"anchors\":[],\"captured_by\":\"adapter\",\
                   \"captured_at\":\"2026-11-20T09:20:01.123Z\"}', \
                 NULL, NULL, NULL, 1, ?, ?)",
        SPACE,
        SUBJECT,
        NOW,
        NOW,
    )
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
    let stored = sqlx::query!(
        "SELECT scope_visibility, revision FROM memory_records WHERE record_id = 'rec_1'",
    )
    .fetch_one(&mut *tx)
    .await
    .expect("record");
    let (visibility, revision) = (stored.scope_visibility, stored.revision);
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

    sqlx::query!(
        "UPDATE memory_receipts SET result_json = ? WHERE idempotency_key = ?",
        r#"{"candidate_ids":"not-an-array","classified":1}"#,
        "cap-tamper",
    )
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
    sqlx::query!(
        "UPDATE memory_grants SET revoked_at = ? WHERE principal_id = 'p_revoked'",
        NOW,
    )
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

/// §7.0.5: the identical request under one idempotency key replays the stored
/// receipt — one candidate, one receipt, no second effect.
#[tokio::test]
async fn identical_capture_key_replays_the_stored_receipt() {
    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["capture"]).await;
    let adapter = MemoryAdapter::new(true);
    let request = json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "capture_scope": { "visibility": "shared" },
        "content": { "text": "one effect for one key" },
        "idempotency_key": "cap-replay"
    });

    let mut tx = harness.begin().await;
    let first = ready(
        adapter
            .capture(&mut tx, &principal(PRINCIPAL), request.clone())
            .await,
    );
    tx.commit().await.expect("commit");

    let mut tx = harness.begin().await;
    let replay = ready(
        adapter
            .capture(&mut tx, &principal(PRINCIPAL), request)
            .await,
    );
    assert_eq!(replay["receipt_id"], first["receipt_id"]);
    assert_eq!(replay["result"], first["result"]);
    assert_eq!(harness.count(&mut tx, "memory_candidates").await, 1);
    assert_eq!(harness.count(&mut tx, "memory_receipts").await, 1);
    tx.rollback().await.expect("rollback");
}

/// §7.0.4: a stale expected revision rejects before the write path and the
/// record keeps its stored revision — no second write under a wrong CAS.
#[tokio::test]
async fn stale_record_revision_rejects_without_a_second_write() {
    let harness = Harness::open().await;
    harness
        .seed_grant(PRINCIPAL, &["records_read", "records_write"])
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
                "content": { "text": "the record a stale update must not touch" },
                "idempotency_key": "rec-cas"
            }),
        )
        .await;
    let receipt_id = match stage {
        SpokeResult::Ok(MemoryStage::JournalPending { receipt_id, .. }) => receipt_id,
        other => panic!("expected a journal pending stage, got {other:?}"),
    };
    tx.commit().await.expect("commit");
    // The caller publishes the intent's file targets before settling.
    let mut tx = harness.begin().await;
    let targets = harness.targets(&mut tx, &receipt_id).await;
    tx.rollback().await.expect("rollback");
    assert!(harness.publish(&targets).await, "publication must confirm");

    let mut tx = harness.begin().await;
    let body = adapter
        .settle_journal(&mut tx, &SpaceId::new(SPACE), &receipt_id)
        .await;
    let settled = match body {
        SpokeResult::Ok(body) => body,
        SpokeResult::Reject(reject) => panic!("settle rejected: {reject:?}"),
    };
    let record_id = settled["result"]["record_id"]
        .as_str()
        .expect("record id")
        .to_string();
    tx.commit().await.expect("commit settle");

    let mut tx = harness.begin().await;
    let result = adapter
        .update_record(
            &mut tx,
            &principal(PRINCIPAL),
            json!({
                "space_id": SPACE,
                "record_id": record_id,
                "expected_revision": 7,
                "idempotency_key": "rec-cas-stale",
                "content": { "text": "a stale overwrite" }
            }),
        )
        .await;
    let refusal = reject(&result);
    assert_eq!(refusal.code, SpokeRejectCode::RevisionConflict);
    assert_eq!(retry_class(refusal), "terminal");
    assert_eq!(harness.count(&mut tx, "memory_receipts").await, 1);
    let revision: i64 = sqlx::query_scalar!(
        "SELECT revision FROM memory_records WHERE record_id = ?",
        &record_id,
    )
    .fetch_one(&mut *tx)
    .await
    .expect("record");
    assert_eq!(revision, 1);
    tx.rollback().await.expect("rollback");
}

/// §7.0.5/Y04: the full sequence — grant, effect, revoke A with B still live.
/// The requester polls the receipt through B while replay of the A effect
/// refuses `NOT_AUTHORIZED`, and a non-participant sees the same refusal for
/// known and unknown ids.
#[tokio::test]
async fn revoked_coverage_blocks_replay_but_not_poll() {
    let harness = Harness::open().await;
    harness
        .seed_grant(PRINCIPAL, &["capture", "records_read"])
        .await;
    harness.seed_grant("p_admin", &["admin"]).await;
    let adapter = MemoryAdapter::new(true);
    let capture = json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "capture_scope": { "visibility": "shared" },
        "content": { "text": "an effect granted under grant A" },
        "idempotency_key": "cap-y04-seq"
    });

    // 1-2: admin issues grant A (capture) to the requester; the requester
    // stages its effect under A.
    let mut tx = harness.begin().await;
    let granted = ready(
        adapter
            .grant(
                &mut tx,
                &principal("p_admin"),
                json!({
                    "space_id": SPACE,
                    "principal_id": "p_grantee",
                    "capabilities": ["capture"],
                    "idempotency_key": "grt-a"
                }),
            )
            .await,
    );
    let grant_a = granted["result"]["grant_id"]
        .as_str()
        .expect("grant id")
        .to_string();
    tx.commit().await.expect("commit grant");

    let mut tx = harness.begin().await;
    let body = ready(
        adapter
            .capture(&mut tx, &principal("p_grantee"), capture.clone())
            .await,
    );
    let receipt_id = body["receipt_id"].as_str().expect("receipt").to_string();
    tx.commit().await.expect("commit capture");

    // 3-4: revoke A and issue B (read-only participation) for the requester.
    let mut tx = harness.begin().await;
    let _ = ready(
        adapter
            .revoke_grant(
                &mut tx,
                &principal("p_admin"),
                json!({
                    "space_id": SPACE,
                    "grant_id": grant_a,
                    "expected_revision": 1,
                    "idempotency_key": "grt-a-revoke"
                }),
            )
            .await,
    );
    let _ = ready(
        adapter
            .grant(
                &mut tx,
                &principal("p_admin"),
                json!({
                    "space_id": SPACE,
                    "principal_id": "p_grantee",
                    "capabilities": ["records_read"],
                    "idempotency_key": "grt-b"
                }),
            )
            .await,
    );
    tx.commit().await.expect("commit revoke+grant");

    // Poll succeeds through B; replay of the A effect refuses; the stranger
    // sees the same refusal for known and unknown ids.
    let mut tx = harness.begin().await;
    let poll = ready(
        adapter
            .get_receipt(
                &mut tx,
                &principal("p_grantee"),
                json!({ "space_id": SPACE, "receipt_id": receipt_id }),
            )
            .await,
    );
    assert_eq!(poll["receipt"]["status"], "committed");

    let replay = adapter
        .capture(&mut tx, &principal("p_grantee"), capture)
        .await;
    let refusal = reject(&replay);
    assert_eq!(refusal.code, SpokeRejectCode::NotAuthorized);
    assert_eq!(retry_class(refusal), "terminal");

    let known = adapter
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
    let known = reject(&known);
    let unknown = reject(&unknown);
    assert_eq!(known.code, SpokeRejectCode::NotAuthorized);
    assert_eq!(known.code, unknown.code);
    assert_eq!(known.message, unknown.message);
    tx.rollback().await.expect("rollback");
}

/// §5.2: the caller commits the staged intent, publishes, then a fresh
/// transaction settles intent and receipt together; a still-pending journal
/// receipt answers `OUTCOME_UNCERTAIN` on replay and `pending` on poll.
#[tokio::test]
async fn settle_journal_applies_intent_and_commits_receipt() {
    let harness = Harness::open().await;
    harness
        .seed_grant(PRINCIPAL, &["records_write", "records_read"])
        .await;
    let adapter = MemoryAdapter::new(true);
    let request = json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "kind": "fact",
        "scope": { "visibility": "shared" },
        "content": { "text": "a journaled record" },
        "idempotency_key": "rec-journal"
    });

    let mut tx1 = harness.begin().await;
    let stage = adapter
        .create_record(&mut tx1, &principal(PRINCIPAL), request.clone())
        .await;
    let receipt_id = match stage {
        SpokeResult::Ok(MemoryStage::JournalPending { receipt_id, .. }) => receipt_id,
        other => panic!("expected a journal pending stage, got {other:?}"),
    };
    // Baseline uncertainty before commit: the same key cannot re-stage.
    let replay = adapter
        .create_record(&mut tx1, &principal(PRINCIPAL), request.clone())
        .await;
    let refusal = reject(&replay);
    assert_eq!(refusal.code, SpokeRejectCode::OutcomeUncertain);
    assert_eq!(retry_class(refusal), "uncertain");
    tx1.commit().await.expect("commit stage");

    // Committed stage, still unsettled: replay stays uncertain and the poll
    // reports the pending status honestly.
    let mut tx = harness.begin().await;
    let replay = adapter
        .create_record(&mut tx, &principal(PRINCIPAL), request.clone())
        .await;
    let refusal = reject(&replay);
    assert_eq!(refusal.code, SpokeRejectCode::OutcomeUncertain);
    let poll = adapter
        .get_receipt(
            &mut tx,
            &principal(PRINCIPAL),
            json!({ "space_id": SPACE, "receipt_id": receipt_id }),
        )
        .await;
    let refusal = reject(&poll);
    assert_eq!(refusal.code, SpokeRejectCode::OutcomeUncertain);
    assert_eq!(retry_class(refusal), "uncertain");
    tx.rollback().await.expect("rollback");

    // The caller's publication phase: every owed file target must land before
    // settlement — until then the intent keeps its targets.
    let mut tx = harness.begin().await;
    let targets = harness.targets(&mut tx, &receipt_id).await;
    assert!(!targets.is_empty(), "the staged intent owes file targets");
    tx.rollback().await.expect("rollback");
    assert!(harness.publish(&targets).await, "publication must confirm");

    // After confirmed publication the caller settles in a fresh transaction.
    let mut tx2 = harness.begin().await;
    let settled = adapter
        .settle_journal(&mut tx2, &SpaceId::new(SPACE), &receipt_id)
        .await;
    let body = match settled {
        SpokeResult::Ok(body) => body,
        SpokeResult::Reject(reject) => panic!("settle rejected: {reject:?}"),
    };
    assert_eq!(body["status"], "committed");
    // Settlement scrubs the owed targets the publication already covered.
    assert!(harness.targets(&mut tx2, &receipt_id).await.is_empty());
    tx2.commit().await.expect("commit settle");

    let applied = sqlx::query!(
        "SELECT state, targets_json FROM memory_journal WHERE receipt_id = ?",
        &receipt_id,
    )
    .fetch_one(&harness.pool)
    .await
    .expect("intent");
    let (state, targets) = (applied.state, applied.targets_json);
    assert_eq!(state, "applied");
    assert_eq!(
        serde_json::from_str::<Value>(&targets).expect("targets"),
        json!([])
    );

    // Settled: the replay returns the stored result instead of a new effect.
    let mut tx = harness.begin().await;
    let replay = ready(
        adapter
            .create_record(&mut tx, &principal(PRINCIPAL), request)
            .await,
    );
    assert_eq!(replay["receipt_id"], receipt_id);
    assert_eq!(harness.count(&mut tx, "memory_records").await, 1);
    assert_eq!(harness.count(&mut tx, "memory_receipts").await, 1);
    tx.rollback().await.expect("rollback");
}

/// §5.2: settlement is atomic — an intent that already moved is uncertainty,
/// and a pending receipt with no journaled intent is refused; a non-journal
/// committed receipt settles as its own stored result.
#[tokio::test]
async fn settle_journal_reports_mismatch_as_uncertain() {
    let harness = Harness::open().await;
    harness
        .seed_grant(PRINCIPAL, &["records_write", "records_read", "capture"])
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
                "content": { "text": "a journaled record" },
                "idempotency_key": "rec-journal-mis"
            }),
        )
        .await;
    let pending_id = match stage {
        SpokeResult::Ok(MemoryStage::JournalPending { receipt_id, .. }) => receipt_id,
        other => panic!("expected a journal pending stage, got {other:?}"),
    };
    let capture = ready(
        adapter
            .capture(
                &mut tx,
                &principal(PRINCIPAL),
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "capture_scope": { "visibility": "shared" },
                    "content": { "text": "a non-journal receipt" },
                    "idempotency_key": "cap-nonjournal"
                }),
            )
            .await,
    );
    let committed_id = capture["receipt_id"].as_str().expect("receipt").to_string();
    sqlx::query!(
        "INSERT INTO memory_receipts \
         (receipt_id, space_id, requester_principal_id, op, idempotency_key, \
          payload_digest, status, created_at) \
         VALUES ('rct_orphan', ?, ?, 'memory.records.create', 'orphan-key', ?, 'pending', ?)",
        SPACE,
        PRINCIPAL,
        "a".repeat(64),
        NOW,
    )
    .execute(&mut *tx)
    .await
    .expect("seed orphan pending receipt");
    tx.commit().await.expect("commit");

    let mut tx = harness.begin().await;
    // The pending receipt carries no intent: refusing, never a receipt/success.
    let orphan = adapter
        .settle_journal(&mut tx, &SpaceId::new(SPACE), "rct_orphan")
        .await;
    let refusal = reject(&orphan);
    assert_eq!(refusal.code, SpokeRejectCode::InvalidInput);
    assert_eq!(retry_class(refusal), "terminal");

    // An already-committed non-journal receipt returns its stored result.
    let settled = adapter
        .settle_journal(&mut tx, &SpaceId::new(SPACE), &committed_id)
        .await;
    assert!(settled.is_ok(), "committed receipt settles: {settled:?}");

    // Force the pending intent ahead of its receipt: the settle pair refuses
    // as uncertainty and the caller's rollback keeps both sides untouched.
    sqlx::query!(
        "UPDATE memory_journal SET state = 'applied' WHERE receipt_id = ?",
        &pending_id,
    )
    .execute(&mut *tx)
    .await
    .expect("move intent");
    let moved = adapter
        .settle_journal(&mut tx, &SpaceId::new(SPACE), &pending_id)
        .await;
    let refusal = reject(&moved);
    assert_eq!(refusal.code, SpokeRejectCode::OutcomeUncertain);
    assert_eq!(retry_class(refusal), "uncertain");
    tx.rollback().await.expect("rollback");

    let pair = sqlx::query!(
        "SELECT j.state, r.status FROM memory_journal j \
         JOIN memory_receipts r ON r.receipt_id = j.receipt_id \
         WHERE j.receipt_id = ?",
        &pending_id,
    )
    .fetch_one(&harness.pool)
    .await
    .expect("pair");
    let (state, status) = (pair.state, pair.status);
    assert_eq!((state.as_str(), status.as_str()), ("pending", "pending"));
}

/// §7.27/F2: the caller composes the work outside the transaction, then a
/// fresh transaction inserts the envelope and commits the receipt for the
/// same owner; a tampered owner rolls both back.
#[tokio::test]
async fn complete_reflection_settles_only_for_the_same_owner() {
    let harness = Harness::open().await;
    harness
        .seed_grant(PRINCIPAL, &["reflect", "records_read", "records_write"])
        .await;
    sqlx::query!(
        "INSERT INTO memory_records \
         (record_id, space_id, subject_id, kind, content_json, scope_visibility, \
          scope_context_id, provenance_json, import_ref_json, occurred_at, tombstone_json, \
          revision, created_at, updated_at) \
         VALUES ('rec_1', ?, ?, 'fact', '{\"text\":\"a live basis record\"}', 'shared', NULL, \
                 '{\"source_context_ids\":[],\"anchors\":[],\"captured_by\":\"adapter\",\
                   \"captured_at\":\"2026-11-20T09:20:01.123Z\"}', \
                 NULL, NULL, NULL, 1, ?, ?)",
        SPACE,
        SUBJECT,
        NOW,
        NOW,
    )
    .execute(&harness.pool)
    .await
    .expect("seed record");
    sqlx::query!(
        "INSERT INTO memory_profiles \
         (space_id, subject_id, profile_kind, document_json, updated_by, revision, created_at, \
          updated_at) VALUES (?, ?, 'personality', ?, 'adapter', 1, ?, ?)",
        SPACE,
        SUBJECT,
        r#"{"body":"a real personality","format":"markdown"}"#,
        NOW,
        NOW,
    )
    .execute(&harness.pool)
    .await
    .expect("seed profile");

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
    let digest = state["state_digest"].as_str().expect("digest").to_string();
    let request = json!({ "space_id": SPACE, "subject_id": SUBJECT,
        "selection": { "mode": "subject_all" },
        "expected_state_digest": digest,
        "idempotency_key": "gen-two-phase" });
    let staged = adapter
        .generate_reflection(&mut tx, &principal(PRINCIPAL), request.clone())
        .await;
    let work = match staged {
        SpokeResult::Ok(MemoryStage::ReflectionPending(work)) => work,
        other => panic!("expected a reflection pending stage, got {other:?}"),
    };
    // The parked window: a leased pending receipt, no envelope, no cost.
    let parked = sqlx::query!(
        "SELECT status, owner_id FROM memory_receipts WHERE receipt_id = ?",
        &work.receipt_id,
    )
    .fetch_one(&mut *tx)
    .await
    .expect("pending receipt");
    assert_eq!(parked.status, "pending");
    assert_eq!(parked.owner_id.as_deref(), Some(work.owner_id.as_str()));
    assert_eq!(harness.count(&mut tx, "memory_reflections").await, 0);
    tx.commit().await.expect("commit lease");

    // Phase 2: compose outside the transaction, then settle as the same owner.
    let effect = work.compose();
    let mut tx2 = harness.begin().await;
    let completed = adapter.complete_reflection(&mut tx2, effect).await;
    let body = match completed {
        SpokeResult::Ok(body) => body,
        SpokeResult::Reject(reject) => panic!("completion rejected: {reject:?}"),
    };
    assert_eq!(body["status"], "committed");
    let reflection_id = body["result"]["reflection_id"]
        .as_str()
        .expect("reflection id")
        .to_string();
    tx2.commit().await.expect("commit completion");

    let envelope = sqlx::query!(
        "SELECT cost_unit_kind AS \"cost_unit_kind!\", cost_units \
         FROM memory_reflections WHERE reflection_id = ?",
        &reflection_id,
    )
    .fetch_one(&harness.pool)
    .await
    .expect("envelope");
    assert_eq!(envelope.cost_unit_kind, "basis_record");
    assert_eq!(envelope.cost_units, Some(1.0));

    // Terminal replay returns the stored committed result.
    let mut tx = harness.begin().await;
    let replay = ready(
        adapter
            .generate_reflection(&mut tx, &principal(PRINCIPAL), request)
            .await,
    );
    assert_eq!(
        replay["result"]["reflection_id"].as_str().expect("id"),
        reflection_id
    );
    assert_eq!(harness.count(&mut tx, "memory_reflections").await, 1);
    tx.rollback().await.expect("rollback");
}

/// §7.27/F2: a completion whose owner no longer matches the lease settles
/// nothing — the envelope and the receipt rollback together — and a replay of
/// the same key still answers uncertainty.
#[tokio::test]
async fn cross_owner_reflection_completion_rolls_both_back() {
    let harness = Harness::open().await;
    harness
        .seed_grant(PRINCIPAL, &["reflect", "records_read", "records_write"])
        .await;
    sqlx::query!(
        "INSERT INTO memory_records \
         (record_id, space_id, subject_id, kind, content_json, scope_visibility, \
          scope_context_id, provenance_json, import_ref_json, occurred_at, tombstone_json, \
          revision, created_at, updated_at) \
         VALUES ('rec_1', ?, ?, 'fact', '{\"text\":\"a live basis record\"}', 'shared', NULL, \
                 '{\"source_context_ids\":[],\"anchors\":[],\"captured_by\":\"adapter\",\
                   \"captured_at\":\"2026-11-20T09:20:01.123Z\"}', \
                 NULL, NULL, NULL, 1, ?, ?)",
        SPACE,
        SUBJECT,
        NOW,
        NOW,
    )
    .execute(&harness.pool)
    .await
    .expect("seed record");
    sqlx::query!(
        "INSERT INTO memory_profiles \
         (space_id, subject_id, profile_kind, document_json, updated_by, revision, created_at, \
          updated_at) VALUES (?, ?, 'personality', ?, 'adapter', 1, ?, ?)",
        SPACE,
        SUBJECT,
        r#"{"body":"a real personality","format":"markdown"}"#,
        NOW,
        NOW,
    )
    .execute(&harness.pool)
    .await
    .expect("seed profile");

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
    let digest = state["state_digest"].as_str().expect("digest").to_string();
    let request = json!({ "space_id": SPACE, "subject_id": SUBJECT,
        "selection": { "mode": "subject_all" },
        "expected_state_digest": digest,
        "idempotency_key": "gen-cross-owner" });
    let staged = adapter
        .generate_reflection(&mut tx, &principal(PRINCIPAL), request.clone())
        .await;
    let work = match staged {
        SpokeResult::Ok(MemoryStage::ReflectionPending(work)) => work,
        other => panic!("expected a reflection pending stage, got {other:?}"),
    };
    tx.commit().await.expect("commit lease");

    // A completion carrying a different lease owner never settles.
    let mut effect = work.compose();
    let receipt_id = effect.receipt_id.clone();
    effect.owner_id = "own_other".to_string();
    let mut tx2 = harness.begin().await;
    let completed = adapter.complete_reflection(&mut tx2, effect).await;
    let refusal = reject(&completed);
    assert_eq!(refusal.code, SpokeRejectCode::OutcomeUncertain);
    assert_eq!(retry_class(refusal), "uncertain");
    tx2.rollback().await.expect("rollback");

    assert_eq!(harness.count_pool("memory_reflections").await, 0);
    let status: String = sqlx::query_scalar!(
        "SELECT status FROM memory_receipts WHERE receipt_id = ?",
        &receipt_id,
    )
    .fetch_one(&harness.pool)
    .await
    .expect("receipt");
    assert_eq!(status, "pending");

    // A same-key replay of the still-pending effect stays uncertain.
    let mut tx = harness.begin().await;
    let replay = adapter
        .generate_reflection(&mut tx, &principal(PRINCIPAL), request)
        .await;
    let refusal = reject(&replay);
    assert_eq!(refusal.code, SpokeRejectCode::OutcomeUncertain);
    tx.rollback().await.expect("rollback");
}

/// §5.2: a storage failure mid-staging surfaces as a retryable internal
/// reject — never a receipt or success — and the caller's rollback erases
/// the partial write: the record was inserted, the intent insert failed,
/// so no record, receipt or intent survives.
#[tokio::test]
async fn staging_storage_failure_rolls_back_the_partial_write() {
    let harness = Harness::open().await;
    harness
        .seed_grant(PRINCIPAL, &["records_write", "records_read"])
        .await;
    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;

    // Failing the journal table inside the caller's transaction makes the
    // intent insert fail AFTER the record insert: a genuine partial write.
    sqlx::query!("DROP TABLE memory_journal")
        .execute(&mut *tx)
        .await
        .expect("drop journal");
    let result = adapter
        .create_record(
            &mut tx,
            &principal(PRINCIPAL),
            json!({
                "space_id": SPACE,
                "subject_id": SUBJECT,
                "kind": "fact",
                "scope": { "visibility": "shared" },
                "content": { "text": "a record whose intent staging fails" },
                "idempotency_key": "rec-fault"
            }),
        )
        .await;
    let refusal = reject(&result);
    assert_eq!(refusal.code, SpokeRejectCode::InternalError);
    assert_eq!(retry_class(refusal), "retryable");
    // The partial write was real: the record sat inside this transaction.
    assert_eq!(harness.count(&mut tx, "memory_records").await, 1);
    tx.rollback().await.expect("rollback erases the stage");

    assert_eq!(harness.count_pool("memory_records").await, 0);
    assert_eq!(harness.count_pool("memory_receipts").await, 0);
    assert_eq!(harness.count_pool("memory_journal").await, 0);
}

/// §5.2/F-001(b): a failed publication attempt leaves the intent pending with
/// its targets — the caller must not settle, the poll answers
/// `OUTCOME_UNCERTAIN`, and the settle retried after republish scrubs them.
#[tokio::test]
async fn partial_publish_recovery_keeps_intent_pending_until_republish() {
    let harness = Harness::open().await;
    harness
        .seed_grant(
            PRINCIPAL,
            &["capture", "review", "records_write", "records_read"],
        )
        .await;
    let adapter = MemoryAdapter::new(true);

    // Stage a capture, then dismiss it: the journaled intent owes the consumed
    // candidate's file target.
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
                    "content": { "text": "a capture whose publish attempt tears" },
                    "idempotency_key": "cap-partial"
                }),
            )
            .await,
    );
    let candidate_id = body["result"]["candidate_ids"][0]
        .as_str()
        .expect("candidate id")
        .to_string();
    tx.commit().await.expect("commit capture");

    let mut tx1 = harness.begin().await;
    let stage = adapter
        .dismiss_candidate(
            &mut tx1,
            &principal(PRINCIPAL),
            json!({
                "space_id": SPACE,
                "candidate_id": candidate_id,
                "expected_revision": 1,
                "idempotency_key": "rev-partial"
            }),
        )
        .await;
    let receipt_id = match stage {
        SpokeResult::Ok(MemoryStage::JournalPending { receipt_id, .. }) => receipt_id,
        other => panic!("expected a journal pending stage, got {other:?}"),
    };
    tx1.commit().await.expect("commit stage");

    // A torn write: the target file exists but holds truncated bytes, so the
    // publication attempt fails confirmation and the caller must not settle.
    let mut tx = harness.begin().await;
    let targets = harness.targets(&mut tx, &receipt_id).await;
    tx.rollback().await.expect("rollback");
    assert!(!targets.is_empty(), "the staged intent owes file targets");
    assert!(harness.publish(&targets).await, "publication writes land");
    let torn = harness
        ._dir
        .path()
        .join(targets[0]["path"].as_str().expect("target path"));
    std::fs::write(&torn, "torn").expect("simulate torn write");
    assert!(
        !harness.confirm(&targets).await,
        "torn bytes fail confirmation"
    );

    // Unsettled: the poll answers OUTCOME_UNCERTAIN and the intent keeps its
    // owed targets until settlement scrubs them.
    let mut tx = harness.begin().await;
    assert_eq!(
        harness.targets(&mut tx, &receipt_id).await.len(),
        targets.len()
    );
    let poll = adapter
        .get_receipt(
            &mut tx,
            &principal(PRINCIPAL),
            json!({ "space_id": SPACE, "receipt_id": receipt_id }),
        )
        .await;
    let refusal = reject(&poll);
    assert_eq!(refusal.code, SpokeRejectCode::OutcomeUncertain);
    assert_eq!(retry_class(refusal), "uncertain");
    tx.rollback().await.expect("rollback");

    // Recovery: republishing lands every owed target, then the settle in a
    // fresh transaction commits the receipt and scrubs the intent's targets.
    assert!(harness.publish(&targets).await, "republish must confirm");
    let mut tx2 = harness.begin().await;
    let settled = adapter
        .settle_journal(&mut tx2, &SpaceId::new(SPACE), &receipt_id)
        .await;
    let body = match settled {
        SpokeResult::Ok(body) => body,
        SpokeResult::Reject(reject) => panic!("settle rejected: {reject:?}"),
    };
    assert_eq!(body["status"], "committed");
    assert!(harness.targets(&mut tx2, &receipt_id).await.is_empty());
    tx2.commit().await.expect("commit settle");
}

/// §5.2/F-002: a storage failure inside the settlement transaction — after
/// publication already committed — is an unsettled effect, reported as
/// `OUTCOME_UNCERTAIN`/`uncertain`, never the retryable pre-commit class.
#[tokio::test]
async fn settle_journal_storage_failure_reports_uncertain() {
    let harness = Harness::open().await;
    harness
        .seed_grant(PRINCIPAL, &["records_write", "records_read"])
        .await;
    let adapter = MemoryAdapter::new(true);

    let mut tx1 = harness.begin().await;
    let stage = adapter
        .create_record(
            &mut tx1,
            &principal(PRINCIPAL),
            json!({
                "space_id": SPACE,
                "subject_id": SUBJECT,
                "kind": "fact",
                "scope": { "visibility": "shared" },
                "content": { "text": "a journaled record" },
                "idempotency_key": "rec-fault-settle"
            }),
        )
        .await;
    let receipt_id = match stage {
        SpokeResult::Ok(MemoryStage::JournalPending { receipt_id, .. }) => receipt_id,
        other => panic!("expected a journal pending stage, got {other:?}"),
    };
    tx1.commit().await.expect("commit stage");

    let mut tx = harness.begin().await;
    let targets = harness.targets(&mut tx, &receipt_id).await;
    tx.rollback().await.expect("rollback");
    assert!(harness.publish(&targets).await, "publication must confirm");

    // The journal table is gone when transaction 2 reads the intent: the
    // published effect is durable, the settlement outcome unknown — uncertain,
    // and the caller's rollback restores the table with the intent pending.
    let mut tx2 = harness.begin().await;
    sqlx::query!("ALTER TABLE memory_journal RENAME TO memory_journal_broken")
        .execute(&mut *tx2)
        .await
        .expect("force storage failure");
    let settled = adapter
        .settle_journal(&mut tx2, &SpaceId::new(SPACE), &receipt_id)
        .await;
    let refusal = reject(&settled);
    assert_eq!(refusal.code, SpokeRejectCode::OutcomeUncertain);
    assert_eq!(retry_class(refusal), "uncertain");
    tx2.rollback()
        .await
        .expect("rollback erases nothing further");

    let state: String = sqlx::query_scalar!(
        "SELECT state FROM memory_journal WHERE receipt_id = ?",
        &receipt_id,
    )
    .fetch_one(&harness.pool)
    .await
    .expect("intent");
    assert_eq!(state, "pending");
}

/// §7.27/F-002: the same mapping holds for reflection completion — a storage
/// failure after out-of-transaction composition is an unsettled effect, not a
/// retryable pre-commit reject.
#[tokio::test]
async fn complete_reflection_storage_failure_reports_uncertain() {
    let harness = Harness::open().await;
    harness
        .seed_grant(PRINCIPAL, &["reflect", "records_read", "records_write"])
        .await;
    sqlx::query!(
        "INSERT INTO memory_records \
         (record_id, space_id, subject_id, kind, content_json, scope_visibility, \
          scope_context_id, provenance_json, import_ref_json, occurred_at, tombstone_json, \
          revision, created_at, updated_at) \
         VALUES ('rec_1', ?, ?, 'fact', '{\"text\":\"a live basis record\"}', 'shared', NULL, \
                 '{\"source_context_ids\":[],\"anchors\":[],\"captured_by\":\"adapter\",\
                   \"captured_at\":\"2026-11-20T09:20:01.123Z\"}', \
                 NULL, NULL, NULL, 1, ?, ?)",
        SPACE,
        SUBJECT,
        NOW,
        NOW,
    )
    .execute(&harness.pool)
    .await
    .expect("seed record");
    sqlx::query!(
        "INSERT INTO memory_profiles \
         (space_id, subject_id, profile_kind, document_json, updated_by, revision, created_at, \
          updated_at) VALUES (?, ?, 'personality', ?, 'adapter', 1, ?, ?)",
        SPACE,
        SUBJECT,
        r#"{"body":"a real personality","format":"markdown"}"#,
        NOW,
        NOW,
    )
    .execute(&harness.pool)
    .await
    .expect("seed profile");

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
    let digest = state["state_digest"].as_str().expect("digest").to_string();
    let staged = adapter
        .generate_reflection(
            &mut tx,
            &principal(PRINCIPAL),
            json!({ "space_id": SPACE, "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" },
                    "expected_state_digest": digest,
                    "idempotency_key": "gen-fault" }),
        )
        .await;
    let work = match staged {
        SpokeResult::Ok(MemoryStage::ReflectionPending(work)) => work,
        other => panic!("expected a reflection pending stage, got {other:?}"),
    };
    tx.commit().await.expect("commit lease");

    // Composition happened; the envelope insert then hits a missing table in
    // transaction 2 — an unsettled effect the caller must resolve.
    let effect = work.compose();
    let receipt_id = effect.receipt_id.clone();
    let mut tx2 = harness.begin().await;
    sqlx::query!("ALTER TABLE memory_reflections RENAME TO memory_reflections_broken")
        .execute(&mut *tx2)
        .await
        .expect("force storage failure");
    let completed = adapter.complete_reflection(&mut tx2, effect).await;
    let refusal = reject(&completed);
    assert_eq!(refusal.code, SpokeRejectCode::OutcomeUncertain);
    assert_eq!(retry_class(refusal), "uncertain");
    tx2.rollback().await.expect("rollback restores the table");

    let status: String = sqlx::query_scalar!(
        "SELECT status FROM memory_receipts WHERE receipt_id = ?",
        &receipt_id,
    )
    .fetch_one(&harness.pool)
    .await
    .expect("receipt");
    assert_eq!(status, "pending");
}

/// I-000045: canonical review decisions — the internally-tagged
/// `{"action": "promote"|"fragment"|"drop", …}` spellings — pass the DTO and
/// the internal parser end to end and journal their outcome, while every
/// non-canonical spelling rejects as INVALID_INPUT.
#[tokio::test]
async fn canonical_review_decisions_journal_and_legacy_shapes_reject() {
    let harness = Harness::open().await;
    harness
        .seed_grant(PRINCIPAL, &["capture", "review", "records_write"])
        .await;
    let adapter = MemoryAdapter::new(true);

    async fn capture(harness: &Harness, key: &str) -> String {
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
                        "content": { "text": "a canonical review candidate to consume" },
                        "idempotency_key": key
                    }),
                )
                .await,
        );
        tx.commit().await.expect("commit capture");
        body["result"]["candidate_ids"][0]
            .as_str()
            .expect("candidate id")
            .to_string()
    }

    async fn review(
        harness: &Harness,
        adapter: &MemoryAdapter,
        candidate_id: &str,
        decision: Value,
        key: &str,
    ) -> SpokeResult<MemoryStage> {
        let mut tx = harness.begin().await;
        let result = adapter
            .review_candidate(
                &mut tx,
                &principal(PRINCIPAL),
                json!({
                    "space_id": SPACE,
                    "candidate_id": candidate_id,
                    "decision": decision,
                    "expected_revision": 1,
                    "idempotency_key": key
                }),
            )
            .await;
        tx.commit().await.expect("commit");
        result
    }

    async fn outcome(harness: &Harness, receipt_id: &str) -> Value {
        let result: Option<String> =
            sqlx::query_scalar("SELECT result_json FROM memory_journal WHERE receipt_id = ?")
                .bind(receipt_id)
                .fetch_one(&harness.pool)
                .await
                .expect("journal intent");
        serde_json::from_str(&result.expect("result json")).expect("result json")
    }

    fn journaled(stage: SpokeResult<MemoryStage>) -> String {
        match stage {
            SpokeResult::Ok(MemoryStage::JournalPending { receipt_id, .. }) => receipt_id,
            other => panic!("expected a journal-pending stage, got {other:?}"),
        }
    }

    // Canonical promote stages one retained record.
    let candidate_id = capture(&harness, "cap-canonical-promote").await;
    let stage = review(
        &harness,
        &adapter,
        &candidate_id,
        json!({"action": "promote"}),
        "rev-canonical-promote",
    )
    .await;
    let receipt_id = journaled(stage);
    let result = outcome(&harness, &receipt_id).await;
    assert_eq!(result["outcome"], "promoted");
    assert_eq!(result["record_ids"].as_array().expect("records").len(), 1);
    // An explicit `content_override: null` decodes as absent and the staged
    // record keeps the candidate's own content.
    let candidate_id = capture(&harness, "cap-null-override").await;
    let stage = review(
        &harness,
        &adapter,
        &candidate_id,
        json!({"action": "promote", "content_override": null}),
        "rev-null-override",
    )
    .await;
    let receipt_id = journaled(stage);
    let result = outcome(&harness, &receipt_id).await;
    assert_eq!(result["outcome"], "promoted");
    let record_id = result["record_ids"][0].as_str().expect("record id");
    let content_json: String =
        sqlx::query_scalar("SELECT content_json FROM memory_records WHERE record_id = ?")
            .bind(record_id)
            .fetch_one(&harness.pool)
            .await
            .expect("stored record");
    assert_eq!(
        serde_json::from_str::<Value>(&content_json).expect("record content")["text"],
        "a canonical review candidate to consume"
    );

    // Canonical fragment stages the two named fragments.
    let candidate_id = capture(&harness, "cap-canonical-fragment").await;
    let stage = review(
        &harness,
        &adapter,
        &candidate_id,
        json!({
            "action": "fragment",
            "fragments": [
                {"kind": "note", "content": {"text": "first fragment"}},
                {"kind": "note", "content": {"text": "second fragment"}}
            ]
        }),
        "rev-canonical-fragment",
    )
    .await;
    let receipt_id = journaled(stage);
    let result = outcome(&harness, &receipt_id).await;
    assert_eq!(result["outcome"], "fragmented");
    assert_eq!(result["record_ids"].as_array().expect("records").len(), 2);

    // Canonical drop consumes without records; the rationale member is legal.
    let candidate_id = capture(&harness, "cap-canonical-drop").await;
    let stage = review(
        &harness,
        &adapter,
        &candidate_id,
        json!({"action": "drop", "rationale": "not worth keeping"}),
        "rev-canonical-drop",
    )
    .await;
    let receipt_id = journaled(stage);
    let result = outcome(&harness, &receipt_id).await;
    assert_eq!(result["outcome"], "dropped");
    assert_eq!(result["record_ids"], json!([]));

    // Non-canonical spellings reject as INVALID_INPUT before any effect.
    let candidate_id = capture(&harness, "cap-rejected-shapes").await;
    for (index, decision) in [
        json!("drop"),                                        // bare string variant
        json!({"promote": {}}),                               // legacy member spelling
        json!({"action": "promote", "bogus": 1}),             // extra member
        json!({"action": "promote", "content_override": 42}), // non-object override
        json!({"action": "rewrite"}),                         // unknown action
        json!({"action": "fragment", "fragments": []}),       // validator: minItems 1
    ]
    .into_iter()
    .enumerate()
    {
        let result = review(
            &harness,
            &adapter,
            &candidate_id,
            decision,
            &format!("rev-rejected-{index}"),
        )
        .await;
        let refusal = reject(&result);
        assert_eq!(refusal.code, SpokeRejectCode::InvalidInput);
        assert_eq!(retry_class(refusal), "terminal");
    }
    // The rejected candidate was never consumed.
    assert_eq!(harness.count_pool("memory_candidates").await, 5);
    let consumed: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM memory_candidates WHERE status = 'consumed'")
            .fetch_one(&harness.pool)
            .await
            .expect("count consumed");
    assert_eq!(consumed, 4);
}

/// I-000046: canonical fence targets — `{"type": "subject"}` and
/// `{"type": "context", "context_id": "…"}` — fence and unfence exactly as
/// §7.7 prescribes, while every non-canonical target spelling rejects as
/// INVALID_INPUT.
#[tokio::test]
async fn canonical_fence_targets_transition_and_legacy_shapes_reject() {
    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["admin"]).await;
    let adapter = MemoryAdapter::new(true);

    async fn fence(
        harness: &Harness,
        adapter: &MemoryAdapter,
        action: &str,
        target: Value,
        revision: u64,
        key: &str,
    ) -> SpokeResult<MemoryStage> {
        let mut tx = harness.begin().await;
        let result = adapter
            .fence_subject(
                &mut tx,
                &principal(PRINCIPAL),
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "action": action,
                    "target": target,
                    "expected_revision": revision,
                    "idempotency_key": key
                }),
            )
            .await;
        tx.commit().await.expect("commit");
        result
    }

    // A subject target fences the whole subject.
    let body = ready(
        fence(
            &harness,
            &adapter,
            "fence",
            json!({"type": "subject"}),
            1,
            "fnc-subject",
        )
        .await,
    );
    assert_eq!(body["result"]["status"], "fenced");
    assert_eq!(body["result"]["revision"], 2);

    // A context target keeps the status and records the context.
    let body = ready(
        fence(
            &harness,
            &adapter,
            "fence",
            json!({"type": "context", "context_id": "ctx_1"}),
            2,
            "fnc-ctx-1",
        )
        .await,
    );
    assert_eq!(body["result"]["fenced_contexts"], json!(["ctx_1"]));
    assert_eq!(body["result"]["revision"], 3);

    let body = ready(
        fence(
            &harness,
            &adapter,
            "unfence",
            json!({"type": "context", "context_id": "ctx_1"}),
            3,
            "fnc-ctx-1-out",
        )
        .await,
    );
    assert_eq!(body["result"]["fenced_contexts"], json!([]));
    assert_eq!(body["result"]["revision"], 4);

    // Non-canonical targets reject as INVALID_INPUT before any effect.
    for (index, target) in [
        json!("subject"),                              // bare string variant
        json!({"context": "ctx_9"}),                   // legacy member spelling
        json!({"type": "subject", "context_id": "x"}), // context_id on subject
        json!({"type": "context"}),                    // missing context_id
        json!({"type": "unknown"}),                    // unknown target type
    ]
    .into_iter()
    .enumerate()
    {
        let result = fence(
            &harness,
            &adapter,
            "fence",
            target,
            4,
            &format!("fnc-rejected-{index}"),
        )
        .await;
        let refusal = reject(&result);
        assert_eq!(refusal.code, SpokeRejectCode::InvalidInput);
        assert_eq!(retry_class(refusal), "terminal");
    }

    // The pinned validator maps context-archive to INVALID_INPUT too.
    let result = fence(
        &harness,
        &adapter,
        "archive",
        json!({"type": "context", "context_id": "ctx_2"}),
        4,
        "fnc-ctx-archive",
    )
    .await;
    let refusal = reject(&result);
    assert_eq!(refusal.code, SpokeRejectCode::InvalidInput);

    // Rejected targets left the subject at revision 4, status still fenced.
    let (status, fenced, revision): (String, String, i64) = sqlx::query_as(
        "SELECT status, fenced_contexts_json, revision FROM memory_subjects \
         WHERE space_id = 'sp_1' AND subject_id = 's_1'",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("subject row");
    assert_eq!(status, "fenced");
    assert_eq!(
        serde_json::from_str::<Value>(&fenced).expect("json"),
        json!([])
    );
    assert_eq!(revision, 4);
}

/// Seed one live shared record with an explicit `created_at`, which is what
/// recall's ranking orders by.
async fn seed_record(
    tx: &mut Transaction<'_, Sqlite>,
    record_id: &str,
    text: &str,
    created_at: &str,
) {
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
            "source_context_ids": [],
            "anchors": [],
            "captured_by": "adapter",
            "captured_at": NOW
        })
        .to_string(),
    )
    .bind(created_at)
    .bind(created_at)
    .execute(&mut **tx)
    .await
    .expect("seed record");
}

/// The front matter a projection carries, as parsed `key: value` pairs. Values
/// stay in their serialized form so the test can decode the quoted ones.
fn front_matter(content: &str) -> Vec<(String, String)> {
    let mut blocks = content.split("---\n");
    blocks.next().expect("opening delimiter");
    blocks
        .next()
        .expect("front matter block")
        .lines()
        .map(|line| {
            let (key, value) = line.split_once(": ").expect("key: value line");
            (key.to_string(), value.to_string())
        })
        .collect()
}

/// §9.3/BUG-01: a byte-budget overflow must not mint a cursor that repeats the
/// same offset. The oversized record is passed once (and named once), every
/// servable record is served exactly once, and paging terminates.
#[tokio::test]
async fn recall_byte_budget_pages_advance_and_terminate() {
    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["recall"]).await;
    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;
    seed_record(&mut tx, "rec_a", "aaaaaaaa", "2026-11-20T09:00:03.000Z").await;
    seed_record(
        &mut tx,
        "rec_big",
        &"x".repeat(32_768),
        "2026-11-20T09:00:02.000Z",
    )
    .await;
    seed_record(&mut tx, "rec_b", "bbbbbbbb", "2026-11-20T09:00:01.000Z").await;

    let mut request = json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "selection": { "mode": "subject_all" },
        "budget": { "max_items": 2, "max_bytes": 64 },
    });
    let (mut served, mut cursors, mut overflow) = (Vec::new(), Vec::new(), Vec::new());
    let mut pages = 0u32;
    loop {
        pages += 1;
        assert!(pages <= 6, "byte-budget paging did not terminate");
        let body = ready(
            adapter
                .recall(&mut tx, &principal(PRINCIPAL), request.clone())
                .await,
        );
        for record in body["records"].as_array().expect("records") {
            served.push(record["record_id"].as_str().expect("id").to_string());
        }
        if let Some(id) = body["overflow_record_id"].as_str() {
            overflow.push(id.to_string());
        }
        let Some(cursor) = body["next_cursor"].as_str() else {
            assert_eq!(body["truncated"], false);
            break;
        };
        assert_eq!(body["truncated"], true);
        assert!(
            !cursors.iter().any(|seen| seen == cursor),
            "cursor repeats {cursor}"
        );
        cursors.push(cursor.to_string());
        request["cursor"] = json!(cursor);
    }
    // Every servable record served exactly once; the oversized one never is.
    assert_eq!(served, vec!["rec_a", "rec_b"]);
    assert!(overflow.iter().all(|id| id == "rec_big"), "{overflow:?}");
    assert!(
        !overflow.is_empty(),
        "the skipped record is still named once"
    );

    // The skipped record was the last one: paging ends without a cursor and
    // still reports the terminal overflow.
    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["recall"]).await;
    let mut tx = harness.begin().await;
    seed_record(&mut tx, "rec_ok", "aaaaaaaa", "2026-11-20T09:00:02.000Z").await;
    seed_record(
        &mut tx,
        "rec_tail",
        &"x".repeat(32_768),
        "2026-11-20T09:00:01.000Z",
    )
    .await;
    let body = ready(
        adapter
            .recall(
                &mut tx,
                &principal(PRINCIPAL),
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" },
                    "budget": { "max_items": 1, "max_bytes": 64 },
                }),
            )
            .await,
    );
    let cursor = body["next_cursor"].as_str().expect("first page advances");
    let body = ready(
        adapter
            .recall(
                &mut tx,
                &principal(PRINCIPAL),
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" },
                    "budget": { "max_items": 1, "max_bytes": 64 },
                    "cursor": cursor,
                }),
            )
            .await,
    );
    assert_eq!(body["records"], json!([]));
    assert_eq!(body["truncated"], false);
    assert_eq!(body["next_cursor"], Value::Null);
    assert_eq!(body["overflow_record_id"], "rec_tail");
    tx.rollback().await.expect("rollback");
}

/// §6.6/BUG-02: a caller-derived scalar carrying a newline cannot inject a
/// second front-matter key into the published projection.
#[tokio::test]
async fn record_projection_escapes_injected_front_matter() {
    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["records_write"]).await;
    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;
    let stage = adapter
        .create_record(
            &mut tx,
            &principal(PRINCIPAL),
            json!({
                "space_id": SPACE,
                "subject_id": SUBJECT,
                "kind": "fact\nrevision: 999",
                "scope": { "visibility": "context", "context_id": "ctx\nvisibility: shared" },
                "content": { "text": "an authored record body" },
                "idempotency_key": "inject-record"
            }),
        )
        .await;
    let receipt_id = match stage {
        SpokeResult::Ok(MemoryStage::JournalPending { receipt_id, .. }) => receipt_id,
        other => panic!("expected a journal pending stage, got {other:?}"),
    };
    let targets = harness.targets(&mut tx, &receipt_id).await;
    let content = targets[0]["content"].as_str().expect("content");
    let fields = front_matter(content);
    let mut keys: Vec<&str> = fields.iter().map(|(key, _)| key.as_str()).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "context_id",
            "created_at",
            "kind",
            "record_id",
            "revision",
            "space_id",
            "subject_id",
            "updated_at",
            "visibility",
        ]
    );
    let field = |key: &str| {
        fields
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.clone())
            .expect("field")
    };
    assert_eq!(field("revision"), "1");
    assert_eq!(
        serde_json::from_str::<String>(&field("kind")).expect("quoted kind"),
        "fact\nrevision: 999"
    );
    assert_eq!(
        serde_json::from_str::<String>(&field("context_id")).expect("quoted context"),
        "ctx\nvisibility: shared"
    );
    tx.rollback().await.expect("rollback");
}

/// §6.8/BUG-02: the profile projection escapes an open-vocabulary
/// `profile_kind` the same way.
#[tokio::test]
async fn profile_projection_escapes_injected_front_matter() {
    let harness = Harness::open().await;
    harness.seed_grant(PRINCIPAL, &["profile_write"]).await;
    let adapter = MemoryAdapter::new(true);
    let mut tx = harness.begin().await;
    let stage = adapter
        .update_profile(
            &mut tx,
            &principal(PRINCIPAL),
            json!({
                "space_id": SPACE,
                "subject_id": SUBJECT,
                "profile_kind": "personality\nrevision: 999",
                "document": { "body": "an authored personality", "format": "markdown" },
                "expected_revision": null,
                "idempotency_key": "inject-profile"
            }),
        )
        .await;
    let receipt_id = match stage {
        SpokeResult::Ok(MemoryStage::JournalPending { receipt_id, .. }) => receipt_id,
        other => panic!("expected a journal pending stage, got {other:?}"),
    };
    let targets = harness.targets(&mut tx, &receipt_id).await;
    let content = targets[0]["content"].as_str().expect("content");
    let fields = front_matter(content);
    let mut keys: Vec<&str> = fields.iter().map(|(key, _)| key.as_str()).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "created_at",
            "profile_kind",
            "revision",
            "space_id",
            "subject_id",
            "updated_at",
            "updated_by",
        ]
    );
    let revision = fields
        .iter()
        .filter(|(key, _)| key == "revision")
        .map(|(_, value)| value.as_str())
        .collect::<Vec<_>>();
    assert_eq!(revision, vec!["1"]);
    let profile_kind = fields
        .iter()
        .find(|(key, _)| key == "profile_kind")
        .map(|(_, value)| value.clone())
        .expect("profile kind");
    assert_eq!(
        serde_json::from_str::<String>(&profile_kind).expect("quoted profile kind"),
        "personality\nrevision: 999"
    );
    tx.rollback().await.expect("rollback");
}
