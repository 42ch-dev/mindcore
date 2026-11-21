//! End-to-end proof of the production open path (`I-000068`): opening a fresh database through
//! `MindStore::open` installs the protocol schema, so a host needs only its two control-plane
//! provisioning rows and every protocol step can then run through `MemoryAdapter`.

use chrono::Utc;
use mindcore_base::boundary::CallerId;
use mindcore_base::ids::SpaceId;
use mindcore_base::ir::{MemoryGrantRow, MemoryRepository};
use mindcore_spoke::{MemoryAdapter, MemoryStage};
use mindcore_sqlite::{MindStore, SqliteMemoryRepository};
use serde_json::{json, Map, Value};
use spoke_operations::SpokeResult;
use spoke_schemas::Principal;
use sqlx::SqlitePool;

const SPACE: &str = "wren";
const SUBJECT: &str = "wren";
const CONTEXT: &str = "tavern";
const BODY: &str = "The ferry leaves the lower dock at first light.";

fn principal(id: &str) -> Principal {
    serde_json::from_value(json!({ "principal_id": id, "extensions": {} }))
        .expect("principal wire shape")
}

fn ready(stage: SpokeResult<MemoryStage>, context: &str) -> Value {
    match stage {
        SpokeResult::Ok(MemoryStage::Ready(body)) => body,
        SpokeResult::Ok(other) => panic!("{context}: expected a ready stage, got {other:?}"),
        SpokeResult::Reject(reject) => panic!("{context}: unexpectedly rejected with {reject:?}"),
    }
}

/// Host control-plane provisioning: the protocol has no create-space or first-grant operation, so
/// the host writes exactly two rows — one active space and its first grant. Everything after this is
/// a protocol operation through `MemoryAdapter`.
async fn provision_space(pool: &SqlitePool, caller: &str) {
    let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    sqlx::query(
        "INSERT INTO memory_spaces \
         (space_id, display_name, authority_host_id, authority_generation, status, revision, \
          created_at) \
         VALUES (?, ?, 'host_bootstrap', 1, 'active', 1, ?)",
    )
    .bind(SPACE)
    .bind(SPACE)
    .bind(&now)
    .execute(pool)
    .await
    .expect("provision the space");

    let mut tx = pool.begin().await.expect("begin the grant transaction");
    SqliteMemoryRepository
        .insert_grant(
            &mut tx,
            &MemoryGrantRow {
                grant_id: format!("grt_{SPACE}"),
                space_id: SpaceId::new(SPACE),
                subject_id: None,
                principal_id: CallerId::new(caller),
                capabilities: [
                    "capture",
                    "review",
                    "records_read",
                    "records_write",
                    "recall",
                ]
                .map(str::to_string)
                .to_vec(),
                context_ids: Vec::new(),
                shared_access: false,
                expires_at: None,
                revoked_at: None,
                revision: serde_json::Number::from(1),
                created_at: now,
                extensions: Map::new(),
            },
        )
        .await
        .expect("provision the grant");
    tx.commit().await.expect("commit the grant");
}

async fn recall(pool: &SqlitePool, adapter: &MemoryAdapter, caller: &Principal) -> Vec<Value> {
    let mut tx = pool.begin().await.expect("begin the recall");
    let body = ready(
        adapter
            .recall(
                &mut tx,
                caller,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "selected_contexts", "context_ids": [CONTEXT] },
                    "budget": { "max_items": 10 },
                }),
            )
            .await,
        "recall",
    );
    tx.rollback().await.expect("rollback the recall");
    body["records"].as_array().expect("records array").clone()
}

#[tokio::test]
async fn production_open_serves_recall_after_bootstrap() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = MindStore::open(&path).await.expect("open store");
    let pool = store.pool();
    let caller = principal("caller_wren");
    let adapter = MemoryAdapter::new(false);

    provision_space(&pool, "caller_wren").await;

    let mut tx = pool.begin().await.expect("begin the subject");
    ready(
        adapter
            .ensure_subject(
                &mut tx,
                &caller,
                json!({ "space_id": SPACE, "subject_id": SUBJECT }),
            )
            .await,
        "ensure the subject",
    );
    tx.commit().await.expect("commit the subject");

    let mut tx = pool.begin().await.expect("begin the capture");
    let captured = ready(
        adapter
            .capture(
                &mut tx,
                &caller,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "capture_scope": { "visibility": "context", "context_id": CONTEXT },
                    "content": { "text": BODY },
                    "occurred_at": "2026-11-21T08:00:00Z",
                    "idempotency_key": "cap-bootstrap-ferry",
                }),
            )
            .await,
        "capture",
    );
    let candidate_id = captured["result"]["candidate_ids"][0]
        .as_str()
        .expect("candidate id")
        .to_string();
    tx.commit().await.expect("commit the capture");

    // A pending candidate is not knowledge yet: capture alone serves no recall.
    assert!(
        recall(&pool, &adapter, &caller).await.is_empty(),
        "a captured candidate must not be recallable before review"
    );

    let mut tx = pool.begin().await.expect("begin the review");
    let (space, receipt_id) = match adapter
        .review_candidate(
            &mut tx,
            &caller,
            json!({
                "space_id": SPACE,
                "candidate_id": candidate_id,
                "decision": { "action": "promote" },
                "expected_revision": 1,
                "idempotency_key": "rev-bootstrap-ferry",
            }),
        )
        .await
    {
        SpokeResult::Ok(MemoryStage::JournalPending { space, receipt_id }) => (space, receipt_id),
        SpokeResult::Ok(other) => panic!("review must stage a journal intent, got {other:?}"),
        SpokeResult::Reject(reject) => panic!("review unexpectedly rejected with {reject:?}"),
    };
    tx.commit().await.expect("commit the review");

    // The staged intent owes file targets; publish them under this test's own root and confirm
    // the published bytes before settling.
    let targets: String =
        sqlx::query_scalar("SELECT targets_json FROM memory_journal WHERE receipt_id = ?")
            .bind(&receipt_id)
            .fetch_one(&pool)
            .await
            .expect("journal intent");
    let targets: Vec<Value> = serde_json::from_str(&targets).expect("journal targets");
    assert!(!targets.is_empty(), "the staged intent owes file targets");
    for target in &targets {
        let target_path = temp
            .path()
            .join(target["path"].as_str().expect("target path"));
        std::fs::create_dir_all(target_path.parent().expect("target directory"))
            .expect("create the target directory");
        let content = target["content"].as_str().expect("target content");
        std::fs::write(&target_path, content).expect("publish the target");
        assert_eq!(
            std::fs::read_to_string(&target_path).expect("read the published target"),
            content,
            "the published bytes are the intent's"
        );
    }

    let mut tx = pool.begin().await.expect("begin the settlement");
    let receipt = match adapter.settle_journal(&mut tx, &space, &receipt_id).await {
        SpokeResult::Ok(receipt) => receipt,
        SpokeResult::Reject(reject) => panic!("settlement unexpectedly rejected with {reject:?}"),
    };
    tx.commit().await.expect("commit the settlement");
    assert_eq!(receipt["status"], json!("committed"));
    let record_id = receipt["result"]["record_ids"][0]
        .as_str()
        .expect("retained record id")
        .to_string();

    let records = recall(&pool, &adapter, &caller).await;
    assert_eq!(records.len(), 1, "the settled record is recallable");
    assert_eq!(records[0]["record_id"], json!(record_id));
    assert_eq!(records[0]["content"]["text"], json!(BODY));

    // The retained record is in the file, not the process: a fresh open still serves it.
    store.close().await;
    let store = MindStore::open(&path).await.expect("reopen the store");
    let pool = store.pool();
    let adapter = MemoryAdapter::new(false);
    let reopened = recall(&pool, &adapter, &caller).await;
    assert_eq!(reopened.len(), 1, "the retained record survives reopen");
    assert_eq!(reopened[0]["record_id"], json!(record_id));
    assert_eq!(reopened[0]["content"]["text"], json!(BODY));
    store.close().await;
}
