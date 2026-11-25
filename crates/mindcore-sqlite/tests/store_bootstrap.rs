//! End-to-end proof of the production open path (`I-000068`): opening a fresh database through
//! `MindStore::open` installs the protocol schema, so a host needs only one `create` call and
//! every protocol step can then run through `MemoryAdapter`.

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::{MindId, SpaceId};
use mindcore_base::ir::MemoryRepository;
use mindcore_spoke::{MemoryAdapter, MemoryStage};
use mindcore_sqlite::minds::CreateMind;
use mindcore_sqlite::{MindStore, SqliteMemoryRepository};
use serde_json::{json, Value};
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

    match store
        .create_mind(&CreateMind {
            mind_id: Some(MindId::new(SPACE)),
            display_name: Some(SPACE.to_string()),
            owner: CallerId::new("caller_wren"),
        })
        .await
    {
        SpokeResult::Ok(_) => {}
        SpokeResult::Reject(reject) => panic!("create unexpectedly rejected with {reject:?}"),
    }

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

    let mut tx = pool.begin().await.expect("begin the journal read");
    let intent = SqliteMemoryRepository
        .journal_intent(&mut tx, &SpaceId::new(SPACE), &receipt_id)
        .await
        .expect("journal intent")
        .expect("the staged receipt holds a journal intent");
    tx.rollback().await.expect("rollback the journal read");
    let targets = intent.targets;
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
