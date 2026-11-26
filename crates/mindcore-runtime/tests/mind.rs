//! The Mind handle `mindcore_runtime::Mind`: `open` through the registry under the caller's
//! live grants, the registry's own `NotFound` for a Mind the caller cannot see, and a frozen
//! Mind opening because the handle acts on no status from the row.

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::{MindId, SpaceId};
use mindcore_base::ir::MemoryRepository;
use mindcore_runtime::minds::{self, CreateMind};
use mindcore_runtime::Mind;
use mindcore_sqlite::{MindStore, SqliteMemoryRepository};
use serde_json::Number;
use spoke_operations::{SpokeRejectCode, SpokeResult};
use tempfile::TempDir;

const OWNER: &str = "caller_wren";

fn request(owner: &str) -> CreateMind {
    CreateMind {
        mind_id: None,
        display_name: Some("Wren".to_string()),
        owner: CallerId::new(owner),
    }
}

fn ok<T>(result: SpokeResult<T>) -> T {
    match result {
        SpokeResult::Ok(value) => value,
        SpokeResult::Reject(reject) => panic!("expected success, got {reject:?}"),
    }
}

fn reject_code<T>(result: SpokeResult<T>) -> SpokeRejectCode {
    match result {
        SpokeResult::Reject(reject) => reject.code,
        SpokeResult::Ok(_) => panic!("expected a refusal"),
    }
}

async fn fixture() -> (TempDir, MindStore) {
    let dir = TempDir::new().expect("temporary directory");
    let store = MindStore::open(&dir.path().join("minds.db"))
        .await
        .expect("open store");
    (dir, store)
}

async fn freeze_space(store: &MindStore, space_id: &str) {
    let repo = SqliteMemoryRepository;
    let mut tx = store.pool().begin().await.expect("begin");
    let mut space = repo
        .space(&mut tx, &SpaceId::new(space_id))
        .await
        .expect("read space")
        .expect("the space row exists");
    let observed = space.revision.clone();
    space.status = "frozen".to_string();
    space.revision = Number::from(space.revision.as_i64().unwrap_or(0) + 1);
    assert!(
        repo.update_space(&mut tx, &space, &observed)
            .await
            .expect("freeze the space"),
        "the freeze applies"
    );
    tx.commit().await.expect("commit");
}

#[tokio::test]
async fn open_returns_the_handle_of_the_mind_the_caller_may_see() {
    let (_dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);

    let handle = ok(Mind::open(&store, &owner, &created.id).await);
    assert_eq!(handle.row(), &created, "the handle keeps the row open read");
    assert_eq!(
        handle.carrier_entry_id(),
        created.carrier_entry_id,
        "the carrier the row names"
    );
    assert!(
        !handle.carrier_entry_id().is_empty(),
        "a carrier coordinate, never `ke_carrier` re-derived here"
    );
}

#[tokio::test]
async fn a_foreign_mind_refuses_the_registry_not_found() {
    let (_dir, store) = fixture().await;
    let created = ok(minds::create(&store, &request(OWNER)).await);

    let outsider = CallerId::new("caller_else");
    assert_eq!(
        reject_code(Mind::open(&store, &outsider, &created.id).await),
        SpokeRejectCode::NotFound,
        "a Mind the caller cannot see answers the registry's own refusal"
    );

    let owner = CallerId::new(OWNER);
    let never = MindId::new("spc_never_created");
    assert_eq!(
        reject_code(Mind::open(&store, &owner, &never).await),
        SpokeRejectCode::NotFound
    );
}

#[tokio::test]
async fn a_frozen_mind_opens_with_no_status_acted_on() {
    let (_dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);

    freeze_space(&store, created.id.as_str()).await;

    let handle = ok(Mind::open(&store, &owner, &created.id).await);
    assert_eq!(
        handle.row().status,
        mindcore_runtime::minds::MindStatus::Frozen,
        "the row carries the frozen status and open still answers the handle"
    );
    assert_eq!(handle.carrier_entry_id(), created.carrier_entry_id);
}
