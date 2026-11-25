//! The library entry points `create`, `list` and `get` in `mindcore_runtime::minds`, run
//! end to end against a real SQLite store opened through the production `MindStore::open`
//! path: a creation the owner reads back, refusals for a caller without a grant and an id
//! never created, and identity across a reopen.

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::MindId;
use mindcore_runtime::minds::{self, CreateMind};
use mindcore_sqlite::MindStore;
use spoke_operations::{SpokeRejectCode, SpokeResult};
use tempfile::TempDir;

const OWNER: &str = "caller_wren";

struct Fixture {
    dir: TempDir,
    store: MindStore,
}

impl Fixture {
    async fn open() -> Self {
        let dir = TempDir::new().expect("temporary directory");
        let store = MindStore::open(&dir.path().join("minds.db"))
            .await
            .expect("open store");
        Fixture { dir, store }
    }

    fn path(&self) -> std::path::PathBuf {
        self.dir.path().join("minds.db")
    }
}

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

#[tokio::test]
async fn create_list_and_get_run_through_the_runtime() {
    let fixture = Fixture::open().await;
    let owner = CallerId::new(OWNER);

    let mind = ok(minds::create(&fixture.store, &request(OWNER)).await);
    assert_eq!(mind.display_name.as_deref(), Some("Wren"));

    let listed = ok(minds::list(&fixture.store, &owner).await);
    assert_eq!(listed, vec![mind.clone()]);

    let read = ok(minds::get(&fixture.store, &owner, &mind.id).await);
    assert_eq!(read, mind);

    let other = CallerId::new("caller_else");
    assert!(ok(minds::list(&fixture.store, &other).await).is_empty());
}

#[tokio::test]
async fn get_answers_not_found_through_the_runtime() {
    let fixture = Fixture::open().await;
    let mind = ok(minds::create(&fixture.store, &request(OWNER)).await);

    let outsider = CallerId::new("caller_else");
    assert_eq!(
        reject_code(minds::get(&fixture.store, &outsider, &mind.id).await),
        SpokeRejectCode::NotFound
    );

    let owner = CallerId::new(OWNER);
    let never = MindId::new("spc_never_created");
    assert_eq!(
        reject_code(minds::get(&fixture.store, &owner, &never).await),
        SpokeRejectCode::NotFound
    );
}

#[tokio::test]
async fn get_returns_the_same_mind_after_reopen_through_the_runtime() {
    let fixture = Fixture::open().await;
    let owner = CallerId::new(OWNER);
    let mind = ok(minds::create(&fixture.store, &request(OWNER)).await);
    let path = fixture.path();
    fixture.store.close().await;

    let reopened = MindStore::open(&path).await.expect("reopen store");
    let read = ok(minds::get(&reopened, &owner, &mind.id).await);
    assert_eq!(read, mind);
}
