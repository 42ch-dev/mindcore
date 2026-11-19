//! Behaviour of the memory path over real SQLite files.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use mindcore_core::ids::{ContextId, SpaceId, SubjectId};
use mindcore_core::memory::{MemoryCandidate, MemoryError, MemoryStore, RecallQuery};
use mindcore_sqlite::MindStore;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn database_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before the epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "mindcore-{}-{label}-{nanos}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

// WAL leaves siblings next to the database; a rerun must start from an empty store.
fn remove_database(path: &Path) {
    let _ = std::fs::remove_file(path);
    for suffix in ["-wal", "-shm"] {
        let mut sibling = path.as_os_str().to_os_string();
        sibling.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(sibling));
    }
}

fn observed_at(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).expect("valid timestamp")
}

fn candidate_in(candidate_id: &str, space: &str, subject: &str, context: &str) -> MemoryCandidate {
    MemoryCandidate {
        candidate_id: candidate_id.to_string(),
        space: SpaceId::new(space),
        subject: SubjectId::new(subject),
        context: ContextId::new(context),
        content: format!("{candidate_id} content"),
        observed_at: observed_at(1_700_000_000),
    }
}

fn recall_in(space: &str, subject: &str, context: Option<&str>) -> RecallQuery {
    RecallQuery {
        space: SpaceId::new(space),
        subject: SubjectId::new(subject),
        context: context.map(ContextId::new),
    }
}

#[tokio::test]
async fn candidate_is_not_recallable_before_review() {
    let path = database_path("unreviewed");
    let store = MindStore::open(&path).await.expect("open store");

    store
        .capture(candidate_in("cnd_unreviewed", "wren", "wren", "tavern"))
        .await
        .expect("capture");

    let recalled = store
        .recall(&recall_in("wren", "wren", None))
        .await
        .expect("recall");
    assert!(recalled.is_empty());

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn retain_preserves_original_placement() {
    let path = database_path("placement");
    let store = MindStore::open(&path).await.expect("open store");

    store
        .capture(candidate_in("cnd_tavern", "wren", "wren", "tavern"))
        .await
        .expect("capture");
    store
        .retain("cnd_tavern", "req-placement")
        .await
        .expect("retain");

    let in_tavern = store
        .recall(&recall_in("wren", "wren", Some("tavern")))
        .await
        .expect("recall tavern");
    assert_eq!(in_tavern.len(), 1);
    assert_eq!(in_tavern[0].context, ContextId::new("tavern"));

    let at_gate = store
        .recall(&recall_in("wren", "wren", Some("gate")))
        .await
        .expect("recall gate");
    assert!(at_gate.is_empty());

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn retain_is_idempotent_by_request() {
    let path = database_path("idempotent");
    let store = MindStore::open(&path).await.expect("open store");

    store
        .capture(candidate_in("cnd_repeat", "wren", "wren", "tavern"))
        .await
        .expect("capture");
    let first = store.retain("cnd_repeat", "req-1").await.expect("retain");
    let replayed = store
        .retain("cnd_repeat", "req-1")
        .await
        .expect("replayed retain");

    assert_eq!(first.record_id, replayed.record_id);
    assert_eq!(first.recorded_at, replayed.recorded_at);

    let recalled = store
        .recall(&recall_in("wren", "wren", None))
        .await
        .expect("recall");
    assert_eq!(recalled.len(), 1);

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn second_request_for_retained_candidate_is_refused() {
    let path = database_path("reviewed");
    let store = MindStore::open(&path).await.expect("open store");

    store
        .capture(candidate_in("cnd_once", "wren", "wren", "tavern"))
        .await
        .expect("capture");
    store.retain("cnd_once", "req-1").await.expect("retain");

    let refused = store.retain("cnd_once", "req-2").await;
    assert!(matches!(
        refused,
        Err(MemoryError::CandidateAlreadyReviewed)
    ));

    let recalled = store
        .recall(&recall_in("wren", "wren", None))
        .await
        .expect("recall");
    assert_eq!(recalled.len(), 1);

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn recall_orders_by_recorded_at_then_record_id() {
    let path = database_path("ordering");
    let store = MindStore::open(&path).await.expect("open store");

    for (index, candidate_id) in ["cnd_first", "cnd_second", "cnd_third"].iter().enumerate() {
        store
            .capture(candidate_in(candidate_id, "wren", "wren", "tavern"))
            .await
            .expect("capture");
        store
            .retain(candidate_id, &format!("req-order-{index}"))
            .await
            .expect("retain");
        std::thread::sleep(Duration::from_millis(5));
    }

    let recalled = store
        .recall(&recall_in("wren", "wren", None))
        .await
        .expect("recall");
    let order: Vec<&str> = recalled
        .iter()
        .map(|record| record.candidate_id.as_str())
        .collect();
    assert_eq!(order, ["cnd_first", "cnd_second", "cnd_third"]);
    assert!(recalled
        .windows(2)
        .all(|pair| pair[0].recorded_at < pair[1].recorded_at));

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn records_survive_reopen() {
    let path = database_path("reopen");

    let store = MindStore::open(&path).await.expect("open store");
    store
        .capture(candidate_in("cnd_keep", "wren", "wren", "tavern"))
        .await
        .expect("capture");
    let retained = store.retain("cnd_keep", "req-keep").await.expect("retain");
    store.close().await;

    let reopened = MindStore::open(&path).await.expect("reopen store");
    let recalled = reopened
        .recall(&recall_in("wren", "wren", None))
        .await
        .expect("recall");
    assert_eq!(recalled, vec![retained]);
    reopened.close().await;

    remove_database(&path);
}

#[tokio::test]
async fn recall_is_scoped_by_space_and_subject() {
    let path = database_path("scoped");
    let store = MindStore::open(&path).await.expect("open store");

    store
        .capture(candidate_in("cnd_scoped", "wren", "wren", "tavern"))
        .await
        .expect("capture");
    store
        .retain("cnd_scoped", "req-scoped")
        .await
        .expect("retain");

    let other_space = store
        .recall(&recall_in("studio", "wren", None))
        .await
        .expect("recall other space");
    assert!(other_space.is_empty());

    let other_subject = store
        .recall(&recall_in("wren", "someone-else", None))
        .await
        .expect("recall other subject");
    assert!(other_subject.is_empty());

    let scoped = store
        .recall(&recall_in("wren", "wren", None))
        .await
        .expect("recall scoped");
    assert_eq!(scoped.len(), 1);

    store.close().await;
    remove_database(&path);
}
