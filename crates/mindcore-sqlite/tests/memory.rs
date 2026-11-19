//! Behaviour of the memory path over real SQLite files.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use mindcore_core::ids::{ContextId, SpaceId, SubjectId};
use mindcore_core::memory::{MemoryCandidate, MemoryError, MemoryStore, RecallQuery};
use mindcore_sqlite::MindStore;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;

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

// Fixture state the public API cannot produce: fixed timestamps and a receipt without its record.
async fn fixture_writer(path: &Path) -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(path)
                .busy_timeout(Duration::from_secs(5)),
        )
        .await
        .expect("open fixture writer")
}

async fn set_recorded_at(path: &Path, values: &[(&str, &str)]) {
    let writer = fixture_writer(path).await;
    for (candidate_id, recorded_at) in values {
        sqlx::query("UPDATE memory_records SET recorded_at = ? WHERE candidate_id = ?")
            .bind(recorded_at)
            .bind(candidate_id)
            .execute(&writer)
            .await
            .expect("set recorded_at");
    }
    writer.close().await;
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
async fn capture_of_identical_fields_leaves_the_row_unchanged() {
    let path = database_path("capture-equal");
    let store = MindStore::open(&path).await.expect("open store");

    let original = candidate_in("cnd_same", "wren", "wren", "tavern");
    store.capture(original.clone()).await.expect("capture");
    store
        .capture(original.clone())
        .await
        .expect("replayed capture");

    assert_eq!(
        store.candidate("cnd_same").await.expect("candidate"),
        Some(original)
    );

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn capture_with_a_changed_field_conflicts_without_replacing() {
    let path = database_path("capture-changed");
    let store = MindStore::open(&path).await.expect("open store");

    let original = candidate_in("cnd_changed", "wren", "wren", "tavern");
    store.capture(original.clone()).await.expect("capture");

    let mut changed = original.clone();
    changed.content = "rewritten content".to_string();
    let refused = store.capture(changed).await;
    assert!(matches!(refused, Err(MemoryError::IdempotencyConflict)));

    assert_eq!(
        store.candidate("cnd_changed").await.expect("candidate"),
        Some(original)
    );

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn reused_request_for_another_candidate_creates_no_record() {
    let path = database_path("request-reuse");
    let store = MindStore::open(&path).await.expect("open store");

    store
        .capture(candidate_in("cnd_a", "wren", "wren", "tavern"))
        .await
        .expect("capture a");
    store
        .capture(candidate_in("cnd_b", "wren", "wren", "tavern"))
        .await
        .expect("capture b");
    let retained = store.retain("cnd_a", "req-shared").await.expect("retain a");

    let reused = store.retain("cnd_b", "req-shared").await;
    assert!(matches!(reused, Err(MemoryError::IdempotencyConflict)));

    let recalled = store
        .recall(&recall_in("wren", "wren", None))
        .await
        .expect("recall");
    assert_eq!(recalled, vec![retained]);

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn retain_of_an_unknown_candidate_is_not_found() {
    let path = database_path("unknown-candidate");
    let store = MindStore::open(&path).await.expect("open store");

    let refused = store.retain("cnd_missing", "req-missing").await;
    assert!(
        matches!(&refused, Err(MemoryError::CandidateNotFound(id)) if id == "cnd_missing"),
        "unexpected outcome: {refused:?}"
    );

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn receipt_without_its_record_is_a_store_error() {
    let path = database_path("orphan-receipt");
    let store = MindStore::open(&path).await.expect("open store");

    // review_receipts.record_id has no foreign key, so this state is reachable.
    let writer = fixture_writer(&path).await;
    sqlx::query(
        "INSERT INTO review_receipts (request_id, candidate_id, record_id) VALUES (?, ?, ?)",
    )
    .bind("req-orphan")
    .bind("cnd_orphan")
    .bind("rec_cnd_orphan")
    .execute(&writer)
    .await
    .expect("insert receipt");
    writer.close().await;

    let refused = store.retain("cnd_orphan", "req-orphan").await;
    assert!(
        matches!(refused, Err(MemoryError::Store(_))),
        "unexpected outcome: {refused:?}"
    );

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn recall_orders_by_recorded_at_then_record_id() {
    let path = database_path("ordering");
    let store = MindStore::open(&path).await.expect("open store");

    // Insertion order deliberately differs from the expected result order.
    for (index, candidate_id) in ["cnd_zeta", "cnd_alpha", "cnd_mid", "cnd_tie_b", "cnd_tie_a"]
        .iter()
        .enumerate()
    {
        store
            .capture(candidate_in(candidate_id, "wren", "wren", "tavern"))
            .await
            .expect("capture");
        store
            .retain(candidate_id, &format!("req-order-{index}"))
            .await
            .expect("retain");
    }

    set_recorded_at(
        &path,
        &[
            // Fraction of a second after a whole second, and the next whole second.
            ("cnd_alpha", "2023-11-14T22:13:20.500000000Z"),
            ("cnd_zeta", "2023-11-14T22:13:21.000000000Z"),
            // Whole-second instants; equal timestamps tie-break on record_id, not insertion order.
            ("cnd_mid", "2023-11-14T22:13:20.000000000Z"),
            ("cnd_tie_b", "2023-11-14T22:13:20.000000000Z"),
            ("cnd_tie_a", "2023-11-14T22:13:20.000000000Z"),
        ],
    )
    .await;

    let recalled = store
        .recall(&recall_in("wren", "wren", None))
        .await
        .expect("recall");
    let order: Vec<&str> = recalled
        .iter()
        .map(|record| record.candidate_id.as_str())
        .collect();
    assert_eq!(
        order,
        ["cnd_mid", "cnd_tie_a", "cnd_tie_b", "cnd_alpha", "cnd_zeta"]
    );

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

// Two handles on one file used to let both transactions read "no receipt" and then fail on the lock
// upgrade. The write reservation is now taken before the reads, so the loser waits and re-reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_handles_serialize_retain() {
    let path = database_path("two-handles");
    let first_handle = MindStore::open(&path).await.expect("open first store");
    let second_handle = MindStore::open(&path).await.expect("open second store");

    first_handle
        .capture(candidate_in("cnd_shared", "wren", "wren", "tavern"))
        .await
        .expect("capture shared");
    first_handle
        .capture(candidate_in("cnd_other", "wren", "wren", "tavern"))
        .await
        .expect("capture other");

    // Replaying one request on both handles returns the winner's record on both.
    let (left, right) = tokio::join!(
        first_handle.retain("cnd_shared", "req-race"),
        second_handle.retain("cnd_shared", "req-race"),
    );
    let left = left.expect("first replay");
    let right = right.expect("second replay");
    assert_eq!(left, right);

    // Two distinct requests for one candidate: one wins, the loser is refused, neither errors.
    let (left, right) = tokio::join!(
        first_handle.retain("cnd_other", "req-race-a"),
        second_handle.retain("cnd_other", "req-race-b"),
    );
    match (left, right) {
        (Ok(_), Err(MemoryError::CandidateAlreadyReviewed))
        | (Err(MemoryError::CandidateAlreadyReviewed), Ok(_)) => {}
        (left, right) => panic!("unexpected concurrent outcome: {left:?} / {right:?}"),
    }

    let recalled = first_handle
        .recall(&recall_in("wren", "wren", None))
        .await
        .expect("recall");
    assert_eq!(recalled.len(), 2);

    first_handle.close().await;
    second_handle.close().await;
    remove_database(&path);
}
