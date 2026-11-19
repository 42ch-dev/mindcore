//! Behaviour of the scoped knowledge port over real SQLite files.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mindcore_base::knowledge::{adoption_anchor, KnowledgeScope};
use mindcore_sqlite::{MindStore, ScopedKnowledgeStore};
use serde_json::{json, Map, Value};
use spoke_operations::{KnowledgeEntryPort, SpokeRejectCode, SpokeResult};
use spoke_schemas::knowledge_entry::{
    ExtensionKey, KnowledgeEntryBody, KnowledgeEntryCanonicalName, KnowledgeEntryOwner,
};
use spoke_schemas::source_anchor::ExtensionKey as AnchorExtensionKey;
use spoke_schemas::KnowledgeEntry;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn database_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before the epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "mindcore-knowledge-{}-{label}-{nanos}-{}",
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

// A second pool on the same file, so a test can hold SQLite's write reservation without writing.
async fn second_connection(path: &Path) -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(path)
                .busy_timeout(Duration::from_secs(5)),
        )
        .await
        .expect("open second connection")
}

fn entry_with(entry_id: &str, summary: &str) -> KnowledgeEntry {
    KnowledgeEntry {
        body: KnowledgeEntryBody {
            summary: Some(summary.to_string()),
            ..KnowledgeEntryBody::default()
        },
        canonical_name: KnowledgeEntryCanonicalName::try_from(format!("{entry_id} name"))
            .expect("non-empty canonical name"),
        created_at: None,
        disclosure: None,
        entry_id: entry_id.to_string(),
        entry_type: "location".to_string(),
        extensions: HashMap::new(),
        modules: HashMap::new(),
        owner: None,
        revision: None,
        schema_version: NonZeroU64::new(1).expect("non-zero wire baseline"),
        source_anchor: None,
        status: "confirmed".to_string(),
        updated_at: None,
    }
}

fn namespace(key: &str) -> ExtensionKey {
    ExtensionKey::try_from(key).expect("valid extension key")
}

fn summary_of(entry: &KnowledgeEntry) -> Option<&str> {
    entry.body.summary.as_deref()
}

fn as_json(entry: &KnowledgeEntry) -> Value {
    serde_json::to_value(entry).expect("entry serializes")
}

fn expect_ok<T>(result: SpokeResult<T>, context: &str) -> T {
    match result {
        SpokeResult::Ok(value) => value,
        SpokeResult::Reject(reject) => panic!("{context}: unexpectedly rejected with {reject:?}"),
    }
}

fn expect_reject<T: std::fmt::Debug>(result: SpokeResult<T>, context: &str) -> SpokeRejectCode {
    match result {
        SpokeResult::Ok(value) => panic!("{context}: unexpectedly accepted with {value:?}"),
        SpokeResult::Reject(reject) => reject.code,
    }
}

#[tokio::test]
async fn create_assigns_revision_one() {
    let path = database_path("create");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    let created = expect_ok(
        port.put_knowledge_entry(entry_with("ke_gate", "closed after dusk"), None)
            .await,
        "create",
    );
    assert_eq!(created.revision, Some(1));

    let fetched = expect_ok(port.get_knowledge_entry("ke_gate").await, "get");
    assert_eq!(as_json(&fetched), as_json(&created));
    assert_eq!(summary_of(&fetched), Some("closed after dusk"));

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn create_twice_is_refused() {
    let path = database_path("create-twice");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    let created = expect_ok(
        port.put_knowledge_entry(entry_with("ke_gate", "closed after dusk"), None)
            .await,
        "create",
    );
    let refused = port
        .put_knowledge_entry(entry_with("ke_gate", "open all night"), None)
        .await;
    assert_eq!(
        expect_reject(refused, "second create"),
        SpokeRejectCode::KnowledgeEntryAlreadyExists
    );

    let fetched = expect_ok(port.get_knowledge_entry("ke_gate").await, "get");
    assert_eq!(as_json(&fetched), as_json(&created));

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn conditional_update_bumps_revision() {
    let path = database_path("update");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    expect_ok(
        port.put_knowledge_entry(entry_with("ke_gate", "closed after dusk"), None)
            .await,
        "create",
    );
    let updated = expect_ok(
        port.put_knowledge_entry(entry_with("ke_gate", "closed from dusk to dawn"), Some(1))
            .await,
        "conditional update",
    );
    assert_eq!(updated.revision, Some(2));
    assert_eq!(summary_of(&updated), Some("closed from dusk to dawn"));

    let fetched = expect_ok(port.get_knowledge_entry("ke_gate").await, "get");
    assert_eq!(as_json(&fetched), as_json(&updated));

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn submitted_timestamps_are_persisted_verbatim() {
    let path = database_path("timestamps");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    let mut submitted = entry_with("ke_gate", "closed after dusk");
    submitted.created_at = Some(
        "2026-01-02T03:04:05.123456789Z"
            .parse()
            .expect("valid RFC 3339"),
    );
    submitted.updated_at = Some("2026-02-03T04:05:06Z".parse().expect("valid RFC 3339"));

    let created = expect_ok(port.put_knowledge_entry(submitted, None).await, "create");
    let created_json = as_json(&created);
    assert_eq!(created_json["revision"], json!(1));
    assert_eq!(
        created_json["created_at"],
        json!("2026-01-02T03:04:05.123456789Z")
    );
    assert_eq!(created_json["updated_at"], json!("2026-02-03T04:05:06Z"));

    let fetched = expect_ok(port.get_knowledge_entry("ke_gate").await, "get");
    assert_eq!(as_json(&fetched), created_json);

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn stale_update_is_refused_without_change() {
    let path = database_path("stale");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    expect_ok(
        port.put_knowledge_entry(entry_with("ke_gate", "closed after dusk"), None)
            .await,
        "create",
    );
    let current = expect_ok(
        port.put_knowledge_entry(entry_with("ke_gate", "closed from dusk to dawn"), Some(1))
            .await,
        "first update",
    );

    let refused = port
        .put_knowledge_entry(entry_with("ke_gate", "open all night"), Some(1))
        .await;
    assert_eq!(
        expect_reject(refused, "stale update"),
        SpokeRejectCode::StoredRevisionStale
    );

    // An update aimed at an absent row is stale too, not a create.
    let absent = port
        .put_knowledge_entry(entry_with("ke_missing", "never written"), Some(1))
        .await;
    assert_eq!(
        expect_reject(absent, "update of absent entry"),
        SpokeRejectCode::StoredRevisionStale
    );
    // A refusal must leave no row behind.
    let absent_get = port.get_knowledge_entry("ke_missing").await;
    assert_eq!(
        expect_reject(absent_get, "get of absent entry"),
        SpokeRejectCode::KnowledgeEntryNotFound
    );

    let fetched = expect_ok(port.get_knowledge_entry("ke_gate").await, "get");
    assert_eq!(as_json(&fetched), as_json(&current));
    assert_eq!(fetched.revision, Some(2));
    assert_eq!(summary_of(&fetched), Some("closed from dusk to dawn"));

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn entries_survive_reopen() {
    let path = database_path("reopen");

    let store = MindStore::open(&path).await.expect("open store");
    let written = expect_ok(
        store
            .knowledge(KnowledgeScope::new("wren"))
            .put_knowledge_entry(entry_with("ke_gate", "closed after dusk"), None)
            .await,
        "create",
    );
    store.close().await;

    let reopened = MindStore::open(&path).await.expect("reopen store");
    let fetched = expect_ok(
        reopened
            .knowledge(KnowledgeScope::new("wren"))
            .get_knowledge_entry("ke_gate")
            .await,
        "get after reopen",
    );
    assert_eq!(as_json(&fetched), as_json(&written));
    reopened.close().await;

    remove_database(&path);
}

#[tokio::test]
async fn cross_handle_cas_lets_exactly_one_writer_win() {
    let path = database_path("cross-handle");
    let left_store = MindStore::open(&path).await.expect("open left store");
    let right_store = MindStore::open(&path).await.expect("open right store");
    let left = left_store.knowledge(KnowledgeScope::new("wren"));
    let right = right_store.knowledge(KnowledgeScope::new("wren"));

    expect_ok(
        left.put_knowledge_entry(entry_with("ke_gate", "closed after dusk"), None)
            .await,
        "create",
    );

    let (left_result, right_result) = tokio::join!(
        left.put_knowledge_entry(entry_with("ke_gate", "left writer"), Some(1)),
        right.put_knowledge_entry(entry_with("ke_gate", "right writer"), Some(1)),
    );
    let winner = match (left_result, right_result) {
        (SpokeResult::Ok(entry), SpokeResult::Reject(reject))
        | (SpokeResult::Reject(reject), SpokeResult::Ok(entry)) => {
            assert_eq!(reject.code, SpokeRejectCode::StoredRevisionStale);
            entry
        }
        _ => panic!("exactly one concurrent put must win"),
    };
    assert_eq!(winner.revision, Some(2));

    // Both handles must see the winner's payload and revision, never the loser's.
    for port in [&left, &right] {
        let fetched = expect_ok(port.get_knowledge_entry("ke_gate").await, "get");
        assert_eq!(as_json(&fetched), as_json(&winner));
    }

    left_store.close().await;
    right_store.close().await;
    remove_database(&path);
}

/// Structural proof that `put` holds SQLite's write reservation before the claim read.
///
/// The port under test runs on a one-connection pool with `busy_timeout` zero, and a second
/// connection holds `BEGIN IMMEDIATE` — SQLite's write reservation — without writing. Any `put`
/// that asks for the reservation at `BEGIN` fails there, never reaching the revision read; a put
/// that deferred it (`BEGIN DEFERRED`) instead fails *inside* the write with a different message,
/// so the begin-boundary failure mode itself is the assertion. Not proven: the internal lock
/// sequence during `BEGIN IMMEDIATE` — only that the reservation is requested at begin, blocking
/// reads, and that a refused put leaves no row.
#[tokio::test]
async fn put_takes_the_write_reservation_before_the_claim_read() {
    let path = database_path("reservation");
    let store = MindStore::open(&path).await.expect("open store");
    // The tested port never waits: contention must fail at begin, not after a timeout.
    let port = ScopedKnowledgeStore::new(
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .busy_timeout(Duration::ZERO),
            )
            .await
            .expect("open zero-timeout port pool"),
        KnowledgeScope::new("wren"),
    );

    // A held write reservation that writes nothing: the put below can only fail at `BEGIN
    // IMMEDIATE` if the reservation is taken before the claim read.
    let holder = second_connection(&path).await;
    let reservation = holder
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("hold the write reservation");

    let reject = match port
        .put_knowledge_entry(entry_with("ke_gate", "closed after dusk"), None)
        .await
    {
        SpokeResult::Ok(entry) => {
            panic!("put wrote while another connection held the write reservation: {entry:?}")
        }
        SpokeResult::Reject(reject) => reject,
    };
    assert_eq!(reject.code, SpokeRejectCode::InternalError);
    assert!(
        reject.message.contains("knowledge write could not begin"),
        "the put must fail at BEGIN, not at the later write: {}",
        reject.message
    );
    assert!(
        reject.message.contains("locked"),
        "the begin failure must carry SQLite's lock condition: {}",
        reject.message
    );

    // The refused put wrote no row: the collection is still empty.
    assert!(expect_ok(port.list_entries().await, "list after refused put").is_empty());

    reservation
        .rollback()
        .await
        .expect("release the write reservation");
    holder.close().await;

    let created = expect_ok(
        port.put_knowledge_entry(entry_with("ke_gate", "closed after dusk"), None)
            .await,
        "create once the reservation is released",
    );
    assert_eq!(created.revision, Some(1));
    let fetched = expect_ok(port.get_knowledge_entry("ke_gate").await, "get");
    assert_eq!(as_json(&fetched), as_json(&created));

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn same_entry_id_in_two_collections_isolation() {
    let path = database_path("collections");
    let store = MindStore::open(&path).await.expect("open store");
    let wren = store.knowledge(KnowledgeScope::new("wren"));
    let studio = store.knowledge(KnowledgeScope::new("studio"));

    expect_ok(
        wren.put_knowledge_entry(entry_with("ke_gate", "Wren gate closes at dusk"), None)
            .await,
        "create wren",
    );
    expect_ok(
        studio
            .put_knowledge_entry(entry_with("ke_gate", "Studio gate stays open"), None)
            .await,
        "create studio",
    );

    let mut wren_update = entry_with("ke_gate", "Wren gate closes at dusk and reopens at dawn");
    wren_update.owner = Some(
        KnowledgeEntryOwner::try_from("ke_wren_record_office").expect("non-empty owner entry id"),
    );
    wren_update.extensions = HashMap::from([(
        namespace("mindcore_test"),
        Map::from_iter([("viewpoint".to_string(), json!("studio"))]),
    )]);
    let updated = expect_ok(
        wren.put_knowledge_entry(wren_update, Some(1)).await,
        "update wren",
    );
    assert_eq!(updated.revision, Some(2));

    let wren_fetched = expect_ok(wren.get_knowledge_entry("ke_gate").await, "get wren");
    assert_eq!(as_json(&wren_fetched), as_json(&updated));
    assert_eq!(
        summary_of(&wren_fetched),
        Some("Wren gate closes at dusk and reopens at dawn")
    );
    assert_eq!(
        wren_fetched.owner.as_deref().map(String::as_str),
        Some("ke_wren_record_office")
    );
    assert_eq!(
        wren_fetched
            .extensions
            .get(&namespace("mindcore_test"))
            .and_then(|fields| fields.get("viewpoint")),
        Some(&json!("studio"))
    );

    let studio_fetched = expect_ok(studio.get_knowledge_entry("ke_gate").await, "get studio");
    assert_eq!(studio_fetched.revision, Some(1));
    assert_eq!(summary_of(&studio_fetched), Some("Studio gate stays open"));
    assert!(studio_fetched.owner.is_none());
    assert!(studio_fetched.extensions.is_empty());

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn list_entries_is_scoped_and_ordered() {
    let path = database_path("list");
    let store = MindStore::open(&path).await.expect("open store");
    let wren = store.knowledge(KnowledgeScope::new("wren"));
    let studio = store.knowledge(KnowledgeScope::new("studio"));

    // Insertion order is the reverse of the expected result order.
    for (entry_id, summary) in [
        ("ke_zulu", "Wren mill lane"),
        ("ke_alpha", "Wren north gate"),
    ] {
        expect_ok(
            wren.put_knowledge_entry(entry_with(entry_id, summary), None)
                .await,
            "create wren entry",
        );
    }
    expect_ok(
        studio
            .put_knowledge_entry(entry_with("ke_mid", "Studio loft"), None)
            .await,
        "create studio entry",
    );

    let listed = expect_ok(wren.list_entries().await, "list wren");
    let ids: Vec<&str> = listed.iter().map(|entry| entry.entry_id.as_str()).collect();
    assert_eq!(ids, ["ke_alpha", "ke_zulu"]);

    let studio_listed = expect_ok(studio.list_entries().await, "list studio");
    let studio_ids: Vec<&str> = studio_listed
        .iter()
        .map(|entry| entry.entry_id.as_str())
        .collect();
    assert_eq!(studio_ids, ["ke_mid"]);

    store.close().await;
    remove_database(&path);
}

#[test]
fn adoption_anchor_shape() {
    let basis = "adopted from the village fact sheet";
    let anchor = adoption_anchor("ke_shared_north_gate", basis);

    assert_eq!(anchor.source_id, "shared:ke_shared_north_gate");
    assert_eq!(anchor.schema_version, NonZeroU64::new(1).expect("non-zero"));
    let adoption = AnchorExtensionKey::try_from("mindcore_adoption").expect("valid extension key");
    assert_eq!(
        anchor
            .extensions
            .get(&adoption)
            .and_then(|fields| fields.get("basis")),
        Some(&json!(basis))
    );
    assert!(anchor.span.is_none());
    assert!(anchor.label.is_none());
    assert!(anchor.mime_type.is_none());

    // The shared fixture is the pinned wire shape the anchor points back to.
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/iteration-01/shared-collection.json"
    ))
    .expect("fixture is JSON");
    assert_eq!(fixture["collection"], json!("village-facts"));
    let entries: Vec<KnowledgeEntry> = serde_json::from_value(fixture["entries"].clone())
        .expect("fixture entries are wire entries");
    assert_eq!(entries.len(), 1);
    assert_eq!(format!("shared:{}", entries[0].entry_id), anchor.source_id);
}
