//! Behaviour of the scoped knowledge port over real SQLite files.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mindcore_base::cognition::CARRIER_NAMESPACE;
use mindcore_base::knowledge::{adoption_anchor, KnowledgeScope};
use mindcore_sqlite::{append_cognition, write_entry, MindStore, ScopedKnowledgeStore};
use serde_json::{json, Map, Value};
use spoke_operations::{KnowledgeEntryPort, SpokeRejectCode, SpokeResult};
use spoke_schemas::knowledge_entry::{
    ExtensionKey, KnowledgeEntryBody, KnowledgeEntryCanonicalName, KnowledgeEntryModulesKey,
    KnowledgeEntryModulesValue, KnowledgeEntryOwner,
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

// A connection enforcing foreign keys the way `MindStore::open` configures the production pool.
async fn foreign_keys_connection(path: &Path) -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(path)
                .busy_timeout(Duration::from_secs(5))
                .foreign_keys(true),
        )
        .await
        .expect("open foreign-key connection")
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

fn cognition_module_key() -> KnowledgeEntryModulesKey {
    KnowledgeEntryModulesKey::try_from("cognition").expect("valid module key")
}

fn cognition_module_value(module: Value) -> KnowledgeEntryModulesValue {
    KnowledgeEntryModulesValue::from(
        module
            .as_object()
            .expect("module fixture is an object")
            .clone(),
    )
}

/// An entry submitting the cognition envelope with `records` inline.
fn cognition_entry(entry_id: &str, summary: &str, records: Vec<Value>) -> KnowledgeEntry {
    let mut entry = entry_with(entry_id, summary);
    entry.modules.insert(
        cognition_module_key(),
        cognition_module_value(json!({
            "records": records,
            "world_facts": [],
            "extensions": {},
        })),
    );
    entry
}

// One current-view belief record held by `holder`: the shape a dedicated append accepts.
fn belief(holder: &str, record_id: &str) -> Value {
    json!({
        "kind": "belief",
        "order": 1,
        "record_id": record_id,
        "holder": {
            "kind": "knowledge_entry",
            "id": holder,
            "namespace": CARRIER_NAMESPACE,
        },
        "target": {
            "carrier": {
                "kind": "knowledge_entry",
                "id": "ke_shared_north_gate",
                "namespace": CARRIER_NAMESPACE,
            },
            "target_id": "prop_north_gate_closed",
            "kind": "proposition",
        },
        "assessor": {
            "kind": "knowledge_entry",
            "id": holder,
            "namespace": CARRIER_NAMESPACE,
        },
        "assessor_role": "holder_self_report",
        "basis": {
            "kind": "evidence",
            "evidence": [{
                "kind": "memory_record",
                "id": "mem_hearsay",
                "namespace": CARRIER_NAMESPACE,
            }],
        },
        "assessment_context": {"context_id": "ctx_dusk_at_the_gate"},
        "effective_at": "2026-11-19T18:00:00Z",
        "assessed_at": "2026-11-19T18:05:00Z",
        "view": "current",
        "q": {
            "value": 0.4,
            "assertion": "belief_attribution",
            "method": "single_self_report",
            "calibration": "uncalibrated",
        },
        "extensions": {},
    })
}

fn module_of(entry: &KnowledgeEntry) -> Option<&Map<String, Value>> {
    match entry.modules.get(&cognition_module_key()) {
        Some(KnowledgeEntryModulesValue::Object(module)) => Some(module),
        _ => None,
    }
}

fn record_ids(entry: &KnowledgeEntry) -> Vec<String> {
    module_of(entry)
        .and_then(|module| module.get("records"))
        .and_then(Value::as_array)
        .map(|records| {
            records
                .iter()
                .filter_map(|record| {
                    record
                        .get("record_id")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The preimage rows of one entry, in revision order.
async fn revision_rows(
    connection: &SqlitePool,
    entry_id: &str,
) -> Vec<(i64, String, Option<i64>, String)> {
    sqlx::query_as(
        "SELECT revision, payload, cognition_head_seq, recorded_at \
         FROM knowledge_entry_revisions \
         WHERE collection = 'wren' AND entry_id = ? ORDER BY revision ASC",
    )
    .bind(entry_id)
    .fetch_all(connection)
    .await
    .expect("preimage rows")
}

/// Every cognition record row of one entry, in storage order.
async fn record_rows(
    connection: &SqlitePool,
    entry_id: &str,
) -> Vec<(i64, String, String, Option<i64>)> {
    sqlx::query_as(
        "SELECT append_seq, record_id, payload, append_prior_shape FROM cognition_records \
         WHERE collection = 'wren' AND entry_id = ? ORDER BY append_seq ASC",
    )
    .bind(entry_id)
    .fetch_all(connection)
    .await
    .expect("record rows")
}

async fn stored_updated_at(connection: &SqlitePool, entry_id: &str) -> String {
    sqlx::query_scalar(
        "SELECT updated_at FROM knowledge_entries WHERE collection = 'wren' AND entry_id = ?",
    )
    .bind(entry_id)
    .fetch_one(connection)
    .await
    .expect("stored updated_at")
}

async fn stored_shape(connection: &SqlitePool, entry_id: &str, append_seq: i64) -> Option<i64> {
    sqlx::query_scalar(
        "SELECT append_prior_shape FROM cognition_records \
         WHERE collection = 'wren' AND entry_id = ? AND append_seq = ?",
    )
    .bind(entry_id)
    .bind(append_seq)
    .fetch_one(connection)
    .await
    .expect("stored append shape")
}

/// One dedicated append through its own write transaction, as the workflow's commit does.
async fn append(connection: &SqlitePool, entry_id: &str, expected: u64, record_id: &str) -> u64 {
    let mut tx = connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("begin append");
    let assigned = expect_ok(
        append_cognition(
            &mut tx,
            "wren",
            entry_id,
            expected,
            &belief(entry_id, record_id),
        )
        .await,
        "dedicated append",
    );
    tx.commit().await.expect("commit append");
    assigned
}

fn expect_revision_not_found<T: std::fmt::Debug>(result: SpokeResult<T>, context: &str) {
    match result {
        SpokeResult::Ok(value) => panic!("{context}: unexpectedly returned {value:?}"),
        SpokeResult::Reject(reject) => {
            assert_eq!(
                reject.code,
                SpokeRejectCode::KnowledgeEntryNotFound,
                "{context}"
            );
            assert_eq!(
                reject.message, "knowledge entry revision not found",
                "{context}"
            );
            assert!(
                reject.details.is_none(),
                "{context}: an absent revision carries no details"
            );
        }
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
async fn update_keeps_the_stored_creation_instant_in_the_payload() {
    let path = database_path("created-at");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    let mut submitted = entry_with("ke_gate", "closed after dusk");
    submitted.created_at = Some(
        "2026-01-02T03:04:05.123456789Z"
            .parse()
            .expect("valid RFC 3339"),
    );
    expect_ok(port.put_knowledge_entry(submitted, None).await, "create");

    // The update submits a fresher creation instant. The row column keeps the
    // original, so the committed payload must carry that original too.
    let mut update = entry_with("ke_gate", "closed from dusk to dawn");
    update.created_at = Some("2026-03-04T05:06:07Z".parse().expect("valid RFC 3339"));
    let updated = expect_ok(port.put_knowledge_entry(update, Some(1)).await, "update");

    let connection = second_connection(&path).await;
    let stored: String = sqlx::query_scalar(
        "SELECT created_at FROM knowledge_entries \
         WHERE collection = 'wren' AND entry_id = 'ke_gate'",
    )
    .fetch_one(&connection)
    .await
    .expect("stored creation instant");
    assert_eq!(
        as_json(&updated)["created_at"],
        json!(stored),
        "the payload and the row column agree on created_at"
    );

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

#[tokio::test]
async fn a_create_saves_no_preimage() {
    let path = database_path("revision-create");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let reader = second_connection(&path).await;

    expect_ok(
        port.put_knowledge_entry(entry_with("ke_gate", "closed after dusk"), None)
            .await,
        "create a module-less entry",
    );
    // A create carrying a record only patches the head: nothing is displaced.
    expect_ok(
        port.put_knowledge_entry(
            cognition_entry(
                "ke_signal",
                "lit at dusk",
                vec![belief("ke_signal", "cog_signal")],
            ),
            None,
        )
        .await,
        "create an entry with a record",
    );

    assert!(revision_rows(&reader, "ke_gate").await.is_empty());
    assert!(revision_rows(&reader, "ke_signal").await.is_empty());

    reader.close().await;
    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn every_update_saves_the_replaced_revision_in_order() {
    let path = database_path("revision-order");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let reader = second_connection(&path).await;

    let created = expect_ok(
        port.put_knowledge_entry(
            cognition_entry("ke_gate", "closed after dusk", Vec::new()),
            None,
        )
        .await,
        "create",
    );
    let mut observed = vec![as_json(&created)];
    let mut written_at = vec![stored_updated_at(&reader, "ke_gate").await];
    for (base, summary) in [
        (1u64, "closed from dusk to dawn"),
        (2, "closed until the bell"),
        (3, "open all night"),
    ] {
        let updated = expect_ok(
            port.put_knowledge_entry(cognition_entry("ke_gate", summary, Vec::new()), Some(base))
                .await,
            "update",
        );
        observed.push(as_json(&updated));
        written_at.push(stored_updated_at(&reader, "ke_gate").await);
    }

    let rows = revision_rows(&reader, "ke_gate").await;
    assert_eq!(
        rows.len(),
        3,
        "each of the three updates saves one preimage"
    );
    for (index, (revision, payload, head, recorded_at)) in rows.iter().enumerate() {
        assert_eq!(
            *revision,
            index as i64 + 1,
            "preimages are keyed by the revision they held"
        );
        assert_eq!(
            serde_json::from_str::<Value>(payload).expect("stored payload is JSON"),
            observed[index],
            "the preimage keeps the exact payload revision {} observed",
            index + 1
        );
        assert_eq!(*head, None, "a headless revision stores no cognition head");
        assert_eq!(
            recorded_at, &written_at[index],
            "recorded_at is the instant the replaced state was written"
        );
    }

    reader.close().await;
    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn a_full_put_with_a_record_suffix_saves_its_preimage() {
    let path = database_path("revision-full-put");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let reader = second_connection(&path).await;

    let created = expect_ok(
        port.put_knowledge_entry(
            cognition_entry("ke_gate", "closed after dusk", Vec::new()),
            None,
        )
        .await,
        "create",
    );
    let updated = expect_ok(
        port.put_knowledge_entry(
            cognition_entry(
                "ke_gate",
                "closed from dusk to dawn",
                vec![belief("ke_gate", "cog_first")],
            ),
            Some(1),
        )
        .await,
        "update with one record",
    );
    assert_eq!(updated.revision, Some(2));
    assert_eq!(record_ids(&updated), ["cog_first"]);

    let rows = revision_rows(&reader, "ke_gate").await;
    assert_eq!(
        rows.len(),
        1,
        "a full put is not a dedicated append even when its head advances by one"
    );
    assert_eq!(rows[0].0, 1);
    assert_eq!(
        serde_json::from_str::<Value>(&rows[0].1).expect("stored payload is JSON"),
        as_json(&created)
    );
    assert_eq!(rows[0].2, None);
    assert_eq!(
        stored_shape(&reader, "ke_gate", 1).await,
        None,
        "a record a full put committed carries no append classification"
    );

    reader.close().await;
    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn a_dedicated_append_saves_no_preimage_and_is_classified() {
    let path = database_path("revision-append");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let writer = second_connection(&path).await;

    expect_ok(
        port.put_knowledge_entry(
            cognition_entry("ke_gate", "closed after dusk", Vec::new()),
            None,
        )
        .await,
        "create",
    );
    assert_eq!(append(&writer, "ke_gate", 1, "cog_first").await, 2);

    assert!(
        revision_rows(&writer, "ke_gate").await.is_empty(),
        "a dedicated append displaces no revision"
    );
    assert_eq!(
        stored_shape(&writer, "ke_gate", 1).await,
        Some(0),
        "the module kept its records member"
    );
    let live = expect_ok(port.get_knowledge_entry("ke_gate").await, "live entry");
    assert_eq!(live.revision, Some(2));
    assert_eq!(record_ids(&live), ["cog_first"]);

    writer.close().await;
    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn a_refused_or_rolled_back_write_leaves_no_preimage() {
    let path = database_path("revision-refused");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let writer = second_connection(&path).await;

    expect_ok(
        port.put_knowledge_entry(
            cognition_entry(
                "ke_gate",
                "closed after dusk",
                vec![belief("ke_gate", "cog_first")],
            ),
            None,
        )
        .await,
        "create with a committed record",
    );

    let invalid = port
        .put_knowledge_entry(
            cognition_entry("ke_gate", "drops the committed record", Vec::new()),
            Some(1),
        )
        .await;
    assert_eq!(
        expect_reject(invalid, "update that drops a committed record"),
        SpokeRejectCode::InvalidInput
    );

    let stale = port
        .put_knowledge_entry(
            cognition_entry(
                "ke_gate",
                "stale update",
                vec![belief("ke_gate", "cog_first")],
            ),
            Some(9),
        )
        .await;
    assert_eq!(
        expect_reject(stale, "stale update"),
        SpokeRejectCode::StoredRevisionStale
    );

    // A write that succeeded inside the caller's transaction, then rolled back, leaves nothing —
    // including the record row of the new suffix it submitted.
    let mut tx = writer
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("begin caller transaction");
    expect_ok(
        write_entry(
            &mut tx,
            "wren",
            cognition_entry(
                "ke_gate",
                "rolled back",
                vec![
                    belief("ke_gate", "cog_first"),
                    belief("ke_gate", "cog_second"),
                ],
            ),
            Some(1),
        )
        .await,
        "write inside the caller transaction",
    );
    tx.rollback()
        .await
        .expect("roll back the caller transaction");

    assert!(
        revision_rows(&writer, "ke_gate").await.is_empty(),
        "no refused or rolled-back write leaves a preimage"
    );
    assert!(
        record_rows(&writer, "ke_gate")
            .await
            .iter()
            .all(|(_, record_id, _, _)| record_id != "cog_second"),
        "a rolled-back suffix leaves no record row"
    );
    let live = expect_ok(port.get_knowledge_entry("ke_gate").await, "live entry");
    assert_eq!(live.revision, Some(1));
    assert_eq!(record_ids(&live), ["cog_first"]);

    writer.close().await;
    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn every_assigned_revision_replays_the_dto_it_observed() {
    let path = database_path("replay");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let writer = second_connection(&path).await;

    let created = expect_ok(
        port.put_knowledge_entry(cognition_entry("ke_gate", "first", Vec::new()), None)
            .await,
        "create",
    );
    let second = expect_ok(
        port.put_knowledge_entry(cognition_entry("ke_gate", "second", Vec::new()), Some(1))
            .await,
        "second revision",
    );
    assert_eq!(append(&writer, "ke_gate", 2, "cog_first").await, 3);
    let third = expect_ok(
        port.get_knowledge_entry("ke_gate").await,
        "read after the first append",
    );
    assert_eq!(append(&writer, "ke_gate", 3, "cog_second").await, 4);
    let fourth = expect_ok(
        port.get_knowledge_entry("ke_gate").await,
        "read after the second append",
    );
    let fifth = expect_ok(
        port.put_knowledge_entry(
            cognition_entry(
                "ke_gate",
                "fifth",
                vec![
                    belief("ke_gate", "cog_first"),
                    belief("ke_gate", "cog_second"),
                    belief("ke_gate", "cog_third"),
                ],
            ),
            Some(4),
        )
        .await,
        "later full-put checkpoint",
    );
    assert_eq!(fifth.revision, Some(5));

    for (index, expected) in [&created, &second, &third, &fourth, &fifth]
        .iter()
        .enumerate()
    {
        let revision = index as u64 + 1;
        let replayed = expect_ok(
            port.get_entry_at_revision("ke_gate", revision).await,
            "replay an assigned revision",
        );
        assert_eq!(replayed.revision, Some(revision));
        assert_eq!(
            as_json(&replayed),
            as_json(expected),
            "revision {revision} replays the DTO it observed"
        );
    }

    // Head truncation: each replay carries only the records committed at that revision.
    let at_three = expect_ok(
        port.get_entry_at_revision("ke_gate", 3).await,
        "replay revision 3",
    );
    assert_eq!(record_ids(&at_three), ["cog_first"]);
    let at_four = expect_ok(
        port.get_entry_at_revision("ke_gate", 4).await,
        "replay revision 4",
    );
    assert_eq!(record_ids(&at_four), ["cog_first", "cog_second"]);

    // Replaying the live revision agrees with the current read.
    let current = expect_ok(port.get_knowledge_entry("ke_gate").await, "current");
    let replayed_current = expect_ok(
        port.get_entry_at_revision("ke_gate", 5).await,
        "current replay",
    );
    assert_eq!(as_json(&replayed_current), as_json(&current));

    let saved_revisions = revision_rows(&writer, "ke_gate").await;
    let saved_records = record_rows(&writer, "ke_gate").await;
    for revision in 1..=5 {
        expect_ok(
            port.get_entry_at_revision("ke_gate", revision).await,
            "replay again",
        );
    }
    assert_eq!(
        revision_rows(&writer, "ke_gate").await,
        saved_revisions,
        "replays never rewrite saved preimages"
    );
    assert_eq!(
        record_rows(&writer, "ke_gate").await,
        saved_records,
        "replays never rewrite committed records"
    );

    writer.close().await;
    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn an_absent_revision_answers_like_a_missing_entry() {
    let path = database_path("replay-absent");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    expect_ok(
        port.put_knowledge_entry(
            cognition_entry("ke_gate", "closed after dusk", Vec::new()),
            None,
        )
        .await,
        "create",
    );

    for (entry_id, revision) in [
        ("ke_gate", 0),
        ("ke_gate", 2),
        ("ke_gate", i64::MAX as u64 + 1),
        ("ke_missing", 1),
    ] {
        expect_revision_not_found(
            port.get_entry_at_revision(entry_id, revision).await,
            "an absent revision answers like a missing entry",
        );
    }

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn replay_reverses_the_first_append_on_a_module_less_entry() {
    let path = database_path("replay-shape-one");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let writer = second_connection(&path).await;

    let created = expect_ok(
        port.put_knowledge_entry(entry_with("ke_gate", "closed after dusk"), None)
            .await,
        "create a module-less entry",
    );
    assert!(module_of(&created).is_none());
    assert_eq!(append(&writer, "ke_gate", 1, "cog_first").await, 2);

    let replayed = expect_ok(
        port.get_entry_at_revision("ke_gate", 1).await,
        "replay before the first append",
    );
    assert_eq!(as_json(&replayed), as_json(&created));
    assert!(
        module_of(&replayed).is_none(),
        "the module the first append seeded is removed again"
    );

    let current = expect_ok(
        port.get_entry_at_revision("ke_gate", 2).await,
        "replay after the first append",
    );
    assert_eq!(record_ids(&current), ["cog_first"]);

    writer.close().await;
    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn replay_keeps_the_other_members_when_the_first_append_added_records() {
    let path = database_path("replay-shape-two");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let writer = second_connection(&path).await;

    // The write path rejects a cognition module without `records`, so only that pre-append
    // envelope is seeded; the record row and its shape come from the real append path.
    let world_facts = json!([{"kind": "world_fact", "world_fact_id": "wf_gate"}]);
    let extensions = json!({"mindcore_test": {"viewpoint": "studio"}});
    let mut live = entry_with("ke_gate", "closed after dusk");
    live.revision = Some(1);
    live.modules.insert(
        cognition_module_key(),
        cognition_module_value(json!({
            "world_facts": world_facts,
            "extensions": extensions,
        })),
    );
    sqlx::query(
        "INSERT INTO knowledge_entries \
         (collection, entry_id, revision, payload, cognition_head_seq, created_at, updated_at) \
         VALUES ('wren', 'ke_gate', 1, ?, NULL, '2026-11-21T00:00:00Z', '2026-11-21T00:00:00Z')",
    )
    .bind(serde_json::to_string(&live).expect("entry serializes"))
    .execute(&writer)
    .await
    .expect("seed the pre-append entry");

    assert_eq!(append(&writer, "ke_gate", 1, "cog_first").await, 2);
    assert_eq!(
        stored_shape(&writer, "ke_gate", 1).await,
        Some(2),
        "the append classifies the module it found without records"
    );

    let replayed = expect_ok(
        port.get_entry_at_revision("ke_gate", 1).await,
        "replay before the first append",
    );
    assert_eq!(replayed.revision, Some(1));
    let module = module_of(&replayed).expect("the module survives");
    assert!(
        module.get("records").is_none(),
        "only the records member the append added is removed"
    );
    assert_eq!(
        module.get("world_facts"),
        Some(&world_facts),
        "the other members are kept"
    );
    assert_eq!(
        module.get("extensions"),
        Some(&extensions),
        "the extensions member is kept"
    );

    let current = expect_ok(
        port.get_entry_at_revision("ke_gate", 2).await,
        "replay the live revision",
    );
    assert_eq!(record_ids(&current), ["cog_first"]);

    writer.close().await;
    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn replay_refuses_a_gap_its_append_metadata_contradicts() {
    let path = database_path("replay-contradicted-gap");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let seed = second_connection(&path).await;

    // Live revision 3 with head 2 and no saved revision 2: revision 2 is reachable only through a
    // dedicated append at position 2, yet the row there was committed by a full put, which always
    // saves its preimage. The store contradicts itself, so the gap is not replayable.
    let mut live = cognition_entry("ke_gate", "current", Vec::new());
    live.revision = Some(3);
    sqlx::query(
        "INSERT INTO knowledge_entries \
         (collection, entry_id, revision, payload, cognition_head_seq, created_at, updated_at) \
         VALUES ('wren', 'ke_gate', 3, ?, NULL, '2026-11-21T00:00:00Z', '2026-11-21T00:00:00Z')",
    )
    .bind(serde_json::to_string(&live).expect("entry serializes"))
    .execute(&seed)
    .await
    .expect("seed the live entry");
    for (append_seq, record_id, shape) in [(1, "cog_first", Some(0)), (2, "cog_second", None)] {
        sqlx::query(
            "INSERT INTO cognition_records \
             (collection, entry_id, append_seq, record_id, payload, append_prior_shape) \
             VALUES ('wren', 'ke_gate', ?, ?, ?, ?)",
        )
        .bind(append_seq)
        .bind(record_id)
        .bind(serde_json::to_string(&belief("ke_gate", record_id)).expect("record serializes"))
        .bind(shape)
        .execute(&seed)
        .await
        .expect("seed the record row");
    }
    sqlx::query(
        "UPDATE knowledge_entries SET cognition_head_seq = 2 \
         WHERE collection = 'wren' AND entry_id = 'ke_gate'",
    )
    .execute(&seed)
    .await
    .expect("point the head at the records");

    assert_eq!(
        expect_reject(
            port.get_entry_at_revision("ke_gate", 2).await,
            "replay across a contradicted gap"
        ),
        SpokeRejectCode::InternalError
    );

    seed.close().await;
    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn replay_refuses_a_fractional_position_masking_a_missing_one() {
    let path = database_path("replay-fractional-gap");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let seed = second_connection(&path).await;

    // A gap of two that a count accepts: position 2 is missing, yet a classified fractional row at
    // 1.5 sits inside the crossed interval alongside position 3, which the head points at. The
    // positions, not their number, decide whether the interval is a run of dedicated appends.
    let mut live = cognition_entry("ke_gate", "current", Vec::new());
    live.revision = Some(4);
    sqlx::query(
        "INSERT INTO knowledge_entries \
         (collection, entry_id, revision, payload, cognition_head_seq, created_at, updated_at) \
         VALUES ('wren', 'ke_gate', 4, ?, NULL, '2026-11-21T00:00:00Z', '2026-11-21T00:00:00Z')",
    )
    .bind(serde_json::to_string(&live).expect("entry serializes"))
    .execute(&seed)
    .await
    .expect("seed the live entry");
    sqlx::query(
        "INSERT INTO cognition_records \
         (collection, entry_id, append_seq, record_id, payload, append_prior_shape) \
         VALUES ('wren', 'ke_gate', ?, ?, ?, 0)",
    )
    .bind(1.5f64)
    .bind("cog_fractional")
    .bind(serde_json::to_string(&belief("ke_gate", "cog_fractional")).expect("record serializes"))
    .execute(&seed)
    .await
    .expect("seed the fractional record row");
    sqlx::query(
        "INSERT INTO cognition_records \
         (collection, entry_id, append_seq, record_id, payload, append_prior_shape) \
         VALUES ('wren', 'ke_gate', ?, ?, ?, 0)",
    )
    .bind(3i64)
    .bind("cog_integral")
    .bind(serde_json::to_string(&belief("ke_gate", "cog_integral")).expect("record serializes"))
    .execute(&seed)
    .await
    .expect("seed the record row the head points at");
    sqlx::query(
        "UPDATE knowledge_entries SET cognition_head_seq = 3 \
         WHERE collection = 'wren' AND entry_id = 'ke_gate'",
    )
    .execute(&seed)
    .await
    .expect("point the head at the integral position");

    assert_eq!(
        expect_reject(
            port.get_entry_at_revision("ke_gate", 2).await,
            "replay across a fractional position"
        ),
        SpokeRejectCode::InternalError
    );

    seed.close().await;
    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn an_exhausted_revision_space_is_refused_before_any_write() {
    let path = database_path("revision-exhausted");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let writer = second_connection(&path).await;

    expect_ok(
        port.put_knowledge_entry(cognition_entry("ke_gate", "first", Vec::new()), None)
            .await,
        "create",
    );
    // The last storable revision: the next one is not representable in the revision column.
    sqlx::query(
        "UPDATE knowledge_entries SET revision = ? \
         WHERE collection = 'wren' AND entry_id = 'ke_gate'",
    )
    .bind(i64::MAX)
    .execute(&writer)
    .await
    .expect("seed the last storable revision");
    let preimages = revision_rows(&writer, "ke_gate").await;
    let records = record_rows(&writer, "ke_gate").await;

    let full_put = port
        .put_knowledge_entry(
            cognition_entry(
                "ke_gate",
                "unrepresentable",
                vec![belief("ke_gate", "cog_beyond")],
            ),
            Some(i64::MAX as u64),
        )
        .await;
    assert_eq!(
        expect_reject(full_put, "full put at revision exhaustion"),
        SpokeRejectCode::InternalError
    );

    let mut tx = writer
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("begin append");
    let appended = append_cognition(
        &mut tx,
        "wren",
        "ke_gate",
        i64::MAX as u64,
        &belief("ke_gate", "cog_beyond"),
    )
    .await;
    assert_eq!(
        expect_reject(appended, "append at revision exhaustion"),
        SpokeRejectCode::InternalError
    );
    tx.rollback().await.expect("roll back the refused append");

    let stored: i64 = sqlx::query_scalar(
        "SELECT revision FROM knowledge_entries WHERE collection = 'wren' AND entry_id = 'ke_gate'",
    )
    .fetch_one(&writer)
    .await
    .expect("stored revision");
    assert_eq!(stored, i64::MAX, "a refused write leaves the revision");
    assert_eq!(
        revision_rows(&writer, "ke_gate").await,
        preimages,
        "a refused write saves no preimage"
    );
    assert_eq!(
        record_rows(&writer, "ke_gate").await,
        records,
        "a refused write inserts no record row"
    );

    writer.close().await;
    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn deleting_a_headless_parent_with_history_is_refused() {
    let path = database_path("revision-parent-delete");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let enforcing = foreign_keys_connection(&path).await;

    // A headless entry: no record row references it, so only the preimage key can refuse the
    // delete — the non-cascading foreign key D11 declares.
    expect_ok(
        port.put_knowledge_entry(entry_with("ke_gate", "first"), None)
            .await,
        "create",
    );
    expect_ok(
        port.put_knowledge_entry(entry_with("ke_gate", "second"), Some(1))
            .await,
        "second revision",
    );
    let saved = revision_rows(&enforcing, "ke_gate").await;
    assert_eq!(saved.len(), 1, "the update saved one preimage");

    let deleted = sqlx::query(
        "DELETE FROM knowledge_entries WHERE collection = 'wren' AND entry_id = 'ke_gate'",
    )
    .execute(&enforcing)
    .await;
    assert!(
        deleted.is_err(),
        "the non-cascading parent key refuses the delete"
    );
    assert_eq!(
        revision_rows(&enforcing, "ke_gate").await,
        saved,
        "the preimage survives the refused delete"
    );

    enforcing.close().await;
    store.close().await;
    remove_database(&path);
}
