//! Behaviour of the scoped knowledge port over real SQLite files.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mindcore_base::cognition::CARRIER_NAMESPACE;
use mindcore_base::knowledge::{adoption_anchor, KnowledgeScope};
use mindcore_spi::{adopt_knowledge_entry, AdoptionOutcome};
use mindcore_sqlite::{
    append_cognition, write_entry, MindStore, ScopedKnowledgeStore, SERVED_KNOWLEDGE_LIST_LIMIT,
};
use serde_json::{json, Map, Value};
use spoke_operations::{KnowledgeEntryPort, SpokeRejectCode, SpokeResult};
use spoke_schemas::knowledge_entry::{
    ExtensionKey, KnowledgeEntryBody, KnowledgeEntryCanonicalName, KnowledgeEntryDisclosure,
    KnowledgeEntryModulesKey, KnowledgeEntryModulesValue, KnowledgeEntryOwner, Shared3,
    SourceAnchor as EntrySourceAnchor,
};
use spoke_schemas::source_anchor::ExtensionKey as AnchorExtensionKey;
use spoke_schemas::KnowledgeEntry;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use tokio::sync::Barrier;

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

/// One marking through the local CAS operation.
async fn supersede(
    port: &ScopedKnowledgeStore,
    entry_id: &str,
    expected: u64,
    target: &str,
) -> SpokeResult<u64> {
    port.supersede_entry(entry_id, expected, target).await
}

async fn stored_payload(connection: &SqlitePool, entry_id: &str) -> String {
    sqlx::query_scalar(
        "SELECT payload FROM knowledge_entries WHERE collection = 'wren' AND entry_id = ?",
    )
    .bind(entry_id)
    .fetch_one(connection)
    .await
    .expect("stored payload")
}

async fn stored_marker(connection: &SqlitePool, entry_id: &str) -> Option<String> {
    sqlx::query_scalar(
        "SELECT superseded_by FROM knowledge_entries WHERE collection = 'wren' AND entry_id = ?",
    )
    .bind(entry_id)
    .fetch_one(connection)
    .await
    .expect("stored marker")
}

async fn stored_revision(connection: &SqlitePool, entry_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT revision FROM knowledge_entries WHERE collection = 'wren' AND entry_id = ?",
    )
    .bind(entry_id)
    .fetch_one(connection)
    .await
    .expect("stored revision")
}

/// Everything a refused write must leave untouched: the payload, the marker, the revision, the
/// preimages and the record rows.
async fn stored_state(
    connection: &SqlitePool,
    entry_id: &str,
) -> (
    String,
    Option<String>,
    i64,
    Vec<(i64, String, Option<i64>, String)>,
    Vec<(i64, String, String, Option<i64>)>,
) {
    (
        stored_payload(connection, entry_id).await,
        stored_marker(connection, entry_id).await,
        stored_revision(connection, entry_id).await,
        revision_rows(connection, entry_id).await,
        record_rows(connection, entry_id).await,
    )
}

/// An owner-private entry: the viewpoint-free disclosure predicate hides it.
fn private_entry(entry_id: &str, summary: &str) -> KnowledgeEntry {
    let mut entry = entry_with(entry_id, summary);
    entry.disclosure =
        Some(KnowledgeEntryDisclosure::from_str("owner-private").expect("non-empty disclosure"));
    entry.owner = Some(KnowledgeEntryOwner::try_from("kb_mira").expect("non-empty owner"));
    entry
}

/// An entry carrying an adoption anchor of the pinned wire shape; `revision` adds the versioned
/// `source_revision` member.
fn adopted_entry(entry_id: &str, shared_id: &str, revision: Option<u64>) -> KnowledgeEntry {
    let mut anchor = adoption_anchor(shared_id, "hearsay");
    if let Some(revision) = revision {
        anchor
            .extensions
            .get_mut(&AnchorExtensionKey::try_from("mindcore_adoption").expect("valid key"))
            .expect("the adoption anchor carries its namespace")
            .insert("source_revision".to_string(), json!(revision));
    }
    let anchor: EntrySourceAnchor =
        serde_json::from_value(serde_json::to_value(anchor).expect("anchor serializes"))
            .expect("anchor wire shape matches the knowledge_entry copy");
    let mut entry = entry_with(entry_id, "adopted copy");
    entry.source_anchor = Some(anchor);
    entry
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

#[tokio::test]
async fn supersede_marks_only_the_source_and_saves_its_preimage() {
    let path = database_path("supersede-mark");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let reader = second_connection(&path).await;

    expect_ok(
        port.put_knowledge_entry(
            cognition_entry(
                "ke_old_gate",
                "the gate was closed",
                vec![belief("ke_old_gate", "cog_gate")],
            ),
            None,
        )
        .await,
        "create the source",
    );
    expect_ok(
        port.put_knowledge_entry(entry_with("ke_new_gate", "the gate is open"), None)
            .await,
        "create the target",
    );
    let source_payload = stored_payload(&reader, "ke_old_gate").await;
    let source_written_at = stored_updated_at(&reader, "ke_old_gate").await;
    let target_payload = stored_payload(&reader, "ke_new_gate").await;
    let records = record_rows(&reader, "ke_old_gate").await;

    let assigned = expect_ok(
        supersede(&port, "ke_old_gate", 1, "ke_new_gate").await,
        "mark the source superseded",
    );
    assert_eq!(assigned, 2, "marking advances the source revision");
    assert_eq!(
        stored_marker(&reader, "ke_old_gate").await.as_deref(),
        Some("ke_new_gate")
    );
    assert_eq!(stored_revision(&reader, "ke_old_gate").await, 2);

    // Only the marker, the revision and the store instant move: the payload keeps every field with
    // the advanced DTO revision, and the cognition history is untouched.
    let marked: KnowledgeEntry =
        serde_json::from_str(&stored_payload(&reader, "ke_old_gate").await)
            .expect("the marked payload is an entry");
    assert_eq!(marked.revision, Some(2));
    assert_eq!(summary_of(&marked), Some("the gate was closed"));
    assert!(
        record_ids(&marked).is_empty(),
        "the payload keeps the envelope only"
    );
    assert_eq!(record_rows(&reader, "ke_old_gate").await, records);
    assert!(stored_updated_at(&reader, "ke_old_gate").await > source_written_at);

    // The revision trigger captures the displaced preimage, exactly as it does for a full put.
    let saved = revision_rows(&reader, "ke_old_gate").await;
    assert_eq!(saved.len(), 1, "marking saves one preimage");
    assert_eq!(saved[0].0, 1);
    assert_eq!(
        saved[0].1, source_payload,
        "the preimage is the replaced payload"
    );
    assert_eq!(saved[0].2, Some(1), "the preimage keeps the cognition head");
    assert_eq!(
        saved[0].3, source_written_at,
        "the preimage carries the instant the replaced state was written"
    );

    // The target is not written at all.
    assert_eq!(stored_marker(&reader, "ke_new_gate").await, None);
    assert_eq!(stored_revision(&reader, "ke_new_gate").await, 1);
    assert_eq!(stored_payload(&reader, "ke_new_gate").await, target_payload);
    assert!(revision_rows(&reader, "ke_new_gate").await.is_empty());

    store.close().await;
    reader.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn supersede_refusals_change_neither_row() {
    let path = database_path("supersede-refusals");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let studio = store.knowledge(KnowledgeScope::new("studio"));
    let reader = second_connection(&path).await;

    for entry_id in ["ke_source", "ke_target", "ke_retired"] {
        expect_ok(
            port.put_knowledge_entry(entry_with(entry_id, entry_id), None)
                .await,
            "seed the served collection",
        );
    }
    expect_ok(
        studio
            .put_knowledge_entry(entry_with("ke_studio", "elsewhere"), None)
            .await,
        "seed the other collection",
    );
    expect_ok(
        supersede(&port, "ke_retired", 1, "ke_target").await,
        "mark the retired entry",
    );

    let source = stored_state(&reader, "ke_source").await;
    let target = stored_state(&reader, "ke_target").await;
    let retired = stored_state(&reader, "ke_retired").await;

    let stale = supersede(&port, "ke_source", 2, "ke_target").await;
    assert_eq!(
        expect_reject(stale, "stale expected revision"),
        SpokeRejectCode::StoredRevisionStale
    );
    let missing = supersede(&port, "ke_absent", 1, "ke_target").await;
    assert_eq!(
        expect_reject(missing, "absent source"),
        SpokeRejectCode::KnowledgeEntryNotFound
    );
    let foreign = supersede(&port, "ke_source", 1, "ke_studio").await;
    assert_eq!(
        expect_reject(foreign, "successor outside the collection"),
        SpokeRejectCode::KnowledgeEntryNotFound
    );
    let itself = supersede(&port, "ke_source", 1, "ke_source").await;
    assert_eq!(
        expect_reject(itself, "self reference"),
        SpokeRejectCode::InvalidInput
    );
    let remark = supersede(&port, "ke_retired", 2, "ke_target").await;
    assert_eq!(
        expect_reject(remark, "already marked source"),
        SpokeRejectCode::InvalidInput
    );
    let retired_target = supersede(&port, "ke_source", 1, "ke_retired").await;
    assert_eq!(
        expect_reject(retired_target, "marked successor"),
        SpokeRejectCode::InvalidInput
    );

    assert_eq!(stored_state(&reader, "ke_source").await, source);
    assert_eq!(stored_state(&reader, "ke_target").await, target);
    assert_eq!(stored_state(&reader, "ke_retired").await, retired);

    // A cycle cannot close on write: once the source is marked, the reverse names it as a target.
    expect_ok(
        supersede(&port, "ke_source", 1, "ke_target").await,
        "mark the source",
    );
    let cycle = supersede(&port, "ke_target", 1, "ke_source").await;
    assert_eq!(
        expect_reject(cycle, "cycle-closing successor"),
        SpokeRejectCode::InvalidInput
    );
    assert_eq!(stored_marker(&reader, "ke_target").await, None);
    assert_eq!(stored_revision(&reader, "ke_target").await, 1);

    store.close().await;
    reader.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn a_superseded_entry_refuses_puts_and_appends() {
    let path = database_path("supersede-writes");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let reader = second_connection(&path).await;

    expect_ok(
        port.put_knowledge_entry(
            cognition_entry(
                "ke_source",
                "the gate was closed",
                vec![belief("ke_source", "cog_gate")],
            ),
            None,
        )
        .await,
        "create the source",
    );
    expect_ok(
        port.put_knowledge_entry(entry_with("ke_target", "the gate is open"), None)
            .await,
        "create the target",
    );
    expect_ok(
        supersede(&port, "ke_source", 1, "ke_target").await,
        "mark the source",
    );
    let marked = stored_state(&reader, "ke_source").await;

    let conditional = port
        .put_knowledge_entry(
            cognition_entry(
                "ke_source",
                "rewritten",
                vec![
                    belief("ke_source", "cog_gate"),
                    belief("ke_source", "cog_after"),
                ],
            ),
            Some(2),
        )
        .await;
    assert_eq!(
        expect_reject(conditional, "conditional put of a marked row"),
        SpokeRejectCode::InvalidInput
    );
    let create = port
        .put_knowledge_entry(entry_with("ke_source", "created again"), None)
        .await;
    assert_eq!(
        expect_reject(create, "create-only put of a marked row"),
        SpokeRejectCode::InvalidInput
    );

    // A dedicated append refuses the marked carrier before any record row is written.
    let mut tx = reader
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("begin append");
    let appended = append_cognition(
        &mut tx,
        "wren",
        "ke_source",
        2,
        &belief("ke_source", "cog_after"),
    )
    .await;
    assert_eq!(
        expect_reject(appended, "append to a marked carrier"),
        SpokeRejectCode::InvalidInput
    );
    tx.rollback().await.expect("roll back the refused append");

    assert_eq!(
        stored_state(&reader, "ke_source").await,
        marked,
        "a refused write changes no row, record or preimage"
    );

    store.close().await;
    reader.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn a_failed_supersede_leaves_no_marker() {
    let path = database_path("supersede-rollback");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let reader = second_connection(&path).await;

    expect_ok(
        port.put_knowledge_entry(entry_with("ke_source", "the gate was closed"), None)
            .await,
        "create the source",
    );
    expect_ok(
        port.put_knowledge_entry(entry_with("ke_target", "the gate is open"), None)
            .await,
        "create the target",
    );
    // A preimage already occupies the revision the trigger would save, so the update fails after
    // both ids were accepted — the one failure that runs past every refusal.
    sqlx::query(
        "INSERT INTO knowledge_entry_revisions \
         (collection, entry_id, revision, payload, cognition_head_seq, recorded_at) \
         VALUES ('wren', 'ke_source', 1, '{}', NULL, '2026-11-20T09:00:00.000000000Z')",
    )
    .execute(&reader)
    .await
    .expect("seed a contradictory preimage");

    let before = stored_state(&reader, "ke_source").await;
    let failed = supersede(&port, "ke_source", 1, "ke_target").await;
    assert_eq!(
        expect_reject(failed, "marking against a contradictory history"),
        SpokeRejectCode::InternalError
    );
    assert_eq!(
        stored_state(&reader, "ke_source").await,
        before,
        "the failed marking rolls back"
    );
    assert_eq!(stored_marker(&reader, "ke_source").await, None);

    store.close().await;
    reader.close().await;
    remove_database(&path);
}

/// The CAS reads the stored revision column: a payload that disagrees with it is corrupt stored
/// state and refuses before any write, so the marking cannot skip the trigger's preimage.
#[tokio::test]
async fn the_supersede_cas_reads_the_stored_revision_column() {
    let path = database_path("supersede-drift");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let reader = second_connection(&path).await;

    expect_ok(
        port.put_knowledge_entry(entry_with("ke_target", "the gate is open"), None)
            .await,
        "create the target",
    );
    // Stored drift: the column sits at revision 2 while the payload still claims revision 1.
    let mut drifted = entry_with("ke_drift", "the gate was closed");
    drifted.revision = Some(1);
    let payload = serde_json::to_string(&drifted).expect("entry serializes");
    sqlx::query(
        "INSERT INTO knowledge_entries \
         (collection, entry_id, revision, payload, created_at, updated_at) \
         VALUES ('wren', 'ke_drift', 2, ?, \
                 '2026-11-20T09:00:00.000000000Z', '2026-11-20T09:00:00.000000000Z')",
    )
    .bind(&payload)
    .execute(&reader)
    .await
    .expect("seed a drifted row");

    let before = stored_state(&reader, "ke_drift").await;
    let refused = supersede(&port, "ke_drift", 1, "ke_target").await;
    assert_eq!(
        expect_reject(refused, "stored revision column and payload disagree"),
        SpokeRejectCode::InternalError
    );
    assert_eq!(
        stored_state(&reader, "ke_drift").await,
        before,
        "a refused marking changes nothing"
    );
    assert_eq!(stored_marker(&reader, "ke_drift").await, None);
    assert_eq!(
        stored_revision(&reader, "ke_drift").await,
        2,
        "the column stays as stored"
    );
    assert!(
        revision_rows(&reader, "ke_drift").await.is_empty(),
        "the refused marking saves no preimage"
    );

    store.close().await;
    reader.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn default_reads_and_the_list_hide_a_superseded_entry() {
    let path = database_path("supersede-visibility");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let reader = second_connection(&path).await;

    expect_ok(
        port.put_knowledge_entry(entry_with("ke_alpha", "north gate"), None)
            .await,
        "seed the live successor",
    );
    expect_ok(
        port.put_knowledge_entry(
            cognition_entry(
                "ke_mid",
                "old north gate notes",
                vec![belief("ke_mid", "cog_mid")],
            ),
            None,
        )
        .await,
        "seed the superseded entry",
    );
    expect_ok(
        port.put_knowledge_entry(entry_with("ke_zulu", "mill lane"), None)
            .await,
        "seed the later entry",
    );
    expect_ok(
        supersede(&port, "ke_mid", 1, "ke_alpha").await,
        "mark the middle entry superseded",
    );

    // The default read hides the marked row behind the one local absence.
    match port.get_knowledge_entry("ke_mid").await {
        SpokeResult::Ok(value) => {
            panic!("default read of a superseded entry returned {value:?}")
        }
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::KnowledgeEntryNotFound);
            assert_eq!(
                reject.message,
                "no knowledge entry ke_mid in collection wren"
            );
            assert!(reject.details.is_none());
        }
    }

    // The list keeps its order and statement shape and simply omits the marked row.
    let listed = expect_ok(port.list_entries().await, "list with one marked entry");
    let ids: Vec<&str> = listed.iter().map(|entry| entry.entry_id.as_str()).collect();
    assert_eq!(ids, ["ke_alpha", "ke_zulu"]);
    assert_eq!(record_ids(&listed[0]), Vec::<String>::new());
    assert_eq!(
        expect_ok(
            port.get_knowledge_entry("ke_alpha").await,
            "live entry read"
        )
        .revision,
        Some(1)
    );
    assert_eq!(
        summary_of(&expect_ok(
            port.get_knowledge_entry("ke_alpha").await,
            "live entry read"
        )),
        Some("north gate")
    );

    // The shared transaction projection and the carrier header read are the same default view.
    let mut tx = reader.begin().await.expect("begin read");
    let shared = expect_ok(
        mindcore_sqlite::list_entries_in(&mut tx, "wren").await,
        "shared list projection",
    );
    tx.commit().await.expect("commit read");
    assert_eq!(
        shared
            .iter()
            .map(|entry| entry.entry_id.as_str())
            .collect::<Vec<_>>(),
        vec!["ke_alpha", "ke_zulu"]
    );
    let header = mindcore_sqlite::read_carrier_header(&reader, "wren", "ke_mid").await;
    assert_eq!(
        expect_reject(header, "carrier header of a superseded entry"),
        SpokeRejectCode::KnowledgeEntryNotFound
    );
    let live = expect_ok(
        mindcore_sqlite::read_carrier_header(&reader, "wren", "ke_alpha").await,
        "carrier header of a live entry",
    );
    assert_eq!(live.revision, 1);

    // Explicit revision replay stays independent: the history it addresses does not move.
    let replayed = expect_ok(
        port.get_entry_at_revision("ke_mid", 1).await,
        "replay of the superseded entry",
    );
    assert_eq!(summary_of(&replayed), Some("old north gate notes"));
    assert_eq!(record_ids(&replayed), vec!["cog_mid"]);

    store.close().await;
    reader.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn the_explicit_read_returns_a_superseded_entry_with_its_marker() {
    let path = database_path("supersede-explicit");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    expect_ok(
        port.put_knowledge_entry(entry_with("ke_alpha", "north gate"), None)
            .await,
        "seed the live successor",
    );
    expect_ok(
        port.put_knowledge_entry(
            cognition_entry(
                "ke_mid",
                "old north gate notes",
                vec![belief("ke_mid", "cog_mid")],
            ),
            None,
        )
        .await,
        "seed the superseded entry",
    );
    expect_ok(
        supersede(&port, "ke_mid", 1, "ke_alpha").await,
        "mark the middle entry superseded",
    );

    let marked = expect_ok(
        port.get_entry_including_superseded("ke_mid").await,
        "explicit read of a superseded entry",
    );
    assert_eq!(marked.superseded_by.as_deref(), Some("ke_alpha"));
    assert_eq!(marked.entry.revision, Some(2));
    assert_eq!(summary_of(&marked.entry), Some("old north gate notes"));
    assert_eq!(
        record_ids(&marked.entry),
        vec!["cog_mid"],
        "the explicit read projects the stored records"
    );

    let live = expect_ok(
        port.get_entry_including_superseded("ke_alpha").await,
        "explicit read of a live entry",
    );
    assert_eq!(live.superseded_by, None);
    assert_eq!(live.entry.revision, Some(1));

    match port.get_entry_including_superseded("ke_absent").await {
        SpokeResult::Ok(value) => panic!("explicit read of a missing entry returned {value:?}"),
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::KnowledgeEntryNotFound);
            assert_eq!(
                reject.message,
                "no knowledge entry ke_absent in collection wren"
            );
            assert!(reject.details.is_none());
        }
    }

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn an_adoption_anchor_resolves_to_the_first_live_successor() {
    let path = database_path("supersede-anchor");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    for entry_id in ["ke_stage_one", "ke_stage_two", "ke_stage_three"] {
        expect_ok(
            port.put_knowledge_entry(entry_with(entry_id, entry_id), None)
                .await,
            "seed the chain",
        );
    }
    expect_ok(
        supersede(&port, "ke_stage_one", 1, "ke_stage_two").await,
        "retire the first stage",
    );
    expect_ok(
        supersede(&port, "ke_stage_two", 1, "ke_stage_three").await,
        "retire the second stage",
    );

    let one_hop = adopted_entry("ke_mine_one_hop", "ke_stage_two", None);
    assert_eq!(
        expect_ok(
            port.resolve_adoption_anchor(&one_hop).await,
            "an anchor one hop from a live entry"
        ),
        "ke_stage_three"
    );
    let two_hops = adopted_entry("ke_mine_two_hops", "ke_stage_one", None);
    assert_eq!(
        expect_ok(
            port.resolve_adoption_anchor(&two_hops).await,
            "a legacy anchor two hops from a live entry"
        ),
        "ke_stage_three"
    );
    let versioned = adopted_entry("ke_mine_versioned", "ke_stage_one", Some(3));
    assert_eq!(
        expect_ok(
            port.resolve_adoption_anchor(&versioned).await,
            "a versioned anchor two hops from a live entry"
        ),
        "ke_stage_three"
    );
    let current = adopted_entry("ke_mine_current", "ke_stage_three", None);
    assert_eq!(
        expect_ok(
            port.resolve_adoption_anchor(&current).await,
            "an anchor already on a live entry"
        ),
        "ke_stage_three"
    );

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn an_unresolvable_adoption_anchor_answers_one_absence() {
    let path = database_path("supersede-anchor-absence");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    expect_ok(
        port.put_knowledge_entry(entry_with("ke_chain", "a retired entry"), None)
            .await,
        "seed the retired entry",
    );
    expect_ok(
        port.put_knowledge_entry(private_entry("ke_private", "mira's private notes"), None)
            .await,
        "seed the owner-private entry",
    );
    expect_ok(
        supersede(&port, "ke_chain", 1, "ke_private").await,
        "retire the entry behind a private successor",
    );

    for (anchored_id, source, absence) in [
        ("ke_mine_gone", "ke_gone", "ke_gone"),
        ("ke_mine_hidden_start", "ke_private", "ke_private"),
        ("ke_mine_hidden_hop", "ke_chain", "ke_chain"),
    ] {
        let anchored = adopted_entry(anchored_id, source, None);
        match port.resolve_adoption_anchor(&anchored).await {
            SpokeResult::Ok(resolved) => {
                panic!("an unresolvable anchor answered {resolved}")
            }
            SpokeResult::Reject(reject) => {
                assert_eq!(reject.code, SpokeRejectCode::KnowledgeEntryNotFound);
                assert_eq!(
                    reject.message,
                    format!("no knowledge entry {absence} in collection wren"),
                    "the absence is keyed to the hop the anchor names"
                );
                assert!(
                    reject.details.is_none(),
                    "one generic absence carries no chain"
                );
            }
        }
    }

    let anonymous = entry_with("ke_mine_anonymous", "no anchor");
    assert_eq!(
        expect_reject(
            port.resolve_adoption_anchor(&anonymous).await,
            "an entry without an adoption anchor"
        ),
        SpokeRejectCode::InvalidInput
    );
    let empty_locator = adopted_entry("ke_mine_empty", "", None);
    assert_eq!(
        expect_reject(
            port.resolve_adoption_anchor(&empty_locator).await,
            "an anchor naming no shared entry"
        ),
        SpokeRejectCode::InvalidInput
    );

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn a_cycle_in_stored_markers_is_a_storage_failure() {
    let path = database_path("supersede-cycle");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let reader = second_connection(&path).await;

    for entry_id in ["ke_cycle_a", "ke_cycle_b"] {
        expect_ok(
            port.put_knowledge_entry(entry_with(entry_id, entry_id), None)
                .await,
            "seed the cyclic pair",
        );
    }
    // Marking cannot close a cycle; corrupt stored data can, and the resolver must fail rather
    // than return a partial chain.
    for (entry_id, successor) in [("ke_cycle_a", "ke_cycle_b"), ("ke_cycle_b", "ke_cycle_a")] {
        sqlx::query(
            "UPDATE knowledge_entries SET superseded_by = ? \
             WHERE collection = 'wren' AND entry_id = ?",
        )
        .bind(successor)
        .bind(entry_id)
        .execute(&reader)
        .await
        .expect("seed the cyclic marker");
    }
    let anchored = adopted_entry("ke_mine_cycle", "ke_cycle_a", None);
    match port.resolve_adoption_anchor(&anchored).await {
        SpokeResult::Ok(resolved) => panic!("a cyclic chain answered {resolved}"),
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::InternalError);
            assert!(
                reject.message.contains("cycle"),
                "the failure names the cycle: {}",
                reject.message
            );
        }
    }

    store.close().await;
    reader.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn the_list_bound_counts_superseded_rows() {
    let path = database_path("supersede-bound");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let reader = second_connection(&path).await;

    let mut tx = reader
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("begin seed");
    for index in 0..SERVED_KNOWLEDGE_LIST_LIMIT {
        expect_ok(
            write_entry(
                &mut tx,
                "wren",
                entry_with(&format!("ke_{index:04}"), "seeded"),
                None,
            )
            .await,
            "seed a live entry",
        );
    }
    tx.commit().await.expect("commit seed");

    expect_ok(
        supersede(&port, "ke_0000", 1, "ke_0001").await,
        "retire the first stored row",
    );
    let listed = expect_ok(
        port.list_entries().await,
        "a collection at the bound with one marked row",
    );
    assert_eq!(
        listed.len(),
        SERVED_KNOWLEDGE_LIST_LIMIT - 1,
        "the list serves the live entries only"
    );
    assert_eq!(listed[0].entry_id, "ke_0001", "the order is unchanged");

    // The bound stays on stored rows: one more row — live or marked — refuses the list.
    let mut tx = reader
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("begin seed");
    expect_ok(
        write_entry(
            &mut tx,
            "wren",
            entry_with("ke_over", "one row past the bound"),
            None,
        )
        .await,
        "seed one row past the bound",
    );
    tx.commit().await.expect("commit seed");
    assert_eq!(
        expect_reject(
            port.list_entries().await,
            "a collection one stored row past the bound"
        ),
        SpokeRejectCode::InternalError
    );

    store.close().await;
    reader.close().await;
    remove_database(&path);
}

/// The marker's foreign key itself: a successor must exist in the same collection, and a
/// referenced successor cannot be deleted. Removing the key or making it cascade fails this test.
#[tokio::test]
async fn the_marker_foreign_key_binds_successors_to_the_same_collection() {
    let path = database_path("supersede-foreign-key");
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let studio = store.knowledge(KnowledgeScope::new("studio"));
    let enforcing = foreign_keys_connection(&path).await;

    for entry_id in ["ke_source", "ke_target"] {
        expect_ok(
            port.put_knowledge_entry(entry_with(entry_id, entry_id), None)
                .await,
            "seed the served collection",
        );
    }
    expect_ok(
        studio
            .put_knowledge_entry(entry_with("ke_studio", "elsewhere"), None)
            .await,
        "seed the other collection",
    );

    // A direct write naming a successor that exists only in another collection fails.
    let foreign = sqlx::query(
        "UPDATE knowledge_entries SET superseded_by = 'ke_studio' \
         WHERE collection = 'wren' AND entry_id = 'ke_source'",
    )
    .execute(&enforcing)
    .await
    .expect_err("a cross-collection successor violates the foreign key");
    assert!(
        foreign
            .as_database_error()
            .is_some_and(|error| error.is_foreign_key_violation()),
        "{foreign}"
    );
    assert_eq!(stored_marker(&enforcing, "ke_source").await, None);

    // After a valid mark, the referenced successor cannot be deleted: no cascade removes it.
    expect_ok(
        supersede(&port, "ke_source", 1, "ke_target").await,
        "mark the source",
    );
    let deleted = sqlx::query(
        "DELETE FROM knowledge_entries WHERE collection = 'wren' AND entry_id = 'ke_target'",
    )
    .execute(&enforcing)
    .await
    .expect_err("deleting a referenced successor violates the foreign key");
    assert!(
        deleted
            .as_database_error()
            .is_some_and(|error| error.is_foreign_key_violation()),
        "{deleted}"
    );
    assert_eq!(
        stored_marker(&enforcing, "ke_source").await.as_deref(),
        Some("ke_target")
    );
    let rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM knowledge_entries \
         WHERE collection = 'wren' AND entry_id IN ('ke_source', 'ke_target')",
    )
    .fetch_one(&enforcing)
    .await
    .expect("both rows survive");
    assert_eq!(rows, 2);

    enforcing.close().await;
    store.close().await;
    remove_database(&path);
}

// ---------- supported adoption of a shared entry into a personal collection ----------

async fn stored_row(connection: &SqlitePool, collection: &str, entry_id: &str) -> (i64, String) {
    sqlx::query_as(
        "SELECT revision, payload FROM knowledge_entries \
         WHERE collection = ? AND entry_id = ?",
    )
    .bind(collection)
    .bind(entry_id)
    .fetch_one(connection)
    .await
    .expect("stored row")
}

fn adoption_anchor_json(entry: &KnowledgeEntry) -> Value {
    serde_json::to_value(
        entry
            .source_anchor
            .as_ref()
            .expect("the adopted copy carries its anchor"),
    )
    .expect("anchor serializes")
}

fn anchor_source_revision(entry: &KnowledgeEntry) -> Option<u64> {
    adoption_anchor_json(entry)["extensions"]["mindcore_adoption"]["source_revision"].as_u64()
}

/// A fixed instant, so a copy that inherited its source's timestamps would show one.
fn instant(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .expect("valid instant")
        .with_timezone(&Utc)
}

fn expect_source_absent(result: SpokeResult<AdoptionOutcome>, context: &str) {
    match result {
        SpokeResult::Ok(outcome) => panic!("{context}: unexpectedly adopted {outcome:?}"),
        SpokeResult::Reject(reject) => {
            assert_eq!(
                reject.code,
                SpokeRejectCode::KnowledgeEntryNotFound,
                "{context}"
            );
            assert_eq!(
                reject.message, "source knowledge entry not found",
                "{context}"
            );
            assert!(
                reject.details.is_none(),
                "{context}: a generic absence carries no details"
            );
        }
    }
}

#[tokio::test]
async fn adoption_writes_one_anchored_copy_and_returns_it_on_repeat() {
    let path = database_path("adopt-repeat");
    let store = MindStore::open(&path).await.expect("open store");
    let src = store.knowledge(KnowledgeScope::new("shared"));
    let dst = store.knowledge(KnowledgeScope::new("wren"));
    let reader = second_connection(&path).await;

    let shared_id = "ke_shared_north_gate";
    let mut shared = cognition_entry(
        shared_id,
        "The north gate is shut after dusk.",
        vec![belief(shared_id, "cog_shared_gate")],
    );
    shared.created_at = Some(instant("2026-11-01T09:00:00Z"));
    shared.updated_at = Some(Shared3(instant("2026-11-02T09:00:00Z")));
    let seeded = expect_ok(
        src.put_knowledge_entry(shared.clone(), None).await,
        "seed the shared source",
    );
    assert!(
        seeded.created_at.is_some() && seeded.updated_at.is_some(),
        "the source carries both timestamps, so the copy's cleared ones mean something"
    );
    let source_before = stored_row(&reader, "shared", shared_id).await;

    let created = match expect_ok(
        adopt_knowledge_entry(&src, &dst, shared_id, 1).await,
        "first adoption",
    ) {
        AdoptionOutcome::Created(created) => created,
        other => panic!("the first adoption must create the copy, got {other:?}"),
    };
    assert_eq!(created.entry_id, shared_id);
    assert_eq!(created.revision, Some(1));
    assert_eq!(created.created_at, None);
    assert!(created.updated_at.is_none());
    assert_eq!(created.entry_type, shared.entry_type);
    assert_eq!(created.status, shared.status);
    let anchor = adoption_anchor_json(&created);
    assert_eq!(anchor["source_id"], json!(format!("shared:{shared_id}")));
    assert_eq!(
        anchor["extensions"]["mindcore_adoption"]["basis"],
        json!("shared-entry")
    );
    assert_eq!(anchor_source_revision(&created), Some(1));
    assert!(
        module_of(&created).is_some_and(|module| module.contains_key("world_facts")),
        "the cognition envelope and its members survive"
    );
    assert!(
        record_ids(&created).is_empty(),
        "the source's judgments must not become personal committed records"
    );
    assert_eq!(
        module_of(&created).and_then(|module| module.get("records")),
        Some(&json!([])),
        "the copy's cognition history is an explicit empty array, not a dropped member"
    );

    // The source is only ever read: payload and revision are byte-identical afterwards.
    assert_eq!(
        stored_row(&reader, "shared", shared_id).await,
        source_before
    );

    // The source advances; both its old and its new visible revision name the same copy.
    let mut advanced = shared;
    advanced.body.summary = Some("The north gate reopens at dawn.".to_string());
    let advanced = expect_ok(
        src.put_knowledge_entry(advanced, Some(1)).await,
        "advance the source",
    );
    assert_eq!(advanced.revision, Some(2));

    for (expected, context) in [
        (1, "repeat at the adopted revision"),
        (2, "repeat at the current visible revision"),
    ] {
        let repeat = match expect_ok(
            adopt_knowledge_entry(&src, &dst, shared_id, expected).await,
            context,
        ) {
            AdoptionOutcome::AlreadyPresent(existing) => existing,
            other => panic!("{context}: expected the existing copy, got {other:?}"),
        };
        assert_eq!(as_json(&repeat), as_json(&created), "{context}");
        assert_eq!(anchor_source_revision(&repeat), Some(1), "{context}");
    }

    // A request naming neither revision is a conflict, never a second write.
    assert_eq!(
        expect_reject(
            adopt_knowledge_entry(&src, &dst, shared_id, 3).await,
            "a request naming neither revision",
        ),
        SpokeRejectCode::RevisionConflict
    );

    // No repeat refreshed or rewrote the copy, and only that one row exists.
    assert_eq!(stored_row(&reader, "wren", shared_id).await.0, 1);
    let rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM knowledge_entries WHERE collection = 'wren' AND entry_id = ?",
    )
    .bind(shared_id)
    .fetch_one(&reader)
    .await
    .expect("destination row count");
    assert_eq!(rows, 1);

    reader.close().await;
    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn adoption_answers_generic_absence_when_the_adopted_source_is_superseded() {
    let path = database_path("adopt-superseded-source");
    let store = MindStore::open(&path).await.expect("open store");
    let src = store.knowledge(KnowledgeScope::new("shared"));
    let dst = store.knowledge(KnowledgeScope::new("wren"));
    let reader = second_connection(&path).await;

    let shared_id = "ke_shared_gate";
    expect_ok(
        src.put_knowledge_entry(entry_with(shared_id, "shut after dusk"), None)
            .await,
        "seed the source",
    );
    let adopted = match expect_ok(
        adopt_knowledge_entry(&src, &dst, shared_id, 1).await,
        "first adoption",
    ) {
        AdoptionOutcome::Created(created) => created,
        other => panic!("the first adoption must create the copy, got {other:?}"),
    };

    expect_ok(
        src.put_knowledge_entry(entry_with("ke_shared_successor", "open again"), None)
            .await,
        "seed the successor",
    );
    expect_ok(
        supersede(&src, shared_id, 1, "ke_shared_successor").await,
        "mark the source superseded",
    );
    let copy_before = stored_row(&reader, "wren", shared_id).await;

    // The source gate runs before the repeat path: an adopted source that is now hidden answers
    // the same generic absence as a missing one, and the stored copy stays exactly as adopted.
    expect_source_absent(
        adopt_knowledge_entry(&src, &dst, shared_id, 1).await,
        "a repeat whose source turned superseded",
    );
    assert_eq!(
        stored_row(&reader, "wren", shared_id).await,
        copy_before,
        "the refused repeat leaves the adopted copy untouched"
    );
    assert_eq!(
        as_json(&expect_ok(
            dst.get_knowledge_entry(shared_id).await,
            "read the adopted copy back"
        )),
        as_json(&adopted)
    );

    reader.close().await;
    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn adoption_answers_one_generic_absence_for_missing_private_and_superseded_sources() {
    let path = database_path("adopt-absence");
    let store = MindStore::open(&path).await.expect("open store");
    let src = store.knowledge(KnowledgeScope::new("shared"));
    let dst = store.knowledge(KnowledgeScope::new("wren"));
    let reader = second_connection(&path).await;

    expect_source_absent(
        adopt_knowledge_entry(&src, &dst, "ke_absent", 1).await,
        "a missing source",
    );

    expect_ok(
        src.put_knowledge_entry(private_entry("ke_private", "wren only"), None)
            .await,
        "seed an owner-private source",
    );
    expect_source_absent(
        adopt_knowledge_entry(&src, &dst, "ke_private", 1).await,
        "an owner-private source",
    );

    for entry_id in ["ke_stage_one", "ke_stage_two"] {
        expect_ok(
            src.put_knowledge_entry(entry_with(entry_id, entry_id), None)
                .await,
            "seed the supersede pair",
        );
    }
    expect_ok(
        supersede(&src, "ke_stage_one", 1, "ke_stage_two").await,
        "mark the source superseded",
    );
    let superseded_before = stored_row(&reader, "shared", "ke_stage_one").await;
    expect_source_absent(
        adopt_knowledge_entry(&src, &dst, "ke_stage_one", 1).await,
        "a superseded source",
    );
    assert_eq!(
        stored_row(&reader, "shared", "ke_stage_one").await,
        superseded_before,
        "a refused adoption writes nothing"
    );

    assert!(
        expect_ok(dst.list_entries().await, "list the destination").is_empty(),
        "no refusal claims the destination slot"
    );

    reader.close().await;
    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn adoption_never_overwrites_a_destination_occupant_or_adopts_a_stale_revision() {
    let path = database_path("adopt-collision");
    let store = MindStore::open(&path).await.expect("open store");
    let src = store.knowledge(KnowledgeScope::new("shared"));
    let dst = store.knowledge(KnowledgeScope::new("wren"));
    let reader = second_connection(&path).await;

    expect_ok(
        src.put_knowledge_entry(entry_with("ke_shared_gate", "shared fact"), None)
            .await,
        "seed the source",
    );

    // An unrelated occupant of the exact key is a conflict, never overwritten.
    let occupant = expect_ok(
        dst.put_knowledge_entry(entry_with("ke_shared_gate", "wren's own note"), None)
            .await,
        "seed an unrelated occupant",
    );
    assert_eq!(
        expect_reject(
            adopt_knowledge_entry(&src, &dst, "ke_shared_gate", 1).await,
            "an unrelated occupant",
        ),
        SpokeRejectCode::KnowledgeEntryAlreadyExists
    );
    assert_eq!(
        as_json(&expect_ok(
            dst.get_knowledge_entry("ke_shared_gate").await,
            "read the occupant back"
        )),
        as_json(&occupant)
    );

    // A legacy anchor without a recorded revision is still not this adoption.
    expect_ok(
        src.put_knowledge_entry(entry_with("ke_legacy_gate", "shared fact"), None)
            .await,
        "seed the legacy source",
    );
    let legacy = expect_ok(
        dst.put_knowledge_entry(
            adopted_entry("ke_legacy_gate", "ke_legacy_gate", None),
            None,
        )
        .await,
        "seed a legacy occupant",
    );
    assert_eq!(
        expect_reject(
            adopt_knowledge_entry(&src, &dst, "ke_legacy_gate", 1).await,
            "a legacy anchor without a revision",
        ),
        SpokeRejectCode::KnowledgeEntryAlreadyExists
    );
    assert_eq!(
        as_json(&expect_ok(
            dst.get_knowledge_entry("ke_legacy_gate").await,
            "read the legacy occupant back"
        )),
        as_json(&legacy)
    );

    // An absent slot with a stale expectation is a revision conflict and stays empty.
    expect_ok(
        src.put_knowledge_entry(entry_with("ke_fresh_gate", "shared fact"), None)
            .await,
        "seed a fresh source",
    );
    assert_eq!(
        expect_reject(
            adopt_knowledge_entry(&src, &dst, "ke_fresh_gate", 5).await,
            "a stale expected revision",
        ),
        SpokeRejectCode::RevisionConflict
    );
    assert_eq!(
        expect_reject(
            dst.get_knowledge_entry("ke_fresh_gate").await,
            "the refused slot stays empty",
        ),
        SpokeRejectCode::KnowledgeEntryNotFound
    );

    reader.close().await;
    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn adoption_answers_occupancy_for_a_superseded_destination_occupant() {
    let path = database_path("adopt-superseded-occupant");
    let store = MindStore::open(&path).await.expect("open store");
    let src = store.knowledge(KnowledgeScope::new("shared"));
    let dst = store.knowledge(KnowledgeScope::new("wren"));
    let reader = second_connection(&path).await;

    expect_ok(
        src.put_knowledge_entry(entry_with("ke_shared_gate", "shared fact"), None)
            .await,
        "seed the source",
    );
    for entry_id in ["ke_shared_gate", "ke_wren_successor"] {
        expect_ok(
            dst.put_knowledge_entry(entry_with(entry_id, entry_id), None)
                .await,
            "seed the wren occupant pair",
        );
    }
    expect_ok(
        supersede(&dst, "ke_shared_gate", 1, "ke_wren_successor").await,
        "mark the wren occupant superseded",
    );
    let occupant_before = stored_state(&reader, "ke_shared_gate").await;

    // The live read hides the marked row, but the slot is occupied: a superseded occupant answers
    // the same occupancy refusal as any unrelated one, and it is never overwritten.
    match adopt_knowledge_entry(&src, &dst, "ke_shared_gate", 1).await {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::KnowledgeEntryAlreadyExists);
            assert_eq!(
                reject.message, "knowledge entry ke_shared_gate already exists",
                "the occupancy refusal carries no supersede diagnostic"
            );
            assert!(reject.details.is_none());
        }
        other => panic!("a superseded occupant must refuse the adoption, got {other:?}"),
    }
    assert_eq!(
        stored_state(&reader, "ke_shared_gate").await,
        occupant_before,
        "the superseded occupant keeps its marker, payload, revision, preimages and records"
    );

    reader.close().await;
    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn adoption_propagates_a_create_refusal_that_names_no_occupant() {
    let path = database_path("adopt-invalid-copy");
    let store = MindStore::open(&path).await.expect("open store");
    let dst = store.knowledge(KnowledgeScope::new("wren"));

    // A source whose cognition module is otherwise valid but whose `records` member is present and
    // not an array. Only a port double can hand adoption that shape: a stored row is read back with
    // `records` projected from the record rows. Only the narrowed branch leaves the member
    // untouched, so the destination gate refuses the copy and that plain `InvalidInput` must
    // surface as itself: only the adapter's structured superseded detail means occupancy. The
    // pre-fix unconditional replacement would mask the member as `[]`, the copy would validate, and
    // the adoption would commit — which this test rejects.
    let mut broken = cognition_entry("ke_broken_carrier", "shared fact", Vec::new());
    broken.revision = Some(1);
    let Some(KnowledgeEntryModulesValue::Object(module)) =
        broken.modules.get_mut(&cognition_module_key())
    else {
        panic!("the fixture entry carries an object cognition module")
    };
    module.insert("records".to_string(), Value::Null);
    let src = FixedSource(broken);

    match adopt_knowledge_entry(&src, &dst, "ke_broken_carrier", 1).await {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::InvalidInput);
            assert!(
                reject.details.is_none(),
                "a validation refusal is not the structured occupancy detail"
            );
        }
        other => panic!("an unwritable copy must refuse the adoption, got {other:?}"),
    }
    assert_eq!(
        expect_reject(
            dst.get_knowledge_entry("ke_broken_carrier").await,
            "the refused copy claims no destination slot",
        ),
        SpokeRejectCode::KnowledgeEntryNotFound,
        "the destination holds no adopted module, so neither `null` nor `[]` lands in the \
         non-array slot the source carries"
    );

    store.close().await;
    remove_database(&path);
}

/// A source port answering one fixed entry: a stored row is read back with
/// `modules.cognition.records` projected from its record rows, so only a double can hand adoption a
/// non-array member.
struct FixedSource(KnowledgeEntry);

#[async_trait]
impl KnowledgeEntryPort for FixedSource {
    async fn get_knowledge_entry(&self, _entry_id: &str) -> SpokeResult<KnowledgeEntry> {
        SpokeResult::Ok(self.0.clone())
    }

    async fn put_knowledge_entry(
        &self,
        _entry: KnowledgeEntry,
        _expected_base_revision: Option<u64>,
    ) -> SpokeResult<KnowledgeEntry> {
        panic!("the source is only ever read")
    }
}

/// A destination port that parks its first absent slot read until its peer has made the same read,
/// so both adoptions observe the empty slot before either can write. Later reads — the
/// create-conflict re-read among them — pass straight through.
///
/// The two ports share one two-party [`Barrier`]: a party that already arrived is never counted
/// twice, so repolling one future cannot stand in for the missing peer and release it early.
/// shortcut: a side that never reaches its first absent read leaves the join pending, add a timeout
/// if this test ever hangs in CI.
struct GatedDestination {
    inner: ScopedKnowledgeStore,
    first_absent: Arc<Barrier>,
    saw_empty: AtomicBool,
}

impl GatedDestination {
    fn new(inner: ScopedKnowledgeStore, first_absent: Arc<Barrier>) -> Self {
        Self {
            inner,
            first_absent,
            saw_empty: AtomicBool::new(false),
        }
    }

    /// Whether this port has already observed the empty slot.
    fn observed_absent(&self) -> bool {
        self.saw_empty.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl KnowledgeEntryPort for GatedDestination {
    async fn get_knowledge_entry(&self, entry_id: &str) -> SpokeResult<KnowledgeEntry> {
        let read = self.inner.get_knowledge_entry(entry_id).await;
        let absent = matches!(
            &read,
            SpokeResult::Reject(reject)
                if reject.code == SpokeRejectCode::KnowledgeEntryNotFound
        );
        // Only this port's first absent read reports; both reports must be in before either side
        // may reach its create.
        if absent && !self.saw_empty.swap(true, Ordering::SeqCst) {
            self.first_absent.wait().await;
        }
        read
    }

    async fn put_knowledge_entry(
        &self,
        entry: KnowledgeEntry,
        expected_base_revision: Option<u64>,
    ) -> SpokeResult<KnowledgeEntry> {
        self.inner
            .put_knowledge_entry(entry, expected_base_revision)
            .await
    }
}

#[tokio::test]
async fn two_handles_adopt_one_copy_and_agree_on_it() {
    let path = database_path("adopt-cross-handle");
    let left_store = MindStore::open(&path).await.expect("open left store");
    let right_store = MindStore::open(&path).await.expect("open right store");
    let left_src = left_store.knowledge(KnowledgeScope::new("shared"));
    let right_src = right_store.knowledge(KnowledgeScope::new("shared"));
    let left_dst = left_store.knowledge(KnowledgeScope::new("wren"));
    let right_dst = right_store.knowledge(KnowledgeScope::new("wren"));
    // One rendezvous for both ports: a side leaves its first absent read only once its peer has
    // reported the same absence, so neither create can precede the other read.
    let first_absent = Arc::new(Barrier::new(2));
    let left_gated = GatedDestination::new(
        left_store.knowledge(KnowledgeScope::new("wren")),
        Arc::clone(&first_absent),
    );
    let right_gated = GatedDestination::new(
        right_store.knowledge(KnowledgeScope::new("wren")),
        first_absent,
    );
    let reader = second_connection(&path).await;

    let shared_id = "ke_shared_gate";
    expect_ok(
        left_src
            .put_knowledge_entry(entry_with(shared_id, "shut after dusk"), None)
            .await,
        "seed the source",
    );

    // Both destination reads see the empty slot before either write starts, so the create that
    // loses the claim can only answer `AlreadyPresent` through the post-conflict re-read.
    let (left, right) = tokio::join!(
        adopt_knowledge_entry(&left_src, &left_gated, shared_id, 1),
        adopt_knowledge_entry(&right_src, &right_gated, shared_id, 1),
    );
    assert!(
        left_gated.observed_absent() && right_gated.observed_absent(),
        "both adoptions must have read the destination slot as empty"
    );
    let (created, present) = match (
        expect_ok(left, "left adoption"),
        expect_ok(right, "right adoption"),
    ) {
        (AdoptionOutcome::Created(created), AdoptionOutcome::AlreadyPresent(present))
        | (AdoptionOutcome::AlreadyPresent(present), AdoptionOutcome::Created(created)) => {
            (created, present)
        }
        other => panic!("exactly one concurrent adoption must create, got {other:?}"),
    };
    assert_eq!(as_json(&created), as_json(&present));

    for port in [&left_dst, &right_dst] {
        let read = expect_ok(port.get_knowledge_entry(shared_id).await, "read the copy");
        assert_eq!(as_json(&read), as_json(&created));
        assert_eq!(anchor_source_revision(&read), Some(1));
    }
    let rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM knowledge_entries WHERE collection = 'wren' AND entry_id = ?",
    )
    .bind(shared_id)
    .fetch_one(&reader)
    .await
    .expect("destination row count");
    assert_eq!(rows, 1);

    reader.close().await;
    left_store.close().await;
    right_store.close().await;
    remove_database(&path);
}
