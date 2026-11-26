//! The Mind registry operations: `create`'s four writes in one transaction and the refusals a
//! taken coordinate answers, `list` and `get` under the caller's live grants, freezing as
//! deactivation, and identity across a reopen.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::path::Path;

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::{MindId, SpaceId};
use mindcore_base::ir::{MemoryGrantRow, MemoryRepository, MemorySpaceRow};
use mindcore_sqlite::minds::{
    self, CreateMind, Mind, MindStatus, CARRIER_ENTRY_ID, MIND_ALREADY_EXISTS,
    MIND_CARRIER_CANNOT_BE_SUPERSEDED, MIND_COLLECTION_OCCUPIED, MIND_ID_INVALID,
};
use mindcore_sqlite::{write_entry, MindStore, SqliteMemoryRepository};
use serde_json::{Map, Number};
use spoke_operations::{KnowledgeEntryPort, SpokeReject, SpokeRejectCode, SpokeResult};
use spoke_schemas::knowledge_entry::{KnowledgeEntryBody, KnowledgeEntryCanonicalName};
use spoke_schemas::KnowledgeEntry;
use sqlx::SqlitePool;

const TIME: &str = "2026-11-26T00:00:00.000Z";

async fn open_store(path: &Path) -> MindStore {
    MindStore::open(path).await.expect("open store")
}

fn request(mind_id: Option<&str>, owner: &str) -> CreateMind {
    CreateMind {
        mind_id: mind_id.map(MindId::new),
        display_name: Some("Wren".to_string()),
        owner: CallerId::new(owner),
    }
}

async fn pool_begin(store: &MindStore) -> sqlx::Transaction<'static, sqlx::Sqlite> {
    store.pool().begin().await.expect("begin a transaction")
}

fn reject<T>(result: SpokeResult<T>) -> SpokeReject {
    match result {
        SpokeResult::Reject(reject) => reject,
        SpokeResult::Ok(_) => panic!("expected a refusal"),
    }
}

fn ok<T>(result: SpokeResult<T>) -> T {
    match result {
        SpokeResult::Ok(value) => value,
        SpokeResult::Reject(reject) => panic!("expected success, got {reject:?}"),
    }
}

async fn count(pool: &SqlitePool, table: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
        .fetch_one(pool)
        .await
        .expect("count rows")
}

fn space_row(space_id: &str) -> MemorySpaceRow {
    MemorySpaceRow {
        space_id: SpaceId::new(space_id),
        display_name: Some(space_id.to_string()),
        authority_host_id: "mindcore".to_string(),
        authority_generation: 1,
        status: "active".to_string(),
        revision: Number::from(1),
        created_at: TIME.to_string(),
        pending_receipts: 0,
    }
}

async fn insert_space(pool: &SqlitePool, space_id: &str) {
    let repo = SqliteMemoryRepository;
    let mut tx = pool.begin().await.expect("begin");
    repo.insert_space(&mut tx, &space_row(space_id))
        .await
        .expect("insert space");
    tx.commit().await.expect("commit");
}

fn grant_row(
    grant_id: &str,
    space_id: &str,
    principal: &str,
    expires_at: Option<&str>,
) -> MemoryGrantRow {
    MemoryGrantRow {
        grant_id: grant_id.to_string(),
        space_id: SpaceId::new(space_id),
        subject_id: None,
        principal_id: CallerId::new(principal),
        capabilities: vec!["recall".to_string()],
        context_ids: Vec::new(),
        shared_access: false,
        expires_at: expires_at.map(str::to_string),
        revoked_at: None,
        revision: Number::from(1),
        created_at: TIME.to_string(),
        extensions: Map::new(),
    }
}

async fn insert_grant(pool: &SqlitePool, row: &MemoryGrantRow) {
    let repo = SqliteMemoryRepository;
    let mut tx = pool.begin().await.expect("begin");
    repo.insert_grant(&mut tx, row).await.expect("insert grant");
    tx.commit().await.expect("commit");
}

fn plain_carrier(entry_id: &str) -> KnowledgeEntry {
    KnowledgeEntry {
        adoption_anchor: None,
        body: KnowledgeEntryBody::default(),
        canonical_name: KnowledgeEntryCanonicalName::try_from(format!("{entry_id} carrier"))
            .expect("non-empty canonical name"),
        created_at: None,
        disclosure: None,
        entry_id: entry_id.to_string(),
        entry_type: "character".to_string(),
        extensions: HashMap::new(),
        modules: HashMap::new(),
        owner: None,
        revision: Some(1),
        schema_version: NonZeroU64::new(1).expect("non-zero wire baseline"),
        source_anchor: None,
        status: "confirmed".to_string(),
        updated_at: None,
    }
}

/// A pool on the same file without the migrator or the foreign-key enforcement, copied from
/// `store_migrations.rs`.
async fn raw_pool(path: &Path) -> SqlitePool {
    sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true)
                .foreign_keys(false),
        )
        .await
        .expect("open a raw pool on the store file")
}

#[tokio::test]
async fn create_with_a_named_id_writes_space_carrier_record_and_grant() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    let caller = CallerId::new("caller_wren");

    let mind = ok(store
        .create_mind(&request(Some("wren"), "caller_wren"))
        .await);
    assert_eq!(mind.id, MindId::new("wren"));
    assert_eq!(mind.display_name.as_deref(), Some("Wren"));
    assert_eq!(mind.status, MindStatus::Active);
    assert_eq!(mind.knowledge.collection, "mind:wren");
    assert_eq!(mind.carrier_entry_id, CARRIER_ENTRY_ID);

    let pool = store.pool();
    let repo = SqliteMemoryRepository;
    let space_id = SpaceId::new("wren");
    let mut tx = pool.begin().await.expect("begin");
    let space = repo
        .space(&mut tx, &space_id)
        .await
        .expect("read space")
        .expect("the space row exists");
    assert_eq!(space.status, "active");
    assert_eq!(space.display_name.as_deref(), Some("Wren"));
    assert_eq!(space.authority_host_id, "mindcore");
    assert_eq!(space.authority_generation, 1);
    assert_eq!(space.revision, Number::from(1));
    let grants = repo
        .list_grants(&mut tx, &space_id, Some("caller_wren"))
        .await
        .expect("read grants");
    tx.commit().await.expect("commit");
    assert_eq!(grants.len(), 1, "exactly one grant for the owner");
    let grant = &grants[0];
    // The set the create default grant contract fixes, asserted independently of the constant
    // the producer writes from: a dropped or renamed capability fails here.
    let specified: std::collections::BTreeSet<&str> = [
        "recall",
        "capture",
        "review",
        "records_write",
        "knowledge_read",
        "knowledge_write",
        "admin",
    ]
    .into_iter()
    .collect();
    let actual: std::collections::BTreeSet<&str> =
        grant.capabilities.iter().map(String::as_str).collect();
    assert_eq!(actual, specified);
    assert!(grant.subject_id.is_none());
    assert!(grant.expires_at.is_none());
    assert!(grant.context_ids.is_empty());

    let entry = ok(store
        .knowledge(mind.knowledge.clone())
        .get_knowledge_entry(CARRIER_ENTRY_ID)
        .await);
    assert_eq!(entry.revision, Some(1));
    assert_eq!(entry.entry_type, "character");
    assert!(entry.modules.is_empty());

    let minds_row: (String, String) = sqlx::query_as(
        "SELECT knowledge_collection, carrier_entry_id FROM minds WHERE space_id = ?",
    )
    .bind("wren")
    .fetch_one(&pool)
    .await
    .expect("the minds row exists");
    assert_eq!(minds_row.0, "mind:wren");
    assert_eq!(minds_row.1, CARRIER_ENTRY_ID);

    let subjects =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory_subjects WHERE space_id = ?")
            .bind("wren")
            .fetch_one(&pool)
            .await
            .expect("count subjects");
    assert_eq!(subjects, 0, "create establishes no subject");

    let seen = ok(store.get_mind(&caller, &MindId::new("wren")).await);
    assert_eq!(mind, seen, "the returned Mind is what get answers");
}

#[tokio::test]
async fn create_without_a_named_id_assigns_one() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    let caller = CallerId::new("caller_wren");

    let first = ok(store.create_mind(&request(None, "caller_wren")).await);
    assert!(first.id.as_str().starts_with("spc_"));
    assert!(!first.id.as_str().is_empty());
    let second = ok(store.create_mind(&request(None, "caller_wren")).await);
    assert_ne!(first.id, second.id, "each call assigns a different id");
    let seen_first = ok(store.get_mind(&caller, &first.id).await);
    let seen_second = ok(store.get_mind(&caller, &second.id).await);
    assert_eq!(first, seen_first);
    assert_eq!(second, seen_second);
}

#[tokio::test]
async fn create_refuses_an_empty_id_or_owner() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;

    let refused = reject(store.create_mind(&request(Some(""), "caller_wren")).await);
    assert_eq!(refused.code, SpokeRejectCode::InvalidInput);
    let refused = reject(
        store
            .create_mind(&CreateMind {
                owner: CallerId::new(""),
                ..request(Some("wren"), "caller_wren")
            })
            .await,
    );
    assert_eq!(refused.code, SpokeRejectCode::InvalidInput);

    let pool = store.pool();
    for table in [
        "memory_spaces",
        "knowledge_entries",
        "minds",
        "memory_grants",
    ] {
        assert_eq!(count(&pool, table).await, 0, "nothing written in {table}");
    }
}

#[tokio::test]
async fn a_taken_space_is_refused_and_writes_nothing() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    let pool = store.pool();
    insert_space(&pool, "wren").await;

    let refused = reject(
        store
            .create_mind(&request(Some("wren"), "caller_wren"))
            .await,
    );
    assert_eq!(refused.code, SpokeRejectCode::InvalidInput);
    assert_eq!(
        refused
            .details
            .as_ref()
            .and_then(|details| details.get("reason"))
            .and_then(|reason| reason.as_str()),
        Some(MIND_ALREADY_EXISTS)
    );
    for table in ["knowledge_entries", "minds", "memory_grants"] {
        assert_eq!(count(&pool, table).await, 0, "nothing written in {table}");
    }
    let listed = ok(store.list_minds(&CallerId::new("caller_wren")).await);
    assert!(listed.is_empty());
}

#[tokio::test]
async fn a_taken_carrier_slot_leaves_no_row() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    let pool = store.pool();
    let mut tx = pool.begin().await.expect("begin");
    match write_entry(&mut tx, "mind:wren", plain_carrier(CARRIER_ENTRY_ID), None).await {
        SpokeResult::Ok(_) => {}
        SpokeResult::Reject(reject) => panic!("seed the carrier slot: {reject:?}"),
    }
    tx.commit().await.expect("commit");

    let refused = reject(
        store
            .create_mind(&request(Some("wren"), "caller_wren"))
            .await,
    );
    // The slot and every other entry in the collection answer one refusal — the occupied
    // collection — and its message never names the internal carrier id.
    assert_eq!(refused.code, SpokeRejectCode::InvalidInput);
    assert_eq!(
        refused
            .details
            .as_ref()
            .and_then(|details| details.get("reason"))
            .and_then(|reason| reason.as_str()),
        Some(MIND_COLLECTION_OCCUPIED)
    );
    assert!(
        !refused.message.contains(CARRIER_ENTRY_ID),
        "the refusal does not leak the internal carrier id: {}",
        refused.message
    );
    for table in ["memory_spaces", "minds", "memory_grants"] {
        assert_eq!(count(&pool, table).await, 0, "nothing written in {table}");
    }
}

#[tokio::test]
async fn a_taken_registry_row_leaves_no_row() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    store.close().await;

    // An orphan registry row: foreign keys are off on this pool, so the missing space and
    // carrier do not refuse the insert.
    let raw = raw_pool(&path).await;
    sqlx::query(
        "INSERT INTO minds (space_id, knowledge_collection, carrier_entry_id) VALUES (?, ?, ?)",
    )
    .bind("wren")
    .bind("mind:elsewhere")
    .bind("ke_elsewhere")
    .execute(&raw)
    .await
    .expect("write the orphan registry row");
    raw.close().await;

    let store = open_store(&path).await;
    let refused = reject(
        store
            .create_mind(&request(Some("wren"), "caller_wren"))
            .await,
    );
    assert_eq!(refused.code, SpokeRejectCode::InvalidInput);
    assert_eq!(
        refused
            .details
            .as_ref()
            .and_then(|details| details.get("reason"))
            .and_then(|reason| reason.as_str()),
        Some(MIND_ALREADY_EXISTS)
    );
    let pool = store.pool();
    assert_eq!(count(&pool, "memory_spaces").await, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM knowledge_entries WHERE collection = ?")
            .bind("mind:wren")
            .fetch_one(&pool)
            .await
            .expect("count the carrier entries"),
        0,
        "the carrier the refused create wrote is rolled back"
    );
    assert_eq!(count(&pool, "memory_grants").await, 0);
    assert_eq!(
        count(&pool, "minds").await,
        1,
        "the orphan row is untouched"
    );
}

#[tokio::test]
async fn a_grant_named_like_a_mind_in_another_space_does_not_block_it() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    let pool = store.pool();
    insert_space(&pool, "spc_other").await;
    insert_grant(
        &pool,
        &grant_row("grt_wren", "spc_other", "caller_other", None),
    )
    .await;

    // The default grant id is minted independently: a grant in another space, even one named
    // after this Mind, reserves nothing of the Mind's name.
    let mind = ok(store
        .create_mind(&request(Some("wren"), "caller_wren"))
        .await);
    assert_eq!(mind.id, MindId::new("wren"));
    let grant_ids: Vec<String> =
        sqlx::query_scalar("SELECT grant_id FROM memory_grants WHERE space_id = ?")
            .bind("wren")
            .fetch_all(&pool)
            .await
            .expect("read the wren grants");
    assert_eq!(grant_ids.len(), 1, "exactly one default grant row");
    assert_ne!(
        grant_ids[0], "grt_wren",
        "the grant id does not derive from the Mind id"
    );
    let seen = ok(store
        .get_mind(&CallerId::new("caller_wren"), &MindId::new("wren"))
        .await);
    assert_eq!(mind, seen);
}

#[tokio::test]
async fn the_borrowed_create_commits_only_with_the_callers_transaction() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    let pool = store.pool();

    let mut tx = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("begin the write");
    ok(minds::create(&mut tx, &request(Some("wren"), "caller_wren")).await);
    tx.rollback().await.expect("roll back");
    for table in [
        "memory_spaces",
        "knowledge_entries",
        "minds",
        "memory_grants",
    ] {
        assert_eq!(count(&pool, table).await, 0, "rolled back in {table}");
    }

    let mut tx = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("begin the write");
    ok(minds::create(&mut tx, &request(Some("wren"), "caller_wren")).await);
    tx.commit().await.expect("commit");
    for table in [
        "memory_spaces",
        "knowledge_entries",
        "minds",
        "memory_grants",
    ] {
        assert_eq!(count(&pool, table).await, 1, "committed in {table}");
    }
}

#[tokio::test]
async fn list_answers_the_minds_the_caller_holds_a_live_grant_in() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    let pool = store.pool();
    ok(store
        .create_mind(&request(Some("wren"), "caller_wren"))
        .await);
    ok(store
        .create_mind(&request(Some("studio"), "caller_studio"))
        .await);

    let wren = CallerId::new("caller_wren");
    let studio = CallerId::new("caller_studio");
    let third = CallerId::new("caller_third");
    let names = |minds: &[Mind]| {
        minds
            .iter()
            .map(|mind| mind.id.as_str().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(names(&ok(store.list_minds(&wren).await)), vec!["wren"]);
    assert_eq!(names(&ok(store.list_minds(&studio).await)), vec!["studio"]);
    assert!(ok(store.list_minds(&third).await).is_empty());

    // A revoked grant no longer reaches its Mind.
    sqlx::query("UPDATE memory_grants SET revoked_at = ? WHERE space_id = ?")
        .bind(TIME)
        .bind("wren")
        .execute(&pool)
        .await
        .expect("revoke the wren grant");
    assert!(ok(store.list_minds(&wren).await).is_empty());

    // An expired grant — the expiry spelled with a `+08:00` offset and in the past — reaches
    // nothing.
    insert_grant(
        &pool,
        &grant_row(
            "grt_expired",
            "studio",
            "caller_expired",
            Some("2020-01-01T00:00:00+08:00"),
        ),
    )
    .await;
    assert!(ok(store.list_minds(&CallerId::new("caller_expired")).await).is_empty());

    // A grant that expires in the future, spelled with a `+08:00` offset, is live.
    insert_grant(
        &pool,
        &grant_row(
            "grt_future",
            "studio",
            "caller_future",
            Some("2099-01-01T00:00:00+08:00"),
        ),
    )
    .await;
    assert_eq!(
        names(&ok(store.list_minds(&CallerId::new("caller_future")).await)),
        vec!["studio"]
    );

    // One grant in each Mind lists both, in id order.
    insert_grant(
        &pool,
        &grant_row("grt_both_wren", "wren", "caller_both", None),
    )
    .await;
    insert_grant(
        &pool,
        &grant_row("grt_both_studio", "studio", "caller_both", None),
    )
    .await;
    assert_eq!(
        names(&ok(store.list_minds(&CallerId::new("caller_both")).await)),
        vec!["studio", "wren"]
    );
}

#[tokio::test]
async fn get_answers_not_found_for_an_unseen_or_unknown_mind() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    ok(store
        .create_mind(&request(Some("wren"), "caller_wren"))
        .await);

    let unseen = reject(
        store
            .get_mind(&CallerId::new("caller_third"), &MindId::new("wren"))
            .await,
    );
    assert_eq!(unseen.code, SpokeRejectCode::NotFound);
    let unknown = reject(
        store
            .get_mind(&CallerId::new("caller_wren"), &MindId::new("never"))
            .await,
    );
    assert_eq!(unknown.code, SpokeRejectCode::NotFound);
}

#[tokio::test]
async fn a_frozen_space_freezes_the_mind_and_unfreezing_reactivates_it() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    ok(store
        .create_mind(&request(Some("wren"), "caller_wren"))
        .await);
    let caller = CallerId::new("caller_wren");

    let pool = store.pool();
    let repo = SqliteMemoryRepository;
    let space_id = SpaceId::new("wren");
    let mut tx = pool.begin().await.expect("begin");
    let mut space = repo
        .space(&mut tx, &space_id)
        .await
        .expect("read space")
        .expect("the space row exists");
    let observed = space.revision.clone();
    space.status = "frozen".to_string();
    space.revision = Number::from(2);
    assert!(
        repo.update_space(&mut tx, &space, &observed)
            .await
            .expect("freeze the space"),
        "the freeze applies"
    );
    tx.commit().await.expect("commit");

    let frozen = ok(store.get_mind(&caller, &MindId::new("wren")).await);
    assert_eq!(frozen.status, MindStatus::Frozen);
    let listed = ok(store.list_minds(&caller).await);
    assert_eq!(listed.len(), 1, "a frozen Mind stays listed");
    assert_eq!(listed[0].status, MindStatus::Frozen);

    let mut tx = pool.begin().await.expect("begin");
    let mut space = repo
        .space(&mut tx, &space_id)
        .await
        .expect("read space")
        .expect("the space row exists");
    let observed = space.revision.clone();
    space.status = "active".to_string();
    space.revision = Number::from(3);
    assert!(
        repo.update_space(&mut tx, &space, &observed)
            .await
            .expect("unfreeze the space"),
        "the unfreeze applies"
    );
    tx.commit().await.expect("commit");

    let active = ok(store.get_mind(&caller, &MindId::new("wren")).await);
    assert_eq!(active.status, MindStatus::Active);
    assert_eq!(
        count(&pool, "minds").await,
        1,
        "one registry row throughout"
    );
}

#[tokio::test]
async fn a_populated_collection_is_refused_and_the_seed_survives() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    let pool = store.pool();
    // A host-populated `mind:wren` collection with no wren space and no registry row: the new
    // Mind must never claim entries it did not create.
    let mut tx = pool.begin().await.expect("begin");
    match write_entry(&mut tx, "mind:wren", plain_carrier("ke_foreign"), None).await {
        SpokeResult::Ok(_) => {}
        SpokeResult::Reject(reject) => panic!("seed the foreign entry: {reject:?}"),
    }
    tx.commit().await.expect("commit");

    let refused = reject(
        store
            .create_mind(&request(Some("wren"), "caller_wren"))
            .await,
    );
    assert_eq!(refused.code, SpokeRejectCode::InvalidInput);
    assert_eq!(
        refused
            .details
            .as_ref()
            .and_then(|details| details.get("reason"))
            .and_then(|reason| reason.as_str()),
        Some(MIND_COLLECTION_OCCUPIED)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM knowledge_entries WHERE collection = ? AND entry_id = ?"
        )
        .bind("mind:wren")
        .bind("ke_foreign")
        .fetch_one(&pool)
        .await
        .expect("count the seeded entry"),
        1,
        "the seeded entry is untouched"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM knowledge_entries WHERE collection = ?")
            .bind("mind:wren")
            .fetch_one(&pool)
            .await
            .expect("count the collection"),
        1,
        "no carrier joined the populated collection"
    );
    for table in ["memory_spaces", "minds", "memory_grants"] {
        assert_eq!(count(&pool, table).await, 0, "nothing written in {table}");
    }
}

#[tokio::test]
async fn an_id_the_schema_cannot_hold_is_invalid_input() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;

    // SQLite's `length()` counts up to the first NUL, so a NUL-leading id fails the registry's
    // `length(space_id) > 0` check. The schema's refusal is invalid input, never an internal
    // error.
    let refused = reject(
        store
            .create_mind(&request(Some("\0wren"), "caller_wren"))
            .await,
    );
    assert_eq!(refused.code, SpokeRejectCode::InvalidInput);
    assert_eq!(
        refused
            .details
            .as_ref()
            .and_then(|details| details.get("reason"))
            .and_then(|reason| reason.as_str()),
        Some(MIND_ID_INVALID),
        "the refusal names the invalid id, not internal storage"
    );

    // A very long id is one the schema can hold: neither the space key nor the registry check
    // has a length cap, so creation succeeds.
    let long_id = "x".repeat(4096);
    let request = CreateMind {
        mind_id: Some(MindId::new(long_id.clone())),
        display_name: None,
        owner: CallerId::new("caller_wren"),
    };
    let mind = ok(store.create_mind(&request).await);
    assert_eq!(mind.id.as_str(), long_id);

    let pool = store.pool();
    for table in [
        "memory_spaces",
        "knowledge_entries",
        "minds",
        "memory_grants",
    ] {
        assert_eq!(
            count(&pool, table).await,
            1,
            "exactly the successful create's row in {table}"
        );
    }
}

#[tokio::test]
async fn a_malformed_expiry_hides_the_mind() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    let pool = store.pool();
    ok(store
        .create_mind(&request(Some("wren"), "caller_wren"))
        .await);

    insert_grant(
        &pool,
        &grant_row(
            "grt_broken",
            "wren",
            "caller_broken",
            Some("not-a-timestamp"),
        ),
    )
    .await;
    assert!(
        ok(store.list_minds(&CallerId::new("caller_broken")).await).is_empty(),
        "an expiry that does not parse is never live"
    );
    let unseen = reject(
        store
            .get_mind(&CallerId::new("caller_broken"), &MindId::new("wren"))
            .await,
    );
    assert_eq!(unseen.code, SpokeRejectCode::NotFound);
}

#[tokio::test]
async fn an_expired_grant_before_a_live_one_lists_the_mind_once() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    let pool = store.pool();
    ok(store
        .create_mind(&request(Some("wren"), "caller_wren"))
        .await);

    // Two grants for one caller on one Mind: the earlier candidate in `grant_id` order is
    // expired and a later one is live; the Mind is listed exactly once.
    insert_grant(
        &pool,
        &grant_row(
            "grt_aa_expired",
            "wren",
            "caller_pair",
            Some("2020-01-01T00:00:00Z"),
        ),
    )
    .await;
    insert_grant(
        &pool,
        &grant_row("grt_bb_live", "wren", "caller_pair", None),
    )
    .await;
    let listed = ok(store.list_minds(&CallerId::new("caller_pair")).await);
    assert_eq!(listed.len(), 1, "the Mind appears exactly once");
    assert_eq!(listed[0].id, MindId::new("wren"));
    let seen = ok(store
        .get_mind(&CallerId::new("caller_pair"), &MindId::new("wren"))
        .await);
    assert_eq!(seen.id, MindId::new("wren"));
}

#[tokio::test]
async fn two_live_grants_for_one_caller_list_the_mind_once() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    let pool = store.pool();
    ok(store
        .create_mind(&request(Some("wren"), "caller_wren"))
        .await);

    // Two live grants for the same caller on the same Mind: without the adjacent-group
    // de-duplication the Mind would be listed once per grant.
    insert_grant(
        &pool,
        &grant_row("grt_cc_recall", "wren", "caller_pair", None),
    )
    .await;
    insert_grant(
        &pool,
        &grant_row("grt_dd_capture", "wren", "caller_pair", None),
    )
    .await;
    let listed = ok(store.list_minds(&CallerId::new("caller_pair")).await);
    assert_eq!(listed.len(), 1, "two live grants still list one Mind");
    assert_eq!(listed[0].id, MindId::new("wren"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_grant_expiring_during_the_connection_wait_is_not_live() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    let pool = store.pool();
    ok(store
        .create_mind(&request(Some("wren"), "caller_wren"))
        .await);

    // A grant that expires a few tens of milliseconds from now. A liveness decision taken before
    // the read begins would still see it live after the wait; one taken per row must not.
    let expiry = (chrono::Utc::now() + chrono::Duration::milliseconds(40))
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();
    insert_grant(
        &pool,
        &grant_row("grt_fleeting", "wren", "caller_wait", Some(&expiry)),
    )
    .await;

    // The pool's one connection is held open by an unrelated borrower, so `list_minds` waits
    // for its connection while the expiry passes; WAL readers are not lock-blocked, so the wait
    // is purely the pool's.
    let (release, held) = tokio::sync::oneshot::channel::<()>();
    let (started_tx, mut started_rx) = tokio::sync::mpsc::channel::<()>(1);
    let blocker_pool = pool.clone();
    let blocker = tokio::spawn(async move {
        let connection = blocker_pool.acquire().await.expect("hold the connection");
        started_tx.send(()).await.expect("signal the hold");
        let _ = held.await;
        drop(connection);
    });
    started_rx
        .recv()
        .await
        .expect("the blocker holds the connection");

    // `list_minds` starts now, before the expiry; the borrower lets go after the expiry has
    // passed, so the read's rows arrive with the grant already dead.
    let caller = CallerId::new("caller_wait");
    let releaser = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        let _ = release.send(());
    });
    let listed = store.list_minds(&caller).await;
    releaser.await.expect("join the releaser");
    blocker.await.expect("join the blocker");
    let listed = ok(listed);
    assert!(
        listed.is_empty(),
        "a grant whose expiry passed during the wait is not live"
    );
    let unseen = reject(store.get_mind(&caller, &MindId::new("wren")).await);
    assert_eq!(unseen.code, SpokeRejectCode::NotFound);
}

#[tokio::test]
async fn a_registered_carrier_cannot_be_superseded() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    let mind = ok(store
        .create_mind(&request(Some("wren"), "caller_wren"))
        .await);
    let port = store.knowledge(mind.knowledge.clone());

    // The successor exists in the same collection, exactly as the retirement sequence sets up.
    let mut tx = pool_begin(&store).await;
    match write_entry(&mut tx, "mind:wren", plain_carrier("ke_next"), None).await {
        SpokeResult::Ok(_) => {}
        SpokeResult::Reject(reject) => panic!("seed the successor: {reject:?}"),
    }
    tx.commit().await.expect("commit");

    let refused = reject(port.supersede_entry(CARRIER_ENTRY_ID, 1, "ke_next").await);
    assert_eq!(refused.code, SpokeRejectCode::InvalidInput);
    assert_eq!(
        refused
            .details
            .as_ref()
            .and_then(|details| details.get("reason"))
            .and_then(|reason| reason.as_str()),
        Some(MIND_CARRIER_CANNOT_BE_SUPERSEDED)
    );

    // The carrier stays readable and appendable: its revision advances through the ordinary
    // knowledge write.
    let carrier = ok(port.get_knowledge_entry(CARRIER_ENTRY_ID).await);
    assert_eq!(carrier.revision, Some(1));
    let updated = ok(port.put_knowledge_entry(carrier.clone(), Some(1)).await);
    assert_eq!(updated.revision, Some(2), "the carrier still advances");
}

#[tokio::test]
async fn an_unregistered_entry_in_a_minds_collection_still_supersedes() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    let mind = ok(store
        .create_mind(&request(Some("wren"), "caller_wren"))
        .await);
    let port = store.knowledge(mind.knowledge.clone());

    for entry_id in ["ke_note", "ke_note_next"] {
        let mut tx = pool_begin(&store).await;
        match write_entry(&mut tx, "mind:wren", plain_carrier(entry_id), None).await {
            SpokeResult::Ok(_) => {}
            SpokeResult::Reject(reject) => panic!("seed {entry_id}: {reject:?}"),
        }
        tx.commit().await.expect("commit");
    }
    let marked = ok(port.supersede_entry("ke_note", 1, "ke_note_next").await);
    assert_eq!(marked, 2, "an unregistered entry retires as before");
}

#[tokio::test]
async fn an_entry_in_an_unregistered_collection_still_supersedes() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    ok(store
        .create_mind(&request(Some("wren"), "caller_wren"))
        .await);
    let port = store.knowledge(mindcore_base::knowledge::KnowledgeScope::new("unrelated"));

    // The same entry id in a collection no Mind registers keeps the retirement behaviour.
    for entry_id in [CARRIER_ENTRY_ID, "ke_next"] {
        let mut tx = pool_begin(&store).await;
        match write_entry(&mut tx, "unrelated", plain_carrier(entry_id), None).await {
            SpokeResult::Ok(_) => {}
            SpokeResult::Reject(reject) => panic!("seed {entry_id}: {reject:?}"),
        }
        tx.commit().await.expect("commit");
    }
    let marked = ok(port.supersede_entry(CARRIER_ENTRY_ID, 1, "ke_next").await);
    assert_eq!(marked, 2, "the carrier's id in another collection retires");
}

#[tokio::test]
async fn get_returns_the_same_mind_after_reopen() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    let created = ok(store
        .create_mind(&request(Some("wren"), "caller_wren"))
        .await);
    store.close().await;

    let store = open_store(&path).await;
    let seen = ok(store
        .get_mind(&CallerId::new("caller_wren"), &MindId::new("wren"))
        .await);
    assert_eq!(created, seen);
}

/// An undecodable grant row is invisible, not fatal: the registry's reads decode row by row, so a
/// good row sorted after a bad one still lists, gets and admits the Mind. `query_as`'s `try_map`
/// would have ended the stream at the bad row.
#[tokio::test]
async fn a_good_grant_after_an_undecodable_row_still_lists_the_mind() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    let store = open_store(&path).await;
    let pool = store.pool();
    let caller = CallerId::new("caller_wren");
    ok(store
        .create_mind(&request(Some("wren"), "caller_wren"))
        .await);

    // Replace the created grant with a bad row that sorts first and a good row that sorts after
    // it. `expires_at` is selected by the visibility join; the blob cast stores bytes the driver
    // cannot hand over as text.
    sqlx::query("DELETE FROM memory_grants WHERE space_id = 'wren'")
        .execute(&pool)
        .await
        .expect("drop the created grant");
    for (grant_id, expires_at) in [
        ("grant_a_bad", "CAST(X'80' AS TEXT)"),
        ("grant_z_good", "NULL"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, \
              context_ids_json, shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES ('{grant_id}', 'wren', NULL, 'caller_wren', '[\"recall\"]', '[]', 0, \
              {expires_at}, NULL, 1, '{TIME}')"
        )))
        .execute(&pool)
        .await
        .expect("insert grant");
    }

    let listed = ok(store.list_minds(&caller).await);
    assert_eq!(
        listed
            .iter()
            .map(|mind| mind.id.as_str().to_string())
            .collect::<Vec<_>>(),
        vec!["wren".to_string()],
        "the good row after the bad one still lists the Mind"
    );
    assert_eq!(
        ok(store.get_mind(&caller, &MindId::new("wren")).await).id,
        MindId::new("wren"),
        "the point read applies the same rule"
    );

    // The served list's id read shares it.
    let mut tx = pool_begin(&store).await;
    assert_eq!(
        ok(mindcore_sqlite::minds::visible_mind_ids(&mut tx, "caller_wren").await),
        vec!["wren".to_string()],
        "the id read omits the undecodable row and keeps serving"
    );
    tx.rollback().await.expect("rollback");
}
