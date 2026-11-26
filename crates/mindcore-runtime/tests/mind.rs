//! The Mind handle `mindcore_runtime::Mind`: `open` through the registry under the caller's
//! live grants, the registry's own `NotFound` for a Mind the caller cannot see, and a frozen
//! Mind opening because the handle acts on no status from the row.

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::{mint, MindId, SpaceId};
use mindcore_base::ir::MemoryGrantRow;
use mindcore_base::ir::MemoryRepository;
use mindcore_runtime::journal_dir;
use mindcore_runtime::minds::{self, CreateMind};
use mindcore_runtime::{Capture, Mind, Recall};
use mindcore_spi::SpiError;
use mindcore_sqlite::{MindStore, SqliteMemoryRepository};
use chrono::Utc;
use serde_json::{json, Map, Number, Value};
use spoke_schemas::data::knowledge_entry::{
    KnowledgeEntryBody, KnowledgeEntryCanonicalName,
};
use spoke_schemas::KnowledgeEntry;
use spoke_operations::{SpokeReject, SpokeRejectCode, SpokeResult};
use spoke_schemas::memory_candidates_review_request::Decision;
use std::sync::mpsc;
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

const SUBJECT: &str = "subj_wren";

fn capture(key: &str, text: &str) -> Capture<'static> {
    Capture {
        subject: SUBJECT,
        context: None,
        content: Box::leak(Box::new(json!({ "text": text }))),
        occurred_at: None,
        kind_hint: None,
        idempotency_key: Box::leak(key.to_string().into_boxed_str()),
    }
}

fn recall_all() -> (Recall<'static>, Value) {
    let selection = Box::leak(Box::new(json!({ "mode": "subject_all" })));
    (
        Recall {
            subject: SUBJECT,
            selection,
            max_items: Some(10),
            max_bytes: None,
            query: None,
            cursor: None,
        },
        selection.clone(),
    )
}

fn promote() -> spoke_schemas::memory_candidates_review_request::Decision {
    serde_json::from_value(json!({ "action": "promote" })).expect("promote decision")
}

fn value_of<T: serde::Serialize>(response: &T) -> Value {
    serde_json::to_value(response).expect("response serializes")
}

fn retry_class(reject: &SpokeReject) -> &str {
    reject
        .details
        .as_ref()
        .and_then(|details| details.get("retry_class"))
        .and_then(Value::as_str)
        .expect("a retry class")
}

fn reject_of<T>(result: SpokeResult<T>) -> SpokeReject {
    match result {
        SpokeResult::Reject(reject) => reject,
        SpokeResult::Ok(_) => panic!("expected a refusal"),
    }
}

/// Capture one candidate through the handle and answer its id.
async fn capture_candidate(handle: &Mind, key: &str, text: &str) -> String {
    let response = ok(handle.capture(&capture(key, text)).await);
    let body = value_of(&response);
    assert_eq!(
        body["status"], "committed",
        "the capture commits its receipt"
    );
    body["result"]["candidate_ids"][0]
        .as_str()
        .expect("candidate id")
        .to_string()
}

/// Register the test subject on the Mind's space, as the embedding host does before capture.
async fn ensure_subject(store: &MindStore, space_id: &str) {
    let adapter = mindcore_spoke::MemoryAdapter::new(false);
    let caller: spoke_schemas::Principal = serde_json::from_value(json!({
        "principal_id": OWNER,
        "extensions": {},
    }))
    .expect("principal");
    let mut tx = store.pool().begin().await.expect("begin the subject write");
    let stage = adapter
        .ensure_subject(
            &mut tx,
            &caller,
            json!({ "space_id": space_id, "subject_id": SUBJECT }),
        )
        .await;
    assert!(stage.is_ok(), "the subject registers: {stage:?}");
    tx.commit().await.expect("commit the subject");
}

async fn count_records(pool: &sqlx::SqlitePool) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory_records")
        .fetch_one(pool)
        .await
        .expect("count record rows")
}

async fn count_intents(pool: &sqlx::SqlitePool) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory_journal")
        .fetch_one(pool)
        .await
        .expect("count intent rows")
}

async fn intent_states(pool: &sqlx::SqlitePool) -> Vec<String> {
    sqlx::query_scalar::<_, String>("SELECT state FROM memory_journal")
        .fetch_all(pool)
        .await
        .expect("intent states")
}

#[tokio::test]
async fn capture_review_then_recall_returns_the_record_for_the_named_subject() {
    let (dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    ensure_subject(&store, created.id.as_str()).await;
    let handle = ok(Mind::open(&store, &owner, &created.id).await)
        .with_journal_publisher(journal_dir(dir.path()));

    let candidate_id = capture_candidate(&handle, "cap-facade-recall", "the gate is shut").await;
    let reviewed = ok(handle
        .review(&candidate_id, &promote(), "rev-facade-recall")
        .await);
    let receipt = value_of(&reviewed);
    assert_eq!(
        receipt["status"], "committed",
        "review answers a settled receipt"
    );
    let record_id = receipt["result"]["record_ids"][0]
        .as_str()
        .expect("a retained record id")
        .to_string();

    let (recall, _) = recall_all();
    let page = value_of(&ok(handle.recall(&recall).await));
    let records = page["records"].as_array().expect("a records page");
    assert!(
        records
            .iter()
            .any(|record| record["record_id"] == record_id),
        "recall after capture and review returns the record: {page}"
    );

    // The caller-named subject is passed through, never defaulted: a subject the caller did
    // not write holds no records, while the candidate and the record sit on `subj_wren`.
    let other = Recall {
        subject: "subj_someone_else",
        ..recall_all().0
    };
    let other_page = value_of(&ok(handle.recall(&other).await));
    assert_eq!(
        other_page["records"].as_array().expect("records").len(),
        0,
        "nothing leaks onto another subject"
    );
    let subject: String =
        sqlx::query_scalar("SELECT subject_id FROM memory_records WHERE record_id = ?")
            .bind(&record_id)
            .fetch_one(&store.pool())
            .await
            .expect("the record's subject row");
    assert_eq!(subject, SUBJECT);

    // journal_dir published the projection files under the scratch root at their own paths.
    let published = dir.path().join("records").join(format!("{record_id}.md"));
    assert!(
        published.exists(),
        "the record projection was published under the journal root"
    );
}

#[tokio::test]
async fn a_refused_publication_leaves_nothing_staged_and_the_same_key_then_settles() {
    let (dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    ensure_subject(&store, created.id.as_str()).await;
    let candidate_id = {
        let handle = ok(Mind::open(&store, &owner, &created.id).await)
            .with_journal_publisher(journal_dir(dir.path()));
        capture_candidate(&handle, "cap-refused-publish", "a note to refuse").await
    };

    // First attempt: the publisher refuses with its own Settlement refusal.
    let refused = SpokeReject {
        code: SpokeRejectCode::InternalError,
        message: "the medium is full".to_string(),
        details: Some(serde_json::Map::from_iter([(
            "retry_class".to_string(),
            Value::String("retryable".to_string()),
        )])),
    };
    let expected = refused.clone();
    let failing = ok(Mind::open(&store, &owner, &created.id).await)
        .with_journal_publisher(move |_| Err(SpiError::Settlement(expected.clone())));
    let reject = reject_of(
        failing
            .review(&candidate_id, &promote(), "rev-refused-publish")
            .await,
    );
    assert_eq!(
        reject, refused,
        "the publisher's Settlement refusal travels unchanged"
    );

    // Nothing is durable: no record row, no intent, and the candidate is still pending.
    assert_eq!(count_records(&store.pool()).await, 0, "no record row");
    assert_eq!(count_intents(&store.pool()).await, 0, "no intent");
    let status: String =
        sqlx::query_scalar("SELECT status FROM memory_candidates WHERE candidate_id = ?")
            .bind(&candidate_id)
            .fetch_one(&store.pool())
            .await
            .expect("the candidate row");
    assert_eq!(status, "pending", "the candidate stays pending");

    // A non-Settlement publisher refusal answers the retryable INTERNAL_ERROR instead.
    let io_failing = ok(Mind::open(&store, &owner, &created.id).await)
        .with_journal_publisher(move |_| Err(SpiError::EmbeddingFailed("disk gone".to_string())));
    let reject = reject_of(
        io_failing
            .review(&candidate_id, &promote(), "rev-refused-publish")
            .await,
    );
    assert_eq!(reject.code, SpokeRejectCode::InternalError);
    assert_eq!(retry_class(&reject), "retryable");
    assert_eq!(count_records(&store.pool()).await, 0, "still no record row");
    assert_eq!(count_intents(&store.pool()).await, 0, "still no intent");

    // The same key then settles once the medium accepts the publication.
    let working = ok(Mind::open(&store, &owner, &created.id).await)
        .with_journal_publisher(journal_dir(dir.path()));
    let reviewed = ok(working
        .review(&candidate_id, &promote(), "rev-refused-publish")
        .await);
    assert_eq!(
        value_of(&reviewed)["status"],
        "committed",
        "the same key settles after the refused publication"
    );
    assert_eq!(count_records(&store.pool()).await, 1, "one record row");

    // Critical 1: the same key replays. Consuming the candidate bumped its revision to 2 —
    // asserted here so the next assertion cannot silently pass on an unchanged row — so a
    // facade that re-read the row would send a different `expected_revision`, change the
    // payload digest and be answered `IdempotencyConflict`. The settled receipt replays
    // instead, byte-identically.
    let candidate_revision: i64 =
        sqlx::query_scalar("SELECT revision FROM memory_candidates WHERE candidate_id = ?")
            .bind(&candidate_id)
            .fetch_one(&store.pool())
            .await
            .expect("the consumed candidate row");
    assert_eq!(
        candidate_revision, 2,
        "consumption moved the stored revision"
    );
    let replayed = ok(working
        .review(&candidate_id, &promote(), "rev-refused-publish")
        .await);
    assert_eq!(
        value_of(&replayed),
        value_of(&reviewed),
        "an identical review replays the recorded receipt"
    );
    assert_eq!(
        count_records(&store.pool()).await,
        1,
        "a replay writes no second record"
    );
    assert_eq!(
        count_intents(&store.pool()).await,
        1,
        "a replay stages no second intent"
    );

    // A different decision under the same key stays a conflict — the digest still guards it.
    let different =
        serde_json::from_value::<Decision>(json!({ "action": "drop" })).expect("drop decision");
    let conflict = reject_of(
        working
            .review(&candidate_id, &different, "rev-refused-publish")
            .await,
    );
    assert_eq!(
        conflict.code,
        SpokeRejectCode::IdempotencyConflict,
        "a different payload under one key is refused"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_settle_answers_outcome_uncertain_and_leaves_the_intent_pending() {
    let (dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    ensure_subject(&store, created.id.as_str()).await;
    let pool = store.pool();
    let journal_root = dir.path().to_path_buf();
    let handle = ok(Mind::open(&store, &owner, &created.id).await)
        .with_journal_publisher(journal_dir(&journal_root));
    let candidate_id = capture_candidate(
        &handle,
        "cap-refused-settle",
        "a note whose settlement fails",
    )
    .await;

    // The publisher queues an interference task on the store's one-connection pool: the
    // staging transaction still holds that connection, so the task cannot acquire until the
    // staging commit releases it. sqlx's pool issues connections FIFO-fair in registration
    // order, and the task proves it registered before the publisher returns: the spawned task
    // polls its acquire future once (registration happens on the first poll, while the one
    // connection is still checked out) and signals that through the handshake channel. The
    // settlement transaction asks only after the commit, so it is always behind the
    // interference task in the queue, and the settle always reads a moved intent — the order
    // is the semaphore's, never the scheduler's.
    let interference_pool = pool.clone();
    let handle =
        ok(Mind::open(&store, &owner, &created.id).await).with_journal_publisher(move |targets| {
            journal_dir(&journal_root)(targets)?;
            let interference_pool = interference_pool.clone();
            let (registered, is_registered) = mpsc::channel::<()>();
            tokio::spawn(async move {
                let acquire = interference_pool.acquire();
                tokio::pin!(acquire);
                assert!(
                    futures::poll!(acquire.as_mut()).is_pending(),
                    "the staging transaction still holds the one connection"
                );
                registered.send(()).expect("signal the registration");
                let mut connection = acquire.await.expect("the one connection");
                sqlx::query("UPDATE memory_journal SET state = 'deferred'")
                    .execute(&mut *connection)
                    .await
                    .expect("move the intent out of pending");
            });
            is_registered
                .recv()
                .expect("the interference task registered its acquisition");
            Ok(())
        });

    let reject = reject_of(
        handle
            .review(&candidate_id, &promote(), "rev-refused-settle")
            .await,
    );
    assert_eq!(
        reject.code,
        SpokeRejectCode::OutcomeUncertain,
        "a refused settle answers OUTCOME_UNCERTAIN: {reject:?}"
    );
    assert_eq!(retry_class(&reject), "uncertain");
    assert_eq!(
        intent_states(&pool).await,
        vec!["deferred".to_string()],
        "the intent is left unsettled, never silently applied"
    );
    assert_eq!(
        count_records(&pool).await,
        1,
        "the staged record is durable"
    );
}

/// The plan's step 1 is a real read of the candidate: a pending candidate imported at a
/// non-1 revision stages at that revision, and its settled same-key replay still reaches the
/// recorded receipt. A constant `expected_revision` would refuse the first call with
/// `RevisionConflict`.
#[tokio::test]
async fn a_pending_candidate_at_a_non_one_revision_reviews_and_replays() {
    let (dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    ensure_subject(&store, created.id.as_str()).await;
    let handle = ok(Mind::open(&store, &owner, &created.id).await)
        .with_journal_publisher(journal_dir(dir.path()));
    let candidate_id =
        capture_candidate(&handle, "cap-imported-revision", "imported at five").await;

    // An imported candidate keeps its own revision (transfer/activate.rs): make this one 5.
    sqlx::query("UPDATE memory_candidates SET revision = 5 WHERE candidate_id = ?")
        .bind(&candidate_id)
        .execute(&store.pool())
        .await
        .expect("raise the pending candidate revision");

    let reviewed = ok(handle
        .review(&candidate_id, &promote(), "rev-imported-revision")
        .await);
    let receipt = value_of(&reviewed);
    assert_eq!(
        receipt["status"], "committed",
        "revision 5 stages and settles"
    );
    let stored: i64 =
        sqlx::query_scalar("SELECT revision FROM memory_candidates WHERE candidate_id = ?")
            .bind(&candidate_id)
            .fetch_one(&store.pool())
            .await
            .expect("the consumed candidate row");
    assert_eq!(stored, 6, "consumption bumped the imported revision");

    let replayed = ok(handle
        .review(&candidate_id, &promote(), "rev-imported-revision")
        .await);
    assert_eq!(
        value_of(&replayed),
        receipt,
        "the same key replays the recorded receipt after a non-1 revision"
    );
    let different =
        serde_json::from_value::<Decision>(json!({ "action": "drop" })).expect("drop decision");
    let conflict = reject_of(
        handle
            .review(&candidate_id, &different, "rev-imported-revision")
            .await,
    );
    assert_eq!(
        conflict.code,
        SpokeRejectCode::IdempotencyConflict,
        "a different decision under one key still refuses"
    );
}

/// The public publisher creates a root that does not exist yet and leaves the target's exact
/// bytes at the target's own path; the durable directory chain it syncs is unit-tested in
/// `journal.rs` (deepest created directory first, ending at the nearest existing ancestor).
#[tokio::test]
async fn the_public_publisher_creates_and_publishes_a_nested_root() {
    let dir = TempDir::new().expect("temporary directory");
    let root = dir.path().join("nested/publish/root");
    assert!(!root.exists(), "the root starts absent");
    let publisher = mindcore_runtime::journal_dir(&root);
    publisher(&[json!({
        "path": "records/rec_nested.md",
        "content": "exact published bytes",
    })])
    .expect("publish into a root that does not exist yet");
    assert_eq!(
        std::fs::read_to_string(root.join("records").join("rec_nested.md"))
            .expect("the published target"),
        "exact published bytes"
    );
}

/// A relative root is anchored at the current directory: a root that does not exist yet is
/// created and published instead of refusing on the empty parent path. The scratch tree sits
/// directly under the package root cargo runs tests from — no pre-existing intermediate can
/// stand in for the current-directory anchor — and is removed before the assertions.
#[tokio::test]
async fn the_public_publisher_handles_a_relative_root_that_does_not_exist_yet() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after the epoch")
        .as_nanos();
    let relative = format!("journal-relative-{}-{unique}", std::process::id());
    let root = std::path::PathBuf::from(&relative);
    assert!(!root.exists(), "the relative root starts absent");
    let publisher = mindcore_runtime::journal_dir(&relative);
    let published = publisher(&[json!({
        "path": "records/rec_relative.md",
        "content": "relative root bytes",
    })]);
    let read_back = std::fs::read_to_string(root.join("records").join("rec_relative.md"));
    std::fs::remove_dir_all(&root).ok();
    published.expect("a relative root publishes");
    assert_eq!(
        read_back.expect("the published target"),
        "relative root bytes",
        "the relative root holds the target at its own path"
    );
}

// ---------- Task 3: knowledge operations on the handle ----------

/// One plain entry the test writes; the store assigns and checks the revision.
fn entry(entry_id: &str, name: &str) -> KnowledgeEntry {
    KnowledgeEntry {
        adoption_anchor: None,
        body: KnowledgeEntryBody::default(),
        canonical_name: KnowledgeEntryCanonicalName::try_from(name.to_string())
            .expect("a valid canonical name"),
        created_at: None,
        disclosure: None,
        entry_id: entry_id.to_string(),
        entry_type: "character".to_string(),
        extensions: std::collections::HashMap::new(),
        modules: std::collections::HashMap::new(),
        owner: None,
        revision: Some(1),
        // SAFETY: the wire baseline one is not zero.
        schema_version: unsafe { std::num::NonZeroU64::new_unchecked(1) },
        source_anchor: None,
        status: "confirmed".to_string(),
        updated_at: None,
    }
}

/// One extra live grant row for `principal` in the Mind's space, carrying only
/// `capabilities` — the limited callers of the refusal tests.
async fn grant(store: &MindStore, space_id: &str, principal: &str, capabilities: &[&str]) {
    let row = MemoryGrantRow {
        grant_id: mint("grt"),
        space_id: SpaceId::new(space_id),
        subject_id: None,
        principal_id: CallerId::new(principal),
        capabilities: capabilities.iter().map(|capability| capability.to_string()).collect(),
        context_ids: Vec::new(),
        shared_access: false,
        expires_at: None,
        revoked_at: None,
        revision: Number::from(1),
        created_at: Utc::now().to_rfc3339(),
        extensions: Map::new(),
    };
    let mut tx = store.pool().begin().await.expect("begin the grant write");
    SqliteMemoryRepository
        .insert_grant(&mut tx, &row)
        .await
        .expect("insert the grant");
    tx.commit().await.expect("commit the grant");
}

#[tokio::test]
async fn knowledge_write_then_read_by_revision_answers_the_stored_entry() {
    let (_dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    let handle = ok(Mind::open(&store, &owner, &created.id).await);

    let written = ok(handle
        .knowledge_put(entry("ke_fact", "fact one"), None)
        .await);
    assert_eq!(written.revision, Some(1), "the create assigns revision 1");

    let current = ok(handle.knowledge_get("ke_fact", None).await);
    assert_eq!(current.entry_id, "ke_fact");
    assert_eq!(current.revision, Some(1));

    let at_revision = ok(handle.knowledge_get("ke_fact", Some(1)).await);
    assert_eq!(
        at_revision.entry_id, "ke_fact",
        "the revision read answers the stored entry"
    );
    assert_eq!(at_revision.revision, Some(1));

    let listed = ok(handle.knowledge_list().await);
    assert!(
        listed.iter().any(|entry| entry.entry_id == "ke_fact"),
        "the list carries the written entry beside the carrier"
    );
    assert!(
        listed.iter().any(|entry| entry.entry_id == created.carrier_entry_id),
        "the list reads the Mind's own collection, carrier included"
    );
}

#[tokio::test]
async fn a_stale_revision_refuses_stored_revision_stale() {
    let (_dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    let handle = ok(Mind::open(&store, &owner, &created.id).await);

    ok(handle.knowledge_put(entry("ke_fact", "fact one"), None).await);
    ok(handle
        .knowledge_put(entry("ke_fact", "fact two"), Some(1))
        .await);

    assert_eq!(
        reject_code(handle.knowledge_put(entry("ke_fact", "fact three"), Some(1)).await),
        SpokeRejectCode::StoredRevisionStale,
        "a put against a moved revision refuses the store's own stale refusal"
    );
    let current = ok(handle.knowledge_get("ke_fact", None).await);
    assert_eq!(current.revision, Some(2), "the refused write never landed");
}

#[tokio::test]
async fn a_caller_without_knowledge_write_is_refused_and_the_row_is_untouched() {
    let (_dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    let owner_handle = ok(Mind::open(&store, &owner, &created.id).await);
    ok(owner_handle
        .knowledge_put(entry("ke_fact", "fact one"), None)
        .await);

    let reader = CallerId::new("caller_reader");
    grant(&store, created.id.as_str(), "caller_reader", &["knowledge_read"]).await;
    let reader_handle = ok(Mind::open(&store, &reader, &created.id).await);

    let refuse = reject_of(reader_handle
        .knowledge_put(entry("ke_fact", "defaced"), Some(1))
        .await);
    assert_eq!(
        refuse.code,
        SpokeRejectCode::NotAuthorized,
        "a caller without knowledge_write is refused"
    );

    let after = ok(owner_handle.knowledge_get("ke_fact", None).await);
    assert_eq!(after.revision, Some(1), "the row is untouched");
    assert_eq!(
        after.canonical_name.as_str(),
        "fact one",
        "the payload the refused caller targeted is unchanged"
    );
}

#[tokio::test]
async fn a_caller_without_knowledge_read_reads_nothing() {
    let (_dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    let owner_handle = ok(Mind::open(&store, &owner, &created.id).await);
    ok(owner_handle
        .knowledge_put(entry("ke_fact", "fact one"), None)
        .await);

    let writer = CallerId::new("caller_writer");
    grant(&store, created.id.as_str(), "caller_writer", &["knowledge_write"]).await;
    let writer_handle = ok(Mind::open(&store, &writer, &created.id).await);

    assert_eq!(
        reject_code(writer_handle.knowledge_get("ke_fact", None).await),
        SpokeRejectCode::NotAuthorized,
        "a caller without knowledge_read reads no entry"
    );
    assert_eq!(
        reject_code(writer_handle.knowledge_list().await),
        SpokeRejectCode::NotAuthorized,
        "a caller without knowledge_read reads no list"
    );
}
