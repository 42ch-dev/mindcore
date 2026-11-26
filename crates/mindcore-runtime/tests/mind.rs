//! The Mind handle `mindcore_runtime::Mind`: `open` through the registry under the caller's
//! live grants, the registry's own `NotFound` for a Mind the caller cannot see, and a frozen
//! Mind opening because the handle acts on no status from the row.

use chrono::Utc;
use mindcore_base::boundary::CallerId;
use mindcore_base::ids::SubjectId;
use mindcore_base::ids::{mint, MindId, SpaceId};
use mindcore_base::ir::MemoryRepository;
use mindcore_base::ir::{MemoryGrantRow, MemoryJournalRow};
use mindcore_runtime::journal_dir;
use mindcore_runtime::minds::{self, CreateMind};
use mindcore_runtime::{Capture, Deliberation, Mind, MindProviders, Recall};
use mindcore_spi::SpiError;
use mindcore_sqlite::{MindStore, SqliteMemoryRepository};
use serde_json::{json, Map, Number, Value};
use spoke_operations::{SpokeReject, SpokeRejectCode, SpokeResult};
use spoke_schemas::data::knowledge_entry::{KnowledgeEntryBody, KnowledgeEntryCanonicalName};
use spoke_schemas::memory_candidates_review_request::Decision;
use spoke_schemas::KnowledgeEntry;
use std::sync::{mpsc, Arc};
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
                // One poll with a no-op waker: the future must be pending because the staging
                // transaction still holds the pool's one connection, and this first poll is
                // what registers the task in the pool's FIFO queue.
                let pending = std::future::poll_fn(|cx| {
                    let polled = std::future::Future::poll(acquire.as_mut(), cx);
                    assert!(
                        polled.is_pending(),
                        "the one connection is still checked out"
                    );
                    std::task::Poll::Ready(())
                })
                .await;
                let _ = pending;
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

/// The `OUTCOME_UNCERTAIN` recovery contract: a failure after the staging commit answers the
/// uncertain outcome once, and a same-key repeat continues through the fold — publish, then
/// settle — instead of answering the uncertain outcome again. The repeat settles the intent,
/// the record stays exactly the one the first attempt staged, and the receipt answers
/// committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_repeat_after_outcome_uncertain_continues_the_fold_and_settles() {
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
        "cap-uncertain-recovery",
        "a note whose first settlement fails",
    )
    .await;

    // The interference handshake: a task registered on the one-connection pool moves the
    // intent out of pending the moment the staging commit releases the connection, so the
    // first settle is refused — the `OUTCOME_UNCERTAIN` path.
    let interference_pool = pool.clone();
    let blocking =
        ok(Mind::open(&store, &owner, &created.id).await).with_journal_publisher(move |targets| {
            journal_dir(&journal_root)(targets)?;
            let interference_pool = interference_pool.clone();
            let (registered, is_registered) = mpsc::channel::<()>();
            tokio::spawn(async move {
                let acquire = interference_pool.acquire();
                tokio::pin!(acquire);
                std::future::poll_fn(|cx| {
                    let polled = std::future::Future::poll(acquire.as_mut(), cx);
                    assert!(
                        polled.is_pending(),
                        "the one connection is still checked out"
                    );
                    std::task::Poll::Ready(())
                })
                .await;
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

    let first = reject_of(
        blocking
            .review(&candidate_id, &promote(), "rev-uncertain-recovery")
            .await,
    );
    assert_eq!(
        first.code,
        SpokeRejectCode::OutcomeUncertain,
        "the refused settle answers OUTCOME_UNCERTAIN"
    );

    // Restore the intent to pending — the crash window the contract covers is exactly this
    // state: staging durable, intent unsettled — and repeat the review with the same key.
    sqlx::query("UPDATE memory_journal SET state = 'pending'")
        .execute(&store.pool())
        .await
        .expect("restore the intent to pending");
    let working = ok(Mind::open(&store, &owner, &created.id).await)
        .with_journal_publisher(journal_dir(dir.path()));
    let recovered = ok(working
        .review(&candidate_id, &promote(), "rev-uncertain-recovery")
        .await);
    assert_eq!(
        value_of(&recovered)["status"],
        "committed",
        "the same-key repeat continues the fold and settles"
    );
    assert_eq!(
        count_records(&pool).await,
        1,
        "the record is exactly the one the first attempt staged"
    );
    assert_eq!(
        intent_states(&pool).await,
        vec!["applied".to_string()],
        "the intent is settled, never re-staged"
    );

    // A further repeat is the ordinary replay of a settled receipt.
    let replayed = ok(working
        .review(&candidate_id, &promote(), "rev-uncertain-recovery")
        .await);
    assert_eq!(
        value_of(&replayed),
        value_of(&recovered),
        "a settled key replays the committed receipt"
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

/// The root is the host's: the publisher never creates it. A missing root is refused with the
/// requirement named, and nothing is created anywhere.
#[tokio::test]
async fn the_public_publisher_refuses_a_root_that_does_not_exist() {
    let dir = TempDir::new().expect("temporary directory");
    let root = dir.path().join("nested/publish/root");
    assert!(!root.exists(), "the root starts absent");
    let publisher = mindcore_runtime::journal_dir(&root);
    let published = publisher(&[json!({
        "path": "records/rec_nested.md",
        "content": "exact published bytes",
    })]);
    let error = published.expect_err("a missing root is refused");
    let message = format!("{error:?}");
    assert!(
        message.contains("does not exist"),
        "the refusal names the missing root requirement: {message}"
    );
    assert!(
        !dir.path().join("nested").exists(),
        "the publisher created nothing"
    );
}

/// A root the host created receives a nested subtree beneath it: the publisher creates the
/// directories between the root and the target and lands the exact bytes at the target's own
/// path.
#[tokio::test]
async fn the_public_publisher_publishes_a_nested_subtree_under_an_existing_root() {
    let dir = TempDir::new().expect("temporary directory");
    let root = dir.path().join("journal");
    std::fs::create_dir_all(&root).expect("the host creates the root");
    let publisher = mindcore_runtime::journal_dir(&root);
    publisher(&[json!({
        "path": "deeply/nested/records/rec_nested.md",
        "content": "exact published bytes",
    })])
    .expect("publish beneath an existing root");
    assert_eq!(
        std::fs::read_to_string(
            root.join("deeply")
                .join("nested")
                .join("records")
                .join("rec_nested.md")
        )
        .expect("the published target"),
        "exact published bytes"
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
        capabilities: capabilities
            .iter()
            .map(|capability| capability.to_string())
            .collect(),
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
        listed
            .iter()
            .any(|entry| entry.entry_id == created.carrier_entry_id),
        "the list reads the Mind's own collection, carrier included"
    );
}

#[tokio::test]
async fn a_stale_revision_refuses_stored_revision_stale() {
    let (_dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    let handle = ok(Mind::open(&store, &owner, &created.id).await);

    ok(handle
        .knowledge_put(entry("ke_fact", "fact one"), None)
        .await);
    ok(handle
        .knowledge_put(entry("ke_fact", "fact two"), Some(1))
        .await);

    assert_eq!(
        reject_code(
            handle
                .knowledge_put(entry("ke_fact", "fact three"), Some(1))
                .await
        ),
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
    grant(
        &store,
        created.id.as_str(),
        "caller_reader",
        &["knowledge_read"],
    )
    .await;
    let reader_handle = ok(Mind::open(&store, &reader, &created.id).await);

    let refuse = reject_of(
        reader_handle
            .knowledge_put(entry("ke_fact", "defaced"), Some(1))
            .await,
    );
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
    grant(
        &store,
        created.id.as_str(),
        "caller_writer",
        &["knowledge_write"],
    )
    .await;
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

// ---------- Task 4: cognition read and the space-level deliberate ----------

/// One promotion over the supplied evidence; `record_ids` names no memory record
/// because a subjectless snapshot serves no memory page.
struct SettlingReasoner;

#[async_trait::async_trait]
impl mindcore_spi::Reasoner for SettlingReasoner {
    async fn reason_structured(
        &self,
        request: mindcore_spi::StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        assert_eq!(
            request.task_kind,
            mindcore_spi::CognitiveTaskKind::Deliberate
        );
        let payload: Value =
            serde_json::from_str(request.user_payload).expect("the payload is JSON");
        assert!(
            payload["records"]
                .as_array()
                .is_some_and(|records| records.is_empty()),
            "a subjectless snapshot serves no memory page: {}",
            request.user_payload
        );
        Ok(json!({
            "recommendation": { "kind": "promote", "target_kind": "belief" },
            "rationale": "the supplied evidence carries the north gate state",
            "evidence_ids": ["ev_gate"],
            "record_ids": [],
            "record": {
                "kind": "belief",
                "order": 1,
                "q": {
                    "value": 0.8,
                    "assertion": "belief_attribution",
                    "method": "two_witness_reports",
                    "calibration": "uncalibrated"
                },
                "assessor_role": "holder_self_report",
                "basis": { "kind": "evidence", "evidence": [] }
            }
        }))
    }
}

/// A verdict citing a memory record the subjectless snapshot never admitted.
struct CitingReasoner;

#[async_trait::async_trait]
impl mindcore_spi::Reasoner for CitingReasoner {
    async fn reason_structured(
        &self,
        _request: mindcore_spi::StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        Ok(json!({
            "recommendation": { "kind": "promote", "target_kind": "belief" },
            "rationale": "rests on a record this snapshot never served",
            "evidence_ids": ["ev_gate"],
            "record_ids": ["rec_not_admitted"],
            "record": {
                "kind": "belief",
                "order": 1,
                "q": {
                    "value": 0.8,
                    "assertion": "belief_attribution",
                    "method": "two_witness_reports",
                    "calibration": "uncalibrated"
                },
                "assessor_role": "holder_self_report"
            }
        }))
    }
}

struct AutoPolicy;

#[async_trait::async_trait]
impl mindcore_spi::PolicyHook for AutoPolicy {
    async fn evaluate_settlement(
        &self,
        _draft: mindcore_spi::SettlementDraft<'_>,
    ) -> Result<mindcore_spi::PolicyDecision, SpiError> {
        Ok(mindcore_spi::PolicyDecision::AutoSettle)
    }
}

/// The carrier as the Mind's target: the facade's holder is the row's carrier.
fn cognition_target(carrier_entry_id: &str) -> spoke_schemas::data::cognition::CognitionTarget {
    serde_json::from_value(json!({
        "carrier": { "kind": "knowledge_entry", "namespace": "mindcore", "id": carrier_entry_id },
        "target_id": "prop_north_gate",
        "kind": "proposition",
    }))
    .expect("a valid cognition target")
}

/// One deliberation over the fixed evidence, with the providers the test injects:
/// `with_providers` consumes the handle, so each call opens its own with them.
async fn deliberate(
    store: &MindStore,
    caller: &CallerId,
    mind_id: &MindId,
    target: &spoke_schemas::data::cognition::CognitionTarget,
    reasoner: Arc<dyn mindcore_spi::Reasoner>,
) -> SpokeResult<mindcore_spi::DeliberateOutcome> {
    let evidence = [mindcore_spi::Evidence {
        id: "ev_gate",
        text: "the north gate is closed at dusk",
    }];
    let providers = MindProviders {
        reasoner: Some(reasoner),
        policy_hook: Some(Arc::new(AutoPolicy)),
        ..MindProviders::default()
    };
    let handle = ok(Mind::open(store, caller, mind_id).await).with_providers(providers);
    handle
        .deliberate(&Deliberation {
            target,
            evidence: &evidence,
            timeout: std::time::Duration::from_secs(5),
        })
        .await
}

/// One unsettled durable intent for `subject_id` in the Mind's space, written
/// directly: the served absorb can leave one, and the facade's check is the
/// space-level one.
async fn settle_never(store: &MindStore, space_id: &str, subject_id: &str) {
    let row = MemoryJournalRow {
        seq: 0,
        receipt_id: mint("rct"),
        space_id: SpaceId::new(space_id),
        subject_id: SubjectId::new(subject_id),
        requester: CallerId::new(OWNER),
        operation: "memory.candidates.review".to_string(),
        idempotency_key: Some("intent_unsettled".to_string()),
        state: "pending".to_string(),
        targets: Vec::new(),
        result: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
    };
    let mut tx = store.pool().begin().await.expect("begin the intent write");
    SqliteMemoryRepository
        .insert_journal_intent(&mut tx, &row)
        .await
        .expect("insert the unsettled intent");
    tx.commit().await.expect("commit the intent");
}

#[tokio::test]
async fn cognition_read_answers_none_then_the_judgment_and_mind_state() {
    let (_dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    let handle = ok(Mind::open(&store, &owner, &created.id).await);

    assert!(
        ok(handle.cognition_read().await).is_none(),
        "a fresh carrier holds no judgment, which is a state and not a refusal"
    );

    // The plan's own consequence: a fresh Mind has no judgment to read, so the
    // carrier is written through the one validation boundary — an explicit
    // `knowledge_put` carrying one cognition record.
    let carrier = ok(handle.knowledge_get(handle.carrier_entry_id(), None).await);
    let revision = carrier.revision.expect("a stored carrier has a revision");
    let record = json!({
        "kind": "belief",
        "order": 1,
        "record_id": "cog_one",
        "holder": {
            "kind": "knowledge_entry",
            "namespace": "mindcore",
            "id": created.carrier_entry_id,
        },
        "target": {
            "carrier": {
                "kind": "knowledge_entry",
                "namespace": "mindcore",
                "id": created.carrier_entry_id,
            },
            "target_id": "prop_north_gate",
            "kind": "proposition",
        },
        "assessor": {
            "kind": "principal",
            "namespace": "mindcore",
            "id": OWNER,
        },
        "assessor_role": "holder_self_report",
        "basis": {
            "kind": "evidence",
            "evidence": [{ "kind": "memory_record", "namespace": "mindcore", "id": "rec_gate" }],
        },
        "assessment_context": { "context_id": created.id.as_str() },
        "assessed_at": "2026-11-20T09:00:00Z",
        "effective_at": "2026-11-20T09:00:00Z",
        "view": "current",
        "q": {
            "value": 0.8,
            "assertion": "belief_attribution",
            "method": "two_witness_reports",
            "calibration": "uncalibrated",
        },
        "extensions": {},
    });
    let next = mindcore_base::cognition::append_cognition_record(&carrier, record)
        .expect("one valid record appends to the carrier");
    let written = ok(handle.knowledge_put(next, Some(revision)).await);
    assert_eq!(
        written.revision,
        Some(revision + 1),
        "the carrier update is one explicit revision step"
    );

    let cognition = ok(handle.cognition_read().await).expect("one record is a judgment");
    assert_eq!(cognition.records.len(), 1, "the one current-view record");
    assert_eq!(
        cognition.records[0]["record_id"],
        json!("cog_one"),
        "the judgment is the record the carrier holds"
    );
    assert_eq!(
        cognition.mind_state["mind_state_id"],
        json!(created.id.as_str()),
        "the Mind's id names its derived state"
    );
    assert_eq!(
        cognition.mind_state["holder_entry_id"],
        json!(created.carrier_entry_id),
        "the state descends from the Mind's carrier"
    );
    assert!(
        cognition.carrier_revision >= 1,
        "the read served an assigned carrier revision"
    );
}

#[tokio::test]
async fn deliberate_without_a_space_wide_recall_grant_is_refused_and_writes_nothing() {
    let (_dir, store) = fixture().await;
    let created = ok(minds::create(&store, &request(OWNER)).await);
    let owner = CallerId::new(OWNER);
    let owner_handle = ok(Mind::open(&store, &owner, &created.id).await);

    // The knowledge capabilities admit the carrier; no `recall` grant was ever
    // written for this caller, so the space-level revalidation must refuse.
    let knowledge_only = CallerId::new("caller_knowledge_only");
    grant(
        &store,
        created.id.as_str(),
        "caller_knowledge_only",
        &["knowledge_read", "knowledge_write"],
    )
    .await;
    let target = cognition_target(&created.carrier_entry_id);
    let refuse = reject_of(
        deliberate(
            &store,
            &knowledge_only,
            &created.id,
            &target,
            Arc::new(SettlingReasoner),
        )
        .await,
    );
    assert_eq!(
        refuse.code,
        SpokeRejectCode::NotAuthorized,
        "a caller without the space-wide recall grant is refused"
    );

    assert!(
        ok(owner_handle.cognition_read().await).is_none(),
        "the refused deliberation wrote no record"
    );
}

#[tokio::test]
async fn an_unsettled_intent_in_another_subject_refuses_deliberate() {
    let (_dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    let handle = ok(Mind::open(&store, &owner, &created.id).await);

    settle_never(&store, created.id.as_str(), "subj_other").await;

    let target = cognition_target(&created.carrier_entry_id);
    let refuse = reject_of(
        deliberate(
            &store,
            &owner,
            &created.id,
            &target,
            Arc::new(SettlingReasoner),
        )
        .await,
    );
    assert_eq!(refuse.code, SpokeRejectCode::InternalError);
    let details = refuse.details.as_ref().expect("structured details");
    assert_eq!(
        details.get("reason").and_then(Value::as_str),
        Some("UNRECONCILED"),
        "the space-level withhold"
    );
    assert_eq!(
        details.get("space_id").and_then(Value::as_str),
        Some(created.id.as_str()),
        "the refusal names the space and no subject"
    );
    assert!(details.get("subject_id").is_none());
    assert!(
        ok(handle.cognition_read().await).is_none(),
        "the refused deliberation wrote no record"
    );
}

#[tokio::test]
async fn a_verdict_citing_a_memory_record_is_refused() {
    let (_dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    let handle = ok(Mind::open(&store, &owner, &created.id).await);

    let target = cognition_target(&created.carrier_entry_id);
    let refuse = reject_of(
        deliberate(
            &store,
            &owner,
            &created.id,
            &target,
            Arc::new(CitingReasoner),
        )
        .await,
    );
    assert_eq!(
        refuse.code,
        SpokeRejectCode::InternalError,
        "a citation outside the subjectless snapshot is refused, never widened"
    );
    assert!(
        ok(handle.cognition_read().await).is_none(),
        "the refused verdict wrote no record"
    );
}

/// The subjectless promotion: a `prior` basis needs no memory record, which is
/// the only basis a space-level snapshot can admit — it carries no page to cite.
fn prior_basis_verdict() -> Value {
    json!({
        "recommendation": { "kind": "promote", "target_kind": "belief" },
        "rationale": "the gate has been closed at dusk all season",
        "evidence_ids": ["ev_gate"],
        "record_ids": [],
        "record": {
            "kind": "belief",
            "order": 1,
            "q": {
                "value": 0.8,
                "assertion": "belief_attribution",
                "method": "two_witness_reports",
                "calibration": "uncalibrated"
            },
            "assessor_role": "holder_self_report",
            "basis": {
                "kind": "prior",
                "description": "every dusk patrol reported the gate closed"
            }
        }
    })
}

struct PriorBasisReasoner;

#[async_trait::async_trait]
impl mindcore_spi::Reasoner for PriorBasisReasoner {
    async fn reason_structured(
        &self,
        _request: mindcore_spi::StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        Ok(prior_basis_verdict())
    }
}

/// Settles nothing itself: it leaves an unsettled intent in the Mind's space for
/// another subject while the snapshot is already past, so the refusal must come
/// from the commit's space-level withhold.
struct RecordingIntentReasoner {
    pool: sqlx::SqlitePool,
    space_id: String,
}

#[async_trait::async_trait]
impl mindcore_spi::Reasoner for RecordingIntentReasoner {
    async fn reason_structured(
        &self,
        _request: mindcore_spi::StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        let row = MemoryJournalRow {
            seq: 0,
            receipt_id: mint("rct"),
            space_id: SpaceId::new(self.space_id.as_str()),
            subject_id: SubjectId::new("subj_other"),
            requester: CallerId::new(OWNER),
            operation: "memory.candidates.review".to_string(),
            idempotency_key: Some("intent_after_snapshot".to_string()),
            state: "pending".to_string(),
            targets: Vec::new(),
            result: None,
            created_at: Utc::now().to_rfc3339(),
            updated_at: Utc::now().to_rfc3339(),
        };
        let mut tx = self.pool.begin().await.expect("begin the late intent");
        SqliteMemoryRepository
            .insert_journal_intent(&mut tx, &row)
            .await
            .map_err(|error| SpiError::InvalidInput(format!("late intent: {error}")))?;
        tx.commit()
            .await
            .map_err(|error| SpiError::InvalidInput(format!("late intent commit: {error}")))?;
        Ok(prior_basis_verdict())
    }
}

#[tokio::test]
async fn a_space_level_verdict_settles_with_the_mind_context_and_derives_its_state() {
    let (_dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    let target = cognition_target(&created.carrier_entry_id);

    let outcome = ok(deliberate(
        &store,
        &owner,
        &created.id,
        &target,
        Arc::new(PriorBasisReasoner),
    )
    .await);
    let mind_state_version = match outcome {
        mindcore_spi::DeliberateOutcome::Settled {
            mind_state_version, ..
        } => mind_state_version,
        other => panic!("the auto-settled verdict must commit: {other:?}"),
    };
    assert_eq!(
        mind_state_version, 2,
        "the carrier advances one revision past its creation"
    );

    let handle = ok(Mind::open(&store, &owner, &created.id).await);
    let cognition = ok(handle.cognition_read().await).expect("the settled judgment derives");
    assert_eq!(cognition.records.len(), 1, "the one committed record");
    assert_eq!(
        cognition.records[0]["assessment_context"]["context_id"],
        json!(created.id.as_str()),
        "the stored record carries the Mind's id as its context"
    );
    assert_eq!(
        cognition.records[0]["holder"]["id"],
        json!(created.carrier_entry_id),
        "the record is held by the Mind's carrier"
    );
    assert_eq!(
        cognition.mind_state["holder_entry_id"],
        json!(created.carrier_entry_id),
        "the derived state descends from that carrier"
    );
}

#[tokio::test]
async fn an_intent_that_arrives_after_the_snapshot_refuses_the_commit() {
    let (_dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    let handle = ok(Mind::open(&store, &owner, &created.id).await);
    let target = cognition_target(&created.carrier_entry_id);

    let reasoner = Arc::new(RecordingIntentReasoner {
        pool: store.pool().clone(),
        space_id: created.id.as_str().to_string(),
    });
    let refuse = reject_of(deliberate(&store, &owner, &created.id, &target, reasoner).await);
    assert_eq!(refuse.code, SpokeRejectCode::InternalError);
    let details = refuse.details.as_ref().expect("structured details");
    assert_eq!(
        details.get("reason").and_then(Value::as_str),
        Some("UNRECONCILED"),
        "the space-level withhold refused the commit"
    );
    assert_eq!(
        details.get("space_id").and_then(Value::as_str),
        Some(created.id.as_str())
    );

    assert!(
        ok(handle.cognition_read().await).is_none(),
        "the refused commit wrote no record"
    );
    let carrier = ok(handle.knowledge_get(handle.carrier_entry_id(), None).await);
    assert_eq!(
        carrier.revision,
        Some(1),
        "the refused commit moved no carrier revision"
    );
}

// ---------- QC fix wave: settled-key replay, publisher confinement and executor offload ----------

/// A settled key's replay reaches the receipt probe no matter what else the space holds
/// unsettled: another of the same subject's intents, or an intent of another subject in the
/// same space. The replay is metadata-identical to its first review, so the probe answers the
/// committed receipt — never the content withhold, which guards subject content and says
/// nothing about receipts.
#[tokio::test]
async fn a_settled_replay_survives_any_unsettled_intent_in_the_space() {
    let (dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    ensure_subject(&store, created.id.as_str()).await;
    let handle = ok(Mind::open(&store, &owner, &created.id).await)
        .with_journal_publisher(journal_dir(dir.path()));
    let candidate_id = capture_candidate(&handle, "cap-settled-replay", "a settled note").await;
    let settled = ok(handle
        .review(&candidate_id, &promote(), "rev-settled-replay")
        .await);
    assert_eq!(
        value_of(&settled)["status"],
        "committed",
        "the first review settles"
    );

    // An unsettled intent for the same subject, as a crashed served absorb would leave one.
    settle_never(&store, created.id.as_str(), SUBJECT).await;
    let replayed = ok(handle
        .review(&candidate_id, &promote(), "rev-settled-replay")
        .await);
    assert_eq!(
        value_of(&replayed),
        value_of(&settled),
        "the settled key replays beside the same subject's unsettled intent"
    );
    assert_eq!(
        count_records(&store.pool()).await,
        1,
        "no second record is staged"
    );
    assert_eq!(
        count_intents(&store.pool()).await,
        2,
        "no second replay intent"
    );

    // The same key also replays beside an intent of another subject in the same space.
    settle_never(&store, created.id.as_str(), "subj_elsewhere").await;
    let replayed = ok(handle
        .review(&candidate_id, &promote(), "rev-settled-replay")
        .await);
    assert_eq!(
        value_of(&replayed),
        value_of(&settled),
        "the settled key replays beside another subject's unsettled intent"
    );
    assert_eq!(
        count_records(&store.pool()).await,
        1,
        "still no second record"
    );
}

/// Only the replay bypasses the unsettled-intent withhold: a fresh review — a new key over a
/// candidate of a subject whose intent is unsettled — is refused with the adapter's own
/// UNRECONCILED answer, exactly as before the facade ever probed receipts.
#[tokio::test]
async fn a_fresh_review_is_refused_while_the_subjects_intent_is_unsettled() {
    let (dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    ensure_subject(&store, created.id.as_str()).await;
    let handle = ok(Mind::open(&store, &owner, &created.id).await)
        .with_journal_publisher(journal_dir(dir.path()));
    let candidate_id = capture_candidate(&handle, "cap-fresh-withhold", "a fresh note").await;

    settle_never(&store, created.id.as_str(), SUBJECT).await;
    let reject = reject_of(
        handle
            .review(&candidate_id, &promote(), "rev-fresh-withhold")
            .await,
    );
    assert_eq!(
        reject.code,
        SpokeRejectCode::InternalError,
        "the withhold travels as the adapter's own refusal"
    );
    assert_eq!(
        reject
            .details
            .as_ref()
            .and_then(|details| details.get("reason")),
        Some(&json!("UNRECONCILED")),
        "a fresh review is withheld while the subject's intent is unsettled: {reject:?}"
    );
    assert_eq!(count_records(&store.pool()).await, 0, "nothing is staged");
    assert_eq!(
        count_intents(&store.pool()).await,
        1,
        "only the pre-existing intent stands"
    );

    // Once the intent settles, the same review proceeds — the withhold is live state, not a
    // dead candidate.
    sqlx::query("UPDATE memory_journal SET state = 'applied'")
        .execute(&store.pool())
        .await
        .expect("settle the outstanding intent");
    let reviewed = ok(handle
        .review(&candidate_id, &promote(), "rev-fresh-withhold")
        .await);
    assert_eq!(
        value_of(&reviewed)["status"],
        "committed",
        "the fresh review settles after the intent does"
    );
}

/// The publisher confines every target to its root: an absolute target path or one carrying
/// `..` is refused with nothing written outside the root.
#[tokio::test]
async fn the_public_publisher_refuses_a_target_outside_its_root() {
    let dir = TempDir::new().expect("temporary directory");
    let root = dir.path().join("journal");
    std::fs::create_dir_all(&root).expect("create the root");
    let publisher = mindcore_runtime::journal_dir(&root);

    let escaped = dir.path().join("escaped.md");
    let absolute = publisher(&[json!({
        "path": escaped.to_str().expect("utf-8 scratch path"),
        "content": "escape",
    })]);
    assert!(absolute.is_err(), "an absolute target is refused");
    assert!(!escaped.exists(), "nothing was written outside the root");

    let parent = publisher(&[json!({
        "path": "../escaped.md",
        "content": "escape",
    })]);
    assert!(parent.is_err(), "a parent traversal target is refused");
    assert!(
        !dir.path().join("escaped.md").exists(),
        "no traversal write landed"
    );

    // Confined targets still publish exactly as before.
    publisher(&[json!({ "path": "records/rec_confined.md", "content": "confined bytes" })])
        .expect("a confined target publishes");
    assert_eq!(
        std::fs::read_to_string(root.join("records").join("rec_confined.md"))
            .expect("the confined target"),
        "confined bytes"
    );
}

/// The write cannot follow a symlink even when the swap happens between attempts: publish
/// once, replace the landed target with a symlink to an outside file, publish again — the
/// outside destination is untouched, and the target is the regular file the rename left.
#[tokio::test]
async fn the_public_publisher_never_writes_through_a_swapped_in_target_symlink() {
    let dir = TempDir::new().expect("temporary directory");
    let root = dir.path().join("journal");
    std::fs::create_dir_all(&root).expect("the host creates the root");
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(&outside).expect("create the escape destination");
    let publisher = mindcore_runtime::journal_dir(&root);

    publisher(&[json!({
        "path": "records/rec_swap.md",
        "content": "first attempt",
    })])
    .expect("the first publication lands");
    // The swap: the landed regular file is replaced by a symlink to the outside destination.
    std::fs::remove_file(root.join("records").join("rec_swap.md")).expect("remove the first write");
    std::os::unix::fs::symlink(
        outside.join("victim.md"),
        root.join("records").join("rec_swap.md"),
    )
    .expect("swap in the target symlink");

    publisher(&[json!({
        "path": "records/rec_swap.md",
        "content": "second attempt",
    })])
    .expect("the rename replaces the swapped symlink instead of writing through it");
    assert!(
        !outside.join("victim.md").exists(),
        "the outside destination was never written"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("records").join("rec_swap.md"))
            .expect("the target content"),
        "second attempt",
        "the target holds the retry's bytes as a regular file"
    );
}

/// The temporary file itself is created exclusively: a symlink pre-placed at the temporary
/// name is refused rather than followed, and the victim outside the root is never written.
#[tokio::test]
async fn the_public_publisher_never_writes_through_a_preplaced_temporary_symlink() {
    let dir = TempDir::new().expect("temporary directory");
    let root = dir.path().join("journal");
    std::fs::create_dir_all(&root).expect("the host creates the root");
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(&outside).expect("create the escape destination");
    let publisher = mindcore_runtime::journal_dir(&root);

    // Compute the temporary name exactly as the publisher does and pre-place a symlink there.
    let target_path = "records/rec_preplaced.md";
    let file = root.join(target_path);
    let parent = file.parent().expect("the target parent").to_path_buf();
    std::fs::create_dir_all(&parent).expect("create the target parent");
    let temporary = parent.join(format!(".journal-{}-{:016x}.tmp", std::process::id(), {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        file.hash(&mut hasher);
        hasher.finish()
    }));
    std::os::unix::fs::symlink(outside.join("victim.md"), &temporary)
        .expect("pre-place the temporary symlink");

    let published = publisher(&[json!({
        "path": target_path,
        "content": "must never land outside",
    })]);
    assert!(
        published.is_err(),
        "a pre-placed symlink at the temporary name is refused"
    );
    assert!(
        !outside.join("victim.md").exists(),
        "the victim outside the root was never written"
    );
}

/// The review's publication runs off the async worker. The proof is the probe's own lifecycle:
/// the probe task is spawned while the publisher is parked inside its flush, and its join
/// handle is observed finished — an observed state, never a scheduling assumption — before the
/// publisher is released. Publication inlined on the executor would park the runtime thread in
/// the publisher's blocking wait, the probe could never be polled, the handle would never
/// finish, and the watchdog would name that exact missing signal. The watchdog is disarmed on
/// success.
#[tokio::test]
async fn publication_blocks_no_executor_progress_and_commits_after_it_confirms() {
    let disarm = Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let disarm = disarm.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(30));
            if !disarm.load(std::sync::atomic::Ordering::SeqCst) {
                eprintln!("TIMEOUT: the probe never completed — executor progress stopped while the publisher was parked");
                std::process::exit(2);
            }
        });
    }

    let (dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    ensure_subject(&store, created.id.as_str()).await;
    let journal_root = dir.path().to_path_buf();
    let handle = ok(Mind::open(&store, &owner, &created.id).await)
        .with_journal_publisher(journal_dir(&journal_root));
    let candidate_id = capture_candidate(&handle, "cap-offworker", "a parked publication").await;

    // The observer is opened before the review starts, so nothing migrates or acquires the
    // write while the staging transaction holds it.
    let observer = MindStore::open(&dir.path().join("minds.db"))
        .await
        .expect("open the observer store");

    // The publication handshake: the publisher signals `entered` the moment it is inside its
    // flush, then parks on a blocking channel until the test releases it.
    let (entered, is_entered) = mpsc::channel::<()>();
    let (release, is_released) = tokio::sync::oneshot::channel::<()>();
    let is_released = std::sync::Mutex::new(Some(is_released));
    let parked_root = journal_root.clone();
    let parked =
        ok(Mind::open(&store, &owner, &created.id).await).with_journal_publisher(move |targets| {
            journal_dir(&parked_root)(targets)?;
            entered.send(()).expect("signal the publisher entered");
            let receiver = is_released
                .lock()
                .expect("the release lock")
                .take()
                .expect("the publisher is entered once");
            receiver.blocking_recv().expect("wait for the test release");
            Ok(())
        });

    let review = tokio::spawn(async move {
        parked
            .review(&candidate_id, &promote(), "rev-offworker")
            .await
    });

    // Wait for the publisher's entry signal on a blocking thread — the receive itself is the
    // observed state: it completes exactly when the publisher is inside its flush, however the
    // scheduler gets there.
    let entry =
        tokio::task::spawn_blocking(move || is_entered.recv().expect("the publisher entry signal"));
    entry.await.expect("the entry signal arrived");

    // The probe: spawned after the entry signal, a bare unit of executor work. Its completion
    // is proof of executor progress — the publisher is parked, so the probe completing means
    // something other than the blocked publication polled it. The observation loop advances
    // only on the handle's own observed state, with `yield_now` as a politeness hint to the
    // scheduler, never as the ordering proof: the handle either finishes or it does not.
    let probe = tokio::spawn(async {});
    let mut ticks = 0u32;
    while !probe.is_finished() {
        tokio::task::yield_now().await;
        ticks += 1;
        assert!(
            ticks < 1_000_000,
            "the probe never completed — executor progress stopped while the publisher was parked"
        );
    }
    probe.await.expect("the probe joins");
    // Nothing commits before the publication confirms: the pre-opened observer reads the
    // committed state, and the staging transaction's uncommitted rows are invisible to it.
    assert_eq!(
        count_records(&observer.pool()).await,
        0,
        "nothing commits before the publication confirms"
    );

    release.send(()).expect("release the publisher");
    let reviewed = review.await.expect("the review task joins");
    assert_eq!(
        value_of(&ok(reviewed))["status"],
        "committed",
        "the review settles after the publication confirms"
    );
    disarm.store(true, std::sync::atomic::Ordering::SeqCst);
}

/// Cancellation keeps transaction ownership until the started publication ends. The invariant
/// is tested directly, not through the retry: the pool has exactly one connection, so while
/// the first publication is parked the connection is free only if the cancelled attempt
/// released its transaction early — and a witness acquisition observes that directly. The
/// witness is spawned with a sentinel behind it: the sentinel finishes only after the runtime
/// has polled every task that was ready before it, so its finished handle is the observed
/// state that the witness was polled to its acquisition. What the witness then does is the
/// evidence, observed rather than assumed: it must NOT acquire while the first publication is
/// parked — a parked flush with a released transaction would hand the witness the connection
/// and the test would fail naming the premature acquisition — and it MUST acquire once the
/// first publication has ended, which is when the correct implementation's rollback releases
/// the connection. The watchdog is disarmed on success.
#[tokio::test]
async fn a_cancelled_review_keeps_the_transaction_until_the_publication_ends() {
    let disarm = Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let disarm = disarm.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(30));
            if !disarm.load(std::sync::atomic::Ordering::SeqCst) {
                eprintln!(
                    "TIMEOUT: the publication handshake never completed — a signal never arrived"
                );
                std::process::exit(2);
            }
        });
    }

    let (dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let created = ok(minds::create(&store, &request(OWNER)).await);
    ensure_subject(&store, created.id.as_str()).await;
    let journal_root = dir.path().to_path_buf();
    let handle = ok(Mind::open(&store, &owner, &created.id).await)
        .with_journal_publisher(journal_dir(&journal_root));
    let candidate_id = capture_candidate(&handle, "cap-cancelled", "a cancelled review").await;

    let (entered, is_entered) = mpsc::channel::<()>();
    let (release, is_released) = tokio::sync::oneshot::channel::<()>();
    let is_released = std::sync::Mutex::new(Some(is_released));
    let first_finished = Arc::new(std::sync::Mutex::new(false));
    let finished_pub = first_finished.clone();
    let parked_root = journal_root.clone();
    let parked =
        ok(Mind::open(&store, &owner, &created.id).await).with_journal_publisher(move |targets| {
            journal_dir(&parked_root)(targets)?;
            entered.send(()).expect("signal the publisher entered");
            let receiver = is_released
                .lock()
                .expect("the release lock")
                .take()
                .expect("the publisher is entered once");
            receiver.blocking_recv().expect("wait for the test release");
            *finished_pub.lock().expect("the finish lock") = true;
            Ok(())
        });

    let cancelled_id = candidate_id.clone();
    let review = tokio::spawn(async move {
        parked
            .review(&cancelled_id, &promote(), "rev-cancelled")
            .await
    });

    // Wait for the publisher's entry signal, then cancel. The receive itself is the observed
    // state: it completes exactly when the publisher is inside its flush.
    let entry =
        tokio::task::spawn_blocking(move || is_entered.recv().expect("the publisher entry signal"));
    entry.await.expect("the entry signal arrived");
    review.abort();

    // The witness: a bare pool acquisition. The pool has one connection, so it completes
    // exactly when the connection is free — and the connection is free while the publication
    // is parked only if the cancelled attempt released its transaction early. The sentinel is
    // spawned behind it: its finished handle is the observed state that the witness was
    // polled to its acquisition — the witness can only be waiting on the connection or already
    // holding it, never merely unscheduled.
    let witness_pool = store.pool();
    let witness = tokio::spawn(async move {
        let connection = witness_pool
            .acquire()
            .await
            .expect("the witness acquisition");
        drop(connection);
    });
    let sentinel = tokio::spawn(async {});
    let mut ticks = 0u32;
    while !sentinel.is_finished() {
        tokio::task::yield_now().await;
        ticks += 1;
        assert!(
            ticks < 1_000_000,
            "the sentinel never finished — the runtime never polled the spawned tasks"
        );
    }
    // The witness is provably at its acquisition. Give an early release every chance to hand
    // it the connection: the rollback after a cancelled attempt runs on sqlx's connection
    // worker thread, so the observation window spans real thread scheduling — this is the
    // detection window for a regression, not an ordering proof for the correct behavior, and
    // its length bounds nothing the fixed implementation depends on.
    let window = std::time::Instant::now();
    while window.elapsed() < std::time::Duration::from_millis(500) {
        assert!(
            !witness.is_finished(),
            "the witness acquired the connection while the cancelled attempt's publication was still parked: the transaction was released before the publication ended"
        );
        tokio::task::yield_now().await;
    }

    // Release the first publication: its flush ends, the finish flag is set, and only then
    // may the connection become free — the correct implementation's rollback runs after the
    // blocking task returns the transaction.
    release.send(()).expect("release the publisher");
    let mut ticks = 0u32;
    while !witness.is_finished() {
        tokio::task::yield_now().await;
        ticks += 1;
        assert!(
            ticks < 1_000_000,
            "the witness never acquired after the publication ended — the connection was never released"
        );
    }
    witness.await.expect("the witness joins");
    assert!(
        *first_finished.lock().expect("the finish lock"),
        "the first publication finished before the connection became free"
    );

    // The end state is clean: the cancelled attempt left nothing behind.
    assert_eq!(
        count_records(&store.pool()).await,
        0,
        "the cancelled attempt rolled everything back"
    );
    assert_eq!(
        count_intents(&store.pool()).await,
        0,
        "no intent survives the cancellation"
    );

    // And the same key still settles through a fresh attempt — the rollback is complete, not
    // merely invisible.
    let working = ok(Mind::open(&store, &owner, &created.id).await)
        .with_journal_publisher(journal_dir(&journal_root));
    let reviewed = ok(working
        .review(&candidate_id, &promote(), "rev-cancelled")
        .await);
    assert_eq!(
        value_of(&reviewed)["status"],
        "committed",
        "the same key settles after the cancelled attempt rolled back"
    );
    assert_eq!(
        count_records(&store.pool()).await,
        1,
        "exactly one record across both attempts"
    );
    assert_eq!(
        count_intents(&store.pool()).await,
        1,
        "exactly one intent across both attempts"
    );
    disarm.store(true, std::sync::atomic::Ordering::SeqCst);
}

/// `cognition_read` reads only the collection its admitted Mind resolved at open: a colliding
/// entry in another Mind's collection — same carrier entry id, poisoned content — never leaks
/// into the read, because the collection is the row's own, never a caller-supplied input.
#[tokio::test]
async fn cognition_read_reads_only_the_collection_of_the_admitted_mind() {
    let (_dir, store) = fixture().await;
    let owner = CallerId::new(OWNER);
    let first = ok(minds::create(&store, &request(OWNER)).await);
    let second = ok(minds::create(&store, &request(OWNER)).await);
    assert_ne!(
        first.knowledge.collection, second.knowledge.collection,
        "each Mind owns its own collection"
    );

    // A poisoned record for the first Mind's carrier entry id, written into the second Mind's
    // collection. The record validates: its holder is the second carrier it actually lives
    // with, so the write passes the one validation boundary.
    let second_handle = ok(Mind::open(&store, &owner, &second.id).await);
    let second_carrier = ok(second_handle
        .knowledge_get(second_handle.carrier_entry_id(), None)
        .await);
    let poisoned = json!({
        "kind": "belief",
        "order": 1,
        "record_id": "cog_poisoned",
        "holder": {
            "kind": "knowledge_entry",
            "namespace": "mindcore",
            "id": second.carrier_entry_id,
        },
        "target": {
            "carrier": {
                "kind": "knowledge_entry",
                "namespace": "mindcore",
                "id": second.carrier_entry_id,
            },
            "target_id": "prop_collision",
            "kind": "proposition",
        },
        "assessor": {
            "kind": "principal",
            "namespace": "mindcore",
            "id": OWNER,
        },
        "assessor_role": "holder_self_report",
        "basis": {
            "kind": "evidence",
            "evidence": [{ "kind": "memory_record", "namespace": "mindcore", "id": "rec_collision" }],
        },
        "assessment_context": { "context_id": second.id.as_str() },
        "assessed_at": "2026-11-20T09:00:00Z",
        "effective_at": "2026-11-20T09:00:00Z",
        "view": "current",
        "q": {
            "value": 0.9,
            "assertion": "belief_attribution",
            "method": "two_witness_reports",
            "calibration": "uncalibrated",
        },
        "extensions": {},
    });
    let appended = mindcore_base::cognition::append_cognition_record(&second_carrier, poisoned)
        .expect("the poisoned record validates in its own carrier");
    let revision = second_carrier
        .revision
        .expect("a stored carrier has a revision");
    ok(second_handle.knowledge_put(appended, Some(revision)).await);

    // The first Mind's read is untouched by anything the second collection holds.
    let first_handle = ok(Mind::open(&store, &owner, &first.id).await);
    assert!(
        ok(first_handle.cognition_read().await).is_none(),
        "the first Mind still answers the fresh-Mind None"
    );
    let cognition = ok(second_handle.cognition_read().await).expect("one judgment");
    assert_eq!(
        cognition.records.len(),
        1,
        "the second Mind reads its own collection's record"
    );
    assert_eq!(
        cognition.records[0]["record_id"],
        json!("cog_poisoned"),
        "the read is the admitted Mind's own carrier"
    );
}

/// The publisher's confinement is filesystem resolution: a target whose parent resolves
/// through a symlink inside the root to a destination outside it is refused, and the external
/// destination is never written.
#[tokio::test]
async fn the_public_publisher_refuses_a_target_that_resolves_outside_its_root() {
    let dir = TempDir::new().expect("temporary directory");
    let root = dir.path().join("journal");
    std::fs::create_dir_all(&root).expect("create the root");
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(&outside).expect("create the escape destination");
    std::os::unix::fs::symlink(&outside, root.join("records")).expect("plant the symlink");

    let publisher = mindcore_runtime::journal_dir(&root);
    let escaped = publisher(&[json!({
        "path": "records/rec_symlink.md",
        "content": "escape through the link",
    })]);
    assert!(
        escaped.is_err(),
        "a target resolving outside the root is refused"
    );
    assert!(
        !outside.join("rec_symlink.md").exists(),
        "nothing was written at the external destination"
    );

    // A symlink at the target file itself cannot redirect the write either: the bytes are
    // written to a temporary file in the same directory and renamed over the target — rename
    // replaces the directory entry, it never follows what the entry points to.
    std::fs::create_dir_all(root.join("candidates")).expect("create the target parent");
    std::os::unix::fs::symlink(
        outside.join("planted.md"),
        root.join("candidates").join("rec_target.md"),
    )
    .expect("plant the target symlink");
    publisher(&[json!({
        "path": "candidates/rec_target.md",
        "content": "confined despite the target symlink",
    })])
    .expect("the rename replaces the symlink instead of following it");
    assert!(
        !outside.join("planted.md").exists(),
        "nothing was written through the target symlink"
    );
    let landed = std::fs::symlink_metadata(root.join("candidates").join("rec_target.md"))
        .expect("the target entry");
    assert!(
        landed.file_type().is_file(),
        "the target is a regular file after the rename, not the planted symlink"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("candidates").join("rec_target.md"))
            .expect("the target content"),
        "confined despite the target symlink"
    );

    // The parent chain stays inside the root, and the confined target lands.
    publisher(&[json!({
        "path": "profiles/rec_confined.md",
        "content": "confined bytes",
    })])
    .expect("a genuinely confined target still publishes");
    assert_eq!(
        std::fs::read_to_string(root.join("profiles").join("rec_confined.md"))
            .expect("the confined target"),
        "confined bytes"
    );
}
