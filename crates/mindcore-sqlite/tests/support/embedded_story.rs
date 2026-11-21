//! Single owner of the S1–S8 first-mind story: the scenario, its assertions, and its step
//! labels live here and nowhere else. `tests/embedded_mind.rs` and `examples/iteration_01.rs`
//! both include this file via `#[path]`; the example is a thin runner, the test a thin caller.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use mindcore_base::boundary::CallerId;
use mindcore_base::cognition::{
    append_cognition_record, derive_mind_state, mind_state_value, MindError,
};
use mindcore_base::ids::SpaceId;
use mindcore_base::ir::{MemoryGrantRow, MemoryRepository};
use mindcore_base::knowledge::{adoption_anchor, KnowledgeScope};
use mindcore_spoke::{MemoryAdapter, MemoryStage};
use mindcore_sqlite::cognition::{read_carrier, write_carrier};
use mindcore_sqlite::{MindStore, SqliteMemoryRepository};
use serde_json::{json, Map, Value};
use spoke_operations::{validate_mind_state, KnowledgeEntryPort, SpokeRejectCode, SpokeResult};
use spoke_schemas::knowledge_entry::{
    ExtensionKey, KnowledgeEntryBody, KnowledgeEntryCanonicalName, KnowledgeEntryOwner,
};
use spoke_schemas::{KnowledgeEntry, Principal};
use sqlx::SqlitePool;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Scoped temp directory holding one SQLite database; dropping it removes the database
/// together with its `-wal`/`-shm` siblings, including when an assertion panics.
pub struct TempDb {
    dir: PathBuf,
}

impl TempDb {
    pub fn new(label: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before the epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "mindcore-mind-{}-{label}-{nanos}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create scoped temp dir");
        Self { dir }
    }

    pub fn database_path(&self) -> PathBuf {
        self.dir.join("mindcore.db")
    }

    /// The scratch root the protocol's journaled file effects are published under.
    pub fn root(&self) -> &Path {
        &self.dir
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn timestamp(text: &str) -> DateTime<Utc> {
    text.parse().expect("valid RFC 3339 timestamp")
}

// The shared seed is a fixture file, not a second fixture: it is read as typed entries.
fn shared_fixture() -> (String, Vec<KnowledgeEntry>) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/iteration-01/shared-collection.json");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("fixture {} is readable: {error}", path.display()));
    let fixture: Value = serde_json::from_str(&text).expect("fixture is JSON");
    let collection = fixture["collection"]
        .as_str()
        .expect("fixture names a collection")
        .to_string();
    let entries: Vec<KnowledgeEntry> = serde_json::from_value(fixture["entries"].clone())
        .expect("fixture entries are wire entries");
    (collection, entries)
}

pub fn entry(entry_id: &str, entry_type: &str, summary: &str) -> KnowledgeEntry {
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
        entry_type: entry_type.to_string(),
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

fn reference(kind: &str, id: &str) -> Value {
    json!({"kind": kind, "id": id, "namespace": "mindcore"})
}

// Task 1's record, verbatim, with the scenario's truth, acquisition route and retained record.
fn belief_record(
    record_id: &str,
    truth: &str,
    acquisition: &str,
    evidence_record_id: &str,
    assessed_at: &str,
) -> Value {
    json!({
        "assessed_at": assessed_at,
        "assessment_context": {"context_id": "ctx_dusk_at_the_gate"},
        "assessor": reference("knowledge_entry", "ke_wren_cognition"),
        "assessor_role": "holder_self_report",
        "basis": {
            "acquisition": acquisition,
            "evidence": [reference("memory_record", evidence_record_id)],
            "kind": "evidence",
        },
        "effective_at": "2026-11-19T18:00:00Z",
        "extensions": {},
        "holder": reference("knowledge_entry", "ke_wren_cognition"),
        "kind": "belief",
        "order": 1,
        "q": {
            "assertion": "belief_attribution",
            "calibration": "uncalibrated",
            "method": "single_self_report",
            "value": 0.4,
        },
        "record_id": record_id,
        "target": {
            "carrier": reference("knowledge_entry", "ke_shared_north_gate"),
            "kind": "proposition",
            "target_id": "prop_north_gate_closed",
        },
        "truth": truth,
        "view": "current",
    })
}

// Pinned schemas ship one copy of `SourceAnchor` per schema and only the knowledge_entry copy is
// accepted by `KnowledgeEntry.source_anchor`; their wire shapes are identical.
pub fn entry_anchor(
    anchor: spoke_schemas::SourceAnchor,
) -> spoke_schemas::knowledge_entry::SourceAnchor {
    serde_json::from_value(serde_json::to_value(anchor).expect("anchor serializes"))
        .expect("anchor wire shape matches the knowledge_entry copy")
}

pub fn json_of(entry: &KnowledgeEntry) -> Value {
    serde_json::to_value(entry).expect("entry serializes")
}

pub fn record_ids(records: &[Value]) -> Vec<&str> {
    records
        .iter()
        .filter_map(|record| record.get("record_id").and_then(Value::as_str))
        .collect()
}

// The envelope holder and every assessment's holder/assessor must name one entry.
fn assert_holder_consistency(value: &Value) {
    let holder = &value["holder_entry_id"];
    for item in value["cognition"]["items"]
        .as_array()
        .expect("projection items")
    {
        for assessment in item["assessments"].as_array().expect("assessment group") {
            assert_eq!(&assessment["holder"]["id"], holder, "assessment holder");
            assert_eq!(&assessment["assessor"]["id"], holder, "assessment assessor");
        }
    }
}

/// The authenticated caller the host supplies; never payload-asserted.
pub fn principal(id: &str) -> Principal {
    serde_json::from_value(json!({ "principal_id": id, "extensions": {} }))
        .expect("principal wire shape")
}

fn ready(stage: SpokeResult<MemoryStage>, context: &str) -> Value {
    match expect_ok(stage, context) {
        MemoryStage::Ready(body) => body,
        other => panic!("{context}: expected a ready stage, got {other:?}"),
    }
}

// Host control-plane provisioning: the protocol has no create-space or first-grant operation, so
// the host writes one active space and one space-wide grant per space. Everything after this is a
// protocol operation.
pub async fn provision_space(pool: &SqlitePool, space: &str, caller: &str) {
    let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    sqlx::query(
        "INSERT INTO memory_spaces \
         (space_id, display_name, authority_host_id, authority_generation, status, revision, \
          created_at) \
         VALUES (?, ?, 'host_story', 1, 'active', 1, ?)",
    )
    .bind(space)
    .bind(space)
    .bind(&now)
    .execute(pool)
    .await
    .expect("provision the space");
    let mut tx = pool.begin().await.expect("begin the grant transaction");
    SqliteMemoryRepository
        .insert_grant(
            &mut tx,
            &MemoryGrantRow {
                grant_id: format!("grt_{space}"),
                space_id: SpaceId::new(space),
                subject_id: None,
                principal_id: CallerId::new(caller),
                capabilities: [
                    "capture",
                    "review",
                    "records_read",
                    "records_write",
                    "recall",
                ]
                .map(str::to_string)
                .to_vec(),
                context_ids: Vec::new(),
                shared_access: false,
                expires_at: None,
                revoked_at: None,
                revision: serde_json::Number::from(1),
                created_at: now,
                extensions: Map::new(),
            },
        )
        .await
        .expect("provision the grant");
    tx.commit().await.expect("commit the grant");
}

/// One adapter subject registration: the story's Wren and the race tests' shared subject.
pub async fn ensure_subject(pool: &SqlitePool, adapter: &MemoryAdapter, caller: &Principal) {
    let mut tx = pool.begin().await.expect("begin the subject");
    ready(
        adapter
            .ensure_subject(
                &mut tx,
                caller,
                json!({ "space_id": "wren", "subject_id": "wren" }),
            )
            .await,
        "ensure the wren subject",
    );
    tx.commit().await.expect("commit the subject");
}

/// One adapter capture over a caller transaction; reports the pending candidate's identity.
pub async fn capture(
    pool: &SqlitePool,
    adapter: &MemoryAdapter,
    caller: &Principal,
    request: Value,
) -> String {
    let mut tx = pool.begin().await.expect("begin the capture");
    let body = ready(adapter.capture(&mut tx, caller, request).await, "capture");
    tx.commit().await.expect("commit the capture");
    body["result"]["candidate_ids"][0]
        .as_str()
        .expect("candidate id")
        .to_string()
}

/// One adapter recall over a caller transaction; the selection is the §6.12 wire member.
pub async fn recalled_by(
    pool: &SqlitePool,
    adapter: &MemoryAdapter,
    caller: &Principal,
    space: &str,
    subject: &str,
    selection: Value,
) -> Vec<Value> {
    let mut tx = pool.begin().await.expect("begin the recall");
    let body = ready(
        adapter
            .recall(
                &mut tx,
                caller,
                json!({
                    "space_id": space,
                    "subject_id": subject,
                    "selection": selection,
                    "budget": { "max_items": 10 },
                }),
            )
            .await,
        "recall",
    );
    tx.rollback().await.expect("rollback the recall");
    body["records"].as_array().expect("records array").clone()
}

/// Wren's recall in one space: the story's own subject, other spaces included.
pub async fn recalled(
    pool: &SqlitePool,
    adapter: &MemoryAdapter,
    caller: &Principal,
    space: &str,
    selection: Value,
) -> Vec<Value> {
    recalled_by(pool, adapter, caller, space, "wren", selection).await
}

fn in_context(context: &str) -> Value {
    json!({ "mode": "selected_contexts", "context_ids": [context] })
}

fn review_request(candidate_id: &str, key: &str) -> Value {
    json!({
        "space_id": "wren",
        "candidate_id": candidate_id,
        "decision": { "action": "promote" },
        "expected_revision": 1,
        "idempotency_key": key,
    })
}

/// Wren's review round: promote the candidate, commit the staged intent and publish every owed
/// target into the caller's scratch root; the receipt stays pending for settlement.
pub async fn stage_review(
    pool: &SqlitePool,
    adapter: &MemoryAdapter,
    caller: &Principal,
    root: &Path,
    candidate_id: &str,
    key: &str,
) -> SpokeResult<String> {
    let mut tx = pool.begin().await.expect("begin the review");
    let stage = adapter
        .review_candidate(&mut tx, caller, review_request(candidate_id, key))
        .await;
    let receipt_id = match stage {
        SpokeResult::Ok(MemoryStage::JournalPending { receipt_id, .. }) => receipt_id,
        SpokeResult::Ok(other) => panic!("review must stage a journal intent, got {other:?}"),
        SpokeResult::Reject(reject) => {
            tx.rollback().await.expect("rollback the refused review");
            return SpokeResult::Reject(reject);
        }
    };
    tx.commit().await.expect("commit the review");

    let owed: String =
        sqlx::query_scalar("SELECT targets_json FROM memory_journal WHERE receipt_id = ?")
            .bind(&receipt_id)
            .fetch_one(pool)
            .await
            .expect("journal intent");
    let owed: Vec<Value> = serde_json::from_str(&owed).expect("journal targets");
    assert!(!owed.is_empty(), "the staged intent owes file targets");
    for target in &owed {
        let path = root.join(target["path"].as_str().expect("target path"));
        std::fs::create_dir_all(path.parent().expect("target directory"))
            .expect("create the target directory");
        std::fs::write(&path, target["content"].as_str().expect("target content"))
            .expect("publish the target");
        assert_eq!(
            std::fs::read_to_string(&path).expect("read the published target"),
            target["content"].as_str().expect("target content"),
            "the published bytes are the intent's"
        );
    }
    SpokeResult::Ok(receipt_id)
}

/// Settle one staged receipt in its own caller transaction and report the receipt body.
pub async fn settle_receipt(
    pool: &SqlitePool,
    adapter: &MemoryAdapter,
    space: &str,
    receipt_id: &str,
) -> SpokeResult<Value> {
    let mut tx = pool.begin().await.expect("begin the settlement");
    let settled = adapter
        .settle_journal(&mut tx, &SpaceId::new(space), receipt_id)
        .await;
    match settled {
        SpokeResult::Ok(receipt) => {
            tx.commit().await.expect("commit the settlement");
            SpokeResult::Ok(receipt)
        }
        SpokeResult::Reject(reject) => {
            tx.rollback()
                .await
                .expect("rollback the refused settlement");
            SpokeResult::Reject(reject)
        }
    }
}

/// One full retain attempt: stage, publish, settle. A refusal is returned instead of panicking so
/// competing callers can report which side lost.
pub async fn attempt_retain(
    pool: &SqlitePool,
    adapter: &MemoryAdapter,
    caller: &Principal,
    root: &Path,
    candidate_id: &str,
    key: &str,
) -> SpokeResult<(String, String)> {
    let receipt_id = match stage_review(pool, adapter, caller, root, candidate_id, key).await {
        SpokeResult::Ok(receipt_id) => receipt_id,
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    let receipt = match settle_receipt(pool, adapter, "wren", &receipt_id).await {
        SpokeResult::Ok(receipt) => receipt,
        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
    };
    assert_eq!(receipt["status"], json!("committed"));
    let record_id = receipt["result"]["record_ids"][0]
        .as_str()
        .expect("retained record id")
        .to_string();
    SpokeResult::Ok((receipt_id, record_id))
}

/// The story's single-threaded retain: an unexpected refusal fails the run.
async fn retain(
    pool: &SqlitePool,
    adapter: &MemoryAdapter,
    caller: &Principal,
    root: &Path,
    candidate_id: &str,
    key: &str,
) -> (String, String) {
    match attempt_retain(pool, adapter, caller, root, candidate_id, key).await {
        SpokeResult::Ok(identities) => identities,
        SpokeResult::Reject(reject) => {
            panic!("review the candidate: unexpectedly rejected with {reject:?}")
        }
    }
}

pub fn expect_ok<T>(result: SpokeResult<T>, context: &str) -> T {
    match result {
        SpokeResult::Ok(value) => value,
        SpokeResult::Reject(reject) => panic!("{context}: unexpectedly rejected with {reject:?}"),
    }
}

pub fn expect_reject<T: std::fmt::Debug>(result: SpokeResult<T>, context: &str) -> SpokeRejectCode {
    match result {
        SpokeResult::Ok(value) => panic!("{context}: unexpectedly accepted with {value:?}"),
        SpokeResult::Reject(reject) => reject.code,
    }
}

fn step(number: u8, text: &str) {
    println!("{number}. {text}");
}

/// Runs the S1–S8 first-mind story against a fresh store in a scoped temp directory, asserting
/// every step in-process and printing each step label so the example stays a readable story.
pub async fn run_story(label: &str) {
    let temp = TempDb::new(label);
    let path = temp.database_path();
    let store = MindStore::open(&path).await.expect("open store");

    // Setup: three collections, two principals, two protocol spaces, one shared fixture entry.
    let wren_caller = principal("caller_wren");
    let studio_caller = principal("caller_studio");
    let pool = store.pool();
    let adapter = MemoryAdapter::new(false);
    provision_space(&pool, "wren", "caller_wren").await;
    provision_space(&pool, "studio", "caller_studio").await;
    ensure_subject(&pool, &adapter, &wren_caller).await;

    let wren = store.knowledge(KnowledgeScope::new("wren"));
    let studio = store.knowledge(KnowledgeScope::new("studio"));
    let facts = store.knowledge(KnowledgeScope::new("village-facts"));

    let (shared_collection, shared_entries) = shared_fixture();
    assert_eq!(shared_collection, "village-facts");
    let shared_entry = shared_entries
        .into_iter()
        .find(|candidate| candidate.entry_id == "ke_shared_north_gate")
        .expect("the fixture carries the shared north gate entry");
    let shared_entry_id = shared_entry.entry_id.clone();
    let shared_seeded = expect_ok(
        facts.put_knowledge_entry(shared_entry, None).await,
        "seed the shared fixture entry",
    );
    assert_eq!(shared_seeded.revision, Some(1));
    println!(
        "setup: seeded {shared_entry_id} in village-facts at revision 1; store {}",
        path.display()
    );

    // S1: Wren hears about the gate; hearing is not yet knowledge.
    let hearing = capture(
        &pool,
        &adapter,
        &wren_caller,
        json!({
            "space_id": "wren",
            "subject_id": "wren",
            "capture_scope": { "visibility": "context", "context_id": "tavern" },
            "content": { "text": "A traveller says the north gate is shut once the dusk bell rings." },
            "occurred_at": "2026-11-19T17:40:00Z",
            "idempotency_key": "cap-wren-hearsay",
        }),
    )
    .await;
    assert!(
        recalled(&pool, &adapter, &wren_caller, "wren", in_context("tavern"))
            .await
            .is_empty()
    );
    assert!(recalled(
        &pool,
        &adapter,
        &studio_caller,
        "studio",
        json!({ "mode": "subject_all" })
    )
    .await
    .is_empty());
    assert_eq!(
        expect_reject(
            wren.get_knowledge_entry("ke_wren_north_gate").await,
            "personal entry before adoption",
        ),
        SpokeRejectCode::KnowledgeEntryNotFound
    );
    step(
        1,
        &format!("hearing: captured {hearing} in wren/tavern; recall empty; no personal entry"),
    );

    // S2: a catalog read through the shared scope changes nothing on Wren's side.
    let shared_read = expect_ok(
        facts.get_knowledge_entry(&shared_entry_id).await,
        "read the shared entry",
    );
    assert_eq!(json_of(&shared_read), json_of(&shared_seeded));
    assert!(expect_ok(wren.list_entries().await, "list wren").is_empty());
    assert!(expect_ok(studio.list_entries().await, "list studio").is_empty());
    assert!(
        matches!(derive_mind_state(&shared_read), Err(MindError::NoRecords)),
        "a catalog read must not derive cognition"
    );
    step(
        2,
        "catalog read: shared entry revision 1 unchanged; wren has no entries; derived state NoRecords",
    );

    // S3: adoption writes a personal entry and retains the hearing, leaving the shared entry alone.
    let mut adoption = entry(
        "ke_wren_north_gate",
        "location",
        "Wren believes the north gate is shut after dusk; the traveller said so in the tavern.",
    );
    adoption.source_anchor = Some(entry_anchor(adoption_anchor(&shared_entry_id, "hearsay")));
    let adopted = expect_ok(
        wren.put_knowledge_entry(adoption, None).await,
        "write the personal entry",
    );
    assert_eq!(adopted.revision, Some(1));
    let personal_reread = expect_ok(
        wren.get_knowledge_entry("ke_wren_north_gate").await,
        "read the personal entry back",
    );
    assert_eq!(json_of(&personal_reread), json_of(&adopted));
    let anchor = personal_reread
        .source_anchor
        .as_ref()
        .expect("the personal entry carries its adoption anchor");
    assert_eq!(anchor.source_id, format!("shared:{shared_entry_id}"));
    assert_eq!(
        serde_json::to_value(anchor).expect("anchor serializes")["extensions"]["mindcore_adoption"]
            ["basis"],
        json!("hearsay")
    );

    let (hearsay_receipt, hearsay) = retain(
        &pool,
        &adapter,
        &wren_caller,
        temp.root(),
        &hearing,
        "rev-wren-hearsay",
    )
    .await;

    // The same review key replays its committed receipt instead of staging a second record.
    let mut tx = pool.begin().await.expect("begin the review replay");
    let replay = ready(
        adapter
            .review_candidate(
                &mut tx,
                &wren_caller,
                review_request(&hearing, "rev-wren-hearsay"),
            )
            .await,
        "replay the same review request",
    );
    tx.rollback().await.expect("rollback the review replay");
    assert_eq!(replay["receipt_id"], json!(hearsay_receipt));
    assert_eq!(replay["result"]["record_ids"], json!([hearsay.clone()]));

    // A second review of the same candidate under a fresh key is refused: one candidate yields
    // one retained record.
    let mut tx = pool.begin().await.expect("begin the second review");
    assert_eq!(
        expect_reject(
            adapter
                .review_candidate(
                    &mut tx,
                    &wren_caller,
                    review_request(&hearing, "rev-wren-hearsay-again"),
                )
                .await,
            "second review of the retained candidate",
        ),
        SpokeRejectCode::CandidateConsumed
    );
    tx.rollback().await.expect("rollback the second review");

    // Scoped isolation, now that a record exists: the record is recallable in its own scope and
    // must not answer another space's or another subject's recall. S1's cross-space recall ran
    // while both spaces were empty, so it could not tell a filtered recall from an unfiltered one;
    // the positive control below keeps these refusals from being vacuous in turn.
    let own_scope = recalled(&pool, &adapter, &wren_caller, "wren", in_context("tavern")).await;
    assert_eq!(
        record_ids(&own_scope),
        vec![hearsay.as_str()],
        "positive control: the retained record is recallable in Wren's own scope"
    );
    let other_space = recalled(
        &pool,
        &adapter,
        &studio_caller,
        "studio",
        json!({ "mode": "subject_all" }),
    )
    .await;
    assert!(
        other_space.is_empty(),
        "a Wren record must not cross into Studio's space: {other_space:?}"
    );
    let other_subject = recalled_by(
        &pool,
        &adapter,
        &wren_caller,
        "wren",
        "mason",
        json!({ "mode": "subject_all" }),
    )
    .await;
    assert!(
        other_subject.is_empty(),
        "a Wren record must not answer another subject's recall in the same space: {other_subject:?}"
    );

    let shared_after_adoption = expect_ok(
        facts.get_knowledge_entry(&shared_entry_id).await,
        "read the shared entry after adoption",
    );
    assert_eq!(json_of(&shared_after_adoption), json_of(&shared_read));
    assert_eq!(shared_after_adoption.revision, Some(1));
    step(
        3,
        &format!(
            "adoption: ke_wren_north_gate revision 1 anchored to shared:{shared_entry_id} (hearsay); hearing retained as {hearsay} in receipt {hearsay_receipt}; shared entry unchanged"
        ),
    );

    // S4: the personal carrier starts empty, then records a doubt citing the retained hearsay.
    let carrier_entry = entry(
        "ke_wren_cognition",
        "character",
        "Wren's current view of the north gate.",
    );
    expect_ok(
        wren.put_knowledge_entry(carrier_entry, None).await,
        "create the cognition carrier",
    );
    let carrier = expect_ok(
        read_carrier(&wren, "ke_wren_cognition").await,
        "read the empty carrier",
    );
    // The supplied assessment time is bound once, so the projection must return this exact value.
    let doubt_assessed_at = "2026-11-19T18:05:00Z";
    let doubt = append_cognition_record(
        &carrier,
        belief_record(
            "cog_hearsay",
            "Unknown",
            "Testimony",
            &hearsay,
            doubt_assessed_at,
        ),
    )
    .expect("append the doubt record");
    let carrier = expect_ok(
        write_carrier(&wren, doubt, 1).await,
        "persist the doubt record",
    );
    assert_eq!(carrier.revision, Some(2));

    let doubt_view = derive_mind_state(&carrier).expect("derive the doubt view");
    assert_eq!(record_ids(&doubt_view.records), vec!["cog_hearsay"]);
    let doubt_value =
        mind_state_value(&doubt_view, "ms_wren_story").expect("render the doubt mind state");
    expect_ok(validate_mind_state(&doubt_value), "doubt mind state");
    assert_eq!(doubt_value["holder_entry_id"], json!(carrier.entry_id));
    assert_holder_consistency(&doubt_value);
    let doubt_record = &doubt_view.records[0];
    assert_eq!(doubt_record["truth"], json!("Unknown"));
    assert_eq!(doubt_record["basis"]["acquisition"], json!("Testimony"));
    assert_eq!(doubt_record["basis"]["evidence"][0]["id"], json!(hearsay));
    step(
        4,
        "doubt: cog_hearsay persisted (truth Unknown, Testimony) at carrier revision 2; mind state valid",
    );

    // S5: perception at the gate revises the judgment; the revision is conditional.
    let gate_observed_at = "2026-11-19T19:30:00Z";
    let observation = capture(
        &pool,
        &adapter,
        &wren_caller,
        json!({
            "space_id": "wren",
            "subject_id": "wren",
            "capture_scope": { "visibility": "context", "context_id": "gate" },
            "content": { "text": "At the gate after dusk: the bar is lifted and the north gate stands open." },
            "occurred_at": gate_observed_at,
            "idempotency_key": "cap-wren-gate",
        }),
    )
    .await;
    let (_, observed) = retain(
        &pool,
        &adapter,
        &wren_caller,
        temp.root(),
        &observation,
        "rev-wren-gate",
    )
    .await;
    let at_the_gate = recalled(&pool, &adapter, &wren_caller, "wren", in_context("gate")).await;
    assert_eq!(record_ids(&at_the_gate), vec![observed.as_str()]);
    assert_eq!(
        at_the_gate[0]["scope"],
        json!({ "visibility": "context", "context_id": "gate" })
    );
    assert_eq!(at_the_gate[0]["occurred_at"], json!(gate_observed_at));

    let carrier = expect_ok(
        read_carrier(&wren, "ke_wren_cognition").await,
        "read the carrier before the second judgment",
    );
    let revision_before = carrier.revision.expect("the carrier holds a revision");
    assert_eq!(revision_before, 2);
    // Same binding rule as the doubt: the projection must return this exact value.
    let revision_assessed_at = "2026-11-19T19:35:00Z";
    let reconsidered = append_cognition_record(
        &carrier,
        belief_record(
            "cog_observed",
            "False",
            "Perception",
            &observed,
            revision_assessed_at,
        ),
    )
    .expect("append the observation record");
    let carrier = expect_ok(
        write_carrier(&wren, reconsidered, revision_before).await,
        "persist the second judgment",
    );
    assert_eq!(carrier.revision, Some(3));

    let revised_view = derive_mind_state(&carrier).expect("derive the revised view");
    assert_eq!(
        record_ids(&revised_view.records),
        vec!["cog_hearsay", "cog_observed"]
    );
    let revised_value =
        mind_state_value(&revised_view, "ms_wren_story").expect("render the revised mind state");
    expect_ok(validate_mind_state(&revised_value), "revised mind state");

    let items = revised_value["cognition"]["items"]
        .as_array()
        .expect("projection items");
    assert_eq!(items.len(), 1);
    let assessments = items[0]["assessments"]
        .as_array()
        .expect("assessment group");
    assert_eq!(record_ids(assessments), vec!["cog_hearsay", "cog_observed"]);
    assert_eq!(
        items[0]["selection"]["selected_record_id"],
        json!("cog_observed")
    );
    assert_eq!(assessments[1]["truth"], json!("False"));
    assert_eq!(assessments[1]["basis"]["acquisition"], json!("Perception"));
    assert_eq!(
        assessments[1]["basis"]["evidence"][0]["id"],
        json!(observed)
    );
    assert_eq!(revised_value["holder_entry_id"], json!(carrier.entry_id));
    assert_holder_consistency(&revised_value);
    assert_eq!(
        &revised_view.records[0], doubt_record,
        "appending the revision must preserve the first record"
    );

    // Value preservation: the projection must return the `assessed_at` supplied above, not a
    // store-assigned or recomputed time. Expected values are the bound inputs themselves.
    assert_eq!(
        assessments[0]["assessed_at"],
        json!(doubt_assessed_at),
        "the doubt projection returns the supplied assessed_at"
    );
    assert_eq!(
        assessments[1]["assessed_at"],
        json!(revision_assessed_at),
        "the revision projection returns the supplied assessed_at"
    );

    let revision_assessed = timestamp(
        assessments[1]["assessed_at"]
            .as_str()
            .expect("the revision carries its assessment time"),
    );
    let doubt_assessed = timestamp(
        assessments[0]["assessed_at"]
            .as_str()
            .expect("the doubt carries its assessment time"),
    );
    // Story times only: the store-assigned `created_at` is wall-clock, so comparing against it
    // would fail as soon as real time passes the story's fixed instant.
    let gate_observed = timestamp(gate_observed_at);
    assert!(
        revision_assessed >= gate_observed,
        "the revision was assessed at {revision_assessed}, before the observation it cites (observed at {gate_observed})"
    );
    assert!(
        revision_assessed >= doubt_assessed,
        "the revision was assessed at {revision_assessed}, before the judgment it revises ({doubt_assessed})"
    );

    let personal_after_revision = expect_ok(
        wren.get_knowledge_entry("ke_wren_north_gate").await,
        "read the adopted entry after the revision",
    );
    assert_eq!(json_of(&personal_after_revision), json_of(&adopted));
    let tavern = recalled(&pool, &adapter, &wren_caller, "wren", in_context("tavern")).await;
    assert_eq!(record_ids(&tavern), vec![hearsay.as_str()]);
    step(
        5,
        &format!(
            "evidence: cog_observed persisted (truth False, Perception, evidence {observed}) at carrier revision 3; selected cog_observed; adopted entry unchanged; hearsay still recallable"
        ),
    );

    // S6: the shared catalog moves on without touching Wren's entry or derived records.
    let shared_before_update = expect_ok(
        facts.get_knowledge_entry(&shared_entry_id).await,
        "read the shared entry before its update",
    );
    let changed_summary = "The village north gate is closed after dusk and guarded at night.";
    let mut shared_update = shared_before_update.clone();
    shared_update.body.summary = Some(changed_summary.to_string());
    let shared_updated = expect_ok(
        facts.put_knowledge_entry(shared_update, Some(1)).await,
        "update the shared entry",
    );
    assert_eq!(shared_updated.revision, Some(2));
    let shared_reread = expect_ok(
        facts.get_knowledge_entry(&shared_entry_id).await,
        "read the shared entry after its update",
    );
    assert_eq!(json_of(&shared_reread), json_of(&shared_updated));
    assert_eq!(
        shared_reread.body.summary.as_deref(),
        Some(changed_summary),
        "the reread carries the requested shared content"
    );
    assert_ne!(json_of(&shared_reread), json_of(&shared_before_update));

    let personal_after_shared = expect_ok(
        wren.get_knowledge_entry("ke_wren_north_gate").await,
        "read the personal entry after the shared change",
    );
    assert_eq!(json_of(&personal_after_shared), json_of(&adopted));
    let carrier_after_shared = expect_ok(
        read_carrier(&wren, "ke_wren_cognition").await,
        "read the carrier after the shared change",
    );
    let view_after_shared =
        derive_mind_state(&carrier_after_shared).expect("derive after the shared change");
    assert_eq!(
        view_after_shared.records, revised_view.records,
        "derived records are unchanged by the shared update"
    );
    assert_eq!(view_after_shared.carrier_revision, 3);
    let view_after_shared_value = mind_state_value(&view_after_shared, "ms_wren_story")
        .expect("render the mind state after the shared change");
    assert_eq!(
        view_after_shared_value, revised_value,
        "the rendered projection is unchanged by the shared update"
    );
    step(
        6,
        "shared change: shared entry revision 2 carries the new summary; Wren entry revision 1; derived records and projection identical to step 5",
    );

    // S7: the file, not the process, holds the mind.
    let expected_value =
        mind_state_value(&view_after_shared, "ms_wren_story").expect("render the pre-close state");
    let expected_holder = view_after_shared.holder_entry_id.clone();
    let expected_revision = view_after_shared.carrier_revision;
    let expected_records = record_ids(&view_after_shared.records);
    store.close().await;

    let store = MindStore::open(&path).await.expect("reopen the store");
    let pool = store.pool();
    let adapter = MemoryAdapter::new(false);
    let wren = store.knowledge(KnowledgeScope::new("wren"));
    let studio = store.knowledge(KnowledgeScope::new("studio"));
    let carrier_reopened = expect_ok(
        read_carrier(&wren, "ke_wren_cognition").await,
        "read the carrier after reopen",
    );
    let reopened_view = derive_mind_state(&carrier_reopened).expect("derive after reopen");
    assert_eq!(reopened_view.holder_entry_id, expected_holder);
    assert_eq!(reopened_view.carrier_revision, expected_revision);
    assert_eq!(record_ids(&reopened_view.records), expected_records);
    assert_eq!(
        mind_state_value(&reopened_view, "ms_wren_story").expect("render after reopen"),
        expected_value
    );
    assert!(expect_ok(studio.list_entries().await, "list studio after reopen").is_empty());
    let reopened_tavern =
        recalled(&pool, &adapter, &wren_caller, "wren", in_context("tavern")).await;
    assert_eq!(record_ids(&reopened_tavern), vec![hearsay.as_str()]);
    step(
        7,
        &format!(
            "reopen: derived state equals the pre-close state (holder {}, revision 3, records cog_hearsay, cog_observed); {hearsay} still recallable; studio empty",
            reopened_view.holder_entry_id
        ),
    );

    // S8: scope and revision refusals leave both collections exactly as they were.
    assert_eq!(
        expect_reject(
            studio.get_knowledge_entry("ke_wren_north_gate").await,
            "Studio read of Wren's entry",
        ),
        SpokeRejectCode::KnowledgeEntryNotFound
    );

    let personal = expect_ok(
        wren.get_knowledge_entry("ke_wren_north_gate").await,
        "read Wren's entry before the restatement",
    );
    let mut restated = personal.clone();
    restated.body.summary =
        Some("Wren's note: the north gate is shut after dusk (studio viewpoint).".to_string());
    restated.owner =
        Some(KnowledgeEntryOwner::try_from("ke_studio_record_office").expect("non-empty owner"));
    restated.extensions = HashMap::from([(
        ExtensionKey::try_from("mindcore_test").expect("valid extension key"),
        Map::from_iter([("viewpoint".to_string(), json!("studio"))]),
    )]);
    let updated = expect_ok(
        wren.put_knowledge_entry(restated, Some(1)).await,
        "restate Wren's entry",
    );
    assert_eq!(updated.revision, Some(2));
    assert_eq!(
        updated.owner.as_deref().map(String::as_str),
        Some("ke_studio_record_office")
    );

    let wren_read = expect_ok(
        wren.get_knowledge_entry("ke_wren_north_gate").await,
        "Wren read after the restatement",
    );
    assert_eq!(json_of(&wren_read), json_of(&updated));
    assert_eq!(
        wren_read
            .extensions
            .get(&ExtensionKey::try_from("mindcore_test").expect("valid extension key"))
            .and_then(|fields| fields.get("viewpoint")),
        Some(&json!("studio"))
    );
    assert_eq!(
        expect_reject(
            studio.get_knowledge_entry("ke_wren_north_gate").await,
            "Studio read after the restatement",
        ),
        SpokeRejectCode::KnowledgeEntryNotFound
    );

    // A write aimed at a collection that holds no such row is stale, never a create.
    assert_eq!(
        expect_reject(
            studio.put_knowledge_entry(updated.clone(), Some(2)).await,
            "Studio write of Wren's entry",
        ),
        SpokeRejectCode::StoredRevisionStale
    );

    let mut different = personal.clone();
    different.body.summary = Some("A different claim about the north gate.".to_string());
    assert_eq!(
        expect_reject(
            wren.put_knowledge_entry(different, Some(1)).await,
            "write with the pre-update revision",
        ),
        SpokeRejectCode::StoredRevisionStale
    );

    let wren_final = expect_ok(
        wren.get_knowledge_entry("ke_wren_north_gate").await,
        "Wren read after the refusals",
    );
    assert_eq!(json_of(&wren_final), json_of(&updated));
    assert_eq!(
        expect_reject(
            studio.get_knowledge_entry("ke_wren_north_gate").await,
            "Studio read after the refusals",
        ),
        SpokeRejectCode::KnowledgeEntryNotFound
    );
    assert!(expect_ok(
        studio.list_entries().await,
        "list studio after the refusals"
    )
    .is_empty());
    step(
        8,
        "refusals: studio read KnowledgeEntryNotFound; Wren update revision 2; wrong-scope and stale writes StoredRevisionStale; both scopes unchanged",
    );

    store.close().await;
}
