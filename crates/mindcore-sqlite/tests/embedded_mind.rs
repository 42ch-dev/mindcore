//! The S1–S8 first-mind story: memory, adoption, doubt, evidence, reopen and scope refusals.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use mindcore_core::boundary::{CallerId, TrustedScope};
use mindcore_core::cognition::{
    append_cognition_record, derive_mind_state, mind_state_value, MindError,
};
use mindcore_core::ids::{ContextId, SpaceId, SubjectId};
use mindcore_core::knowledge::{adoption_anchor, KnowledgeScope};
use mindcore_core::memory::{MemoryCandidate, MemoryStore, RecallQuery};
use mindcore_sqlite::cognition::{read_carrier, write_carrier};
use mindcore_sqlite::MindStore;
use serde_json::{json, Map, Value};
use spoke_operations::{validate_mind_state, KnowledgeEntryPort, SpokeRejectCode, SpokeResult};
use spoke_schemas::knowledge_entry::{
    ExtensionKey, KnowledgeEntryBody, KnowledgeEntryCanonicalName, KnowledgeEntryOwner,
};
use spoke_schemas::KnowledgeEntry;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Scoped temp directory holding one SQLite database; dropping it removes the database
/// together with its `-wal`/`-shm` siblings, including when an assertion panics.
struct TempDb {
    dir: PathBuf,
}

impl TempDb {
    fn new(label: &str) -> Self {
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

    fn database_path(&self) -> PathBuf {
        self.dir.join("mindcore.db")
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

fn entry(entry_id: &str, entry_type: &str, summary: &str) -> KnowledgeEntry {
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
fn entry_anchor(
    anchor: spoke_schemas::SourceAnchor,
) -> spoke_schemas::knowledge_entry::SourceAnchor {
    serde_json::from_value(serde_json::to_value(anchor).expect("anchor serializes"))
        .expect("anchor wire shape matches the knowledge_entry copy")
}

fn json_of(entry: &KnowledgeEntry) -> Value {
    serde_json::to_value(entry).expect("entry serializes")
}

fn record_ids(records: &[Value]) -> Vec<&str> {
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

fn recall_in(scope: &TrustedScope, context: Option<&str>) -> RecallQuery {
    RecallQuery {
        space: scope.space().clone(),
        subject: SubjectId::new("wren"),
        context: context.map(ContextId::new),
    }
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
async fn first_mind_story() {
    let temp = TempDb::new("story");
    let path = temp.database_path();
    let store = MindStore::open(&path).await.expect("open store");

    // Setup: three collections, two trusted scopes, one shared fixture entry.
    let wren_scope = TrustedScope::new(SpaceId::new("wren"), CallerId::new("caller_wren"));
    let studio_scope = TrustedScope::new(SpaceId::new("studio"), CallerId::new("caller_studio"));
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

    // S1: Wren hears about the gate; hearing is not yet knowledge.
    let hearing = MemoryCandidate {
        candidate_id: "cnd_wren_hearsay".to_string(),
        space: wren_scope.space().clone(),
        subject: SubjectId::new("wren"),
        context: ContextId::new("tavern"),
        content: "A traveller says the north gate is shut once the dusk bell rings.".to_string(),
        observed_at: timestamp("2026-11-19T17:40:00Z"),
    };
    store
        .capture(hearing.clone())
        .await
        .expect("capture the hearing");
    assert!(store
        .recall(&recall_in(&wren_scope, Some("tavern")))
        .await
        .expect("recall the tavern")
        .is_empty());
    assert!(store
        .recall(&recall_in(&studio_scope, None))
        .await
        .expect("recall Studio's space")
        .is_empty());
    assert_eq!(
        expect_reject(
            wren.get_knowledge_entry("ke_wren_north_gate").await,
            "personal entry before adoption",
        ),
        SpokeRejectCode::KnowledgeEntryNotFound
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

    let hearsay = store
        .retain(&hearing.candidate_id, "req-wren-hearsay")
        .await
        .expect("retain the hearing");
    let replayed = store
        .retain(&hearing.candidate_id, "req-wren-hearsay")
        .await
        .expect("replay the same review request");
    assert_eq!(replayed, hearsay);

    let shared_after_adoption = expect_ok(
        facts.get_knowledge_entry(&shared_entry_id).await,
        "read the shared entry after adoption",
    );
    assert_eq!(json_of(&shared_after_adoption), json_of(&shared_read));
    assert_eq!(shared_after_adoption.revision, Some(1));

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
    let doubt = append_cognition_record(
        &carrier,
        belief_record(
            "cog_hearsay",
            "Unknown",
            "Testimony",
            &hearsay.record_id,
            "2026-11-19T18:05:00Z",
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
    assert_eq!(
        doubt_record["basis"]["evidence"][0]["id"],
        json!(hearsay.record_id)
    );

    // S5: perception at the gate revises the judgment; the revision is conditional.
    let observation = MemoryCandidate {
        candidate_id: "cnd_wren_gate".to_string(),
        space: wren_scope.space().clone(),
        subject: SubjectId::new("wren"),
        context: ContextId::new("gate"),
        content: "At the gate after dusk: the bar is lifted and the north gate stands open."
            .to_string(),
        observed_at: timestamp("2026-11-19T19:30:00Z"),
    };
    store
        .capture(observation.clone())
        .await
        .expect("capture the observation");
    let observed = store
        .retain("cnd_wren_gate", "req-wren-gate")
        .await
        .expect("retain the observation");
    assert_eq!(observed.context.as_str(), "gate");
    assert_eq!(
        store
            .recall(&recall_in(&wren_scope, Some("gate")))
            .await
            .expect("recall the gate"),
        vec![observed.clone()]
    );

    let carrier = expect_ok(
        read_carrier(&wren, "ke_wren_cognition").await,
        "read the carrier before the second judgment",
    );
    let revision_before = carrier.revision.expect("the carrier holds a revision");
    assert_eq!(revision_before, 2);
    let reconsidered = append_cognition_record(
        &carrier,
        belief_record(
            "cog_observed",
            "False",
            "Perception",
            &observed.record_id,
            "2026-11-19T19:35:00Z",
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
        json!(observed.record_id)
    );
    assert_eq!(revised_value["holder_entry_id"], json!(carrier.entry_id));
    assert_holder_consistency(&revised_value);
    assert_eq!(
        &revised_view.records[0], doubt_record,
        "appending the revision must preserve the first record"
    );

    let assessed_at = timestamp(
        assessments[1]["assessed_at"]
            .as_str()
            .expect("the revision carries its assessment time"),
    );
    let doubt_assessed_at = timestamp(
        assessments[0]["assessed_at"]
            .as_str()
            .expect("the doubt carries its assessment time"),
    );
    // Story times only: the store-assigned `recorded_at` is wall-clock, so comparing against it
    // would fail as soon as real time passes the story's fixed instant.
    assert!(
        assessed_at >= observation.observed_at,
        "the revision was assessed at {assessed_at}, before the observation it cites (observed at {})",
        observation.observed_at
    );
    assert!(
        assessed_at >= doubt_assessed_at,
        "the revision was assessed at {assessed_at}, before the judgment it revises ({doubt_assessed_at})"
    );

    let personal_after_revision = expect_ok(
        wren.get_knowledge_entry("ke_wren_north_gate").await,
        "read the adopted entry after the revision",
    );
    assert_eq!(json_of(&personal_after_revision), json_of(&adopted));
    let tavern = store
        .recall(&recall_in(&wren_scope, Some("tavern")))
        .await
        .expect("recall the earlier memory");
    assert_eq!(tavern, vec![hearsay.clone()]);

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

    // S7: the file, not the process, holds the mind.
    let expected_value =
        mind_state_value(&view_after_shared, "ms_wren_story").expect("render the pre-close state");
    let expected_holder = view_after_shared.holder_entry_id.clone();
    let expected_revision = view_after_shared.carrier_revision;
    let expected_records = record_ids(&view_after_shared.records);
    store.close().await;

    let store = MindStore::open(&path).await.expect("reopen the store");
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

    store.close().await;
}

#[tokio::test]
async fn refusals_leave_state_unchanged() {
    let temp = TempDb::new("refusals");
    let path = temp.database_path();
    let store = MindStore::open(&path).await.expect("open store");
    let wren = store.knowledge(KnowledgeScope::new("wren"));
    let studio = store.knowledge(KnowledgeScope::new("studio"));

    let mut adoption = entry(
        "ke_wren_north_gate",
        "location",
        "Wren believes the north gate is shut after dusk.",
    );
    adoption.source_anchor = Some(entry_anchor(adoption_anchor(
        "ke_shared_north_gate",
        "hearsay",
    )));
    let personal = expect_ok(
        wren.put_knowledge_entry(adoption, None).await,
        "write the personal entry",
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
        expect_reject(
            studio.get_knowledge_entry("ke_wren_north_gate").await,
            "Studio read of Wren's entry",
        ),
        SpokeRejectCode::KnowledgeEntryNotFound
    );
    assert_eq!(
        expect_reject(
            studio.put_knowledge_entry(updated.clone(), Some(2)).await,
            "Studio write at Wren's revision",
        ),
        SpokeRejectCode::StoredRevisionStale
    );
    let mut different = personal.clone();
    different.body.summary = Some("A different claim about the north gate.".to_string());
    assert_eq!(
        expect_reject(
            wren.put_knowledge_entry(different, Some(1)).await,
            "Wren write at the pre-update revision",
        ),
        SpokeRejectCode::StoredRevisionStale
    );

    let wren_final = expect_ok(
        wren.get_knowledge_entry("ke_wren_north_gate").await,
        "Wren read after the refusals",
    );
    assert_eq!(json_of(&wren_final), json_of(&updated));
    assert_eq!(wren_final.revision, Some(2));
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

    store.close().await;
}
