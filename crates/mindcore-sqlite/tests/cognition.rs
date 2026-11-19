//! Behaviour of the cognition carrier, the derived MindState and the scoped carrier helpers.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use mindcore_core::cognition::{
    append_cognition_record, derive_mind_state, mind_state_value, MindError, CARRIER_NAMESPACE,
};
use mindcore_core::knowledge::KnowledgeScope;
use mindcore_sqlite::cognition::{read_carrier, write_carrier};
use mindcore_sqlite::MindStore;
use serde_json::{json, Value};
use spoke_operations::{
    validate_cognition_module, validate_cognition_record, validate_mind_state, KnowledgeEntryPort,
    SpokeRejectCode, SpokeResult,
};
use spoke_schemas::knowledge_entry::{
    KnowledgeEntryBody, KnowledgeEntryCanonicalName, KnowledgeEntryModulesKey,
    KnowledgeEntryModulesValue,
};
use spoke_schemas::KnowledgeEntry;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Scoped temp directory holding one SQLite database; dropping it removes the database
/// together with its `-wal`/`-shm` siblings, including when an assertion panics.
struct TempDb {
    dir: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before the epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "mindcore-cognition-{}-{nanos}-{}",
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

fn carrier(entry_id: &str, revision: Option<u64>) -> KnowledgeEntry {
    KnowledgeEntry {
        body: KnowledgeEntryBody::default(),
        canonical_name: KnowledgeEntryCanonicalName::try_from(format!("{entry_id} name"))
            .expect("non-empty canonical name"),
        created_at: None,
        disclosure: None,
        entry_id: entry_id.to_string(),
        entry_type: "character".to_string(),
        extensions: HashMap::new(),
        modules: HashMap::new(),
        owner: None,
        revision,
        schema_version: NonZeroU64::new(1).expect("non-zero wire baseline"),
        source_anchor: None,
        status: "confirmed".to_string(),
        updated_at: None,
    }
}

fn reference(kind: &str, id: &str) -> Value {
    json!({"kind": kind, "id": id, "namespace": CARRIER_NAMESPACE})
}

// A holder reference in another realm: same shape, different namespace.
fn foreign_reference(kind: &str, id: &str) -> Value {
    json!({"kind": kind, "id": id, "namespace": "other_realm"})
}

// One current-view belief record about the adopted north-gate proposition.
fn belief(record_id: &str, q: f64) -> Value {
    json!({
        "kind": "belief",
        "order": 1,
        "record_id": record_id,
        "holder": reference("knowledge_entry", "ke_wren"),
        "target": {
            "carrier": reference("knowledge_entry", "ke_shared_north_gate"),
            "target_id": "prop_north_gate_closed",
            "kind": "proposition",
        },
        "assessor": reference("knowledge_entry", "ke_wren"),
        "assessor_role": "holder_self_report",
        "basis": {
            "kind": "evidence",
            "evidence": [reference("memory_record", "mem_hearsay")],
        },
        "assessment_context": {"context_id": "ctx_dusk_at_the_gate"},
        "effective_at": "2026-11-19T18:00:00Z",
        "assessed_at": "2026-11-19T18:05:00Z",
        "view": "current",
        "q": {
            "value": q,
            "assertion": "belief_attribution",
            "method": "single_self_report",
            "calibration": "uncalibrated",
        },
        "extensions": {},
    })
}

fn with_module(entry_id: &str, revision: Option<u64>, module: Value) -> KnowledgeEntry {
    let mut entry = carrier(entry_id, revision);
    entry.modules.insert(
        KnowledgeEntryModulesKey::try_from("cognition").expect("valid module key"),
        KnowledgeEntryModulesValue::from(
            module
                .as_object()
                .expect("module fixture must be an object")
                .clone(),
        ),
    );
    entry
}

fn module_of(entry: &KnowledgeEntry) -> Value {
    serde_json::to_value(&entry.modules).expect("modules serialize")["cognition"].clone()
}

fn module_keys(records: &[Value]) -> Vec<&str> {
    records
        .iter()
        .filter_map(|record| record.get("record_id").and_then(Value::as_str))
        .collect()
}

fn expect_ok<T>(result: SpokeResult<T>, context: &str) -> T {
    match result {
        SpokeResult::Ok(value) => value,
        SpokeResult::Reject(reject) => panic!("{context}: unexpectedly rejected with {reject:?}"),
    }
}

#[tokio::test]
async fn append_creates_module_and_validates() {
    let temp = TempDb::new();
    let store = MindStore::open(&temp.database_path())
        .await
        .expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    expect_ok(
        port.put_knowledge_entry(carrier("ke_wren", None), None)
            .await,
        "create carrier",
    );
    let read = expect_ok(read_carrier(&port, "ke_wren").await, "read carrier");
    let appended = append_cognition_record(&read, belief("cog_hearsay", 0.4)).expect("append");
    let stored = expect_ok(write_carrier(&port, appended, 1).await, "write carrier");

    assert_eq!(stored.revision, Some(2));
    let module = module_of(&stored);
    assert_eq!(module["records"].as_array().map(Vec::len), Some(1));
    expect_ok(validate_cognition_module(&module), "carrier module");
    store.close().await;
}

#[test]
fn duplicate_record_id_is_rejected() {
    let once = append_cognition_record(&carrier("ke_wren", Some(1)), belief("cog_hearsay", 0.4))
        .expect("first append");
    match append_cognition_record(&once, belief("cog_hearsay", 0.6)) {
        Err(MindError::RecordInvalid(message)) => assert!(message.contains("cog_hearsay")),
        other => panic!("duplicate record id must be RecordInvalid, got {other:?}"),
    }
}

#[test]
fn derive_requires_records() {
    assert!(matches!(
        derive_mind_state(&carrier("ke_wren", Some(1))),
        Err(MindError::NoRecords)
    ));

    let empty = with_module(
        "ke_wren",
        Some(1),
        json!({"records": [], "world_facts": [], "extensions": {}}),
    );
    assert!(matches!(
        derive_mind_state(&empty),
        Err(MindError::NoRecords)
    ));
}

#[test]
fn derived_view_matches_carrier() {
    let first = append_cognition_record(&carrier("ke_wren", Some(3)), belief("cog_hearsay", 0.35))
        .expect("first append");
    let second =
        append_cognition_record(&first, belief("cog_observed", 0.65)).expect("second append");

    let view = derive_mind_state(&second).expect("derive mind state");
    assert_eq!(view.holder_entry_id, "ke_wren");
    assert_eq!(view.carrier_revision, 3);
    assert_eq!(
        module_keys(&view.records),
        vec!["cog_hearsay", "cog_observed"]
    );
}

#[test]
fn mind_state_value_passes_validation() {
    let first = append_cognition_record(&carrier("ke_wren", Some(2)), belief("cog_hearsay", 0.35))
        .expect("first append");
    let second =
        append_cognition_record(&first, belief("cog_observed", 0.65)).expect("second append");
    let view = derive_mind_state(&second).expect("derive mind state");

    let value = mind_state_value(&view, "ms_wren_dusk").expect("render mind state");
    expect_ok(validate_mind_state(&value), "mind state envelope");
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["holder_entry_id"], "ke_wren");

    let items = value["cognition"]["items"]
        .as_array()
        .expect("projection items");
    assert_eq!(items.len(), 1);
    let assessments = items[0]["assessments"].as_array().expect("assessments");
    assert_eq!(
        module_keys(assessments),
        vec!["cog_hearsay", "cog_observed"]
    );
    assert_eq!(items[0]["selection"]["selected_record_id"], "cog_observed");
    assert_eq!(
        items[0]["selection"]["input_record_ids"],
        json!(["cog_hearsay", "cog_observed"])
    );
    assert_eq!(
        items[0]["selection"]["policy"]["policy_id"],
        "mindcore_append_order"
    );
    assert_eq!(items[0]["selection"]["policy"]["policy_version"], "1");
}

#[test]
fn append_does_not_change_revision() {
    let input = carrier("ke_wren", Some(7));
    let output =
        append_cognition_record(&input, belief("cog_hearsay", 0.4)).expect("append record");
    assert_eq!(output.revision, input.revision);
}

#[test]
fn invalid_record_is_rejected_and_carrier_unchanged() {
    let input = append_cognition_record(&carrier("ke_wren", Some(1)), belief("cog_hearsay", 0.4))
        .expect("append valid record");
    let before = module_of(&input);

    let mut invalid = belief("cog_bad_order", 0.5);
    invalid["order"] = json!(4);
    match append_cognition_record(&input, invalid) {
        Err(MindError::RecordInvalid(message)) => {
            assert!(message.contains("wire-shape"), "got {message}")
        }
        other => panic!("invalid record must be RecordInvalid, got {other:?}"),
    }
    assert_eq!(module_of(&input), before);
}

#[test]
fn non_current_record_is_rejected() {
    let mut recorded = belief("cog_recorded", 0.4);
    recorded["view"] = json!("recorded_at");
    // The pinned record validator is placement-agnostic, so only the current-view
    // rule can reject this record at append time.
    expect_ok(
        validate_cognition_record(&recorded),
        "recorded_at fixture is a valid placed record",
    );

    match append_cognition_record(&carrier("ke_wren", Some(1)), recorded) {
        Err(MindError::RecordInvalid(message)) => {
            assert!(message.contains("cog_recorded"), "got {message}")
        }
        other => panic!("non-current record must be RecordInvalid, got {other:?}"),
    }
}

#[test]
fn malformed_module_is_rejected_by_append_and_derive() {
    let broken = with_module(
        "ke_wren",
        Some(1),
        json!({
            "records": [{"kind": "belief", "record_id": "cog_broken"}],
            "world_facts": [],
            "extensions": {},
        }),
    );

    match append_cognition_record(&broken, belief("cog_hearsay", 0.4)) {
        Err(MindError::CarrierInvalid(message)) => {
            assert!(message.contains("module"), "got {message}")
        }
        other => panic!("malformed module must be CarrierInvalid on append, got {other:?}"),
    }
    let Err(MindError::CarrierInvalid(message)) = derive_mind_state(&broken) else {
        panic!("malformed module must be CarrierInvalid on derive")
    };
    assert!(message.contains("module"), "got {message}");
}

#[tokio::test]
async fn carrier_write_rejects_malformed_module_without_touching_row() {
    let temp = TempDb::new();
    let store = MindStore::open(&temp.database_path())
        .await
        .expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    let created = expect_ok(
        port.put_knowledge_entry(carrier("ke_wren", None), None)
            .await,
        "create carrier",
    );
    assert_eq!(created.revision, Some(1));
    let before = expect_ok(read_carrier(&port, "ke_wren").await, "read carrier");

    let broken = with_module(
        "ke_wren",
        Some(1),
        json!({
            "records": [{"kind": "belief", "record_id": "cog_broken"}],
            "world_facts": [],
            "extensions": {},
        }),
    );
    // The port is the single validation point: a direct put of a malformed module at the correct
    // base revision must be refused before any row is written.
    match port.put_knowledge_entry(broken.clone(), Some(1)).await {
        SpokeResult::Reject(reject) => assert_eq!(reject.code, SpokeRejectCode::InvalidInput),
        SpokeResult::Ok(entry) => panic!("malformed module was persisted: {entry:?}"),
    }
    // The carrier helper delegates to that same check.
    match write_carrier(&port, broken, 1).await {
        SpokeResult::Reject(reject) => assert_eq!(reject.code, SpokeRejectCode::InvalidInput),
        SpokeResult::Ok(entry) => panic!("malformed module was persisted: {entry:?}"),
    }

    let after = expect_ok(read_carrier(&port, "ke_wren").await, "reread carrier");
    assert_eq!(after.revision, Some(1));
    assert_eq!(
        serde_json::to_value(&after).expect("entry serializes"),
        serde_json::to_value(&before).expect("entry serializes"),
        "the refused write must leave the previous payload unchanged"
    );
    store.close().await;
}

// A record with a foreign holder passes the pinned module validator, so only the carrier identity
// check can refuse it at the persistence boundary.
#[tokio::test]
async fn carrier_write_rejects_foreign_holder_without_touching_row() {
    let temp = TempDb::new();
    let store = MindStore::open(&temp.database_path())
        .await
        .expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    expect_ok(
        port.put_knowledge_entry(carrier("ke_wren", None), None)
            .await,
        "create carrier",
    );
    let before = expect_ok(read_carrier(&port, "ke_wren").await, "read carrier");

    let mut foreign = belief("cog_foreign", 0.4);
    foreign["holder"] = reference("knowledge_entry", "ke_studio");
    let attempted = with_module(
        "ke_wren",
        Some(1),
        json!({"records": [foreign], "world_facts": [], "extensions": {}}),
    );
    expect_ok(
        validate_cognition_module(&module_of(&attempted)),
        "module shape",
    );

    match port.put_knowledge_entry(attempted, Some(1)).await {
        SpokeResult::Reject(reject) => assert_eq!(reject.code, SpokeRejectCode::InvalidInput),
        SpokeResult::Ok(entry) => panic!("foreign holder was persisted: {entry:?}"),
    }

    let after = expect_ok(read_carrier(&port, "ke_wren").await, "reread carrier");
    assert_eq!(after.revision, Some(1));
    assert_eq!(
        serde_json::to_value(&after).expect("entry serializes"),
        serde_json::to_value(&before).expect("entry serializes"),
        "the refused write must leave the previous payload unchanged"
    );
    store.close().await;
}
// A caller-supplied record with a missing or non-string record_id must get the pinned public
// diagnostic, not a panic or the wire validator's generic message, at the same public boundary
// that checks every other record shape.
#[test]
fn record_without_record_id_returns_typed_rejection() {
    let mut no_id = belief("cog_no_id", 0.4);
    no_id
        .as_object_mut()
        .expect("record object")
        .remove("record_id");
    let mut non_string = belief("cog_typed_id", 0.4);
    non_string["record_id"] = json!(7);

    for broken in [no_id, non_string] {
        match append_cognition_record(&carrier("ke_wren", Some(1)), broken) {
            Err(MindError::RecordInvalid(message)) => {
                assert_eq!(message, "record has no usable record_id")
            }
            Err(error) => {
                panic!("a record without a usable record_id must be RecordInvalid, got {error}")
            }
            Ok(_) => panic!("a record without a usable record_id must be rejected, not appended"),
        }
    }
}

// A record held by the right kind and id but in another realm passes the pinned module
// validator, so only the full holder tuple can refuse it at the persistence boundary.
#[tokio::test]
async fn carrier_write_rejects_foreign_holder_namespace_without_touching_row() {
    let temp = TempDb::new();
    let store = MindStore::open(&temp.database_path())
        .await
        .expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    expect_ok(
        port.put_knowledge_entry(carrier("ke_wren", None), None)
            .await,
        "create carrier",
    );
    let before = expect_ok(read_carrier(&port, "ke_wren").await, "read carrier");

    let mut foreign = belief("cog_foreign_realm", 0.4);
    foreign["holder"] = foreign_reference("knowledge_entry", "ke_wren");
    let attempted = with_module(
        "ke_wren",
        Some(1),
        json!({"records": [foreign], "world_facts": [], "extensions": {}}),
    );
    expect_ok(
        validate_cognition_module(&module_of(&attempted)),
        "module shape",
    );

    match port.put_knowledge_entry(attempted, Some(1)).await {
        SpokeResult::Reject(reject) => assert_eq!(reject.code, SpokeRejectCode::InvalidInput),
        SpokeResult::Ok(entry) => panic!("foreign realm holder was persisted: {entry:?}"),
    }

    let after = expect_ok(read_carrier(&port, "ke_wren").await, "reread carrier");
    assert_eq!(after.revision, Some(1));
    assert_eq!(
        serde_json::to_value(&after).expect("entry serializes"),
        serde_json::to_value(&before).expect("entry serializes"),
        "the refused write must leave the previous payload unchanged"
    );
    store.close().await;
}

// A record held by a different kind of entity but the same raw id passes the pinned module
// validator, so only the full holder comparison can refuse it at the persistence boundary.
#[tokio::test]
async fn carrier_write_rejects_foreign_holder_kind_without_touching_row() {
    let temp = TempDb::new();
    let store = MindStore::open(&temp.database_path())
        .await
        .expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    expect_ok(
        port.put_knowledge_entry(carrier("ke_wren", None), None)
            .await,
        "create carrier",
    );
    let before = expect_ok(read_carrier(&port, "ke_wren").await, "read carrier");

    let mut foreign = belief("cog_foreign_kind", 0.4);
    foreign["holder"] = reference("memory_record", "ke_wren");
    let attempted = with_module(
        "ke_wren",
        Some(1),
        json!({"records": [foreign], "world_facts": [], "extensions": {}}),
    );
    expect_ok(
        validate_cognition_module(&module_of(&attempted)),
        "module shape",
    );

    match port.put_knowledge_entry(attempted, Some(1)).await {
        SpokeResult::Reject(reject) => assert_eq!(reject.code, SpokeRejectCode::InvalidInput),
        SpokeResult::Ok(entry) => panic!("foreign holder kind was persisted: {entry:?}"),
    }

    let after = expect_ok(read_carrier(&port, "ke_wren").await, "reread carrier");
    assert_eq!(after.revision, Some(1));
    assert_eq!(
        serde_json::to_value(&after).expect("entry serializes"),
        serde_json::to_value(&before).expect("entry serializes"),
        "the refused write must leave the previous payload unchanged"
    );
    store.close().await;
}

#[tokio::test]
async fn carrier_write_rejects_recorded_at_record_without_touching_row() {
    let temp = TempDb::new();
    let store = MindStore::open(&temp.database_path())
        .await
        .expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    expect_ok(
        port.put_knowledge_entry(carrier("ke_wren", None), None)
            .await,
        "create carrier",
    );
    let before = expect_ok(read_carrier(&port, "ke_wren").await, "read carrier");

    let mut recorded = belief("cog_recorded", 0.4);
    recorded["view"] = json!("recorded_at");
    let attempted = with_module(
        "ke_wren",
        Some(1),
        json!({"records": [recorded], "world_facts": [], "extensions": {}}),
    );

    match port.put_knowledge_entry(attempted, Some(1)).await {
        SpokeResult::Reject(reject) => assert_eq!(reject.code, SpokeRejectCode::InvalidInput),
        SpokeResult::Ok(entry) => panic!("recorded_at record was persisted: {entry:?}"),
    }

    let after = expect_ok(read_carrier(&port, "ke_wren").await, "reread carrier");
    assert_eq!(after.revision, Some(1));
    assert_eq!(
        serde_json::to_value(&after).expect("entry serializes"),
        serde_json::to_value(&before).expect("entry serializes"),
        "the refused write must leave the previous payload unchanged"
    );
    store.close().await;
}

#[test]
fn derive_rejects_carrier_records_that_violate_the_contract() {
    let mut recorded = belief("cog_recorded", 0.4);
    recorded["view"] = json!("recorded_at");
    let recorded_carrier = with_module(
        "ke_wren",
        Some(1),
        json!({"records": [recorded], "world_facts": [], "extensions": {}}),
    );
    assert!(
        matches!(
            derive_mind_state(&recorded_carrier),
            Err(MindError::CarrierInvalid(_))
        ),
        "a non-current record must be CarrierInvalid on derive"
    );

    let mut foreign = belief("cog_foreign", 0.4);
    foreign["holder"] = reference("knowledge_entry", "ke_studio");
    let foreign_carrier = with_module(
        "ke_wren",
        Some(1),
        json!({"records": [foreign], "world_facts": [], "extensions": {}}),
    );
    expect_ok(
        validate_cognition_module(&module_of(&foreign_carrier)),
        "foreign holder module shape",
    );
    let Err(MindError::CarrierInvalid(message)) = derive_mind_state(&foreign_carrier) else {
        panic!("a foreign holder must be CarrierInvalid on derive")
    };
    assert!(message.contains("cog_foreign"), "got {message}");

    let mut foreign_realm = belief("cog_foreign_realm", 0.4);
    foreign_realm["holder"] = foreign_reference("knowledge_entry", "ke_wren");
    let foreign_realm_carrier = with_module(
        "ke_wren",
        Some(1),
        json!({"records": [foreign_realm], "world_facts": [], "extensions": {}}),
    );
    expect_ok(
        validate_cognition_module(&module_of(&foreign_realm_carrier)),
        "foreign realm module shape",
    );
    let Err(MindError::CarrierInvalid(message)) = derive_mind_state(&foreign_realm_carrier) else {
        panic!("a holder in another realm must be CarrierInvalid on derive")
    };
    assert!(message.contains("cog_foreign_realm"), "got {message}");
}

#[test]
fn records_without_revision_are_rejected() {
    let entry = append_cognition_record(&carrier("ke_wren", None), belief("cog_hearsay", 0.4))
        .expect("append record to unrevisioned carrier");
    assert_eq!(entry.revision, None);

    let Err(MindError::CarrierInvalid(message)) = derive_mind_state(&entry) else {
        panic!("records without a revision must be CarrierInvalid")
    };
    assert!(message.contains("revision"), "got {message}");
}
