//! Behaviour of the cognition carrier, the derived MindState and the scoped carrier helpers.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use mindcore_base::cognition::{
    append_cognition_record, derive_mind_state, mind_state_value, MindError, CARRIER_NAMESPACE,
};
use mindcore_base::knowledge::KnowledgeScope;
use mindcore_sqlite::cognition::{
    insert_record, read_carrier, read_current_record, read_record, read_records, write_carrier,
};
use mindcore_sqlite::{append_cognition, read_carrier_header, MindStore};
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
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;

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

/// One narrated order-0 world fact: the envelope value a stored carrier must
/// preserve beside its records.
fn world_fact() -> Value {
    json!({
        "fact_id": "fact_gate_curfew",
        "holder": {
            "kind": "world",
            "id": "world_almanac",
            "namespace": CARRIER_NAMESPACE,
        },
        "target": {
            "carrier": {
                "kind": "knowledge_entry",
                "id": "ke_shared_north_gate",
                "namespace": CARRIER_NAMESPACE,
            },
            "kind": "proposition",
            "target_id": "prop_north_gate_closed",
        },
        "order": 0,
        "assessment_context": {"context_id": "ctx_village_almanac"},
        "effective_at": "2026-11-19T18:00:00Z",
        "recorded_at": "2026-11-19T18:05:00Z",
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

/// The pinned `BeliefOrder` accepts 0–3 only, and order 2/3 require a nested
/// `about` target whose chain depth follows the order: 1 is plain, 2 nests one
/// `about`, 3 nests two. The outer target identity is kept, so records stay in
/// the same projection group as their order-1 siblings.
fn with_order(mut record: Value, order: i64) -> Value {
    record["order"] = json!(order);
    if order > 1 {
        let mut about = json!({
            "carrier": {
                "kind": "knowledge_entry",
                "id": "ke_shared_north_gate",
                "namespace": CARRIER_NAMESPACE,
            },
            "kind": "proposition",
            "target_id": "prop_gate_archway",
        });
        for depth in 1..order - 1 {
            about = json!({
                "about": about,
                "carrier": {
                    "kind": "knowledge_entry",
                    "id": "ke_shared_north_gate",
                    "namespace": CARRIER_NAMESPACE,
                },
                "kind": "proposition",
                "target_id": format!("prop_gate_layer_{depth}"),
            });
        }
        record["target"]["about"] = about;
    }
    record
}

fn module_keys(records: &[Value]) -> Vec<&str> {
    records
        .iter()
        .filter_map(|record| record.get("record_id").and_then(Value::as_str))
        .collect()
}

/// A second single-connection pool on the same file, so a test can hold SQLite's
/// write reservation itself: storage-level appends borrow its transactions.
async fn second_pool(path: &Path) -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(path)
                .foreign_keys(true),
        )
        .await
        .expect("open second connection")
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

/// I-000036: the pinned typed `BeliefOrder` reads `i64` only, so a record whose `order`
/// is a non-integer JSON representation (`1.0`) passes wire-shape validation but can
/// never be read back. The carrier boundary refuses it on append, and a direct put
/// carrying one is refused at `put_knowledge_entry` before the write reservation.
#[test]
fn float_order_record_is_rejected_on_append() {
    let mut float_order = belief("cog_float_order", 0.4);
    float_order["order"] = json!(1.0);
    // The wire-shape gate itself still accepts the float: the representation
    // rule is a carrier contract, not a schema change.
    expect_ok(
        validate_cognition_record(&float_order),
        "float order passes wire-shape",
    );

    match append_cognition_record(&carrier("ke_wren", Some(1)), float_order) {
        Err(MindError::RecordInvalid(message)) => {
            assert!(message.contains("cog_float_order"), "got {message}")
        }
        other => panic!("float order must be RecordInvalid on append, got {other:?}"),
    }
}

/// I-000036: a direct `put_knowledge_entry` cannot bypass the carrier rule — a module
/// carrying `order: 1.0` is refused before the write reservation, while the same
/// records written with integer `order` persist and render.
#[tokio::test]
async fn float_order_record_is_rejected_on_direct_put() {
    let temp = TempDb::new();
    let store = MindStore::open(&temp.database_path())
        .await
        .expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    let mut float_order = belief("cog_float_order", 0.4);
    float_order["order"] = json!(1.0);
    let poisoned = with_module(
        "ke_wren",
        None,
        json!({"records": [float_order], "world_facts": [], "extensions": {}}),
    );
    match port.put_knowledge_entry(poisoned, None).await {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::InvalidInput);
            assert!(reject.message.contains("cog_float_order"), "got {reject:?}");
        }
        SpokeResult::Ok(_) => panic!("a float-order carrier must not persist"),
    }

    let clean = with_module(
        "ke_wren",
        None,
        json!({
            "records": [belief("cog_hearsay", 0.4)],
            "world_facts": [],
            "extensions": {},
        }),
    );
    let stored = expect_ok(
        port.put_knowledge_entry(clean, None).await,
        "integer-order carrier",
    );
    assert_eq!(stored.revision, Some(1));
    let view = derive_mind_state(&stored).expect("derive integer carrier");
    let value = mind_state_value(&view, "ms_wren_dusk").expect("render mind state");
    expect_ok(validate_mind_state(&value), "integer-order mind state");
    store.close().await;
}

/// The record rows are the storage authority: they read back in `append_seq` order rather than
/// the records' own `order`, a scoped `record_id` addresses one row, and a duplicate id in the
/// same holder is invalid input. The header read sees the assigned revision with no head until
/// an append advances it.
#[tokio::test]
async fn record_rows_keep_append_order_and_scoped_ids() {
    let temp = TempDb::new();
    let path = temp.database_path();
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    expect_ok(
        port.put_knowledge_entry(carrier("ke_wren", None), None)
            .await,
        "create carrier",
    );

    // A second connection so the test holds the write reservation itself.
    let pool = second_pool(&path).await;

    let header = expect_ok(
        read_carrier_header(&pool, "wren", "ke_wren").await,
        "header",
    );
    assert_eq!((header.revision, header.head_seq), (1, None));

    // The second record sorts first by its own `order`: only the storage order may decide.
    let mut second_by_storage = belief("cog_early", 0.6);
    second_by_storage["order"] = json!(2);
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.expect("begin");
    expect_ok(
        insert_record(&mut tx, "wren", "ke_wren", 1, &belief("cog_late", 0.4)).await,
        "first record",
    );
    expect_ok(
        insert_record(&mut tx, "wren", "ke_wren", 2, &second_by_storage).await,
        "second record",
    );
    match insert_record(&mut tx, "wren", "ke_wren", 3, &belief("cog_late", 0.9)).await {
        SpokeResult::Reject(reject) => assert_eq!(reject.code, SpokeRejectCode::InvalidInput),
        SpokeResult::Ok(()) => panic!("a duplicate scoped record_id must not be stored"),
    }
    tx.commit().await.expect("commit");

    let records = expect_ok(read_records(&pool, "wren", "ke_wren").await, "records");
    assert_eq!(module_keys(&records), vec!["cog_late", "cog_early"]);

    let scoped = expect_ok(
        read_record(&pool, "wren", "ke_wren", "cog_early").await,
        "scoped read",
    )
    .expect("the stored record is addressable by its scoped id");
    assert_eq!(scoped["q"]["value"].as_f64(), Some(0.6));

    store.close().await;
}

/// CAS regression: an append against a stale expected revision is refused with the
/// generic stale rejection (the workflow maps it to the retryable conflict) and
/// leaves the committed record, head and revision untouched.
#[tokio::test]
async fn append_rejects_a_stale_revision_without_touching_rows() {
    let temp = TempDb::new();
    let path = temp.database_path();
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    expect_ok(
        port.put_knowledge_entry(carrier("ke_wren", None), None)
            .await,
        "create carrier",
    );
    let pool = second_pool(&path).await;

    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.expect("begin");
    let assigned = expect_ok(
        append_cognition(&mut tx, "wren", "ke_wren", 1, &belief("cog_hearsay", 0.4)).await,
        "first append",
    );
    assert_eq!(assigned, 2);
    tx.commit().await.expect("commit");

    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.expect("begin");
    match append_cognition(&mut tx, "wren", "ke_wren", 1, &belief("cog_newer", 0.5)).await {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::StoredRevisionStale);
            let details = reject.details.as_ref().expect("stale details");
            assert_eq!(details.get("expected_base_revision"), Some(&json!(1)));
            assert_eq!(details.get("stored_revision"), Some(&json!(2)));
        }
        SpokeResult::Ok(_) => panic!("a stale expected revision must not append"),
    }
    tx.rollback().await.expect("rollback");

    let header = expect_ok(
        read_carrier_header(&pool, "wren", "ke_wren").await,
        "header",
    );
    assert_eq!((header.revision, header.head_seq), (2, Some(1)));
    let records = expect_ok(read_records(&pool, "wren", "ke_wren").await, "records");
    assert_eq!(module_keys(&records), vec!["cog_hearsay"]);
    store.close().await;
}

/// CAS regression: a duplicate scoped record_id at the current revision is invalid
/// input at the storage append — not a storage failure — and writes nothing.
#[tokio::test]
async fn append_rejects_a_duplicate_id_at_the_current_revision() {
    let temp = TempDb::new();
    let path = temp.database_path();
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    expect_ok(
        port.put_knowledge_entry(carrier("ke_wren", None), None)
            .await,
        "create carrier",
    );
    let pool = second_pool(&path).await;

    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.expect("begin");
    let assigned = expect_ok(
        append_cognition(&mut tx, "wren", "ke_wren", 1, &belief("cog_hearsay", 0.4)).await,
        "first append",
    );
    assert_eq!(assigned, 2);
    tx.commit().await.expect("commit");

    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.expect("begin");
    match append_cognition(&mut tx, "wren", "ke_wren", 2, &belief("cog_hearsay", 0.9)).await {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::InvalidInput);
            assert!(
                reject.message.contains("cog_hearsay") && reject.message.contains("already exists"),
                "got {reject:?}"
            );
        }
        SpokeResult::Ok(_) => panic!("a duplicate scoped record_id must not append"),
    }
    tx.rollback().await.expect("rollback");

    let header = expect_ok(
        read_carrier_header(&pool, "wren", "ke_wren").await,
        "header",
    );
    assert_eq!((header.revision, header.head_seq), (2, Some(1)));
    let current = expect_ok(
        read_current_record(&pool, "wren", "ke_wren").await,
        "current",
    )
    .expect("the committed record is the head");
    assert_eq!(
        current["q"]["value"].as_f64(),
        Some(0.4),
        "the duplicate never overwrote the committed record"
    );
    store.close().await;
}

/// Rollback of the caller's transaction discards the record row, the head advance
/// and the revision advance together.
#[tokio::test]
async fn rollback_discards_an_append_entirely() {
    let temp = TempDb::new();
    let path = temp.database_path();
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    expect_ok(
        port.put_knowledge_entry(carrier("ke_wren", None), None)
            .await,
        "create carrier",
    );
    let pool = second_pool(&path).await;

    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.expect("begin");
    match append_cognition(
        &mut tx,
        "wren",
        "ke_wren",
        1,
        &belief("cog_rolled_back", 0.4),
    )
    .await
    {
        SpokeResult::Ok(assigned) => assert_eq!(assigned, 2, "the append succeeds first"),
        SpokeResult::Reject(reject) => panic!("the append itself must succeed: {reject:?}"),
    }
    tx.rollback().await.expect("rollback");

    let header = expect_ok(
        read_carrier_header(&pool, "wren", "ke_wren").await,
        "header",
    );
    assert_eq!(
        (header.revision, header.head_seq),
        (1, None),
        "the rollback restores the empty carrier"
    );
    let records = expect_ok(read_records(&pool, "wren", "ke_wren").await, "records");
    assert!(records.is_empty(), "no record row survives the rollback");
    assert_eq!(
        expect_ok(
            read_current_record(&pool, "wren", "ke_wren").await,
            "current"
        ),
        None
    );
    store.close().await;
}

/// Module presence is explicit storage state: an absent module stays absent with no
/// head, an empty records member stays present-but-empty with no head, and the first
/// append onto a module-less carrier seeds the same envelope a full put would have.
#[tokio::test]
async fn missing_and_empty_cognition_modules_stay_explicit() {
    let temp = TempDb::new();
    let path = temp.database_path();
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let pool = second_pool(&path).await;

    let created = expect_ok(
        port.put_knowledge_entry(carrier("ke_plain", None), None)
            .await,
        "create module-less carrier",
    );
    assert_eq!(created.revision, Some(1));
    let created_empty = expect_ok(
        port.put_knowledge_entry(
            with_module(
                "ke_empty",
                None,
                json!({"records": [], "world_facts": [], "extensions": {}}),
            ),
            None,
        )
        .await,
        "create empty-records carrier",
    );
    assert_eq!(created_empty.revision, Some(1));

    let plain = expect_ok(
        read_carrier(&port, "ke_plain").await,
        "read module-less carrier",
    );
    assert!(
        serde_json::to_value(&plain).expect("entry serializes")["modules"]
            .get("cognition")
            .is_none(),
        "an absent module stays absent"
    );
    let empty = expect_ok(read_carrier(&port, "ke_empty").await, "read empty carrier");
    assert_eq!(
        module_of(&empty)["records"],
        json!([]),
        "an empty records member stays present"
    );
    assert!(matches!(
        derive_mind_state(&plain),
        Err(MindError::NoRecords)
    ));
    assert!(matches!(
        derive_mind_state(&empty),
        Err(MindError::NoRecords)
    ));
    for entry_id in ["ke_plain", "ke_empty"] {
        let header = expect_ok(read_carrier_header(&pool, "wren", entry_id).await, "header");
        assert_eq!(header.head_seq, None, "{entry_id} has no rows to point at");
    }

    // The first storage append onto the module-less carrier seeds the envelope.
    let mut seeded = belief("cog_seeded", 0.4);
    seeded["holder"] = reference("knowledge_entry", "ke_plain");
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.expect("begin");
    match append_cognition(&mut tx, "wren", "ke_plain", 1, &seeded).await {
        SpokeResult::Ok(assigned) => assert_eq!(assigned, 2),
        SpokeResult::Reject(reject) => {
            panic!("append onto a module-less carrier rejected: {reject:?}")
        }
    }
    tx.commit().await.expect("commit");

    let seeded_read = expect_ok(read_carrier(&port, "ke_plain").await, "read seeded carrier");
    let records = module_of(&seeded_read)["records"]
        .as_array()
        .expect("records")
        .clone();
    assert_eq!(module_keys(&records), vec!["cog_seeded"]);
    assert_eq!(
        module_of(&seeded_read)["world_facts"],
        json!([]),
        "the seeded envelope matches the full-put shape"
    );
    let header = expect_ok(
        read_carrier_header(&pool, "wren", "ke_plain").await,
        "seeded header",
    );
    assert_eq!((header.revision, header.head_seq), (2, Some(1)));
    store.close().await;
}

/// Full-put CAS: the committed record prefix is immutable — dropping, rewriting or
/// reordering it is invalid input, every refusal leaves the stored carrier untouched,
/// and a put that only appends a suffix commits the new revision.
#[tokio::test]
async fn full_put_keeps_the_committed_prefix_immutable() {
    let temp = TempDb::new();
    let store = MindStore::open(&temp.database_path())
        .await
        .expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    let first = with_order(belief("cog_prefix_first", 0.4), 2);
    let second = belief("cog_prefix_second", 0.6);
    let created = expect_ok(
        port.put_knowledge_entry(
            with_module(
                "ke_wren",
                None,
                json!({
                    "records": [first.clone(), second.clone()],
                    "world_facts": [world_fact()],
                    "extensions": {},
                }),
            ),
            None,
        )
        .await,
        "create carrier with two records",
    );
    assert_eq!(created.revision, Some(1));
    let baseline = expect_ok(read_carrier(&port, "ke_wren").await, "read carrier");

    // Dropping a committed record is invalid input.
    match port
        .put_knowledge_entry(
            with_module(
                "ke_wren",
                Some(1),
                json!({
                    "records": [second.clone()],
                    "world_facts": [world_fact()],
                    "extensions": {},
                }),
            ),
            Some(1),
        )
        .await
    {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::InvalidInput);
            assert!(
                reject.message.contains("drops 1 committed"),
                "got {reject:?}"
            );
        }
        SpokeResult::Ok(entry) => panic!("a prefix-dropping put was persisted: {entry:?}"),
    }

    // Rewriting a committed record's value is invalid input.
    let rewritten = with_order(belief("cog_prefix_first", 0.45), 2);
    match port
        .put_knowledge_entry(
            with_module(
                "ke_wren",
                Some(1),
                json!({
                    "records": [rewritten, second.clone()],
                    "world_facts": [world_fact()],
                    "extensions": {},
                }),
            ),
            Some(1),
        )
        .await
    {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::InvalidInput);
            assert!(
                reject
                    .message
                    .contains("rewrites committed cognition record at position 0"),
                "got {reject:?}"
            );
        }
        SpokeResult::Ok(entry) => panic!("a prefix-rewriting put was persisted: {entry:?}"),
    }

    // Reordering the committed prefix is invalid input too.
    match port
        .put_knowledge_entry(
            with_module(
                "ke_wren",
                Some(1),
                json!({
                    "records": [second.clone(), first.clone()],
                    "world_facts": [world_fact()],
                    "extensions": {},
                }),
            ),
            Some(1),
        )
        .await
    {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::InvalidInput);
            assert!(
                reject
                    .message
                    .contains("rewrites committed cognition record at position 0"),
                "got {reject:?}"
            );
        }
        SpokeResult::Ok(entry) => panic!("a prefix-reordering put was persisted: {entry:?}"),
    }

    // Every refusal left the stored carrier exactly as it was.
    let reread = expect_ok(read_carrier(&port, "ke_wren").await, "reread carrier");
    assert_eq!(reread.revision, Some(1));
    assert_eq!(
        serde_json::to_value(&reread).expect("entry serializes"),
        serde_json::to_value(&baseline).expect("entry serializes"),
        "the refused puts must leave the committed prefix untouched"
    );

    // A put that appends a validated suffix commits.
    let suffix = belief("cog_prefix_third", 0.75);
    let updated = expect_ok(
        port.put_knowledge_entry(
            with_module(
                "ke_wren",
                Some(1),
                json!({
                    "records": [first, second, suffix],
                    "world_facts": [world_fact()],
                    "extensions": {},
                }),
            ),
            Some(1),
        )
        .await,
        "append-only put",
    );
    assert_eq!(updated.revision, Some(2));
    let reread = expect_ok(
        read_carrier(&port, "ke_wren").await,
        "read after suffix put",
    );
    let records = module_of(&reread)["records"]
        .as_array()
        .expect("records")
        .clone();
    assert_eq!(
        module_keys(&records),
        vec!["cog_prefix_first", "cog_prefix_second", "cog_prefix_third"]
    );
    assert_eq!(
        records[0]["q"]["value"].as_f64(),
        Some(0.4),
        "the committed prefix keeps its values"
    );
    assert_eq!(
        module_of(&reread)["world_facts"],
        json!([world_fact()]),
        "the put preserves the module envelope beside the appended suffix"
    );
    store.close().await;
}

/// Per-holder isolation: heads, records and record-id uniqueness are scoped per
/// (collection, entry) — two holders in one collection and the same entry id in two
/// collections keep independent pointers, and one holder's record id never blocks
/// another holder's.
#[tokio::test]
async fn holders_and_collections_isolate_heads_records_and_ids() {
    let temp = TempDb::new();
    let path = temp.database_path();
    let store = MindStore::open(&path).await.expect("open store");
    let wren = store.knowledge(KnowledgeScope::new("wren"));
    let studio = store.knowledge(KnowledgeScope::new("studio"));
    let pool = second_pool(&path).await;

    expect_ok(
        wren.put_knowledge_entry(carrier("ke_wren", None), None)
            .await,
        "create wren holder",
    );
    expect_ok(
        wren.put_knowledge_entry(carrier("ke_fox", None), None)
            .await,
        "create fox holder",
    );
    expect_ok(
        studio
            .put_knowledge_entry(carrier("ke_wren", None), None)
            .await,
        "create studio holder",
    );

    let held = |entry_id: &str, record_id: &str, q: f64| {
        let mut record = belief(record_id, q);
        record["holder"] = reference("knowledge_entry", entry_id);
        record
    };

    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.expect("begin");
    let w1 = expect_ok(
        append_cognition(
            &mut tx,
            "wren",
            "ke_wren",
            1,
            &held("ke_wren", "cog_w1", 0.4),
        )
        .await,
        "append w1",
    );
    assert_eq!(w1, 2);
    let f1 = expect_ok(
        append_cognition(&mut tx, "wren", "ke_fox", 1, &held("ke_fox", "cog_f1", 0.5)).await,
        "append f1",
    );
    assert_eq!(f1, 2);
    let w2 = expect_ok(
        append_cognition(
            &mut tx,
            "wren",
            "ke_wren",
            2,
            &held("ke_wren", "cog_w2", 0.6),
        )
        .await,
        "append w2",
    );
    assert_eq!(w2, 3);
    let s1 = expect_ok(
        append_cognition(
            &mut tx,
            "studio",
            "ke_wren",
            1,
            &held("ke_wren", "cog_s1", 0.7),
        )
        .await,
        "append s1",
    );
    assert_eq!(s1, 2);
    // The same record_id as wren's committed cog_w1 is a different scoped id in studio.
    let s2 = expect_ok(
        append_cognition(
            &mut tx,
            "studio",
            "ke_wren",
            2,
            &held("ke_wren", "cog_w1", 0.9),
        )
        .await,
        "studio reuses wren's record id",
    );
    assert_eq!(s2, 3);
    tx.commit().await.expect("commit");

    // Current judgments are per holder.
    let wren_current = expect_ok(
        read_current_record(&pool, "wren", "ke_wren").await,
        "wren current",
    )
    .expect("wren has a head");
    assert_eq!(wren_current["record_id"].as_str(), Some("cog_w2"));
    let fox_current = expect_ok(
        read_current_record(&pool, "wren", "ke_fox").await,
        "fox current",
    )
    .expect("fox has a head");
    assert_eq!(fox_current["record_id"].as_str(), Some("cog_f1"));
    let studio_current = expect_ok(
        read_current_record(&pool, "studio", "ke_wren").await,
        "studio current",
    )
    .expect("studio has a head");
    assert_eq!(studio_current["record_id"].as_str(), Some("cog_w1"));
    assert_eq!(studio_current["q"]["value"].as_f64(), Some(0.9));

    // Historical lookups never cross the scope.
    let fox_sees_w1 = expect_ok(
        read_record(&pool, "wren", "ke_fox", "cog_w1").await,
        "fox scoped lookup",
    );
    assert_eq!(fox_sees_w1, None);
    let fox_records = expect_ok(read_records(&pool, "wren", "ke_fox").await, "fox records");
    assert_eq!(module_keys(&fox_records), vec!["cog_f1"]);

    // Independent revisions and heads per holder and per collection.
    for (collection, entry_id, expected) in [
        ("wren", "ke_wren", (3, Some(2))),
        ("wren", "ke_fox", (2, Some(1))),
        ("studio", "ke_wren", (3, Some(2))),
    ] {
        let header = expect_ok(
            read_carrier_header(&pool, collection, entry_id).await,
            "header",
        );
        assert_eq!(
            (header.revision, header.head_seq),
            expected,
            "{collection}/{entry_id}"
        );
    }
    store.close().await;
}

/// Historical rows stay addressable by scoped id while the head moves, and reads
/// follow append order, not the records' own `order` field.
#[tokio::test]
async fn historical_records_stay_addressable_while_the_head_moves() {
    let temp = TempDb::new();
    let path = temp.database_path();
    let store = MindStore::open(&path).await.expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));
    let pool = second_pool(&path).await;
    expect_ok(
        port.put_knowledge_entry(carrier("ke_wren", None), None)
            .await,
        "create carrier",
    );

    // Appended oldest first; the DTO `order` field counts the other way.
    let oldest = with_order(belief("cog_oldest", 0.2), 3);
    let middle = with_order(belief("cog_middle", 0.5), 1);
    let newest = with_order(belief("cog_newest", 0.8), 2);

    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.expect("begin");
    for (expected, record) in [(1, &oldest), (2, &middle), (3, &newest)] {
        let assigned = expect_ok(
            append_cognition(&mut tx, "wren", "ke_wren", expected, record).await,
            "append",
        );
        assert_eq!(assigned, expected + 1);
    }
    tx.commit().await.expect("commit");

    let historical = expect_ok(
        read_record(&pool, "wren", "ke_wren", "cog_oldest").await,
        "oldest lookup",
    )
    .expect("the historical row is addressable");
    assert_eq!(historical["q"]["value"].as_f64(), Some(0.2));
    let middle_read = expect_ok(
        read_record(&pool, "wren", "ke_wren", "cog_middle").await,
        "middle lookup",
    )
    .expect("the historical row is addressable");
    assert_eq!(middle_read["q"]["value"].as_f64(), Some(0.5));
    let missing = expect_ok(
        read_record(&pool, "wren", "ke_wren", "cog_absent").await,
        "missing lookup",
    );
    assert_eq!(missing, None);

    let records = expect_ok(read_records(&pool, "wren", "ke_wren").await, "records");
    assert_eq!(
        module_keys(&records),
        vec!["cog_oldest", "cog_middle", "cog_newest"],
        "append order, not the DTO order field"
    );
    let current = expect_ok(
        read_current_record(&pool, "wren", "ke_wren").await,
        "current",
    )
    .expect("the head is the newest record");
    assert_eq!(current["record_id"].as_str(), Some("cog_newest"));
    let header = expect_ok(
        read_carrier_header(&pool, "wren", "ke_wren").await,
        "header",
    );
    assert_eq!((header.revision, header.head_seq), (4, Some(3)));
    store.close().await;
}

/// The storage projection equals the pre-change composition: the same records put as
/// rows read back as the exact full DTO the in-memory carrier holds, and the derived
/// `MindState` — two groups here — is byte-identical, selecting within each group by
/// append order, never by the DTO `order` field.
#[tokio::test]
async fn projection_matches_the_pre_change_fixtures_across_groups() {
    let temp = TempDb::new();
    let store = MindStore::open(&temp.database_path())
        .await
        .expect("open store");
    let port = store.knowledge(KnowledgeScope::new("wren"));

    // In-memory composition over inline records: the pre-change DTO path.
    let seeded = with_module(
        "ke_wren",
        Some(1),
        json!({
            "records": [],
            "world_facts": [world_fact()],
            "extensions": {},
        }),
    );
    let north_first = with_order(belief("cog_a", 0.4), 2);
    let north_second = belief("cog_b", 0.7);
    let mut south = belief("cog_c", 0.3);
    south["target"]["target_id"] = json!("prop_south_gate_open");

    let mut in_memory = seeded;
    for record in [north_first.clone(), north_second.clone(), south.clone()] {
        in_memory = append_cognition_record(&in_memory, record).expect("in-memory append");
    }
    let mem_view = derive_mind_state(&in_memory).expect("derive from the inline carrier");
    let mem_state = mind_state_value(&mem_view, "ms_wren_fixture").expect("render the inline view");

    // The same records stored as rows must project to the same DTO and view.
    let created = expect_ok(
        port.put_knowledge_entry(
            with_module(
                "ke_wren",
                None,
                json!({
                    "records": [north_first, north_second, south],
                    "world_facts": [world_fact()],
                    "extensions": {},
                }),
            ),
            None,
        )
        .await,
        "create carrier with three records",
    );
    assert_eq!(created.revision, Some(1));
    let stored = expect_ok(read_carrier(&port, "ke_wren").await, "read stored carrier");
    assert_eq!(
        module_of(&stored),
        module_of(&in_memory),
        "the row projection must equal the pre-change inline DTO"
    );

    let view = derive_mind_state(&stored).expect("derive from the stored carrier");
    assert_eq!(view.holder_entry_id, "ke_wren");
    assert_eq!(view.carrier_revision, 1);
    assert_eq!(view.records, mem_view.records);
    let state = mind_state_value(&view, "ms_wren_fixture").expect("render the stored view");
    assert_eq!(
        state, mem_state,
        "the derived MindState must be byte-identical to the pre-change derivation"
    );

    // Two addressed targets are two groups, in first-appearance order; the last
    // appended record of each group wins, whatever its `order` says.
    let items = state["cognition"]["items"]
        .as_array()
        .expect("projection items");
    assert_eq!(items.len(), 2);
    assert_eq!(
        items[0]["target"]["target_id"],
        json!("prop_north_gate_closed")
    );
    assert_eq!(
        items[0]["selection"]["input_record_ids"],
        json!(["cog_a", "cog_b"])
    );
    assert_eq!(
        items[0]["selection"]["selected_record_id"], "cog_b",
        "append order wins inside the group, not the DTO order field"
    );
    assert_eq!(
        items[1]["target"]["target_id"],
        json!("prop_south_gate_open")
    );
    assert_eq!(items[1]["selection"]["input_record_ids"], json!(["cog_c"]));
    assert_eq!(items[1]["selection"]["selected_record_id"], "cog_c");
    store.close().await;
}
