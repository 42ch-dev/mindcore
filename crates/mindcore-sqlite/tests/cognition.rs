//! Behaviour of the cognition carrier, the derived MindState and the scoped carrier helpers.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use mindcore_core::cognition::{
    append_cognition_record, derive_mind_state, mind_state_value, MindError,
};
use mindcore_core::knowledge::KnowledgeScope;
use mindcore_sqlite::cognition::{read_carrier, write_carrier};
use mindcore_sqlite::MindStore;
use serde_json::{json, Value};
use spoke_operations::{
    validate_cognition_module, validate_mind_state, KnowledgeEntryPort, SpokeResult,
};
use spoke_schemas::knowledge_entry::{
    KnowledgeEntryBody, KnowledgeEntryCanonicalName, KnowledgeEntryModulesKey,
    KnowledgeEntryModulesValue,
};
use spoke_schemas::KnowledgeEntry;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn database_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before the epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "mindcore-cognition-{}-{nanos}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
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
    json!({"kind": kind, "id": id, "namespace": "mindcore"})
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
    let path = database_path();
    let store = MindStore::open(&path).await.expect("open store");
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

    let mut empty = carrier("ke_wren", Some(1));
    empty.modules.insert(
        KnowledgeEntryModulesKey::try_from("cognition").expect("valid module key"),
        KnowledgeEntryModulesValue::from(
            json!({"records": [], "world_facts": [], "extensions": {}})
                .as_object()
                .expect("module object")
                .clone(),
        ),
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
