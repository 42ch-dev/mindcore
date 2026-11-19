//! Cognition carrier over `KnowledgeEntry.modules.cognition` and the derived `MindState` view.
//!
//! The carrier stores only current-view standard records in append order; judgment is never
//! rewritten. `MindState` is derived on demand and never persisted.

use serde_json::{json, Map, Value};

use spoke_operations::{
    project_cognition_records, validate_cognition_module, validate_cognition_record,
    validate_mind_state, CurrentViewPolicy, SpokeReject, SpokeResult,
};
use spoke_schemas::data::cognition::{
    CognitionFilter, CognitionSelection, SelectionPolicyIdentity,
};
use spoke_schemas::knowledge_entry::{KnowledgeEntryModulesKey, KnowledgeEntryModulesValue};
use spoke_schemas::{CognitionRecord, KnowledgeEntry};

/// Module namespace of the current-view cognition carrier.
const COGNITION_MODULE: &str = "cognition";

/// Projection policy identity: the last record in carrier order wins within one group.
const CURRENT_VIEW_POLICY: (&str, &str) = ("mindcore_append_order", "1");

/// Realm of every holder reference a carrier can hold: the Mindcore project namespace.
/// A carrier's records are always stored next to the `KnowledgeEntry` they belong to and the
/// comparison never crosses stores, so one fixed realm identifies every holder Mindcore can
/// persist today.
/// shortcut: single fixed realm; revisit when cross-authority import or multi-realm storage lands.
pub const CARRIER_NAMESPACE: &str = "mindcore";

#[derive(Debug, thiserror::Error)]
pub enum MindError {
    #[error("cognition module rejected: {0}")]
    CarrierInvalid(String),
    #[error("cognition record rejected: {0}")]
    RecordInvalid(String),
    #[error("carrier entry has no current-view records")]
    NoRecords,
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// The derived, non-persisted view of one holder.
pub struct MindStateView {
    pub holder_entry_id: String,
    pub carrier_revision: u64,
    pub records: Vec<Value>,
}

/// Carrier invariants for one stored record: `current` view, and held by the carrier entry.
/// Compared fields: `record.view` must be `"current"`, and `record.holder` must be the complete
/// reference `{kind: "knowledge_entry", namespace: CARRIER_NAMESPACE, id: carrier_entry_id}` —
/// the carrier is always a `KnowledgeEntry` stored in the Mindcore realm, and the pinned schema
/// guarantees all three fields on every validated record's holder reference. Nothing else on the
/// record (target, assessor, timestamps) is checked here.
/// Returns the violation message when the record cannot live in `carrier_entry_id`'s carrier.
pub fn carrier_record_violation(record: &Value, carrier_entry_id: &str) -> Option<String> {
    let record_id = record
        .get("record_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if record.get("view").and_then(Value::as_str) != Some("current") {
        return Some(format!(
            "record {record_id} is not placed in the current view"
        ));
    }
    let holder = record.get("holder").and_then(Value::as_object);
    let holder_field = |field: &str| {
        holder
            .and_then(|holder| holder.get(field))
            .and_then(Value::as_str)
    };
    let expected = (
        Some("knowledge_entry"),
        Some(CARRIER_NAMESPACE),
        Some(carrier_entry_id),
    );
    let actual = (
        holder_field("kind"),
        holder_field("namespace"),
        holder_field("id"),
    );
    if actual != expected {
        return Some(format!(
            "record {record_id} names holder {:?}/{:?}/{:?} but the carrier entry is knowledge_entry/{CARRIER_NAMESPACE}/{carrier_entry_id:?}",
            actual.0, actual.1, actual.2
        ));
    }
    None
}

/// Appends one current-view record to the carrier's `modules.cognition.records`, preserving order.
/// Rejects a duplicate record_id and any record that fails SPOKE validation.
pub fn append_cognition_record(
    carrier: &KnowledgeEntry,
    record: Value,
) -> Result<KnowledgeEntry, MindError> {
    let record_id = record
        .get("record_id")
        .and_then(Value::as_str)
        .ok_or_else(|| MindError::RecordInvalid("record has no usable record_id".to_string()))?;
    reject_record(&record)?;
    if let Some(violation) = carrier_record_violation(&record, &carrier.entry_id) {
        return Err(MindError::RecordInvalid(violation));
    }

    let mut next = carrier.clone();
    let mut module = match next.modules.get(&module_key()) {
        Some(KnowledgeEntryModulesValue::Object(module)) => module.clone(),
        Some(KnowledgeEntryModulesValue::Array(_)) => {
            return Err(MindError::CarrierInvalid(
                "cognition module must be a plain object".to_string(),
            ))
        }
        None => Map::from_iter([
            ("records".to_string(), Value::Array(Vec::new())),
            ("world_facts".to_string(), Value::Array(Vec::new())),
            ("extensions".to_string(), Value::Object(Map::new())),
        ]),
    };
    let records = module
        .get_mut("records")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| {
            MindError::CarrierInvalid("cognition module records must be an array".to_string())
        })?;
    let duplicate = records
        .iter()
        .any(|existing| existing.get("record_id").and_then(Value::as_str) == Some(record_id));
    if duplicate {
        return Err(MindError::RecordInvalid(format!(
            "record_id {record_id} already exists in the carrier"
        )));
    }
    records.push(record);
    next.modules
        .insert(module_key(), KnowledgeEntryModulesValue::Object(module));

    let module = module_value(&next).expect("the module was just inserted");
    reject_module(&module)?;
    Ok(next)
}

/// Derives the holder view from the carrier without persisting anything.
pub fn derive_mind_state(carrier: &KnowledgeEntry) -> Result<MindStateView, MindError> {
    let Some(module) = module_value(carrier) else {
        return Err(MindError::NoRecords);
    };
    reject_module(&module)?;
    let records: Vec<Value> = match module.get("records").and_then(Value::as_array) {
        Some(records) if !records.is_empty() => records.clone(),
        _ => return Err(MindError::NoRecords),
    };
    for record in &records {
        if let Some(violation) = carrier_record_violation(record, &carrier.entry_id) {
            return Err(MindError::CarrierInvalid(format!(
                "carrier {}: {violation}",
                carrier.entry_id
            )));
        }
    }
    let carrier_revision = carrier.revision.ok_or_else(|| {
        MindError::CarrierInvalid(format!(
            "carrier {} holds records but no assigned revision",
            carrier.entry_id
        ))
    })?;
    Ok(MindStateView {
        holder_entry_id: carrier.entry_id.clone(),
        carrier_revision,
        records,
    })
}

/// Renders the SPOKE `MindState` wire value for a derived view; must pass `validate_mind_state`.
pub fn mind_state_value(view: &MindStateView, mind_state_id: &str) -> Result<Value, MindError> {
    let records: Vec<CognitionRecord> = view
        .records
        .iter()
        .map(|record| serde_json::from_value(record.clone()))
        .collect::<Result<_, _>>()?;
    let policy = SelectionPolicyIdentity {
        policy_id: CURRENT_VIEW_POLICY
            .0
            .try_into()
            .map_err(|error| MindError::CarrierInvalid(format!("projection policy id: {error}")))?,
        policy_version: CURRENT_VIEW_POLICY.1.try_into().map_err(|error| {
            MindError::CarrierInvalid(format!("projection policy version: {error}"))
        })?,
    };
    let selection = CognitionSelection::Current {
        filter: CognitionFilter::default(),
        selection_policy: policy,
    };
    let projection = match project_cognition_records(&records, &selection, Some(&LastCommitted)) {
        SpokeResult::Ok(projection) => projection,
        SpokeResult::Reject(reject) => return Err(MindError::RecordInvalid(reject_text(&reject))),
    };

    let envelope = json!({
        "schema_version": 1,
        "mind_state_id": mind_state_id,
        "holder_entry_id": view.holder_entry_id,
        "cognition": projection,
        "extensions": {},
    });
    if let SpokeResult::Reject(reject) = validate_mind_state(&envelope) {
        return Err(MindError::CarrierInvalid(format!(
            "mind_state rejected: {}",
            reject_text(&reject)
        )));
    }
    Ok(envelope)
}

/// Selects the last record of one group in carrier order.
struct LastCommitted;

impl CurrentViewPolicy for LastCommitted {
    fn select(&self, assessments: &[&CognitionRecord]) -> Option<String> {
        assessments
            .last()
            .map(|record| record_id(record).to_string())
    }
}

fn record_id(record: &CognitionRecord) -> &str {
    match record {
        CognitionRecord::BeliefRecord(record) => record.record_id.as_str(),
        CognitionRecord::EpistemicAssessment(record) => record.record_id.as_str(),
        CognitionRecord::PerceptionRecord(record) => record.record_id.as_str(),
    }
}

fn module_key() -> KnowledgeEntryModulesKey {
    COGNITION_MODULE.parse().expect("valid module namespace")
}

fn module_value(carrier: &KnowledgeEntry) -> Option<Value> {
    match carrier.modules.get(&module_key())? {
        KnowledgeEntryModulesValue::Object(module) => Some(Value::Object(module.clone())),
        KnowledgeEntryModulesValue::Array(items) => Some(Value::Array(items.clone())),
    }
}

fn reject_record(record: &Value) -> Result<(), MindError> {
    match validate_cognition_record(record) {
        SpokeResult::Ok(()) => Ok(()),
        SpokeResult::Reject(reject) => Err(MindError::RecordInvalid(reject_text(&reject))),
    }
}

fn reject_module(module: &Value) -> Result<(), MindError> {
    match validate_cognition_module(module) {
        SpokeResult::Ok(()) => Ok(()),
        SpokeResult::Reject(reject) => Err(MindError::CarrierInvalid(reject_text(&reject))),
    }
}

fn reject_text(reject: &SpokeReject) -> String {
    format!("{}: {}", reject.code, reject.message)
}
