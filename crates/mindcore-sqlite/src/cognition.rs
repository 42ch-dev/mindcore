//! Carrier reads and writes over one scoped knowledge collection.

use serde_json::Value;
use spoke_operations::{validate_cognition_module, KnowledgeEntryPort, SpokeResult};
use spoke_schemas::knowledge_entry::{KnowledgeEntryModulesKey, KnowledgeEntryModulesValue};
use spoke_schemas::KnowledgeEntry;

use crate::knowledge::ScopedKnowledgeStore;

/// Reads one carrier from the port's fixed collection.
pub async fn read_carrier(
    store: &ScopedKnowledgeStore,
    entry_id: &str,
) -> SpokeResult<KnowledgeEntry> {
    store.get_knowledge_entry(entry_id).await
}

/// Writes a carrier against the revision the caller derived it from.
///
/// A present `modules.cognition` value is validated before the put, so an invalid module never
/// becomes the authoritative current judgment. The generic knowledge `put` stays unvalidated.
pub async fn write_carrier(
    store: &ScopedKnowledgeStore,
    entry: KnowledgeEntry,
    expected_revision: u64,
) -> SpokeResult<KnowledgeEntry> {
    if let Some(module) = cognition_module(&entry) {
        if let SpokeResult::Reject(reject) = validate_cognition_module(&module) {
            return SpokeResult::Reject(reject);
        }
    }
    store
        .put_knowledge_entry(entry, Some(expected_revision))
        .await
}

fn cognition_module(entry: &KnowledgeEntry) -> Option<Value> {
    let key: KnowledgeEntryModulesKey = "cognition".parse().expect("valid module namespace");
    match entry.modules.get(&key)? {
        KnowledgeEntryModulesValue::Object(module) => Some(Value::Object(module.clone())),
        KnowledgeEntryModulesValue::Array(items) => Some(Value::Array(items.clone())),
    }
}
