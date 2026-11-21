//! Scoped knowledge: the one collection a knowledge port serves and the pointer back to a shared entry.

use std::collections::HashMap;
use std::num::NonZeroU64;

use serde_json::{Map, Value};
use spoke_schemas::knowledge_entry::ExtensionKey as EntryExtensionKey;
use spoke_schemas::source_anchor::ExtensionKey;
use spoke_schemas::{KnowledgeEntry, SourceAnchor};

/// The collection a knowledge store serves; fixed when the store is built.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KnowledgeScope {
    pub collection: String,
}

impl KnowledgeScope {
    pub fn new(collection: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
        }
    }
}

/// Builds the provenance pointer from a personal entry back to the shared entry it was adopted from.
pub fn adoption_anchor(shared_entry_id: &str, basis: &str) -> SourceAnchor {
    // Both constants satisfy the pinned wire pattern `^[a-z][a-z0-9_-]*$` and the `>= 1` bound.
    let namespace = ExtensionKey::try_from("mindcore_adoption").expect("valid extension key");
    let mut extensions = HashMap::new();
    extensions.insert(
        namespace,
        Map::from_iter([("basis".to_string(), Value::String(basis.to_string()))]),
    );
    SourceAnchor {
        extensions,
        label: None,
        mime_type: None,
        schema_version: NonZeroU64::new(1).expect("non-zero wire baseline"),
        source_id: format!("shared:{shared_entry_id}"),
        span: None,
    }
}

/// The shared entry id an adoption anchor points at and, when it records one, the adopted revision.
///
/// Recognizes only the Mindcore namespace with a non-empty `basis` and a non-empty id after
/// `shared:`; a legacy anchor without `source_revision` still resolves by its source id, while a
/// present revision must be a positive integer with nothing else naming an assigned revision.
pub fn adoption_anchor_parts(entry: &KnowledgeEntry) -> Option<(&str, Option<u64>)> {
    let anchor = entry.source_anchor.as_ref()?;
    let source_id = anchor.source_id.strip_prefix("shared:")?;
    if source_id.is_empty() {
        return None;
    }
    let namespace = EntryExtensionKey::try_from("mindcore_adoption").ok()?;
    let fields = anchor.extensions.get(&namespace)?;
    if fields.get("basis")?.as_str()?.is_empty() {
        return None;
    }
    let revision = match fields.get("source_revision") {
        Some(revision) => Some(revision.as_u64().filter(|revision| *revision > 0)?),
        None => None,
    };
    Some((source_id, revision))
}
