//! Scoped knowledge: the one collection a knowledge port serves and the pointer back to a shared entry.

use std::collections::HashMap;
use std::num::NonZeroU64;

use serde_json::{Map, Value};
use spoke_schemas::source_anchor::ExtensionKey;
use spoke_schemas::SourceAnchor;

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
