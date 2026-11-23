//! Scoped knowledge: the one collection a knowledge port serves and the pointer back to a shared entry.

use std::collections::HashMap;
use std::num::NonZeroU64;

use serde_json::{Map, Value};
use spoke_schemas::knowledge_entry::{
    ExtensionKey as EntryExtensionKey, Shared1, Shared2, SourceAnchor as EntrySourceAnchor,
};
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

/// Build an adoption anchor **without** a source revision — the legacy provenance pointer from a
/// personal entry back to the shared entry it was adopted from.
///
/// This shape cannot be recognized by `mindcore_spi::adopt_knowledge_entry`: an anchor with no
/// recorded revision answers as an unrelated occupant on a repeat. New callers use
/// [`adoption_anchor_at_revision`]; this builder stays for fixtures that predate it.
pub fn adoption_anchor(shared_entry_id: &str, basis: &str) -> SourceAnchor {
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

/// Builds the adoption anchor of a copy taken at `source_revision`.
///
/// Returns the entry's own anchor type directly: the copy's `source_anchor` field is assigned the
/// result, so no wire round trip is needed between the two generated copies of the type.
///
/// Answers `None` for the two shapes [`adoption_anchor_parts`] cannot read back — an empty shared
/// id and a zero revision — so a caller refuses before a copy is anchored on them.
pub fn adoption_anchor_at_revision(
    shared_entry_id: &str,
    source_revision: u64,
) -> Option<EntrySourceAnchor> {
    if shared_entry_id.is_empty() || source_revision == 0 {
        return None;
    }
    let namespace = EntryExtensionKey::try_from("mindcore_adoption").expect("valid extension key");
    let extension = Map::from_iter([
        (
            "basis".to_string(),
            Value::String("shared-entry".to_string()),
        ),
        ("source_revision".to_string(), Value::from(source_revision)),
    ]);
    Some(EntrySourceAnchor {
        extensions: Shared2::from(HashMap::from([(namespace, extension)])),
        label: None,
        mime_type: None,
        schema_version: Shared1::from(NonZeroU64::new(1).expect("non-zero wire baseline")),
        source_id: format!("shared:{shared_entry_id}"),
        span: None,
    })
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
