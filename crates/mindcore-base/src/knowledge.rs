//! Scoped knowledge: the one collection a knowledge port serves and the pointer back to a shared entry.

use std::collections::HashMap;
use std::num::NonZeroU64;

use serde_json::{Map, Value};
use spoke_operations::{SpokeReject, SpokeRejectCode};
use spoke_schemas::knowledge_entry::{
    AdoptionAnchor as EntryAdoptionAnchor, AdoptionAnchorSourceEntryId,
    AdoptionAnchorSourceScopeId, Shared1, Shared2,
};
use spoke_schemas::KnowledgeEntry;

/// The largest revision the served contract can carry: every revision member lives at or below the
/// IEEE-754 safe-integer ceiling, and a stored value above it cannot form a conformant response.
pub const SERVED_REVISION_BOUND: u64 = (1 << 53) - 1;

/// The fixed message an adoption storage fault carries onto the wire; the diagnostic stays
/// host-side.
pub const ADOPTION_FAULT: &str = "knowledge adoption could not complete";

/// The one `details.kind` every adoption fault carries. The failing stage, the SQL statement and
/// the stored payload are host-side diagnostics: they name local tables and columns, so they never
/// reach the signed refusal.
pub const ADOPTION_FAULT_KIND: &str = "store";

/// The fixed fault an adoption answers with: the fixed message, [`ADOPTION_FAULT_KIND`] as its one
/// `details.kind`, and nothing else. The kind takes no argument, so no caller can put its own value
/// on the wire: the SPI's occupancy classification and the runtime's refusal hygiene build the same
/// refusal, and the diagnostic that caused it stays host-side.
pub fn adoption_fault() -> SpokeReject {
    SpokeReject {
        code: SpokeRejectCode::InternalError,
        message: ADOPTION_FAULT.into(),
        details: Some(Map::from_iter([(
            "kind".to_string(),
            Value::String(ADOPTION_FAULT_KIND.to_string()),
        )])),
    }
}

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

/// Builds the adoption anchor of a copy taken from `source_scope_id` at `source_revision`.
///
/// Answers `None` for the shapes an anchor cannot carry — an empty scope id, an empty entry id and
/// a zero revision — so a caller refuses before a copy is anchored on them.
pub fn adoption_anchor(
    source_scope_id: &str,
    source_entry_id: &str,
    source_revision: u64,
) -> Option<EntryAdoptionAnchor> {
    if source_revision == 0 {
        return None;
    }
    Some(EntryAdoptionAnchor {
        extensions: Shared2::from(HashMap::new()),
        schema_version: Shared1::from(NonZeroU64::new(1).expect("non-zero wire baseline")),
        source_entry_id: AdoptionAnchorSourceEntryId::try_from(source_entry_id).ok()?,
        source_revision,
        source_scope_id: AdoptionAnchorSourceScopeId::try_from(source_scope_id).ok()?,
    })
}

/// The source `(scope id, entry id, revision)` triple an adoption anchor names, or `None` when the
/// entry carries no anchor.
pub fn adoption_anchor_parts(entry: &KnowledgeEntry) -> Option<(&str, &str, u64)> {
    let anchor = entry.adoption_anchor.as_ref()?;
    Some((
        anchor.source_scope_id.as_str(),
        anchor.source_entry_id.as_str(),
        anchor.source_revision,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use spoke_schemas::knowledge_entry::{KnowledgeEntryBody, KnowledgeEntryCanonicalName};

    fn entry_carrying(anchor: Option<EntryAdoptionAnchor>) -> KnowledgeEntry {
        KnowledgeEntry {
            adoption_anchor: anchor,
            body: KnowledgeEntryBody::default(),
            canonical_name: KnowledgeEntryCanonicalName::try_from("ke_shared name")
                .expect("non-empty canonical name"),
            created_at: None,
            disclosure: None,
            entry_id: "ke_shared".to_string(),
            entry_type: "location".to_string(),
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

    #[test]
    fn the_anchor_triple_round_trips() {
        let anchor = adoption_anchor("spc_shared", "ke_shared", 3).expect("a named triple");
        assert_eq!(
            adoption_anchor_parts(&entry_carrying(Some(anchor))),
            Some(("spc_shared", "ke_shared", 3))
        );
    }

    #[test]
    fn an_empty_id_or_a_zero_revision_names_no_anchor() {
        assert!(adoption_anchor("", "ke_shared", 1).is_none());
        assert!(adoption_anchor("spc_shared", "", 1).is_none());
        assert!(adoption_anchor("spc_shared", "ke_shared", 0).is_none());
        assert_eq!(adoption_anchor_parts(&entry_carrying(None)), None);
    }
}
