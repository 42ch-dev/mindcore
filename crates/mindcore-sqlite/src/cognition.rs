//! Carrier reads and writes over one scoped knowledge collection.

use spoke_operations::{KnowledgeEntryPort, SpokeResult};
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
/// Validation lives in the port's `put`, the single persistence boundary, so this stays a thin
/// revision-guarded write.
pub async fn write_carrier(
    store: &ScopedKnowledgeStore,
    entry: KnowledgeEntry,
    expected_revision: u64,
) -> SpokeResult<KnowledgeEntry> {
    store
        .put_knowledge_entry(entry, Some(expected_revision))
        .await
}
