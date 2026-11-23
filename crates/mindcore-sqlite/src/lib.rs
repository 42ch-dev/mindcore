//! SQLite (SQLx) backend implementing the mindcore-base storage traits over one local database file.

mod knowledge;
mod repository;
mod store;

pub mod cognition;
pub mod vectors;

pub use knowledge::{
    append_cognition, list_entries_in, prune_entry_history, read_carrier_header, read_entry,
    validate_admission_scope, write_entry, CarrierHeader, KnowledgeEntryLifecycle, PruneReport,
    RoutedKnowledgeStore, ScopedKnowledgeStore, SERVED_KNOWLEDGE_LIST_LIMIT,
};
pub use repository::{
    MemoryExportRow, MemoryImportRow, MemoryReceiptSummary, SqliteMemoryRepository,
    SqliteRepositoryError,
};
pub use store::MindStore;
