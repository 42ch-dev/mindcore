//! SQLite (SQLx) backend implementing the mindcore-base storage traits over one local database file.

mod knowledge;
mod repository;
mod store;

pub mod cognition;
pub mod minds;
pub mod vectors;

pub use knowledge::{
    append_cognition, bind_caller, bound_caller, list_entries_in, load_retirement_entry,
    prune_entry_history, read_carrier_header, read_entry, read_entry_at_revision,
    retirement_successors_of, validate_admission_scope, write_entry, CarrierHeader,
    KnowledgeEntryLifecycle, PruneReport, RoutedKnowledgeStore, ScopedKnowledgeStore,
    CALLER_MEMBER, CALLER_NAMESPACE, SERVED_KNOWLEDGE_LIST_LIMIT,
};
pub use repository::{
    decodable_grants, MemoryExportRow, MemoryImportRow, MemoryReceiptSummary,
    SqliteMemoryRepository, SqliteRepositoryError,
};
pub use store::MindStore;
