//! SQLite (SQLx) backend implementing the mindcore-base storage traits over one local database file.

mod knowledge;
mod repository;
mod store;

pub mod cognition;
pub mod vectors;

pub use knowledge::{
    append_cognition, list_entries_in, read_carrier_header, read_entry, validate_admission_scope,
    write_entry, CarrierHeader, KnowledgeEntryLifecycle, RoutedKnowledgeStore,
    ScopedKnowledgeStore, SERVED_KNOWLEDGE_LIST_LIMIT,
};
pub use repository::{SqliteMemoryRepository, SqliteRepositoryError};
pub use store::MindStore;
