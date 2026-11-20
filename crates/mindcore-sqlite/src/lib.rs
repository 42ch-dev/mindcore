//! SQLite (SQLx) backend implementing the mindcore-base storage traits over one local database file.

mod knowledge;
mod memory;
mod repository;
mod store;

pub mod cognition;

pub use knowledge::{
    list_entries_in, read_entry, validate_admission_scope, write_entry, RoutedKnowledgeStore,
    ScopedKnowledgeStore, SERVED_KNOWLEDGE_LIST_LIMIT,
};
pub use repository::{SqliteMemoryRepository, SqliteRepositoryError};
pub use store::MindStore;
