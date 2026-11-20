//! SQLite (SQLx) backend implementing the mindcore-base storage traits over one local database file.

mod knowledge;
mod memory;
mod repository;
mod store;

pub mod cognition;

pub use knowledge::{
    append_cognition, list_entries_in, read_carrier_header, read_entry, write_entry, CarrierHeader,
    ScopedKnowledgeStore,
};
pub use repository::{SqliteMemoryRepository, SqliteRepositoryError};
pub use store::MindStore;
