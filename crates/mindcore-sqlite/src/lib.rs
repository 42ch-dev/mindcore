//! SQLite (SQLx) backend implementing the mindcore-base storage traits over one local database file.

mod knowledge;
mod memory;
mod repository;
mod store;

pub mod cognition;

pub use knowledge::ScopedKnowledgeStore;
pub use repository::{SqliteMemoryRepository, SqliteRepositoryError};
pub use store::MindStore;
