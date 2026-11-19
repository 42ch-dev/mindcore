//! SQLite (SQLx) backend implementing the mindcore-core storage traits over one local database file.

mod knowledge;
mod memory;
mod store;

pub use knowledge::ScopedKnowledgeStore;
pub use store::MindStore;
