//! SQLite (SQLx) backend implementing the mindcore-core storage traits over one local database file.

mod memory;
mod store;

pub use store::MindStore;
