//! Storage-agnostic mindcore domain core: SPOKE contracts and the backend traits they are served through.
//!
//! [`ir`] defines records and the borrowed-transaction repository boundary without choosing a
//! backend. [`ir::wire`] converts raw SPOKE JSON without normalizing member presence. A future
//! `mindcore-spoke` crate belongs above that boundary: it owns protocol admission and delegates
//! storage to an implementation of [`ir::MemoryRepository`]. The existing [`memory::MemoryStore`]
//! path remains separate.

pub mod boundary;
pub mod cognition;
pub mod ids;
pub mod ir;
pub mod knowledge;
pub mod memory;
