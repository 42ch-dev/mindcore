# ADR: One initial migration and no second memory schema

Status: implemented — the production open path could not serve the protocol surface that every test proved
Date: 2026-11-21

## Problem

`MindStore::open` applied a migration that created three legacy memory tables with nine columns each, while the recall path served by `MemoryAdapter` needed the ten-table protocol shape. That shape existed only in a test fixture which every test applied itself. No test ever asked the production open path to serve a request, so the split stayed invisible: a freshly opened database answered `no such table: memory_spaces`.

Two candidate fixes existed. Add a third migration that creates the protocol tables, keeping the legacy ones as dead weight. Or rebuild the migration set and delete the legacy track.

## Decision

Rebuild. `0001_init.sql` becomes the only migration and carries the protocol memory tables, `cognition_records`, `knowledge_entries` and `memory_embedding_vectors`; `0002_memory_embedding_vectors.sql` is deleted. The legacy `impl MemoryStore for MindStore`, `crates/mindcore-sqlite/src/memory.rs`, the `MemoryStore` trait and the legacy DTOs are removed; `RecallQuery` stays because the protocol repository uses it. Tests obtain the schema only from `open` and seed rows.

This is legal because the workspace is unreleased: all five crates are `publish = false` at `0.1.0`, so no migration history is owed. An existing database is refused by its migration checksum (`VersionMissing(n)` for a removed applied version, a checksum mismatch for a changed one) and recreated.

## Alternatives considered

- **Add a migration for the protocol tables and keep the legacy ones.** Rejected: the dead shape stays, a reader cannot tell which is authoritative, and the schema keeps living in two places. It also preserves the fixture as a second source of truth.
- **Keep `MemoryStore` beside the protocol repository.** Rejected: two memory semantics on one store type is the defect. Removing it is a breaking API change, accepted because no workspace consumer existed outside the implementor and its two tests.
- **Migrate data from the legacy shape.** Rejected: nothing in production has data to migrate, and a migration path would have to be maintained and tested for a shape that never shipped.

## Consequences

- Starting state equals target state: there is no intermediate schema for a test or a host to honour accidentally.
- Every existing local database becomes invalid. The public guidance scopes recreation to a migration mismatch and says a busy, locked or I/O-failing database is retried — the distinction matters because only the first case is unrecoverable.
- Deleting a public trait is a one-way door for any future embedder; the workspace had no such consumer, and re-adding a trait later is cheaper than carrying two semantics now.
- The fixture disappears as a schema source, so a migration regression now fails `store_bootstrap` rather than passing silently.

## References

- `.agents/specs/data/store-schema.md`
- `crates/mindcore-sqlite/migrations/0001_init.sql`
- `crates/mindcore-sqlite/tests/store_bootstrap.rs`
