# Data: store schema and migration policy

## Scope

The SQLite schema a host gets from `MindStore::open`, and the policy for changing it before the first release. It does not define protocol semantics (those live with the adapter and repository), persistence for remote faces, or any upgrade path.

## Current state

`crates/mindcore-sqlite/migrations/` holds exactly one file, `0001_init.sql`. It creates, in dependency order:

- the ten protocol memory tables — `memory_spaces`, `memory_subjects`, `memory_grants`, `memory_candidates`, `memory_records`, `memory_receipts`, `memory_journal`, `memory_profiles`, `memory_reflections`, `memory_provider_meta` — with the column names, types, `CHECK` clauses, `WITHOUT ROWID` declarations and indexes that `SqliteMemoryRepository` decodes;
- `cognition_records` and `knowledge_entries`, in that order, with the composite foreign key from the holder's `cognition_head_seq` to the record primary key;
- `memory_embedding_vectors`, keyed `(record_id, identity_hash)` with the record revision, dimension and little-endian `f32` bytes, plus the identity index.

`MindStore::open` applies the embedded set with `PRAGMA foreign_keys` on, so every foreign key resolves at creation time. The protocol memory tables carry no fixture-only foreign keys: the Nexus writer guards, outbox/immutability triggers, `nexus_writer_*` UDFs and the three `core_*` tables remain caller-owned and are not installed here.

There is one schema source. Tests open a temporary database through `MindStore::open` and seed rows; no test file declares `CREATE TABLE` for a table `open` creates. SQLx preparation reads the same migration into an empty file.

## Migration policy

The workspace is unreleased: every crate is `publish = false` at `0.1.0`, so no migration history is owed. The set is therefore **edited in place** and a database is recreated rather than upgraded.

`MindStore::open` refuses a database whose applied set no longer matches, with two distinct shapes:

- `VersionMissing(n)` — the database applied a version the embedded set no longer contains.
- A migration checksum mismatch — the applied version is still embedded but its SQL changed.

Both mean recreate. A busy or locked database, an I/O error or a permissions problem is not a migration mismatch: fix the cause and retry; never delete a valid file.

## Invariants

- One schema source: the migration. Tests seed data, never tables.
- The production open path is the only way a test obtains the schema, so a regression in the migration fails the end-to-end test instead of passing through a fixture that supplies the missing table.
- A migration change and the code that reads it land together; an intermediate state where the schema moved and the readers did not is not a deliverable.
- Column names, types and constraints are shared with the borrowed-transaction repository; changing one without the other breaks decoding at runtime, not at compile time.

## References

- `crates/mindcore-sqlite/migrations/0001_init.sql`
- `crates/mindcore-sqlite/src/store.rs`
- `crates/mindcore-sqlite/tests/store_bootstrap.rs`
- `.agents/adr/implemented/architecture/2026-11-21-single-migration-store-bootstrap.md`
