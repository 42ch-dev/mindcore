---
module: mindcore-sqlite
date: 2026-11-21
problem_type: architecture_pattern
category: architecture-patterns
severity: high
plan_id: store-protocol-schema
applies_when:
  - A store crate ships a migration while its tests build the same schema from their own DDL
  - Two persistence shapes exist for one concept and only one of them is exercised end to end
  - A pre-release project can still rewrite its migration set instead of adding one
tags:
  - sqlite
  - migration
  - test-fixture
  - bootstrap
  - schema
last_updated: 2026-11-21
---

# One migration, one schema source: let the production open path be the only DDL

## Problem

`MindStore::open` applied `crates/mindcore-sqlite/migrations/0001_init.sql`, which created three
legacy memory tables with nine columns each. The recall path served by `MemoryAdapter` needed the
ten-table protocol shape, and that shape existed only in
`crates/mindcore-sqlite/tests/fixtures/settlement-prepare.sql` — a file every test applied itself.

Nothing failed, because no test ever asked the production open path to serve a request. The
mismatch was invisible until a host tried it: a freshly opened database answered `no such table:
memory_spaces`.

## Root cause

Two schema sources for one concept. A test that installs its own DDL proves the repository works
against *that* DDL; it says nothing about what `open` produces. The fixture also hid the split: it
carried the protocol tables, so the borrowed-transaction tests passed while the shipped migration
stayed behind.

The deeper cause is sequencing. The protocol shape arrived through a caller-migrated Nexus schema
(the borrowed-transaction route) before the local store needed it, so the local migration kept the
older shape and the fixture carried the newer one.

## Solution

1. **Rebuild the migration set as one file.** `0001_init.sql` now carries the protocol memory tables,
   `knowledge_entries`, `cognition_records` and `memory_embedding_vectors`; `0002` is deleted. Only
   `0001_init.sql` remains under `crates/mindcore-sqlite/migrations/`.
2. **Delete the legacy track rather than keep both.** `crates/mindcore-sqlite/src/memory.rs`, its
   `impl MemoryStore for MindStore`, the trait in `crates/mindcore-base/src/memory.rs` and the
   legacy DTOs are gone; `RecallQuery` stays because the protocol repository uses it.
3. **Make tests consume the migration.** Every harness calls `MindStore::open` and seeds rows; no
   test file contains `CREATE TABLE`. `crates/mindcore-sqlite/tests/store_bootstrap.rs` is the
   regression: open a fresh file, provision a space and its first grant, then drive subject
   creation, capture, review, settlement and recall through `MemoryAdapter`, and assert the record
   survives a reopen.
4. **Keep deliberate faults as faults.** Missing-table and poisoned-row injections stay; they inject
   a defect, not a schema.

## Why it works

The starting state now equals the target state, so there is no intermediate shape for a test to
accidentally honour. A regression in the migration fails `store_bootstrap` instead of passing through
a fixture that quietly supplies the missing table.

Pre-release status is what makes step 1 legal: all five crates are `publish = false` at `0.1.0`, so
no migration history is owed and the checksum refusal is an accepted cost. The public guidance has to
say so precisely — recreate only for `VersionMissing(n)` or a checksum mismatch; a busy, locked or
I/O-failing database is retried, never replaced.

## What does not work

- **Adding a third migration that creates the protocol tables.** The dead shape stays, and a reader
  cannot tell which one is authoritative. It also preserves the fixture as a second source.
- **Keeping the legacy `MemoryStore` beside the protocol repository.** Two memory semantics on one
  store type is the defect itself; the next change re-derives the wrong one.
- **Letting tests build their own schema "because it is only a fixture".** That is the mechanism by
  which the mismatch survived.

## Prevention

- A schema exists in exactly one place: the migration. Tests seed data, never tables.
- The end-to-end proof is `open` + public adapter calls, not a harness that shares setup with the
  code under test.
- Public documentation about recreate-only stores names the refusal reason, not the failing call.

## References

- `crates/mindcore-sqlite/migrations/0001_init.sql`
- `crates/mindcore-sqlite/tests/store_bootstrap.rs`
- `crates/mindcore-sqlite/src/store.rs` (`MindStore::open` and its migration notes)
- `.agents/knowledge/architecture-patterns/borrowed-transaction-sqlite-repository.md`
