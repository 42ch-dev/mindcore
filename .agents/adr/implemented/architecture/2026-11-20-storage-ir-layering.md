# ADR: Storage-independent IR between protocol adapters and storage backends

Status: implemented — `crates/mindcore-base/src/ir.rs` plus `ir::wire`
Date: 2026-11-20

## Problem

Mindcore persisted SPOKE payload JSON directly and implemented `KnowledgeEntryPort` in the same crate that owned SQLite. Protocol semantics (reject mapping, idempotency, revision CAS, cognition-module validation) and storage lived together, so a second backend would have to re-implement those semantics, and any divergence would surface as inconsistent refusals rather than a compile error.

Nexus had already solved this shape for memory: one `LocalMemoryProvider` implements `spoke_operations::MemoryPort`, a provider-private `storage.rs` owns the transaction layer, and nothing below re-implements the port.

## Decision

Insert a storage-independent record model (IR) and a repository boundary under a single protocol adapter:

- `mindcore-base::ir` holds the IR record families and the wire codec; it contains no SQL, connection handle or wire-version constant.
- `MemoryRepository` borrows the caller's already-open transaction; it never opens, migrates or commits.
- The protocol adapter (future `mindcore-spoke`) is the only place that converts wire types, maps reject codes and applies replay/CAS/grant rules.

This iteration materialised the contract only; the SQLite read/write path was deliberately left on its current implementation so the parity baseline stayed comparable.

## Consequences

- Adding a backend costs a repository implementation, not a protocol re-implementation.
- Every read and write pays one central conversion. Accepted once, instead of N times per backend.
- No path owns a transaction for an operation a caller wants to envelope. That is exactly the constraint Nexus imposes, and `SqliteMemoryRepository` meets it by borrowing the embedding caller's `&mut Tx<'_>` — it never begins, commits or rolls back. `MindStore::open` bootstraps the standalone protocol schema and owns the pool for the standalone route; it does not take ownership of embedded mutations (`I-000020` is closed history).
- Memory rows map onto the Nexus memory schema, so the parity selectors run unchanged against a caller-migrated database; the repository persists through the IR on that schema (`I-000021` is closed history; `.agents/specs/architecture/storage-ir-boundary.md:14,31-38`).

## Alternatives considered

- **Backend-per-port** (each storage implements the SPOKE ports directly): duplicates reject mapping, idempotency probing, revision CAS and grant evaluation once per backend; rejected.
- **Keep the current coupling, add a trait later**: defers the decision past the point where a second backend appears, and leaves wire shape and storage shape indistinguishable; rejected.

## Evidence

- `crates/mindcore-base/tests/ir_roundtrip.rs` — round trip preserves open fields, `extensions`, array order, duplicates, absence-vs-null; unknown fields in closed objects are refused; three mutations turn it red.
- `crates/mindcore-base/src/ir.rs` — repository trait with the borrowed-transaction expectation, stated without naming `sqlx`.
- `.agents/specs/architecture/storage-ir-boundary.md` — the promoted contract.
