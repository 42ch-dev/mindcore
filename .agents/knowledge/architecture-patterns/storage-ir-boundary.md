---
module: mindcore-base
date: 2026-11-20
problem_type: architecture_pattern
category: architecture-patterns
severity: medium
plan_id: v02-embed-seam
applies_when:
  - A durable store persists protocol payloads directly
  - A second storage backend must serve the same protocol semantics
  - Wire types and stored types are the same shape today
tags:
  - architecture
  - persistence
  - ir
---

# Storage-independent IR boundary with a single protocol adapter

## Problem

A durable store that persists protocol payloads directly couples three concerns: the wire shape, the protocol semantics (reject codes, idempotency, revision CAS, grant evaluation) and the storage mechanism. Adding a second backend then means re-implementing the semantics per backend, and any divergence shows up as inconsistent refusals rather than a compile error.

Nexus had already solved this shape for its memory provider: one `LocalMemoryProvider` facade implements `spoke_operations::MemoryPort`, a provider-private `storage.rs` owns the transaction layer, and `policy.rs` owns the declared surface. New storage does not re-implement the port.

## Solution

Three layers, with the protocol adapter written once:

1. **IR (record model)** — storage-independent record types (`MemoryCandidateIr`, `MemoryRecordIr`, `ReviewReceiptIr`, `KnowledgeEntryIr`). No SQL, no connection handle, no wire version constant. Wire-version knowledge stays at the boundary.
2. **Repository boundary** — a trait the storage backend implements. It states its transaction expectation explicitly: the *caller's* already-open transaction is borrowed; the repository never opens, migrates or commits. Express this without naming `sqlx`, so the trait stays backend-agnostic.
3. **Protocol adapter (one)** — converts wire types to IR and back, owns reject mapping, idempotency probing, revision CAS and grant evaluation, and is the only place those rules exist. A future `mindcore-spoke` crate is that layer; providers below it stay thin.

## Fidelity rule for wire interconversion

A round trip must preserve, or explicitly reject:

| Property | Rule |
|---|---|
| Open fields (unknown keys in open objects) | preserved verbatim |
| `extensions` | preserved, keys and values; key order is not part of the contract |
| Array order and duplicates | preserved |
| Value and field presence | absent stays absent; explicit `null` stays `null` — never normalised to each other |
| Unknown fields in closed objects | rejected, never dropped |

Proof: a round-trip test over wire fixtures, constructible without a database, that fails when a mapping drops or defaults a field.

## Consequences

- Adding a backend costs a repository implementation, not a protocol re-implementation.
- The adapter is a hot-path cost: every read and write converts. Accept it once, centrally, rather than N times.
- The IR must be allowed to grow fields the wire does not have yet; the reverse (wire fields the IR cannot hold) is a bug by definition.
- A borrowed-transaction repository is incompatible with a store that owns its own pool. Choosing the borrowed shape is a commitment: the store must not also open its own transaction for the same operation.

## Evidence

- `crates/mindcore-base/src/ir.rs`, `crates/mindcore-base/src/ir/wire.rs`, `crates/mindcore-base/tests/ir_roundtrip.rs` (iteration `v02-embed`).
- Nexus `2d3cd8dd5` `crates/nexus-memory-provider/src/{lib,storage,policy}.rs` for the facade/private-storage split.
- The gap this page did not close: `mindcore-sqlite` still owns its pool and transaction, so no repository borrows a caller's transaction yet (`I-000020`).
