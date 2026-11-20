# Architecture: mind model and embedded persistence

## Scope

The first delivered contract of Mindcore: what a Mind is, which records carry which transition, and what the embedded SQLite store guarantees. It covers memory, personal and shared `KnowledgeEntry` storage, the cognition carrier and derived `MindState`. It excludes remote transport, capability negotiation, data movement, provider swap and caller authentication.

## Current state

- `mindcore-base` holds the transport-independent types and rules: ids (`MindId`, `SpaceId`, `ContextId`, `SubjectId`), `TrustedScope` (adapter-supplied caller plus space, never persisted), memory types and the `MemoryStore` trait, the knowledge scope and adoption-anchor helper, and the cognition carrier helpers with `MindState` derivation. It performs no I/O.
- `mindcore-sqlite` implements `MindStore`: each opened store owns its own single-connection SQLx pool, runs the embedded migrator once (applying any pending migrations, currently 0001_init.sql), and sets WAL, foreign keys and a five-second busy timeout on that file. `MemoryStore` and the per-collection `ScopedKnowledgeStore` (implementing the pinned `KnowledgeEntryPort`) are implemented there.
- A `Mind` is a stable logical actor identity (Creator, Character); it is not a caller, a database file, a collection or a wire type. A caller is identified by `TrustedScope` and is never part of persisted mental state. Reading a shared collection changes no personal record.
- Memory: `capture` creates a candidate, `retain` produces the record, `recall` reads records only. `capture` is idempotent by candidate id; `retain` is idempotent by request id and refuses a second distinct request for the same candidate. Records keep the candidate's original placement (`space`, `subject`, `context`) and recall orders by `recorded_at ASC, record_id ASC`.
- Knowledge: an entry is created with `expected_base_revision = None` only when absent, updated with `Some(revision)` only when the stored revision equals it, and the adapter assigns `1` or `stored + 1` into both the column and the payload. Every write binds the collection fixed when the port instance was constructed; refusals leave the row untouched.
- Cognition: current judgment lives on the holder's `KnowledgeEntry` under `modules.cognition` (`{records, world_facts, extensions}`); each append validates the incoming record as `view = "current"` and rejects duplicate record ids. Each record is stored as its own row in `cognition_records` (keyed `(collection, entry_id, append_seq)`, scoped unique `record_id`); `knowledge_entries.cognition_head_seq` is the only current-judgment pointer, and an append inserts one row and advances the head and the carrier revision inside the caller's transaction without reading or rewriting earlier records. A full DTO read projects `records` from those rows in `append_seq` order; the stored payload keeps the module envelope without the inline array, so an absent module stays absent and a present empty array stays empty. A stored record is accepted only when its holder is exactly `{kind: "knowledge_entry", namespace: "mindcore", id: <carrier entry id>}` — checked at `put_knowledge_entry` before the write reservation, on every storage append, and again in `derive_mind_state`. `MindState` is derived from the carrier's authorized revision and is never persisted.
- A present `modules.cognition` module is validated at the single persistence boundary (`put_knowledge_entry`) before the write reservation, and every appended record is validated by the storage append (`append_cognition`) on the caller's transaction, so no path can commit an invalid carrier.
- A present `order` on a stored cognition record must be written as a JSON integer representation: the pinned `BeliefOrder` type reads `i64` only, so `1.0` (or any non-integer number) is refused at the same carrier boundary — append and `put_knowledge_entry` share this rule, and the generic IR codec is not narrowed.

## Invariants

- Each opened `MindStore` owns one pool and runs one migrator against the database file it was opened on; exactly one store per file is the caller's obligation, and two stores on one file are unsupported rather than refused.
- A mutating operation takes the SQLite write reservation (`BEGIN IMMEDIATE`) before reading claim or revision state.
- A candidate is not recallable before review; a rejected write changes neither payload nor revision.
- An entry is resolvable only through a port bound to its own collection, even when two collections hold the same raw `entry_id`; no API accepts a caller-supplied collection, `owner` or `viewpoint` as a selector.
- Only an explicit carrier update changes the current judgment; adoption, a shared-content change and a read never do. Appending preserves the previously committed records byte-for-byte.
- Legacy `MemoryStore` capture and retain write timestamps as fixed-width RFC 3339 UTC, and `capture` refuses a caller timestamp it cannot read back before insert.
- `ScopedKnowledgeStore` keeps the caller's timestamp values in the `KnowledgeEntry` payload without that memory round-trip refusal; the store encodes only its separate SQL bookkeeping columns. Raw timestamp spelling is not a guarantee of the typed knowledge port — wire fidelity belongs to the IR codec (`storage-ir-boundary.md`).

## Where it lives

- `crates/mindcore-base/src/{ids,boundary,memory,knowledge,cognition}.rs`
- `crates/mindcore-sqlite/src/{store,memory,knowledge,cognition}.rs`
- Decision: [standalone embedded SQLite store](../../adr/implemented/architecture/2026-11-20-embedded-sqlite-storage.md)
- `crates/mindcore-sqlite/migrations/0001_init.sql`
- Tests: `crates/mindcore-sqlite/tests/{memory,knowledge,cognition,embedded_mind}.rs`; runnable scenario: `crates/mindcore-sqlite/examples/iteration_01.rs`
- Fixture: `fixtures/iteration-01/shared-collection.json`
- Commands: `cargo test --locked -p mindcore-sqlite`, `cargo run --locked -p mindcore-sqlite --example iteration_01`, `cargo clippy --locked --workspace --all-targets -- -D warnings`

## Change obligations

- Changing the schema, a table, or the migration ownership requires updating this page in the same change and proving reopen behaviour.
- Changing a write path must keep the write reservation before any claim read and must not add a second validation point for the cognition module.
- Changing the carrier shape or the derivation requires updating this page and the SPOKE validator usage together.
- Adding a caller-controlled selector to any storage API is a contract change: it must be rejected or this page must record the authorization model explicitly.
