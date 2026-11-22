# Architecture: mind model and embedded persistence

## Scope

The first delivered contract of Mindcore: what a Mind is, which records carry which transition, and what the embedded SQLite store guarantees. It covers memory, personal and shared `KnowledgeEntry` storage, the cognition carrier and derived `MindState`. It excludes remote transport, capability negotiation, data movement, provider swap and caller authentication.

## Current state

- `mindcore-base` holds the transport-independent types and rules: ids (`MindId`, `SpaceId`, `ContextId`, `SubjectId`), `CallerId` (the adapter-supplied caller name the durable rows keep as attribution), the `ir` record families with their wire codec and the borrowed-transaction `MemoryRepository` boundary, the `RecallQuery` selection that boundary takes, the knowledge scope and adoption-anchor helper, and the cognition carrier helpers with `MindState` derivation. It performs no I/O.
- `mindcore-sqlite` implements `MindStore`: each opened store owns its own single-connection SQLx pool, runs the embedded migrator once (one initial file, `0001_init.sql`, carrying the ten protocol memory tables, `knowledge_entries`, `cognition_records` and `memory_embedding_vectors`), and sets WAL, foreign keys and a five-second busy timeout on that file. `knowledge_entries.history_floor_revision` is the persisted replay floor — the first revision above the newest deleted preimage, defaulting to `1` so a fresh row reads as "nothing pruned". `SqliteMemoryRepository` — the protocol repository, borrowing the caller's transaction rather than owning a pool — and the per-collection `ScopedKnowledgeStore` (implementing the pinned `KnowledgeEntryPort`) are implemented there.
- A `Mind` is a stable logical actor identity (Creator, Character); it is not a caller, a database file, a collection or a wire type. A caller is identified by `CallerId`, a plain string the embedding adapter supplies: records keep it as attribution (`requester`, `updated_by`), it is not mental state, and it decides no admission.
- Memory: `capture` stages a pending candidate; candidate review consumes it, projecting one retained record (`promote`), several (`fragment`) or none (`drop`), and `dismiss` consumes it without a record. Recall reads live records only. Each mutation carries an idempotency key: a replay returns the recorded outcome instead of repeating the effect, and a second review of a consumed candidate is refused. A record keeps its candidate's placement (`space`, `subject`, context) and a repository read orders by `created_at DESC, record_id`; recall withholds a subject whose journal holds an unsettled intent.
- Knowledge: an entry is created with `expected_base_revision = None` only when absent, updated with `Some(revision)` only when the stored revision equals it, and the adapter assigns `1` or `stored + 1` into both the column and the payload. Every write binds the collection fixed when the port instance was constructed; refusals leave the row untouched. Revision replay reads the entry's persisted `history_floor_revision` first and refuses a positive revision below it as truncated (`details.reason` `HISTORY_TRUNCATED`) before checkpoint selection, so a pruned preimage never answers with current content; at or above the floor the selection, live fallback and positional proof are unchanged.
- Cognition: current judgment is a per-record row set under the holder's `KnowledgeEntry`. `cognition_records` stores one validated record JSON per row keyed `(collection, entry_id, append_seq)`, and `knowledge_entries.cognition_head_seq` is the single current-judgment pointer with a composite foreign key back to that row; `NULL` means no records yet. An append reads the carrier header without hydrating history, compares the expected carrier revision, validates the record (holder, view, integer `order`), inserts one row and advances head plus outer revision in the caller's transaction. Full DTO reads reconstruct `modules.cognition.records` from one SQLite snapshot in `append_seq` order; an absent module stays absent, a present empty array stays empty. A stored record is accepted only when its holder is exactly `{kind: "knowledge_entry", namespace: "mindcore", id: <carrier entry id>}`. `MindState` is derived from the carrier's authorized revision and is never persisted. Details: [`data/cognition-record-storage.md`](../data/cognition-record-storage.md).
- A present `modules.cognition` module is validated at the single persistence boundary (`put_knowledge_entry`) before the write reservation, and every appended record is validated by the storage append (`append_cognition`) on the caller's transaction, so no path can commit an invalid carrier.
- A present `order` on a stored cognition record must be written as a JSON integer representation: the pinned `BeliefOrder` type reads `i64` only, so `1.0` (or any non-integer number) is refused at the same carrier boundary — append and `put_knowledge_entry` share this rule, and the generic IR codec is not narrowed.

## Invariants

- Each opened `MindStore` owns one pool and runs one migrator against the database file it was opened on; exactly one store per file is the caller's obligation, and two stores on one file are unsupported rather than refused.
- A mutating operation takes the SQLite write reservation (`BEGIN IMMEDIATE`) before reading claim or revision state.
- A candidate is not recallable before review; a rejected write changes neither payload nor revision.
- An entry is resolvable only through a port bound to its own collection, even when two collections hold the same raw `entry_id`; no API accepts a caller-supplied collection, `owner` or `viewpoint` as a selector.
- Only an explicit carrier update changes the current judgment; adoption, a shared-content change and a read never do. Appending preserves the previously committed records byte-for-byte.
- `ScopedKnowledgeStore` keeps the caller's timestamp values in the `KnowledgeEntry` payload without that memory round-trip refusal; the store encodes only its separate SQL bookkeeping columns. Raw timestamp spelling is not a guarantee of the typed knowledge port — wire fidelity belongs to the IR codec (`storage-ir-boundary.md`).

## Where it lives

- `crates/mindcore-base/src/{ids,boundary,memory,knowledge,cognition}.rs`, `crates/mindcore-base/src/ir.rs`, `crates/mindcore-base/src/ir/wire.rs` and `crates/mindcore-base/src/ir/wire/memory.rs`
- `crates/mindcore-sqlite/src/{store,repository,knowledge,cognition,vectors}.rs`
- Decisions: [standalone embedded SQLite store](../../adr/implemented/architecture/2026-11-20-embedded-sqlite-storage.md), [one initial migration and no second memory schema](../../adr/implemented/architecture/2026-11-21-single-migration-store-bootstrap.md)
- `crates/mindcore-sqlite/migrations/0001_init.sql`
- Tests: `crates/mindcore-sqlite/tests/{store_bootstrap,borrowed_memory_repository,concurrent_settlement,knowledge,cognition,cognition_cost,embedded_mind,embed_parity}.rs`; runnable scenario: `crates/mindcore-sqlite/examples/iteration_01.rs`
- Fixture: `fixtures/iteration-01/shared-collection.json`
- Commands: `cargo test --locked -p mindcore-sqlite`, `cargo run --locked -p mindcore-sqlite --example iteration_01`, `cargo clippy --locked --workspace --all-targets -- -D warnings`

## Change obligations

- Changing the schema, a table, or the migration ownership requires updating this page in the same change and proving reopen behaviour.
- Changing a write path must keep the write reservation before any claim read and must not add a second validation point for the cognition module.
- Changing the carrier shape or the derivation requires updating this page and the SPOKE validator usage together.
- Adding a caller-controlled selector to any storage API is a contract change: it must be rejected or this page must record the authorization model explicitly.
