# Data: cognition record storage

## Scope

SQLite persistence for a holder's cognition under its `KnowledgeEntry`. This does not add a remote history endpoint, change SPOKE wire types, persist `MindState`, or upgrade existing database contents.

## Current state

The authority scope is `(collection, entry_id)`, the existing `KnowledgeScope` plus its holder entry; Memory `space_id`, record holder strings and request timestamps do not partition this authority.

- `cognition_records(collection TEXT NOT NULL, entry_id TEXT NOT NULL, append_seq INTEGER NOT NULL CHECK (append_seq > 0), record_id TEXT NOT NULL, payload TEXT NOT NULL)` stores one validated record JSON value per row. The primary key is `(collection, entry_id, append_seq)`, uniqueness is `(collection, entry_id, record_id)`, and `(collection, entry_id)` references `knowledge_entries`. Historical rows are addressable by that scoped record id; ordered reads use `append_seq ASC`, not the DTO's `order`, timestamps, or lexical id order.
- `knowledge_entries.cognition_head_seq INTEGER NULL` is the only stored current-judgment pointer. A composite foreign key `(collection, entry_id, cognition_head_seq)` references the record primary key. `NULL` means no records; the first append is `1`, each subsequent append increments the head. Current judgment is a scoped carrier/record indexed lookup with constant rows read. A full history or full grouped `MindState` projection is not a constant-size read.
- The parent payload keeps its outer DTO and cognition module envelope (`world_facts`, `extensions` and member presence), but not an inline records array. Full `KnowledgeEntry` reads reconstruct `modules.cognition.records` from the same snapshot in append order; an absent module stays absent and a present empty array stays empty. The stock wire validator and `MindState` projection see the same values and order; the projection still selects the last record within each SPOKE group, not the overall head for every group.

The storage append operation borrows the existing write transaction. After rechecking admission, withholding and cited revisions, read the carrier header without hydrating history, compare the expected carrier revision, validate the incoming record including holder, view and integer `order`, insert the one row and advance both the head and outer revision atomically. A stale observed revision stays a retryable `RevisionConflict` on `commit_cognition`; duplicate id at the current revision remains invalid input. Rollback leaves all rows and the revision unchanged. After successful commit return the assigned carrier revision; do not rebuild the full DTO or derive all groups to return that scalar.

A full-entry put still validates the incoming authority and uses the same CAS. On create, insert the submitted validated records in order. On update, the submitted record array must carry the immutable committed prefix unchanged; insert only its new suffix. Removing, reordering or rewriting a committed record is invalid. Full DTO puts and full DTO reads necessarily process their full payload; only the dedicated append/current lookup are bounded independently of history. Recreate old pre-release databases instead of migrating or dual-writing the old format: an applied migration whose embedded file changed is refused by its checksum at open, so the operator deletes the file and reopens it.

## Invariants

- One holder has one authoritative revision and head; a rejected or replayed append cannot advance it or duplicate a row.
- Historical rows are immutable, an append never touches them, and ordered projections use the same SQLite snapshot as the carrier revision.
- Ablation: the scoped head avoids scanning history for the current judgment; the scoped record-id uniqueness makes duplicate rejection independent of history size.

## Where it lives

- `crates/mindcore-sqlite/migrations/` and `crates/mindcore-sqlite/src/{cognition,knowledge}.rs` own schema, transaction operations and the DTO projection boundary.
- `crates/mindcore-spoke/src/workflows.rs` supplies caller admission and the expected carrier revision. `crates/mindcore-base/src/cognition.rs` owns the record rules and derived `MindState` projection.

## Change obligations

Changes to carrier storage, ordering, DTO shape, or CAS must update this contract and the corresponding append, projection and rollback tests together.
