# Data: cognition record storage

## Scope

SQLite persistence for a holder's cognition under its `KnowledgeEntry`. This does not add a remote history endpoint, change SPOKE wire types, persist `MindState`, or upgrade existing database contents.

## Current state

The authority scope is `(collection, entry_id)`, the existing `KnowledgeScope` plus its holder entry; Memory `space_id`, record holder strings and request timestamps do not partition this authority.

- `cognition_records(collection TEXT NOT NULL, entry_id TEXT NOT NULL, append_seq INTEGER NOT NULL CHECK (append_seq > 0), record_id TEXT NOT NULL, payload TEXT NOT NULL, append_prior_shape INTEGER NULL CHECK (append_prior_shape IN (0, 1, 2)))` stores one validated record JSON value per row. `append_prior_shape` is `NULL` on a record a full-entry put committed and, on a dedicated append's record, classifies the envelope that append replaced: `0` kept its `records` member, `1` had no cognition module, `2` had a module without `records`. The primary key is `(collection, entry_id, append_seq)`, uniqueness is `(collection, entry_id, record_id)`, and `(collection, entry_id)` references `knowledge_entries`. Historical rows are addressable by that scoped record id; ordered reads use `append_seq ASC`, not the DTO's `order`, timestamps, or lexical id order.
- `knowledge_entries.cognition_head_seq INTEGER NULL` is the only stored current-judgment pointer. A composite foreign key `(collection, entry_id, cognition_head_seq)` references the record primary key. `NULL` means no records; the first append is `1`, each subsequent append increments the head. Current judgment is a scoped carrier/record indexed lookup over a live carrier with constant rows read. A full history or full grouped `MindState` projection is not a constant-size read.
- The parent payload keeps its outer DTO and cognition module envelope (`world_facts`, `extensions` and member presence), but not an inline records array. Full `KnowledgeEntry` reads reconstruct `modules.cognition.records` from the same snapshot in append order; an absent module stays absent and a present empty array stays empty. The stock wire validator and `MindState` projection see the same values and order; the projection still selects the last record within each SPOKE group, not the overall head for every group.

History is physical, not a payload rebuild. A revision-changing full put saves the payload, cognition head and write instant it displaced into `knowledge_entry_revisions` through a `BEFORE UPDATE OF revision` trigger on `knowledge_entries`; the live row always supplies the newest revision. A dedicated append saves no preimage — it keeps its three driver-issued statements, two changed rows and scale-independent VDBE work — and its record's `append_prior_shape` lets a later replay reverse the first append's envelope change. Replay (`ScopedKnowledgeStore::get_entry_at_revision`) selects the first saved revision at or above the requested one, otherwise the live row when its revision reaches the requested one, subtracts the gap from that checkpoint's head, and reads only the record rows through the derived head in `append_seq` order; it never loads a later record and truncates in Rust. An exact checkpoint supplies its payload and head; a gap before it consists only of dedicated appends. The gap is verified, not assumed: every crossed head position must be a classified append row, so a missing position or a record a full put committed is a storage error (`InternalError`) rather than a replay. A derived zero head restores the pre-append envelope from `append_seq = 1`'s shape: remove the module for `1`, remove only `records` for `2`, keep the empty member for `0`. Revision zero, an unassigned revision, a value outside the stored range and a missing entry all answer `KnowledgeEntryNotFound` with `knowledge entry revision not found` and no details.

The storage append operation borrows the existing write transaction. After rechecking admission, withholding and cited revisions, read the carrier header without hydrating history, compare the expected carrier revision, validate the incoming record including holder, view and integer `order`, insert the one row and advance both the head and outer revision atomically. A stale observed revision stays a retryable `RevisionConflict` on `commit_cognition`; duplicate id at the current revision remains invalid input. Rollback leaves all rows and the revision unchanged. After successful commit return the assigned carrier revision; do not rebuild the full DTO or derive all groups to return that scalar.

A superseded carrier takes no new judgment and offers no current judgment: `knowledge_entries.superseded_by` is selected by that same header statement, so a marked row answers like an absent one — the workflow's header read reports `KnowledgeEntryNotFound`, the storage current-judgment lookup returns none, and the storage append refuses with invalid input before inserting a record row or advancing the revision. Its records stay addressable by scoped id and through revision replay. A full-entry put refuses a marked row the same way, in the statement that already reads its stored revision.

A full-entry put still validates the incoming authority and uses the same CAS. On create, insert the submitted validated records in order. On update, the submitted record array must carry the immutable committed prefix unchanged; insert only its new suffix. Removing, reordering or rewriting a committed record is invalid. Full DTO puts and full DTO reads necessarily process their full payload; only the dedicated append/current lookup are bounded independently of history. Recreate old pre-release databases instead of migrating or dual-writing the old format: an applied migration whose embedded file changed is refused by its checksum at open, so the operator deletes the file and reopens it.

## Invariants

- One holder has one authoritative revision and head; a rejected or replayed append cannot advance it or duplicate a row.
- Historical rows are immutable, an append never touches them, and ordered projections use the same SQLite snapshot as the carrier revision.
- A replay selects its checkpoint and hydrates its records in one read transaction and never rewrites a saved preimage or a committed record row.
- A successful write assigns a revision the signed revision column can represent: an unrepresentable next revision is refused with `InternalError` before any record, preimage or carrier row is written.
- Ablation: the scoped head avoids scanning history for the current judgment; the scoped record-id uniqueness makes duplicate rejection independent of history size.

## Where it lives

- `crates/mindcore-sqlite/migrations/` and `crates/mindcore-sqlite/src/{cognition,knowledge}.rs` own schema, transaction operations and the DTO projection boundary.
- `crates/mindcore-spoke/src/workflows.rs` supplies caller admission and the expected carrier revision. `crates/mindcore-base/src/cognition.rs` owns the record rules and derived `MindState` projection.

## Change obligations

Changes to carrier storage, ordering, DTO shape, or CAS must update this contract and the corresponding append, projection and rollback tests together.
