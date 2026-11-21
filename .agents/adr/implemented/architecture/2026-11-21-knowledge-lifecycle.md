# Knowledge lifecycle: preimage by trigger, retirement by marker

Status: implemented — a trigger keeps the preimage without breaking the cost contract, and a marker retires an entry without dangling its anchors
Date: 2026-11-21
Iteration: `008-knowledge-lifecycle`
Plans: `revision-history`, `supersede`, `adoption-entry`, `lifecycle-proof`
Issues: `I-000073`, `I-000075`, `I-000074`, `I-000076`

## Problem

A knowledge entry was a row that could be overwritten but never observed: `write_entry` replaced `payload` in one `UPDATE`, so the previous version ceased to exist, and the only way to retire a pair of duplicate entries would have been a delete that left every adoption anchor pointing at a missing id. A host had no supported way to adopt a shared entry either — the codebase defined an adoption anchor but no `src/` caller, and the test support built the copy by hand.

Three constraints shaped the answer:

1. A cost contract (`cognition_cost`, `knowledge_list_cost`) pins the statement count of the write and read paths, so history could not be a literal `INSERT` in Rust.
2. `knowledge_entries` is already referenced by `cognition_records` and by adoption anchors that outlive any single entry.
3. The single-migration policy means the schema is edited in place, not extended by a second migration.

## Decision

- **Preimage via a `BEFORE UPDATE OF revision` trigger.** The trigger inserts the replaced row, its head and its own `updated_at` into `knowledge_entry_revisions`, keyed `(collection, entry_id, revision)`. The Rust write path is untouched, so the cost contract still measures the same statements. `cognition_records` gains a nullable `append_prior_shape` so a dedicated append is distinguishable from a full put — the classification is what makes replay's positional gap proof possible.
- **Replay from the nearest saved checkpoint.** Reading revision N takes the first saved revision at or above N and truncates cognition records to that row's head. A gap must be proven by position (consecutive integers, each classified), not by row count: SQLite's non-strict columns admit a `REAL` in an integer position.
- **Retirement by marker, not deletion.** `knowledge_entries.superseded_by` names a same-collection successor through a non-cascading composite foreign key. The write advances the carrier revision, so the trigger captures its preimage. Default reads and lists add a live predicate; one explicit read returns the marker; anchor resolution walks the chain to the first live entry and refuses a cycle as `InternalError`. The list's bound rides on that predicate, so the limit counts **live** rows: retiring an entry frees list budget rather than consuming it, and a collection stays listable while its live set fits. Counting stored rows instead would make a collection permanently unlistable after `SERVED_KNOWLEDGE_LIST_LIMIT` create-and-retire cycles, because nothing shrinks the stored row count.
- **Adoption as a free function over two ports.** `mindcore_spi::adopt_knowledge_entry` takes the caller's source and destination ports, addresses the copy by the source `entry_id`, and treats the anchor's source revision as the idempotency basis. It adds no trait, no table, no index and no `MindWorkflows` change.
- **One anchor parser.** `mindcore_base::knowledge::adoption_anchor_parts` is the only reader of the anchor shape; the SQLite resolver consumes it rather than owning a second copy.
- **Structured refusal classification.** The store tells "the destination slot is occupied by a retired entry" from "your payload is invalid" with a structured detail on the same `InvalidInput` code, never by message matching.

### Adoption is a domain operation, so the SPI consumes `mindcore-base`'s anchor codec

`adopt_knowledge_entry` is a domain-level contract operation, not storage plumbing: it is declared and implemented on the SPI side of the boundary because a host composing two ports is its caller. Reading the adoption anchor is part of that contract, so the SPI consumes `mindcore_base::knowledge::adoption_anchor_parts` and `crates/mindcore-spi/Cargo.toml` now depends on `mindcore-base`. That narrows the rule the capability-boundary ADR recorded — the contract crate carrying no `mindcore-base` dependency — for this one module.

The alternative, a second anchor codec inside `mindcore-spi`, was rejected during review: two parsers for one wire shape is the exact defect the change set out to remove, and the copies would drift the first time the anchor shape moved.

## Consequences

- An entry's history is complete and immutable, and replay is available for every assigned revision.
- An adoption anchor keeps its original provenance even when it now resolves elsewhere, which is the point: it records what was adopted, not what is currently believed.
- A failed write leaves neither a preimage nor a marker: the trigger runs inside the caller's transaction.
- Retirement never removes a row, so storage grows monotonically; there is no compaction or archival policy yet, and none is implied.
- A record whose payload revision disagrees with its column is refused as inconsistent state rather than silently accepted; that is a stronger guarantee than the previous CAS, which trusted the payload.
- The append classification column is load-bearing: removing it, or admitting a non-integer `append_seq`, weakens replay without failing a happy-path test.
- `mindcore-spi` now depends on `mindcore-base` for the anchor codec, so a change to that helper is SPI-visible; the rest of `mindcore-base` still moves without the SPI surface.

## Alternatives considered

- **A Rust-side history insert.** Rejected: it breaks the append path's statement-count contract, and a second write path for lifecycle events invites divergence.
- **A delete-and-repoint retirement.** Rejected: it destroys the record of what was adopted and can dangle an anchor between the two writes.
- **An adoption `KnowledgeAdoption` trait.** Rejected as scope the iteration did not need: the caller already holds both ports, and a free function leaves the SPI surface unchanged.
- **Counting rows to prove a replay gap.** Rejected after review: a counted `REAL` position masks a missing integer.

## References

- `.agents/specs/data/knowledge-lifecycle.md`
- `.agents/specs/data/store-schema.md`
- `.agents/knowledge/architecture-patterns/sql-trigger-preimage-with-cost-contract.md`
- `.agents/knowledge/architecture-patterns/retire-with-a-marker-not-a-delete.md`
- `.agents/adr/implemented/architecture/2026-11-21-single-migration-store-bootstrap.md`
