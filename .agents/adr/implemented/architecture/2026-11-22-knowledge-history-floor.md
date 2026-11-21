# ADR: A stored floor bounds knowledge history; pruning is explicit and permanent

Status: implemented — history can be bounded, and a read below the bound refuses instead of guessing
Date: 2026-11-22

## Problem

Every revision-changing full put writes the displaced preimage into `knowledge_entry_revisions`, and nothing removed one. `.agents/specs/data/knowledge-lifecycle.md` recorded the gap plainly: history grows with writes, no retention policy exists.

Bounding it is not a delete loop. A range read answers revision N by taking the first saved checkpoint **at or above** N and deriving the earlier head by subtracting the crossed gap, which is a positional proof: every crossed dedicated append must be the consecutive integer in the range. Delete an older checkpoint and an early read no longer reports "that revision is gone" — it selects a newer one, computes a larger gap, fails the proof and reports `InternalError`, i.e. *the storage is broken*, for a revision we removed ourselves. Worse, a read that falls through to the live-row fallback would answer a truncated request with current content, indistinguishable from a correct answer.

## Decision

Two things, landing together:

1. **A floor on the entry row.** `knowledge_entries.history_floor_revision`, `NOT NULL DEFAULT 1`, is the earliest revision the entry can still answer. `read_entry_revision` compares the request to the floor **before** selecting a checkpoint; at or above the floor nothing changes, below it the read refuses.
2. **A classifiable refusal.** `KnowledgeEntryNotFound` with `details.reason: "HISTORY_TRUNCATED"` and the message `knowledge entry revision was pruned`. Ordinary absence keeps `details: None`, so a caller can `match` "never existed" apart from "we removed it".
3. **Explicit pruning only.** `ScopedKnowledgeStore::prune_history(entry_id, keep)` — an inherent method, because `KnowledgeEntryPort` belongs to the external `spoke_operations` crate and must not grow. `keep` is a call argument with no global default; nothing prunes on a write, on `MindStore::open`, or in a background task. The delete and the floor update share one `BEGIN IMMEDIATE`; the boundary is probed with two rows rather than loading every checkpoint; the floor-only update does not fire the revision-capture trigger.

`spoke-operations` 0.17.0 defines no dedicated code for pruned history, which is why the refusal reuses `KnowledgeEntryNotFound` with a structured detail rather than inventing a code the dependency cannot carry.

## Alternatives considered

- **Tolerate a gap in the positional proof.** Rejected: a dedicated append leaves no preimage, so a missing preimage is indistinguishable from a deleted one. Widening the proof would weaken replay for every entry, not only the pruned ones.
- **Keep the floor in a separate table.** Rejected: the read already fetches the entry row for the live fallback; a second table adds a join and a second lifecycle for one integer.
- **Prune implicitly — after each put, or on open.** Rejected: deleting audit history is a contract-level destructive act. A caller must pay for it deliberately, see how many rows went, and be able to predict which paths can destroy data.
- **A configuration default for `keep`.** Rejected: a workspace-level "keep N" makes silent data loss a default rather than a decision.
- **Archive before delete.** Rejected: an export path adds a consistency surface that must be maintained and tested forever, and it does not bound growth in the database.
- **A defensive branch for "zero rows deleted but the floor moved".** Rejected as unreachable: when more checkpoints than `keep` exist, the next-older row is strictly below the new floor, so the delete always removes at least one row. A regression that prunes and then re-prunes with a smaller window pins the contract instead.

## Consequences

- A read degrades from "silently wrong or internally broken" to "explicit refusal" once history is bounded — the property the whole change exists for.
- The floor is reportable, so a caller can learn what it lost before deciding whether pruning is acceptable.
- Pruning is permanent. There is no undo, no archive, and no retention policy that runs on its own; a host that wants bounded history must call the operation.
- Cost is one column and one comparison on the hottest read path. The prune probe is constant-work in rows read, at the price of a possible index walk for a very large `keep`.
- The floor joins the schema's recreate-on-mismatch policy: no migration path, no back-fill.

## References

- `.agents/specs/data/knowledge-lifecycle.md` § Current state, § Invariants, § Known limits
- `.agents/specs/data/store-schema.md` § Current state
- `crates/mindcore-sqlite/src/knowledge.rs` — `read_entry_revision`, `prune_history`
- `crates/mindcore-sqlite/tests/knowledge_lifecycle.rs`
- `.agents/knowledge/architecture-patterns/revision-floor-gates-replay-before-selection.md`
