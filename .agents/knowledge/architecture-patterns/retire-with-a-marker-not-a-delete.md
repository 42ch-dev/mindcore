---
module: mindcore-sqlite
date: 2026-11-21
problem_type: architecture_pattern
category: architecture-patterns
severity: medium
plan_id: supersede
applies_when:
  - Knowledge or memory records accumulate and must be merged or retired
  - An identifier is referenced by other records that outlive it
  - A default read must hide retired content without breaking provenance
tags:
  - sqlite
  - lifecycle
  - provenance
  - foreign-key
---

# Retire a record with a marker, never a delete

## Problem

Two records eventually say the same thing. The obvious cleanup is to delete or rewrite the obsolete one. Both are wrong when anything outside the table names it: an adoption anchor, a citation, a workflow snapshot. A delete leaves the reference dangling; a rewrite destroys the record of what was adopted, which is the one thing provenance exists to preserve.

The temptation is a "just this once" deletion inside a migration or a repair script. It removes the evidence that the system ever produced the now-invalid state, which is exactly what makes a later inconsistency unexplainable.

## Resolution

A retired record keeps its row and gains a marker naming its successor:

```sql
ALTER-style addition in the single migration:
  knowledge_entries.superseded_by TEXT NULL
  FOREIGN KEY (collection, superseded_by) REFERENCES knowledge_entries(collection, entry_id)
```

- **The marker is data on the retired record**, not a rewrite of it. Its payload, revision and history stay.
- **The marker advances the carrier revision**, so the write goes through the same immediate transaction, the same CAS, and (where a preimage trigger exists) the same history capture as any other change. There is no second write path for lifecycle.
- **The foreign key is scoped to the same collection and does not cascade.** A marker can only name a sibling, and deleting a referenced successor fails.
- **Visibility is a read-side predicate with an explicit override.** A point read and the list both constrain their query with `superseded_by IS NULL`, so a retired row answers the generic absence and never reaches the decoder. The list's bound rides on that same predicate, so it counts **live** rows: retiring an entry frees list budget instead of consuming it, while a live set over the limit still refuses. One explicit read returns the row with its marker so a caller can follow the chain. The served surface keeps one generic absence — retired is not a disclosure state.
- **Resolution follows the chain to the first live entry.** A cycle is `InternalError`: it is corrupt stored state, not a caller error and not an empty result.

Ordering that a review will find if you get it wrong: read the **stored revision column**, not the revision inside the payload, and refuse a disagreement between them before writing. A drifted row otherwise satisfies the CAS, advances nothing, and silently skips the preimage — the marker appears with no history of the state it replaced.

## Fails-first evidence that discriminates

- Mark a row, then read it: default read and list omit it, explicit read returns payload plus marker, and the row still sits in the table.
- Point an adoption anchor at a marked row: it resolves to the successor rather than reporting a bare absence.
- Seed a two-hop chain: resolution lands on the live end; seed a cycle: `InternalError`.
- Delete a referenced successor: the foreign key refuses.
- Seed a row whose payload revision disagrees with the column: the CAS refuses with `InternalError` instead of accepting the caller's stale expectation.

## Pitfalls

- **Hiding must happen before decoding.** If the point read decodes the payload and rejects afterwards, a marked row with corrupt JSON reports a storage error instead of the generic absence — an existence oracle over a row that is supposed to be invisible.
- **A derived view can resurrect the retired content.** A "current judgment" lookup that joins the carrier without the live predicate keeps serving the head of a retired entry. Audit every lookup that names the entry, not just the ones the plan mentions.
- **Refusal classification must be structured.** When one surface must distinguish "the slot is occupied by a retired record" from "my payload is invalid", pass a structured detail on the refusal and match on code plus detail. Matching a message string is a bug waiting for a translation.
