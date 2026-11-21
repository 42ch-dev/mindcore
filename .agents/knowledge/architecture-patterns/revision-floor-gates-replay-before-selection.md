---
module: mindcore-sqlite
date: 2026-11-22
problem_type: architecture_pattern
category: architecture-patterns
severity: medium
plan_id: history-retention
applies_when:
  - A read is answered from the nearest saved checkpoint at or above the requested position
  - Older checkpoints can be deleted, so the requested position may be below everything that remains
  - The same read has a fallback that answers with current state when no checkpoint is recent enough
tags:
  - replay
  - truncation
  - sqlite
  - checkpoint
  - read-path
last_updated: 2026-11-22
---

# A replay floor is a precondition on the read, not a consequence of checkpoint selection

## Problem

Knowledge entries keep one preimage row per replaced revision, and a range read answers revision N by
taking the first saved checkpoint at or above N, then deriving the earlier head by subtracting the
crossed gap. That derivation is a **positional proof**: every crossed append must be the consecutive
integer in the range, because a dedicated append advances the revision without writing a preimage.

Deleting old preimages breaks the assumption. The read no longer reports "that revision is gone"; it
selects a newer checkpoint, computes a larger gap, and fails the positional proof. The caller sees an
`InternalError` — the storage is broken — for a revision we ourselves removed. And a read that reaches
the live-row fallback is worse: it answers a truncated request with current content, which is
indistinguishable from a correct answer at the boundary.

## Root cause

Truncation is being treated as a property of the checkpoint set instead of a property of the entry.
Two distinct questions got merged into one statement:

1. *What is the earliest revision this entry can still answer?* — a stored fact.
2. *Which saved row answers revision N?* — a selection over that fact.

Once they are one query, absence of an old checkpoint is indistinguishable from absence of an old
revision, and the fallback cannot tell a legitimate answer from a fabricated one.

## Pattern

1. **Persist the floor on the entry row.** `history_floor_revision`, `NOT NULL DEFAULT 1`, so every
   existing row reads as "nothing pruned" without back-fill, and the read that fetches the live row
   for its fallback also fetches the floor.
2. **Gate before selecting.** Compare the requested revision to the floor *before* choosing a
   checkpoint. Below the floor: refuse. At or above: run the existing selection, positional proof and
   fallback unchanged, so pruning cannot change the answer for anything the floor still covers.
3. **Make the refusal classifiable.** A truncated revision answers the same reject code as an ordinary
   absence with a structured `details.reason` token, and a message that says the revision was pruned.
   Callers match the code and token, never the message; ordinary absence keeps no details. A caller
   must be able to write a `match` that separates "never existed" from "we removed it".
4. **Prune explicitly, in one transaction, and probe in constant work.** The delete is by
   `revision < floor` under the primary key, the floor update rides the same `BEGIN IMMEDIATE`, and the
   boundary is found with two rows (`LIMIT 2 OFFSET keep - 1`) rather than loading every checkpoint.
   The floor-only update must not fire the revision-capture trigger.

## What did not work

- **Tolerating a gap in the positional proof.** The proof exists because a dedicated append leaves no
  preimage; a missing preimage is therefore indistinguishable from a deleted one. Widening the proof
  to accept gaps would silently weaken replay for every entry, not just the pruned ones.
- **Defensive "deleted zero rows but wrote the floor" handling.** Review asked for it; the case is
  unreachable, because if more checkpoints than `keep` exist then the next-older row is strictly below
  the new floor and the delete always removes at least one row. The right response was a regression
  that prunes, then prunes again with a smaller `keep` — not an unreachable branch.
- **Reading the entry's payload to prove it is live.** Pruning only needs existence; a payload decode
  in the write transaction is work the operation never uses.

## Trade-off

One column on the entry row and one comparison in the hottest read path, in exchange for a read that
degrades from "silently wrong or internally broken" to "explicit refusal" once history is bounded.
The floor also makes the surviving window reportable, which is what a caller needs before deciding
whether pruned history is acceptable at all.

## Verification shape

Snapshot `get_entry_at_revision` for **every** revision at or above the future floor *before* pruning,
prune, read the same revisions, and compare bytes. A test that compares write returns to post-prune
reads passes even when replay is wrong on both sides. Pair it with a regression that prunes and then
re-prunes with a smaller window, so the delete-count contract is pinned rather than assumed.
