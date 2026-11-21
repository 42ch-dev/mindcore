---
module: mindcore-sqlite
date: 2026-11-21
problem_type: architecture_pattern
category: architecture-patterns
severity: medium
plan_id: revision-history
applies_when:
  - A write path must retain the state it replaces
  - A cost contract pins the statement count of that write path
  - SQLite holds the table and the migration is the only schema source
tags:
  - sqlite
  - trigger
  - history
  - cost-contract
---

# Write a preimage with a trigger when a cost contract pins the write path

## Problem

An update must keep the row it replaces. The obvious implementation adds one `INSERT` to the Rust write path — and that breaks a contract that already exists: `cognition_cost` asserts the append path writes a fixed number of statements, so a new literal `INSERT` changes the measured cost and the assertion fails, or someone relaxes the threshold, which removes the guarantee that made the assertion worth having.

The temptation is a second statement guarded by a flag, or a background task, or accepting "history is best effort". Each one trades a real guarantee for a cosmetic number.

## Resolution

Put the preimage write in the schema, not in the caller:

```sql
CREATE TABLE knowledge_entry_revisions (
  collection TEXT NOT NULL,
  entry_id   TEXT NOT NULL,
  revision   INTEGER NOT NULL,
  payload    TEXT NOT NULL,
  cognition_head_seq INTEGER,
  recorded_at TEXT NOT NULL,
  PRIMARY KEY (collection, entry_id, revision),
  FOREIGN KEY (collection, entry_id) REFERENCES knowledge_entries(collection, entry_id)
);

CREATE TRIGGER knowledge_entries_capture_revision
BEFORE UPDATE OF revision ON knowledge_entries
WHEN NEW.revision <> OLD.revision
  AND <the dedicated-append case is excluded here>
BEGIN
  INSERT INTO knowledge_entry_revisions(...)
  VALUES (OLD.collection, OLD.entry_id, OLD.revision, OLD.payload,
          OLD.cognition_head_seq, OLD.updated_at);
END;
```

- **`OLD` is the preimage and carries the timestamp that state was written with.** Recording `OLD.updated_at` beats sampling the clock again: the history row says when that version lived, not when it was archived.
- **The composite primary key makes the write idempotent by construction.** One row per assigned revision; no duplicate and no rewrite.
- **The foreign key is deliberately non-cascading.** A parent cannot be deleted while its history exists, which is exactly the invariant a "keep history" feature is for.
- **The Rust write path is untouched**, so the cost contract still measures the same statements.

## The two traps that cost a review round each

1. **The skip predicate is part of the contract.** A dedicated append that only advances the carrier must not also snapshot, or the cost assertion sees the extra write. The predicate must name that case precisely — and because it is now load-bearing for a cost guarantee, a comment is owed: the trigger is where the invalidation is conserved, not the Rust source.
2. **Proving a replay gap by row count is not proving it by position.** A first attempt checked "the number of classified rows in the gap equals the gap size". SQLite's non-`STRICT` columns accept a `REAL` in an `INTEGER` position, so a row at `append_seq = 1.5` is counted and masks a missing `2`. The correct check reads the positions and requires them to be exactly the consecutive integers in the range. Any count-based invariant over a non-strict column is a latent acceptance bug.

## Fails-first evidence that discriminates

- Seed an `append_seq = 1.5` row plus a row at `3` and no row at `2`; a count-based check accepts the store, a positional check refuses it with `InternalError`.
- Seed a revision and delete its parent; the non-cascading foreign key refuses and the preimage survives.
- Assert the cost test's statement and step counts **before** the change and after: the trigger must not move them.

## Pitfalls

- A trigger is invisible to a reader of the Rust write path. Every schema page listing the readers must name the trigger, or the next maintainer adds a second history mechanism beside it.
- `WHEN NEW.revision <> OLD.revision` is what keeps a same-revision bookkeeping update from writing a duplicate-key row; without it the trigger turns a harmless update into a constraint failure.
- The trigger fires inside the caller's transaction, so it inherits the caller's rollback: a failed write leaves neither the new row nor the preimage.
