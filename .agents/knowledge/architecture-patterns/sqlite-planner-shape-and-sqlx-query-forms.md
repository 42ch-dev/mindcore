---
module: mindcore-sqlite
date: 2026-11-23
problem_type: architecture_pattern
category: architecture-patterns
severity: low
applies_when:
  - A served listing decides visibility from one principal's grants before it pages
  - A query filters rows by membership in a set the caller already holds in memory
  - A lookup is measured by statement count or by SQLite virtual-machine steps
tags:
  - sqlite
  - query-plan
  - sqlx
  - streaming
---

# Shape a grant read as one streaming scan plus one set projection, and pick the sqlx form that owns a cache entry

## Context

`memory.spaces.list` needs two things before it pages: the set of spaces a principal
can see, and the grant revisions of exactly those spaces for the cursor's snapshot marker.
The obvious reads — load every grant of the principal into memory, or query once per
visible space — make the cost follow the principal's whole grant history or the number of
stored spaces, neither of which is what the caller asked for.

## Guidance

1. **One streaming scan, then one projection over its result.** `scan_principal_grants`
   decodes the principal's grants one row at a time through a `fetch` stream and hands each
   row to a visitor, so peak memory is a row plus the delivery buffer. The caller keeps
   only the `space_id` of each live grant, then `principal_grant_revisions` takes that set
   back as a JSON array and returns the `(grant_id, revision)` pairs of the visible spaces
   only. Peak memory follows the visible grants, not the whole history.
2. **Filter orphans with `EXISTS`, not `IN`.**
   `WHERE principal_id = ? AND EXISTS (SELECT 1 FROM memory_spaces WHERE
   memory_spaces.space_id = memory_grants.space_id)` makes SQLite read the grant index once
   over the principal and probe one space row per grant. The `IN (SELECT space_id FROM
   memory_spaces)` spelling probes the index once per stored space. The filter also keeps an
   orphan grant — no space row, unconstrained payload — out of the decode, since no listing
   can show it.
3. **Take a column off the index with a unary `+`.** In
   `WHERE principal_id = ? AND +space_id IN (SELECT value FROM json_each(?))`, the `+`
   prevents SQLite from using an index on `space_id`, so it reads the index once over the
   principal and tests the visible set there instead of probing the index once per visible
   space.
4. **Match the sqlx form to the query cache.** `sqlx::query_as!` is compile-checked against
   the offline query cache (or a live `DATABASE_URL`). A lookup that owns no offline cache
   entry — `journal_by_receipt` is one — uses the runtime form
   `sqlx::query_as::<_, JournalRow>("…")`, which decodes through `FromRow` at run time.
5. **sqlx's progress callback is inverted from SQLite's.** SQLite interrupts a statement
   when its progress handler returns non-zero; sqlx's `set_progress_handler(n, closure)`
   continues while the closure returns `true`. A step counter therefore returns `true`, or
   the first measured statement is interrupted.

## Why This Matters

The planner's shape decides what a read's cost grows with. The scan-then-project pair makes
it grow with what the caller can see; the `EXISTS` and `+space_id` spellings keep the index
probes proportional to one principal's rows rather than to every stored space. Both are
invisible in the SQL text's result and show only in the plan, so a "simpler" rewrite of
either predicate passes every assertion and still changes the cost.

## When to Apply

- A listing derives a visible set from one indexed column and then needs a projection over
  that set.
- A child table may hold rows whose parent is gone and no reader may ever surface them.
- A statement is asserted by statement count or by VDBE steps, so its form must stay stable.

## Examples

### Before

```sql
-- One index probe per stored space, and one index probe per visible space.
SELECT grant_id, revision FROM memory_grants
 WHERE principal_id = ? AND space_id IN (SELECT space_id FROM memory_spaces);
```

### After

```sql
-- One index read over the principal; the visible set is tested there.
SELECT grant_id, revision FROM memory_grants
 WHERE principal_id = ? AND +space_id IN (SELECT value FROM json_each(?));
```
