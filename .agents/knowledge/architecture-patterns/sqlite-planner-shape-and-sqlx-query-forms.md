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
  - A streamed read feeds a decision that compares a stored timestamp with "now"
last_updated: 2026-11-26
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
Loading every grant of the principal into memory makes peak memory follow the whole grant
history, and querying once per visible space makes the revision read cost one statement per
visible space. The shape below removes both without making the listing read only what the
caller can see: the scan still walks the whole grant history and the listing still fetches
every stored space.

## Guidance

1. **One streaming scan, then one projection over its result.** `scan_principal_grants`
   decodes the principal's grants one row at a time through a `fetch` stream and hands each
   row to a visitor, so peak memory on the grant side is a row plus the delivery buffer and
   the set of `space_id`s the visitor keeps from live grants. The scan still walks the
   principal's whole grant history, expired and revoked rows included; what it avoids is
   holding that history. `principal_grant_revisions` then takes the visible set back as a
   JSON array and returns the `(grant_id, revision)` pairs of the visible spaces only.
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
6. **Read the clock when the row arrives, not before the query.** A streaming read that decides
   liveness compares each row's expiry with the current instant at the moment the row is decoded.
   Taking one instant before `fetch` makes the decision stale for every row that arrives after the
   single-connection pool makes the query wait: a grant that expires during the wait is still
   admitted. A scan's cost is not only memory; its duration is part of the correctness of any
   decision it feeds.

## Why This Matters

The planner's shape decides what a read's cost grows with, and the reads here are bounded in
different ways. The visible set bounds the revision projection only, and it bounds that
statement's output — the `(grant_id, revision)` pairs of the visible spaces — not the walk
over the principal's grant rows that produces them. The scan decodes the principal's whole
grant history one row at a time, and `list_spaces` materializes every stored space before
the caller filters, so the listing's memory is not bounded by the visible set either. What
the scan bounds is its own side: one decoded grant row at a time and the visible-id set,
instead of the whole grant history held as rows. The projection is then materialized in
full — one pair for every grant of the principal in the visible spaces, expired and revoked
grants included — so the revision materialization grows with the grant history of the
visible spaces, and a single visible space can retain many pairs. The `EXISTS` and
`+space_id` spellings keep the index probes proportional to one principal's rows rather than
to every stored space. None of this shows in the SQL text's result; it shows only in the
plan, so a "simpler" rewrite of either predicate passes every assertion and still changes
the cost.

## When to Apply

- A listing derives a visible set from one indexed column and then needs a projection over
  that set.
- A child table may hold rows whose parent is gone and no reader may ever surface them.
- A statement is asserted by statement count or by VDBE steps, so its form must stay stable.

## Examples

The two statements differ only in the unary `+` and select the same rows: the revisions of
one principal's grants in the caller's visible spaces.

### Before

With `space_id` written as is, SQLite probes the grant index once per visible space.

```sql
SELECT grant_id, revision FROM memory_grants
 WHERE principal_id = ? AND space_id IN (SELECT value FROM json_each(?));
```

### After

The unary `+` takes `space_id` off the index, so SQLite reads the grant index once over the
principal and tests the visible set there.

```sql
SELECT grant_id, revision FROM memory_grants
 WHERE principal_id = ? AND +space_id IN (SELECT value FROM json_each(?));
```

### A single undecodable column aborts the whole row stream

`query_as!` compiles to `sqlx::query(...).try_map(...)`, and the `?` inside that mapper ends the
stream. One row whose column cannot be decoded (bad UTF-8, malformed JSON, an unparseable
timestamp) therefore hides every row sorted after it — so a "skip the bad row and keep going" rule
written outside the mapper never runs. The same shape appears in a visibility read that projects a
whole page.

### After

When a read must tolerate a bad row, do not use `query_as!`. Select with `sqlx::query` and decode
each `SqliteRow` yourself in a loop that records the failure and continues:

```rust
let mut rows = sqlx::query("SELECT grant_id, capability, expires_at FROM memory_grants WHERE space_id = ?")
    .bind(space_id).fetch(tx.as_mut());
let mut out = Vec::new();
while let Some(row) = rows.try_next().await? {          // stream survives the bad row
    match grant_of(&row) { Ok(g) => out.push(g), Err(_) => continue }   // row-local failure
}
```

The rule this encodes: decide whether a row is decodable **at the row**, and let the loop decide
what a failure means (skip it for a visibility read, refuse for a single-row point read). A
`try_map` mapper cannot express that choice, because its failure is the stream's failure.
