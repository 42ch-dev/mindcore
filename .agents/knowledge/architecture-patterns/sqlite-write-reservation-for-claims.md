---
module: mindcore-sqlite
date: 2026-11-19
problem_type: architecture_pattern
category: architecture-patterns
severity: medium
plan_id: v01-store
applies_when:
  - A SQLite-backed store implements conditional claims or optimistic-concurrency writes
  - Several store handles in one process may write the same row
  - An operation must be idempotent under concurrent retries
tags:
  - sqlite
  - concurrency
  - idempotency
  - occ
---

# Reserve the SQLite write lock before reading claim state

## Problem

Two `MindStore` handles opened on the same file each own a one-connection pool. A deferred transaction (`pool.begin()`) reads "no receipt, candidate unreviewed", then the other handle commits. The second transaction tries to upgrade its read snapshot and receives `SQLITE_BUSY` / `SQLITE_BUSY_SNAPSHOT`, which surfaces as a generic storage error instead of the operation's contractual outcome (replay returns the original record; a second request is refused).

## Resolution

- Start the mutating transaction with `BEGIN IMMEDIATE` (`pool.begin_with("BEGIN IMMEDIATE")` in SQLx), so the write reservation is held **before** any read of the claim state.
- Set a bounded `busy_timeout`; the loser waits and then observes the winner's committed rows instead of failing on lock upgrade.
- Keep the reads, the comparison and the writes in that one transaction, and let every error path roll back and release the connection.
- For an optimistic-concurrency port, compare the expected revision inside the same immediate transaction, then persist `stored + 1` into the column **and** the payload, leaving all other payload fields byte-identical.

## Fails-first evidence that discriminates

Holding `BEGIN IMMEDIATE` on a second connection and polling the write with a timeout is not a proof: a stalled worker can pass the test with a deferred transaction. The observed discriminator is the failure mode — with `pool.begin()` the write died in `~10 ms` with `database is locked (code: 5)` mapped to an internal error, while the immediate variant stayed blocked for the whole reservation window. Prefer a structural guard (a reservation-gated read path) over elapsed-time inference; if only a timing test is available, track the weakness as a residual.

## Pitfalls

- `max_connections(1)` serializes one pool, not all handles to the same file.
- A strict RFC 3339 parser rejects expanded years (`+10000-…`) that the writer can emit; validate readability by round-tripping the stored text before insert, or a committed row can become unreadable and unrecoverable.
- Variable-width fractional seconds break lexicographic ordering of `TEXT` timestamps; format with a fixed nine-digit fraction.
