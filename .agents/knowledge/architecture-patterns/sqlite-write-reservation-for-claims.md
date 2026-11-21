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

## Proving contention, not just failure

A race test that accepts "the loser returned a retryable error" proves nothing about contention: every pre-commit driver failure — a busy lock, a lost snapshot, an I/O error — produces the same shape. The test passes whether or not the intended branch ran.

Three ways to make the assertion name the branch, in order of strength:

1. **A test-only interleave seam** that lets the loser observe claim state the winner then changes, so it reaches the in-transaction `rows_affected == 0` predicate deterministically. Strongest, but it costs production plumbing: an interleave hook has to reach the repository through whatever adapter sits above it.
2. **A pinned reject detail.** If the contended path has a distinct message or class, assert the exact text alongside the code and the retry class. A generic failure cannot satisfy it. This is what `concurrent_settlement` does: the loser must carry `a concurrent writer refused this transaction before commit`.
3. **A written justification of unreachability.** When the branch genuinely cannot run (SQLite refuses the loser at the write lock before the predicate executes), state that in the module and assert the sequential case explicitly. Do not leave the branch to a race test that never reaches it and never says so.

Whichever you choose, the failure mode to avoid is the same: an assertion whose condition is satisfied by an unrelated error class, so a regression into the generic fallback passes silently.

## Pitfalls

- `max_connections(1)` serializes one pool, not all handles to the same file.
- A strict RFC 3339 parser rejects expanded years (`+10000-…`) that the writer can emit; validate readability by round-tripping the stored text before insert, or a committed row can become unreadable and unrecoverable.
- Variable-width fractional seconds break lexicographic ordering of `TEXT` timestamps; format with a fixed nine-digit fraction.
