# ADR: Standalone embedded SQLite store — one file, one pool, one embedded migrator

Status: implemented — `crates/mindcore-sqlite/src/store.rs` opens one local file as the standalone store
Date: 2026-11-20
Class: architecture
Related: `2026-11-20-borrowed-transaction-seam.md`

## Problem

Mindcore needs durable local storage without an external database, a migration tool or a connection manager on the caller's side. Two decisions have to be fixed so that every caller gets the same file semantics: who owns the pool and migrations, and how a competing handle is expected to behave.

The borrowed-transaction seam already answers the opposite case. `SqliteMemoryRepository` is a zero-sized value whose methods take the caller's open transaction; it owns neither a pool nor migrations, because an embedding host (the Nexus guarded writer or a test harness) supplies both. Standalone `MindStore` cannot borrow from a host that does not exist, so it must own that layer itself — without turning the borrowed repository into a second pool-owning path.

## Decision

`MindStore::open(path)` is the standalone storage entry point:

1. **One database file, one single-connection pool.** `open` creates the file when absent and opens a `SqlitePool` capped at `max_connections(1)`. All store operations on that file go through the one connection, so a `BEGIN IMMEDIATE` reservation is never contended by a second connection of the same handle.
2. **WAL, foreign keys and a five-second busy timeout.** The connection uses `journal_mode=WAL`, `foreign_keys=ON` and `busy_timeout(5s)`. WAL lets readers proceed while a writer holds its reservation; the timeout is a bounded wait for another handle's lock, not a promise that the competing operation always waits out its window or succeeds.
3. **The store owns the embedded migrator.** `sqlx::migrate!("./migrations")` runs at `open`, applying any pending migration to that file. Migrations ship inside the crate; no caller step is required.
4. **The standalone path stays separate from the borrowed seam.** `SqliteMemoryRepository` continues to borrow a caller's transaction and owns neither pool nor migrations (borrowed-transaction ADR, iter003-settle). `MindStore` is not a wrapper around it, and the repository is never opened through `MindStore::open`.
5. **One store per file is the caller's obligation.** `open` does not detect a second `MindStore` on the same file; two stores on one file are unsupported rather than refused. Two handles coordinate only through SQLite's file locks and the busy timeout.

## Alternatives considered

- **Only the borrowed-transaction repository, with every caller opening its own pool and running migrations.** Rejected: standalone consumers (examples, tests, a local single-process deployment) would each re-implement the same connect options and migrator invocation, and any divergence would surface as different file semantics. The seam remains available for hosts that already own a transaction.
- **A multi-connection pool.** Rejected: the store's write model depends on one logical writer per handle and on `BEGIN IMMEDIATE` ordering; extra connections would create self-contention and expose lock failures as generic storage errors without adding throughput for a single-process local store.
- **Rollback journal instead of WAL.** Rejected: readers would block on a writer's reservation, so recall and knowledge reads would contend with capture and retain on the same file. WAL's snapshot reads fit the read-heavy access pattern.
- **No busy timeout, failing fast on lock contention.** Rejected: a competing handle would surface `SQLITE_BUSY` immediately as an internal error instead of first observing the committed winner, which is exactly the failure mode the write-reservation rule exists to avoid. A zero timeout was kept only in tests that assert the raw failure.
- **Enforce one store per file inside `open`.** Rejected: process-level locking would add a second coordination mechanism that does not cover every deployment shape, and cross-process locks still end in the same SQLite contention. The single-store rule is documented instead.
- **An external migration runner.** Rejected: it adds an operational step and a version-skew surface for a crate that already ships its schema; the embedded migrator keeps the file usable after a plain `open`.

## Consequences

- **Lock contention still fails.** The five-second busy timeout bounds how long a second handle waits; a competing operation can still surface `SQLITE_BUSY` after that bound. The timeout is not a delivery guarantee.
- **Applied migrations are never edited.** The embedded migrator records each applied migration and its checksum in `_sqlx_migrations`; editing an applied migration makes an existing file fail the next `open`. Every schema change goes in a new migration file (sqlx checksum semantics).
- **WAL leaves siblings.** Each open file carries `-wal` and `-shm` files next to it; fixtures and tests must remove them to start from an empty store.
- **Two stores on one file are unsupported.** Neither `open` nor the store detects this; correctness under that configuration rests on the caller.
- **Standalone and embedded stores are distinct contracts.** A future capability that must run inside an embedding host's transaction belongs on `SqliteMemoryRepository`, not on `MindStore`; adding an autocommit or pool-owning overload to the repository would break the seam.
- **Reopen behaviour is part of the contract.** `open` on an existing file must leave it usable with its existing rows and applied migrations; changing connect options is a change to this record.

## References

- Plan: `.mstar/plans/hardening.md` (iteration iter004-harden)
- ADR: `.agents/adr/implemented/architecture/2026-11-20-borrowed-transaction-seam.md`
- Spec: `.agents/specs/architecture/mind-model.md`; wire-fidelity contract: `.agents/specs/architecture/storage-ir-boundary.md`
- Knowledge: `.agents/knowledge/architecture-patterns/sqlite-write-reservation-for-claims.md`, `.agents/knowledge/architecture-patterns/borrowed-transaction-sqlite-repository.md`
