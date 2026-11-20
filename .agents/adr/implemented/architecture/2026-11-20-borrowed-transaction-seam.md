# ADR: Caller-owned transaction settlement seam

Status: implemented — `SqliteMemoryRepository` borrows the caller's transaction, so adapter work runs inside the embedding host's guarded writer boundary
Date: 2026-11-20
Class: architecture
Supersedes: the pool-owning `MindStore::open` write path (v02 provably incompatible with guarded embedding)

## Problem

Nexus's writer protocol admits writers at connection/pool level and runs each logical effect inside its own `BEGIN IMMEDIATE` boundary. A store that opens its own pool and commits its own transactions per mutation can never execute inside that boundary, so embedded parity was proven impossible in iteration v02-embed (I-000020). Mindcore rows that do not map onto the Nexus memory schema make the parity selectors unportable in the same way (I-000021).

The seam is what breaks again if it is re-opened. Any repository method that opens, commits or rolls back a transaction — or a pool-owning convenience path reused on the embedded route — silently restores the pool ownership this decision removes, and the parity evidence stops proving anything about the embedding host.

## Decision

1. **The caller owns the transaction.** `SqliteMemoryRepository` is a zero-sized value; every method takes `&mut sqlx::Transaction<'_, sqlx::Sqlite>` borrowed from the caller and may only reborrow it internally. The repository never opens, commits, or rolls back a transaction. Admission, migrations, and the effect boundary stay with the caller (Nexus guarded writer or a test harness).
2. **Adopt the shared physical schema; no views, no data migration.** Mindcore memory rows use the Nexus table/column names so parity selectors run unchanged against a caller-migrated database. Documented deviations only: `owner_id`/`lease_expires_at` inlined into the fixture; per-record `extensions` are not persisted (reconstructed empty on read, dropped on write at the codec boundary).
3. **One protocol adapter above the seam.** `mindcore-spoke` owns all wire↔IR conversion, reject mapping, and replay/CAS/grant orchestration, and drives SQLite exclusively through the repository on the caller's transaction. No port logic below the adapter (v02 D3).
4. **CAS updates are predicates, not increments.** Updates take the replacement row plus the observed revision (or observed state + owner, with SQL NULL matching, space-scoped) and persist the replacement's revision verbatim. The predicate and the version-write policy are two independent choices: `WHERE revision = observed_revision` decides whether the write may commit at all, while the `SET` clause decides what revision the committed row carries.

## Alternatives considered

- **Keep the pool-owning store and expose an autocommit write path for embedding.** Rejected: a pooled connection cannot join the host's `BEGIN IMMEDIATE`, so admission and the effect boundary would stop belonging to the caller — the exact v02 failure (I-000020). Pool-owning convenience entry points remain legacy-only, outside the embedded path.
- **Put port logic below the seam, one implementation per storage backend.** Rejected: wire↔IR conversion, reject mapping and replay/CAS/grant orchestration would be duplicated per backend and drift apart. One adapter above the seam keeps a single place where a wire-level refusal is decided.
- **Keep a Mindcore-native schema and emulate the provider's selectors with views.** Rejected: a view layer would have to reproduce the provider's table and column semantics and would hide the storage differences parity exists to expose. The shared physical schema lets the selectors run unchanged, and nothing was published, so no data migration was owed.
- **CAS updates as SQL increments (`revision = revision + 1`).** Rejected for its version-write policy, not for a missing predicate: an increment could still be guarded by `WHERE revision = observed_revision` and would then reject a stale write just as well. What it cannot do is persist the caller's replacement revision verbatim, because it computes the new revision itself instead of writing the one the caller supplied.

## Consequences

- Parity evidence runs unchanged on both sides of the seam (iteration iter003-settle: 47-row matrix, 43 pass / 4 decision-classified).
- Repository APIs thread a transaction handle; convenience pool-owning entry points remain legacy-only, outside the embedded path.
- Per-record `extensions` need a durable home if a future consumer must persist them — a schema change owned by the upstream provider, not a local column (see knowledge doc `borrowed-transaction-sqlite-repository.md`).
- Withheld-read isolation for unsettled journal intents (spec §5.2/D-10) is tracked open as I-000043; the seam does not block it.

## References

- Plan: `.mstar/plans/settle-repo.md`, `.mstar/plans/settle-adapter.md` (iteration iter003-settle)
- Knowledge: `.agents/knowledge/architecture-patterns/borrowed-transaction-sqlite-repository.md`, `.agents/knowledge/architecture-patterns/protocol-adapter-two-phase-pattern.md`
- Spec: `.agents/specs/architecture/storage-ir-boundary.md`
