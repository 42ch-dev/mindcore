# ADR: Caller-owned transaction settlement seam

- Date: 2026-11-20
- Status: implemented (settle-repo + settle-adapter plans, iteration iter003-settle)
- Class: architecture
- Supersedes: the pool-owning `MindStore::open` write path (v02 provably incompatible with guarded embedding)

## Context

Nexus's writer protocol admits writers at connection/pool level and runs each logical effect inside its own `BEGIN IMMEDIATE` boundary. Mindcore's store originally opened its own pool and committed its own transactions per mutation, so nothing could execute inside the guarded boundary — embedded parity was proven impossible in iteration v02-embed (I-000020). Mindcore rows also did not map onto the Nexus memory schema (I-000021).

## Decision

1. **The caller owns the transaction.** `SqliteMemoryRepository` is a zero-sized value; every method takes `&mut sqlx::Transaction<'_, sqlx::Sqlite>` borrowed from the caller and may only reborrow it internally. The repository never opens, commits, or rolls back a transaction. Admission, migrations, and the effect boundary stay with the caller (Nexus guarded writer or a test harness).
2. **Adopt the shared physical schema; no views, no data migration.** Mindcore memory rows use the Nexus table/column names so parity selectors run unchanged against a caller-migrated database. Documented deviations only: `owner_id`/`lease_expires_at` inlined into the fixture; per-record `extensions` are not persisted (reconstructed empty on read, dropped on write at the codec boundary).
3. **One protocol adapter above the seam.** `mindcore-spoke` owns all wire↔IR conversion, reject mapping, and replay/CAS/grant orchestration, and drives SQLite exclusively through the repository on the caller's transaction. No port logic below the adapter (v02 D3).
4. **CAS updates are predicates, not increments.** Updates take the replacement row plus the observed revision (or observed state + owner, with SQL NULL matching, space-scoped) and persist the replacement's revision verbatim.

## Consequences

- Parity evidence runs unchanged on both sides of the seam (iteration iter003-settle: 47-row matrix, 43 pass / 4 decision-classified).
- Repository APIs thread a transaction handle; convenience pool-owning entry points remain legacy-only, outside the embedded path.
- Per-record `extensions` need a durable home if a future consumer must persist them — a schema change owned by the upstream provider, not a local column (see knowledge doc `borrowed-transaction-sqlite-repository.md`).
- Withheld-read isolation for unsettled journal intents (spec §5.2/D-10) is tracked open as I-000043; the seam does not block it.

## References

- Plan: `.mstar/plans/settle-repo.md`, `.mstar/plans/settle-adapter.md` (iteration iter003-settle)
- Knowledge: `.agents/knowledge/architecture-patterns/borrowed-transaction-sqlite-repository.md`, `.agents/knowledge/architecture-patterns/protocol-adapter-two-phase-pattern.md`
- Spec: `.agents/specs/architecture/storage-ir-boundary.md`
