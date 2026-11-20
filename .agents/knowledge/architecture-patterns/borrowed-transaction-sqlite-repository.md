---
module: mindcore-sqlite
date: 2026-11-20
problem_type: architecture_pattern
category: architecture-patterns
severity: medium
plan_id: settle-repo
applies_when:
  - A repository must run inside a caller-owned transaction guarded by another subsystem's writer protocol
  - The same physical schema is shared with an upstream provider (Nexus) while the local store keeps its own IR shapes
  - Persisted rows must round-trip through a wire codec without gaining schema columns
tags:
  - sqlite
  - borrowed-transaction
  - schema-mapping
  - codec
---

# Borrow the caller's transaction; adopt the shared physical schema; keep extensions out of it

## Problem

`MindStore::open` owned its pool and committed its own transactions, so nothing could run inside Nexus's guarded `BEGIN IMMEDIATE` effect boundary — embedded parity was provably impossible (v02 `I-000020`). Mindcore's rows also did not map onto the Nexus memory schema (`I-000021`), and the codec needed a story for data the shared schema does not persist (per-record `extensions`).

## Pattern (as implemented in `settle-repo`)

1. **Zero-sized repository, borrowed handle.** `SqliteMemoryRepository` holds no state; every method takes `&mut sqlx::Transaction<'_, Sqlite>` from the caller and may only reborrow (`&mut **tx`) for statement helpers. No pool, no `begin/commit/rollback`, no lifetime-bound wrapper. The caller alone owns admission, migrations, commit and rollback — the repository is inert outside a caller-held transaction.
2. **Adopt the shared physical schema, no views, no data migration.** Mapping (not translating) onto Nexus's table/column names makes guarded parity selectors run unmodified against the real caller-migrated database. Record the collisions and deviations explicitly; inline ALTER-added columns in the DDL fixture rather than replaying migrations.
3. **Columns carry wire-shaped JSON.** Row-to-IR codecs reuse the wire codec shapes, so a column stores what the wire member carries; rows that fail to decode are `InvalidRow`, never panics (`Row::try_get` everywhere), never silently widened.
4. **What the shared schema cannot persist, reconstruct at the codec boundary.** Per-record `extensions` are not stored: write-side drops non-empty maps (read-side rebuilds empty). The wire-IR round-trip stays faithful because the codec treats extensions as transient. Do NOT add columns the upstream DDL lacks — SQLx prepares green against a local fixture and then fails at runtime on the real database.
5. **CAS updates are predicates, not increments.** Updates take the replacement row plus the observed revision (or observed state + owner, with `owner_id IS ?` NULL handling, plus `space_id` in every predicate) and store the replacement's revision verbatim. Increment-on-write silently rewrites the caller's intended version.

## Why it works

The guarded writer keeps its effect boundary; the repository stays a pure row-language over borrowed state; parity selectors execute unchanged on both sides of the seam. Codec-only concerns (extensions) never leak into DDL, so schema evolution stays owned by the upstream provider.

## Traps

- A local fixture that adds "convenient" columns guarantees prepare-pass/runtime-fail divergence.
- Update predicates missing space scoping let a forged row cross domains under a matching revision.
- Persisted selection/mode strings must parse strictly (`InvalidRow`), never default to permissive empties — a dropped context entry can pass a restricted coverage check.
