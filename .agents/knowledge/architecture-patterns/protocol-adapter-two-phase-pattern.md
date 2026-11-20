---
module: mindcore-spoke
date: 2026-11-20
problem_type: architecture_pattern
category: architecture-patterns
severity: medium
plan_id: settle-adapter
applies_when:
  - A protocol adapter fronts a borrowed-transaction repository for a pinned wire contract
  - Effects span two caller transactions (stage, then settle after an external publication)
  - Authorization has participation-shaped rules (receipts visible to requester or admin) over revocable, expiring grants
tags:
  - adapter
  - two-phase
  - idempotency
  - authorization
---

# Protocol adapters over borrowed-transaction repositories: stage in tx 1, settle in tx 2, never decide inside

## Problem

A wire-protocol adapter must map pinned operations onto a repository that runs inside a caller-owned transaction, while some effects (journal file publication, model-driven reflection) cannot complete inside one transaction. Naive designs commit inside the adapter, retry across the boundary, or let a staging failure masquerade as a receipt.

## Pattern (as implemented in `settle-adapter`)

1. **Zero-state adapter, transaction-explicit entry points.** `MemoryAdapter` holds the repository by value; every operation takes `&mut Transaction` plus the caller-authenticated principal. The adapter classifies, it never owns, commits, or retries.
2. **Two-phase effects with a local staging value.** Operations that need publication return `JournalPending { space, receipt_id }` / `ReflectionPending(ReflectionWork)` — local values, not wire data. The caller commits tx 1, publishes, then calls `settle_journal`/`complete_reflection` with a FRESH transaction. Settlement is refused-classified as `OUTCOME_UNCERTAIN/uncertain` until publication is confirmed; the caller cannot return committed success before settlement.
3. **Idempotency digests bind the ORIGINAL request.** The digest is the canonical form of the raw input `Value`, not of the validated DTO — DTO normalization would let different payloads replay one receipt. Same digest replays the stored receipt; different digest on the same key is `IDEMPOTENCY_CONFLICT`.
4. **Replay re-checks coverage; participation is predicate-exact.** A stored receipt is returned only after the current grant set re-authorizes the original operation. "Live grant" means not-revoked AND not-expired, with expiry parsed as an RFC-3339 instant (fail-closed on unparsable). Poll visibility (requester or admin) and replay authorization are separate checks; no participation yields the same refusal for known and unknown ids (no existence oracle).
5. **Rejections are a closed vocabulary with retry classes.** Every failure maps to a pinned reject code + `retry_class`; storage failures map `InvalidRow`/`Conflict`/`Sql` → honest classes, and phase-2 storage failures are `OUTCOME_UNCERTAIN` (unsettled effect), never retryable `INTERNAL_ERROR` (that class means "no commit was possible").
6. **Replayed committed responses re-validate against the pinned response type.** A tampered or schema-incompatible stored result must not escape as wire success.

## Traps

- Validating a DTO then serializing IT for storage rewrites member presence; keep the original `Value` subtrees at the record boundary.
- Expiry compared as a string silently admits timezone-offset grants.
- Completeness checks on profiles/documents must inspect content, not just existence + revision.
- Guarded inserts on absent rows must honor a caller-supplied expected revision before writing.
- A test whose injected failure fires before staging begins proves nothing about rollback of partial writes — fail mid-staging.
