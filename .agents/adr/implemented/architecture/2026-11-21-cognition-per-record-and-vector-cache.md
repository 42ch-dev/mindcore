# ADR: Per-record cognition history and an identity-keyed vector cache

Status: implemented — whole-carrier rewrites and per-request re-embedding both scaled with history, and neither problem needed a new index, service or wire type
Date: 2026-11-21

## Problem

Cognition records lived inline in `KnowledgeEntry.modules.cognition.records`. Every append cloned the array, pushed, and re-encoded the entire module inside the caller's write transaction, so both CPU and lock hold time grew with history length, and every reader of the current judgment had to hydrate the whole array. Separately, `recall_context` rebuilt an embedding batch for the whole page on every call, because no vector was persisted: a host that supplied an `Embedder` paid provider latency proportional to page size on every recall.

## Decision

Cognition records are stored per row. `cognition_records` keys `(collection, entry_id, append_seq)` with one validated record JSON per row and `(collection, entry_id, record_id)` uniqueness; `knowledge_entries.cognition_head_seq` is the single current-judgment pointer, with a composite foreign key back to that row. An append reads only the carrier header, compares the expected carrier revision, validates the record (holder, view, integer `order`), inserts one row and advances head plus outer revision in the caller's transaction. The state stays one row and one revision per holder; a full DTO read reconstructs `modules.cognition.records` from one snapshot in `append_seq` order, so the projection is unchanged. The raw row insert is crate-private: the only commit surfaces are the guarded append and the full-entry put.

Vectors are cached per record and identity. `identity_hash` is the JCS SHA-256 of `{provider_id, model_id, model_version, dim}`; rows are keyed `(record_id, identity_hash)` with the record revision, dimension and little-endian `f32` bytes. A hit requires the exact identity, the current authorized revision and the dimension, so a text change is a miss rather than a stale approximation. `recall_context` reuses the page's cached vectors, keeps one validated query vector per workflow instance keyed by (principal, space, subject, exact trimmed text, identity), and embeds only missing or stale page texts. Release and cache fill are one binding boundary: when the page has fills, the transaction takes `BEGIN IMMEDIATE` before reauthorization, so no other connection can commit between the reauthorization read and the fill write; a refusal or a moved set aborts release. A post-settlement warm binds to the revision the settlement actually committed and is best-effort: a failure leaves a miss, never a rolled-back or retried Memory effect. Without an embedder or identity the vector table is never read or written.

No data migration for the cognition change: it rewrites `0001`, so a database whose applied `0001` no longer matches its file is refused by the embedded migrator's checksum at open and must be recreated. The vector table originally landed as a separate additive `0002` migration that applied in place to a matching database; that step is history — `0002` was deleted when `0001` became the only migration and took the vector table with it (`2026-11-21-single-migration-store-bootstrap.md:14`).

## Alternatives considered

- **Bound the inline array instead (ring buffer, compaction, truncation).** Rejected: any of them either discards history the projection expects or keeps the whole-array rewrite the change exists to remove.
- **`sqlite-vec` / ANN index.** Rejected: `vec0` is a flat exact index, so it buys a constant factor and no complexity change, while adding a per-platform C artifact to a library that must embed.
- **Caller-supplied vectors instead of a host `Embedder`.** Rejected: it would move the provider contract onto every caller and delete the existing, tested failure classification.
- **A global vector index over all records.** Rejected: scoring is exact over an already authorized page; a global index would change ranking semantics and add an unbounded search surface.
- **Per-revision vector rows.** Rejected: one row per record/identity plus a freshness revision marker bounds growth; keeping every revision multiplies storage for no recall benefit.

## Consequences

- Append work is constant in history: measured at 126 VDBE steps for both 1 and 1000 prior records, and the current-judgment read is a constant 42-step head lookup.
- A warm recall makes zero provider calls; a new query embeds only its query text. The persisted and fresh paths score to the same bits, proven against real SQLite BLOBs.
- A database whose applied `0001` no longer matches its file must be recreated, by design; a host that wants continuity exports and re-imports rather than upgrading in place. The additive `0002` step is history: that file no longer exists, so a fresh open applies the vector table from `0001`.
- Identity rotation is a host operation: quiesce in-flight recalls, then call `MindcoreSurface::retire_embedding_identity`, which clears the retired identity's query slot and deletes its stored partition by `identity_hash` in one statement. The workspace has no production rotation caller yet.
- A fresh production `MindStore::open` installs the protocol schema, cognition and vector tables included, so the runtime serves a freshly opened store rather than a test fixture; `I-000068` is closed (`crates/mindcore-sqlite/src/store.rs:69-99`, `.agents/specs/architecture/serving-seam.md:46`).
