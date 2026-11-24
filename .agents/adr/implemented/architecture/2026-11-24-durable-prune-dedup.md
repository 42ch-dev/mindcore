# ADR: A prune's dedup record lives in the store's own database

Status: implemented — a same-key retry no longer re-executes a committed prune after a crash or a reassembly
Date: 2026-11-24

## Problem

`history.revisions.prune` is destructive and idempotent by key. Connect dedups it through `HistoryDedupStore`, keyed by `(op, effective scope, requester principal id, idempotency_key)` and holding the request digest plus the settled outcome, and `HistoryPortService::new` installed the released in-memory default. That store belongs to one service instance, and `MindcoreServe::surface` builds a new service per assembled surface, so a reassembly — or any restart — started with no record.

A same-key retry that finds no record executes the prune again against the current state. If new checkpoints were written between the two calls, the retry deletes checkpoints the original call never saw. The delete is permanent and the caller sees a successful prune, so nothing surfaces the loss.

Closing the window has a shape the trait already offers: `with_dedup_store` takes a durable store.

## Decision

The history face composes `SqliteHistoryDedupStore` from `crates/mindcore-runtime/src/history_dedup.rs` through `HistoryPortService::with_dedup_store`, over `self.store.pool()` — the same file and the same single-connection pool the knowledge store serves. The record is a table in the store's own schema, `history_dedup`, added to `0001_init.sql` rather than to a second migration file.

`lookup` **claims** the key before the port runs, by inserting the row with `payload_digest` and `outcome_json` both `NULL`; `record` then upserts both in one statement. The claim is what covers the interval Connect does not: it records only after the port returns, while the prune commits inside the port, so a crash in between leaves a committed prune with no outcome. A row that holds a claim and no outcome answers `Evicted`, which Connect turns into `RECEIPT_EXPIRED` without calling the port.

Faults fail closed. Any statement fault in `lookup`, and any stored outcome the private codec cannot decode, answer `Evicted`: re-executing a destructive prune is the outcome this store exists to prevent. A failed `record` leaves the claim in place and reports the sqlx diagnostic on the host's stderr, following the `adoption_fault` convention; no log facade is added.

The codec is hand-written because `SpokeResult`, `SpokeReject` and `SpokeRejectCode` carry no serde implementation: `{"ok": <value>}` or `{"reject": {"code", "message", "details"}}`, the code as its `as_str()` wire string and read back through `try_from_str`. Decoding is strict — exactly one variant key and exactly the three reject members — so a stored row that does not carry the shape this writer emits answers `Evicted` instead of replaying a half-read outcome. That is a shape check on this module's own output, not a tamper defense: a writer that edits the database can store any well-formed outcome it likes, and nothing here authenticates it. `details: None` stores as `null`, so an empty `details` object cannot decode to `None`.

There is no retention and no eviction: one row per retry identity for the database lifetime. Nothing orders or expires rows, so the table carries no timestamp column.

## Alternatives considered

- **Keep the in-memory default.** Rejected: it is the defect. A reassembled surface starts empty, which is exactly the state the lost-prune replay needs.
- **Record the outcome only, without the claim.** Rejected: Connect calls `record` after the port commits, so a crash in that gap still leaves a committed prune with no record and a retry that executes it again. The window this change exists to close would stay open; only the restart case would be covered.
- **Record inside the prune's own transaction.** Rejected: the receipt belongs to Connect's retry identity, which does not reach the port — the port receives the admitted scope and request, not the `idempotency_key`. Threading it through would change the released `HistoryPort` contract, not just this crate.
- **A database-backed lock for cross-process single-flight.** Rejected: it is a second concurrency mechanism to implement, test and keep correct across processes, for a deployment shape the host already constrains. The claim row already stops the second process from executing; it answers `RECEIPT_EXPIRED` instead of waiting, and one serving process per database stays the host obligation.
- **A new crate dependency or a second migration file.** Rejected: `HistoryDedupStore` is a `spoke-connect` trait behind `remote-adapter`, which only the runtime composes, and the migration set is one initial file edited in place.
- **A serde implementation on the upstream result types.** Rejected: they belong to a published crate; a private codec is four functions and keeps the wire spelling in one place.

## Consequences

- The commit-to-record window no longer re-executes. A prune whose process died between its committed port call and `record` answers `RECEIPT_EXPIRED` on retry — at most once, not exactly once. The retry does not replay the lost `{deleted, floor_revision}` either; closing that needs the port to receive the retry identity, which the released contract does not pass (`I-000107`).
- The claim is taken before the port runs, so `RECEIPT_EXPIRED` reports only that no outcome was recorded for the key — never that the original request did not execute. What history remains is observable: the entry's persisted floor and which checkpoints still read, which tells the caller whether another prune is still needed but does not attribute the floor to that request. Whether the request executed is not observable, because one claim row covers a port never called, a port call still in flight, and a port call whose process died before `record` — the last two may have committed. A caller therefore re-reads the floor and the surviving checkpoints to decide whether another prune is needed under any `idempotency_key`, and never treats the refusal as that evidence (`I-000108`).
- Two processes serving one database turn the later same-key caller into `RECEIPT_EXPIRED` instead of making it wait (`I-000109`). One serving process per database stays the host obligation and is stated in the module docs.
- The table grows for the database lifetime with the number of distinct prune retry identities. A bounded retention policy would need a reader to order rows by age; none exists, so no timestamp column is written.
- The claim, the port call and the record each take and release the pool's one connection in turn, so no operation holds two. A prune serializes behind whatever else shares that connection.
- Adoption keeps the released in-memory default: the copy is not destructive, and its replay path is governed by the create-only CAS and the adoption anchor instead.
- The table joins the schema's recreate-on-mismatch policy: no migration path, no back-fill.

## References

- `.agents/specs/architecture/serving-seam.md` § History — the dedup paragraphs and the refusal table
- `.agents/specs/data/store-schema.md` § Current state, § Where it lives
- `.agents/specs/architecture/mind-model.md` § Current state
- `crates/mindcore-runtime/src/history_dedup.rs` — `SqliteHistoryDedupStore`, `encode_outcome`, `decode_outcome`
- `crates/mindcore-sqlite/migrations/0001_init.sql` — `history_dedup`
- `crates/mindcore-runtime/tests/loopback.rs` — `a_prune_replays_its_recorded_outcome_after_the_store_is_reopened`
