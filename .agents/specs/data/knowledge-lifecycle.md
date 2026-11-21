# Data: knowledge lifecycle

## Scope

How a knowledge entry behaves over time: the history a write leaves behind, the operation that adopts a shared entry into a personal collection, and the marker that retires one entry in favour of another. It does not define memory semantics, cognition record shape, or remote (Connect) behaviour.

## Current state

### Revision history

`knowledge_entry_revisions` holds one row per replaced revision:

| Column | Meaning |
|---|---|
| `collection`, `entry_id`, `revision` | the entry and the revision the payload carries; together the primary key |
| `payload` | the payload exactly as stored at that revision |
| `cognition_head_seq` | the head that payload was stored with, or null when it had none |
| `recorded_at` | the replaced state's own `updated_at`, not the archiving instant |

The table is written by `knowledge_entries_capture_revision`, the migration's `BEFORE UPDATE OF revision` trigger on `knowledge_entries`, not by the Rust write path: a cost contract pins the append path's statement count, so history must not add a literal statement there. The trigger is the table's only inserter, and an explicit prune its only deleter; the primary key rejects a second row for the same revision, so a repeated capture fails as a constraint violation rather than duplicating the preimage. The trigger's skip predicate excludes the dedicated cognition append, which advances the carrier without replacing an entry payload.

A create writes no history row, and a refused write (stale base revision, invalid module, missing committed record, exhausted revision space) writes none.

**Retention.** `ScopedKnowledgeStore::prune_history(entry_id, keep)` is the one operation that removes history, and only a caller asks for it: the checkpoints it deletes are gone permanently — not archived, recoverable or replayable afterwards. `keep` counts checkpoints, newest first, and never the live row. While more than `keep` checkpoints exist, the prune deletes every checkpoint below the `keep`-th newest and sets `history_floor_revision` to the revision just above the newest deleted checkpoint — the earliest revision the entry can still answer. That floor need not be a stored checkpoint: a dedicated append advances the revision without writing a preimage, so revisions between surviving checkpoints are derived from the next checkpoint and stay replayable. Otherwise the prune deletes nothing, reports `deleted: 0`, and leaves the stored floor alone, so a repeated prune with the same `keep` is a no-op. A `keep` of zero is refused. Only a live entry is pruned; a retired one answers the generic absence and keeps every checkpoint.

**Replay.** Reading an entry as of revision N is gated by the entry's persisted `history_floor_revision` before checkpoint selection: a positive revision below the floor has no preimage left to derive it from, so it is refused rather than looked up — after selection it would be indistinguishable from "no saved revision that recent", which is the live fallback, and that fallback would answer the request with current content. At or above the floor, replay returns the stored preimage at the first saved revision at or above N — the live row stands in when no saved revision is that recent — with cognition records truncated to that checkpoint's head minus the number of dedicated appends crossed, that is, its revision minus N. The reconstructible gap between the **requested** revision and the nearest saved checkpoint at or above it consists only of dedicated appends, so the read proves that by position: the crossed `append_seq` values must be exactly the consecutive integers in the range, each classified. A missing, extra, duplicate or non-integer position is `InternalError` — a row count is not a proof, because SQLite's non-strict columns accept a `REAL` in an integer position and a counted fractional row masks a missing one.

A revision below the entry's floor answers `KnowledgeEntryNotFound` with the message `knowledge entry revision was pruned` and `details.reason` `HISTORY_TRUNCATED`; a caller matches the code and that token, never the message.

An unassigned revision, revision 0, a value above the representable range, a revision above the live one, and a missing entry all answer `KnowledgeEntryNotFound` with the message `knowledge entry revision not found` and no details.

### Adoption

`mindcore_spi::adopt_knowledge_entry(src, dst, entry_id, expected_source_revision)` copies a shared entry into a personal collection through the caller's two scoped ports.

- The destination is addressed by the **same `entry_id`**; the collection distinguishes the copy.
- The copy carries one Mindcore adoption anchor naming the source entry and the source revision it was taken at. `adoption_anchor_at_revision` writes that shape; `adoption_anchor` writes only the legacy revision-less form, which the adoption path cannot recognize on a repeat. `mindcore_base::knowledge::adoption_anchor_parts` is the shape's single **reader** — every consumer, including the SQLite resolver and the adoption idempotency check, goes through that one function, so a shape the parser cannot read back is a defect in the builder.
- The anchor's recorded revision is the copy's identity. A repeat at that revision returns the existing copy (`AlreadyPresent`) with its original anchor; a request naming the source's current revision instead is `RevisionConflict` naming the revision the copy was adopted at, even when the source now sits at the requested one. A repeat never rewrites the copy and never re-points the anchor at a newer source revision.
- A missing, retired or undisclosable source answers one generic `KnowledgeEntryNotFound`; a source the caller cannot see is indistinguishable from one that does not exist. An empty source id or a zero revision is refused before any I/O.
- A destination slot occupied by anything else — including a retired entry — answers `KnowledgeEntryAlreadyExists` and is left byte-identical. The store distinguishes that case from a genuine validation failure by a structured refusal detail, never by matching a message.

### Retirement

`knowledge_entries.superseded_by` names the entry that replaces this one.

- The marker is storage-owned: a submitted payload cannot set or clear it. The write advances the carrier revision through the ordinary CAS path, so its preimage is captured like any other change.
- Its composite foreign key `(collection, superseded_by)` references the entry primary key and does not cascade: a marker can only name a same-collection sibling, and deleting a referenced successor fails.
- Default reads and lists omit a retired entry; `get_entry_including_superseded` returns it with its marker.
- An adoption anchor naming a retired entry resolves to the first live entry along the chain. A hidden or missing hop answers the generic absence keyed by the anchor's original id; a cycle is `InternalError`.
- A retired carrier has no current judgment: the current-record lookup joins the live predicate, while explicit historical reads and revision replay stay available.
- Adoption never copies a retired source, and a repeat adoption against a source that has since been retired answers the generic absence; an existing copy keeps its original provenance.

### Shared scope rehearsal

A shared `World` scope carries the same lifecycle contract as any other collection. `I-000009`'s seven acceptance facts hold on one isolated `shared:world` scope, and `crates/mindcore-sqlite/tests/shared_scope_rehearsal.rs` asserts each of them against real stores rather than a served face:

| Fact | Contract |
|---|---|
| Revisions | An entry read as of an earlier revision returns that revision's own payload. |
| References | A personal copy carries one adoption anchor naming its shared source id and adopted revision; resolution runs from the copy outward through the same collection, following each retirement marker to the first live entry. There is no reverse lookup from source to copies. § Retirement owns what a hidden, missing or cyclic hop answers; this rehearsal walks only a live chain. |
| Holder / binding disclosure | On the same route, an entry carrying the `owner-private` disclosure is read and listed by its owner viewpoint and answers the generic absence to a foreign viewpoint; the entry's disclosure policy, not the viewpoint pair, decides visibility, and after retirement both viewpoints see the same absence. |
| Promotion | Writing the personal copy leaves the shared source's revision and stored payload bytes unchanged. |
| Carrier transaction invariants | The carrier head and its entry settle in one `BEGIN IMMEDIATE`; a rollback leaves neither visible. |
| Backup / restore | A store closed, copied as a file and reopened keeps the persisted revisions, anchors, retirement markers and source bytes, and the disclosure and transaction probes still hold on the copy. |
| Single-writer switch | With the second handle's busy timeout at zero, a write is refused without waiting while the first handle holds the write reservation and leaves no partial row; the same write succeeds on a retry after that handle released the reservation and then closed. |

**Disclosure.** Ordinary reads and lists omit a retired entry for both viewpoints the rehearsal exercises — an owner and a foreign viewpoint see the same generic absence once the entry is retired. The explicit lifecycle read (`get_entry_including_superseded`) takes only an entry id and no viewpoint, and returns the row with its `superseded_by` marker.

**Backup.** A file copy is a backup only of a closed store: committed frames of a live WAL database may still sit in `-wal`, so the main file alone is not a consistent snapshot. Closing the last connection checkpoints them, and the reopened copy carries revisions, anchors, retirement markers, carrier state and payload bytes unchanged.

**Single writer.** The rehearsal's two handles exercise SQLite's exclusion, not a supported concurrency mode: one store per file is the `MindStore` contract, and two stores on one file are unsupported rather than refused (`mind-model.md` § Invariants). Three distinct conditions decide what a caller sees, and the rehearsal fixes all three rather than proving one implies another:

- the **wait timeout** — the rehearsal sets the second store's `busy_timeout` to zero, so a contended acquisition is refused immediately instead of waiting out the production five-second handler;
- **release of the write lock** — the first store's reservation is rolled back;
- **closing the handle** — the first store is closed.

Only one writer can hold the reservation, and an acquisition that cannot obtain it within the configured busy wait is refused without a write. The rehearsal's retry runs after both the rollback and the close, so it shows that ordered scenario, not that either step alone would let the write through.

## Invariants

- History, adoption and retirement share the existing immediate-transaction discipline: the write reservation is taken before claim state is read.
- Pruning shares that discipline: one `BEGIN IMMEDIATE` reservation covers the entry read, the window computation, the deletes and the floor update. It writes only `history_floor_revision` on the entry row — never the revision — so the capture trigger saves no checkpoint of the prune itself. A retired entry's history is never deleted, and no cognition record or live payload is touched.
- The disclosure predicate is unchanged: hiding is one generic absence on every served path.
- The current-row read, the header read, the default list and the scoped put keep their statement counts; the filter is an added predicate.
- A retired row and its history are never deleted; no committed cognition record is rewritten.
- The CAS compares the stored revision **column** against the caller's expectation; the marking path additionally refuses a column/payload disagreement before writing, while the full put never decodes the stored payload and therefore cannot detect that drift.

## Where it lives

- History table, trigger, retirement column: `crates/mindcore-sqlite/migrations/0001_init.sql`.
- Write path, replay, explicit prune, retirement write, resolution, current-record predicate: `crates/mindcore-sqlite/src/knowledge.rs`, `cognition.rs`.
- Adoption and the anchor codec: `crates/mindcore-spi/src/adoption.rs`, `crates/mindcore-base/src/knowledge.rs`.
- End-to-end evidence: `crates/mindcore-sqlite/tests/knowledge_lifecycle.rs`.

## Served failure messages

A storage failure on the served knowledge face answers `InternalError` with the fixed text `knowledge storage is unavailable` and no details. The one exempt refusal is an over-bound list: both faces answer `InternalError` with `details.reason` `COLLECTION_OVER_CAPACITY` and `details.limit` set to the bound it crossed (`SERVED_KNOWLEDGE_LIST_LIMIT`), so a caller tells capacity apart from a storage fault. The served face forwards it unchanged — it is about the peer's own collection and names no local schema. Driver text is never forwarded to an admitted peer, because it names local tables and columns. A host that needs the underlying cause observes it in its own storage layer or its own logs — the library writes none.

## Known limits

- History is bounded only by an explicit `prune_history` call: there is no retention schedule, no default `keep`, and no archive, so a pruned preimage cannot be recovered.
- A below-floor refusal means the revision's preimage is gone, not that the revision was never stored: a revision at or above the floor is derived from the next surviving checkpoint and still answers exactly.
- The append classification column distinguishes a full-put record from a dedicated append; that classification is load-bearing for replay's positional proof.
- Adoption resolves anchors by walking same-collection markers; a host that needs cross-collection provenance resolution does not get it here.

## Change obligations

A change to the history table, the trigger, the retirement column or the anchor shape lands together with this page, the reader that decodes it, and the lifecycle scenario. Removing the trigger's skip predicate or the positional proof silently weakens replay without failing a test that only compares happy-path output.
