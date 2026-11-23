# Data: knowledge lifecycle

## Scope

How a knowledge entry behaves over time: the history a write leaves behind, the operation that adopts a shared entry into a personal collection, and the marker that retires one entry in favour of another. It also names which of these operations are served as protocol operations and which of this page's results they carry; the serving face's admission, wire decode, refusal set, snapshot projection and prune dedup are not this page's (`.agents/specs/architecture/serving-seam.md`). It does not define memory semantics, cognition record shape, or Connect transport behaviour.

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

A revision below the entry's floor answers `HistoryTruncated` with the message `knowledge entry revision was pruned` and `details.floor_revision` set to the entry's floor; a caller matches the code and that member, never the message.

An unassigned revision, revision 0, a value above the representable range, a revision above the live one, and a missing entry all answer `KnowledgeEntryNotFound` with the message `knowledge entry revision not found` and no details.

**Served reachability.** Both local operations are published as protocol operations when the host enables `ServeFaces.history`: `history.revisions.get` replays one revision and `history.revisions.prune` runs the same prune. `RoutedKnowledgeStore` implements `spoke_operations::HistoryPort` beside `ScopedKnowledgePort`, so the served ops reuse this page's replay, retention, admission and disclosure rules rather than a second history path: a served replay answers this page's `HistoryTruncated` and `KnowledgeEntryNotFound` cases, and a served prune returns this page's `PruneReport`. The wire decode, the protocol refusal set and its `details`, the `RevisionSnapshot` projection, the `knowledge_entry`-only coverage and the prune dedup, including how long its records live, belong to `.agents/specs/architecture/serving-seam.md` alone; that page is where the read and prune denials are distinguished.

### Adoption

`mindcore_spi::adopt_knowledge_entry(src, dst, source_scope_id, source_entry_id,
destination_entry_id, expected_source_revision)` copies a shared entry into a personal collection
through the caller's two scoped ports. It is the local half of the served `knowledge.adopt`
operation; the retirement-chain resolution that precedes it belongs to the served face, so the
source this function sees is already the terminal. The served defaults, refusal vocabulary, terminal
resolution, destination selection and dedup are
[`serving-seam.md`](../architecture/serving-seam.md)'s; everything below is this helper's own
behavior over the two ports it is given.

- The destination is addressed by `destination_entry_id`, never by the source id: the caller
  resolves it — the served runtime defaults it to the resolved terminal's id, and a supplied value
  may name the copy or that same terminal id — and the collection distinguishes the copy from its
  source.
- The copy carries the protocol's `KnowledgeEntry.adoption_anchor` and leaves `source_anchor` unset.
  The anchor is the `(source_scope_id, source_entry_id, source_revision)` triple: the wire scope id
  of the source, the entry id, and the source revision it read. `source_scope_id` is not the source
  collection — it is a separate identifier; see the route bullet below.
  `mindcore_base::knowledge::adoption_anchor` is the shape's single **builder** and
  `adoption_anchor_parts` its single **reader** — every consumer, including the SQLite resolver and
  the adoption idempotency check, goes through that one reader, so a shape it cannot read back is a
  defect in the builder. The builder answers `None` for an empty scope id, an empty entry id or a
  zero revision.
- The source scope id a host serves is `AdoptionRoute.scope_id` (`crates/mindcore-runtime/src/lib.rs`),
  the id naming the configured source on the wire; assembly refuses an empty one, and it is the
  value every copy of that source records. It is a separate identifier from
  `AdoptionRoute.source.collection`, the collection the source port actually reads.
- The copy's anchor triple is its identity. A repeat naming the same source scope, entry and
  revision returns the existing copy (`AlreadyPresent`) with its original anchor; a request naming
  the source's current revision instead is `REVISION_CONFLICT` with exactly
  `details.adopted_revision`, the revision the copy was adopted at, even when the source now sits at
  the requested one. A repeat never rewrites the copy and never re-points the anchor at a newer
  source revision.
- A stored anchor is immutable on every put path. The Connect responder gates `port.knowledge.put`
  with `spoke_operations::assert_adoption_anchor_preserved`, and the local
  `KnowledgeEntryPort::put_knowledge_entry` path reaches the same helper through `write_entry`,
  between the CAS check and the write. A put that replaces or drops a stored anchor refuses
  `ADOPTION_ANCHOR_IMMUTABLE` with `details.entry_id` and stores nothing, or `INVALID_INPUT` when an
  anchor cannot be canonicalized; a create, an entry without an anchor and an identical anchor all
  pass. Adoption is the intended producer of an anchor, but the gate enforces preservation, not
  exclusivity: a create, or a put whose stored entry carries no anchor, may set one.
- A missing, retired or undisclosable source answers one generic `NOT_FOUND`; a source the caller
  cannot see is indistinguishable from one that does not exist. An empty scope id, an empty source
  id, an empty destination id or a zero revision is refused before any I/O as `INVALID_INPUT`.
- A destination slot occupied by anything else — including a retired entry, and including an anchor
  naming another source scope or entry — answers `REVISION_CONFLICT` with no `details` and is left
  byte-identical. The store distinguishes "this slot holds a superseded entry" from a genuine
  validation failure by a structured refusal detail, never by matching a message.
- A create-only put can meet the occupancy after the empty read: a local destination refuses it as
  `KNOWLEDGE_ENTRY_ALREADY_EXISTS`, while a 0.18 remote destination refuses the caller-OCC fence
  before the provider as `REVISION_CONFLICT`. Either refusal re-reads the slot and answers by the
  anchor rule above, so the same-triple winner returns `AlreadyPresent` and a copy at another
  revision returns `REVISION_CONFLICT` with `details.adopted_revision`; the refusal code a
  destination chose never changes the outcome. The re-read alone decides: an answer of absence
  (`NOT_FOUND`) or occupancy (`KNOWLEDGE_ENTRY_ALREADY_EXISTS`) becomes this call's own occupancy
  refusal — `REVISION_CONFLICT` with no `details` — and the put's refusal and its details are
  discarded, because no slot claim was established. Every other re-read refusal is forwarded as the
  destination reported it: `NOT_AUTHORIZED`, `REVISION_CONFLICT` from the destination, and the
  storage codes, which the served face turns into the fixed `InternalError` rather than the put's
  own refusal. A put refusal that is neither an occupancy code nor the structured
  superseded-occupant detail is not a slot claim and propagates unchanged. A stored anchor revision
  above `2^53 - 1` is the fixed store fault, never a refusal carrying the out-of-range integer; that
  check applies to an anchor naming the requested source scope and entry, so an anchor naming
  anything else is the ordinary occupancy above.
- The copy's `cognition` module is removed rather than emptied: only a present module is dropped,
  and every other shape is left for the destination to validate rather than being masked as an
  empty history. `body`, `canonical_name`, `entry_type` and `status` come from the source, and
  `revision`, `created_at` and `updated_at` are left for the store to assign.
- **Governance is inherited, not reset.** The copy keeps the source's `owner` and `disclosure`.
  The contract resets them to the destination's; Mindcore defines no destination governance, because
  the destination is the collection the knowledge face serves — the access boundary itself — and the
  served face refuses every source the viewpoint-free predicate does not see. An inherited policy
  therefore cannot disclose anything the admitted principal could not already read. This is a
  recorded deviation, not an open task.

### Retirement

`knowledge_entries.superseded_by` names the entry that replaces this one.

- The marker is storage-owned: a submitted payload cannot set or clear it. The write advances the carrier revision through the ordinary CAS path, so its preimage is captured like any other change.
- Its composite foreign key `(collection, superseded_by)` references the entry primary key and does not cascade: a marker can only name a same-collection sibling, and deleting a referenced successor fails.
- Default reads and lists omit a retired entry; `get_entry_including_superseded` returns it with its marker.
- An adoption anchor naming a retired entry resolves to the first live entry along the chain. A hidden or missing hop answers the generic absence keyed by the anchor's original id; a cycle is `InternalError`.
- A retired carrier has no current judgment: the current-record lookup joins the live predicate, while explicit historical reads and revision replay stay available.
- Adoption never copies a retired row: the served face resolves the requested source to the first
  live entry along the chain and copies that terminal, so the copy's anchor names a live row. The
  copy's stored `entry_id` is `destination_entry_id` when the caller supplied one — including the
  terminal's own id — and that terminal's id only when the member was absent. The served chain,
  revision-domain and refusal rules are
  [`serving-seam.md`](../architecture/serving-seam.md) § terminal anchor and § refusals.

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
- The disclosure predicate is unchanged: a hidden entry, or a retired one read through the ordinary reads and lists, is one generic local absence. The explicit lifecycle read and revision replay still see a retired entry (§ Retirement). The served face's read and prune denials are `.agents/specs/architecture/serving-seam.md`'s.
- The current-row read, the header read, the default list and the scoped put keep their statement counts; the filter is an added predicate.
- A retired row and its history are never deleted; no committed cognition record is rewritten.
- The CAS compares the stored revision **column** against the caller's expectation; the marking path additionally refuses a column/payload disagreement before writing, while the full put decodes the stored payload for the anchor gate but never compares that payload's revision with the revision column, so it cannot detect that drift.

## Where it lives

- History table, trigger, retirement column: `crates/mindcore-sqlite/migrations/0001_init.sql`.
- Write path, replay, explicit prune, retirement write, resolution, current-record predicate: `crates/mindcore-sqlite/src/knowledge.rs`, `cognition.rs`.
- Adoption and the anchor codec: `crates/mindcore-spi/src/adoption.rs`, `crates/mindcore-base/src/knowledge.rs`.
- The served half — scope comparison, retirement-chain resolution, the one transaction and the
  refusal hygiene: `crates/mindcore-runtime/src/adoption.rs`; its wire contract is
  `.agents/specs/architecture/serving-seam.md`.
- End-to-end evidence: `crates/mindcore-sqlite/tests/knowledge_lifecycle.rs`.
- Served history: the `HistoryPort` impl and `RevisionSnapshot` projection in `crates/mindcore-sqlite/src/knowledge.rs`; the flag, `HistoryAdmission` and capability in `crates/mindcore-runtime/src/lib.rs`; over-the-wire evidence in `crates/mindcore-runtime/tests/loopback.rs`.

## Served failure messages

A storage failure on the served knowledge face answers `InternalError` with the fixed text `knowledge storage is unavailable` and no details. The one exempt refusal is an over-bound list: both faces answer `InternalError` whose `details` carries `reason` `COLLECTION_OVER_CAPACITY` and `limit` set to the bound it crossed (`SERVED_KNOWLEDGE_LIST_LIMIT`), so a caller tells capacity apart from a storage fault. The served face forwards it unchanged — it is about the peer's own collection and names no local schema. Driver text is never forwarded to an admitted peer, because it names local tables and columns. A host that needs the underlying cause observes it in its own storage layer or its own logs — the library writes none.

## Known limits

- History is bounded only by an explicit `prune_history` call: there is no retention schedule, no default `keep`, and no archive, so a pruned preimage cannot be recovered.
- A below-floor refusal means the revision's preimage is gone, not that the revision was never stored: a revision at or above the floor is derived from the next surviving checkpoint and still answers exactly.
- The append classification column distinguishes a full-put record from a dedicated append; that classification is load-bearing for replay's positional proof.
- Adoption resolves anchors by walking same-collection markers; a host that needs cross-collection provenance resolution does not get it here.

## Change obligations

A change to the history table, the trigger, the retirement column or the anchor shape lands together with this page, the reader that decodes it, and the lifecycle scenario. Removing the trigger's skip predicate or the positional proof silently weakens replay without failing a test that only compares happy-path output. A change to which local history results are served lands here; a change to the served projection, the protocol refusal set or its `details` belongs to `.agents/specs/architecture/serving-seam.md` alone, with the loopback assertions.
