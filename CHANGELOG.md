# Changelog

## [Unreleased]

- **Breaking — `mindcore-runtime`:** the private product tool `tools.mindcore.adopt` is removed, not
  deprecated. Adoption is served only as the published protocol operation `knowledge.adopt` under the
  `spoke-knowledge-adoption` capability, which requires `spoke-knowledge`; the runtime filters the
  reserved id out of every host registration, so no manifest advertises it and no handler answers it.
  A caller migrates the call as `entry_id` → `source_entry_id`, `expected_source_revision` →
  `source_revision`, and expects `outcome` `adopted` instead of `created` (`already_present` and
  `entry` keep their meanings). The request gains required `source_scope_id` — it must equal
  `AdoptionRoute.scope_id`, and any other value is the same generic `NOT_FOUND` as a missing source —
  required `destination_scope_id` — it must equal the peer's admitted scope id, or `NOT_AUTHORIZED`
  before the source is read — required `idempotency_key`, and optional `destination_entry_id`, which
  names the copy and defaults to the resolved terminal's entry id; assembly now also refuses a
  source scope equal to `ServeFaces.scope_id`. The destination is no longer per-principal: the copy
  is written into the collection the knowledge face serves (`ServeFaces.knowledge`), not the
  adopting principal's `workflow_routes` entry, so it is readable through the `knowledge.get`/`list`
  that share the adoption's admission. `workflow_routes` now selects and authorizes only the
  routed workflow calls and plays no part in adoption. This is a breaking assembly change: a host
  whose adoption source collection equals that knowledge collection assembled before — the copy
  went to the principal's distinct route collection — and is now refused with `InvalidInput` ("the
  adoption source collection is the collection the knowledge face serves"). Migrate by naming a
  distinct adoption source collection, different from `ServeFaces.knowledge.collection` and from
  every `workflow_routes` destination, and by dropping any expectation that a route names an
  adoption destination. Two further refusals: the admitted scope's
  `entry_ids`/`entry_types` selection now applies to the destination, so an excluded entry is
  neither served nor written and answers the ordinary occupancy `REVISION_CONFLICT` with no
  details — byte-identical to a visible occupant's, so the answer reports slot occupancy only and
  never the occupying entry's content, revision or visibility — `NOT_AUTHORIZED` staying the answer
  for a `destination_scope_id` that is not the admitted scope; and a resolved terminal revision above
  `2^53-1` refuses before the write instead of committing a copy whose response the wire cannot
  carry. Two behaviour changes: adoption admits through the knowledge face's `(principals,
  knowledge_admission)` pair, so a routed principal that face does not admit
  answers `NOT_AUTHORIZED` where the product tool served it; and the source resolves through
  `resolve_retirement_chain` before the copy, so the anchor names the live terminal (the copy's own
  entry id is the supplied `destination_entry_id` or, when absent, that terminal's id), a
  live source that moved off `source_revision` is `REVISION_CONFLICT` with no details, and a chain
  that cannot resolve is `SOURCE_RETIRED` with `details.retirement_chain`. The local refusal
  vocabulary is the protocol's — `NOT_FOUND`, `NOT_AUTHORIZED`, `REVISION_CONFLICT` with
  `details.adopted_revision` only for a copy adopted at another revision, `SOURCE_RETIRED`,
  `ADOPTION_ANCHOR_IMMUTABLE` and `INVALID_INPUT` — and the `owner`/`disclosure` reset is not
  applied, so a copy inherits the source's governance. Connect's `AdoptionPortService` supplies the
  idempotency envelope, including `IDEMPOTENCY_CONFLICT` and `RECEIPT_EXPIRED`.
- `mindcore-spoke` / `mindcore-runtime`: `ServeFaces.memory` takes an `Arc<dyn ServedMemory>` — a
  `RemoteMemoryService` that reports the `memory.*` operations it dispatches — and the runtime
  advertises `spoke-memory-transfer` exactly when the attached service reports all five §7.29–§7.33
  operations. A bare `RemoteMemoryService` gains an empty `impl ServedMemory for … {}` and keeps
  advertising `spoke-memory` only. `mindcore_spoke::TransferService` binds the five transfer
  operations to the host's pool and peer-to-principal admission, owning one transaction per invoke.
- `mindcore-runtime`: `ServeFaces.workflow_caller` is replaced by `workflow_routes`, a map from each
  admitted principal id to the one knowledge collection its workflow calls reach. Each destination
  collection belongs to exactly one principal route; a collection named by two routes is refused at
  assembly. An unrouted principal is refused on `deliberate` and still recalls memory with an empty
  knowledge slot. A consumer that sets `workflow_caller` does not compile and must build the map.
- Initial workspace for `mindcore-base` and `mindcore-sqlite`.
- `mindcore-sqlite`: cognition records now have their own table — one row per record in
  `cognition_records`, keyed by `(collection, entry_id, append_seq)` with a scoped unique
  `record_id` — and `knowledge_entries.cognition_head_seq` points at the holder's newest row.
  A database created before this change is recreated, not upgraded: the modified migration is
  refused by its checksum.
- `mindcore-sqlite`: the cognition append and current lookup no longer read or rewrite the
  carrier's history. A workflow settlement validates the record, inserts one row and advances
  the head and carrier revision in the borrowed transaction, returning the assigned revision;
  the carrier payload keeps only the cognition module envelope, and full DTO reads project
  `records` from the rows in append order. A full put must carry the committed prefix
  unchanged and appends only its validated suffix.
- Dependencies: the SPOKE cohort is pinned to `=0.18.0` — `spoke-schemas` and `spoke-operations` in
  `[workspace.dependencies]`, and `spoke-connect` there too, so the SQLite test dependency inherits
  the same pin with `remote-adapter` enabled. 0.18.0 is this release's pin.
- `mindcore-sqlite`: pre-release migrations are mutable and the embedded set is now one initial
  file: `0002` is deleted and its table lives in `0001_init.sql`. There is no upgrade path until the
  first release: a database whose applied migrations still match the embedded file reopens normally,
  while `MindStore::open` refuses one that applied the removed `0002` with SQLx's `VersionMissing(2)`
  and one whose applied SQL has since changed with a checksum mismatch. Recreate the file only for one
  of those two migration mismatches, or move it aside first if you want to keep it; a busy, locked or
  unreadable database is retried, not recreated.
- `mindcore-base` / `mindcore-sqlite`: the legacy memory port is removed, not deprecated. The public
  `MemoryStore` trait, its `MindStore` implementation and the DTOs it carried — `MemoryCandidate`,
  `MemoryRecord` and `MemoryError` — are gone; the borrowed-transaction `MemoryRepository` boundary
  and the workflow settlements above replace them. `RecallQuery` stays. A consumer that implements or
  calls `MemoryStore` does not compile against this release and must move to `MemoryRepository`.
- `mindcore-sqlite`: optional retained-vector storage. `memory_embedding_vectors` holds one
  little-endian `f32` vector per `(record_id, identity_hash)` with its record revision and dimension,
  indexed by identity so a retired identity's whole partition is one delete. The table is part of the
  single initial migration and starts empty.
- `mindcore-sqlite`: a knowledge list hydrates its page's cognition records in one statement keyed
  by the page's entries, not one statement per entry.
- `mindcore-spoke`: the recall knowledge slot applies the served list's disclosure predicate. The
  workflow binding carries no admitted viewpoint, so an `owner`-private entry never enters the
  pack, whatever budget the recall asks for.
- `mindcore-spi` / `mindcore-spoke`: a tombstone holds no vector. `records.delete` prunes the
  record's stored vector rows inside the tombstone's own transaction, and the recall page's miss
  set covers live records only, so a tombstoned body is never re-embedded or filled.
- `mindcore-spi` / `mindcore-spoke`: a confirmed absorb settlement warms the retained vector of each
  fresh or updated record when the host supplies an embedder with a stable serving identity. The
  settlement reports the revision it committed for every record it names, and the warm embeds only a
  record whose live read still serves that revision — one a later writer moved before the read is
  obsolete work and makes no provider call and no row. The provider call runs outside the settlement
  transaction, and the fill rechecks the live revision and withholding, so work that outlived its
  record writes nothing. A failed warm is a cache miss, never a failed, rolled-back or replayed Memory
  mutation; no embedder, no serving identity and an idempotent replay write nothing.
- `mindcore-spi` / `mindcore-spoke`: recall now reuses the retained vectors. The page is authorized
  first, the query vector is resolved from one validated process-local slot keyed by principal, space,
  subject, exact query and serving identity, and the stored record vectors come from one snapshot that
  rebuilds admission and withholding and refuses a page that moved before any row is probed — a moved
  record is `REVISION_CONFLICT`, never a decode of its newer row, and one page-shaped statement reads
  the whole page. Only missing or stale revisions are embedded; the release reauthorizes the page and
  the knowledge set and writes those fresh vectors in the same transaction, taking SQLite's write
  reservation at begin when it has a fill, so a concurrent writer cannot invalidate the
  reauthorization snapshot between the check and the write: a refusal aborts the
  release and no awaited cache write can observe one afterwards, while a cache-only storage failure is
  a miss. The query slot is installed only once that boundary succeeds, and a late
  `EmbeddingsUnavailable` keeps a warm slot instead of replacing it. An identical warm recall makes no
  provider call. No embedder, no serving identity or a cold `EmbeddingsUnavailable` leaves the vector
  table untouched and keeps the adapter's lexical order. Retiring an identity clears its query slot as
  well as its stored partition. Ranking is unchanged, and the persisted path scores bit-identically to
  the in-memory one, tie order included.
- `mindcore-runtime`: `MindcoreSurface::retire_embedding_identity` is the one supported host entry for
  identity rotation. It clears the retired identity's in-memory query slot and deletes its stored
  partition in one `identity_hash` statement, returning the rows deleted, and leaves another identity's
  rows and query slot warm. Call it between recalls: it does not drain a recall already in flight, and a
  fill that commits afterwards re-creates rows it deleted.
- **Breaking — `mindcore-spi` / `mindcore-spoke`:** `WorkflowBinding::commit_cognition` takes the
  request deadline as its last parameter, so every implementation and test double carries it. The
  settlement binding bounds its pool-connection wait by that deadline: a settle whose deadline passes
  while it waits for the store's only connection answers `DeadlineExceeded` and opens no transaction,
  and a connection in hand is re-checked before `BEGIN IMMEDIATE`, so an expiry observed on either
  of those waits refuses and writes nothing. A write that has already begun keeps its actual or
  uncertain outcome, which may land after the deadline. The non-contended commit is unchanged.
  `mindcore-spoke` now names `tokio` with the `time` feature directly; the lockfile is
  unchanged. Closes the residual in which an expired settle could still wait on the store connection.
