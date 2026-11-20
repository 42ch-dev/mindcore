# Changelog

## [Unreleased]

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
- Dependencies: the SPOKE cohort is pinned to `=0.17.0` — `spoke-schemas` and `spoke-operations` in
  `[workspace.dependencies]`, and `spoke-connect` there too, so the SQLite test dependency inherits
  the same pin with `remote-adapter` enabled.
- `mindcore-sqlite`: pre-release migrations are mutable. A database that already applied a migration
  whose embedded file has since changed is refused by `MindStore::open` with a checksum mismatch;
  there is no upgrade path until the first release. A database whose applied migrations still match
  opens normally, whatever commit created it — but cross-commit compatibility is unsupported and
  untested.
- `mindcore-sqlite`: optional retained-vector storage. `memory_embedding_vectors` holds one
  little-endian `f32` vector per `(record_id, identity_hash)` with its record revision and dimension,
  indexed by identity so a retired identity's whole partition is one delete. The new migration
  applies to a database created before it; the table starts empty.
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
