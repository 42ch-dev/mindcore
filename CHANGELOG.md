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
  provider call runs outside the settlement transaction, and the fill rechecks the committed
  revision and withholding, so work that outlived its record writes nothing. A failed warm is a
  cache miss, never a failed, rolled-back or replayed Memory mutation; no embedder, no serving
  identity and an idempotent replay write nothing.
