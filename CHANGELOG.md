# Changelog

## [Unreleased]

- Initial workspace for `mindcore-base` and `mindcore-sqlite`.
- `mindcore-sqlite`: cognition records now have their own table — one row per record in
  `cognition_records`, keyed by `(collection, entry_id, append_seq)` with a scoped unique
  `record_id` — and `knowledge_entries.cognition_head_seq` points at the holder's newest row.
  A database created before this change is recreated, not upgraded: the modified migration is
  refused by its checksum.
- `mindcore-sqlite`: pre-release migrations are mutable. A database that already applied a migration
  whose embedded file has since changed is refused by `MindStore::open` with a checksum mismatch;
  there is no upgrade path until the first release. A database whose applied migrations still match
  opens normally, whatever commit created it — but cross-commit compatibility is unsupported and
  untested.
