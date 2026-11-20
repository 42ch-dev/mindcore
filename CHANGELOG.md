# Changelog

## [Unreleased]

- Initial workspace for `mindcore-base` and `mindcore-sqlite`.
- Dependencies: the SPOKE cohort is pinned to `=0.17.0` — `spoke-schemas` and `spoke-operations` in
  `[workspace.dependencies]`, and `spoke-connect` there too, so the SQLite test dependency inherits
  the same pin with `remote-adapter` enabled.
- `mindcore-sqlite`: pre-release migrations are mutable. A database that already applied a migration
  whose embedded file has since changed is refused by `MindStore::open` with a checksum mismatch;
  there is no upgrade path until the first release. A database whose applied migrations still match
  opens normally, whatever commit created it — but cross-commit compatibility is unsupported and
  untested.
