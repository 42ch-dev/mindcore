# Changelog

## [Unreleased]

- Initial workspace for `mindcore-base` and `mindcore-sqlite`.
- `mindcore-sqlite`: pre-release migrations are mutable. A database file created by an earlier
  commit fails `MindStore::open` with a checksum mismatch and must be recreated; there is no
  upgrade path until the first release.
