# SemVer & Storage Compatibility Policy

`mindcore` follows SemVer 2.0. This document is the single source of truth for
how version numbers move and what storage compatibility is guaranteed. It is
machine-checked by `tooling/semver/check.mjs`, which runs in CI on every push
and pull request.

## Publish scope

| Crate              | Published to crates.io            |
| ------------------ | --------------------------------- |
| `mindcore-base`    | yes — public API                  |
| `mindcore-spi`     | yes — public API                  |
| `mindcore-sqlite`  | yes — public API                  |
| `mindcore-spoke`   | no — `publish = false`            |
| `mindcore-runtime` | no — `publish = false`            |

## Pre-1.0 rules

While the workspace version is `0.x.y`:

- A **minor** bump (`0.x.0`) is the breaking-change release: API removals,
  signature changes and storage-format changes land only on minor bumps.
- A **patch** bump (`0.x.y`) carries fixes only — no API or storage-format
  changes.
- `cargo semver-checks` already encodes this convention: pre-1.0 it treats the
  minor position as the SemVer "major" position.

## Dependency-follow rules

- **SPOKE follow rule** — `spoke-schemas`, `spoke-operations` and
  `spoke-connect` are declared at the `0.18` minor line
  (`>=0.18.0, <0.19.0`) and are part of the public crates' API surface.
  `Cargo.lock` remains the build pin: it fixes the exact `0.18.x` version
  every checkout, CI run and `cargo publish --dry-run` builds against. A
  `spoke-*` **minor** bump forces a mindcore **minor** bump; a `spoke-*`
  **patch** bump may ship as a mindcore **patch**.
- **sqlx major rule** — `sqlx` types are public: `MindStore::pool()` hands
  callers a `sqlx::SqlitePool`. A sqlx **major** bump is therefore a breaking
  change for consumers and forces a mindcore major-level bump even while
  pre-1.0 — it must land on a minor bump, never a patch.

## Storage compatibility

`mindcore-sqlite` embeds its schema from `crates/mindcore-sqlite/migrations/`
via `sqlx::migrate!`.

- **Published migration files are immutable.** Once a release ships a migration
  file, its bytes never change; a checksum mismatch makes `sqlx` refuse an
  existing database.
- **New schema is append-only.** Changes arrive as new migration files with
  increasing version prefixes; shipped files are never edited or reordered.
- **`open` refuses an unknown applied set.** `MindStore::open` declines a
  database whose applied migrations no longer match the embedded set (sqlx's
  `VersionMissing`, or a checksum mismatch) rather than guessing an upgrade
  path.
- **Pre-release exception.** Before the first published release the migration
  files are still mutable and cross-commit database compatibility is
  unsupported; the rules above take effect at the first `v*` tag.

`tooling/migrations/check.mjs` guards this policy in CI.

## Machine enforcement

`node tooling/semver/check.mjs` fails the build when:

- a publishable crate still declares `publish = false`;
- an internal `path` dependency in a publishable crate's `[dependencies]` lacks
  `version` (it could not resolve on crates.io);
- `mindcore-spi` declares the `test-scoring` feature — a test-only probe that
  must never ship;
- `[workspace.package] version` is not valid SemVer.

Once a `v*` tag exists it additionally runs
`cargo semver-checks check-release -p <crate> --baseline-rev <tag>` for each
publishable crate. Before the first tag it prints
`no baseline tag; API diff check skipped` and passes.
