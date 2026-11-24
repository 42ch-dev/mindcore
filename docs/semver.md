# SemVer & Storage Compatibility Policy

`mindcore` follows SemVer 2.0. This document is the single source of truth for
how version numbers move and what storage compatibility is guaranteed. The
static rules below and the API diff are machine-checked by
`tooling/semver/check.mjs`, which runs in CI on every push and pull request and
again in `release.yml`'s read-only `semver-gate` job, before the `release` job
creates the tag. The dependency-follow rules and the choice between a minor and
a patch bump are reviewed with the release PR; the checkers do not classify a
change for you.

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

- **SPOKE follow rule** — `spoke-schemas` and `spoke-operations` are part of
  the published crates' API surface; `spoke-connect` is a workspace
  dev-dependency of `mindcore-sqlite` and is not published. All three are
  declared at the `0.18` minor line (`>=0.18.1, <0.19.0`) and follow the same
  version policy. `Cargo.lock` remains the build pin: it fixes the exact
  `0.18.x` version every checkout, CI run and `cargo publish --dry-run` builds
  against. A `spoke-*` **minor** bump forces a mindcore **minor** bump; a
  `spoke-*` **patch** bump may ship as a mindcore **patch**.
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
- **First upload, then immutable.** The first upload to crates.io closed the
  pre-release window: its migration set is the published one and the rules
  above hold from there — a prerelease tag locks the shipped set too, because
  the release PR writes the lock. An upload does not move the *API* baseline:
  that baseline is the latest stable `v*` tag, so an alpha upload claims no
  compatible API.

`tooling/migrations/check.mjs` guards this policy in CI. The release PR runs it with
`--write-lock` so every migration it ships is locked before the tag; `release.yml`'s
`semver-gate` runs the same command on the tagged commit and fails if the lock changes, which
catches a migration merged after the release PR opened.

## Machine enforcement

`node tooling/semver/check.mjs` fails the build when:

- a publishable crate still declares `publish = false`;
- an internal `path` dependency in a publishable crate's packaged dependency
  tables (`[dependencies]`, `[build-dependencies]`, and their `target.*`
  variants) lacks `version`, including one inherited from
  `[workspace.dependencies]` (it could not resolve on crates.io; path
  dev-dependencies are exempt because cargo drops them);
- `mindcore-spi` declares the `test-scoring` feature — a test-only probe that
  must never ship;
- `[workspace.package] version` is not valid SemVer.

Once a stable `v*` tag exists it additionally runs
`cargo semver-checks check-release -p <crate> --baseline-rev <tag>` for each
publishable crate. The baseline is the newest stable `v*` tag — on a tag
commit, the previous stable release reachable from its parent. A prerelease tag
never qualifies: it is Git-only, claims no registry version, and would otherwise
be compared against as if it had shipped one. The API diff
fails the build only on a release tag commit (when the tag itself must carry a
compatible API), unless `--blocking` is passed: the flag forces the diff to
block everywhere, which is how `release.yml`'s `semver-gate` job checks the
merged release PR before `release` creates the tag. Off a tag commit without
the flag, violations are reported as non-blocking warnings, so a breaking
change can land on `main` and be enforced by a release PR's version bump. A
shallow clone that hides every `v*` tag is an error — fetch the tags and their
history (`actions/checkout: fetch-depth: 0`) — not a pass. No stable `v*` tag
— a fresh repository, or one whose history holds only prerelease tags — prints
`no baseline tag; API diff check skipped` and passes, as does a release tag with
no previous stable `v*` tag, the first release: there is no predecessor to
compare against, so the API diff does not run (rather than
comparing the tag against itself), and `tooling/migrations/check.mjs` treats
the working tree as the first published set.

`semver-gate` skips `check.mjs` entirely for a prerelease version (one with a
`-` suffix): a prerelease is tagged and released from Git but never uploads, so
it claims no registry version and owes no compatible API to one. Stable releases
always run the gate.
