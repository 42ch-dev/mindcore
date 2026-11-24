# Releasing to crates.io

The release workflow creates the tag and the GitHub Release. Publishing the crates is a separate,
manually dispatched step: `.github/workflows/publish.yml`.

## Publish order

```
mindcore-base → mindcore-spi → mindcore-sqlite
```

crates.io resolves a dependency only after it is already published, and the three crates depend on
each other through internal path edges:

- `mindcore-base` has no internal dependency.
- `mindcore-spi` depends on `mindcore-base`.
- `mindcore-sqlite` depends on `mindcore-base`.

The workflow publishes in that order and waits for each version to appear in the crates.io sparse
index before it starts the next crate. The index, not the web page or the API, is what the next
`cargo publish` resolves dependencies against, so this wait is what stops a propagation delay from
failing the next crate. It stops on the first failure: a crate that cannot be published leaves the
later crates unpublished rather than half released. A version already in the index is skipped, so
re-running the workflow after a partial failure continues from the first crate that is still missing
— a version can never be uploaded twice.

## Preconditions

Check these before the first run; each one makes `cargo publish` fail rather than publish something
wrong:

- Each of the three crates is publishable: `publish` is unset or lists `crates-io` (not `publish =
  false`).
- Every internal dependency edge carries a `version` next to its `path`. The registry ignores
  `path`, so a bare `{ path = "../mindcore-base" }` publishes a manifest nobody can resolve.
- `mindcore-sqlite` ships its `sqlx` offline cache: `crates/mindcore-sqlite/.sqlx` is a symlink to
  the workspace `.sqlx`, and the crate's `include` list carries `.sqlx/*.json`. The crate uses
  compile-time `sqlx::query!` macros, so the packaged crate only builds when that cache is inside
  it. A symlink that is missing after clone means the publish fails during Cargo's verification
  build, not at the registry.
- The workspace version in `Cargo.toml` is the version you intend to publish. The workflow reads it
  when the `version` input is empty, and refuses an input that disagrees with it.
- The tag for that version exists (`release.yml` creates it). `cargo publish` does not need it, but
  the release notes and the tag should describe what is on crates.io.

## Token

`CARGO_REGISTRY_TOKEN` is a repository secret holding a crates.io API token. The token must belong
to an owner of all three crate names, because the first publish claims each name permanently.
Crate ownership on crates.io is per crate, so verify all three names appear under the same account
before the first run.

Workflow permissions are `contents: read`; the only credential the job uses is that token.

## Dry run first

Dispatch the workflow with `dry_run: true`. It passes `--dry-run` to all three `cargo publish`
calls, which packages and verifies each crate without uploading, and skips the index wait.

One limit: `mindcore-spi` and `mindcore-sqlite` resolve `mindcore-base` from the registry while
packaging, so their dry run fails with `no matching package named mindcore-base found` whenever
that `mindcore-base` version is not published yet — which is every release, since the version is
new. A dry run therefore fully rehearses `mindcore-base` and stops at `mindcore-spi`. That is not a
regression in the real publish: `cargo publish` verifies the packaged crate before it uploads, so a
crate that would fail the dry run also fails before anything reaches the registry, and the workflow
stops there as well.

A local `cargo publish --dry-run` reproduces the same checks, but it fails on a workstation whose
`~/.cargo/config.toml` replaces `crates-io` with a mirror (`crates-io is replaced with
non-remote-registry source registry`); runners have no such config, so the workflow is the
reliable place to run it.

## Verify

For each crate, in order:

```bash
cargo search mindcore-base
# the sparse index is what cargo resolves, and it shows a version as soon as it is usable:
curl -fsS -H 'User-Agent: mindcore-release-check' https://index.crates.io/mi/nd/mindcore-base
```

The workflow performs the index check itself between crates, so a later crate never tries to
resolve a dependency the registry cannot offer yet. `cargo search`, the crates.io web page and the
API answer a few seconds to minutes before `cargo`'s own index refresh sees a new version; confirm
all three versions match the workspace version, then confirm docs.rs built each one.

## Rollback

`cargo yank --version <version> <crate>` (or the crates.io UI) marks a version unusable for new
resolutions. It does not delete anything: the version stays downloadable for existing lock files,
the name stays claimed, and the same version number cannot be published again. Yanking all three
crates is the rollback for a broken release; to ship a fix, publish a new version.

## Not published

`mindcore-spoke` and `mindcore-runtime` stay private — they are integration surfaces for this
repository, not a supported public API. They are also not published by the workflow.

## Documentation

docs.rs builds every published crate automatically from the upload; there is no documentation site
to deploy. A failed docs.rs build shows on the crate's docs.rs page and does not affect the
published version.
