# Releasing to crates.io

A release is one flow, driven by two workflows:

1. Dispatch `new-release.yml` (`workflow_dispatch`): it bumps the workspace
   version, renders the changelog section and opens the `release`-labelled
   bump pull request.
2. Review and merge that pull request with the `release` label.
3. `release.yml` runs on the merge. Its `release` job creates the annotated
   `v<version>` tag on the merge commit and the GitHub Release from that
   version's `CHANGELOG.md` section; its `publish-crates` job then uploads the
   three public crates to crates.io.

There is no manual publish step: publishing is the last job of the release
workflow and starts by itself once the release job succeeded. Prerelease tags
(any `v<version>-...`, e.g. `-rc.1`) stop before `publish-crates`, so a
prerelease never reaches the registry.

## Publish order

```
mindcore-base → mindcore-spi → mindcore-sqlite
```

crates.io resolves a dependency only after it is already published, and the three crates depend on
each other through internal path edges:

- `mindcore-base` has no internal dependency.
- `mindcore-spi` depends on `mindcore-base`.
- `mindcore-sqlite` depends on `mindcore-base`.

`publish-crates` publishes in that order, one `cargo publish` per step, and stops on the first
failure: a crate that cannot be published leaves the later crates unpublished rather than half
released.

Each step is idempotent (`tooling/release/publish-crate.sh`): it reads the workspace version from
`cargo metadata`, skips the crate when `https://crates.io/api/v1/crates/<crate>/<version>` already
answers `200`, retries transient registry responses (502/503/504, timeouts) with backoff, and treats
"already exists on crates.io" as success. Re-running the failed job therefore resumes from the first
crate that is still missing — a version can never be uploaded twice.

That check reads the crates.io API, which can answer before `cargo`'s own index view catches up. If
the next crate still cannot resolve the one just uploaded, the job fails there and a re-run
continues from that crate.

## Breaking changes

Land a breaking API change in ordinary pull requests — do not bump the
workspace version outside the release PR. Off a tag commit
`tooling/semver/check.mjs` reports the `cargo semver-checks` diff as
non-blocking, so the PR merges; the release PR's minor bump is what makes the
change releasable, and on the tag commit the same check turns blocking and
verifies the new version against the previous release.

## Preconditions

Check these before the first release; each one makes `cargo publish` fail rather than publish
something wrong:

- Each of the three crates is publishable: `publish` is unset or lists `crates-io` (not `publish =
  false`).
- Every internal dependency edge carries a `version` next to its `path`. The registry ignores
  `path`, so a bare `{ path = "../mindcore-base" }` publishes a manifest nobody can resolve.
- `mindcore-sqlite` ships its `sqlx` offline cache: `crates/mindcore-sqlite/.sqlx` is a symlink to
  the workspace `.sqlx`, and the crate's `include` list carries `.sqlx/*.json`. The crate uses
  compile-time `sqlx::query!` macros, so the packaged crate only builds when that cache is inside
  it. A symlink that is missing after clone means the publish fails during Cargo's verification
  build, not at the registry.
- The workspace version in `Cargo.toml` is the version `release.yml` tagged: the script reads that
  version, so the tag and the registry have to agree.
- The `CARGO_REGISTRY_TOKEN` secret exists (see below).

## Token

`CARGO_REGISTRY_TOKEN` is a repository secret holding a crates.io API token. It must be configured
before the first release: the workflow fails at the first `cargo publish` without it.

The token must belong to an owner of all three crate names, because the first publish claims each
name permanently. Crate ownership on crates.io is per crate, so verify all three names appear under
the same account before the first release.

`publish-crates` has `contents: read`; the only credential the job uses is that token.

The secret is needed because the crate names do not exist on crates.io yet, so Trusted Publishing
cannot be configured in advance. Once the first release has claimed the three names, crates.io
Trusted Publishing (OIDC, no long-lived secret) can replace `CARGO_REGISTRY_TOKEN`: register
`42ch-dev/mindcore` and `.github/workflows/release.yml` as the trusted publisher for each crate,
then swap the token for `rust-lang/crates-io-auth-action` in the `publish-crates` job.

## Verify

For each crate, in order:

```bash
cargo search mindcore-base
# the sparse index is what cargo resolves, and it shows a version as soon as it is usable:
curl -fsS -H 'User-Agent: mindcore-release-check' https://index.crates.io/mi/nd/mindcore-base
```

`cargo search`, the crates.io web page and the API can answer a few seconds before `cargo`'s own
index refresh sees a new version; confirm all three versions match the workspace version, then
confirm docs.rs built each one.

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
