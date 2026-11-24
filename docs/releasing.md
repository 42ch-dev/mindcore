# Releasing to crates.io

A release is one flow, driven by two workflows:

1. Dispatch `new-release.yml` (`workflow_dispatch`): it bumps the workspace
   version, renders the changelog section and opens the `release`-labelled
   bump pull request.
2. Review and merge that pull request with the `release` label.
3. `release.yml` runs on the merge. Its `semver-gate` job first checks the
   merged commit's API diff against the previous release — before any tag
   exists, so a rejected release leaves nothing behind. Its `release` job then
   creates the annotated `v<version>` tag on the merge commit and the GitHub
   Release from that version's `CHANGELOG.md` section; its `publish-crates` job
   then uploads the three public crates to crates.io.

`semver-gate` is read-only (`contents: read`) and checks out the merge commit
without persisted Git credentials: `cargo semver-checks` builds dependencies,
and their build scripts must not run with a token that can write repository
refs.

Its baseline is the newest stable `v*` tag — the previous release actually on
crates.io. Prerelease tags never qualify: a prerelease is Git-only, so comparing
against one would diff against a version the registry never had. With no stable
tag yet (the first release, or a history of prereleases only) the API diff is
skipped.

There is no manual publish step: publishing is the last job of the release
workflow and starts by itself once the release job succeeded. Any SemVer
prerelease — a version with a `-` suffix, such as `v0.2.0-beta.1` or
`v0.2.0-alpha.1` — skips `semver-gate` and stops before `publish-crates`, so a
prerelease never reaches the registry and never claims a version there. It is
not gated because it is not uploaded: there is no registry version to keep
compatible. A stable version is always gated.

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
answers `200`, publishes with `cargo publish --locked`, retries transient registry responses
(502/503/504, timeouts) with backoff, and treats "already exists on crates.io" as success. Re-running
the failed job therefore resumes from the first crate that is still missing — a version can never be
uploaded twice.

After an upload the script waits until the version is listed in the sparse index
(`https://index.crates.io/<path>`) before the next step runs, up to 30 attempts ten seconds apart.
That is the view `cargo` resolves through: the crates.io API answers earlier, so the API check alone
would let `mindcore-spi` start while `mindcore-base` is still unresolvable. A version already listed
there passes on the first check, so a resumed job does not wait.

The script does not run the SemVer check: `semver-gate` runs
`node tooling/semver/check.mjs --blocking` on the merge commit before `release`
creates the tag. `--blocking` is required because HEAD is not yet a tag commit
there, and checking before the tag means a rejected release leaves no tag behind
— the next release then still diffs against the last version that actually
shipped.

## Breaking changes

Land a breaking API change in ordinary pull requests — do not bump the
workspace version outside the release PR. Off a tag commit
`tooling/semver/check.mjs` reports the `cargo semver-checks` diff as
non-blocking, so the PR merges; the release PR's minor bump is what makes the
change releasable, and the `semver-gate` job then runs the same check with
`--blocking` on the merged commit, verifying the new version against the
previous release before the tag exists.

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
- The maintainer's own crates.io token is available for the one local alpha publish (see below);
  the repository stores no registry secret.

## First publish and Trusted Publishing

The three crate names are claimed once by a local publish from the maintainer's machine:

```bash
cargo login                                   # the maintainer's own token; never committed
node tooling/release/bump-version.mjs 0.1.0-alpha.1
cargo publish -p mindcore-base --locked
cargo publish -p mindcore-spi --locked
cargo publish -p mindcore-sqlite --locked
node tooling/release/bump-version.mjs 0.1.0
```

The alpha version is chosen explicitly and never goes through `release.yml`: the workflow uploads
stable versions only, and a prerelease is Git-only. Claiming the names with an alpha keeps the
first stable version free for the normal release flow.

Then, for each crate on crates.io, add this repository and `.github/workflows/release.yml` as a
trusted publisher. From that point `publish-crates` mints a short-lived OIDC token
(`rust-lang/crates-io-auth-action`); the repository stores no registry secret and no secret has to
be rotated.

The verification build runs with `CARGO_REGISTRY_TOKEN` removed (`env -u`), and only the upload
step receives the minted token, so a dependency's build script cannot read it.

`publish-crates` has `contents: read` plus `id-token: write`; those are the only permissions the job
needs.

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
