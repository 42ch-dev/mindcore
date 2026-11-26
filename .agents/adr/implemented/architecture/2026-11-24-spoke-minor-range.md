# ADR: SPOKE crates declared at one minor line

Status: implemented — exact `=0.18.1` pins blocked downstream patch resolution, and a wider range would admit unreviewed breaking changes; the line is now the `0.19` one (`>=0.19.1, <0.20.0`), moved with the cohort the create operation requires
Date: 2026-11-24

## Problem

`spoke-schemas`, `spoke-operations` and `spoke-connect` were pinned with exact `=x.y.z` requirements. Under the 0.x SemVer convention every spoke patch is potentially breaking for a resolver, but the exact pins went further: a downstream consumer that also depended on the spoke crates could never resolve `0.18.2` against mindcore's `=0.18.1`, so every spoke patch — including fixes — forced a mindcore release just to move the pin. The pins made `Cargo.lock` redundant as the compatibility gate and turned a patch-level fix into a coordinated release of two repositories.

## Decision

The three spoke dependencies are declared at one minor line in the workspace `Cargo.toml`, never at an exact version: `0.19.1` today, which is the range `>=0.19.1, <0.20.0`. The line was `0.18.1` (`>=0.18.1, <0.19.0`) until the cohort moved with `2026-11-25-create-default-grant-set.md`'s protocol entry point. `Cargo.lock` remains the build pin: it fixes the exact `x.y.z` version every checkout, CI run and publish verification builds against, and it moves only when someone deliberately updates it. The dependency-follow rule is unchanged: a spoke minor bump forces a mindcore minor bump; a spoke patch bump may ship as a mindcore patch.

## Alternatives considered

- **Keep the exact `=0.18.1` pin.** Rejected: it is the state this decision replaces. Downstream lockfiles cannot satisfy two different exact requirements on the same crate, so every spoke patch — however safe — was unresolvable until mindcore re-released with a matching pin.
- **Declare a wider `>=0.18` (no upper bound).** Rejected: a spoke `0.19` is a minor bump and therefore breaking under the 0.x convention; an open range would let `cargo update` pull it in without a mindcore minor bump and without review.
- **Vendored or `git` dependency on a spoke commit.** Rejected: crates.io refuses a publishable manifest whose dependency has no registry version, and a git edge ties releases to commits the registry cannot resolve.

## Consequences

- `cargo update -p spoke-schemas` can now move the lockfile to a new `0.19.x` patch without a Cargo.toml edit; that update is what ships as a mindcore patch. A `0.20` line still requires editing the requirement and is therefore always a mindcore minor.
- Because the manifest admits a range, the lockfile is the only record of which `0.19.x` a build used; `--locked` builds and the committed `Cargo.lock` carry the pinning duty the manifest gave up.
- Published crates resolve the range at install time: a consumer building `mindcore-sqlite` gets the newest `0.19.x` the registry offers, never below `0.19.1` — the release that pins `memory.spaces.create`'s scoped filter coverage and its created-space profile policy — and not necessarily the version CI tested, which is the accepted cost of letting patch fixes reach downstream without a mindcore release.
