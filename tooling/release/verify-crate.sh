#!/usr/bin/env bash
# Verification build for one workspace package — `cargo publish --dry-run`.
#
# Dependency build scripts run during this build, so it must run where no
# publish credential exists: the release workflow gives it its own job with
# `contents: read` and no `id-token`. The upload is publish-crate.sh in a
# separate credential-bearing job with `--no-verify`.
#
# Usage (repo root; no token needed):
#   ./tooling/release/verify-crate.sh mindcore-base
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <package-name>" >&2
  exit 2
fi

cargo publish -p "$1" --locked --dry-run
