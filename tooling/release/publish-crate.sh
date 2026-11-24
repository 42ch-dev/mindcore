#!/usr/bin/env bash
# Idempotent crates.io publish for one workspace package.
#
# - Skips when <crate>@<workspace-version> is already on crates.io (safe re-run
#   after a partial publish-crates job).
# - Publishes with `--locked`, as every other Cargo command in this repository.
# - Verifies with a `--dry-run` build that has no CARGO_REGISTRY_TOKEN, then
#   uploads with `--no-verify`: a dependency's build script runs the build, so
#   the token must not be present while it does.
# - Retries transient registry responses (502/503/504) at a fixed interval.
# - Treats the registry's duplicate-version answer as success (publish race):
#   cargo's `is already uploaded` and the index's `already exists on crates.io`.
# - Waits for the version to appear in the sparse index after an upload: the
#   next crate resolves it there, and the crates.io API answers earlier.
#
# Usage (repo root, CARGO_REGISTRY_TOKEN set for the upload):
#   ./tooling/release/publish-crate.sh mindcore-base
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <package-name>" >&2
  exit 2
fi

PKG="$1"
MAX_ATTEMPTS="${CARGO_PUBLISH_MAX_ATTEMPTS:-5}"
SLEEP_SECS="${CARGO_PUBLISH_RETRY_SLEEP_SECS:-30}"
INDEX_ATTEMPTS="${CARGO_INDEX_WAIT_ATTEMPTS:-30}"
INDEX_SLEEP_SECS="${CARGO_INDEX_WAIT_SLEEP_SECS:-10}"
USER_AGENT="${CARGO_PUBLISH_USER_AGENT:-mindcore-release/0.1 (github.com/42ch-dev/mindcore)}"

# `set -e` would abort the script on a failed probe before the guard below runs,
# so the substitution is allowed to fail and the guard reports it.
set +e
VERSION="$(
  cargo metadata --no-deps --format-version 1 | python3 -c "
import json, sys
meta = json.load(sys.stdin)
name = sys.argv[1]
for p in meta['packages']:
    if p['name'] == name and p.get('source') is None:
        print(p['version'])
        break
else:
    raise SystemExit(f'workspace package not found: {name}')
" "${PKG}"
)"
set -e
if [[ -z "${VERSION}" ]]; then
  echo "failed to read workspace version for ${PKG}" >&2
  exit 1
fi

crates_io_has_version() {
  local code
  code="$(
    curl -sS --max-time 10 -o /dev/null -w '%{http_code}' -A "${USER_AGENT}" \
      "https://crates.io/api/v1/crates/${PKG}/${VERSION}" || true
  )"
  [[ "${code}" == "200" ]]
}

# Sparse index layout: 1/<name>, 2/<name>, 3/<c>/<name>, <ab>/<cd>/<name>.
index_url() {
  local name="$1" path
  case "${#name}" in
    1) path="1/${name}" ;;
    2) path="2/${name}" ;;
    3) path="3/${name:0:1}/${name}" ;;
    *) path="${name:0:2}/${name:2:2}/${name}" ;;
  esac
  printf 'https://index.crates.io/%s' "${path}"
}

index_has_version() {
  local body
  # A missing crate answers 404 with an empty body, which simply has no match.
  body="$(curl -fsS --max-time 10 -A "${USER_AGENT}" "$(index_url "${PKG}")" 2>/dev/null || true)"
  grep -qF -- "\"vers\":\"${VERSION}\"" <<< "${body}"
}

wait_for_index() {
  local attempt
  for (( attempt = 1; attempt <= INDEX_ATTEMPTS; attempt++ )); do
    if index_has_version; then
      echo "${PKG}@${VERSION} is in the sparse index"
      return 0
    fi
    if (( attempt < INDEX_ATTEMPTS )); then
      echo "waiting for ${PKG}@${VERSION} in the sparse index (attempt ${attempt}/${INDEX_ATTEMPTS})"
      sleep "${INDEX_SLEEP_SECS}"
    fi
  done
  echo "error: ${PKG}@${VERSION} did not reach the sparse index after ${INDEX_ATTEMPTS} attempts" >&2
  return 1
}

if crates_io_has_version; then
  echo "${PKG}@${VERSION} already on crates.io — skip"
  # A resumed job still needs the index ready for the crate above this one; a
  # version already listed there returns on the first check.
  wait_for_index
  exit 0
fi

# Verify before the token exists in the environment: a dependency's build script
# runs during this build and could read CARGO_REGISTRY_TOKEN out of it.
echo "verifying ${PKG} without CARGO_REGISTRY_TOKEN"
set +e
verify_out="$(env -u CARGO_REGISTRY_TOKEN cargo publish -p "${PKG}" --locked --dry-run 2>&1)"
verify_status=$?
set -e
printf '%s\n' "${verify_out}"
if [[ "${verify_status}" -ne 0 ]]; then
  echo "error: verification build of ${PKG} failed" >&2
  exit "${verify_status}"
fi

attempt=1
while true; do
  echo "cargo publish -p ${PKG} --locked --no-verify (attempt ${attempt}/${MAX_ATTEMPTS})"
  set +e
  out="$(cargo publish -p "${PKG}" --locked --no-verify 2>&1)"
  status=$?
  set -e
  printf '%s\n' "${out}"

  if [[ "${status}" -eq 0 ]]; then
    wait_for_index
    exit 0
  fi

  # Cargo answers a duplicate version with `crate version ... is already uploaded`;
  # a racing client can also surface the index's `already exists on crates.io index`.
  if printf '%s\n' "${out}" | grep -qE 'is already uploaded|already exists on crates\.io'; then
    echo "${PKG}@${VERSION} already on crates.io (publish race) — ok"
    wait_for_index
    exit 0
  fi

  if printf '%s\n' "${out}" | grep -Eqi 'got 502|got 503|got 504|timed out|timeout|temporarily unavailable'; then
    if (( attempt >= MAX_ATTEMPTS )); then
      echo "exhausted ${MAX_ATTEMPTS} attempts after transient registry failure" >&2
      exit "${status}"
    fi
    echo "transient registry failure; sleeping ${SLEEP_SECS}s before retry"
    sleep "${SLEEP_SECS}"
    attempt=$((attempt + 1))
    continue
  fi

  exit "${status}"
done
