# For agents

You are working **in** this repository, not embedding the library. This page is the operational
entry: layout, checks, and documentation conventions. User-facing usage lives in
[Quick start](quickstart.md), [Concepts](concepts.md) and [Serving a surface](serving.md).

## Repository map

| Path | Contents |
| --- | --- |
| `crates/mindcore-base` | Domain types, IR, cognition derivation; no I/O |
| `crates/mindcore-spi` | Workflow contracts and provider seams |
| `crates/mindcore-sqlite` | SQLite store, scoped ports, embedded migration |
| `crates/mindcore-spoke` | SPOKE memory protocol adapter |
| `crates/mindcore-runtime` | Connect surface assembly |
| `.agents/specs/` | Normative architecture and data contracts |
| `.agents/adr/` | Decision records, one decision per file |
| `.agents/knowledge/` | Reusable implementation patterns |
| `fixtures/` | Versioned wire fixtures used by tests |
| `.mstar/` | Local harness state; gitignored |

Process artifacts stay under `.mstar/`; results under `.agents/` are tracked and shared.

## Checks

Requires a stable Rust toolchain, `sqlite3`, and `sqlx-cli` at the release of the `sqlx` crate the
workspace resolves (CI installs that version).

```sh
(  # the subshell keeps `set -e` from leaking into the caller's shell
set -e  # stop at the first failing step, as a CI job does

cargo fmt --all -- --check

# The offline query cache must match the embedded migration and query surface.
fixture_dir="$(mktemp -d)"
trap 'rm -rf "$fixture_dir"' EXIT
sqlite3 -bail "$fixture_dir/settlement.db" < crates/mindcore-sqlite/migrations/0001_init.sql
SQLX_OFFLINE=false DATABASE_URL="sqlite://$fixture_dir/settlement.db" \
  cargo sqlx prepare --workspace -- --locked --workspace --all-targets
if [ -n "$(git status --porcelain --untracked-files=all -- .sqlx/)" ]; then
  echo ".sqlx offline cache is stale"
  exit 1
fi

SQLX_OFFLINE=true cargo clippy --locked --workspace --all-targets -- -D warnings
SQLX_OFFLINE=true cargo test --locked --workspace
)
```

CI runs these same steps (`.github/workflows/ci.yml`) after installing `sqlx-cli` at that `sqlx`
release, and fails on any `.sqlx/` drift. `set -e` is what makes the local run match it: a failed
`sqlx prepare` stops the block before the drift test, which can only report a fresh cache when
`prepare` itself succeeded.

To watch the story end to end:

```sh
cargo run --locked -p mindcore-sqlite --example iteration_01
```

The delivered contract is documented in `.agents/specs/architecture/mind-model.md`.

## Documentation conventions

- `README.md` is English; `README.zh-CN.md` is its Chinese translation. Change both in the same
  change and keep the language switcher links intact.
- Everything under `docs/` is English and follows Diátaxis: tutorials, how-to guides, reference
  and explanation. Do not duplicate the specs; link to them.
- `docs/assets/architecture.svg` is the README diagram. Its Archify candidate is
  `docs/assets/architecture.archify.json`. Run Archify `finalize` to an untracked
  `docs/assets/architecture.html`, then export the light/dark SVG from the viewer; only the source
  JSON and the SVG are tracked.
- Do not restate `CONTRIBUTING.md` or `CHANGELOG.md` in the README.

## Decisions and specs

A change that picks one option among real alternatives — a type boundary, a persistence choice, a
dependency — records an ADR under `.agents/adr/`. Changing the schema, a write path or the carrier
shape carries the obligations listed in `.agents/specs/architecture/mind-model.md` § Change
obligations.

## Harness

Morning Star owns this repository's process state. Only `project-manager` sets a plan `Done`;
implementers set `InReview`. Plan, workflow and iteration state resolve through
`mstar path resolve`, never through a path composed from the current worktree.
