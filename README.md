# mindcore

Mindcore is a settlement layer for memory, knowledge and cognition: one durable place to settle
what an agent heard, what it adopted as personal knowledge, and what it currently judges to be
true — with the provenance that keeps those three apart.

The first milestone is implemented: an embedded SQLite store persists memories, personal and
shared `KnowledgeEntry` records with conditional revisions, and the cognition carrier from which
`MindState` is derived. No server or UI process ships — the store is a library you embed, and
`mindcore-runtime` only composes the served surface for a host to expose over its own transport.

## Crates

| Crate | Responsibility |
| --- | --- |
| `mindcore-base` | Storage-agnostic domain core: ids and trusted boundary, memory protocol types and IR, knowledge scope and adoption provenance, the cognition carrier and `MindState` derivation. No I/O. |
| `mindcore-spi` | The MIND ladder contracts, pinned to the published SPOKE 0.17.0 DTOs: recall, absorb and deliberate workflows plus the prompt-reasoner bridge. No storage crate, network or model SDK in the contract surface. |
| `mindcore-sqlite` | SQLite (SQLx) backend: one connection pool, one migration, WAL and a busy timeout; implements the protocol repository behind `MemoryAdapter` and the scoped `KnowledgeEntryPort`, and guards the cognition module at the single persistence boundary. |
| `mindcore-spoke` | SPOKE 0.17.0 memory protocol adapter over the borrowed-transaction repository: wire validation, the wire↔IR boundary, reject mapping, admission, replay and staging. It never opens, commits or rolls back a transaction. |
| `mindcore-runtime` | The served surface: `MindcoreServe` assembles the workflow engine, the SQLite knowledge face and the host's memory face into the pieces a Connect responder is built from, and derives the advertised capabilities and tools from what it serves. It opens no listener, transport or identity; the host supplies those. |

## Storage compatibility

Pre-release migrations are mutable: the embedded set in `crates/mindcore-sqlite/migrations/` is one
initial file, edited in place between commits, and nothing is published yet. There is no upgrade
path until the first release: a database whose applied migrations still match the embedded file
reopens normally and keeps its records. `MindStore::open` runs the embedded migrator and compares it
with the rows recorded in `_sqlx_migrations`; the two migration mismatches it reports are:

- A database that applied the removed `0002` is refused with SQLx's `VersionMissing(2)`, because the
  embedded set no longer contains that version.
- A database whose applied version is still embedded but whose SQL has since changed is refused with
  a migration checksum mismatch.

Recreate the file only for one of those two migration mismatches, or move it aside first if you want
to keep it. A busy or locked database, an I/O error or a permissions problem is not a migration
mismatch: fix the cause and retry `open` instead of recreating the file.

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
`prepare` itself succeeded. To watch the story end to end:

```sh
cargo run --locked -p mindcore-sqlite --example iteration_01
```

The delivered contract is documented in `.agents/specs/architecture/mind-model.md`.

## Not yet available

- A server process or transport of our own: `mindcore-runtime` composes the surface, a host serves it.
- Migration of an existing store or provider (live data movement).

Neither has a roadmap owner or an issue in this repository yet, and neither is a stub here.
