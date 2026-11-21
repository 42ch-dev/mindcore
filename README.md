# mindcore

Mindcore is a settlement layer for memory, knowledge and cognition: one durable place to settle
what an agent heard, what it adopted as personal knowledge, and what it currently judges to be
true — with the provenance that keeps those three apart.

The first milestone is implemented: an embedded SQLite store persists memories, personal and
shared `KnowledgeEntry` records with conditional revisions, and the cognition carrier from which
`MindState` is derived. There is still no server, no UI and no network surface — the store is a
library you embed.

## Crates

| Crate | Responsibility |
| --- | --- |
| `mindcore-base` | Storage-agnostic domain core: ids and trusted boundary, memory protocol types and IR, knowledge scope and adoption provenance, the cognition carrier and `MindState` derivation. No I/O. |
| `mindcore-sqlite` | SQLite (SQLx) backend: one connection pool, one migration, WAL and a busy timeout; implements the protocol repository behind `MemoryAdapter` and the scoped `KnowledgeEntryPort`, and guards the cognition module at the single persistence boundary. |

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

Requires a stable Rust toolchain.

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
```

CI runs these same three commands (`.github/workflows/ci.yml`). To watch the story end to end:

```sh
cargo run --locked -p mindcore-sqlite --example iteration_01
```

The delivered contract is documented in `.agents/specs/architecture/mind-model.md`.

## Not yet available

- Remote serving and capability advertisement.
- Memory transfer between minds.
- Migration of an existing store or provider (live data movement).

Each has an owner in the project roadmap; none is a stub here.
