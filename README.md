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
| `mindcore-base` | Storage-agnostic domain core: ids and trusted boundary, memory types and the `MemoryStore` trait, knowledge scope and adoption provenance, the cognition carrier and `MindState` derivation. No I/O. |
| `mindcore-sqlite` | SQLite (SQLx) backend: one connection pool, one migration, WAL and a busy timeout; implements `MemoryStore` and the scoped `KnowledgeEntryPort`, and guards the cognition module at the single persistence boundary. |

## Storage compatibility

Pre-release migrations are mutable: the schema in `crates/mindcore-sqlite/migrations/` is edited in
place between commits, and nothing is published yet. There is no upgrade path until the first
release.

Whether an existing database file still opens depends on the migrations it already applied, not on
the commit that created it. `MindStore::open` runs the embedded migrations and compares them with the
rows recorded in `_sqlx_migrations`:

- A database whose applied migrations still match the embedded files byte for byte opens normally,
  including one created by an earlier commit. Cross-commit compatibility is unsupported and
  untested: a database from an earlier commit has not been validated against the current schema.
- A database that applied a migration whose file has since changed is refused with a migration
  checksum mismatch. Recreate the file, or move it aside first if you want to keep it.

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
