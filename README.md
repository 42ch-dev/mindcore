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
| `mindcore-spi` | The MIND ladder contracts, pinned to the published SPOKE 0.18.0 DTOs: recall, absorb and deliberate workflows plus the prompt-reasoner bridge. No storage crate, network or model SDK in the contract surface. |
| `mindcore-sqlite` | SQLite (SQLx) backend: one connection pool, one migration, WAL and a busy timeout; implements the protocol repository behind `MemoryAdapter` and the scoped `KnowledgeEntryPort`, and guards the cognition module at the single persistence boundary. |
| `mindcore-spoke` | SPOKE 0.18.0 memory protocol adapter over the borrowed-transaction repository: wire validation, the wire↔IR boundary, reject mapping, admission, replay and staging. It never opens, commits or rolls back a transaction. |
| `mindcore-runtime` | The served surface: `MindcoreServe` assembles the workflow engine, the SQLite knowledge face and the host's memory face into the pieces a Connect responder is built from, and derives the advertised capabilities and tools from what it serves. It opens no listener, transport or identity; the host supplies those. |

## Migrating from `tools.mindcore.adopt`

Adoption is served only as the published protocol operation `knowledge.adopt`, under the
`spoke-knowledge-adoption` capability, and requires `spoke-knowledge`. The private product tool
`tools.mindcore.adopt` is removed: the runtime drops that id from every host registration, so a
manifest carries it in no configuration and no handler answers it. A caller moves as follows.

| Product tool | Protocol operation |
| --- | --- |
| `entry_id` | `source_entry_id` |
| positive integer `expected_source_revision` | positive integer `source_revision` |
| — | `source_scope_id`, required: it must equal `ServeFaces.adoption`'s `AdoptionRoute.scope_id` |
| — | `destination_scope_id`, required: it must equal the peer's admitted scope id |
| — | `idempotency_key`, required: 1–128 code points |
| — | `destination_entry_id`, optional: absent, the copy takes the resolved terminal's entry id |
| `outcome` `created` | `outcome` `adopted` |
| `outcome` `already_present`, `entry` | unchanged |

The destination is no longer per-principal. `knowledge.adopt` writes the copy into the collection
the knowledge face serves (`ServeFaces.knowledge`), not the adopting principal's `workflow_routes`
entry, so a copy is readable through the same `knowledge.get`/`list` that admitted the adoption.
`workflow_routes` now selects and authorizes workflow calls only, and authorizes nothing for
adoption; a principal with knowledge admission and no workflow route adopts normally.

That destination move is a breaking assembly change. A host that set the adoption source and
`ServeFaces.knowledge` to the same collection still assembled, because the copy went to the
principal's distinct route collection; assembly now refuses that configuration (`InvalidInput`, "the
adoption source collection is the collection the knowledge face serves"). Migrate by naming a
distinct adoption source collection — it must differ from `ServeFaces.knowledge.collection` and from
every `workflow_routes` destination — and by no longer expecting a route to name an adoption
destination.

Two behaviour changes come with the move. The knowledge-face admission is now required as well:
adoption admits through `(principals(peer)?, knowledge_admission(peer)?)`, so a routed principal the
knowledge face does not admit answers `NOT_AUTHORIZED` where the product tool served it. And the
source resolves through its retirement chain before the copy is written, so an adopted entry's
anchor names the live terminal rather than a retired hop, and a chain that cannot resolve —
a cycle, an ambiguous merge, a missing hop, more than 32 edges — answers `SOURCE_RETIRED` with
`details.retirement_chain`. The local refusal vocabulary is the protocol's: `NOT_FOUND` for a
missing, private or out-of-scope source, `REVISION_CONFLICT` with `details.adopted_revision` for a
copy adopted at another revision, `NOT_AUTHORIZED` for another destination scope, and
`ADOPTION_ANCHOR_IMMUTABLE` for a put that would detach a stored anchor. The contract's
`owner`/`disclosure` reset is not applied: a copy inherits the source's governance.

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
