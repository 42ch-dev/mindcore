# Migrating from `tools.mindcore.adopt`

Adoption is served only as the published protocol operation `knowledge.adopt`, under the
`spoke-knowledge-adoption` capability, and requires `spoke-knowledge`. The private product tool
`tools.mindcore.adopt` is removed: the runtime drops that id from every host registration, so no
manifest advertises it and no handler answers it.

## Field mapping

A caller moves as follows.

| Product tool | Protocol operation |
| --- | --- |
| `entry_id` | `source_entry_id` |
| positive integer `expected_source_revision` | positive integer `source_revision` |
| — | `source_scope_id`, required: it must equal `ServeFaces.adoption`'s `AdoptionRoute.scope_id` |
| — | `destination_scope_id`, required: the admitted Mind's scope id |
| — | `idempotency_key`, required: 1–128 code points |
| — | `destination_entry_id`, optional: absent, the copy takes the resolved terminal's entry id |
| `outcome` `created` | `outcome` `adopted` |
| `outcome` `already_present`, `entry` | unchanged |

## The destination moved

The destination is no longer per-principal. `knowledge.adopt` writes the copy into the knowledge
collection of the Mind the call is admitted against — the same collection that Mind's
`knowledge.get`/`list` read — not into a collection selected for the adopting principal. A copy is
therefore readable through the same `knowledge.get`/`list` that admitted the adoption.

No host field and no wire member picks that collection: `destination_scope_id` selects the Mind,
and the Mind's registry row supplies the collection it serves.

The slot rules are per collection: it holds one slot per `destination_entry_id`, and the first
adoption of a given id claims it. A repeat answers `already_present` with the stored copy only
when its anchor names the same source revision. A copy adopted at another revision is
`REVISION_CONFLICT` with `details.adopted_revision`, and a peer whose admitted selection hides the
copy is answered the occupancy refusal, not the copy.

Adoption consults no workflow-facing setting: it needs one live grant in that Mind's space,
covering both `knowledge_write` and `knowledge_read`. A principal holding that grant adopts
normally.

## Breaking assembly change

A host that pointed the adoption source at a collection a Mind serves used to assemble, because
the copy went to a collection selected per principal. Assembly now refuses that configuration
(`InvalidInput`, "the adoption source <collection> is a registered Mind's knowledge collection"),
and refuses a source scope id equal to a registered Mind's id the same way. Every registered Mind
is a possible destination, so the collision is refused whatever Mind holds it; the port repeats
both checks per call, because a Mind created after assembly can still take a colliding id or
collection.

Migrate by:

1. naming a distinct adoption source collection — it must differ from every registered Mind's
   knowledge collection, and its scope id from every registered Mind's id, and
2. no longer expecting any configuration to name an adoption destination — the admitted Mind's
   registry row is that destination.

## Two behaviour changes

- **Knowledge adoption requires the knowledge grant as well.** Admission runs the Mind's admission
  and then the operation's own grant, so a peer the knowledge face does not admit — or one whose
  live grant there does not cover both `knowledge_write` and `knowledge_read` — answers
  `NOT_AUTHORIZED` where the product tool served it. That includes a replayed adoption: the grant
  is checked at admission, before Connect answers the recorded outcome.
- **The source resolves through its retirement chain before the copy is written.** An adopted
  entry's anchor names the live terminal rather than a retired hop. A chain that cannot resolve —
  a cycle, an ambiguous merge, a missing hop, more than 32 edges — answers `SOURCE_RETIRED` with
  `details.retirement_chain`.

## Refusal vocabulary

The local refusals are the protocol's:

| Refusal | Meaning |
| --- | --- |
| `NOT_FOUND` | missing, private or out-of-scope source |
| `REVISION_CONFLICT` | a copy already adopted at another revision (`details.adopted_revision`) |
| `NOT_AUTHORIZED` | another destination scope |
| `ADOPTION_ANCHOR_IMMUTABLE` | a put that would detach a stored anchor |

The contract's `owner`/`disclosure` reset is not applied: a copy inherits the source's governance.

## Storage compatibility

The first upload to crates.io fixed the migration set: migration files are immutable from then on,
and new schema arrives only as appended files. The set is locked in
`tooling/migrations/migrations.lock.json`, written in the release PR and re-checked on the tagged
commit. A database whose applied migrations still match the embedded set reopens normally and keeps
its records; a pre-release checkout whose `0001_init.sql` differs from the locked file is refused.
[Storage compatibility](storage-compatibility.md) is the full contract for what `open` accepts and
refuses.

`MindStore::open` runs the embedded migrator and compares it with the rows recorded in
`_sqlx_migrations`. The embedded set is `0001_init.sql` and `0002_minds.sql`; the lock still lists
`0001` alone, so `0002` is pending rather than published. Each recorded state resolves as follows:

- **At version 1** — the published set: accepted, and `0002_minds.sql` is applied on top.
- **At version 2, recorded by an earlier `0002`** — the version is embedded, so the differing
  checksum is refused with SQLx's `VersionMismatch(2)`, not `VersionMissing`. A pre-release
  checkout whose `0001_init.sql` changed hits `VersionMismatch(1)` the same way.
- **Above the embedded maximum** — a recorded version the embedded set does not contain: refused
  with SQLx's `VersionMissing(<version>)`.
- **A pending file that reruns over tables already present** — refused with `ExecuteMigration`.

Recreate the file only for one of those migration mismatches, or move it aside first if you want to
keep it. A busy or locked database, an I/O error or a permissions problem is not a migration
mismatch: fix the cause and retry `open` instead of recreating the file.
