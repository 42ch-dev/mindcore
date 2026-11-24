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
| — | `destination_scope_id`, required: it must equal the peer's admitted scope id |
| — | `idempotency_key`, required: 1–128 code points |
| — | `destination_entry_id`, optional: absent, the copy takes the resolved terminal's entry id |
| `outcome` `created` | `outcome` `adopted` |
| `outcome` `already_present`, `entry` | unchanged |

## The destination moved

The destination is no longer per-principal. `knowledge.adopt` writes the copy into the collection
the knowledge face serves (`ServeFaces.knowledge`), not the adopting principal's `workflow_routes`
entry. A copy is therefore readable through the same `knowledge.get`/`list` that admitted the
adoption.

That collection is shared: it holds one slot per `destination_entry_id`, and the first adoption of
a given id claims it. A repeat answers `already_present` with the stored copy only when its anchor
names the same source revision. A copy adopted at another revision is `REVISION_CONFLICT` with
`details.adopted_revision`, and a peer whose admitted selection hides the copy is answered the
occupancy refusal, not the copy.

`workflow_routes` now selects and authorizes workflow calls only, and authorizes nothing for
adoption. A principal with knowledge admission and no workflow route adopts normally.

## Breaking assembly change

A host that set the adoption source and `ServeFaces.knowledge` to the same collection used to
assemble, because the copy went to the principal's distinct route collection. Assembly now refuses
that configuration (`InvalidInput`, "the adoption source collection is the collection the
knowledge face serves").

Migrate by:

1. naming a distinct adoption source collection — it must differ from
   `ServeFaces.knowledge.collection` and from every `workflow_routes` destination, and
2. no longer expecting a route to name an adoption destination.

## Two behaviour changes

- **Knowledge admission is now required as well.** Adoption admits through
  `(principals(peer)?, knowledge_admission(peer)?)`, so a routed principal the knowledge face does
  not admit answers `NOT_AUTHORIZED` where the product tool served it.
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

Pre-release migrations are mutable: the embedded set in `crates/mindcore-sqlite/migrations/` is one
initial file, edited in place between commits, and nothing is published yet. There is no upgrade
path until the first release. A database whose applied migrations still match the embedded file
reopens normally and keeps its records.

`MindStore::open` runs the embedded migrator and compares it with the rows recorded in
`_sqlx_migrations`. The two migration mismatches it reports are:

- A database that applied the removed `0002` is refused with SQLx's `VersionMissing(2)`, because
  the embedded set no longer contains that version.
- A database whose applied version is still embedded but whose SQL has since changed is refused
  with a migration checksum mismatch.

Recreate the file only for one of those two migration mismatches, or move it aside first if you
want to keep it. A busy or locked database, an I/O error or a permissions problem is not a
migration mismatch: fix the cause and retry `open` instead of recreating the file.
