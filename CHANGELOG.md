# Changelog

## [0.1.0] - 2026-11-24

- Dependencies: the SPOKE cohort is pinned to `=0.18.1` — `spoke-schemas` and `spoke-operations` in
  `[workspace.dependencies]`, and `spoke-connect` there too, so the SQLite test dependency inherits
  the same pin with `remote-adapter` enabled. The published 0.18.1 wire changes ride along: shared
  strict RFC 3339 parsing (`spoke_schemas::timestamp`), the inclusive `2^53` revision ceiling on the
  generated DTOs (`spoke_schemas::integers`), and RFC 8785 canonicalization that re-spells a
  revision-typed `±2^53` endpoint so Connect signatures agree with the TypeScript stack.
  `mindcore-base`'s IR decode accepts the pre-0.18.1 space spelling on stored reads — new input is
  refused by the DTO gate — and the transfer digests re-spell the `±2^53` endpoint, so a chunk a
  peer signed at the inclusive revision bound can still be sealed. Decision: `ADR 2026-11-24
  stored wire compatibility`.

- **`mindcore-runtime` / `mindcore-sqlite`:** `history.revisions.prune` is deduplicated durably. The
  history face composes `SqliteHistoryDedupStore` over the knowledge store's own pool, keyed by
  Connect's retry identity, so a same-key retry after a restart or a reassembled surface replays the
  recorded `{deleted, floor_revision}` instead of executing the prune again and deleting checkpoints
  the original call never saw. `lookup` claims the key before the port runs; a key claimed whose
  outcome was never recorded — a crash after the claim and before `record` — answers
  `RECEIPT_EXPIRED` and never reaches the port. Same-key different-payload still answers
  `IDEMPOTENCY_CONFLICT`, but only when a recorded outcome exists to compare against: a retained
  claim with no outcome answers `RECEIPT_EXPIRED` instead, whatever the new payload is. The record is
  the new `history_dedup` table in `0001_init.sql`, keyed by `(namespace, key)` — the routed store's
  collection and the retry key, in separate columns, because a collection name is arbitrary and no
  separator framing keeps them unambiguous — so two history surfaces on one database that share
  a wire scope, a principal and an idempotency key never replay or refuse each other's prunes; a
  same-collection retry replays exactly as before. Each `(namespace, principal)` pair holds at most
  4096 rows, claims included, and nothing is ever deleted: the table carries no timestamp column and
  `lookup` reads no clock, so a recorded outcome replays for the database's lifetime and a forward
  host-clock jump cannot age a live record into a re-executed prune. The quota is per caller — the
  `principal` column is the third member of the retry identity — so one high-churn principal can
  exhaust only its own and never disable prune for another on the same collection. A principal at
  its quota answers an unseen key with `RECEIPT_EXPIRED` without executing the prune, and `record`
  writes no row past the quota. A host that needs more headroom raises the cap or recreates the
  database. One serving process per database is a host obligation. The adoption face keeps the
  in-memory default.

- **Breaking — `mindcore-runtime`:** the private product tool `tools.mindcore.adopt` is removed, not
  deprecated. Adoption is served only as the published protocol operation `knowledge.adopt` under the
  `spoke-knowledge-adoption` capability, which requires `spoke-knowledge`; the runtime filters the
  reserved id out of every host registration, so no manifest advertises it and no handler answers it.
  A caller migrates the call as `entry_id` → `source_entry_id`, `expected_source_revision` →
  `source_revision`, and expects `outcome` `adopted` instead of `created` (`already_present` and
  `entry` keep their meanings). The request gains required `source_scope_id` — it must equal
  `AdoptionRoute.scope_id`, and any other value is the same generic `NOT_FOUND` as a missing source —
  required `destination_scope_id` — it must equal the peer's admitted scope id, or `NOT_AUTHORIZED`
  before the source is read — required `idempotency_key`, and optional `destination_entry_id`, which
  names the copy and defaults to the resolved terminal's entry id; assembly now also refuses a
  source scope equal to `ServeFaces.scope_id`. The destination is no longer per-principal: the copy
  is written into the collection the knowledge face serves (`ServeFaces.knowledge`), not the
  adopting principal's `workflow_routes` entry, so it is readable through the `knowledge.get`/`list`
  that share the adoption's admission. `workflow_routes` now selects and authorizes only the
  routed workflow calls and plays no part in adoption. This is a breaking assembly change: a host
  whose adoption source collection equals that knowledge collection assembled before — the copy
  went to the principal's distinct route collection — and is now refused with `InvalidInput` ("the
  adoption source collection is the collection the knowledge face serves"). Migrate by naming a
  distinct adoption source collection, different from `ServeFaces.knowledge.collection` and from
  every `workflow_routes` destination, and by dropping any expectation that a route names an
  adoption destination. Two further refusals: the admitted scope's
  `entry_ids`/`entry_types` selection now applies to the destination, so an excluded entry is
  neither served nor written and answers the ordinary occupancy `REVISION_CONFLICT` with no
  details — byte-identical to a visible occupant's, so the answer reports slot occupancy only and
  never the occupying entry's content, revision or visibility — `NOT_AUTHORIZED` staying the answer
  for a `destination_scope_id` that is not the admitted scope; and a resolved terminal revision above
  `2^53-1` refuses before the write instead of committing a copy whose response the wire cannot
  carry. Two behaviour changes: adoption admits through the knowledge face's `(principals,
  knowledge_admission)` pair, so a routed principal that face does not admit
  answers `NOT_AUTHORIZED` where the product tool served it; and the source resolves through
  `resolve_retirement_chain` before the copy, so the anchor names the live terminal (the copy's own
  entry id is the supplied `destination_entry_id` or, when absent, that terminal's id), a
  live source that moved off `source_revision` is `REVISION_CONFLICT` with no details, and a chain
  that cannot resolve is `SOURCE_RETIRED` with `details.retirement_chain`. The local refusal
  vocabulary is the protocol's — `NOT_FOUND`, `NOT_AUTHORIZED`, `REVISION_CONFLICT` with
  `details.adopted_revision` only for a copy adopted at another revision, `SOURCE_RETIRED`,
  `ADOPTION_ANCHOR_IMMUTABLE` and `INVALID_INPUT` — and the `owner`/`disclosure` reset is not
  applied, so a copy inherits the source's governance. Connect's `AdoptionPortService` supplies the
  idempotency envelope, including `IDEMPOTENCY_CONFLICT` and `RECEIPT_EXPIRED`.
- **`mindcore-sqlite` — served history prune answers the one absence.** `history.revisions.prune`
  no longer distinguishes a retired or peer-hidden row from a missing one: all three row-level
  answers are `NOT_FOUND` with no `details`, where a retired or hidden row used to answer
  `NOT_AUTHORIZED`. **Only the row-level denial changed.** A caller that matched `NOT_AUTHORIZED` for
  a prune *row* must match `NOT_FOUND` instead, and must keep its `NOT_AUTHORIZED` handling for
  everything decided before any row is read: scope admission, Connect's own comparison of the
  request's `scope` against the admitted scope, and the port's `admit` guard. An unserved
  `entity_kind` keeps `HISTORY_ENTITY_UNSUPPORTED`. The refusal reuses the absent row's constructor,
  so code, message shape and `details` are equal by construction and a peer admitted to the
  collection can no longer probe membership or retirement through prune.
- `mindcore-spoke` / `mindcore-runtime`: `ServeFaces.memory` takes an `Arc<dyn ServedMemory>` — a
  `RemoteMemoryService` that reports the `memory.*` operations it dispatches — and the runtime
  advertises `spoke-memory-transfer` exactly when the attached service reports all five §7.29–§7.33
  operations. A bare `RemoteMemoryService` gains an empty `impl ServedMemory for … {}` and keeps
  advertising `spoke-memory` only. `mindcore_spoke::TransferService` binds the five transfer
  operations to the host's pool and peer-to-principal admission, owning one transaction per invoke.
- `mindcore-runtime`: `ServeFaces.workflow_caller` is replaced by `workflow_routes`, a map from each
  admitted principal id to the one knowledge collection its workflow calls reach. Each destination
  collection belongs to exactly one principal route; a collection named by two routes is refused at
  assembly. An unrouted principal is refused on `deliberate` and still recalls memory with an empty
  knowledge slot. A consumer that sets `workflow_caller` does not compile and must build the map.
- Initial workspace for `mindcore-base` and `mindcore-sqlite`.
- `mindcore-sqlite`: cognition records now have their own table — one row per record in
  `cognition_records`, keyed by `(collection, entry_id, append_seq)` with a scoped unique
  `record_id` — and `knowledge_entries.cognition_head_seq` points at the holder's newest row.
  A database created before this change is recreated, not upgraded: the modified migration is
  refused by its checksum.
- `mindcore-sqlite`: the cognition append and current lookup no longer read or rewrite the
  carrier's history. A workflow settlement validates the record, inserts one row and advances
  the head and carrier revision in the borrowed transaction, returning the assigned revision;
  the carrier payload keeps only the cognition module envelope, and full DTO reads project
  `records` from the rows in append order. A full put must carry the committed prefix
  unchanged and appends only its validated suffix.
- Dependencies: the SPOKE cohort is pinned to `=0.18.0` — `spoke-schemas` and `spoke-operations` in
  `[workspace.dependencies]`, and `spoke-connect` there too, so the SQLite test dependency inherits
  the same pin with `remote-adapter` enabled. 0.18.0 is this release's pin.
- `mindcore-sqlite`: pre-release migrations are mutable and the embedded set is now one initial
  file: `0002` is deleted and its table lives in `0001_init.sql`. There is no upgrade path until the
  first release: a database whose applied migrations still match the embedded file reopens normally,
  while `MindStore::open` refuses one that applied the removed `0002` with SQLx's `VersionMissing(2)`
  and one whose applied SQL has since changed with a checksum mismatch. Recreate the file only for one
  of those two migration mismatches, or move it aside first if you want to keep it; a busy, locked or
  unreadable database is retried, not recreated.
- `mindcore-base` / `mindcore-sqlite`: the legacy memory port is removed, not deprecated. The public
  `MemoryStore` trait, its `MindStore` implementation and the DTOs it carried — `MemoryCandidate`,
  `MemoryRecord` and `MemoryError` — are gone; the borrowed-transaction `MemoryRepository` boundary
  and the workflow settlements above replace them. `RecallQuery` stays. A consumer that implements or
  calls `MemoryStore` does not compile against this release and must move to `MemoryRepository`.
- `mindcore-sqlite`: optional retained-vector storage. `memory_embedding_vectors` holds one
  little-endian `f32` vector per `(record_id, identity_hash)` with its record revision and dimension,
  indexed by identity so a retired identity's whole partition is one delete. The table is part of the
  single initial migration and starts empty.
- `mindcore-sqlite`: a knowledge list hydrates its page's cognition records in one statement keyed
  by the page's entries, not one statement per entry.
- `mindcore-spoke`: the recall knowledge slot applies the served list's disclosure predicate. The
  workflow binding carries no admitted viewpoint, so an `owner`-private entry never enters the
  pack, whatever budget the recall asks for.
- `mindcore-spi` / `mindcore-spoke`: a tombstone holds no vector. `records.delete` prunes the
  record's stored vector rows inside the tombstone's own transaction, and the recall page's miss
  set covers live records only, so a tombstoned body is never re-embedded or filled.
- `mindcore-spi` / `mindcore-spoke`: a confirmed absorb settlement warms the retained vector of each
  fresh or updated record when the host supplies an embedder with a stable serving identity. The
  settlement reports the revision it committed for every record it names, and the warm embeds only a
  record whose live read still serves that revision — one a later writer moved before the read is
  obsolete work and makes no provider call and no row. The provider call runs outside the settlement
  transaction, and the fill rechecks the live revision and withholding, so work that outlived its
  record writes nothing. A failed warm is a cache miss, never a failed, rolled-back or replayed Memory
  mutation; no embedder, no serving identity and an idempotent replay write nothing.
- `mindcore-spi` / `mindcore-spoke`: recall now reuses the retained vectors. The page is authorized
  first, the query vector is resolved from one validated process-local slot keyed by principal, space,
  subject, exact query and serving identity, and the stored record vectors come from one snapshot that
  rebuilds admission and withholding and refuses a page that moved before any row is probed — a moved
  record is `REVISION_CONFLICT`, never a decode of its newer row, and one page-shaped statement reads
  the whole page. Only missing or stale revisions are embedded; the release reauthorizes the page and
  the knowledge set and writes those fresh vectors in the same transaction, taking SQLite's write
  reservation at begin when it has a fill, so a concurrent writer cannot invalidate the
  reauthorization snapshot between the check and the write: a refusal aborts the
  release and no awaited cache write can observe one afterwards, while a cache-only storage failure is
  a miss. The query slot is installed only once that boundary succeeds, and a late
  `EmbeddingsUnavailable` keeps a warm slot instead of replacing it. An identical warm recall makes no
  provider call. No embedder, no serving identity or a cold `EmbeddingsUnavailable` leaves the vector
  table untouched and keeps the adapter's lexical order. Retiring an identity clears its query slot as
  well as its stored partition. Ranking is unchanged, and the persisted path scores bit-identically to
  the in-memory one, tie order included.
- `mindcore-runtime`: `MindcoreSurface::retire_embedding_identity` is the one supported host entry for
  identity rotation. It clears the retired identity's in-memory query slot and deletes its stored
  partition in one `identity_hash` statement, returning the rows deleted, and leaves another identity's
  rows and query slot warm. Call it between recalls: it does not drain a recall already in flight, and a
  fill that commits afterwards re-creates rows it deleted.
- **Breaking — `mindcore-spi` / `mindcore-spoke`:** `WorkflowBinding::commit_cognition` takes the
  request deadline as its last parameter, so every implementation and test double carries it. The
  settlement binding bounds its pool-connection wait by that deadline: a settle whose deadline passes
  while it waits for the store's only connection answers `DeadlineExceeded` and starts no write,
  and a connection in hand is re-checked before `BEGIN IMMEDIATE`, so an expiry observed on either
  of those waits refuses and writes nothing. A write that has already begun keeps its actual or
  uncertain outcome, which may land after the deadline. The non-contended commit is unchanged.
  `mindcore-spoke` now names `tokio` with the `time` feature directly; the lockfile is
  unchanged. Closes the residual in which an expired settle could still wait on the store connection.

### Added

- **mindcore-spoke:** Protocol adapter over the borrowed-transaction repository

- **mindcore-spoke:** Journal settlement and same-owner reflection completion

- **mindcore-sqlite:** Compile-time check settlement queries with a sqlx offline cache

- **mindcore-spi:** Add SPI contract crate with pinned MIND capability surface

- **storage:** Add list_unsettled_intents read primitive

- **spoke:** Withhold subject content reads behind unsettled journal intents

- **mindcore-spi:** Recall_context over a DTO-only workflow binding

- **mindcore-spi:** Absorb over the existing capture, review and update path

- **mindcore-spi:** Deliberate over the cognition carrier with bounded CAS retry

- **mindcore-spi:** Hand the validated dismissal rationale to the lens

- **mindcore-sqlite:** Add per-record cognition storage schema

- **mindcore:** Append and read cognition records per row

- **sqlite:** Serve knowledge-only Connect sessions from the shared store

- **mindcore-runtime:** Assemble the served Mind surface

- **mindcore-runtime:** Serve the three Mind workflows as tools

- **mindcore-runtime:** Prove the served surface end to end

- **mindcore-sqlite:** Add identity-keyed retained vector storage

- **mindcore-spi:** Warm retained vectors after absorb settlement

- **mindcore-spi:** Reuse retained vectors on recall

- **runtime:** Retire an embedding identity through one host entry

- **spi:** Stamp an optional assessment context id on deliberate records (I-000065)

- **knowledge:** Keep the preimage of every replaced revision

- **knowledge:** Replay an entry at any assigned revision

- **knowledge:** Mark an entry superseded and keep its anchors resolvable

- **knowledge:** Adopt a shared entry into a personal collection

- **knowledge:** Carry a replay floor and refuse pruned revisions

- **knowledge:** Prune entry history behind an explicit floor

- **sqlite:** Add the transfer plane tables

- **spoke:** Build and partition the memory export bundle

- **spoke:** Serve memory.exports.create and get

- **spoke:** Serve memory.imports.stage

- **spoke:** Serve memory.imports.activate and discard

- **runtime:** Advertise memory transfer only where it is served

- **runtime:** Report served memory operations and advertise transfer

- **runtime:** Validated the adoption source at assembly

- **runtime:** Serve the adoption tool when configured

- **knowledge:** Move the adoption anchor to the protocol field

- **knowledge:** Refuse a local put that changes a stored adoption_anchor

- **sqlite:** Expose retirement-chain reads for adoption

- **spi:** Apply the adoption contract rules

- **runtime:** Serve knowledge adoption over the protocol face

- **spi:** Emit a validated judgment from deliberate

- **runtime:** Return the judgment on the deliberate response

- **runtime:** Advertise spoke-judgment with the deliberate tool

- **history:** Persist prune dedup in the store's own database

- **history:** Serve prune dedup from the store's own database

- **spi:** Hand the settlement binding its request deadline

- **spoke:** Bound the settlement acquire by the request deadline

- **release:** Add the release baseline helper

- **release:** Add the release baseline helper

- **release:** Check SemVer against the previous stable release

- **release:** Lock the published migration set

- **migrations:** Reject duplicate prefixes and add --write-lock

- **release:** Publish from the release workflow

- **release:** Publish with crates.io Trusted Publishing

- **release:** Allow the first release to keep the current version


### Changed

- **mindcore-spoke:** Prove rollback of the mid-staging partial write

- **mindcore-sqlite:** Settle-parity Task 1 — rewired embed_parity onto the borrowed transaction

- **mindcore-sqlite:** Strengthen embed_parity per Task-1 review F1..F3

- **mindcore-sqlite:** Settle-parity Task 2 — Y04, reopen/settle, O1, F2 terminal arms

- **mindcore-sqlite:** Settle-parity QC fix — D3 restricted visibility, D10 into-shared, L3 file/fault/replay

- **mindcore-sqlite:** Settle-parity fix round 2 — baseline D1/D2 scoped captures, D12/F3 paging, typed O2, settle-path fault + replay file-set

- **mindcore-sqlite:** Settle-parity fix round 3 — file-multiset exactly-once, D2 order, D12 page identity

- **mindcore-sqlite:** Settle-parity fix round 4 — tombstone-row proof, page-1 count restored

- **mindcore-sqlite:** Name the content refusal as a reader contract with a defensive guard

- **mindcore-spi:** Prove the SPI surface with a mock external adapter

- **mindcore-sqlite:** Regression for absent include_shared and literal-null filter (I-000053, I-000054)

- **mindcore-spoke:** Adapter regressions for fence, concealment, tombstone and null filters (I-000053, I-000054, I-000056, I-000050, I-000059)

- **mindcore-spi:** Harden the adapter proof against the prompt-bridge regressions

- **mindcore-spi:** Drop mock-driven assertions, assert shipped behavior

- **mindcore-spi:** Remove tautological assertions and orphan call counters

- **spoke:** Assert receipt poll serves recorded result and error envelopes

- **mindcore-sqlite:** Settle journaled intents in embed_parity fixtures for withheld reads

- **mindcore-spoke:** Regress early content decode on withheld by-ID reads

- Rustfmt the withholding regression tests

- **mindcore-spi:** Assert deadline cancellation without wall-clock margins

- **mindcore-spi:** Prove cross-domain reuse with an engineering lens

- **mindcore-spi:** Compare the absorb reasoner payloads across lenses

- **mindcore-sqlite:** Prove tombstone exclusion with a matching query

- **mindcore-spoke:** Read spaces.list grants in one query

- **mindcore-sqlite:** Reuse the record encoding for provenance

- **mindcore-sqlite:** Move owned row fields in row-to-IR conversion

- **mindcore-sqlite:** Pin constant append cost and cognition CAS regressions

- **mindcore-sqlite:** Bound cognition cost by VDBE steps and pinned byte baselines

- **mindcore-sqlite:** Compare rejected appends against the committed record snapshot

- **sqlite:** Prove a memory-only Connect responder under 0.17

- **sqlite:** Prove the ownership gate and unsupported-selection refusals

- **mindcore-sqlite:** Drop explicit borrows clippy flags in cognition_cost

- **mindcore-spi:** Observe a pre-authorization query-slot probe

- **spi:** Assert reloaded score bits match fresh embeddings

- **mindcore-sqlite:** Apply rustfmt to the cognition and knowledge surfaces

- **sqlite:** Carry the snapshot seam as a braced struct so Default is a real constructor

- **runtime:** Retire a warm identity in the loopback

- **mindcore-sqlite:** Apply rustfmt to Task 2 test files

- **mindcore-sqlite:** Read the staged journal intent through the repository

- **memory:** Bound the batch grant read to visible spaces (I-000066)

- **spoke,spi:** Apply rustfmt

- **knowledge:** Force the adoption race through one shared barrier

- **knowledge:** Prove history, adoption and supersede in one lifecycle

- **knowledge:** Close lifecycle-proof QC fix wave 1

- **knowledge:** Close lifecycle-proof QC fix wave 2

- **runtime:** Prove two routed principals and unrouted recall

- Format the routed-principal loopback assertions

- **runtime:** Assert no cross-collection effect around each routed call

- **runtime:** Prove per-principal recall slots and scope the routed spec

- **settlement:** Pinned the retry class the settlement loser reports

- **connect:** Proved the over-bound capacity details cross a signed session

- **knowledge:** Rehearse the five live facts on a shared scope

- **knowledge:** Give the rollback probe a distinct payload

- **knowledge:** Rehearse restore and the single-writer switch

- **knowledge:** Read the original carrier on the restored copy

- **knowledge:** Verify rehearsal claims against persisted state

- **sqlite:** Drop the uncompiled windows symlink arm

- **spi:** Extract adoption algorithm and add a Send entry

- **runtime:** Proved the adoption fault refusal in one call

- **runtime:** Recorded the fault sink write attempt

- Dropped the two dead exports

- **knowledge:** Align connect OCC assertions with the 0.18 responder

- **knowledge:** Rustfmt the connect OCC assertions

- **runtime:** Prove the prune replay deletes nothing

- **sqlite:** Keep the destination refusal propagation defense

- **runtime:** Remove the product adoption tool

- **runtime:** Prove a recorded chain fault beats the resolver's retirement

- **runtime:** Prove knowledge adoption over a signed loopback session

- **runtime:** Restore rustfmt formatting in the loopback tests

- **runtime:** Assert adoption port absence and fresh-key idempotency

- **runtime:** Keep deliberate draft assertions testing draft

- **runtime:** Prove the served judgment on every deliberation branch

- **loopback:** Assert branch verdicts and judgment identity determinism

- Apply cargo fmt to judgment sources

- **runtime:** Route peer-owner to a collection the knowledge face does not serve

- **history:** Replay a prune's outcome across a reopened database

- **spi:** Pin the settle deadline to the call start, not the commit

- **spoke:** Pin the bounded settle wait on a contended pool

- **spoke:** Prove the settle acquire waits out a short connection hold

- **adoption:** Pin the retired destination occupant to the live occupancy

- **spi:** Anchor the deadline upper bound at the call start

- **loopback:** Cover a retired destination occupant on the signed adoption face

- **spoke:** Prove the pre-BEGIN deadline re-check refuses

- **history:** Index the dedup quota count, and fix the ADR references

- **history:** Drop the unreachable quota-drop warning

- **spi:** Loosen the deliberate deadline slack to 100ms

- **release:** Cover the SemVer checker

- **semver:** Cover every packaged dependency table

- **semver:** Cover tag-time blocking and clean scratch dirs

- **semver:** Cover quoted keys and alias tags

- **release:** Cover the migration guard and document the store contract

- **migrations:** Cover --write-lock and duplicate prefixes


### Documentation

- Record standalone SQLite storage decision and narrow timestamp claims (I-000040, I-000038, I-000039, I-000035)

- Describe the busy timeout and rollback-journal contention accurately

- Correct SQLITE_BUSY_SNAPSHOT cause and pipe freshness metadata failures

- **adr:** Record the mindcore-spi capability boundary decision

- **adr:** Correct the four may-fix claims in the capability-boundary ADR

- **adr:** Tighten Status line and Consequences to match shipped types

- **adr:** Drop the invented 'six SPI-owned types' count and spi-* prefix

- **adr:** Give the borrowed-transaction seam record the implemented shape

- Record the mutable pre-release migration policy

- **storage:** Mark cross-commit database compatibility unsupported and untested

- **adr:** Record the executor-independent deadline timer

- **mindcore-spoke:** Record capture/update receipt retention parity

- Align the vector-cache migration and retention statements

- **changelog:** Name the identity-retirement entry

- **mindcore-sqlite:** State the retirement quiesce contract at the storage layer

- **adr:** Name the supported identity rotation entry

- **adr:** State the real test-only observability boundary

- **mindcore-runtime:** Correct Fixture::open schema-source comment

- Recreate a store only when open refuses it

- Recreate a store only for a migration mismatch

- **knowledge:** Capture store bootstrap and replay-decision patterns

- **specs:** Describe the store schema the migration creates

- **adr:** Record why the absorb decision log lives in memory_journal

- **pattern:** State the marker's second transaction and the retry repair

- **readme:** Match the Checks list to the CI steps

- Align the store specs with the protocol schema and make README checks fail fast

- Restate the knowledge-only ports and complete the store-schema page

- Name the real source of the sqlx-cli version

- **knowledge:** Drop the deleted settlement fixture path

- **knowledge:** Capture the trigger-preimage pattern and its cost-contract traps

- **spec:** Record the adoption anchor contract and the single parser

- State the SPI boundary's surviving half and the replay gap's actual range

- State which CAS base compares the column against the payload

- Align the lifecycle contracts with what the code actually enforces

- **knowledge:** Capture per-principal serving routing

- **adr:** Correct implemented ADRs against HEAD and record principal routing

- **adr:** Apply QC findings on routing and serving claims

- **adr:** Name the assembly as what a per-principal surface duplicates

- **knowledge:** Capture implemented-ADR currency

- **spoke:** State the route-builder refusal accurately

- **runtime:** State the one-collection rule where the route map is declared

- **knowledge:** State the one-collection-per-principal rule

- **changelog:** Record the per-principal route map

- State the drift check, the SPI amendment and the replay head the code actually enforces

- State the served failure message, the trigger cost cost and the anchor reader

- **knowledge-base:** Merge the two adoption-anchor summaries into one

- **knowledge:** Record how to prove contention rather than generic failure

- **knowledge:** Each race pins its own reject, and the seam is the only route to the predicate

- **knowledge:** Describe the retain race's scored disjunction, not a single reject

- **knowledge:** A replay floor gates the read before checkpoint selection

- **knowledge:** A rehearsal asserts on read-back state, not return values

- **adr:** Ground capability decisions in shipped contracts

- **adr:** Own the record-extension decision instead of deferring it downstream

- **adr:** Correct contract rationale and cite source

- **adr:** Distinguish wire round trips from SQLite storage

- **adr:** Clarify extension handling in the schema decision

- **specs:** Put principal routing on the required skeleton

- **specs:** Keep routing behaviour out of the location list

- **knowledge:** Crystallize advertise-only-what-is-installed

- **runtime:** Document adoption and assert its advertisement

- **specs:** Correct route-map citation and narrow the wire invariant

- Served transfer in README, recorded_failure substitution documented

- **transfer:** Correct the served op count and two import-identity claims

- **specs:** Promote behavior contracts out of code comments

- **knowledge:** Promote planner, sqlx and fixture notes out of code comments

- **corpus:** Address QC findings on promoted comment homes

- **corpus:** Correct promoted facts flagged by PR review

- **corpus:** Scope staging absence, expiry sweep and listing cost

- **corpus:** Complete the sweep triggers and the revision materialization claim

- **specs:** Name the protocol adoption_anchor and the put gate

- **task-4:** Correct anchor-producer, scope-id and put-decode claims

- Align specs, README and CHANGELOG with knowledge.adopt

- Correct adoption replay and destination-identity claims

- **specs:** Name the revision carrier and return the SPI result directly

- **specs:** Link the judgment owner and correct the ordering comment

- **specs:** Scope the deadline no-write guarantee to the pre-handoff check

- **spi:** Scope the settlement deadline check to the pre-handoff moment

- State the adoption destination break and its assembly refusal

- **adoption:** Align the raced re-read and the anchor bound with the code

- **adoption:** Name KNOWLEDGE_ENTRY_NOT_FOUND for the destination re-read

- **adoption:** Correct the destination default and the anchor-bound fault

- **adoption:** Record permitted occupancy disclosure and qualify the shared slot

- **history:** Record the durable prune dedup across specs and a decision record

- **history:** An unsettled claim does not prove the prune never ran

- **history:** Separate observable history from an unobservable prune outcome

- **history:** The floor reports current history, not what the request did

- Bound the settlement wait in spec, deadline ADR and changelog

- **changelog:** Narrow the deadline guarantee to an observed expiry

- **spec:** Scope the settle deadline refusal to the observation points

- **specs:** State the uniform concealment invariant on both served faces

- **specs:** Name the destination read as the retired occupant's route

- **specs:** State the observable occupancy sequence and narrow the prune migration

- **adr:** Attribute the settle connection bound to the spoke binding

- **history:** The dedup bound is per collection, not per surface

- **history:** Attribute both service compositions to lib.rs

- **store:** Describe history_dedup's two-column identity in the migration

- **history:** The quota refusal is the caller's own, not the namespace's

- **history:** Say unparsable dedup keys share the unknown bucket

- Add bilingual README and usage docs

- Address review findings

- Clarify empty carrier state and version source

- Drop remaining SPOKE version references from crate comments

- **adr:** Record the release pipeline decision

- **adr:** Release notes come from the committed changelog

- **adr:** A missing changelog section tags nothing

- **adr:** A release prepends a changelog section

- **adr:** Build metadata is refused and a re-dispatch keeps the branch

- **adr:** Name how a release is rebuilt on a newer main

- **adr:** Re-dispatch only labels the open release pull request

- **release:** Align the ADR with the shipped publish path

- **release:** State the SemVer, storage and release contracts

- **semver:** Record the 0.18.1 floor the manifests declare

- **release:** Document trusted publishing and clarify the policy scope

- **release:** Explain the lock gate and the tag-before-upload order


### Fixed

- **mindcore-spoke:** Close task-1 review findings F1-F6

- **mindcore-spoke:** Instant-aware grant expiry and original-request digest

- **mindcore-spoke:** Restore the revoked half of grant liveness

- **mindcore-spoke:** Macro-check the record placement read too

- **mindcore-spoke,mindcore-sqlite:** Close settle-adapter QC wave A-1..A-4

- **mindcore-spoke:** Parse the pinned review-decision and fence-target wire shapes

- **mindcore-spoke:** Treat explicit null content_override as absent

- **mindcore-base:** Clear clippy -D warnings on ReceiptRead/PlacementSelection

- **mindcore-spoke:** Advance recall paging past byte-budget overflow; escape front matter

- **mindcore-spoke:** Escape space_id in projections; assert updated_by injection

- **mindcore-sqlite:** Settle pending receipts past lease expiry

- **mindcore-spoke:** Let expired-lease retries reach the settle takeover

- **mindcore-base:** Bound extension maps by key grammar, not value shape

- **mindcore-sqlite:** Refuse undecodable pending content and negative receipt counts

- **mindcore-sqlite,mindcore-spoke:** Strict reflection decode survives legacy rows (I-000053, I-000054)

- **mindcore-spoke:** Persist-row lifecycle gate, by-id concealment, tombstoned delete (I-000056, I-000050, I-000059)

- **mindcore-spoke:** Reject cognition_filter at request boundaries, charge content cognition bytes (I-000055, I-000061)

- **mindcore-spoke,mindcore-base:** Persist lease takeover, reject float belief order (I-000057, I-000036)

- **mindcore-sqlite:** Stamp takeover lease at claim instant (I-000057 review)

- **mindcore-spoke:** Lazy recall budget walk; test gaps from plan QC tri (QC1-02/QC2-001, QC1-01/04/05/03/06, QC2-002)

- **mindcore-spoke:** Withhold by-ID reads before content selection

- **mindcore-spoke:** Admit the bound knowledge collection to its caller

- **mindcore-spi:** Revalidate cited evidence, keep replay outcomes and precheck the publisher

- **mindcore-spi:** Revalidate cited records on refusal and keep replays pending

- **mindcore-spi:** Gate the carrier write on its own authority and deadline

- **mindcore-spi:** Bound provider calls by the request deadline

- **mindcore-spi:** Report a fresh capture-only absorb without reading it back

- **mindcore-spoke:** Report unconfirmed publication or settlement as uncertain

- **mindcore-sqlite:** Keep the grant batch read inside the trait and skip orphans

- **mindcore:** Prove D12 query-shape independence and correct migration/ADR claims

- **mindcore-sqlite:** Keep the stored created_at in the knowledge update payload

- **mindcore-spi:** Clamp the request timeout so the deadline always exists

- **mindcore-spi:** Report a pending replayed candidate without a review read

- **mindcore:** Classify post-staging failures as uncertain and drop dead seams

- **mindcore-sqlite:** Project full DTO reads from one read snapshot

- **mindcore:** Gate the cognition row writer and pin the exported read

- **sqlite:** Keep the served collection out of knowledge put refusals

- **mindcore-sqlite:** Close plan-QC findings F1-F5 on the served knowledge face

- **mindcore-runtime:** Refuse an unsupported host admission as NotAuthorized

- **mindcore-runtime:** Advertise the closed tool argument shapes

- **mindcore-runtime:** Keep recall admission independent of the deliberation route

- **mindcore-spoke:** Keep an unauthorized knowledge slot out of recall's authorization

- **mindcore-runtime:** Advertise the pinned cognition schema instead of an invalid $ref

- **mindcore-runtime:** Close plan-QC findings F1-F6 on the served surface

- **mindcore-sqlite:** Require integral vector keys and representable dimensions

- **mindcore-spi:** Bind the vector warm to the settled revision

- **mindcore-spi:** Keep recall caches behind the final authorization

- **mindcore-spi:** Bind recall release and fills to one boundary

- **mindcore-spoke:** Reserve the recall write before reauthorization

- **mindcore:** Apply disclosure to the knowledge slot and prune tombstoned vectors

- **mindcore-sqlite:** Normalize the cognition module an append re-encodes

- **mindcore-runtime:** Classify tool refusals by retry class and own policy code

- **mindcore-sqlite:** Provision the space and its first grant in one transaction

- **mindcore-spi:** Record an absorb Pending decision durably

- **mindcore-spoke:** Fail malformed absorb decisions closed

- **spoke,sqlite:** Key the absorb marker read by receipt alone

- **spi:** Blame an invalid caller context on the caller, not the reasoner

- **spi:** Record the pending decision a replay could not

- **spoke:** Report a refused write as contention, not InternalError

- **spi:** Record the whole receipt candidate list on a retry repair

- **mindcore-spoke:** Keep write contention in the retryable pre-commit class

- **mindcore-spi:** Record a replay repair only for a wholly pending receipt

- Index the principal grant scan and correct the README and CHANGELOG claims

- Make the principal the driving key of both grant reads

- A replay never writes the absorb decision marker

- **knowledge:** Refuse an unrepresentable revision and a contradicted gap

- **knowledge:** Prove a replay gap by position instead of row count

- **knowledge:** Harden the supersede CAS, marker reads and the anchor parser

- **knowledge:** Close QC fix wave 1 on adoption

- **knowledge:** Adopt by the anchor revision and correct the lifecycle docs

- **knowledge:** Bound the list over live rows and record the SPI dependency

- **knowledge:** Prove the replay gap by position and index the live list

- **knowledge:** Decide the superseded refusal before decoding the payload

- **serving-routing:** Select the carrier from the request's holder

- **spoke:** Keep one destination collection per principal route

- **spoke:** Refuse a repeated destination collection without naming it

- **knowledge:** Keep a storage failure's driver text off the served face

- **knowledge:** Refuse a non-array records member instead of aborting; correct the anchor builder's two roles

- **sqlite:** Classify the over-bound list refusal and pin the contention message

- **knowledge:** Export PruneReport and prove retention by behaviour

- **knowledge:** Probe the prune boundary in constant work

- **knowledge:** Set the history floor above the deleted checkpoint

- **knowledge:** Guard the history floor as an integer and name it correctly

- **spoke:** Withhold unsettled subjects and measure the header chunk

- **spoke:** End a gapped import and screen the source export

- **transfer:** Freeze the staged result and withdraw the gap rule

- **transfer:** Serve receipt polling, recheck adoption at activation, sweep expiry

- **transfer:** Name an expired import to a late chunk

- **transfer:** Decide import expiry by persisted state

- **transfer:** Terminal imports do not expire

- **runtime:** Close the adoption QC findings

- **runtime:** Kept the adoption fault diagnostic from panic

- Unknown recorded reject is terminal, routing page merged, mind-model names CallerId

- **spoke:** Closed the ten transfer-surface review findings

- **transfer:** Persist export expiry, index the reclaim, seal the typed encoding

- **transfer:** Keep explicit nulls through the seal, tighten the token screen

- **transfer:** Refuse explicit null content instead of changing it

- **transfer:** Screen only the lossy null slots

- **adoption:** Return adopted_revision on a repeat conflict

- **spi:** Classify the Connect create-race refusal and tighten adoption docs

- **history:** Hold the replayed payload to the admitted scope

- **runtime:** Validate the adopted scope id and keep retirement faults

- **runtime:** Honour the admitted destination selection and refuse unservable terminals

- **runtime:** Finish the F-001/F-002 guards and their signed cases

- **spi:** Gate the settlement on the deadline after the projection

- **runtime:** Adopt into the knowledge face's collection, not the route map

- **adoption:** Bound the stored anchor revision and classify the raced re-read

- **adoption:** Answer occupancy for a hidden destination occupant

- **knowledge:** Stop disclosing the hidden successor id in a create-only refusal

- **history:** Decode a dedup outcome only in the shape this module writes

- **spi:** State the deadline boundary and derive the hand-off bound

- **spi:** Qualify the deadline boundary and anchor its test on the reasoner

- **history:** Answer the one absence for a retired or hidden prune target

- **adoption,specs:** Address plan QC findings F-001..F-003

- **adoption:** Answer occupancy for a retired destination row

- **adoption:** End the destination-read borrow with a block scope

- **runtime:** Namespace the prune dedup record and bound each namespace

- **history:** Key dedup rows by column, not by a framed string

- **runtime:** Reclaim expired history dedup rows before claiming

- **history:** Bound prune dedup by principal, not by wall clock

- **history:** Report a prune outcome the quota refused to retain

- **history:** Share one quota for odd keys, and record only into a claim

- **spoke:** Read rows frozen before the 0.18.1 gates

- **release:** Strict SemVer, exact label gate, rerun-safe tagging

- **release:** Allow leading zeros in alphanumeric prerelease identifiers

- **release:** Fetch the release branch before the leased push

- **release:** Preserve curated notes, fail loud on missing sections

- **release:** Build release notes from the merged commit

- **release:** Read release notes from the committed changelog

- **release:** Prepend changelog sections, validate body before tagging

- **release:** Always land a changelog body and clean the scratch dir

- **deps:** Keep the spoke cohort on the 0.18.1 floor

- **release:** Ship the license text with each published crate

- **release:** Ignore alias tags when choosing the baseline

- **release:** Ignore alias tags when choosing the baseline

- **semver:** Check every packaged dependency table for a version

- **semver:** Reject leading zeros in version identifiers

- **semver:** Unquote TOML keys before dependency checks

- **semver:** Keep target table labels valid TOML after unquoting

- **ci:** Build the sqlx fixture from every migration

- **release:** Stage the bumped member manifests in the release PR

- **release:** Lock new migrations and keep the token out of build scripts

- **release:** Refuse to upload after the verification build changes tracked files

- **release:** Require the migration lock to cover the tagged commit

- **release:** Match the transient registry failures cargo reports

- **release:** Bump internal path-dependency versions with the workspace

- **release:** Keep the rest of the line when rewriting a dependency

- **ci:** Build the sqlx workspace for the API diff

- **semver:** Classify the API diff by exit status

- **release:** Isolate publish credentials and complete the tag gates

- **release:** Isolate verification builds from publish credentials


### Internal

- Initialize Morning Star harness ([#1](https://github.com/42ch-dev/mindcore/issues/1))

- Crystallize borrowed-tx repository + shared-schema mapping pattern (settle-repo compound)

- Crystallize two-phase adapter pattern (settle-adapter compound)

- **iteration:** Close iter003-settle — specs/ADR promotion

- Print sqlx cache drift before failing the freshness step

- Order sqlx cache freshness first and pin actions (I-000051, I-000052, I-000037)

- **mindcore-spi:** Lock tokio dev-dependency for the SPI adapter proof

- **mindcore-spi:** Rustfmt line-wrap in mock adapter test

- **iteration:** Close v06-spoke017-serve — specs/ADR promotion

- **iteration:** Close v07-store-protocol-schema — specs/ADR promotion

- **iteration:** Close 008-knowledge-lifecycle — promote the lifecycle spec and ADR, capture two patterns

- **iteration:** Close 009-serving-router — specs promotion

- **iteration:** Close 010-retention-and-shared — history floor ADR

- **comments:** Classify non-doc comments into declared shapes

- **comments:** Label observations as notes

- Gate the corpus through the pinned CLI

- **spoke:** Pin 0.18.0 and compile every target

- Refuse below-floor replays as HistoryTruncated

- Serve history over the routed store

- Admit before keep bound, guard public prune zero-keep, typed snapshot envelope

- Bound embedding dimension by the JCS safe integer range

- Split the dimension boundary test by pointer width

- Compose the history face and advertise it

- Prove the history face over a signed loopback session

- Publish the served history face and its replay projection

- State the runtime dedup surface and its residual window

- Qualify the adoption retry outcome

- **history:** Keep the served wire shape in one spec

- **history:** Scope the local disclosure invariant to ordinary reads

- **spoke:** Pin 0.18.1 and keep the IR timestamp gate strict

- **release:** Keep review edits on release re-dispatch

- **release:** Name the lookup step for what it does

- **release:** Leave an open release PR's title and body alone
