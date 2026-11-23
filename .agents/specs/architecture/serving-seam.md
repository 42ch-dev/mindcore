# Architecture: Mindcore serving seam

## Scope

Signed Rust SPOKE sessions over a host-supplied transport: the Memory, Knowledge, History and adoption faces, and the three Mind workflow tools. The library does not own a listener, accept loop, identity provisioning, publisher, or model provider.

## Current state

### Knowledge

The host installs a `KnowledgeAdmission` callback in `RemoteKnowledgeFace`. Connect supplies the authenticated peer id; the callback returns the peer's one admitted `Scope` or refuses. An exact host-configured route maps the admitted `scope_id` to `KnowledgeScope.collection` on the bound SQLite pool. No fallback guesses a collection from an entry id, `owner`, `viewpoint`, or Memory `SpaceId`. Memory space and workflow knowledge grants remain separate and go through the existing `MemoryWorkflowBinding` admission. The `ke-ownership` protocol gate checks the negotiated flag for a list with an explicit `viewpoint`; it neither chooses the collection nor grants the peer entry access.

`get` and `put` carry no wire scope. `list` names the admitted `scope_id` and `viewpoint`; Connect bounds the request by the admitted scope before calling the store. The scoped adapter binds the collection on every statement. Within it, entry-id, entry-type, source-id and viewpoint filters use SPOKE's knowledge predicate (including owner-private disclosure); an explicit empty set matches nothing. The face does not serve cognition, timeline or extension-based selection; refuse unsupported constraints rather than silently ignoring them. List results are in `entry_id` order. The host admission callback validates the scope it returns with `mindcore_sqlite::validate_admission_scope` before returning it: Connect's list merge keeps only the request's cognition selection, so an admitted cognition selection reaches the collection query unless it is refused at admission. A served list reads at most `SERVED_KNOWLEDGE_LIST_LIMIT` (512) **live** rows of the routed collection — the live predicate rides in the statement — and refuses a larger live set with `InternalError` naming the bound; the bound applies before disclosure filtering and never truncates silently. A retired row therefore never consumes list budget.

The peer's route grants read/write knowledge access within its scope, not unconstrained access to the database. `put` checks the proposed payload and any existing row against the admitted constraints on the same `BEGIN IMMEDIATE` transaction, before exposing a revision or committing a change. A missing expected revision keeps create-only semantics; a supplied one performs compare-and-swap through the common store. A hidden existing row is never overwritten, and hidden entry data/revisions are never returned. No operation probes another collection.

| Refusal | Result |
|---|---|
| Required negotiated family, conditional ownership flag or serving face missing | Wire `op_unsupported`, exposed by the Rust dialer as `CapabilityPortMissing` with `details.wire_code`. |
| Peer unadmitted or list names/widens another scope; unknown trusted route | `NotAuthorized` (`NOT_AUTHORIZED`), no target details. |
| Host admission returns an unsupported selection | `NotAuthorized`, no target details: refused at admission before Connect merges the scope, so the store is never reached. |
| Malformed scope/payload, or a caller-declared (request) scope with an unsupported selection | `InvalidInput`; the unsupported-selection case is refused at the store. The responder's earlier payload-dependent gates still apply. |
| Get id absent or hidden within the routed collection | One generic `KnowledgeEntryNotFound`. An out-of-scope put is `NotAuthorized`. |
| Admitted create hits an existing visible id; admitted update carries a stale/missing stored revision | `KnowledgeEntryAlreadyExists`; `StoredRevisionStale`, respectively. Preserve backend codes; storage failures are `InternalError`. |

### History

The `spoke-history` family publishes two operations over the same signed session: `history.revisions.get` returns a `RevisionSnapshot`, `history.revisions.prune` returns `HistoryPruneResult { deleted, floor_revision }` where `floor_revision` is the local report's floor. The host enables both with `ServeFaces.history`: `false` composes no history service and advertises no `spoke-history`, the composed surface adds it to `RemoteServePortsComposite::knowledge_only(face).with_history(service)`, and `face_capabilities` advertises the family from that one flag.

One store fills two port roles: `RoutedKnowledgeStore` implements `spoke_operations::HistoryPort` beside `ScopedKnowledgePort`, so no second store or runtime history module exists. Admission is the knowledge face's, composed as `HistoryAdmission = (principals(peer)?, knowledge_admission(peer)?)`: a peer outside either callback gets that callback's refusal, and can therefore replay and prune exactly the scope it reads and writes through `port.knowledge.*`. Connect compares a present request `scope` with the admitted scope before calling the port — `NOT_FOUND` on get, `NOT_AUTHORIZED` on prune — and the port re-applies `RoutedKnowledgeStore::admit` as its own guard.

Coverage is `knowledge_entry` alone, because the store keeps no relation revision table. Any other `entity_kind` answers `HISTORY_ENTITY_UNSUPPORTED` after admission and before any statement. The entity's own disclosure rules apply unchanged: a missing, retired or peer-hidden row is one `NOT_FOUND` on get; prune needs a put's permission, so a retired or hidden row answers `NOT_AUTHORIZED` and a missing one `NOT_FOUND`. Get checks two payloads, because visibility is a payload property: the live row decides existence and retirement, and the replayed entry is then held to the same admitted scope, so a revision whose own disclosure, viewpoint, source or entry-type fields fall outside that scope answers the same `NOT_FOUND` with no snapshot. Both ops run in one transaction — a read for get, one `BEGIN IMMEDIATE` covering the permission read and the delete for prune — and every answer passes through the same storage-failure generalization as the knowledge face, so no table or column name reaches a peer.

`RevisionSnapshot` carries `schema_version` `1`, `entity_kind` `knowledge_entry`, the requested `entity_id` and `revision`, `extensions = {}`, and the replayed entry as `snapshot`: the stored entry travels as entity data, so its timestamps keep their stored spelling and a second replay is byte-equal.

| Refusal | `details` |
|---|---|
| `entity_kind` other than `knowledge_entry` | `HISTORY_ENTITY_UNSUPPORTED` with exactly `entity_kind`, the requested kind |
| Revision below the entry's persisted floor | `HISTORY_TRUNCATED` with exactly `floor_revision`, an integer ≥ 0 |
| Revision `0`, above the live revision, or unassigned to the entry | `NOT_FOUND`, none |
| Missing, retired or peer-hidden row on get; missing row on prune | `NOT_FOUND`, none |
| Retired or peer-hidden row on prune | `NOT_AUTHORIZED`, none |
| `keep == 0` | `INVALID_INPUT`, none; refused by the port, since Connect's decoder admits `0` |
| Malformed payload, or a prune integer outside the JCS exact-integer range | `INVALID_INPUT` with `details.op` |
| A failed statement or a contradicted history gap | `INTERNAL_ERROR`, none after generalization |
| Same `idempotency_key` with a different payload digest | `IDEMPOTENCY_CONFLICT`, none |
| Same `idempotency_key` after its record was evicted | `RECEIPT_EXPIRED`, none; the prune is not re-executed |

`HISTORY_NOT_RETAINED` is never answered: every revision at or above the floor is rebuilt from the next checkpoint or the live row, and a contradicted gap is storage corruption, not a retention gap.

Prune is deduplicated by Connect's recorded-outcome store, keyed by `(op, effective scope, requester principal id, idempotency_key)` and holding the request digest plus the settled outcome. Both services keep the released in-memory default. That default bounds **live outcome records** only — 1024 by default — and an evicted key leaves a tombstone so a same-key replay answers `RECEIPT_EXPIRED` instead of re-executing; tombstones are never evicted, so the store's memory grows with the number of distinct evicted keys. The store belongs to one `HistoryPortService`, so its records and tombstones live for exactly that service instance: `MindcoreServe::surface` builds a new service per assembled surface, so a reassembly in the same process starts with no record and no tombstone, exactly as a restart does. Connect's standalone `HistoryPortService::with_dedup_store` accepts a durable store, but that builder is not an injection point into this runtime: `MindcoreServe::surface` constructs `HistoryPortService::new` itself and `ServeFaces` exposes no dedup store and no history service, so every host of this surface serves the in-memory default.

**Dedup consequence:** the default does not outlive the service instance, so a prune whose outcome was lost to a crash — or to a reassembled surface — can be re-executed by a same-key retry after new checkpoints exist, deleting checkpoints the original call never saw — permanent data loss. A durable store narrows that exposure without closing it: Connect records the settled outcome only after the port returns, while the SQLite prune commits inside the port, so a crash between commit and record leaves a committed prune with no record, and a same-key retry executes it again. No dedup store on either side removes that window, and this surface offers no way to inject one. Adoption has no equivalent permanent-loss replay path, but it is not a guaranteed no-write repeat either. While the source still resolves to the terminal the first call copied and the destination already holds the copy, the create-only CAS and the anchor rules make the retry answer `already_present` or `REVISION_CONFLICT` and write nothing. After a reassembly dropped the record, a source since retired to a different terminal resolves the same request to that new terminal; with `destination_entry_id` omitted the copy is then addressed by the new terminal's id, so the retry writes a second copy. The original copy stays intact and nothing is lost — the repeat is simply not guaranteed to write nothing once the terminal changed and no destination id was supplied. `I-000096` tracks the durable store as a medium-severity residual for the upstream builder.

### Workflow tools

`MindcoreServe` derives served capability ids and tool descriptors from installed faces and handlers; the host supplies identity, transport, allowed peers and non-capability manifest metadata. Compose Memory and Knowledge with `RemoteServePortsComposite::knowledge_only(face).with_memory(service)`, and the history family with `.with_history(service)` on that same composite. The Memory service consumes a trusted peer-to-principal mapping and implements the advertised family contract; the runtime does not create fake baseline, reflection or transfer providers. For each accepted transport, register handlers before releasing inbound traffic; `connect_responder` starts its own SPOKE receive task and does not install handlers from the manifest.

The memory face advertises `spoke-memory` whenever a service is attached, and `spoke-memory-transfer` only when that service reports all five §7.29–§7.33 operations through `mindcore_spoke::ServedMemory::operations`. `RemoteMemoryService` exposes one `invoke` seam and names no operation, so the service itself carries the answer: `ServedMemory` extends it with an operation list whose default is empty, and a bare `RemoteMemoryService` therefore never claims the transfer family. The derivation is the only advertisement list: `face_capabilities` compares [`TRANSFER_OPERATIONS`] against what the service reports, so no host-supplied string can claim a family the bound service cannot show it dispatches.

`mindcore_spoke::TransferService` is the service this crate binds: `operations()` returns the six operation strings it dispatches — the five §7.29–§7.33 transfers and §7.28 receipt polling (`memory.receipts.get`) — and `invoke` routes each of them to the matching `MemoryAdapter` call over the host's pool. `RemoteMemoryService::invoke` hands the service no transaction, so the service is the caller: each op runs in one transaction — `BEGIN IMMEDIATE` for the four mutations, reads for `exports.get` and `receipts.get` — and commits only after the adapter answers `Ready`; a commit whose acknowledgement is lost answers `OUTCOME_UNCERTAIN` rather than claiming either outcome. Any other `memory.*` op is refused as `CAPABILITY_PORT_MISSING`.

The service binds its own `MemoryAdapter::new(false)`: the transfer family reads no reflection policy, so the adapter's reflection flag serves only `describe` and the reflection operations, and no transfer op consults it. What the transferred bundle and chunks must satisfy is `data/transfer-bundle-chunking.md`.

An export bundle never discloses beyond its declared scope. A receipt row carries no subject or placement, so only a whole-space export summarizes the space's receipts; a subject-scoped export omits them, because any receipt summary there would name unrelated operations. A withheld subject contributes no subject-scoped grant summary either: naming its `subject_id` would disclose the withholding and dangle at the target's own verification.

A stored document whose `content.structured` (or a pending `proposed.content.structured`) is an explicit `null` cannot be exported: the pinned content type carries no present null in that member, so `memory.exports.create` refuses with `INVALID_INPUT` and `details.reason = "explicit_null_content_not_transferable"` rather than sealing a changed document, and final staging verification refuses an arriving bundle with the same reason. A `null` nested inside `structured` or inside a cognition record is carried unchanged.

The host's peer resolver supplies the `Principal` from the authenticated responder session, not from arguments. `ToolHandler` receives only JSON arguments, so a handler binds to its responder session (weak reference, no ownership cycle). Missing peer/principal is `NotAuthorized`. Do not share a handler closure bound to one session with a different session. Provider capabilities and `DomainLens` are host inputs. Workflows continue to enforce their own live memory/knowledge grants and settlement boundaries.

Connect owns the signed `{ "arguments": {…} }` request and `{ "result": … }` success wrappers. The following keys define closed argument objects; `request`, `holder` and `target` use the corresponding existing SPOKE DTOs. Results use the listed fields and snake-case branch tags:

| Capability id and op | Arguments | Result |
|---|---|---|
| `tools.mindcore.recall_context` | `request` (`MemoryRecallRequest`), positive integer `max_tokens` | `{records, knowledge, tokens_used}` |
| `tools.mindcore.absorb` | `request` (`MemoryCaptureRequest`) | Array of `{status, candidate_id}` with `status` = `pending`, `dropped`, or `settled` (adds `record_ids`) |
| `tools.mindcore.deliberate` | `space_id`, `subject_id`, `holder`, `target`, `evidence` (`[{id, text}]`), positive integer `timeout_ms` | `{status, verdict, judgment}` with `status` = `no_change`, `needs_review` (adds `draft`), or `settled` (adds `record`, `mind_state_version`) |
| `knowledge.adopt` (capability `spoke-knowledge-adoption`, which requires `spoke-knowledge`) | `source_scope_id`, `source_entry_id`, positive integer `source_revision`, `destination_scope_id`, optional `destination_entry_id`, `idempotency_key`, optional `extensions` | `{outcome, entry}` with `outcome` = `adopted` or `already_present`; `entry` is the stored copy either way |

An `absorb` run that updates a record and dismisses its candidate stages two intents in one transaction, the record update and the candidate dismissal, then collects the settled revisions from each staged intent's own result: a revision is yielded only for a record id the result names. The dismissal leg's result names no record, so only the update leg's result yields a revision; walking both legs adds nothing from the dismissal.

**The deliberation judgment.** Every successful `deliberate` response carries a required top-level `judgment` beside `status`, `verdict` and that branch's own members. The projection — target, revision-pinned evidence, outcome, basis, `judgment_id` identity and `created_at` — is [`mind-model.md`](mind-model.md)'s; this page owns only what the seam does with it. The workflow builds and validates the judgment in `crates/mindcore-spi/src/judgment.rs` and the tool boundary forwards it unchanged. A judgment the released validator refuses never reaches the wire: the call answers `InternalError` with `details.kind = "judgment_projection"` and no `retry_class`, and on the `settled` branch that check runs before the cognition record is committed, so a projection defect writes nothing. `spoke-judgment` is advertised exactly when `tools.mindcore.deliberate` is in the final registration list: the family has no operation of its own and gates nothing, so a surface that serves no `deliberate` advertises none.

An `absorb` replay reports what an earlier run of the same capture recorded for each consumed candidate. A candidate consumed by the update leg's dismissal is the one consumption whose result names no record, so the binding reads the update leg's own receipt — keyed by the capture key, the candidate id and `update` — for the record id: a committed receipt reports `settled` with that id and an absent one reports `dropped`. Capture and update receipts are rows of one table under one aged policy, and the binding has no per-operation retention branch; the capture receipt is minted first, so an aged-out capture receipt refuses the replay before an aged-out update receipt could be read.

Two of the three workflow tools are routed, and only they: `deliberate`'s carrier admission and `recall_context`'s knowledge slot. Deliberation settles onto exactly one carrier entry, so `ServeFaces.workflow_routes` maps each exact principal id — not a peer id, and never a value read from the wire — to the collection those operations reach for that principal, and `MemoryWorkflowBinding::with_knowledge_routes` validates the whole map once at assembly and holds it behind one `Arc`; both readers of the routes — the binding's two workflow lookups — read that same map, so no workflow consumer can resolve a different destination for a principal. `absorb` consults no route. Adoption consults no route either: its destination is the knowledge face's own collection, so a copy it writes is one the admitting peer reads back through `knowledge.get`. Each destination collection belongs to exactly one principal route, so a collection named by two routes is refused at assembly; the route never substitutes for a live grant — every carrier write and knowledge read still re-derives the caller's own grants on its own transaction. At carrier admission a principal with no route is refused before any carrier access, as terminal `NotAuthorized` carrying the fixed message `principal has no workflow knowledge route` and no collection, entry, caller id, or other route detail, so it never reveals whether another principal has a route. Recall's slot is independent of the memory page and best effort for the route and the grant: a missing route, or a missing live `knowledge_read` grant in the request's space, returns the memory result with `knowledge: []`, never a refusal. That tolerance covers the route and the grant only — a grant-store read or collection-list failure is not converted into an empty slot; it propagates and fails the recall. With both a route and the grant the slot lists only the routed collection under the same disclosure predicate as the served knowledge list, so an `owner`-private entry never reaches the pack. The host map is the workflow collection allowlist: a valid route need not name a collection that already holds rows, each destination collection is one principal's, and no route is checked against the independent Knowledge face or resolved to a default collection — to change a mapping, assemble a new surface. Assembly returns `InvalidInput` for an empty principal key, an empty destination collection, or a collection named by two routes, before any port or registration exists; an empty map is valid and serves no deliberation, so it installs no `deliberate` handler and advertises neither the capability nor a tool descriptor, and a host registration of that reserved id is dropped rather than advertised or installed. `recall_context` keeps serving without a route map, with no slot to fill, and `absorb` keeps serving because it never consults one. Memory authorization, withholding, snapshot revalidation and token budgeting are unchanged by routing and stay separate rules: the page is still the adapter's own ACL and withholding read, so a memory grant revoked while providers run refuses the reread as before, while a knowledge grant revoked mid-flight empties the reread's slot, which makes the snapshot differ and surfaces as the retryable `RevisionConflict` rather than a released pack.

Settlement returns its original `SpokeReject` (code, message and details); malformed arguments and `SpiError::InvalidInput` return `InvalidInput`. A `PolicyRejected` policy veto is also `InvalidInput`, with `details.kind = "policy_rejected"` and the host's reason as the message — never `NotAuthorized`, which stays the lost-admission code. A deadline returns `InternalError` with `details.kind = "deadline_exceeded"` and `details.retry_class = "retryable"`: nothing committed, so the caller may repeat the call. That no-write guarantee covers only an expiry the SPI observes before handing the settlement to `commit_cognition`; [`mind-model.md`](mind-model.md) owns the boundary, including the store-connection wait the request deadline does not bound (open residual `I-000101`). The other provider errors return `InternalError` with `details.kind` set to `embeddings_unavailable`, `embedding_failed`, `reasoner_unavailable`, `reasoner_failed`, or `invalid_reasoner_output` as appropriate and carry no retry class; do not reflect provider internals into the wire message. Errors use the signed error envelope, never a success DTO. The client must negotiate each tool id it intends to invoke; descriptor `capability_id` and `op` match, and the responder owns the `mindcore` namespace.

### Adoption face

`knowledge.adopt` is the only adoption surface. It is the single registered operation of the
`spoke-knowledge-adoption` family, it requires the co-capability `spoke-knowledge`, and its request
and response are the published 0.18.0 `KnowledgeAdoptRequest` / `KnowledgeAdoptResponse` — no
product request or response type exists. The retired private product adoption tool is removed
rather than kept as a second authority: assembly drops its reserved `tools.mindcore.*` id from every
host registration in both adoption configurations (`crates/mindcore-runtime/src/lib.rs:165`), so no
manifest carries it and no handler answers it even when a host registers the id.

The host enables the face with `ServeFaces.adoption: Option<AdoptionRoute>`, whose `scope_id` names
the configured source on the wire and whose `source` names the `KnowledgeScope` the store reads
(`crates/mindcore-runtime/src/lib.rs:247-257,288-296`). There is no destination setting: the
destination is `ServeFaces.knowledge`, the one collection `knowledge.get`/`list` serve for the
admitted peer, so an adopted copy is always readable back through the face that admitted it.
`workflow_routes` selects workflow collections only and plays no part in adoption. With
`adoption: None` `face_capabilities` adds no capability and no service is composed; with `Some` it
adds `spoke-knowledge-adoption` and `RemoteServePortsComposite::with_adoption` composes
`spoke-connect`'s `AdoptionPortService` over the runtime's `AdoptionService`
(`crates/mindcore-runtime/src/lib.rs:191-214,406-434`).

**Admission is the knowledge face's**, composed as `(principals(peer)?, knowledge_admission(peer)?)`
like the history family's: a peer the knowledge face does not admit cannot adopt, and a routed
principal the knowledge admission refuses answers `NOT_AUTHORIZED` where the removed product tool
would have served it. Neither the principal nor the admitted scope comes from the payload.

**Scope mapping.** Every scope member of the request is compared, never used to select anything:

| Request member | Must equal | Otherwise |
|---|---|---|
| `source_scope_id` | `AdoptionRoute.scope_id` | `NOT_FOUND`, no details — the same generic absence as a missing source |
| `source_entry_id` | a live or retired row of `AdoptionRoute.source` | `NOT_FOUND`, no details |
| `source_revision` | the live source's current revision; never compared for a retired source; never `0` | `REVISION_CONFLICT`, no details / `INVALID_INPUT` |
| `destination_scope_id` | the admitted `Scope.scope_id` | `NOT_AUTHORIZED`, no details, before the source is read |
| `destination_entry_id` | absent → the resolved terminal's `source_entry_id`; a supplied value may name that same id | — |
| `idempotency_key` | 1–128 code points | `INVALID_INPUT`, refused by Connect's decoder |

The destination collection is `ServeFaces.knowledge` and nothing else; the admitted `Scope`'s
`entry_ids`, `entry_types`, `source_id` and `viewpoint` refinements still apply to it, because the
admitted scope is the host's statement about which entries of that collection the peer may reach.
Adoption applies them to the destination entry it would return and to the copy it would write: an
entry outside the admitted selection answers `NOT_AUTHORIZED` with no details and is neither served
nor written, exactly as the routed knowledge face hides it. The host's configured collection selects the
destination; the
admitted selection narrows what the call may touch inside it.

**Assembly** refuses an empty source scope id, an empty source collection, a source collection equal
to `ServeFaces.knowledge`'s collection or to any route destination — including the route of a
principal that is not currently connected — and a source scope id equal to `ServeFaces.scope_id`,
all with `InvalidInput` before any port or registration exists
(`crates/mindcore-runtime/src/lib.rs:105-150`). A source that is also a destination would let one
principal's adopted copy become the shared entry another principal reads, and a source scope equal
to the destination scope would let a principal adopt from its own destination.

**Terminal anchor.** The requested source resolves through `spoke_operations::resolve_retirement_chain`
inside the same transaction before anything is read for the copy: a live source resolves to itself,
and a retired source resolves to the first live entry along its markers, so the copy's
`adoption_anchor` names that terminal's `(source_scope_id, source_entry_id, source_revision)` while
the copy's own `entry_id` is `destination_entry_id` when supplied and the terminal's id otherwise.
A chain that cannot resolve — a cycle, an ambiguous merge, a missing hop, more than the resolver's
32 edges — answers `SOURCE_RETIRED` with `details.retirement_chain`, the visited ids in order. A
retired source's requested revision is not compared, because the anchor records the terminal's own
revision; a live source that moved off the requested revision answers `REVISION_CONFLICT` with no
details. A resolved terminal whose own revision is above Mindcore's served ceiling of `2^53 - 1` has
no conformant success to report, because the anchor is the only carrier of that revision: the
`adoption_anchor.source_revision` member records the terminal's revision and must satisfy the
IEEE-754 safe-integer bound, which is Mindcore's own check and narrower than the numeric check
Connect applies to the request. The copy's own `revision` is never that value — it is the
destination row's write revision, assigned by the store (`1` on a create) — so the response's
`entry.revision` is checked separately on return. The call refuses before writing anything, with the
fixed fault message and `details.kind = "store"`; the copy is never committed for a response the
wire cannot carry.

**Copy and inherited governance.** `body`, `canonical_name`, `entry_type` and `status` come from the
terminal; `modules` loses `cognition` rather than keeping an emptied envelope; `source_anchor` stays
unset; `adoption_anchor` records the terminal triple; `revision`, `created_at` and `updated_at` are
left for the store to assign; the request's `extensions` are not copied. `owner` and `disclosure` are
inherited from the terminal rather than reset to the destination's, because the destination is
`ServeFaces.knowledge` — the collection the knowledge face serves and the access boundary itself —
and the port refuses every source the viewpoint-free predicate does not see, so an inherited policy
cannot disclose anything the admitted principal could not already read. The copy's `entry_id` is the
caller's `destination_entry_id` when supplied — a caller may supply the terminal's own id — and the
terminal's id otherwise; the anchor names the terminal either way.

**Refusals and dedup.** The call reads only the host-configured source and writes only
`ServeFaces.knowledge`, committing on one `BEGIN IMMEDIATE` transaction that commits for `adopted`
and rolls back for `already_present`. `NOT_FOUND`, `NOT_AUTHORIZED`, `REVISION_CONFLICT`,
`SOURCE_RETIRED`, `ADOPTION_ANCHOR_IMMUTABLE` and `INVALID_INPUT` travel as the contract builds
them; a repeat naming the copy's own anchor triple returns `already_present` with the stored copy,
another revision of the same source is `REVISION_CONFLICT` with exactly `details.adopted_revision`,
and any other occupant is `REVISION_CONFLICT` without that member, left byte-identical. A
create-only put that meets an occupancy the preceding read missed re-reads the slot and answers by
the same rule; a re-read answering absence or occupancy is that occupancy, one refusing
`NOT_AUTHORIZED` or `REVISION_CONFLICT` travels as the destination reported it, and one faulting for
storage reasons becomes the fixed `InternalError` below rather than the put's own refusal. Any other
store answer becomes `InternalError` with the fixed message `knowledge adoption could not complete`
and `details.kind` naming the failing stage (`begin`, `commit`, `store` or `response`); the serde,
SQL and stored-payload text stays on the host side and never enters the signed refusal. A stored
`adoption_anchor` revision above `2^53 - 1` is the same fixed `store` fault, never a
`REVISION_CONFLICT` carrying the out-of-range integer — but only when that anchor names the
requested source scope and entry. An unrelated occupant's anchor is never read as this adoption's
revision, so it is the ordinary occupied conflict without `details`. The typed response is built
before the transaction settles, so a copy the response cannot describe rolls the write back instead
of committing one Connect would reject. Connect's `AdoptionPortService` dedups by
`(op, effective scope, requester principal id, idempotency_key)` with the whole-payload digest: a
same-key same-digest replay returns the recorded outcome with `outcome` normalized to
`already_present` and never reaches the port, a same-key different-digest replay answers
`IDEMPOTENCY_CONFLICT`, and a replay of an evicted key answers `RECEIPT_EXPIRED`. The bounded
in-memory default is the shipped store, so its records live for one service instance; see the dedup
consequence above for what a restart exposes.

**Per-peer hiding is a host obligation**, not a runtime setting. Connect signs its hello from one
fixed manifest and the runtime installs the surface's handlers on the responder before inbound
traffic reaches it, while admission learns its peer only from the established session. A host that
must hide adoption from particular peers therefore selects the surface and its matching allowlist
before hello using its own trusted routing, verifies the selected peer against the authenticated
session, and re-evaluates actual admission at call time. The runtime serves one manifest and one
handler set per surface and exposes no per-principal manifest or handler set.

### Store shape

The runtime serves a **protocol-shaped** store: the memory tables the workflows read are the Nexus protocol shape, not the pre-release legacy shape. `MindStore::open` applies the one embedded migration, so a freshly opened store already carries that shape and serves `recall_context` and `deliberate`; the loopback fixture opens that same production path and only seeds the rows the faces read, so it is no longer the only protocol-shaped store. Nexus host-only objects — the writer guards, outbox/immutability triggers, `nexus_writer_*` UDFs and the `core_*` tables — stay absent, so `open` needs no writer runtime. This plan's acceptance is therefore runtime reachability of the served tools against a store the production open path produced.

## Invariants

- No caller-provided principal, scope route, or manifest capability can bypass a host admission or storage guard.
- No baseline provider is required for either face and an absent face/tool is denied. Advertisements and handler registrations describe the same capabilities.
- The adoption source is host configuration and the adoption destination is the knowledge face's own collection: no wire value selects either, the request's scope members are only compared against those two, and a source equal to the destination collection, to any route destination or to the destination scope is refused before serving.
- Ablation: peer admission plus scoped storage are both required. Connect authenticates the peer and bounds the list request; the store applies the admitted collection and entry rules before reading, revealing or mutating data.
- An admitted scope is validated where the face is assembled and again in the store: neither Connect's list merge nor a caller-declared scope may drop or bypass an admitted constraint.
- History grants no access the knowledge face does not: both `history.*` ops admit through the knowledge face's two callbacks and the routed store's own `admit`, so a peer replays and prunes exactly the scope it reads and writes. The `spoke-history` capability is advertised only when `ServeFaces.history` composes the service.
- Every served `deliberate` response carries the judgment its own attempt produced, and every judgment it carries passed the released validator before the call returned; the identity rule [`mind-model.md`](mind-model.md) states holds on the wire, and `spoke-judgment` is advertised exactly with the tool.

## Where it lives

- SQLite scoped store beside `crates/mindcore-sqlite/src/knowledge.rs` and its borrowed-transaction helpers; used both by the Knowledge loopback and by `crates/mindcore-runtime`. The same file implements `HistoryPort` for `RoutedKnowledgeStore` and projects the replay onto `RevisionSnapshot`.
- `crates/mindcore-spoke/src/workflows.rs` owns workflow storage admission and the settlement transaction; `crates/mindcore-spi` owns the workflow contracts and their orchestration.
- `crates/mindcore-runtime/src/lib.rs` owns `ServeFaces` (including `history` and `adoption`), `AdoptionRoute`, the `HistoryAdmission` and `AdoptionAdmission` compositions, the retired-id filter and `face_capabilities`; `spoke-connect`'s `HistoryPortService` and `AdoptionPortService` own the wire decode, the effective-scope check and their dedup stores; `crates/mindcore-runtime/src/adoption.rs` owns the adoption port. `crates/mindcore-runtime/src/tools.rs` owns the served workflow tool registrations and their advertised ABI. `crates/mindcore-runtime/tests/loopback.rs` asserts each advertisement against a signed session, including both history ops and `knowledge.adopt`.

## Change obligations

Changing an advertised face, tool id, argument/result shape, or scope admission rule updates this spec and the signed loopback assertions in the same change. This page is the served history contract's only home: a change to the refusal set, its `details` members, the `RevisionSnapshot` projection, the dedup default or its lifetime updates this page; a change to the local replay or retention semantics those answers carry updates `.agents/specs/data/knowledge-lifecycle.md` as well. This page is also the served adoption contract's only home: a change to the scope mapping, the terminal-anchor rule, the refusal vocabulary, the copy's inherited governance or the idempotency envelope updates this page, `serving-principal-routing.md` and the signed loopback assertions in the same change.
