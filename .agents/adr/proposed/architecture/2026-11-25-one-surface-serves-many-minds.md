# ADR: One served surface serves many Minds

Status: proposed — one served surface serves many Minds, reached by Mind id, in two stages with a recorded fallback; the owner settled it on 2026-11-25, the surface still serves one collection, and the SPOKE-side change the second stage needs is proposed and not assumed
Date: 2026-11-25
Class: architecture
Supersedes: `2026-11-21-principal-routing.md` — its principal-keyed route map, once this is implemented
Related: `2026-11-21-spoke-017-serving-cohort.md`, `2026-11-25-mind-is-one-memory-space.md`, `.agents/specs/architecture/serving-principal-routing.md`, `.agents/specs/architecture/serving-seam.md`

## Problem

A served surface is assembled around one knowledge collection. `ServeFaces.scope_id` is "the one configured route", `ServeFaces.knowledge` is "the collection that route serves", and `ServeFaces.admission` maps each peer to one scope (`crates/mindcore-runtime/src/lib.rs`). `port.knowledge.get`, `port.knowledge.put` and the list operation read and write that collection for every admitted peer, `history.*` serves the same store, and `knowledge.adopt` writes it. The two routed workflows reach a collection through `ServeFaces.workflow_routes`, a host-configured map from principal id to collection that is fixed for the surface's lifetime (`.agents/specs/architecture/serving-principal-routing.md`).

So one process cannot let a peer operate several Minds or list them: a host that wants more than one Mind assembles one surface per Mind. A peer has no wire value that names a Mind; the workflow tools name memory coordinates, and every `deliberate` names its carrier by `holder` (`crates/mindcore-runtime/src/tools.rs`). And because reach is host configuration, a Mind created after assembly is unreachable until the surface is rebuilt.

## Proposal

One served surface serves many Minds, reached by Mind id. This is the design proposed to SPOKE for evaluation (`https://github.com/42ch-dev/spoke/issues/222`); the fallback named below takes over if SPOKE declines either of its two items.

The Mind id rides the selectors SPOKE requests already carry: `space_id` on a memory request and `scope_id` on a knowledge `Scope`. A Mind's id is its space id (`2026-11-25-mind-is-one-memory-space.md`), so no new wire member is needed.

**Resolution and admission.** The wire never names a collection. For each call Mindcore resolves the Mind id in one registry lookup to a space, a collection and a carrier, admits the (principal, Mind) pair, and only then binds the collection to a port.

- Memory: a live grant covering the operation, in that space. This is today's rule.
- Knowledge, history and adoption: a live `knowledge_read` or `knowledge_write` grant in the Mind's space, answered as that Mind's admitted `Scope`.
- Workflow tools, each where it reaches the adapter (`crates/mindcore-spoke/src/workflows.rs`): `recall_context` checks `recall`, and `knowledge_read` only to fill its knowledge slot. `absorb` checks `capture`, `review` to read the candidate and `recall` for its page, then `review` with `records_write` on one grant to review, `records_write` to update and `review` to dismiss. `deliberate` checks `recall` over a `subject_all` page, `knowledge_read` and `knowledge_write`.

**Refuse, do not filter.** Kept (`docs/concepts.md`). Each call admits one Mind, and the handler binds a port to the resolved collection, so no result mixes two Minds. Mind admission precedes every existence check: an unknown Mind id, a retired Mind and a Mind the peer holds no grant for answer one `NOT_AUTHORIZED`, with no `details`, before any subject, entry or carrier is read. Today the memory path answers `NOT_FOUND` for an unknown space and `NOT_AUTHORIZED` for an ungranted one (`crates/mindcore-spoke/src/admission.rs`, `space` and `refuse_coverage`), which lets a peer tell whether a Mind exists.

**Stage 1, no SPOKE change.** A first release may ship it before the SPOKE-side items exist.

- `memory.spaces.list` lists Minds.
- The memory family and the three workflow tools take a Mind id, in place of `space_id` and `holder`; `subject_id` stays.
- A new `tools.mindcore.read_cognition` returns a Mind's current judgment and derived `MindState`.
- One `NOT_AUTHORIZED` answers an unknown, retired or unadmitted Mind before any existence check.

**Stage 2, two SPOKE-side items.** A1 is request-scoped admission: the admission callback receives the `scope_id` a request declares. It is additive and source-compatible. A2 is an optional `scope` on `port.knowledge.get` and `port.knowledge.put`, with a negotiable marker saying the responder honours it; it is a wire change, so a new SPOKE cohort. Stage 2 turns on knowledge, history and adoption per Mind. Until both items exist, those three operations keep one collection per peer and are documented as not Mind-routed.

**The fallback.** If SPOKE declines A1 or A2, the route is Design T: every Mind-scoped operation is a `tools.mindcore.*` tool whose argument object starts with `mind_id`, with no SPOKE change.

**Records and pages this changes.** `ServeFaces.workflow_routes` is replaced by the registry's Mind-to-collection row, which retires the route `2026-11-21-principal-routing.md` records. Its rules that outlive the map hold per Mind: the wire never names a collection, a refusal carries no target details, and a route never substitutes for a live grant. `.agents/specs/architecture/serving-principal-routing.md` gains the request's Mind id as a route input and loses the map, and the knowledge page `.agents/knowledge/architecture-patterns/serving-principal-routing.md` describes that map and goes stale with it. `.agents/specs/architecture/serving-seam.md` changes the tool argument table — `mind_id` on three tools, `space_id` and `holder` removed, one row added — and, in Stage 2, the half of the knowledge contract that says `get` and `put` carry no wire scope. In `2026-11-21-spoke-017-serving-cohort.md` the tool list and the routing sentence are brought up to date. `docs/serving.md` loses its `workflow_routes` row, and `docs/concepts.md` states the closed existence gap.

## Alternatives considered

- **Design T: every Mind-scoped operation a `tools.mindcore.*` tool, no SPOKE change.** Rejected as the primary route and kept as the fallback. It restates `port.knowledge.*`, `history.*` and `knowledge.adopt` as six product tools, which is a second operation surface inside the tool namespace and reverses the migration that replaced `tools.mindcore.adopt` with `knowledge.adopt` (`.agents/specs/architecture/serving-seam.md`). The surface would still carry the per-peer `port.knowledge.*` path beside those tools, and a tool's arguments pass outside SPOKE's scope and cognition gates. The chosen route rides selectors SPOKE already has and keeps the peer on standard operations.
- **One responder per Mind, no SPOKE change.** Rejected: it is not one surface serving many Minds. Each Mind gets its own surface, manifest, handler set and one signed session per peer, a peer has no way to list Minds, and the hello manifest repeats for every (peer, Mind) pair. Whether an unadmitted Mind's address differs from an unknown one is left to the host's transport.
- **One collection, with Minds separated per entry, no SPOKE change.** Rejected: SPOKE's scope predicate has no entry-ownership term and an admitted scope is fixed per peer, so two peers could not be held to different Minds. It also gives up the one-collection-per-Mind binding of `2026-11-25-mind-is-one-memory-space.md`.
- **Ship no peer form until both SPOKE-side items exist.** Rejected: it ties a first release to a SPOKE release this repository does not schedule. Stage 1 needs no SPOKE change and gives a peer per-Mind memory and cognition; what it costs is that knowledge, history and adoption stay one collection per peer until Stage 2.

## Acceptance criteria

- One served surface serves more than one Mind. A call naming a Mind resolves its space, knowledge collection and carrier through the host's admission, and a peer lists and operates only the Minds it is admitted to.
- A signed loopback test runs two Minds and two peers and asserts that an unadmitted Mind answers the same refusal as an absent one, and that adoption still writes only the surface's configured destination.
- A peer with no Mind is refused without target details.
- Stage 1 is served on the pinned SPOKE cohort: `memory.spaces.list` lists Minds, the memory family and the three workflow tools take a Mind id, and `tools.mindcore.read_cognition` returns current judgment and `MindState`.
- A recall, and every other memory read that looks a space up, answers one refusal with one code and no target details for an unknown Mind id, a retired Mind and a Mind the caller holds no grant for; one test pins the three cases on that answer.
- `.agents/specs/architecture/serving-principal-routing.md` and `.agents/specs/architecture/serving-seam.md` are updated in the same change, and the operations that are not Mind-routed are documented as such.

## Risks

- **Before Stage 2 a peer gets per-Mind memory and cognition only.** `port.knowledge.*`, `history.*` and `knowledge.adopt` answer from the surface's one collection, which no Mind owns. A first release that ships Stage 1 alone says so.
- **The fallback waits on an answer that may not come.** Design T takes over only when SPOKE declines A1 or A2. An evaluation left open neither triggers the fallback nor delivers Stage 2, and no date triggers it.
- **The tool argument change breaks existing peers.** The argument objects are closed and the advertised schema lists `required` members (`crates/mindcore-runtime/src/tools.rs`), so a peer built on today's three tools fails after the change. It is a Mindcore release change, not a SPOKE one.
- **A2 without its marker misroutes silently.** A responder that ignores the new `scope` drops it, and the read lands in the peer's default collection. The marker is part of the ask and is not optional for Mindcore.
- **Admission stays synchronous and bounded.** The registry lookup behind a Mind admission runs under the responder's serve budget and must fit inside it.
- **Closing the existence gap changes answers tests may pin.** The memory path answers `NOT_FOUND` for an unknown space today (`crates/mindcore-spoke/src/admission.rs`), and the adapter tests that pin it change with it.
- **Undecided: whether a many-Mind surface composes the operations that are not Mind-routed.** Until Stage 2, `port.knowledge.*`, `history.*` and `knowledge.adopt` either stay on the surface's one collection or are not composed at all. The owner has not chosen.
- **Undecided: the grant rule for a knowledge put that carries cognition records.** `port.knowledge.put` appends `modules.cognition.records` on the admitted scope alone (`crates/mindcore-sqlite/src/knowledge.rs`, `served_put`), where `deliberate` requires `knowledge_write` (`crates/mindcore-spoke/src/workflows.rs`). The asymmetry exists today, and routing knowledge per Mind would carry it to every admitted Mind. The rule is settled before knowledge is routed per Mind.
- **A SPOKE peer does not create Minds.** `create` is not a wire operation — the adapter's space operations are `list_spaces`, `freeze_space` and `unfreeze_space` (`crates/mindcore-spoke/src/control.rs`) — so the default set of `2026-11-25-create-default-grant-set.md` reaches a peer only for a Mind an embedding process created. Whether a peer may create a Mind is undecided.
- **Design T, if taken, costs more than Stage 2**: eight new tools, six of which restate standard operations, and a second knowledge path on the surface.
