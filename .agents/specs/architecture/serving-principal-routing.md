# Architecture: Serving Mind routing

## Scope

How one Mindcore serving surface selects the knowledge collection and carrier a served call reads
and writes: the request's Mind id is the only route input, and the Mind's registry row resolves it.
It also states which served operations are not Mind-routed and are therefore not composed. It does
not define memory semantics, the Connect handshake, refusal vocabulary or carrier record storage.

## Current state

### One route input: the Mind

A serving surface serves many Minds from one assembly, and neither the assembly nor the wire names a
collection. Routing has two inputs, and only the second is caller-declared:

1. **Admission** — authenticated peer → `Principal`, from the host's `MemoryAdmission`. Never a wire
   value.
2. **Route** — the request's Mind id → the registry row's collection and carrier entry. Never a
   caller-named collection.

A Mind's id is its space id (`2026-11-25-mind-is-one-memory-space.md`), so the selectors SPOKE
already carries express it: `space_id` on a memory request and on the served workflow tools'
arguments, and `scope_id` on a knowledge `Scope` once that face returns. The `minds` row
(`crates/mindcore-sqlite/migrations/0002_minds.sql`) is the map: it names the Mind's knowledge
collection and its carrier entry, and no configuration carries that pair a second time.

### Resolution and admission

Every Mind-level operation reaches the store through an adapter entry, and each begins by looking
the Mind up. `admission::admit_mind` (`crates/mindcore-spoke/src/admission.rs`) is that lookup:
the caller's own grant rows in the space first, then the space row. No live grant and no space row
answer one `NOT_AUTHORIZED` with a fixed message and no `details`, so an unknown Mind, an
unadmitted one and a frozen one the caller holds no grant in are one answer.

Coverage is checked after admission (`refuse_coverage`), and the freeze gate after that
(`gate`): a capability the caller's grant does not cover is a distinct refusal, and a frozen Mind
answers `SpaceFrozen` only to an operation that is freeze-gated and outside SPOKE's maintenance
allowlist, while serving every operation that is not freeze-gated. Only a caller that passed
admission reaches either check.

The carrier paths resolve through the registry row on the caller's transaction, after the same
order — `admit_carrier_location` (`crates/mindcore-spoke/src/workflows.rs`) is the shared first
step: a live grant of the caller's (any capability), then the space row, then the registry row. A
`space_id` with no registry row answers the admission refusal before any carrier is read.

The two carriers then differ by what their operation earns first. The cognition read
(`admitted_carrier`) adds the space-wide `knowledge_read` coverage after those steps, because it has
no subject page ahead of the carrier. The host-side `deliberate` handler (`admitted_carrier_entry`)
adds no coverage at all: the workflow reads and revalidates the named subject's page and only then
checks `knowledge_read`/`knowledge_write` on the carrier, so the refusal a request earns first — the
subject's, or the capability's — is the one a caller sees.

### Deliberation

`deliberate` settles onto exactly one carrier entry, so it needs exactly one collection per call.

- The collection and carrier are the registry row's, resolved through
  `MemoryWorkflowBinding::carrier_collection` (`crates/mindcore-spoke/src/workflows.rs`) on the
  caller's transaction; a binding an in-process facade opened may pin its own Mind first. A space
  with no row answers the admission refusal before any carrier access.
- The request does not name the carrier: `holder` is not a member of the served argument object, so
  a peer that still sends it meets the closed object's `InvalidInput`.
- The route fixes only the collection. Existing holder/assessor identity rules still decide whether
  a write is accepted, and a destination collection belongs to one Mind, so a cross-Mind carrier
  write is unreachable.

### Recall

`recall_context` is authorized by the caller's own live `recall` grant; its knowledge slot is
additional, not a condition of the call.

- The slot lists the collection the request's `space_id` row names, under that principal's own live
  `knowledge_read` grant in the same space.
- A space with no registry row, or no live `knowledge_read` grant, receives the memory records with
  an empty knowledge slot. The call does not fail because a row or grant is absent.
- Grant-store read and collection-list failures are not authorization and still fail the call.

`absorb` consults no registry row: it captures and reviews memory in the named space and writes no
carrier.

### Listing

`memory.spaces.list` takes the same admission: its set is the registry's join over the caller's live
grants (`crates/mindcore-sqlite/src/minds.rs`, `visible_mind_ids`), in `id` order, so a peer lists
exactly the Minds it may operate. A frozen Mind is listed with its `frozen` status; a space with no
registry row is not listed even under a live grant. The wire shape, cursor and marker are unchanged.

### The un-routed operations

`port.knowledge.*`, `history.*` and `knowledge.adopt` cannot name a Mind: the retained
`RoutedKnowledgeStore` (and its `HistoryPort`) binds one collection for its lifetime, and adoption's
destination is the knowledge face's own collection. Stage 1 therefore composes none of them:

- the manifest advertises no `spoke-knowledge`, `ke-ownership`, `spoke-history` or
  `spoke-knowledge-adoption`, and the runtime's `MindPorts` answers `None` for every family but
  memory;
- a call to each is refused on the responder's dispatch-deny branch — raw wire `op_unsupported` for
  the knowledge family, `capability_missing` for history and adoption, each surfaced by the Rust
  dialer as `CapabilityPortMissing` with `details.wire_code`.

They return per Mind in Stage 2, once SPOKE carries request-scoped admission and an optional scope on
knowledge get and put (`2026-11-25-one-surface-serves-many-minds.md`). Their contracts stay
[`serving-seam.md`](serving-seam.md)'s.

### Construction

- There is no route map to validate: `ServeFaces` carries no `workflow_routes`, `scope_id`,
  `knowledge` or knowledge admission, and every Mind names its own collection through its row.
- The route is persisted state, not host configuration: it is written once by `create` and not
  derived from arguments, and the wire carries only the Mind id.
- The route never substitutes for a live grant: every carrier write and knowledge read still
  re-derives the caller's own grants on its own transaction.

## Invariants

- A served call reads and writes only the collection the admitted Mind's registry row names, and no
  result mixes two Minds. The host's own memory family service dispatches its operations under the
  host's admission; which of them it dispatches was always the host's.
- One `NOT_AUTHORIZED` with no `details` answers an unknown Mind id and a Mind the caller holds no
  live grant in, a frozen one included, because admission runs before any existence check.
- No refusal discloses another Mind's collection, carrier or entry; the coverage refusal is
  reachable only by a caller already admitted to that Mind.
- The three un-routed operations are never advertised and never served; a restored face re-enters
  composition only per Mind, with `serving-seam.md`'s contract.
- The carrier record format is unchanged.

## Where it lives

- `crates/mindcore-spoke/src/admission.rs` — `admit_mind` (grant rows, then the space row),
  `refuse_coverage`, `refuse_space_coverage` and `gate` (the freeze and subject gates).
- `crates/mindcore-spoke/src/workflows.rs` — `carrier_collection` (the one registry-row lookup both
  readers share), `knowledge_slot`, and `admitted_carrier` / `admitted_carrier_entry` (the carrier
  admissions).
- `crates/mindcore-sqlite/src/minds.rs` — the `minds` row, its visibility join and `create`.
- `crates/mindcore-runtime/src/tools.rs` — the served `deliberate` and `read_cognition` argument
  objects and their handler entry.
- `crates/mindcore-runtime/src/lib.rs` — `MindPorts`, the retired-id filter and `face_capabilities`.
- `.agents/specs/architecture/serving-seam.md` — the served tool ABI and the advertisement rule.
- `crates/mindcore-runtime/tests/loopback.rs` — the signed-session assertions: two Minds and two
  peers, the unadmitted/absent refusal, the frozen cases and the advertised set.
- `crates/mindcore-spoke/tests/memory_adapter.rs` — the admission-first assertions over every
  Mind-level read path and the registry-derived `memory.spaces.list`.

## Known limit

Stage 1 routes memory and cognition per Mind only. `port.knowledge.*`, `history.*` and
`knowledge.adopt` stay un-routed until Stage 2, so a peer gets no knowledge, history or adoption
operation. A Mind's registry row is written once: changing its collection is a new Mind, not a
reconfiguration, and there is no hot reload.

## Change obligations

Changing admission, route resolution, reachability or collection isolation updates this page, the
served tool table in [`serving-seam.md`](serving-seam.md) and the loopback assertions in the same
change, and keeps the invariants above true. A new served operation states here which collection it
reads and which it writes, and how it refuses a caller with no admission to the named Mind. The
knowledge page `.agents/knowledge/architecture-patterns/serving-principal-routing.md` still
describes the retired principal route map; it is stale against this page and is rewritten with that
corpus, not here.
