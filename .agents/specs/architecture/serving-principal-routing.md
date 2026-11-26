# Architecture: Serving Mind routing

## Scope

How one Mindcore serving surface selects the knowledge collection and carrier a served call reads
and writes: the request's Mind id is the only route input, and the Mind's registry row resolves it.
It also states which served operations are not composed. It does
not define memory semantics, the Connect handshake, refusal vocabulary or carrier record storage.
The knowledge and history faces serve their ops per Mind; adoption stays uncomposed.

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
arguments, and `scope_id` on a knowledge `Scope`, which the knowledge face's admission takes from
the request. The `minds` row (`crates/mindcore-sqlite/migrations/0002_minds.sql`) is the map: it
names the Mind's knowledge collection and its carrier entry, and no configuration carries that pair
a second time.

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

### Creating

`memory.spaces.create` is Mind-routed in both directions, because it both names and writes the route:

- The request names the Mind: `space_id` is required, because it is the first member of the dedup
  namespace. A new id has no registry row yet, so there is nothing to resolve — the create writes
  it.
- The write is the library's `create` on the host service's one transaction, so the space row, the
  carrier entry, the registry row and the creator's one default grant row are written once, by one
  operation, and the route is persisted rather than configured
  (`crates/mindcore-spoke/src/control.rs`, `create_space`).
- The order is the port's: dedup probe, then the existing-space `admin` requirement, then the freeze
  gate. Only an existing row is admitted and gated; a new id needs no grant, because an
  authenticated principal creates.
- An existing Mind the caller holds no space-wide live `admin` in answers the same target-free
  `NOT_AUTHORIZED` every other Mind-level path answers — never the library's taken-id refusal,
  which names the id.
- That refusal is target-free, but the call's outcome is not: success means the id was free, that
  refusal means it is taken. Since the request names the id and a new id needs no grant, an
  authenticated peer can probe an id and claim a free one first, taking its space-wide `admin`.
  Both inputs are settled decisions (the caller names the `space_id`; an authenticated principal
  creates), so this is the shape rather than a defect here.
- The invariant below therefore covers operations on an existing Mind and stops at id creation,
  where there is no row to admit against before the create writes one. A host that cannot accept
  the signal needs its own create admission or a server-assigned id. The decision is tracked in
  `I-000155`; the served contract states the signal in [`serving-seam.md`](serving-seam.md).

### The knowledge face

`port.knowledge.get`, `port.knowledge.put` and `port.scope.list_knowledge_entries` are Mind-routed
and served when `ServeFaces.knowledge` is true:

- The route input is the request's Mind id: the `scope_id` of the scope a `get` or `put` declares,
  and the required scope of the list op. No selector names a collection, and a request that names
  no scope names no Mind.
- The admission resolves it. The face's callback maps the peer through the host's `principals`
  admission, then calls `admitted_mind` (`crates/mindcore-spoke/src/workflows.rs`) — the caller's
  own live grant rows, then the space row, then the registry row — and refuses an unknown Mind, an
  unadmitted one and one the caller holds no live grant in with the same target-free
  `NOT_AUTHORIZED`. It then binds the caller into the `Scope` it returns
  (`mindcore_sqlite::bind_caller`) and validates it (`validate_admission_scope`); every successful
  return is bound.
- The registry row resolves on every call, not once per surface. `RoutedKnowledgeStore::admit`
  reads `crates/mindcore-sqlite/src/minds.rs`'s `registered_collection` on the transaction the
  op's statement runs on, so the collection is the admitted Mind's registered one and a `scope_id`
  with no registry row is the same target-free refusal. Two requests for two Minds therefore reach
  two collections through one composed face.
- The route never substitutes for a grant: `MindKnowledgePort` authorizes each operation on the
  caller's own live grant in the admitted Mind's space before the store is reached, per
  [`serving-seam.md`](serving-seam.md) § Knowledge.

### The history face

`history.revisions.get` and `history.revisions.prune` are Mind-routed and served when
`ServeFaces.history` is true:

- The route input is the request's Mind id: the `scope_id` of the scope the op declares. Both ops
  carry the scope on every served call, and a request that declares none reaches the admission with
  no selector, which no Mind id matches; the wire never carries a collection.
- The admission resolves it, and it is the knowledge face's callback without the last step: the
  host's `principals` mapping, then `admitted_mind` over the selector — grant rows, space row,
  registry row — and then no caller binding, because Connect compares a present request scope with
  the admitted one for wire equality. The returned scope names the Mind and nothing else, and an
  unknown Mind, an unadmitted one and a frozen one without a grant are one target-free
  `NOT_AUTHORIZED`.
- **Read and write.** `history.revisions.get` reads the admitted Mind's registered collection and
  writes nothing; `history.revisions.prune` writes that same collection — it deletes checkpoints
  below the kept window and moves the entry's `history_floor_revision` — and touches no other
  Mind's rows. `RoutedKnowledgeStore`'s `HistoryPort` resolves the collection through `admit_history`
  on each op's own transaction, so one face serves many Minds and no collection is captured at
  assembly.
- The route never substitutes for a grant: `MindHistoryPort` authorizes each op on the caller's own
  live grant in the admitted Mind's space — `knowledge_read` for a replay, one grant covering
  `knowledge_write` and `knowledge_read` for a prune — and refuses without a covering grant with the
  knowledge face's own fixed `NOT_AUTHORIZED`, no `details`, before any port call. The full refusal
  split is [`serving-seam.md`](serving-seam.md) § History.
- The prune's dedup row lives under the admitted Mind's registered collection as its namespace,
  resolved per call from the retry key, so one Mind's prune cannot replay or consume another Mind's
  key.

`history` composes only with the knowledge face: asking for it without `ServeFaces.knowledge` is an
assembly `InvalidInput` refusal before any port is built.

`knowledge.adopt` stays uncomposed, as below.

### The uncomposed operations

`knowledge.adopt` is the one served operation this runtime does not compose. Its destination is the
knowledge face's own collection and its one-transaction service was removed with Stage 1's face, so
nothing serves it:

- the manifest advertises no `spoke-knowledge-adoption`, and the runtime's `MindPorts` answers
  `None` for the family;
- a call is refused on the responder's dispatch-deny branch — raw wire `capability_missing`,
  surfaced by the Rust dialer as `CapabilityPortMissing` with `details.wire_code`.

It returns per Mind in a later layer, once that service is re-implemented; the pinned `0.19` cohort
already carries request-scoped admission (A1) and the optional scope on knowledge get and put (A2)
(`2026-11-25-one-surface-serves-many-minds.md`), and this runtime composed the knowledge and
history faces that use them. Its contract stays [`serving-seam.md`](serving-seam.md)'s.

### Construction

- There is no route map to validate: `ServeFaces` carries no `workflow_routes`, `scope_id` or
  knowledge collection, and every Mind names its own collection through its row. `ServeFaces.knowledge`
  and `ServeFaces.history` are switches, not routes: they decide which faces are composed, never
  which collection those faces serve.
- The route is persisted state, not host configuration: it is written once by `create` and not
  derived from arguments, and the wire carries only the Mind id.
- The route never substitutes for a live grant, and the grant check is per execution, not per
  call: Mind admission (the peer mapping and the registry row) runs on every call, while the
  caller's own operation-level grant is re-derived only when the operation actually executes.
  A prune whose retry key already holds a recorded outcome — an earlier success or an earlier
  refusal — returns that outcome after admission without invoking the history port, so its grant
  check does not run again; a fresh key runs the port and its check on its own transaction.

## Invariants

- A served call reads and writes only the collection the admitted Mind's registry row names, and no
  result mixes two Minds. The host's own memory family service dispatches its operations under the
  host's admission; which of them it dispatches was always the host's.
- One `NOT_AUTHORIZED` with no `details` answers an unknown Mind id and a Mind the caller holds no
  live grant in, a frozen one included, because admission runs before any existence check. It
  covers operations on an existing Mind; create's outcome, which reports whether an id is free,
  is scoped in § Creating and does not inherit this invariant.
- No refusal discloses another Mind's collection, carrier or entry; the coverage refusal is
  reachable only by a caller already admitted to that Mind, and an existing Mind's create refusal
  names neither the caller, the Mind nor the missing capability.
- A create writes the Mind's route and its default grant on one transaction, and a repeat create
  writes only its own receipt: the route a later call resolves is the one the first create wrote.
- The composed knowledge and history faces resolve their routes per call: each served op reads the
  admitted Mind's registry row on its own statement's transaction, so one face serves many Minds and
  no collection is captured at assembly.
- The one uncomposed operation is never advertised and never served; a restored face re-enters
  composition only per Mind, with `serving-seam.md`'s contract.
- The carrier record format is unchanged.

## Where it lives

- `crates/mindcore-spoke/src/admission.rs` — `admit_mind` (grant rows, then the space row),
  `refuse_coverage`, `refuse_space_coverage` and `gate` (the freeze and subject gates).
- `crates/mindcore-spoke/src/workflows.rs` — `admitted_mind` (the one admission the composed
  knowledge and history faces both use, and the one adoption must reuse when it is composed: grant
  rows, then the space row, then the registry row), `carrier_collection` (the one registry-row
  lookup both readers share), `knowledge_slot`, and `admitted_carrier` / `admitted_carrier_entry`
  (the carrier admissions).
- `crates/mindcore-spoke/src/served_history.rs` — `MindHistoryPort`, the grant-checking history port
  the composed face installs.
- `crates/mindcore-runtime/src/history_dedup.rs` — `SqliteHistoryDedupStore`, whose namespace is the
  admitted Mind's registered collection, resolved per call from the retry key.
- `crates/mindcore-sqlite/src/minds.rs` — the `minds` row, its visibility join, `create` and
  `registered_collection` (the registry read every served knowledge op resolves its route with).
- `crates/mindcore-sqlite/src/knowledge.rs` — `RoutedKnowledgeStore`, its per-call `admit`, and the
  `HistoryPort` impl whose `admit_history` resolves the admitted Mind's collection on the op's own
  transaction.
- `crates/mindcore-spoke/src/control.rs` — `MemoryAdapter::create_space`, the served create that
  resolves no existing route and writes one; `crates/mindcore-runtime/tests/loopback.rs` drives it
  on a signed session.
- `crates/mindcore-runtime/src/tools.rs` — the served `deliberate` and `read_cognition` argument
  objects and their handler entry.
- `crates/mindcore-runtime/src/lib.rs` — `ServeFaces.knowledge` and `ServeFaces.history`,
  `MindcoreServe::scoped_knowledge` and `MindcoreServe::served_history` (the composed faces and
  their admission callbacks, over one shared `admit_peer`), `MindPorts`, the retired-id filter and
  `face_capabilities`.
- `.agents/specs/architecture/serving-seam.md` — the served tool ABI, the per-operation grant rule
  and the advertisement rule.
- `crates/mindcore-runtime/tests/loopback.rs` — the signed-session assertions: two Minds and two
  peers, the unadmitted/absent refusal, the frozen cases, the served knowledge ops with their two
  adversarial loopbacks, the served history ops per Mind with their grant and admission cases, and
  the advertised set.
- `crates/mindcore-spoke/tests/memory_adapter.rs` — the admission-first assertions over every
  Mind-level read path and the registry-derived `memory.spaces.list`.

## Known limit

Memory, the four Mind tools, `port.knowledge.*` and `history.*` route per Mind. `knowledge.adopt`
stays uncomposed until its layer re-implements the one-transaction service it needs, so a peer gets
no adoption operation. A Mind's registry row is written once: changing its collection is a new
Mind, not a reconfiguration, and there is no hot reload.

## Change obligations

Changing admission, route resolution, reachability or collection isolation updates this page, the
served tool table in [`serving-seam.md`](serving-seam.md) and the loopback assertions in the same
change, and keeps the invariants above true. A new served operation states here which collection it
reads and which it writes, and how it refuses a caller with no admission to the named Mind. The
knowledge page `.agents/knowledge/architecture-patterns/serving-principal-routing.md` still
describes the retired principal route map; it is stale against this page and is rewritten with that
corpus, not here.
