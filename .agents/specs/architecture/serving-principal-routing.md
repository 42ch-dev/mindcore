# Architecture: Serving per-principal routing

## Scope

How one Mindcore serving surface selects the knowledge collection a served call reads and writes:
the routed workflow calls and the shared-entry adoption face. It does not define memory semantics,
the Connect handshake, or carrier record storage.

## Current state

### The map

A serving surface may serve more than one principal. The host supplies a route map keyed by the
principal id its `MemoryAdmission` returns for the authenticated peer. The route names the
`KnowledgeScope` whose collection that principal's carrier entry lives in.

Routing therefore has two independent inputs, and neither is caller-declared:

1. **Admission** — peer → `Principal`, from the host's `MemoryAdmission`.
2. **Route** — `Principal` id → `KnowledgeScope`, from the host's surface configuration.

A caller-supplied principal, scope or collection is never used to select a route.

`ServeFaces.workflow_routes: BTreeMap<String, KnowledgeScope>` carries the map
(`crates/mindcore-runtime/src/lib.rs:297-304`). Keys are exact principal ids, not peer ids; values are
workflow collection scopes. The standalone Knowledge face still uses its own `ServeFaces.scope_id`,
`knowledge`, and `admission`. `MemoryWorkflowBinding::new(pool, reflections)` starts with an
empty private `knowledge_routes`; `with_knowledge_routes` validates and takes ownership of the full
map (`crates/mindcore-spoke/src/workflows.rs:295-326`). The one private lookup,
`knowledge_route(&self, principal: &Principal) -> Option<&KnowledgeScope>`, borrows the scope by
`principal_id(principal)` for both carrier admission and the recall knowledge slot
(`crates/mindcore-spoke/src/workflows.rs:346-348`).

### Deliberation

`deliberate` settles onto a carrier entry, so it needs exactly one routed collection per call.

- The route is resolved from the admitted principal inside the binding; the carrier write reads and
  CAS-writes only that collection.
- The route fixes only the collection. Deliberation reads the carrier entry the request's `holder`
  names inside it, so the existing holder/assessor identity rules decide whether a write is accepted;
  a holder id that exists only in another collection is refused `knowledge_entry_not_found` and
  writes nothing.
- Each destination collection belongs to exactly one principal route: assembly refuses a collection
  named by two routes. The request's `holder` names the entry inside that one collection, so a
  cross-principal carrier write is unreachable.
- A principal with no route is refused `NotAuthorized` with no target details. The refusal never
  reveals whether another principal has a route.
- A surface with no route does not advertise `tools.mindcore.deliberate` and installs no handler
  for it. Memory-face availability is independent: it is served only when the host configures it.

### Recall

`recall_context` is authorized by the caller's own live `RECALL` grant; the knowledge slot is
additional, not a condition of the call.

- The slot lists the collection routed to the admitted principal, under that principal's own live
  `KNOWLEDGE_READ` grant in the request's space.
- A principal with no route, or no live `KNOWLEDGE_READ` grant, receives memory records with an
  empty knowledge slot. The call does not fail because a route or grant is absent.
- Grant-store read and collection-list failures are not authorization and still fail the call.

### Adoption

`knowledge.adopt` reads one host-configured shared collection and writes the collection the
knowledge face itself serves. `ServeFaces.adoption: Option<AdoptionRoute>` names the source by its
wire `scope_id` and its collection (`crates/mindcore-runtime/src/lib.rs:247-257,288-296`); the
destination is `ServeFaces.knowledge`, the one collection `knowledge.get`/`list` read for the
admitted peer. Adoption does not consult `workflow_routes`: that map selects a *workflow* collection
per principal, and the knowledge face takes no per-principal collection, so a route-derived
destination could land a copy where no knowledge-face read reaches it. Neither collection is
selected by a request member: the two scope members are only compared against the configured source
and the admitted scope.

- Assembly refuses an empty source scope id, an empty source collection, a source scope id equal to
  `ServeFaces.scope_id`, and a source collection equal to `ServeFaces.knowledge`'s collection or to
  any route destination, including the route of a principal that is not currently connected
  (`crates/mindcore-runtime/src/lib.rs:105-150`). Without the collection refusal an adopted copy
  could become the shared entry another principal reads, or a copy of the destination itself; the
  scope refusal keeps a principal from adopting out of its own destination scope.
- Admission is the knowledge face's, composed as `(principals(peer)?, knowledge_admission(peer)?)`:
  a peer outside either callback gets that callback's refusal, so a principal the knowledge
  admission refuses answers `NOT_AUTHORIZED` instead of adopting. A principal with knowledge
  admission but no workflow route adopts normally.
- The destination collection is fixed by the host, so every admitted principal adopts into the one
  collection its own knowledge-face reads serve; the admitted scope's entry selection narrows what
  the call may serve or write inside it. Both come from host configuration, never from the payload.
- The source is read-only to the call and only that destination is written, both on one
  `BEGIN IMMEDIATE` transaction, so adoption adds no write outside the destination collection. The
  source handle is a distinct type from the destination handle, and the one write its port must
  expose refuses before any statement runs, so no caller can write the shared collection through
  the source side.
- The source resolves through its retirement chain before the copy is read, so the write lands a
  copy whose anchor names the terminal, never a retired hop.

The scope mapping, the refusal vocabulary and its wire codes, the terminal-anchor rule, the served
revision domain, the copy's inherited governance and the idempotency envelope are
[`serving-seam.md`](serving-seam.md)'s alone; this page states only which collection the route
selects and how admission reaches it.

### Construction

- An empty principal key, an empty collection, or one collection named by two routes is refused
  when the surface is assembled; the refusal does not wait for the first call
  (`crates/mindcore-spoke/src/workflows.rs:295-326`). The adoption source is validated in the same
  assembly step, before any port or registration exists.
- The route map is host configuration: it is not persisted, not derived from arguments, and not
  read from the wire.
- The route never substitutes for a live grant: every carrier write and knowledge read still
  re-derives the grants on its own transaction.

## Invariants

- A served workflow's knowledge access produces no write, disclosure, revision change, entry-count
  change or knowledge-slot entry outside the collection routed to its own admitted principal.
  Adoption is the one served call that touches a second collection: it reads only the
  host-configured source and writes only the knowledge face's own collection, which every peer
  admitted to that scope shares: it holds one slot per destination entry id, the first adoption of a
  given id claims it, a repeat is served that stored copy only at the anchor revision the copy
  records, and a peer whose own admitted selection admits it reads the copy through that face;
  assembly refuses a source that is the destination collection or any route destination. Read-only
  access beyond the routed collection is otherwise not observable through the served surface and is
  therefore not asserted.
- No refusal discloses another principal's route, collection or entry.
- No wire value selects the authenticated principal, the source collection or the destination
  collection: admission supplies the principal, `AdoptionRoute` the source, and `ServeFaces.knowledge`
  the destination. Adoption's arguments name one source entry, its expected source
  revision, an optional destination entry id and an idempotency key, and its result carries that
  copy, whose stored provenance selects nothing.
- The carrier record format is unchanged.

## Where it lives

- `crates/mindcore-runtime/src/lib.rs` — `ServeFaces` (`:261-309`) with `workflow_routes`
  (`:297-304`) and `adoption` (`:288-296`), `AdoptionRoute` (`:247-257`), the adoption admission
  composition (`:195-208`), and the assembly-time refusals in `MindcoreServe::surface`
  (`:105-150`), the adoption source checks among them (`:107-150`).
- `crates/mindcore-spoke/src/workflows.rs` — `MemoryWorkflowBinding::new` (`:92-99`),
  `with_knowledge_routes` (`:295-326`) and the one `knowledge_route` lookup (`:346-348`) both
  routed workflow calls share.
- `crates/mindcore-runtime/src/adoption.rs` — the adoption port: the destination handle over the
  knowledge face's collection, the admitted-selection predicate, and the one `BEGIN IMMEDIATE`
  transaction around the chain resolution and the copy.
- `.agents/specs/architecture/serving-seam.md` — the served tool ABI and the advertisement rule.
- `crates/mindcore-runtime/tests/{loopback,surface}.rs` — the signed-session assertions for each
  route and each advertisement case, and the surface-level adoption-reads-back test.

## Known limit

A route, and the knowledge face's own collection, are fixed for the lifetime of the surface.
Changing either requires assembling a new surface; there is no hot reload of the route map. The
workspace has no periodic reconfiguration requirement, so this is recorded rather than implemented.

## Change obligations

Changing admission, the route map shape, route resolution, reachability or collection isolation
updates this page, the served tool table in [`serving-seam.md`](serving-seam.md) and the loopback
assertions in the same change, and keeps the invariants above true. A new routed call states here
which collection it reads and which it writes, and how it refuses a principal with no route.
