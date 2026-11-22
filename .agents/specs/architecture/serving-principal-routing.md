# Architecture: Serving per-principal routing

## Scope

How one Mindcore serving surface selects the knowledge collection a served call reads and writes:
the routed workflow calls and the shared-entry adoption tool. It does not define memory semantics,
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
(`crates/mindcore-runtime/src/lib.rs:230`). Keys are exact principal ids, not peer ids; values are
workflow collection scopes. The standalone Knowledge face still uses its own `ServeFaces.scope_id`,
`knowledge`, and `admission`. `MemoryWorkflowBinding::new(pool, reflections_enabled)` starts with an
empty private `knowledge_routes`; `with_knowledge_routes` validates and takes ownership of the full
map (`crates/mindcore-spoke/src/workflows.rs:289-321`). The one private lookup,
`knowledge_route(&self, principal: &Principal) -> Option<&KnowledgeScope>`, borrows the scope by
`principal_id(principal)` for both carrier admission and the recall knowledge slot
(`crates/mindcore-spoke/src/workflows.rs:336-343`).

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

`tools.mindcore.adopt` reads one host-configured shared collection and writes the admitted
principal's routed collection. `ServeFaces.adoption: Option<AdoptionRoute>` names the source
(`crates/mindcore-runtime/src/lib.rs:188-190,222`); the destination is that principal's own
`workflow_routes` entry, resolved from the same map the workflow binding takes. Neither collection
is named by the request.

- Assembly refuses a source collection that is empty or equal to any route destination, including
  the route of a principal that is not currently connected
  (`crates/mindcore-runtime/src/lib.rs:98-120`). Without that refusal an adopted copy could become
  the shared entry another principal reads.
- An admitted principal without a route is refused `NotAuthorized` with the fixed message
  `principal has no workflow knowledge route` and no target detail
  (`crates/mindcore-runtime/src/tools.rs:294-315`); the call falls back to no collection and never
  reaches another principal's destination.
- The source is read-only to the call and only the routed destination is written, both on one
  `BEGIN IMMEDIATE` transaction, so adoption adds no write outside the routed collection. The
  source handle is a distinct type from the destination handle, and the one write its port must
  expose refuses before any statement runs, so no caller can write the shared collection through
  the source side.

### Construction

- An empty principal key, an empty collection, or one collection named by two routes is refused
  when the surface is assembled; the refusal does not wait for the first call
  (`crates/mindcore-spoke/src/workflows.rs:289-321`). The adoption source is validated in the same
  assembly step, before any port or registration exists.
- The route map is host configuration: it is not persisted, not derived from arguments, and not
  read from the wire.
- The route never substitutes for a live grant: every carrier write and knowledge read still
  re-derives the grants on its own transaction.

### Known limit

A route is fixed for the lifetime of the surface. Changing a principal's collection requires
assembling a new surface; there is no hot reload of the route map. The workspace has no periodic
reconfiguration requirement, so this is recorded rather than implemented.

## Invariants

- A served workflow's knowledge access produces no write, disclosure, revision change, entry-count
  change or knowledge-slot entry outside the collection routed to its own admitted principal.
  Adoption is the one served call that touches a second collection: it reads only the
  host-configured source and writes only the routed destination, and assembly refuses a source that
  is any route destination. Read-only access beyond the routed collection is otherwise not
  observable through the served surface and is therefore not asserted.
- No refusal discloses another principal's route, collection or entry.
- No wire value selects the authenticated principal, the source collection or the destination
  collection: admission supplies the principal, `AdoptionRoute` the source, and the principal's
  route-map entry the destination. Adoption's arguments name one entry and its expected source
  revision, and its result carries that copy, whose stored provenance selects nothing.
- The carrier record format is unchanged.

## Where it lives

- `crates/mindcore-runtime/src/lib.rs` — `ServeFaces.workflow_routes`, `AdoptionRoute`, and the
  assembly-time refusals that validate both.
- `crates/mindcore-spoke/src/workflows.rs` — `with_knowledge_routes` and the one `knowledge_route`
  lookup both routed workflow calls share.
- `crates/mindcore-runtime/src/tools.rs` — the call-time route lookup for adoption and its
  refusals.
- `.agents/specs/architecture/serving-seam.md` — the served tool ABI and the advertisement rule.
- `crates/mindcore-runtime/tests/loopback.rs` — the signed-session assertions for each route and
  each advertisement case.

## Change obligations

Changing the route map shape, the admission-to-route rule, or the collection any served call
resolves updates this page, `serving-seam.md`'s tool ABI, and the loopback assertions in the same
change. A new routed call states here which collection it reads and which it writes, and how it
refuses a principal with no route.

## Known limit

- `crates/mindcore-runtime/src/lib.rs:136-164` — `ServeFaces`; the `workflow_routes` doc comment
  states the workflow collection allowlist and that routes are never read from the wire.
- `crates/mindcore-spoke/src/workflows.rs:89` — `MemoryWorkflowBinding::new`.
- `crates/mindcore-spoke/src/workflows.rs:289-320` — `MemoryWorkflowBinding::with_knowledge_routes`.
- `crates/mindcore-spoke/src/workflows.rs:340-342` — `MemoryWorkflowBinding::knowledge_route`.
- Served tool ids, argument/result shapes and the surrounding face contract:
  [`serving-seam.md`](serving-seam.md).

## Change obligations

Changing admission, route resolution, reachability or collection isolation updates this page and
the served tool table in [`serving-seam.md`](serving-seam.md) in the same change, and keeps the
invariants above true.
