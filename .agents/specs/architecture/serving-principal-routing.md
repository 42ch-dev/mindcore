# Architecture: Serving per-principal routing

## Scope

How one Mindcore serving surface selects the knowledge collection a workflow call reads and
writes. It does not define memory semantics, the Connect handshake, or carrier record storage.

## Current state

### Admission and route

A serving surface may serve more than one principal. The host supplies a route map keyed by the
principal id its `MemoryAdmission` returns for the authenticated peer. The route names the
`KnowledgeScope` whose collection that principal's carrier entry lives in.

Routing therefore has two independent inputs, and neither is caller-declared:

1. **Admission** — peer → `Principal`, from the host's `MemoryAdmission`.
2. **Route** — `Principal` id → `KnowledgeScope`, from the host's surface configuration.

A caller-supplied principal, scope or collection is never used to select a route.

`ServeFaces.workflow_routes: BTreeMap<String, KnowledgeScope>` carries the map. Keys are exact
principal ids, not peer ids; values are workflow collection scopes. The standalone Knowledge face
still uses its own `ServeFaces.scope_id`, `knowledge`, and `admission`.

The one private lookup, `knowledge_route(&self, principal: &Principal) -> Option<&KnowledgeScope>`,
borrows the scope by `principal_id(principal)` for both carrier admission and the recall knowledge
slot.

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

### Construction

- `MemoryWorkflowBinding::new(pool, reflections_enabled)` starts with an empty private
  `knowledge_routes`; `with_knowledge_routes` validates and takes ownership of the full map.
- An empty principal key or an empty collection is refused when the surface is assembled; the
  refusal does not wait for the first call.
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
  Read-only access beyond that collection is not observable through the served surface and is
  therefore not asserted.
- No refusal discloses another principal's route, collection or entry.
- The wire argument and result objects do not change; only the collection a call resolves to.
- The carrier record format is unchanged.

## Where it lives

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
