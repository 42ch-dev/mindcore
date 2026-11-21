# ADR: Per-principal workflow routes on one serving surface

Status: implemented — one surface serves many principals by mapping each admitted principal id to the one knowledge collection its workflow calls reach, so no caller-declared route or per-call resolver is needed
Date: 2026-11-21
Class: architecture
Related: `2026-11-21-spoke-017-serving-cohort.md`

## Problem

A serving surface may serve more than one principal, and the two routed workflows each need exactly one knowledge collection per call: `deliberate` settles onto one carrier entry, and `recall_context` fills one knowledge slot. Binding the surface to a single caller principal made a multi-principal host assemble one surface per principal, and each surface would have to duplicate its responder, tool registration and accept-loop configuration.

The collection cannot come from the request either. A caller-supplied principal, scope or collection would let the caller select its own route, which is exactly the value the host's admission exists to decide. What is left is a mapping the host fixes before traffic arrives — and the shape of that mapping decides whether routing needs resolver state or a second accept path.

## Decision

One surface serves many principals through a host-configured route map. `ServeFaces.workflow_routes: BTreeMap<String, KnowledgeScope>` maps each exact principal id — not a peer id, and never a value read from the wire — to the collection the routed workflow calls reach for that principal (`crates/mindcore-runtime/src/lib.rs:158`). `MemoryWorkflowBinding::with_knowledge_routes` validates the whole map once at assembly and takes ownership of it (`crates/mindcore-spoke/src/workflows.rs:286-311`); the map is immutable for the binding's lifetime, and repeated destination collections are valid.

Both consumers resolve the route through the one lookup `MemoryWorkflowBinding::knowledge_route` (`crates/mindcore-spoke/src/workflows.rs:329-331`), so the carrier read and its CAS append stay pinned to one collection and recall's slot can never resolve a different one. `absorb` consults no route.

A principal with no route is refused at carrier admission before any carrier access, as terminal `NotAuthorized` carrying the fixed message `principal has no workflow knowledge route` and no route detail (`crates/mindcore-spoke/src/workflows.rs:372-388`). Recall's slot is independent of the memory page and best effort: a missing route, or a missing live `knowledge_read` grant in the request's space, returns the memory result with `knowledge: []` rather than a refusal, while a grant-store or collection-list failure still fails the call (`crates/mindcore-spoke/src/workflows.rs:429-450`).

An empty map is valid and serves no deliberation: the surface advertises neither `tools.mindcore.deliberate` nor its handler, including a host registration with that reserved id (`crates/mindcore-runtime/src/lib.rs:95-109`). An assembly with an empty principal key or an empty destination collection is the host's input error and returns `InvalidInput` before any port or registration exists.

The route is a collection allowlist, not an authorization grant: every carrier write and knowledge read still re-derives the caller's own live grants on its own transaction, so two principals may share a collection without per-holder isolation inside it.

## Alternatives considered

- **One surface per principal, each with its own configured scope.** Rejected: a surface assembled per principal duplicates its ports, manifest, tool registrations and responder registration for every principal. It may reuse the same host Memory service — the host passes clones of one `Arc<dyn RemoteMemoryService>` — so Memory is not what gets multiplied; the assembly is. One route map keeps a single assembly serving many principals.
- **A request-time resolver consulted per call.** Rejected: it reintroduces route resolution on every call and needs state the surface must keep coherent, for a mapping that is fixed host configuration. Validating and taking the map once at assembly makes an invalid route a startup refusal instead of a per-call error.
- **Let the request name the principal's collection.** Rejected: a caller-declared route is a caller-chosen scope. Selection must follow the authenticated peer through the host's admission, never a wire value, and the refusal must not reveal another principal's route.
- **Fall back to a default collection when a principal has no route.** Rejected: a silent default would disclose or write into a collection the host did not configure for that principal. Carrier admission refuses instead, and recall returns an empty slot.

## Consequences

- One surface serves many principals with no caller-declared routes and no resolver state. The cost is immutability: changing a mapping means rebuilding the surface, and the current one neither hot-reloads nor falls back.
- `ServeFaces.workflow_caller` is deleted; a served `deliberate` resolves its carrier collection from the admitted principal's route.
- An empty route map leaves Memory-face availability unchanged — Memory is served only when `faces.memory` is configured — and `absorb` and `recall_context` keep serving with no routed knowledge slot; only `deliberate` is suppressed from the advertisement and the handler set.
- No refusal discloses another principal's route, collection or entry, and no workflow reaches a collection outside its own admitted principal's route.
- Hosts that need carrier isolation between principals configure distinct collections; a shared destination grants no per-holder isolation.

## References

- Spec: `.agents/specs/architecture/serving-seam.md` § Workflow tools (the routed operations and their refusal semantics)
- `.agents/adr/implemented/architecture/2026-11-21-spoke-017-serving-cohort.md` — the serving cohort whose single-caller shape this replaces
