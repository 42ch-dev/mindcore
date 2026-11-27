# ADR: The SPOKE gaps are filled additively first, then standardised as upstream capability

Status: proposed — no carrier, feature declaration or conformance test exists; the gap list is agreed and unimplemented
Date: 2026-11-27
Scope: Which upstream capabilities the layered Knowledge read requires, how they are carried without breaking the wire, and what later becomes standard
Related: `2026-11-27-layered-knowledge-retrieval.md`, `2026-11-27-canonical-knowledge-format.md`, `2026-11-27-per-document-versioning.md`, `2026-11-27-full-lifecycle-delivery-and-evaluation.md`

## Problem

A peer can neither ask for a cheaper view nor receive one. `AssembleRequest` carries only `scope`, `max_entries` and `extensions`, and its own description calls `max_entries` an entry limit hint "not enforced by protocol" (`schemas/ops/assemble-request.schema.json`). A knowledge entry's `body` is closed to summary, tags, attributes and computable maps (`schemas/data/knowledge-entry.schema.json`), so there is no skeleton and no span. `SourceAnchor` is closed with `additionalProperties: false` (`schemas/data/source-anchor.schema.json`), so a document version, a section id or a digest cannot be named on an anchor. The history face serves `get` and `prune` only (`spoke-history.md:68-69`), with an open `entity_kind`, so there is no log, no diff and no attribution. No `view_level` or `hydrate` member or operation exists in the pinned schemas or crates.

These are absences in the library and protocol surface, not a demand signal. A consumer cannot ask for a carrier that does not exist, so deferring the work until two consumers appear leaves the target read model unexpressible on the wire and invites each product to invent its own field. Equally, a product that tunnels all of it through private members creates a second selection vocabulary beside `scope_id`/`space_id` and two validators for one contract.

The stake is that the refactor's read model is only useful to a peer if the peer can request a layer, hydrate a span, cite a document version and read its history. If none of that is expressible, Mindcore serves a capability nobody else can name.

## Proposal

The gaps will be filled in two moves: an additive Phase 1 that changes no schema, generated type or ABI, and a later standardisation phase that promotes the validated carriers.

**Phase 1 will use only extension maps, one manifest extension and product tools.** Mind selection will stay on `scope_id` and `space_id`; no Mind routing member will be added, and no second Mind-routing vocabulary will exist.

| Required capability | Observed at the pin | Gap | Phase 1 carrier | Later standard |
|---|---|---|---|---|
| Layered view selection | `AssembleRequest` has no view level; `max_entries` is a non-binding hint (`schemas/ops/assemble-request.schema.json`) | A peer cannot ask for L0 cards, L1 skeletons or L2 spans | A declared feature plus a view hint in the request's `scope.extensions.mindcore`; view content in the entry's `extensions.mindcore` | A standard request member for view level, with conformance cases per layer |
| Span hydration by handle | `KnowledgeEntry.body` is closed; no hydrate member or operation | A selected span cannot be fetched by reference | Handles in the request extension on `port.knowledge.get`; spans in the entry extension, verified by digest | A first-class hydration operation with its own request and response schema |
| Content anchors and version mapping | `SourceAnchor` is closed; span is unit-less start/end (`schemas/data/source-anchor.schema.json`) | An anchor cannot name a document version, a section or a byte digest | Anchor data under `source_anchor.extensions.mindcore`: document version, section id, span digest | Optional standard members behind a negotiated capability |
| Per-document version fence | `revision` is an integer fence on the entry (`schemas/data/knowledge-entry.schema.json`); no statement defines a document-backed revision | A peer cannot rely on what `revision` names when the entry is a document | The host will define `revision` as that document's immutable version and `expected_base_revision` as its parent; existing stale codes will be reused; conformance tests will pin it. No shared Mind sequence will be used | A normative statement for host-owned document revisions and an optional capability flag |
| File history, log and diff | `history.revisions.get` and `prune` only; `entity_kind` is open with core `knowledge_entry` and `relation` (`spoke-history.md:48,68-69`) | No listing, no diff, no attribution; prune semantics do not fit immutable history | Product tools for log and diff, and replay under a product-defined entity kind | An optional capability family for knowledge history, with log, diff and attribution |
| Capability negotiation | Manifest capabilities are an open string array; `extensions` and `tools` exist (`schemas/data/host-capability-manifest.schema.json`) | A peer cannot learn which knowledge views a host serves | `extensions.mindcore.features[]` in the manifest, declared by capability name | A standard feature set for knowledge views and history, with negotiation rules |
| SDK and FFI parity | Only generated schema types are shared across languages; extension maps round-trip as opaque objects | A product-private member has no parity mechanism or conformance test | Phase 1 changes no generated type, so every SDK carries the payload unchanged | Every new member or operation requires generation and conformance in each SDK and the native binding |

One Mindcore-side prerequisite belongs to this record because Phase 1 cannot work without it: the knowledge admission currently refuses any scope namespace other than the caller binding (`crates/mindcore-sqlite/src/knowledge.rs:91-110`), so it must admit one validated namespace owned by Mindcore while still refusing every other. That change is a Mindcore change, not a SPOKE one.

**Later standardisation will promote the carriers that Phase 1 validates.** Each row above will move from an extension to a schema member or operation only with its conformance cases, a version cohort, and generated parity in every SDK. The Phase 2 schedule will be set by the protocol's own compatibility window, not by a consumer count.

## Alternatives considered

- **Wait for two real consumers before defining anything.** Rejected by the owner, and unsound on its own terms: a capability that cannot be requested cannot accrue consumers, so the gate would never open. The gaps are derived from the target read model and the pinned surface, not from demand.
- **A Mindcore-private wire contract.** Fastest to ship and fully under this project's control. Rejected: it would create a second Mind-routing and selection vocabulary beside `scope_id`/`space_id`, duplicate every validator, and split discovery into two stories. The requirement is an upstream capability, not a private tunnel.
- **Add the standard members now and break the wire.** Cleanest end state. Rejected: both objects are closed and generated types exist in several languages, so a new required member is a cohort-wide break with no negotiation path and no way for an old peer to keep working.
- **Keep everything in extensions permanently.** No cohort, no break. Rejected: extension payloads are shape-checked only, so no peer can validate semantics; there is no conformance suite, no SDK parity, and no deprecation path for a private member that later becomes standard.
- **Reuse domain timeline events or forks for document history.** Rejected: document history is about text authored against a document version, while a timeline event or a fork is domain narrative. Mixing them would make one concept answer two questions and would put history back into the hot surface.
- **Serve layered views only through product tools, never through the protocol.** Viable for a single-host product. Rejected: a peer would then need Mindcore-specific knowledge to read a document, which is the private-contract failure with extra steps.

## Acceptance criteria

- A peer that does not declare the feature will receive today's response bytes for the same request; an old-client conformance case will assert byte equality.
- Every row of the matrix will name its Phase 1 carrier, its later standard and the test that closes it; no row will be gated on a consumer count.
- A Phase 1 client will obtain L0 and L1 content, hydrate a span by handle and verify its digest, read an anchor carrying document version and section id, and read a document's history by version.
- A per-document stale fence will answer with the existing stale code and that document's current version; no Mind-wide value will appear anywhere in the response.
- A request for a feature the host did not declare will be refused with a named reason rather than silently downgraded.
- A static check will show Mind selection still uses `scope_id` and `space_id` only; no second routing member will exist.
- The later standardisation will exist as a written proposal with schema text, conformance cases and SDK parity tests, tracked as planned work.

## Risks

- **Extension payloads are unvalidated in depth.** Peers check the namespace key and that the value is an object; inner members are unchecked. Mitigation: a version key inside the namespace, a conformance suite, and promotion to a schema once the shape settles.
- **Peers may ignore a hint and answer the default view.** Mitigation: a required feature is declared, and a host that cannot honour it refuses with a named reason instead of returning something cheaper than asked.
- **The later cohort may slip.** Mitigation: Phase 1 already delivers the read model to peers; the standardisation stage is planned work with its own acceptance, not an optional extra.
- **SDK lag.** Mitigation: Phase 1 touches no generated type; Phase 2 will not merge before every SDK's generation and parity test exists.
- **Namespace ownership disputes.** Mitigation: the manifest already has a `namespaces` member; Mindcore declares its namespace there and no other product may serve it.
- **A host may define revision semantics differently.** Mitigation: the semantics are stated normatively and pinned by conformance; a peer that needs them requires the later capability flag rather than assuming.
