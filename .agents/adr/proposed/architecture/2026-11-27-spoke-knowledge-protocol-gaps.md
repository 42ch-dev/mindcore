# ADR: The SPOKE gaps are filled additively first, then standardised as upstream capability

Status: proposed — no carrier, receiver path, feature declaration or conformance test exists; the gap list is agreed and unimplemented
Date: 2026-11-27
Scope: Which upstream capabilities the layered Knowledge read and the write path require, which request or operation receives each in Phase 1, and what later becomes standard
Related: `2026-11-27-layered-knowledge-retrieval.md`, `2026-11-27-canonical-knowledge-format.md`, `2026-11-27-per-document-versioning.md`, `2026-11-27-machine-owned-knowledge-writes.md`, `2026-11-27-full-lifecycle-delivery-and-evaluation.md`

## Problem

A peer can neither ask for a cheaper view nor receive one. `AssembleRequest` carries only `scope`, `max_entries` and `extensions`, and its own description calls `max_entries` an entry limit hint "not enforced by protocol" (`schemas/ops/assemble-request.schema.json`). A knowledge entry's `body` is closed to summary, tags, attributes and computable maps (`schemas/data/knowledge-entry.schema.json`), so there is no skeleton and no span. `SourceAnchor` is closed with `additionalProperties: false` (`schemas/data/source-anchor.schema.json`), so a document version, a section id or a digest cannot be named on an anchor. The history face serves `get` and `prune` only (`spoke-history.md:68-69`), with an open `entity_kind`, so there is no log, no diff and no attribution. No `view_level` or `hydrate` member or operation exists in the pinned schemas or crates.

The first draft listed those gaps but did not close the receiver: it did not say which request returns L0 and L1, how a pinned handle differs from a stale current view, or how the local assembly and the native binding receive the new shapes. A gap list without a receiver is not fillable, and a peer cannot tell a host's omission from a refusal.

Two more absences matter. A commit can succeed before its response is lost, and the peer has no way to ask whether its own write landed; replaying the put is refused before the adapter, so the answer is indistinguishable from a competing writer. And the local paths — the assembling workflow and the native-binding delivery form — are not covered by a schema change that only remote peers receive.

These are absences in the library and protocol surface, not a demand signal. A consumer cannot ask for a carrier that does not exist, so deferring the work until two consumers appear leaves the target read model unexpressible on the wire and invites each product to invent its own field.

## Proposal

The gaps will be filled in two moves: an additive Phase 1 that changes no schema, generated type or ABI, and a later standardisation that promotes the validated carriers.

**Phase 1 will use only extension maps, one manifest extension and product tools, and every gap has a named receiver.** Mind selection will stay on `scope_id` and `space_id`; no Mind routing member and no second Mind-routing vocabulary will exist.

**The Phase 1 round trip will be direct, with no new operation.**

1. **L0 and L1.** `port.scope.list_knowledge_entries` will accept a view hint in `scope.extensions.mindcore.view_level` of `l0` or `l1`. The response will carry the card or the skeleton in each entry's `extensions.mindcore`. A no-hint list will keep today's response for dynamic entries and will return a document-backed entry with its card and version only, never its sections.
2. **L2.** `port.knowledge.get` will accept `scope.extensions.mindcore.span_handles[]` and return `extensions.mindcore.spans[]` with the text, section id, document version, byte range and digest. A handle will be **pinned**: the response will be that version's bytes or not found, and it will never answer `ViewStale`.
3. **Current views.** `ViewStale` will apply only to a view opened at a head and compiler version that has since moved — a reading of the current projection, not a pinned handle. A host that serves only the direct round trip need not expose a current view at all.

**The later standard will add the remaining receivers.** The assembling workflow will receive L1 through a view hint on the assemble request and a view extension on the assemble packet, served by a host that composes the spec read leg; a knowledge-only host will continue to refuse assemble, as today. The native-binding delivery form will receive the scoped knowledge face before it can serve two same-named documents in different Minds, and that is an explicit later stage, not an assumption. The standardisation will also promote the receipt and history carriers below.

| Required capability | Observed at the pin | Gap | Phase 1 carrier and receiver | Later standard |
|---|---|---|---|---|
| Layered view selection | `AssembleRequest` has no view level; `max_entries` is a non-binding hint (`schemas/ops/assemble-request.schema.json`) | A peer cannot ask for L0 cards, L1 skeletons or L2 spans | View hint in the list request scope; card and skeleton in the entry extension, received by `port.scope.list_knowledge_entries` | A standard request member and packet extension; the assemble host serves it |
| Span hydration by handle | `KnowledgeEntry.body` is closed; no hydrate member or operation | A selected span cannot be fetched by reference | Handles in the get request scope, spans in the response entry extension; received by `port.knowledge.get`; pinned, never stale | A first-class hydration operation with its own request and response schema |
| Content anchors and version mapping | `SourceAnchor` is closed; span is unit-less start/end (`schemas/data/source-anchor.schema.json`) | An anchor cannot name a document version, a section or a byte digest | Anchor data under `source_anchor.extensions.mindcore`, received on the entry's anchor | Optional standard members behind a negotiated capability |
| Per-document version fence | `revision` is an integer fence on the entry (`schemas/data/knowledge-entry.schema.json`); no statement defines a document-backed revision | A peer cannot rely on what `revision` names when the entry is a document | The host will define `revision` as that document's immutable version and `expected_base_revision` as its parent; existing stale codes will be reused; conformance tests will pin it. No shared Mind sequence will be used | A normative statement for host-owned document revisions and an optional capability flag |
| Uncertain-write receipt | No receipt operation; a replay is refused before the adapter | A peer cannot tell whether its own write landed | A declared `write_receipt` feature and a product tool, admitted with the caller's grant, keyed by request id and digest; received by the same peer that wrote | A standard receipt lookup, negotiated and caller-authorized |
| File history, log and diff | `history.revisions.get` and `prune` only; `entity_kind` is open with core `knowledge_entry` and `relation` (`spoke-history.md:48,68-69`) | No listing, no diff, no attribution; prune semantics do not fit immutable history | Product tools for log and diff, and replay under a product-defined entity kind; received by `tools.<ns>.history_*` | An optional capability family for knowledge history, with log, diff and attribution |
| Capability negotiation | Manifest capabilities are an open string array; `extensions` and `tools` exist (`schemas/data/host-capability-manifest.schema.json`) | A peer cannot learn which knowledge views, receipt or history a host serves | `extensions.mindcore.features[]` in the manifest, declared by capability name; received by the peer at hello | A standard feature set and negotiation rules |
| Multi-language and FFI parity | Only generated schema types are shared across languages; extension maps round-trip as opaque objects | A product-private member has no parity mechanism or conformance test; the native binding has no scoped knowledge face | Phase 1 changes no generated type and uses tools, so every SDK carries the payload unchanged | Every new member or operation requires generation and parity; the native binding exposes the scoped face before two same-named documents can be served |

One Mindcore-side prerequisite belongs to this record because Phase 1 cannot work without it: the knowledge admission currently refuses any scope namespace other than the caller binding (`crates/mindcore-sqlite/src/knowledge.rs:91-110`), so it must admit one validated namespace owned by Mindcore while still refusing every other. That change is a Mindcore change, not a SPOKE one.

**Later standardisation will promote the carriers that Phase 1 validates.** Each row will move from an extension to a schema member or operation only with its conformance cases, a version cohort, and generated parity in every SDK and the native binding. Its schedule will be set by the protocol's compatibility window, not by a consumer count.

## Alternatives considered

- **Wait for two real consumers before defining anything.** Rejected by the owner, and unsound on its own terms: a capability that cannot be requested cannot accrue consumers, so the gate would never open. The gaps are derived from the target read model and the pinned surface.
- **Extend `Assemble` now and let it be the only receiver.** Fewer carriers. Rejected: a knowledge-only host serves no assemble, the packet is a closed object, and an assembly-only path would leave direct list and get unable to return a view.
- **Define the shapes and leave the receiver implicit.** The previous draft's gap list. Rejected: a peer cannot distinguish an unimplemented view from a refusal, and no conformance test can be written against "some request".
- **A Mindcore-private wire contract.** Rejected: it would create a second Mind-routing and selection vocabulary beside `scope_id`/`space_id`, duplicate every validator, and split discovery into two stories.
- **Add the standard members now and break the wire.** Rejected: both objects are closed and generated types exist in several languages, so a new required member is a cohort-wide break with no negotiation path.
- **Keep everything in extensions permanently.** Rejected: extension payloads are shape-checked only, so no peer can validate semantics; there is no conformance suite, no parity, and no deprecation path.
- **Treat a replayed put as the receipt.** Rejected: the responder pre-reads every put and refuses a repeated create or update before the store runs, so a replay cannot distinguish "my write landed" from "someone else's did".
- **Reuse domain timeline events or forks for document history.** Rejected: document history is text authored against a document version, while a timeline event or fork is domain narrative; mixing them would make one concept answer two questions.
- **Serve layered views only through product tools.** Rejected: a peer would need Mindcore-specific knowledge to read a document, which is the private-contract failure with extra steps.

## Acceptance criteria

- A peer that does not declare the feature will receive byte-identical responses for every dynamic entry, and a collection with no document-backed entry will be byte-identical overall; an old-client conformance case will assert both.
- The Phase 1 round trip will be exercised end to end: a list with `view_level` `l0` returns cards only, with `l1` returns cards and skeletons, then a get with handles returns spans whose text hashes to their digests. A commit between the skeleton and the hydration will leave the hydrated bytes at the pinned version and will not answer `ViewStale`.
- A request for `l2` without handles, or for a view level the host did not declare, will be refused with a named reason rather than silently downgraded.
- A dropped create or update response will be resolved by the receipt lookup as `committed` without replaying the put; another principal's request for that id will answer as unknown, and an in-flight attempt will answer `in_flight`.
- A per-document stale fence will answer with the existing stale code and that document's current version; no Mind-wide value will appear.
- The later standardisation will exist as a written proposal with schema text, the assemble packet carrier, the scoped FFI path, conformance cases and SDK parity tests, tracked as planned work.
- Every row of the matrix will name its Phase 1 receiver, its later standard and the test that closes it; no row will be gated on a consumer count.
- A static check will show Mind selection still uses `scope_id` and `space_id` only; no second routing member will exist.

## Risks

- **Extension payloads are unvalidated in depth.** Peers check the namespace key and that the value is an object; inner members are unchecked. Mitigation: a version key inside the namespace, a conformance suite, and promotion to a schema once the shape settles.
- **A peer may ignore a hint.** Mitigation: a required feature is declared, and a host that cannot honour it refuses with a named reason instead of returning something cheaper than asked.
- **The direct round trip needs two requests.** L0 to L2 is a list followed by a get. Mitigation: it is the Phase 1 path by design; the assemble carrier is the later one-request path for hosts that compose it.
- **The native binding lags.** Mitigation: Phase 1 does not touch the ABI and serves the same shapes through tools; the scoped face is a named stage with its own acceptance, not an assumption.
- **The later cohort may slip.** Mitigation: Phase 1 already delivers the read model and the receipt; standardisation is planned work with its own acceptance.
- **A host may define revision semantics differently.** Mitigation: the semantics are stated normatively, pinned by conformance, and a peer that needs them requires the later capability flag.
- **Namespace ownership disputes.** Mitigation: the manifest already has a `namespaces` member; Mindcore declares its namespace there and no other product may serve it.
