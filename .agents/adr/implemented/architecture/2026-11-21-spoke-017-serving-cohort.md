# ADR: Adopt the SPOKE 0.17 serving cohort and a library-shaped Mind surface

Status: implemented — 0.17 makes every Connect family optional and separates Knowledge into its own capability, so Mindcore can serve Memory and Knowledge without a baseline provider or a second accept loop
Date: 2026-11-21

## Problem

On 0.16.1 a Memory-only Connect responder was unrepresentable: `RemoteServePorts` required `BaselinePorts`, so any composition needed a baseline declaration or a fake baseline port, and knowledge listing still rode the baseline `ScopeQueryPort`. Independent Knowledge serving was therefore impossible to advertise honestly. Mindcore had no serving surface at all, so remote consumers could not reach its workflows, and the two planning issues (`I-000019`, `I-000008`, later `I-000034`) were blocked on an upstream release rather than on product work.

## Decision

The workspace pins `spoke-schemas`, `spoke-operations` and `spoke-connect` to `=0.17.0`. Knowledge is served through the standalone `spoke-knowledge` capability: a production `RoutedKnowledgeStore` implementing `ScopedKnowledgePort` over the Mindcore pool, with one host-configured route from an admitted `scope_id` to exactly one `KnowledgeScope.collection`. Peer admission maps the authenticated peer to a typed `Scope` and is validated both where the face is assembled and in the store.

`crates/mindcore-runtime` is a library crate. `MindcoreServe` assembles the workflow engine, the knowledge face and a host-supplied `RemoteMemoryService` into `RemoteServePortsComposite::knowledge_only(face).with_memory(service)`, derives the `HostCapabilityManifest` from the faces and handlers it actually serves, and returns ports, manifest and handler registration to a host-owned `ConnectResponder`. The host supplies identity, transport, trusted peers and non-capability metadata; the crate opens no listener and owns no accept loop. A pure memory responder composes a local `RemoteServePorts` whose only override is `as_memory`, because 0.17 exposes no public memory-only composite constructor.

The three Mind workflows are served as `tools.mindcore.{recall_context, absorb, deliberate}` with closed argument and status-tagged result schemas. Handlers take the peer from the authenticated responder session and resolve the `Principal` through the host admission; they never read identity from arguments. A deliberation settles onto a carrier entry, so the host names one caller principal with `ServeFaces.workflow_caller`, and an unwired route is neither advertised nor installed.

The runtime serves a **protocol-shaped** store. A freshly opened `MindStore` is not one yet, because `0001_init.sql` still creates the pre-protocol memory tables; that gap is tracked as `I-000068` and is out of this decision's scope.

## Alternatives considered

- **Stay on 0.16.1 with a declared or fake baseline provider.** Rejected: it advertises a capability the host does not serve and forces a stub into a path whose whole point is absence honesty.
- **Ship a standalone serving binary with its own accept loop.** Rejected: the acceptance for `I-000034` forbids a second accept loop, and a transport-owning binary would duplicate the host's connection lifecycle.
- **Serve knowledge through an ANN extension (`sqlite-vec`).** Rejected: `vec0` is a flat exact index with no approximate index, it would require distributing a C extension per platform, and it conflicts with the embeddable-library promise.
- **Implement serving in TypeScript or in the Nexus repository.** Rejected: the contracts, storage and tests live in this Rust workspace; a cross-repository change cannot be verified here.

## Consequences

- The migration surface was small: the compile probe and the real build found no Rust breakage, because Mindcore implements none of the affected baseline traits. The 0.17 pin is therefore cheap to hold.
- Runtime reachability is proven on protocol-shaped stores only; until `I-000068` lands, a host that opens a fresh store cannot serve the workflows. The seam spec states this explicitly so a green loopback is never read as production-open evidence.
- A served `deliberate` binds one caller principal to one collection. Per-principal routing is a later shape change, not an extension of this one.
- The tool descriptors carry the pinned cognition schema inline (51 KB) so a manifest-only client can build valid arguments without fetching an external document; the alternative, a remote `$ref`, was rejected because it is unusable offline.
- Test-only observability (the query-slot probe, the scoring probe) stays behind dev-only features; `cargo tree --no-dev-dependencies` must show neither in a production graph.
