# Serving a surface

Use `mindcore-runtime` when a remote SPOKE peer should reach the store through Connect. If your
process embeds the library and calls the ports directly, stop at
[Quick start](quickstart.md) and [Concepts](concepts.md); the runtime only assembles a served
surface, it does not add storage behavior.

## What the runtime does and does not do

It composes the workflow engine, the routed knowledge face, the host's memory service, and the
optional history and adoption faces into `MindcoreSurface`: the ports, a manifest and the tool
handlers a `ConnectResponder` needs. It opens no listener, transport or identity, and it never
invents a journal publisher or an admission.

## Build the serve handle

```rust
use mindcore_runtime::MindcoreServe;

let serve = MindcoreServe::new(
    store,
    false, // matches the memory adapter's reflection setting
    |targets| {
        // publish the journal targets the adapter staged
        Ok(())
    },
);
```

- The store is the same `MindStore` the knowledge face serves. Workflow calls and knowledge ops
  therefore see one snapshot boundary.
- `publish` is the host's journal publisher. A settlement leg without one is refused by the
  binding; the runtime will not supply a no-op.
- The engine never opens a transaction across provider work; each workflow keeps its own
  `SpiError` classification.

## Fill `ServeFaces`

| Field | What the host supplies | Constraint |
| --- | --- | --- |
| `manifest` | hello metadata: host id, roles, namespaces | `capabilities` and `tools` are replaced with what is served; every other member stays yours |
| `scope_id` | the one route id this surface answers | admission must return exactly this id |
| `knowledge` | the collection the knowledge face serves | fixed at assembly |
| `admission` | peer id → admitted `Scope` | never read from the wire |
| `memory` | your `ServedMemory` service | `None` composes and advertises no memory face |
| `history` | serve `spoke-history` over the same routed store | `false` composes and advertises none |
| `principals` | peer id → `Principal` for workflows | the same mapping the memory face uses |
| `providers` | embedder, reasoner and domain lens | injected into each workflow call |
| `adoption` | the one shared source collection | see below |
| `workflow_routes` | principal id → its one workflow collection | the allowlist; each destination belongs to one principal |
| `tools` | extra host tools | the runtime refuses its own ids and the retired adoption id |

`workflow_routes` maps the exact principal id — not a peer id — the host admitted. A principal
without a route reads no knowledge and never writes a carrier; an empty map serves no deliberation
and advertises none. Each destination collection must belong to exactly one route, or assembly
refuses.

## Adoption route

`AdoptionRoute` names a shared source collection and the scope id peers use to reach it. Assembly
refuses when:

- the scope id or collection is empty,
- the source scope id equals the destination scope id,
- the source collection is the knowledge face's collection,
- the source collection is any `workflow_routes` destination, including a route of a principal
  that is not currently connected.

The destination is the knowledge face's collection, shared by every peer admitted there: one slot
per `destination_entry_id`, first adoption claims it. Adoption reuses the knowledge face's
admission, so a peer the knowledge face does not admit cannot adopt, and every copy is readable
through the same `knowledge.get`/`list`.

## Assemble, then register

```rust
use spoke_connect::remote::{connect_responder, ConnectResponderOptions, RemoteIdentity};

let surface = match serve.surface(faces) {
    SpokeResult::Ok(surface) => surface,
    SpokeResult::Reject(reject) => return Err(reject.into()),
};

let responder = connect_responder(ConnectResponderOptions {
    transport: host_transport,
    identity: RemoteIdentity { seed: host_seed },
    manifest: surface.manifest.clone(),
    allowlist,
    peer_keys,
    ports: Some(surface.ports.clone()),
    invoke_timeout_ms: Some(10_000),
})
.await;

// Before inbound hello traffic can reach the responder.
surface.register(&responder)?;
```

`connect_responder` starts its own receive task and never installs handlers from the manifest.
Register before the first dialer connects: a dialer may invoke an advertised tool as soon as the
session is established.

## What gets advertised

The manifest follows the composed faces:

- `spoke-knowledge` and `ke-ownership` always (the knowledge face applies SPOKE's owner-private
  disclosure, so a viewpoint-scoped list is truthful).
- `spoke-judgment` when the deliberation tool is served.
- `spoke-memory` when a memory service is attached; `spoke-memory-transfer` only when that service
  reports every one of `TRANSFER_OPERATIONS`. A bare `RemoteMemoryService` reports none.
- `spoke-history` when `history` is true; `spoke-knowledge-adoption` when an adoption source is
  configured.
- Tools `recall_context`, `absorb` and `deliberate` under the `mindcore` namespace, plus host tools.

The retired product tool `tools.mindcore.adopt` is dropped from every registration: no manifest
advertises it and no handler answers it. `knowledge.adopt` replaces it; migration details are in
[Migrating from `tools.mindcore.adopt`](migrating-from-tools-adopt.md).

## Admission validation

The admitted scope is validated where the face is assembled, because Connect's list merge keeps
only the request's cognition selection: a selection the face does not serve must be refused before
the merge can drop it. The admitted id must also be the one this surface serves — the routed store
refuses any other id.

Host admission is not peer input: an unsupported admitted selection is refused as the same generic
`NotAuthorized` a rejecting resolver returns, never as the caller-declared `InvalidInput` the
routed store keeps for request scopes. The peer learns nothing about what the face does not serve.

## Rotating an embedding identity

`MindcoreSurface::retire_embedding_identity` clears the engine's in-memory query slot and deletes
the stored vectors for that identity. Call it between recalls: a recall already in flight is not
drained, and a fill that commits afterwards recreates rows the call deleted.

## Verify the wiring

The repository's `loopback` test (`crates/mindcore-runtime/tests/loopback.rs`) runs a signed
in-process Connect session against a real SQLite store, including the negative cases: an
unattached face advertises nothing and denies its ops on the wire, and an unserved tool answers
`op_unsupported`.
