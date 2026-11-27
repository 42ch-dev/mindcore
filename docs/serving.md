# Serving a peer

This is the reference page for the SPOKE-peer form: a peer reaches a Mind over a signed SPOKE
Connect session. `mindcore-runtime` assembles what is served; `mindcore-daemon` is the reference
host that serves it over a Unix socket. If your process embeds the library and calls the facade
directly, stop at [Quick start](quickstart.md) — the runtime only assembles a served surface and
adds no storage behavior.

It is a reference, not a tutorial: the host obligations, the daemon's process contract and what
the assembled surface advertises.

## Two layers

- `mindcore-runtime` composes the workflow engine, the per-Mind knowledge face, the host's memory
  service and the optional history and adoption faces into a `MindcoreSurface`: the ports, a
  manifest and the tool handlers a `ConnectResponder` needs. It opens no listener, transport or
  identity, and it never invents a journal publisher or an admission.
- `mindcore-daemon` is the reference host. It owns the process contract — the TOML config, the
  single-instance lock, the store, the journal root, the Unix-socket listener and the drain — and
  supplies what the runtime leaves to a host.

Every form reads and writes the same store through `mindcore-sqlite`, and the cross-process
contract is SPOKE Connect. No HTTP surface exists.

## The reference host

```sh
cargo run --locked -p mindcore-daemon -- --config daemon.toml
```

The binary takes `--config <path>` and `--help`; the file is read once, at start.

```toml
store_path = "mind.db"
socket_path = "mind.sock"
host_id = "wren-bookshelf"
history = false

[identity]
# 64 hex digits, mode 0600: a group- or other-accessible seed is a start failure.
seed_file = "responder.seed"

[journal]
dir = "journal"

[invoke]
# Optional; absent keeps spoke-connect's own default.
timeout_ms = 10000

[peers.wren]
public_key = "<64 hex digits>"
principal = "caller_wren"

# Optional, and present as a whole or not at all.
[adoption]
scope_id = "shared_library"

[adoption.source]
collection = "ke_shared_library"
```

The top level and every table deny unknown keys, so a typo is a start failure rather than a
silently ignored setting. One `[peers.<peer_id>]` row carries both the key the peer's hello must
prove and the `Principal` it answers as, so the allowlist, the peer keys and the principal mapping
cannot disagree.

### Start and stop

The phases run in one order, and every phase begins only after the previous one succeeded:

1. Read and validate the config.
2. Resolve the store's canonical path once, and take the single-instance lock on
   `<store>.lock` **before** the store is opened.
3. Open the store through `MindStore::open`, which runs the embedded migration.
4. Create the journal root and sync its chain, so every entry is durable before the first
   publication writes into it. Directories this start creates are `0700`.
5. Assemble the surface and install both shutdown signals.
6. Bind the Unix socket (`0600`) and run the accept loop. One accepted stream gets its own
   transport and responder in its own task; the responder's first inbound frame is withheld until
   the surface's handlers are registered, so a peer cannot establish a session into an empty
   handler registry.
7. On `SIGTERM` or `SIGINT`, drain: close every live session, stop accepting, remove the socket
   name — only while it is still the socket this daemon bound — close the pool and release the
   lock. A clean drain exits `0`.

A second instance on the same store is refused by the lock and exits `2`; every other start
failure exits `1`. A store reachable under more than one name is refused rather than locked. A
failed start leaves no listener behind: it removes the socket name it bound, unless the socket's
directory is unwritable, in which case the refusal names the leftover path. What else a failed
start may leave behind is the lock file, a store the lock created and the journal root. A socket
name occupied by anything that is not a socket is refused with its file type and never unlinked; a
socket established to be dead is replaced.

One frame on the wire is a 4-byte big-endian length followed by exactly that many body bytes, and
one frame is one Connect envelope. A length of `0` or larger than 16 MiB is a framing fault that
closes the session. `UnixSocketTransport` is public, so a host that speaks to the daemon builds
the client half from the same type.

### What the daemon composes

| Face | Composed | Carries |
| --- | --- | --- |
| knowledge | always — `ServeFaces.knowledge: true` | `port.knowledge.get`, `port.knowledge.put`, `port.scope.list_knowledge_entries` per Mind |
| history | `history = true` | `history.revisions.get`, `history.revisions.prune` per Mind |
| adoption | `[adoption]` present | `knowledge.adopt` (capability `spoke-knowledge-adoption`) |
| memory family | never — `ServeFaces.memory` is `None` | memory is reached through the tools below, not through `spoke-memory` |
| tools | always | `tools.mindcore.recall_context`, `tools.mindcore.absorb`, `tools.mindcore.deliberate`, `tools.mindcore.read_cognition` |
| host tools | none | no extra registration |
| providers | none — `MindProviders::default()` | no embedder, reasoner, policy hook or domain lens |

Because no provider is configured, the tool that needs one is advertised but cannot complete: a
`tools.mindcore.deliberate` call answers `InternalError` with
`details.kind = "reasoner_unavailable"`. A host that needs deliberation assembles its own surface
with the providers it has (below).

The manifest's `host_id` is the config's, its roles are `["data-store"]`, and its capabilities and
tools are derived from what is served; the daemon supplies no capability string of its own.

## What the runtime assembles

```rust
use mindcore_runtime::MindcoreServe;

let serve = MindcoreServe::new(
    store,
    false, // matches the memory adapter's reflection setting
    |targets| {
        // Must publish every staged target. Returning Ok(()) without writing loses them.
        publish_journal_targets(targets)
    },
);
```

`reflections` matches the memory adapter instance the host serves. `publish` is the host's journal
publisher: a settlement leg without one is refused by the binding, and the runtime never supplies a
no-op. The engine runs on the same pool the store serves, so a tool call and a registry read see
one snapshot boundary; it never opens a transaction across provider work.

### Fill `ServeFaces`

| Field | What the host supplies | Constraint |
| --- | --- | --- |
| `manifest` | hello metadata: host id, roles, namespaces | `capabilities` and `tools` are replaced with what is served; every other member stays the host's |
| `memory` | your `ServedMemory` service | `None` composes and advertises no memory face; `spoke-memory-transfer` is earned only by reporting all five transfer operations |
| `knowledge` | whether the per-Mind knowledge face is composed | `true` advertises `spoke-knowledge` and `spoke-knowledge-scoped` together; the face serves every Mind from one composition and resolves the collection from the registry row on every call |
| `history` | whether the per-Mind history face is composed | requires `knowledge`: `true` here with `false` there is an assembly `InvalidInput` refusal before any port exists |
| `adoption` | `Some(AdoptionRoute)`: the one shared source collection, or `None` | describes the source only; see below |
| `principals` | peer id → `Principal` | the mapping the workflow tools and every composed face admit through; never read from the wire |
| `providers` | embedder, reasoner, policy hook and domain lens | injected into each workflow call; a tool whose provider is absent answers the workflow's own refusal |
| `tools` | extra host tools | every descriptor is advertised and installed except the runtime's reserved ids — its four tool ids and the retired `tools.mindcore.adopt` — which are dropped |

There is no route map. A served call names a Mind through the wire (`space_id`, or the knowledge
scope's `scope_id`), admission maps the peer to its `Principal`, and the Mind's registry row names
the collection and the carrier. No host-supplied field selects a collection per principal.

The knowledge, history and adoption admissions are synchronous callbacks that Connect runs on its
blocking pool; they drive the store through `Handle::block_on`, so the host must run the responder
on a **multi-thread** runtime. A single-thread runtime cannot run the callback's store read.

### Assemble, then register

```rust
let surface = match serve.surface(faces).await {
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
session is established. The daemon closes the same window from the transport side by withholding
the first frame until registration succeeds.

### Rotating an embedding identity

`MindcoreSurface::retire_embedding_identity` clears the engine's in-memory query slot and deletes
the stored vectors for that identity. Call it between recalls: a recall already in flight is not
drained, and a fill that commits afterwards recreates rows the call deleted.

## Adoption route

`AdoptionRoute` names a shared source collection and the host-owned scope id peers use to reach it.
It describes the source side only; the destination of every call is the collection the admitted
Mind registered.

Assembly refuses, before any port or registration exists, when:

- the scope id or collection is empty,
- the source collection is the collection of any Mind registered at assembly,
- the source scope id is the id of any Mind registered at assembly,
- the route is configured without `knowledge`.

Every registered Mind is a possible destination, so a collision with one is refused whatever Mind
holds it. A Mind created afterwards can still take a colliding id or collection, and assembly has
already run: the port repeats both registry checks inside the adoption transaction, after the
grant check and before the source is read, and answers the generic `NOT_FOUND`. The invariant
therefore holds without a reassembly, and the caller cannot tell the collision from a missing
source.

The request's `destination_scope_id` selects which Mind the call is admitted against — Connect
hands it to the admission callback as the selector, and that Mind's registry row supplies the
collection. No wire value selects a collection: neither side of a copy is chosen by a request
member. Adoption reuses the knowledge face's admission, so a peer the knowledge face does not
admit cannot adopt, every copy is readable through the same `knowledge.get`/`list`, and a peer
holding no live grant covering both `knowledge_write` and `knowledge_read` in that Mind is refused.

## What gets advertised

The manifest follows the composed faces and the registrations, never a host-supplied capability
string:

- `spoke-memory` when a memory service is attached; `spoke-memory-transfer` only when that service
  reports every one of the five transfer operations. A bare `RemoteMemoryService` reports none.
- `spoke-knowledge` and `spoke-knowledge-scoped` together when `knowledge` is true, and neither
  otherwise — without the marker a per-Mind face could not serve a scoped `get` or `put` at all.
  `ke-ownership` is never advertised: every admitted scope carries no `viewpoint`, so the flag
  cannot be honoured and a request naming one is refused rather than served.
- `spoke-history` when `history` is true, and never otherwise.
- `spoke-knowledge-adoption` when an adoption source is configured.
- `spoke-judgment` when the `tools.mindcore.deliberate` tool is registered, and one capability id
  per registered tool — the runtime's four, plus any host tool that survives the reserved-id
  filter. The `mindcore` namespace is advertised when at least one tool is registered.
- The retired product tool `tools.mindcore.adopt` is dropped from every registration: no manifest
  advertises it and no handler answers it. `knowledge.adopt` replaces it; migration details are in
  [Migrating from `tools.mindcore.adopt`](migrating-from-tools-adopt.md).

A face the host does not switch on is never advertised and never served; its operations keep the
dispatch-deny answer rather than a default collection.

## Admission validation

The admitted scope is validated where the face is assembled, because Connect's list merge keeps
only the request's cognition selection: a selection the face does not serve must be refused before
the merge can drop it. The admitted scope must also name the Mind this surface serves — the routed
store refuses any other id.

Host admission is not peer input: an unsupported admitted selection is refused as the same generic
`NotAuthorized` a rejecting resolver returns, never as the caller-declared `InvalidInput` the
routed store keeps for request scopes. The peer learns nothing about what the face does not serve.

## Verify the wiring

`crates/mindcore-runtime/tests/loopback.rs` runs signed in-process Connect sessions against a real
SQLite store, including the negative cases: a face the host did not compose is never advertised
and its ops answer the dispatch deny, and an unserved tool answers `op_unsupported`.
`crates/mindcore-daemon/tests/serve.rs` drives the binary over a real socket: it binds an
owner-only socket, serves a stored entry to a signed dialer, refuses a second instance on the same
store with exit `2`, and drains on `SIGTERM`, removing the socket it bound.
