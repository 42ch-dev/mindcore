# Architecture: the sidecar daemon

## Scope

`crates/mindcore-daemon`: the packaged host that carries the SPOKE-peer delivery form — one
process, one store file, one Unix socket, serving a `ConnectResponder` to every admitted peer.
This page covers the process contract (config keys, exit codes, the single-instance lock), the
socket framing the daemon owns, the lifecycle order, the rules for which socket names the daemon
removes, and the accept loop.

It leaves out what the served surface serves, advertises and refuses — `serving-seam.md` owns that
— the in-process facade of the embedding form (`facade.md`), the TypeScript native binding
(`native-binding.md`) and the product documentation.

### Non-goals

Each follows from the three-delivery-forms decision.

- **No HTTP.** The only cross-process contract is SPOKE Connect over the Unix socket. The daemon
  contains no HTTP listener, route or request type.
- **No multi-store serving.** One process serves exactly one `store_path`.
- **No second authorization.** The daemon adds no admission rule. The responder's allowlist and
  signature checks authenticate the peer, the `principals` mapping names the `Principal`, and the
  runtime's Mind admission decides every call from that caller's live grants.
- **No memory face.** `ServeFaces.memory` is `None`: the daemon composes no `ServedMemory`
  service, so the `spoke-memory` family is neither served nor advertised.
- **No configurable workflow providers.** `ServeFaces.providers` is `MindProviders::default()`.
  The providers are Rust trait objects an embedding host injects, and a TOML file cannot stand for
  one; consequently `tools.mindcore.deliberate` is advertised (the tool is registered) and is
  answered with the workflow's own unavailability refusal.
- **No listener in a library crate.** `mindcore-runtime`, `mindcore-spoke` and `mindcore-sqlite`
  stay libraries; the transport, the listener and the accept loop live in this crate alone.

## Current state

### The process contract

The binary is `mindcore-daemon --config <path>`. The config is TOML and is read once at start; it
is never re-read, and a running daemon is reconfigured only by restarting it.

| Key | Type | Required | Meaning |
|---|---|---|---|
| `store_path` | path | yes | the one SQLite file the daemon serves |
| `socket_path` | path | yes | the Unix socket the listener binds |
| `host_id` | string | yes, non-empty | `HostCapabilityManifest.host_id` in the signed hello |
| `history` | bool | no, default `false` | composes the per-Mind history face |
| `invoke.timeout_ms` | integer | no | `ConnectResponderOptions.invoke_timeout_ms`; absent keeps spoke_connect's own default |
| `identity.seed_file` | path | yes | file holding the responder's Ed25519 seed as 64 hex digits |
| `peers.<peer_id>.public_key` | string | yes, per peer | the peer's Ed25519 public key as 64 hex digits |
| `peers.<peer_id>.principal` | string | yes, per peer, non-empty | the `Principal.principal_id` the authenticated peer maps to |
| `journal.dir` | path | yes | the journal root `journal_dir` publishes under |
| `adoption.scope_id` | string | with `[adoption]`, non-empty | the host-owned id naming the adoption source on the wire |
| `adoption.source.collection` | string | with `[adoption]`, non-empty | the one collection every admitted Mind adopts from |

The keys are nested as the table writes them. `store_path`, `socket_path`, `host_id` and `history`
are top-level keys and precede the first `[table]`; the others sit in `[invoke]`, `[identity]`,
`[journal]`, `[adoption]` and `[adoption.source]` (or an inline `source = { collection = … }`),
with one `[peers.<peer_id>]` table per peer. `DaemonConfig` and every struct nested in it deny
unknown keys, so a flat `identity_seed_file` or `journal_dir` is a start failure rather than a
silently ignored setting.

`[peers]` may be empty: the daemon then serves nobody, and the allowlist stays fail-closed. The
`[adoption]` table is absent or present as a whole; both of its members are required when it is
present.

A config is unusable, and the daemon exits 1 naming the key, when the file or a required key is
missing, when a key is unknown, when a hex value is not 64 hex digits, when `host_id`, a peer's
`principal` or an adoption member is empty, or when a value has the wrong TOML type. The parent of
`store_path` must resolve as well: one that does not is refused at `resolve` with the same exit
code.

A fault in `identity.seed_file` never prints the seed. The message names the key and the observed
length only, because `main` prints it and the seed is a private key.

| Exit code | Meaning |
|---|---|
| `0` | clean exit after `SIGTERM` or `SIGINT`; also `--help` and `-h`, which print the usage |
| `1` | a start failure: unusable config, a store path that could not be resolved, a store that could not be opened, a journal root that could not be created or made durable, a surface refused at assembly, a shutdown signal the process could not listen for, a socket name that could not be bound or set to its mode. Also a drain that could not remove its socket name |
| `2` | another daemon holds this store's lock |

`--config` is required. `--help` and `-h`, wherever they appear in the argument list, print the
usage to stdout and exit 0; a missing `--config`, a `--config` with no value or given twice, or any
other argument prints the reason and the usage to stderr and exits 1.

A failed start leaves no socket name behind. The one exception is a socket directory nobody can
write to; the refusal then names the leftover path (see Lifecycle order).

### The single-instance lock

One daemon per store file per host. `store_path` is **resolved once**, in `main`, by
`lock::canonical_store_path`, and the same `PathBuf` is handed both to `StoreLock::acquire` and to
`MindStore::open`. The lock, the database and its `-wal` and `-shm` sidecars therefore all name one
file. Resolving twice would let a symlink re-pointed between the two calls open a file the lock
never named.

`StoreLock::acquire` takes one exclusive advisory lock (`File::try_lock()`) on
`<canonical store>.lock`, a file of its own beside the store. Because it is a separate file it sits
outside SQLite's lock space, and the store opens and serves while it is held.

- The lock is taken **before** `MindStore::open`. A second instance therefore exits 2 before it
  opens the database, runs a migration or creates its socket.
- Resolution is `canonicalize` when the store exists, and a dangling final symlink followed by hand,
  so that every spelling of one store lands on one path. A parent that does not resolve, or a chain
  of more than 16 symlink hops, is a config failure with exit 1, and nothing is created.
- `acquire` creates the store as an empty file when it does not exist yet. SQLite then initializes
  that file on open.
- A held lock is `DaemonError::LockHeld`, exit 2, naming the canonical path. A store or lock file
  that cannot be created or opened is a start failure with exit 1.
- The guard is dropped last on every exit path, after the store's connections are closed. A daemon
  that dies any other way releases the lock when the kernel closes its descriptors.
- The lock file is never removed. Removing it would open a window in which one daemon holds the
  lock on a file nobody else can name while a third daemon locks its replacement.
- The lock binds only processes that take it. An embedding host that opens the same file through
  `MindStore::open` is not detected: `MindStore::open` is unchanged.
- The socket is only an address. Two daemons may not share a store; a socket name left behind by a
  crashed daemon is reused (see Lifecycle order).

The guarantee is one daemon per store file. Every name of one store answers one lock: the path
itself and any symlink to it, including a dangling one, whose creation lands on the target's name.
**Every hard link is refused.** A store whose link count is above one does not start, from either
name and in either directory: `store_path <path>: N hard links; a store must have exactly one
name`, exit 1. The link count belongs to the file and not to a directory, so a hard link in another
directory is not possible by construction. The refusal is the mechanism, and it is also what keeps
SQLite's write-ahead log whole: the `-wal` and `-shm` files are named for the path and not the
inode, so a second name would write a log beside itself that a restart through the other name
never reads.

### Framing

`UnixSocketTransport` implements `spoke_connect::remote::Transport` over a `tokio::net::UnixStream`.
One Connect envelope is one frame: a 4-byte unsigned big-endian length, then exactly that many
bytes of body.

- `MAX_FRAME` is 16 MiB (16,777,216 bytes) of body. A length of `0` or above `MAX_FRAME` is a
  protocol error: `recv` answers `TransportError::Io` naming the length, the body is not read, and
  the transport closes itself. `send` refuses such an envelope with `Io` instead of writing a frame
  the peer must reject.
- `send` writes the prefix and the body of one envelope under one mutex, so two sends on one
  stream never interleave.
- `recv` reads the prefix and then the body. A peer that closes at a frame boundary answers
  `TransportError::Closed`, an orderly end; a peer that closes inside a prefix or a body answers
  `TransportError::Io` (`truncated frame`).
- Every framing fault is terminal. The transport marks itself closed, wakes `closed()` and shuts
  the socket down before the error returns, so no later `send` or `recv` succeeds and the fault
  never reaches the responder as a message.
- A write that fails while the transport is still open is terminal as well: part of a frame may
  already be on the stream, so the transport closes itself exactly as it does on a failed read and
  answers `Io`.
- Teardown never waits for either half. The socket's own descriptor is captured before the stream
  is split and `shutdown(2)` is issued on it directly — `SHUT_WR`, which ends the stream for the
  peer, then `SHUT_RD`, which wakes a parked read — under a lock that `send` and `recv` never
  hold. A `send` parked on a peer that stopped reading therefore fails, and answers `Closed`, as
  soon as the transport is closed.
- `close` is idempotent and waits: it returns only once the shutdown has run, whoever started it.
  A closed transport fails every later `send` and `recv`.
- `closed()` resolves once the transport is closed; the connection task awaits it (see Accept
  loop).
- Both ends of a session use the same framing: `UnixSocketTransport::connect(path)` dials and
  `UnixSocketTransport::from_stream(stream)` wraps an accepted stream.

`TransportError` has two variants, `Closed` and `Io(String)`. Framing faults travel in `Io`; the
daemon adds no variant to `spoke-connect`.

### Lifecycle order

```text
config   read and validate --config                    exit 1 on failure
resolve  store_path, once, to one canonical path       exit 1 on failure
lock     create the store if absent, one name only,    exit 2 when held
         try_lock on <canonical store>.lock
store    MindStore::open(that same path)               exit 1 on failure
journal  create journal.dir one level at a time,       exit 1 on failure
         sync every level's parent, on every start
surface  MindcoreServe::surface(faces)                 exit 1 on refusal
signals  listen for SIGTERM and SIGINT                 exit 1 on failure
listen   bind socket_path, set its mode to 0o600       exit 1 on failure
prepare  seed, allowlist, peer keys                    exit 1 on failure, the bound name removed
serve    accept loop
drain    SIGTERM or SIGINT
exit 0, or 1 when the socket name could not be removed
```

Each phase begins only after the previous one succeeded. A failed start leaves no socket name
behind: a failure before `listen` has bound none, and a failure at `prepare` — the seed file gone
or unreadable, or a peer key that does not decode — removes the name it just bound and keeps the
original refusal. A failed `listen` or `prepare` also closes the store's connections before it
returns. A failed start does leave what the earlier phases created: the lock file, the store file
when `lock` created it, and the journal levels.

Three steps owe their timing:

- **`journal`** creates the root one level at a time, from the nearest ancestor that exists, and
  on every start syncs the parent of every level in the chain — the levels it just created and the
  ones it found already there. Syncing only the leaf's parent would leave `journal` durable inside
  a directory that has no entry in its own parent yet, and a start that created a level but failed
  before syncing its entry leaves nothing that marks which entry is unproven, so the next start
  redoes the whole chain. An existing component that is not a directory is refused, naming it:
  publishing into whatever the path really names would be worse than refusing to start. A sync
  that fails is a journal fault naming the directory, because a clean start on a chain that is not
  on disk would promise durability the daemon does not have.
- **`signals`** installs both handlers before the socket name exists. From the moment the name is
  bound an ordinary supervisor stop reaches the drain; a signal the process cannot listen for is
  `DaemonError::Signal`, a start failure with exit 1.
- **`listen`** sets the bound socket's mode to `0o600`, whatever the process umask, because who may
  connect is part of the process contract. A mode that cannot be set removes the name and refuses
  the start.

**Which socket names the daemon removes.** `listen` inspects `socket_path` without following a
symlink, and removes only a socket established to be dead.

- A free name is the ordinary start: it is bound.
- A name occupied by anything that is not a socket — a regular file, a directory, a symlink, a
  FIFO, a device — is refused, naming what occupies it, and nothing is removed. The daemon never
  unlinks a name it does not own, so a `socket_path` that names the store or the seed file is a
  refusal that leaves the file intact.
- A socket is probed once. A connection means a live listener owns the name and the start exits 1.
  A refused connection means the name is left over from a crashed process: the daemon removes it
  and binds again. Any other probe outcome, such as a permission fault, is refused with nothing
  removed, because the name's liveness is unknown.
- That removal is bound to the identity (device and inode) read before the probe, so a socket
  swapped in since is refused and stays.

`bind` reads the identity of the name it just bound. When that read fails it removes the name
before answering the refusal. When the removal fails too, the refusal carries both faults, names
the leftover path and says to remove it before the next start.

**An unwritable socket directory leaves the name.** Nobody can remove a name from a directory
nobody can write to, so a failed start there leaves the bound name behind, and the refusal says so:
both causes, the path, and the recovery. This is a property of the environment, not a clean start.

POSIX cannot unlink by inode, so a process running as the same user can replace the name between
the identity check and the unlink. The daemon is not built to resist that; the limit is tracked as
`I-000172`.

### What the daemon composes

One `MindcoreSurface` is assembled once, at `surface`, and shared by every connection.

| Input | What the daemon supplies |
|---|---|
| `MindcoreServe::new(store, reflections, publish)` | the opened store; `reflections = false`, because no memory service is served; `publish = journal_dir(journal.dir)` |
| `ServeFaces.manifest` | `schema_version: 1`, `host_id` from config, `roles: ["data-store"]`, `namespaces: []`, `extensions: {}`, `capabilities` empty; `tools` is left to the schema's default |
| `ServeFaces.memory` | `None` |
| `ServeFaces.knowledge` | `true` |
| `ServeFaces.history` | `history` from config |
| `ServeFaces.adoption` | `Some(AdoptionRoute { scope_id, source: KnowledgeScope::new(collection) })` when `[adoption]` is configured, else `None` |
| `ServeFaces.principals` | the peer-to-principal mapping of `[peers]`, answering `NotAuthorized` for an authenticated peer with no entry |
| `ServeFaces.providers` | `MindProviders::default()` |
| `ServeFaces.tools` | empty |

The responder's `allowlist` is the set of `[peers]` keys and its `peer_keys` maps each key to the
decoded 32-byte public key. A peer id absent from `[peers]` fails the handshake before any
admission runs.

The runtime replaces the manifest's `capabilities` and `tools` with what the composed faces serve
and adds the `mindcore` namespace. The daemon states no capability, so the signed hello follows
the composed faces exactly:

- always `spoke-knowledge`, `spoke-knowledge-scoped`, the four `tools.mindcore.*` tools
  (`recall_context`, `absorb`, `deliberate`, `read_cognition`) and, with the `deliberate` tool,
  `spoke-judgment`;
- `spoke-history` exactly when `history` is `true`;
- `spoke-knowledge-adoption` exactly when `[adoption]` is configured, and never otherwise, because
  the manifest derivation receives `faces.adoption.is_some()`;
- never `spoke-memory`, `spoke-memory-transfer` or `ke-ownership`.

An `[adoption]` route with an empty `scope_id` or `source.collection` is refused at `config`. The
one assembly refusal of `MindcoreServe::surface` the daemon can meet is an `[adoption]` source that
collides with a registered Mind's knowledge collection or `space_id`: `InvalidInput`, a start
failure with exit 1. (`history` without `knowledge` cannot arise: `knowledge` is always `true`.)

The journal root is the host's to create and make durable before the first publication, so at
`journal` the daemon creates every missing level of `journal.dir`, syncs the parent of every level
in the chain on every start, and refuses a component that is not a directory.

The scoped knowledge and history admissions are synchronous callbacks — they map the peer through
`principals` and then read the store — and Connect runs them on its blocking pool through
`Handle::block_on`. The daemon therefore runs on a multi-thread Tokio runtime; a current-thread
runtime would turn every such admission into an `INTERNAL_ERROR`.

### Accept loop

```text
listener = the name bound at listen
loop
  stream = listener.accept()                     an accept fault ends the loop, exit 1
  spawn one connection task, owned by the set
    transport = UnixSocketTransport::from_stream(stream)
    gated = GatedTransport::new(transport)       the gate starts closed
    responder = connect_responder(ConnectResponderOptions {
      transport: gated, identity: RemoteIdentity { seed }, manifest: surface.manifest.clone(),
      allowlist, peer_keys, ports: Some(surface.ports.clone()), invoke_timeout_ms })
    track the responder                            before the task yields
    surface.register(&responder)                   before the task yields
    gated.open()                                   only once register has succeeded
    hold (responder, transport) until the transport closes
    drop the responder and untrack it
```

- One `Transport` and one `Arc<ConnectResponder>` per accepted connection. `connect_responder`
  runs its own handshake and receive task; the daemon holds the responder and the transport
  together and tracks them while the connection lives.
- **No session is established before the handlers exist.** Each connection's responder is served
  through a `GatedTransport` whose `recv` waits on a gate that the connection task opens only after
  `MindcoreSurface::register` has succeeded. Until then the responder cannot read the dialer's hello,
  so it cannot establish the session, so nothing can be invoked. The gate is what closes the window
  `connect_responder` leaves open on its own: it spawns its task before it returns and installs no
  handlers from the manifest, so a dialer that completes its hello at once could otherwise reach a
  tool lookup first. `send` and `close` are never gated: the hello reply is a `send`, and a start
  that fails must still be able to close the session it opened.
- Two results are the evidence. `tests/serve.rs`'s
  `a_tool_invoked_right_after_the_handshake_is_served` dials with a tool id in its hello and
  asserts that an immediate invoke is answered from the served path, never `op_unsupported` or
  "no handler registered". That test alone passes with or without the gate; what shows the gate is
  load-bearing is that with `open()` removed the integration test's hello times out, and
  `transport.rs`'s `a_closed_gate_withholds_the_hello` pins the same in isolation.
- The responder is tracked and `register` runs on it before the connection task yields:
  `connect_responder` returns without suspending. A refused `register` closes that responder and
  ends the connection, and the gate never opens.
- Every connection task is owned by one `JoinSet`, which also reaps each task as it finishes.
- A connection task ends when its transport closes — the peer hung up, the responder closed the
  session (a refused handshake, a framing fault), or the daemon is draining — and drops the
  responder with it.
- The socket name is created by `bind` and removed by `drain`; the listener is never re-bound.

### Shutdown

`drain` begins on `SIGTERM` or `SIGINT` (`tokio::signal::unix`) and runs in this order:

1. Drop the accept future, and with it the listener, so no further connection is accepted.
2. Call `ConnectResponder::close` on every tracked responder. It is synchronous and idempotent: it
   marks the session `Closed`, fails pending reverse invokes with `session_closed`, aborts the
   in-flight serve dispatches and releases the transport.
3. Shut the connection set down: abort and join every task, including one still setting up.
4. Call `close` again on whatever was tracked inside that window.
5. Remove `socket_path`, but only while the name is still the socket this daemon bound. A name
   that is gone already is the intended end state; a name replaced since the bind belongs to
   whoever replaced it and is refused.
6. Close the store's connection pool. `MindcoreServe` consumes the store, so the daemon closes the
   pool handle it kept: the same pool, with the effect of `MindStore::close`.
7. Drop the lock guard.

The process then exits 0 when step 5 removed the name or found it absent, and exits 1 with a
`socket:` refusal otherwise. A daemon that leaves its name behind does not claim a clean stop. A
daemon killed before step 5 leaves the socket name behind, and the next start removes it at
`listen`.

### End-to-end evidence

`crates/mindcore-daemon/tests/serve.rs` starts the binary Cargo built, with `--config`, and is the
end-to-end evidence for this page: one knowledge read over a real socket (a signed dialer's scoped
`get` answers the entry the fixture stored), exit 0 on `SIGTERM` with the socket gone and the lock
file retained, exit 2 for a second instance on the same store, which binds no socket, a bound
socket whose mode is `0o600`, and a tool invoked the moment the handshake completes that is served
rather than dispatch-denied. The read and the `SIGTERM` are two tests, so the signalled daemon
holds no live session; a drain that ends a live connection is covered in
`crates/mindcore-daemon/src/serve.rs` by
`a_drain_ends_a_connection_parked_before_its_registration`.

## Invariants

- One daemon per store file per host: `store_path` is resolved once, and the same path names the
  lock, the database and its sidecars. The lock is held before the store opens, is released last
  and is never removed; every name of the store answers one lock, and a store with a second hard
  link is refused.
- A failed start leaves no socket name behind; when the socket directory cannot be written to, the
  refusal names the leftover.
- The daemon removes only a socket established to be dead at `listen`, and only the socket it
  bound at `drain`. A name that is not a socket is never removed.
- The socket the daemon binds is `0o600`, whatever the process umask.
- Both shutdown signal handlers are installed before the socket name is bound.
- On every start the parent of every level in the journal root's chain is synced; a component that
  is not a directory is refused, and so is a failed sync.
- One envelope per frame, at most `MAX_FRAME` bytes; a framing fault closes the transport and is
  never delivered to the responder as a message.
- No session is established before every tool handler the daemon serves is registered.
- The socket name is removed only after every session has been told to close and every connection
  task has ended.
- The signed hello is `MindcoreSurface::manifest`, derived by the runtime from the composed faces;
  the daemon states no capability.
- The daemon adds no admission rule: peer authenticity is the responder's, the `Principal` is the
  `principals` mapping's, and Mind admission is the runtime's, on the caller's live grants.
- A diagnostic never carries the seed's digits.
- No library crate gains a listener, a transport or an accept loop.
- The daemon runs on a multi-thread Tokio runtime.

## Where it lives

- `crates/mindcore-daemon/Cargo.toml` — the crate: library target `mindcore_daemon` beside the
  `mindcore-daemon` binary, both `publish = false`.
- `crates/mindcore-daemon/src/lib.rs` — `pub mod config, lock, transport, serve`, and
  `DaemonError`, whose variants are `Config`, `Store`, `Journal`, `Surface`, `Socket`, `Signal` and
  `LockHeld`; `exit_code` is 2 for `LockHeld` and 1 for every other.
- `crates/mindcore-daemon/src/main.rs` — the binary: `--config` parsing and its unit tests, the
  one resolution of `store_path`, the phase sequence and the exit-code mapping.
- `crates/mindcore-daemon/src/config.rs` — `DaemonConfig`: the table above, its TOML loading and
  its validation.
- `crates/mindcore-daemon/src/lock.rs` — `StoreLock` and `canonical_store_path`: the lock file
  beside the store's canonical name, the one-name refusal, and the guard.
- `crates/mindcore-daemon/src/transport.rs` — `UnixSocketTransport` and `GatedTransport`,
  `MAX_FRAME`, `connect` and `from_stream`.
- `crates/mindcore-daemon/src/serve.rs` — the surface assembly, the journal root, the signal
  handlers, the socket-name rules, the accept loop and `drain`.
- `crates/mindcore-daemon/tests/serve.rs` — the integration test over a real socket.
- `crates/mindcore-runtime/src/lib.rs` — `MindcoreServe`, `ServeFaces`, `MindcoreSurface` and the
  manifest derivation the daemon feeds.
- `docs/serving.md` — the host obligations the daemon takes over.

## Change obligations

- Adding, removing or renaming a config key, changing a default, or changing an exit code or the
  phase that produces it updates the key table, the exit-code table and Lifecycle order in the
  same change, and the integration test's config with them.
- Changing the frame layout, `MAX_FRAME`, or which transport faults close the connection updates
  Framing in the same change and the transport unit tests with it.
- Changing what the daemon supplies for any `ServeFaces` member or for `MindcoreServe::new`
  updates the composition table in the same change.
- Changing the lock target, the phase it is taken in, where `store_path` is resolved, or the names
  of a store the daemon refuses updates The single-instance lock or Lifecycle order in the same
  change.
- Changing the socket mode, the point at which the shutdown signals are installed, or how the
  journal root is made durable updates Lifecycle order and Invariants in the same change.
- Changing the point at which the gate opens, or which of `send`, `recv` and `close` it holds,
  updates Accept loop in the same change and
  `a_tool_invoked_right_after_the_handshake_is_served` with it.
- Changing which names `listen` refuses, recovers or removes, or what `drain` removes, updates
  Lifecycle order and Shutdown in the same change.
- A change to `ServeFaces`, `ConnectResponderOptions` or `Transport` in the crates the daemon
  consumes re-checks this page against the new signatures in the same change.
- Moving or adding a listed path updates Where it lives in the same change.
