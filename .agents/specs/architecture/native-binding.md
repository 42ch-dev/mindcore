# Architecture: the TypeScript native binding

## Scope

`crates/mindcore-binding` and `bindings/node`: the native addon through which a TypeScript process
links `mindcore-runtime` into its own process and operates a Mind through the in-process facade —
the second delivery form. This page covers the exported JavaScript surface, the napi-rs features
the addon is built with and why, how a facade refusal reaches JavaScript, and how the addon is
loaded.

It leaves out the facade's own contract (`facade.md`), the served surface (`serving-seam.md`) and
the sidecar daemon (`daemon-kernel.md`).

### Non-goals

- **No second facade and no second write path.** Every export calls an existing public function.
  The addon declares no wire shape, and its only transaction of its own is the subject
  provisioning call below.
- **No space-level surface.** The `memory.*` operations, `port.knowledge.*`, `history.*`,
  `knowledge.adopt`, grants and spaces stay unreleased through the binding; they remain available
  to an embedding host. The binding exports Mind-level operations only.
- **No `deliberate`.** It reasons with a `Reasoner` and other providers an embedding host injects
  as Rust trait objects; a JavaScript host cannot supply one, so the operation is not exported
  rather than exported and always refused.
- **No client of the daemon.** The binding is in-process. A TypeScript host that reaches the
  daemon is using the SPOKE-peer form.
- **No published package.** `bindings/node` is a local, private package with no dependencies; it is
  not published to npm.

## Current state

### Exported JavaScript surface

`bindings/node/index.js` is an ES module. It exports two classes and one error class; every
asynchronous call answers a `Promise`.

```ts
export interface MindcoreClientOptions {
  /** The one SQLite file this client serves. */
  storePath: string;
  /** The authenticated caller every facade call admits. */
  callerId: string;
  /** Journal root `review` publishes under. Absent: `review` refuses `INVALID_INPUT`. */
  journalDir?: string;
}
export interface CreateMindOptions { id?: string; displayName?: string }
export interface MindInfo {
  id: string;
  displayName: string | null;
  status: 'active' | 'frozen';
  knowledgeCollection: string;
  carrierEntryId: string;
  createdAt: string;
}
export interface CaptureInput {
  subject: string;
  context?: string;
  content: object;
  occurredAt?: string;
  kindHint?: string;
  idempotencyKey: string;
}
export interface RecallInput {
  subject: string;
  selection: object;
  maxItems: number;
  maxBytes?: number;
  query?: string;
  cursor?: string;
}
export interface Cognition { carrierRevision: number; records: object[]; mindState: object }

/** A facade refusal: `code` is the SPOKE wire code, `details` its structured payload. */
export declare class MindcoreError extends Error {
  constructor(code: string, message: string, details?: object);
  readonly code: string;
  readonly details: object | undefined;
}
export declare class MindcoreClient {
  private constructor(handle: object);
  static open(options: MindcoreClientOptions): Promise<MindcoreClient>;
  createMind(options?: CreateMindOptions): Promise<MindView>;
  listMinds(): Promise<MindInfo[]>;
  getMind(id: string): Promise<MindView>;
  ensureSubject(mindId: string, subjectId: string): Promise<void>;
}
export declare class MindView {
  private constructor(handle: object);
  get id(): string;
  get carrierEntryId(): string;
  knowledgeGet(entryId: string, revision?: number): Promise<KnowledgeEntry>;
  knowledgePut(entry: KnowledgeEntry, expectedBaseRevision?: number | null): Promise<KnowledgeEntry>;
  knowledgeList(): Promise<KnowledgeEntry[]>;
  capture(input: CaptureInput): Promise<MemoryCaptureResponse>;
  review(candidateId: string, decision: ReviewDecision, idempotencyKey: string): Promise<MemoryReviewResponse>;
  recall(input: RecallInput): Promise<MemoryRecallResponse>;
  cognitionRead(): Promise<Cognition | null>;
}
```

`KnowledgeEntry`, `MemoryCaptureResponse` and `MemoryRecallResponse` are the SPOKE wire objects of
those names, `MemoryReviewResponse` aliases SPOKE's `MemoryCandidatesReviewResponse`, and
`ReviewDecision` is the `decision` member of the SPOKE review request. They cross the boundary as
plain objects with their published field names unchanged, and the addon declares none of them.
`bindings/node/index.d.ts` is written by hand — the addon is built without napi-rs's `type-def`
feature — so it declares each shape once, against the serialized Rust struct of the SPOKE module
named beside it, and a caller reads a response without a second declaration of its own.

Naming follows one rule: exported names, method names, argument names and the members of the
binding's own input and record objects are `camelCase`; a SPOKE wire object keeps its field names.

Two of the operations are not facade calls:

- `MindcoreClient.open` opens the store once with `MindStore::open`; every later call shares that
  one handle and its single-connection pool. Each `openClient` call opens the file again, and the
  binding keeps no process-wide registry, so a host that needs one writer per file reuses one
  client. When
  `journalDir` is given it also creates that root and flushes the entry of every level down to it,
  because the publisher serves an existing root and never creates one; a root it cannot make
  durable is `INTERNAL_ERROR` before any Mind is opened.
- `ensureSubject` provisions the subject row a memory write needs, the duty an embedding host
  performs before `capture`. It is `MemoryAdapter::ensure_subject` on one transaction of the
  client's pool, committed. Without it `capture` answers `NOT_FOUND` for the subject.

`createMind` and `getMind` answer a `MindView`, the opened `Mind` handle; `listMinds` answers the
registry rows. A Mind the caller holds no live grant in answers `NOT_FOUND` from the registry, and
`cognitionRead` answers `null` for a Mind that has judged nothing.

### The Rust side

The addon exports functions, not classes. `MindcoreClient` and `MindView` are opaque `#[napi]`
handles with no JavaScript constructor; the classes above are the `index.js` wrappers that hold
them.

| `index.js` member | Rust export | Facade call |
|---|---|---|
| `MindcoreClient.open(options)` | `openClient(ClientOptions) -> ClientResult` | `MindStore::open(&store_path)` |
| `client.createMind(options?)` | `clientCreateMind(&MindcoreClient, Option<CreateMindOptions>) -> MindResult` | `minds::create`, then `Mind::open` |
| `client.listMinds()` | `clientListMinds(&MindcoreClient) -> Value` | `minds::list` |
| `client.getMind(id)` | `clientGetMind(&MindcoreClient, id: String) -> MindResult` | `Mind::open` |
| `client.ensureSubject(mindId, subjectId)` | `clientEnsureSubject(&MindcoreClient, mind_id: String, subject_id: String)` | `MemoryAdapter::ensure_subject` |
| `mind.id`, `mind.carrierEntryId` | `mindViewInfo(&MindView) -> MindInfo` | `Mind::row` |
| `mind.knowledgeGet(entryId, revision?)` | `mindKnowledgeGet(&MindView, entry_id: String, revision: Option<f64>)` | `Mind::knowledge_get` |
| `mind.knowledgePut(entry, expected?)` | `mindKnowledgePut(&MindView, entry: Value, expected_base_revision: Option<f64>)` | `Mind::knowledge_put` |
| `mind.knowledgeList()` | `mindKnowledgeList(&MindView)` | `Mind::knowledge_list` |
| `mind.capture(input)` | `mindCapture(&MindView, CaptureInput)` | `Mind::capture(&Capture { .. })` |
| `mind.review(candidateId, decision, key)` | `mindReview(&MindView, candidate_id: String, decision: Value, idempotency_key: String)` | `Mind::review` |
| `mind.recall(input)` | `mindRecall(&MindView, RecallInput)` | `Mind::recall(&Recall { .. })` |
| `mind.cognitionRead()` | `mindCognitionRead(&MindView)` | `Mind::cognition_read` |

- `ClientOptions`, `CreateMindOptions`, `CaptureInput` and `RecallInput` are `#[napi(object)]`
  structs with the members of the interfaces above. `CaptureInput.content` and `RecallInput.selection`
  are `serde_json::Value`.
- `MindcoreClient` holds `Arc<MindStore>`, the `CallerId` built from `callerId` and the optional
  journal root. `MindView` holds one `Mind`, opened with `Mind::with_journal_publisher(journal_dir(root))`
  when `journalDir` was given.
- An integer argument (`revision`, `expectedBaseRevision`) arrives as `f64` and is converted to
  `u64`; `maxItems` and `maxBytes` are whole numbers bounded by `u32::MAX`. A wire object (`entry`, `decision`) is decoded
  from `serde_json::Value` into its published type, and a response is serialized back to one.
- `Cognition` and the registry `Mind` row carry no serde derive, so the addon projects each into
  the camelCase object above (`Cognition`, `MindInfo`); `MindStatus` becomes `'active'` or
  `'frozen'`.

### napi-rs features

The addon depends on `napi` 3 with `default-features` kept and `serde-json` and `tokio_rt` added,
on `napi-derive` 3 with `default-features = false` and `strict`, and on `napi-build` 2 in
`build.rs`.

| Feature | On | Why |
|---|---|---|
| `napi4` | default | the lowest N-API version an exported `async fn` needs; `tokio_rt` enables it too |
| `dyn-symbols` | default | the addon links against no Node import library; Node's symbols resolve when it is loaded |
| `tokio_rt` | added | every facade call is `async`; napi-rs runs them on its own Tokio runtime and answers a `Promise`. `async` is an alias of this feature and is not added beside it |
| `serde-json` | added | `serde_json::Value` crosses the boundary as a JavaScript object, which is how the wire objects pass unchanged |
| `strict` (`napi-derive`) | added | every argument is validated against its Rust type before conversion, so a wrong-typed value is refused before any facade call |
| `type-def` (`napi-derive`) | off | writing a `.d.ts` needs the napi-rs CLI, which this repository does not install; `bindings/node/index.d.ts` is written by hand |
| `napi5` … `napi10`, `compat-mode`, `chrono_date` | off | nothing exported uses a newer N-API call, a compatibility runtime, or a JavaScript `Date`; `occurredAt` is an RFC 3339 string |

The crate is a `cdylib` and is not a dependency of any other workspace crate.

### How a refusal reaches JavaScript

napi-rs rejects the `Promise` of an exported `async fn` only with a `napi::Error`, which carries a
message and neither the facade's code nor its details. Every **asynchronous** export therefore
answers a result object, and `index.js` is the one place it is turned into a value or an error:

```text
{ ok: true,  value }                                 resolves the Promise with `value`
{ ok: false, code, message, details }                rejects with MindcoreError(code, message, details)
```

`openClient`, `createMind` and `getMind` answer a handle, which cannot travel inside a
`serde_json::Value`; they build the same object as `ClientResult` or `MindResult`, with the handle
in `value` and the other three members unset. Every other asynchronous export builds it in
`settle`, the one function that maps a `SpokeResult` to it. `mindViewInfo` is the one synchronous
export: it answers the `MindInfo` object directly, because a read of the row the handle already
holds has no `SpokeResult` to settle.

- A facade refusal (`SpokeResult::Reject`) keeps `SpokeReject` whole: `code` is
  `SpokeRejectCode::as_str()` (`NOT_FOUND`, `STORED_REVISION_STALE`, …), `message` is its message
  and `details` its structured payload, or absent when the refusal has none.
- The addon's own failures take the codes whose definitions fit them: a store that cannot be
  opened or a journal root that cannot be made durable is `INTERNAL_ERROR` with the diagnostic as
  the message, and a JavaScript argument that cannot be converted (a negative or fractional
  revision, an undecodable wire object, a malformed `occurredAt`) is `INVALID_INPUT`.
- An answer that will not serialize is the addon's own defect, not a refusal the facade made. It
  answers `BINDING_DEFECT` with the type name and the serialization error, a code no facade
  refusal carries, so a caller reading `code` can never mistake one for the other.
- A value of the wrong TypeScript type is refused by napi-rs's own argument validation, as a
  plain `Error` with no `code`, before the addon runs.

### Loading the addon

`bindings/node/index.js` loads the build artifact directly; nothing is copied or renamed.

1. `MINDCORE_BINDING_PATH` when set — the one path it names, which must exist; a missing file is
   reported as that, not as an unbuilt addon.
2. Otherwise `<repository>/target/debug/` joined with the platform's artifact name:
   `libmindcore_binding.dylib` (`darwin`), `libmindcore_binding.so` (`linux`),
3. Otherwise the same name under `target/release`.

`bindings/node/package.json` declares `os: ["darwin", "linux"]`, and the artifact table carries
exactly that set; a host outside it is reported by name, and naming its own build through
`MINDCORE_BINDING_PATH` still works. When both `target/debug` and `target/release` hold the
artifact and nothing named that variable, debug is loaded and one
`MindcoreBindingShadow` warning names both paths, so a release measurement cannot silently run
debug code.

The load is `process.dlopen` on a fresh `node:module` module, which accepts the artifact's own
name. When no candidate exists the module throws an `Error` naming every path it tried and
`cargo build -p mindcore-binding` as the command that produces the addon.

`bindings/node/package.json` is `"private": true`, `"type": "module"` and declares no dependency,
so there is no install step: `node --test bindings/node/test.mjs` runs against the debug build.

Continuous integration runs the same test: the `binding` step of the `rust` job in
`.github/workflows/ci.yml`, after `cargo test --locked --workspace`, builds the addon with
`cargo build --locked -p mindcore-binding` and runs `node --test bindings/node/test.mjs` on the
job's existing Node.

## Invariants

- The binding is in-process: it opens no socket and contacts no daemon.
- Every export calls an existing public function; the only transaction the binding opens is
  `ensureSubject`'s.
- A facade refusal reaches JavaScript with its code, message and details unchanged.
- No space-level operation is exported.
- One client holds one store handle, and each `openClient` call opens that file again; the binding
  keeps no process-wide registry. A host that needs one writer per file reuses one client for that
  store.
- No `close`/`release` exists on the client: the store pool closes when the client is collected and
  the process exits. A host that must release a database file drops every client and view over it.
- Fault ownership: a `journalDir` name-shape fault (`NotADirectory`, `AlreadyExists`, a file at the
  path) is `INVALID_INPUT`; a permission, space, fsync or resolution fault is `INTERNAL_ERROR`.
  A `storePath` fault is always `INVALID_INPUT` before the pool opens — naming a different path
  fixes it — and only a lock, migration or disk fault after the probes is `INTERNAL_ERROR`.
- A `storePath` that is a symlink is judged by what it names: a link to a file opens, a link to a
  directory is `INVALID_INPUT` with the target's kind. An empty `journalDir` is a name-shape
  `INVALID_INPUT`.
- `maxItems` / `maxBytes` are whole numbers in `0..=4294967295`; anything else (negative,
  fractional, non-finite or above `u32::MAX`) answers `INVALID_INPUT` naming the member and the
  value. A bad caller-supplied `journalDir` is also `INVALID_INPUT`, carrying
  `details.journalDir` and `details.kind`; `INTERNAL_ERROR` is reserved for a join failure or a
  genuine durability fault.
- `ensureSubject` performs no authorization of its own: the adapter's admission admits the caller
  under the subject's `capture` grant, and a caller without that grant on the named Mind answers
  `NOT_AUTHORIZED`.
- The addon is a workspace member but not a default member and not a dependency of any other
  crate, so a bare `cargo check` or `cargo test` at the workspace root does not build it.
- The binding calls the Rust facade in-process and never the SPOKE FFI. A host that reaches a Mind
  through `spoke-connect`'s FFI instead is bounded by that layer's peer-only admission: SPOKE
  0.19.1 assembles a peer-only `KnowledgeAdmission` and its `knowledge_scope(peer_id)` has no
  request selector, so such a host cannot route a Mind per request. That is that layer's contract,
  not this binding's; the two paths are independent.

## Where it lives

- `crates/mindcore-binding/Cargo.toml`, `crates/mindcore-binding/build.rs`,
  `crates/mindcore-binding/src/lib.rs` — the addon: the handles, the exported functions and the
  result-object mapping.
- `bindings/node/index.js` — the loader and the exported classes.
- `bindings/node/index.d.ts` — the hand-written declarations of the exported surface.
- `bindings/node/package.json`, `bindings/node/test.mjs` — the local package and its `node --test`
  file.
- `.github/workflows/ci.yml` — the `binding` step of the `rust` job that builds and tests the
  addon.
- `crates/mindcore-runtime/src/mind.rs`, `crates/mindcore-runtime/src/minds.rs` — the facade the
  exports call; `crates/mindcore-spoke/src/control.rs` — `MemoryAdapter::ensure_subject`.
- `crates/mindcore-runtime/examples/mind_facade.rs` — the facade path `test.mjs` mirrors.

## Change obligations

- Adding, removing or renaming an export, or changing an argument or return object, updates the
  surface block, the Rust table and `bindings/node/index.d.ts` in the same change, and `test.mjs`
  with them.
- A change to `Mind`, `minds::{create, list, get}`, `MemoryAdapter::ensure_subject` or a wire type
  the binding marshals re-checks this page against the new signature in the same change.
- Changing a result-object member, the error class, or which failures map to which code updates
  How a refusal reaches JavaScript in the same change.
- Changing a Cargo feature, the load order or the environment variable updates the feature table or
  Loading the addon in the same change.
- Moving or adding a listed path updates Where it lives in the same change.
