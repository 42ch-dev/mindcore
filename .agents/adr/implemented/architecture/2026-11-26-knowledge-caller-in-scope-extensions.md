# ADR: The served knowledge face carries the caller in `Scope.extensions`

Status: implemented — layer 1 ships the port, the two binding helpers and the guard `MindKnowledgePort`; layer 2 composes the admission callback that writes the binding and the per-Mind served face around it
Date: 2026-11-26
Class: architecture
Related: `2026-11-20-storage-ir-layering.md`, `2026-11-25-one-surface-serves-many-minds.md`, `2026-11-25-mind-is-one-memory-space.md`, `.agents/specs/architecture/serving-seam.md`

## Problem

`I-000137` records that `port.knowledge.put` appended `modules.cognition.records` without consulting the caller's `knowledge_write` grant. `RoutedKnowledgeStore::served_put` writes the submitted records beyond the committed prefix into `cognition_records` through `insert_record`, and before that it checks only the route and SPOKE's disclosure predicate — neither reads a grant. `deliberate` requires `knowledge_write` for the same append, so the served face offered a write path the workflow path refused.

The missing link was identity. `ScopedKnowledgePort` carries no principal (`spoke-operations` `adapter/ports.rs`), so the store cannot know whose grant to read, and the admission callback that does know the peer sees no operation — under A1 it sees the peer and, at most, the request's scope selector. A guard placed inside `RoutedKnowledgeStore` would have to be given a caller the storage port's contract does not carry, or infer one from a payload value.

A peer can put the same extensions namespace in its own request scope. Only Connect's `effective_scope` merge decides what reaches the store: admitted namespaces win over the request's on conflict, while a request's extensions pass through unchanged when the admitted scope carries none (`responder.rs`, the extension merge). That is an upstream rule this workspace neither owns nor enforces — but its observable behaviour is testable from here, and the signed adversarial loopback does exactly that: the merge runs in the responder, and the answer a forging peer receives is what the test asserts. It protects the binding only in its first half; the second half is why an unbound admitted scope is a hole.

## Decision

The caller travels in the `Scope` the admission returns, under this workspace's own extensions namespace: `mindcore_caller` / `principal`, a name the generated `ExtensionKey` pattern `^[a-z][a-z0-9_-]*$` admits. `mindcore_sqlite::bind_caller` is the shape's single writer and `bound_caller` its single reader; `validate_admission_scope` admits that one namespace and still refuses every other extension selection.

`mindcore_spoke::MindKnowledgePort` is the one `ScopedKnowledgePort` a served knowledge face installs. Each of its three methods runs one order — the scope's shape, then the caller's own live grant, then the wrapped store:

- `get_scoped_knowledge_entry` and `list_scoped_knowledge_entries` require one live grant of that principal in the Mind's space covering `knowledge_read`.
- `put_scoped_knowledge_entry` requires one live grant covering **both** `knowledge_write` and `knowledge_read`.

A write requires both because Connect reads the entry through `get` before it writes: an admitted scope that may write but not read could not complete the operation the face offers, and a guard that admitted it would let `put` reach a row the same caller cannot read back. The rule is stated per operation, and one grant must cover the whole set — requirements never union across grants, exactly as `one_grant_covers` already decides for memory.

The guard lives in the protocol adapter, not in `mindcore-sqlite`. Grant evaluation belongs to the protocol adapter (`2026-11-20-storage-ir-layering.md`), which is why the check is not placed inside `RoutedKnowledgeStore`; that store keeps its `ScopedKnowledgePort` implementation because the port wraps it through that trait, and it keeps no grant rule of its own because it is the storage port, not a face. The port evaluates through the adapter's own admission helpers (`admit_mind`, `one_grant_covers`), not a second copy.

The port fails closed, and it answers by what is wrong. A **malformed** scope — `extensions` present but not exactly the binding: an empty map, a foreign namespace, an extra member of `mindcore_caller`, or a `principal` that is not a non-empty string — is refused by `validate_admission_scope` before the binding is read, as a caller input error: `INVALID_INPUT` with the fixed shape message and no `details`. A **well-formed** scope that carries no usable binding (no `extensions` at all), a caller with no live grant in the Mind's space, and a caller whose grant does not cover the operation all answer one fixed `NOT_AUTHORIZED` with no `details`, carrying Connect's own admission-deny message, so a refusal for want of a capability is the same bytes as a refusal for want of admission. No answer on either path names the caller, the capability or the space. A failed statement keeps the knowledge face's fixed storage text.

The cognition append stays. It is guarded, not removed: a caller holding such a grant appends records through `port.knowledge.put` exactly as before.

**Every admitted scope carries the binding.** The merge protects the caller only when the admission writes a namespace to win with, so binding on every successful return is part of the decision, not an implementation detail: a Mind the admission cannot resolve to a live admitted grant is refused, and an unbound scope is never answered. This is why the composed callback in `crates/mindcore-runtime/src/lib.rs` runs `bind_caller` and `validate_admission_scope` on the one scope it returns, on every path.

### What each layer delivers

Layer 1 implemented the port, the two binding helpers and the guard, and proved them directly: `crates/mindcore-spoke/tests/served_knowledge.rs` calls `MindKnowledgePort` with scopes the test binds and shows each refusal before any record is written, and `crates/mindcore-sqlite/src/knowledge.rs`'s tests pin the binding's round trip and `validate_admission_scope`'s narrowing.

Layer 2 delivered the face: `ServeFaces.knowledge` installs `RemoteScopedKnowledgeFace { store: MindKnowledgePort over RoutedKnowledgeStore, admission }`, and the admission resolves the authenticated peer, admits the request's Mind through `admitted_mind`, then binds and validates the scope it returns. `crates/mindcore-runtime/tests/loopback.rs` proves it over a signed session, including `a_peer_supplied_caller_namespace_does_not_override_the_admitted_caller` and `no_wire_response_carries_the_caller`. Layer 2 does not re-claim `I-000137`; it claims this per-operation authorization on the composed face.

## Alternatives considered

- **Gate the whole knowledge face on `knowledge_write`.** Rejected: it refuses every read to a read-only peer, and the served face has three operations with two different requirements. A face-wide capability is not the operation's, and it would also make the read rule depend on a write capability.
- **Drop the cognition append from the served `put`.** Rejected: it removes a capability the contract publishes, and it narrows the operation instead of guarding it. `I-000137` asks for the append to be authorized, not withdrawn, and a face that silently discards submitted modules is a second contract change.
- **Pass the principal through the store's own state.** Rejected: `ScopedKnowledgePort` carries no principal, and a store field would be per-instance state on a port the face assembles per Mind, so it would either pin one caller per surface or need interior mutation the trait does not express. It would also move a grant rule into the storage crate, against the layering decision.
- **Read the principal from the request payload.** Rejected: a caller-declared caller is the identity confusion `MEM-R003` forbids for memory, and it would let any admitted peer name another principal.
- **Infer the caller from the request's scope selector.** Rejected: the selector names a Mind, not a principal, and a peer may name a Mind it holds no grant in.

## Consequences

- **The authorization rests on both halves of Connect's extension merge.** Layer 2 writes the binding in the composed face's admission, and a peer that puts `mindcore_caller` in its own request scope is overridden by the admitted namespace because admitted extensions win on conflict — *and* only because of the other half: a request's extensions pass through unchanged when the admitted scope carries none. The second half is why the face binds on every successful admission and refuses a Mind it cannot resolve instead of answering an unbound scope. This workspace neither owns nor enforces the rule, but its observable behaviour is testable through a signed session, and the two adversarial loopbacks do that and run on every cohort upgrade: a peer-supplied namespace must not decide the caller, and no answer may carry the binding.
- **The composed face is one composition for every Mind.** `ServeFaces.knowledge` installs `RemoteScopedKnowledgeFace { store: MindKnowledgePort over RoutedKnowledgeStore, admission }`; the admission maps the peer through the host's `principals`, admits the request's Mind through `admitted_mind` and binds the caller it admitted. The route is resolved per call by `RoutedKnowledgeStore::admit` on the op's own transaction, so no collection is captured at assembly and one face serves many Minds. The manifest advertises `spoke-knowledge`, `ke-ownership` and `spoke-knowledge-scoped` together exactly when the flag is set. A host that runs the responder on a single-thread runtime cannot run the callback's store read, because the callback is synchronous and Connect runs it on the blocking pool; the `ServeFaces` documentation states the multi-thread requirement.
- **`RoutedKnowledgeStore` stays a public `ScopedKnowledgePort` without a grant rule.** A host that assembles its own face with it skips the guard. The type's documentation says so; this workspace composes it only through `MindKnowledgePort`, in `MindcoreServe::scoped_knowledge` and in tests, and the gap is disclosed rather than closed by a compile-time barrier.
- **Two transactions per served call.** The grant read takes the pool and never shares a snapshot with the store call, so an authorization decision and the write that follows it are not one atomic step. A grant revoked between them can let one read through; the next call is refused.
- **A refusal is unattributable.** Because the answer is byte-identical to the admission refusal, a peer cannot tell a missing grant from a missing Mind. That is the intended disclosure rule, and it costs diagnosability: a host observes the cause in its own admission path, not in the wire answer.
- **The binding is one namespace of this workspace's own.** `mindcore_caller` is product metadata, not a protocol member; a second served face that needs the caller reuses the same namespace and the same two helpers rather than inventing another.
