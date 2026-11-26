# Carrying authorization context in `Scope.extensions`

**Kind**: architecture pattern · **Date**: 2026-11-27 · **Origin**: iteration
`015-mind-serving-stage2`, layer 1 (`spoke-019-mind-create`)

## Problem

A served port may not carry the caller at all. SPOKE's `ScopedKnowledgePort` takes only `&Scope`,
and the scoped admission callback receives the peer id and the request's scope selector but never
the operation name. A host that needs per-operation grant checks therefore appears to have two bad
options: gate the whole face on the strictest capability, or drop the check.

The useful observation is that the port's methods are already separate operations. What is missing
is not the operation but the caller, and the caller can travel with the scope.

## Pattern

1. In the admission callback, resolve the authenticated peer to the principal and the requested
   selector to the resource, exactly as the authorization model already requires.
2. Write the principal into a product-owned namespace of `Scope.extensions` and return that scope.
   The generated `ExtensionKey` grammar is `^[a-z][a-z0-9_-]*$` and values are JSON objects.
3. In the port implementation, read the binding back and evaluate the grant per method, before
   touching the wrapped store.

Grant evaluation stays in the protocol adapter, not in the storage crate: the storage port remains a
plain storage interface that the guarded port wraps.

## The two halves of the upstream rule

The pattern rests on how the responder merges the admitted scope with the request's declared scope.
Both halves must be known, because relying on only one is a security hole:

- **Admitted namespaces win on conflict.** When both scopes carry the same extensions namespace, the
  admitted value replaces the requested one. This is what stops a peer from overriding the binding.
- **A request's extensions pass through when the admitted scope has none.** So if a successful
  admission ever returns an unbound scope, a peer-supplied binding survives and the port will trust
  it. **Every successful admission must bind.** A resource that cannot be resolved to a live admitted
  grant is refused, never answered with an unbound scope.

## Fail closed, and say nothing

A missing binding, a missing live grant and an uncovered operation should produce one fixed refusal
with no details, so a caller cannot distinguish "the resource does not exist" from "you may not touch
it". A malformed scope is a different case — it is a caller input error and answers as one, before
the binding is read. Neither answer names the caller, the capability or the resource.

Keep the accepted scope shape exact. Widening "reject every extension" to "reject other namespaces"
silently admits empty maps and extra members, which is an input-surface regression, not a feature.

## Proof obligations

- The adversarial test is not optional: a peer supplies the binding namespace in its request scope,
  and the admitted binding must win. This is the tripwire for the upstream merge rule on every
  cohort upgrade.
- Assert on raw envelopes, not on typed responses. Typed decode drops unmodelled members, so a leak
  under an unexpected key would be invisible. Inspect object keys as well as values, and scope any
  exception by response context — a refusal that should carry no receipt must not enjoy a receipt's
  attribution exception.
- Show the guard can fail: a test that only exercises the guarded path cannot distinguish a working
  guard from a fixture that never reaches the write.

## Costs and limits

- Authorization and the store operation may end up in separate transactions, leaving a window in
  which a revoked grant still completes one in-flight operation. Where that matters, authorize inside
  the operation's own transaction instead.
- The unguarded storage port remains public if the crate exposes it; a host assembling its own face
  over it bypasses the guard. Say so in the type's documentation.
- The binding is authorization context smuggled through a data-selection object. It is legitimate
  because the protocol reserves extensions for product metadata, but it is a dependency on upstream
  behaviour and must be treated as such in reviews.

## Related

- `.agents/adr/implemented/architecture/2026-11-26-knowledge-caller-in-scope-extensions.md`
- `.agents/specs/architecture/serving-seam.md`
- `durability-chain-is-not-existence.md` — the same discipline applied to persistence claims
