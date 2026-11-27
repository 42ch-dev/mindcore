# Recheck a shared resource against the whole registry at call time

**Kind**: architecture pattern · **Date**: 2026-11-27 · **Origin**: iteration
`015-mind-serving-stage2`, layer 4 (`mind-adoption-face`)

## Problem

Assembly-time validation is not enough for a shared source. If a host configures a shared collection
and only rejects the ones that exist at assembly, a resource created afterwards can silently become
that shared source — and a caller admitted to a different resource can then reach its contents with
no grant checked against it.

## Pattern

Resolve the configured source inside the operation's own transaction, and check it against the whole
registry rather than against the operation's destination alone:

- Run the check after the caller's grant check and before the first source read, so an unauthorized
  caller never learns whether the source exists.
- Answer with a generic refusal that carries no details, indistinguishable from a missing source.
- Keep the destination's own resolution separate: the registry row names the collection the copy
  writes into; the collision check protects the shared source.
- Do not require a grant on the source resource if the source is deliberately not a resource in your
  model — the point is that it must never be one, not that it must be readable by the caller.

## Proof

Build the scenario the bug lives in: configure a collection for a resource that does not exist yet,
create it afterwards, and assert a caller authorized only for a different resource is refused and
nothing is copied. Then narrow the check back to the destination-only comparison and watch the
foreign entry actually get adopted and returned. Without that mutation, a test can pass against a
code path that no longer varies.

When a finding asserts a commit-then-refuse sequence, probe the premise before fixing. In this layer
the store already refused the unserveable shape, so the finding was unreachable; the guard added
afterwards is defence in depth, not a reproduction. Record that distinction instead of claiming a
stronger proof than exists.

## Related

- `carrying-authorization-context-in-scope-extensions.md` — the caller reaches the port bound into
  the scope, and the grant is evaluated per operation
- `never-answer-success-before-the-outcome-is-durable.md` — the same discipline for settlement
