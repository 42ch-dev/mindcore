# Never answer success before the outcome is durable

**Kind**: architecture pattern · **Date**: 2026-11-27 · **Origin**: iteration
`015-mind-serving-stage2`, layer 3 (`mind-history-face`)

## Problem

A destructive operation can commit and then fail to record its own outcome. If the recording step
returns nothing, the caller is told the operation succeeded, and a later retry under the same key
finds no recorded outcome — so it either executes again or answers as though the success never
happened. Both are worse than an honest failure.

This is easy to miss because the defect only appears on a fault path: every happy-path test passes.

## Pattern

Treat the outcome record as part of the answer, not as housekeeping after it.

- If the outcome did not persist, the caller must not receive success. Where the transport contains
  a task failure per request, use that channel deliberately and document the containment.
- Answer **uncertain**, not "no effect", when the operation may have committed. Claiming a clean
  failure would be a second lie: the write may be durable while its outcome record is not.
- Leave the claim untouched so a retry resolves as unsettled rather than re-executing.
- Look for the same bug through every door. In this layer a failed namespace lookup was folded into
  "nothing to settle", which reproduced the acknowledged-success bug without any failing write.
  A resolver that can fail must return a `Result`, and its error arm must not read as "nothing to
  do".

## Proof

Force the settlement write to fail after a real commit — a trigger that refuses the update works
well — and assert three things: the caller does not see success, no outcome row exists, and a retry
does not observe a phantom success. Then check the write actually happened, because "uncertain" is
only honest if the operation may have committed.

A test that merely exercises the happy path proves nothing here, and a test that only asserts the
error code without checking the retry is half a proof.

## Related

- `durability-chain-is-not-existence.md` — existence is not persistence, the read-side version of
  the same discipline
- `carrying-authorization-context-in-scope-extensions.md` — the fault-channel containment used for
  admission and reused for settlement
