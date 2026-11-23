---
module: mindcore-sqlite
date: 2026-11-22
problem_type: workflow_issue
category: workflow-patterns
severity: medium
plan_id: shared-scope-rehearsal
applies_when:
  - A plan proves acceptance facts rather than shipping a feature
  - The proof lives in an integration test against a real store
  - The same code path both performs the write and reports its outcome
  - A probe asserts that a screen rejects one input shape and passes a look-alike
tags:
  - testing
  - rehearsal
  - assertions
  - acceptance
last_updated: 2026-11-23
---

# A rehearsal asserts on read-back state, and every probe needs a positive control

## Problem

A rehearsal plan exists to prove facts, not to ship a feature. Its worst failure is a test that is
green while the fact is false. Three separate probes in one such plan were built the same wrong way:
they asserted on what the operation **returned**, not on what the store now holds.

- An adoption call returns an entry; the anchor was checked on that return value.
- A transaction was rolled back; the probe checked only the post-rollback state, so an update that
  reported success without writing would pass.
- A second store retried a write after the first closed; the probe checked the `put` return value.

All three pass while the capability is absent. None was caught by the plan's own review, because the
reviewer reads the same diff and the same green run.

## Root cause

The observation path and the exercised path are the same value. When a return value is the only
witness, the test proves that the API is *shaped* correctly, not that it *did* anything.

A second hole rides along: a negative assertion without a positive control. "A retired entry is absent
from both viewpoints' lists" also passes if every list is always empty, or if a viewpoint is refused
all reads. The negative side is only meaningful once the positive side is proven in the same test.

## Pattern

1. **Write, then read back through a different path.** `put` → `get`; adopt → re-read the copy from
   the personal collection; retry after the peer closes → read through the retrying store. Assert on
   the read.
2. **Prove the transition before proving the undo.** In a rollback probe, confirm *inside the
   transaction* that the rewrite actually landed (revision, payload, head, records), then roll back and
   confirm the old state returned. An undo test without the "it happened" half proves nothing.
3. **Pair every negative with a positive.** Before asserting that something is invisible, assert that
   the same principal can see something visible. A list that is always empty fails the positive leg.
4. **For a backup, close first and read the copy before writing to it.** A copy of a live WAL pair is
   not a snapshot; and if the restored facts are read after new writes, the read proves nothing about
   what the copy carried.
5. **Name the acceptance item in every assertion message.** A failure must say which fact broke, not
   that an assertion failed.
6. **Give a negative control the look of the positive.** A label that must pass a credential screen
   is a control only when it shares the screen's trigger — the prefix, the alphabet, the segment
   count. A dotless `eyJ…` value shares the JWT prefix but not its segment structure. A
   dotted label such as `eyJreport_42.v1` shares the prefix, the base64url alphabet and a
   place inside the accepted two-to-five segment range; both must still pass, because
   a JWT is decided by its first segment decoding to a JSON object. Without the look-alikes
   the screen could reject on prefix alone and the probe would stay green.
7. **Build credential-shaped fixtures at run time.** Assemble the value from a prefix constant and a
   tail constant (`format!("{header}.{claims}.{signature}")` for each compact serialisation, prefix
   plus a tail of the provider's own length and alphabet for a key) so the fixture source never
   carries a token- or key-shaped literal. The assembled values still exercise the screen, while a
   literal in the tree trips secret scanners and reads as a leaked credential in review.
8. **Enumerate the forms a screen must reject, not the assembly trick.** Assembling the fixtures says
   nothing about which shapes were exercised. For a JWT screen the coverage is every compact
   serialisation: a signed JWS, the same with an empty signature, one with the signature stripped,
   and an encrypted JWE. They differ in segment count and in whether the last segment is
   empty, and each needs its own assembled value and its own assertion, or a screen that misses
   one form stays green.

## What did not work

- **Asserting on the return value** — the three probes above. Each was green with the capability
  absent.
- **Defensive assertions on adjacent state.** Checking only revision and carrier head after a rollback
  looks thorough and still misses a leaked payload, because the payload is the thing that changed.
- **Trusting a green run as the proof.** A rehearsal's evidence is the strength of its assertions; a
  run that never exercised the failure direction is not evidence.

## Trade-off

Read-backs roughly double the assertions in a rehearsal target and require test-local helpers
(`expect_live_disclosure`, `expect_rewrite_written`). That cost is the deliverable: the plan's whole
value is that a future regression turns the test red.

## Verification shape

For each fact, ask: *what single-line change to production code would make this assertion pass while
the capability is gone?* If an answer exists, the probe is not a probe. The three shapes above — read
back, prove the transition, pair the negative with a positive — cover the cases found here.
