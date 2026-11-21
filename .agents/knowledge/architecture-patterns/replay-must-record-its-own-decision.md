---
module: mindcore-spoke
date: 2026-11-21
problem_type: architecture_pattern
category: architecture-patterns
severity: medium
plan_id: residual-polish
applies_when:
  - A workflow may return a decision that leaves work pending, and the same request can arrive again later
  - Replay currently answers by reading the state it originally wrote
  - A human or another subsystem can change that state between the decision and the replay
tags:
  - replay
  - idempotency
  - workflow
  - decision-record
last_updated: 2026-11-21
---

# Replay must report the decision the workflow made, not the state it can currently observe

## Problem

`absorb` can return a pending outcome: it captured a candidate and handed the settlement decision to a
human. When the same request arrived again, the coordinator projected the candidate's *current*
state. If the candidate had since been settled or dismissed elsewhere, replay reported that later
outcome as if absorb had returned it — a host could no longer tell "absorb said pending" from
"absorb never decided".

## Root cause

The workflow treated its own write as the record of its decision. A candidate's state is mutable
state, not a decision log: anything that can settle a candidate can rewrite what replay will answer.
Idempotency needs the original answer, and the only durable place for it is a record the workflow
alone writes and never projects.

## Pattern

1. **Record the decision, in the workflow's own namespace.** Absorb writes one applied, target-free
   row (`op = mindcore.absorb.decision`) in a short `BEGIN IMMEDIATE` transaction after the admitted
   capture commits, keyed by the capture receipt and carrying the pending candidate ids in receipt
   order. No new table: the journal already holds workflow-owned records.
2. **Read the record before projecting anything.** Replay looks up the decision first. Only a request
   with no record falls back to projecting current state — that fallback keeps a request that never
   produced a decision from inheriting someone else's answer. A request with no record is the retry of
   a run that failed before writing, and it repairs only what the receipt still shows: a receipt whose
   candidates are all still pending proves that run settled nothing, so the retry records the receipt's
   complete candidate list — the whole domain the decision ranged over — before returning. A receipt
   that is already mixed (some consumed, some pending) proves nothing about that decision, so the retry
   records no marker and reports each candidate from its own state.
3. **Fail closed on an inconsistent record.** Compare the complete identity the writer recorded
   (receipt, subject, requester, idempotency key) and validate the payload's shape: nonempty, unique,
   receipt-ordered candidate ids, and an object with exactly one member. A malformed or mismatched
   record is an error, never an absent record — otherwise corruption silently degrades to "no
   decision" and hands back a stale projection.
4. **Look the record up by the key that is actually unique.** A lookup filtered on a second column can
   hide a mismatched row as missing; read by the globally unique receipt id and validate scope
   explicitly.

## Why it works

The decision survives exactly as long as its own record, independent of what happens to the candidate.
Replay is idempotent by construction: a replay that finds the record returns the recorded answer,
writes nothing new, and never auto-settles. A replay that finds none while the receipt is still wholly
pending writes the record once — the retry-repair of a run that failed between its capture commit and
the marker write — and every later replay returns that same answer.

## Traps

- **A second transaction is a window.** The decision record commits after the capture. If that write
  fails, return an error instead of a `Pending` success; never swallow it.
- **A partial identity check is worse than none.** Comparing only the receipt and result lets a row
  with a different subject pass as authoritative.
- **Subset checks accept empty lists.** "All listed ids are in the receipt" is satisfied by `[]`, and
  an empty list then means "no pending work" — the opposite of the recorded decision.
- **A repair the receipt cannot prove.** A receipt with some candidates consumed and some pending no
  longer shows what the failed run decided. Recording the whole list there permanently reports a
  candidate that already produced a record as pending. Repair only a wholly pending receipt.
- **Classifying caller input as provider failure.** When a caller-supplied field fails a schema the
  provider draft would also be checked against, reporting `InvalidReasonerOutput` blames the wrong
  side; classify it as an input error and leave provider rejections untouched.

## References

- `crates/mindcore-spoke/src/workflows.rs` (absorb coordinator, decision read/write)
- `crates/mindcore-spi/src/workflows.rs` (`CandidateLifecycle`, `stamp_cognition_record`)
- `.agents/knowledge/architecture-patterns/protocol-adapter-two-phase-pattern.md`
