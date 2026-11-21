# ADR: The absorb decision log lives in `memory_journal`

Status: implemented — the journal already holds workflow-owned records keyed by receipt, so a second persistence surface bought nothing
Date: 2026-11-21

## Problem

`absorb` can end a run with captured candidates still pending, and the same capture request can
arrive again: a retry after a failed follow-up write, or any later replay. Replay must report the
decision that run made — `Pending` — rather than the candidate's current state, because a candidate
is mutable: a human or another subsystem can settle or dismiss it between the decision and the
replay. Without a durable record, a host cannot tell "absorb said pending" from "absorb never
decided", and it reported the later resolution as if the original call had produced it.

The candidate row cannot carry this record. It is state owned by the settlement path, and every
transition that consumes the candidate rewrites it.

## Decision

The decision log is one `memory_journal` row per capture receipt: `op = mindcore.absorb.decision`,
`state = applied`, no targets, and `result = {"pending_candidate_ids": [...]}`. It is keyed by the
receipt id the run's capture committed — a column the journal already declares `UNIQUE` — and
carries that receipt's space, subject, requester and idempotency key so a reader can validate the
complete identity. The pending ids are stored in capture receipt order.

`record_absorb_pending` writes the marker on a short `BEGIN IMMEDIATE` transaction after the capture
commits, after verifying that exact receipt; an equal marker already present is a no-op and a
mismatched one is a storage error. Replay reads the marker before projecting anything, and a replay
whose candidate is still pending with no marker — the retry of a run that failed between the capture
and that write — records the marker before returning, so the `Pending` it reports becomes the answer
later replays give. A replay that projects a consumed candidate stays read-only and reports that
candidate's own recorded outcome.

## Alternatives considered

- **A dedicated decision table.** Rejected: the journal is already the durable record of what a
  workflow did under a receipt, with the receipt as its unique key and target-free applied rows as
  its vocabulary. A new table would duplicate that key, the insertion order and the immutability
  guarantees, and add a second schema source to keep in step with the migration.
- **Annotating the candidate row.** Rejected: the decision has to outlive every later transition,
  and an annotation on exactly the row the settlement rewrites is what replay cannot trust.
- **Treating the capture receipt as the decision.** Rejected: the receipt records the capture, not
  what the absorb run decided about each candidate, so it cannot tell "never decided" from "decided
  pending".

## Consequences

- One marker per capture receipt, so replay is idempotent by construction and the write is safe to
  retry.
- The marker commits in a second transaction after the capture. A failure there fails the run
  instead of returning a `Pending` it never recorded; the retry completes the write.
- The decision survives exactly as long as the receipt it names. A reader must validate the whole
  identity: comparing only the receipt and the payload lets a row with another subject or requester
  pass as authoritative, and a subset check accepts an empty list as "nothing pending".
- `absorb` becomes a second writer of `memory_journal`, so the journal's operation vocabulary must
  keep workflow decisions (`mindcore.absorb.decision`, target-free) apart from settlements that owe
  publications.

## References

- `.agents/specs/architecture/serving-seam.md` (`tools.mindcore.absorb` is the capability that reports `pending`)
- `.agents/specs/data/store-schema.md` (`memory_journal`, the table the marker lives in)
- `crates/mindcore-spoke/src/workflows.rs` (`ABSORB_DECISION_OP`, `absorb_decision`, `record_absorb_pending`)
- `crates/mindcore-spi/src/workflows.rs` (`CandidateLifecycle`, the `absorb` replay path)
- `.agents/knowledge/architecture-patterns/replay-must-record-its-own-decision.md`
