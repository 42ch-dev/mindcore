# ADR: A committed create answers `OUTCOME_UNCERTAIN`, never its own failure
Status: implemented — the binding's create path has answered this way since the settlement code it shares.
Date: 2026-11-28

## Problem

`create` writes the space, the cognition carrier, the registry row and the owner's default grant in
one transaction, then opens the handle. If opening fails after that commit, reporting the open's
refusal as the create's refusal tells the host the create had no effect. A host that retries then
mints a second Mind for one request, and the first is orphaned with no caller that knows its id.

## Decision

After the create commit succeeds, an open failure answers `OUTCOME_UNCERTAIN` carrying the created
`mindId` and the open's own code. The create is never reported as failed once its row is durable.
The answer reuses the settlement code the transfer path uses for "durable, but the answer is
unconfirmed".

## Alternatives considered

- **Report the open refusal as the create refusal.** Rejected: the row is durable, so the answer
  would contradict the store and invite a retry that creates a second Mind.
- **Retry the open inside create until it succeeds.** Rejected: it hides an environment fault
  behind an unbounded loop and still leaves no id for the host when it gives up.
- **Roll the create back with a compensating delete.** Rejected: the transaction already
  committed; the delete races another caller's first read and can destroy a Mind the host believes
  it owns.

## Consequences

Hosts must treat `OUTCOME_UNCERTAIN` as "durable, answer unconfirmed" and reconcile by `mindId`,
the same rule the transfer settlement path imposes. The refusal carries the open's code as a
detail so the host can still diagnose the environment.
