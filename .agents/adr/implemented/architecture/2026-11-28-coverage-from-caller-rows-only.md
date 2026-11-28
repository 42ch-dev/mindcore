# ADR: Coverage is computed from the caller's own grant rows only
Status: implemented — the knowledge coverage gate filters to the caller's rows before projecting.
Date: 2026-11-28

## Problem

A caller's coverage decision must answer for that caller alone. If the gate read the whole space's
grants, another principal's row could change the caller's answer. If it projected every row before
filtering, a dead row of another principal would turn the caller's refusal into a storage error.
A dead row of the caller's own must likewise cover nothing rather than fail the projection.

## Decision

Coverage is computed over the caller-only rows the admission already read. A dead row of the
caller's own covers nothing and is not projected; it does not become a storage error. The refusal
is the same space-coverage refusal the caller would get with no grant at all.

## Alternatives considered

- **Compute coverage over the whole space.** Rejected: another principal's grant would change the
  caller's answer, which is a cross-principal information leak.
- **Project every row, then filter by caller.** Rejected: a dead row in another principal's data
  would turn a refusal into a storage error the caller cannot act on.
- **Treat a dead row as coverage.** Rejected: it grants an operation that has no live grant behind
  it.

## Consequences

Refusal stays a refusal regardless of other rows in the space, and a caller holding only dead rows
is refused. The projection cost is bounded by the caller's own grant rows.
