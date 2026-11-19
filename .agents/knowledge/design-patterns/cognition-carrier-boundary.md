---
module: mindcore-core
date: 2026-11-19
problem_type: design_pattern
category: design-patterns
severity: medium
plan_id: v01-story
applies_when:
  - A current judgment or belief must persist next to the knowledge it is about
  - A wire-validated module is embedded in a larger persisted document
  - Tests compare event times that also exist as wall-clock write times
tags:
  - cognition
  - carrier
  - validation
  - test-determinism
---

# One carrier entry, one validation point, story time for assertions

## Shape

Current judgment lives on the holder's `KnowledgeEntry` under the SPOKE `modules.cognition` carrier (`{records, world_facts, extensions}`). `MindState` is derived from that carrier's authorized revision — never persisted as a second authority. History is an append-only record list; the "current" record is the last committed one by an explicit order field, not by array luck.

## Rules that prevented real defects

- **Validate at the single persistence boundary.** A dedicated `write_carrier` helper is not a boundary: any caller can reach `put_knowledge_entry` directly. Put the module guard there, before the write reservation, and let every helper delegate. A guard on the convenience wrapper alone permits an invalid authoritative revision.
- **One holder identity everywhere.** The envelope's `holder_entry_id`, each assessment's holder/assessor reference, and the persisted carrier entry id must be the same string. Assert the equality; the derived field alone can hide a mismatch that makes the illustration lie.
- **Compare story time to story time.** Persisted `recorded_at` is the store clock. A fixed fictional assessment time compared against it becomes a time bomb that fails once real time passes the story. Assert `assessed_at >= observed_at` between story events, and keep wall-clock comparisons out of scenario assertions.
- **Append must not rewrite history.** After appending a second record, compare the complete earlier record against its pre-append value; id and order assertions alone miss content rewrites that then become the baseline for later snapshots.

## Known limit

A single carrier entry rewrites the whole record list inside the write transaction, so cost grows with history. Acceptable for a first slice with a documented residual; a bounded history or per-record rows is the upgrade path.
