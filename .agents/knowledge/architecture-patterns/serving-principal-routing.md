---
module: mindcore-runtime
date: 2026-11-21
problem_type: architecture_pattern
category: architecture-patterns
severity: medium
plan_id: principal-routing
applies_when:
  - One served surface must serve more than one admitted principal
  - A workflow reads or writes data scoped to the caller rather than global configuration
  - A test is asked to prove that a call did not touch another tenant's data
tags:
  - serving
  - routing
  - multi-principal
  - isolation
  - testing
---

# Route inside the binding by admitted principal, not per surface

## Problem

A serving surface that binds one carrier principal at assembly time looks correct while the host has one tenant. `ServeFaces` held `workflow_caller: Option<String>` and the workflow binding held one admitted collection, so every `deliberate` was compared against that single caller. A second admitted principal could read memory but never settle a carrier through the same signed session; the host had to open one surface per principal, duplicating the responder, manifest and admission wiring.

The tempting alternative — accept a scope or collection from the request — moves tenant selection onto the wire, where a caller can choose another principal's destination.

## Solution

Keep selection host-side and make it a map:

- The route table is surface configuration: `principal_id → KnowledgeScope`, keyed by the id the host's `MemoryAdmission` returns for the authenticated peer. Wire arguments never contribute a key.
- Validation happens once at assembly: an empty principal id or empty collection is `InvalidInput`; an empty map is legal and means the surface serves no carrier workflow.
- The binding resolves the route in exactly one helper, borrowed, by `principal_id(principal)`. Both consumers use it: carrier admission for `deliberate`, and the knowledge slot for `recall_context`. Nothing else reads the map.
- A missing route at carrier admission is terminal, target-free `NotAuthorized`. A missing route at recall is not a refusal: recall returns its memory page with an empty knowledge slot, because memory authorization never depended on a knowledge route.
- Two principals may name the same collection. Their carriers are distinct holder entries, so sharing a destination is a configured choice, not an isolation promise.
- An empty map removes the tool from the final registration set, including any host registration of the same capability id.

## What did not work: proving a read never happened

The first acceptance wording said "neither invocation reads the other collection". That is not observable: the binding exposes no per-statement read seam, and a test-only read counter would add product surface for a property no consumer can act on. A test that watches writes, revisions and row counts cannot distinguish "read A and ignored it" from "never read A".

The working acceptance asserts observable effect instead: for each call, snapshot the non-target collection's entry count, cognition-row count, carrier revision and `updated_at` before the call and compare after it. This catches the failure that matters — a cross-collection write, disclosure or revision bump — without inventing an internal probe.

## Consequences

- Changing a principal's collection requires assembling a new surface; there is no hot reload. That is recorded rather than implemented because no caller reconfigures routes at runtime.
- Route lookup is borrowed and allocation-free, but it does run on every carrier write and recall slot read. Live grants are still re-derived per transaction; a route never substitutes for a grant.
- The refusal stays target-free: it never reveals whether another principal has a route or which collection it names.

## References

- `.agents/specs/architecture/serving-seam.md` — the serving contract and the two route consumers
- `crates/mindcore-spoke/src/workflows.rs` — `MemoryWorkflowBinding`, the route lookup and carrier admission
- `crates/mindcore-runtime/src/lib.rs` — `ServeFaces` and surface assembly
