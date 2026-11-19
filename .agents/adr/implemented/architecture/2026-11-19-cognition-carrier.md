# Cognition carrier on the holder's KnowledgeEntry

Status: implemented — one carrier keeps a single authoritative current judgment without a second store
Date: 2026-11-19

## Problem

A Mind needs a durable current judgment ("what this actor currently believes") next to the knowledge that judgment is about. The iteration could persist that in a dedicated cognition table, in a separate projection, or on the holder's existing `KnowledgeEntry`.

## Decision

Store current judgment as SPOKE `modules.cognition` records on the holder's `KnowledgeEntry`, and derive `MindState` from that carrier's authorized revision on demand. Validate the module at the single persistence boundary (`put_knowledge_entry`) and append records with an explicit order, rejecting duplicate ids.

## Alternatives considered

- **A dedicated cognition table.** Rejected: it becomes a second authority for the current judgment and needs its own revision, authorization and disclosure model.
- **A persisted `MindState` projection.** Rejected: a projection can drift from the carrier and would require reconciliation.
- **Guard only in the convenience carrier helper.** Rejected after review: any caller reaching the port directly could commit an invalid authoritative carrier; one guard at the persistence boundary covers every path.

## Consequences

- The projection group and the holder entry must name one identity; derivation copies the carrier id, so the assessment holder/assessor must equal it.
- Appending rewrites the whole record list inside the write transaction; cost grows with history. A bounded history or per-record rows is the upgrade path, tracked as a residual.
- Scenario assertions must compare story times to story times, not to the store clock, or they become time bombs.
