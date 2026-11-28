# ADR: A registered cognition carrier cannot be superseded
Status: implemented — the supersede path refuses a carrier named by any Mind's registry row.
Date: 2026-11-28

## Problem

A Mind's registry row names its cognition carrier forever. Superseding that entry retires the row
the registry still points at; the carrier can then be neither read nor appended, and the Mind is
stranded with no path back to a live carrier. The failure surfaces later as a storage fault on a
cognition read, far from the supersede that caused it.

## Decision

`knowledge supersede` refuses an entry that is the registered carrier of any Mind in this store,
matching on both `knowledge_collection` and `carrier_entry_id`. The same entry id in another
collection, and ordinary entries of a Mind's own collection, keep the ordinary retirement
behaviour.

## Alternatives considered

- **Allow it and let the next cognition read fail.** Rejected: it turns a state bug into a
  storage fault and strands the Mind with no caller-visible cause.
- **Allow it and rewrite the registry row to a replacement carrier.** Rejected: it silently moves
  the Mind's cognition identity and discards the append-only carrier history the registry was
  protecting.
- **Refuse on the entry id alone.** Rejected: the same id in another collection is a different
  entry and must remain supersedable.

## Consequences

The check is a `COUNT(*)` over `minds` inside the supersede transaction. A Mind whose carrier was
already retired before this rule keeps its row; this decision does not repair existing stores.
