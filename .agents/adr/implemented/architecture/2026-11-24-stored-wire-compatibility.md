# ADR: Stored wire compatibility across the SPOKE 0.18.1 gate

Status: implemented — the upgrade must read rows written by 0.18.0, and the protocol DTOs already refuse the new spellings on ingress.

## Problem

`spoke-schemas` 0.18.1 gates every generated `Timestamp` through `timestamp::parse_strict`, which admits only the RFC 3339 grammar (`T` between date and time), and admits revision integers up to the inclusive `2^53` endpoint. Rows written by 0.18.0 can carry a space-separated timestamp: capture decoded the request for validation but copied the caller's original `occurred_at` string into the candidate, SQLite stored it verbatim, and reads fed it back through the IR decoder. After the pin bump, one such row fails `list_candidates` for the whole subject, and a staged import frozen with that spelling cannot activate. Separately, the transfer digests canonicalize with `serde_jcs`, which refuses the `2^53` integer token that 0.18.1 now admits — a peer can sign an import chunk at the endpoint that this adapter cannot seal.

## Decision

- Strict validation lives at protocol ingress. Every memory operation decodes its request through the generated DTOs first (`wire::decode`), so a new `memory.capture`, `memory.imports.stage` or `memory.imports.activate` carrying the space spelling is refused before any repository access.
- The shared IR decoder accepts the pre-upgrade spelling and keeps the stored string byte-for-byte. It is the only path that reads persisted rows back, so leniency there is the compatibility guarantee, and an accepted record still round-trips to the row it came from.
- Digest canonicalization re-spells an integer exactly at `±2^53` as its equal `f64` before `serde_jcs`. That is the number the TypeScript canonicalizer and connect 0.18.1's JCS emit; a value past the endpoint still refuses, so an unrepresentable integer never collapses onto a neighbour's digest.

## Alternatives considered

- **Rewrite stored rows at upgrade.** Rejected: the store is embedded with no migration API promise, and rewriting every timestamp-bearing column of every table is a wider change than one decoder rule.
- **Normalize timestamps on every stored read before the strict decoder.** Rejected: it repeats the tolerant parse at each reconstruction site — candidates, records, receipts, reports — where one shared decoder covers them all.
- **Keep the IR strict and re-spell on write only.** Rejected: it cannot help rows already on disk, which is the failure being fixed.
- **Refuse the `2^53` endpoint in transfer digests.** Rejected: connect 0.18.1 signs chunks at the endpoint, so refusing here would reject a valid peer request.

## Consequences

- The IR surface admits both date-time separators for a caller that decodes raw wire directly; only the DTO boundary enforces the strict grammar. A record accepted with the space spelling is stored unchanged and normalizes to `T` when a peer decodes its DTO.
- A non-revision member holding exactly `±2^53` also re-spells for the digest: a product-owned bag value at that exact bound is accepted here where connect's path-scoped normalizer would refuse it.
- `mindcore-base` no longer depends on `chrono` for timestamp validation.
