# ADR: Freeze the import identity mapping at staging and apply it at activation

Status: implemented — staging writes the translated payload and identity report, and activation applies them without re-reading the source sequence
Date: 2026-11-22
Class: architecture
Related: `2026-11-22-transfer-capability-advertisement.md`

## Problem

Activation would have to re-derive what staging already verified. A staged bundle arrives as ordered chunks carrying source identifiers. If activation rebuilt the subject/record/candidate mapping and the translated references from the raw chunks, it would re-run verification on a sequence it had already verified, and the destination identity the caller saw in the staged report would not be the identity the flip wrote. Any re-derivation difference would surface only after the visibility flip, when it cannot be reported as a staging refusal.

## Decision

Staging freezes the destinations; activation applies them. The final staging chunk runs verification, builds the target identity mapping and the translated references, and writes both the translated payload and the report through `finish_import`, then deletes the raw source chunks (`crates/mindcore-spoke/src/transfer/stage.rs:684,697`; `crates/mindcore-sqlite/src/repository.rs:1631`). `activate_import` decodes that frozen payload (`decode_staged`, `crates/mindcore-spoke/src/transfer/activate.rs:688`), rechecks it against the live target (`recheck`, `crates/mindcore-spoke/src/transfer/activate.rs:889`), re-derives the caller's adoption cover, and flips visibility without re-reading the source sequence.

## Alternatives considered

- **Keep the raw chunks and derive the mapping at activation.** Rejected: it repeats the whole-sequence verification after the caller has already been told the sequence verified, and the identity mapping the staged report publishes would not be the mapping the flip applies. A late failure would be an internal fault after staging instead of a staging refusal.
- **Store the raw chunks beside the frozen payload.** Rejected: the frozen payload is the only thing activation reads and expiry scrubs, so keeping the sequence would duplicate the transfer's largest artifact for no reader.

## Consequences

- Staging pays one translation pass and one mapping build; activation's work is proportional to the frozen row set.
- An abandoned import expires after `STAGED_IMPORT_TTL_SECONDS`, and the sweep releases the frozen payload as well as any leftover chunk (`crates/mindcore-sqlite/src/repository.rs:1703`).
- Activation cannot apply a subject, record or candidate identity the staged report did not publish: `decode_staged` accepts only the frozen rows and `recheck` refuses a target that moved under them. Two identity classes take a fresh target identity the staged report does not map: an adopted grant, whose id is a store-wide key, and every reflection, which the import re-mints like any other row. §7.31's report carries only `grants_adopted`/`grants_dropped` plus a reflection count. The adoption cover is rechecked against the caller's own live grants at staging and again at activation.
- The frozen payload is trusted only because staging wrote it in the same store on the same transaction seam; a row it cannot decode is an internal fault, never a caller-visible refusal.
- A stored document whose `content.structured` (or a pending `proposed.content.structured`) is an explicit `null` cannot be exported: the pinned content type carries no present null in that member, so the export refuses (`INVALID_INPUT`, `details.reason = "explicit_null_content_not_transferable"`) rather than changing the document, and final staging verification refuses an arriving bundle with the same reason. A `null` nested inside `structured` or inside a cognition record is carried unchanged.
