# Data: transfer bundle, chunking and import staging

## Scope

What an export bundle carries, how it is partitioned into wire chunks and reassembled, and what import staging and activation accept. It covers the transfer-plane behaviour a transfer caller and an importing store rely on. It does not define the columns and indexes of the four transfer-plane tables (`store-schema.md`), the capability advertisement and the per-invocation transaction (`serving-seam.md`), or why identities are frozen at staging ([frozen import identities](../../adr/implemented/architecture/2026-11-22-frozen-import-identities.md)).

## Current state

### Export bundle

- A subject with any unsettled durable intent is withheld whole from an export, its subject row included, exactly as every other authorized read withholds it; the bundle's counts therefore report the post-filter totals.
- A subject-scoped export carries the subject row it names even when the store holds no other subject.
- A consumed candidate's `consumed_by.record_ids` keeps only the record ids the export retains: an excluded id would disclose a withheld row and dangle at the target. The list may be empty.
- With `include_reflections` set, the subject's profiles are read even when the selection withholds them, because reflection staleness recomputes the pinned state digest, which covers profile revisions.

### Reading an export page

`memory.exports.get` re-authorizes the caller against the stored selection before it reads the row's expiry. A `NotAuthorized` or `NotFound` refusal becomes the one generic `export <id>` absence, the same answer an export the store never held gets, so neither the chunk ordinal nor the expiry leaks; any other refusal is returned unchanged. Only an authorized caller can learn that an export is `EXPORT_EXPIRED`.

### Partitioning

`transfer::chunk::partition` turns one sealed bundle into ordered chunks.

- The bundle's own `digest` member must equal the seal the caller computed. A disagreement, or a missing header field, is an internal fault: a bundle whose reassembly could not verify never leaves the adapter.
- Elements are streamed in the fixed array order `subjects`, `records`, `profiles`, `reflections`, `candidates`, `receipt_summaries`, `grants`, and each is measured by its JCS (RFC 8785) length.
- An array that is present but empty is emitted as `[]` on ordinal zero; an absent array is never emitted. `reflections` is the only member an export may omit.
- A chunk's `bytes` is the JCS length of the whole chunk object, `bytes` and `chunk_digest` included, settled as a fixed point because it contains its own digits. `chunk_digest` is measured as its 64-zero placeholder, the digest being a fixed 64 characters.
- `chunk_count` is unknown while the last chunk is still being filled, so the frame is measured with the widest count the stream could produce; the measured size is then never smaller than the emitted one.
- The header chunk is measured even for its first element, because the header alone can push an element that fits an ordinary chunk past the limit. A group past ordinal zero is measured only once it holds two elements, so an oversized single element travels alone.
- Frame lengths are cached per ordinal digit width: only the width is serialized into the frame, so the frame for ordinal 0 and one probe per wider width cover every chunk the bundle can produce.
- A bundle whose arrays are all empty still gets its one header-only chunk.

### Reassembly and final verification

- `reassemble` restores `extensions` to `{}` when the ordinal-zero header omitted it, so the digest and the payload schema agree with the logical bundle the sender digested.
- The source ids of `subjects`, `records` and `candidates` are unique within their array. The identity maps are many-to-one keys: a duplicate would pass verification and then break activation's mapping, stranding the import mid-sequence.
- A grant summary's `principal_ref` is an opaque correlation label: non-empty, without whitespace, and not credential-shaped. Credential-shaped means a live key prefix followed by that provider's own token length and alphabet, or a JWT in JOSE compact serialisation — two to five base64url segments beginning `eyJ` whose first segment decodes to a JSON object. The header is the test: the `eyJ` prefix, the alphabet and a three-segment count are shared by a versioned label such as `eyJreport_42.v1`, which stays a correlation label because its segments do not decode to JSON.

### Staging

`memory.imports.stage` requires a space-level `transfer` and `admin` grant. The family is on the freeze allowlist, so staging into a frozen space is a maintenance path, not a refusal. Each accepted chunk passes its per-chunk checks (`export_id`, ordinal, `chunk_count`, `bundle_digest`, `chunk_digest`, `bytes`) before it is written.

- **Absence.** A source export with no live staging import in this space for this caller answers `IMPORT_CONFLICT` with `details.reason = "no_staging_import"`. That is the only absence answer staging gives: it names no import, ordinal or expiry, and a local export the caller may not page is treated exactly like an export this store never held, because distinguishing them would disclose which guessed export ids exist. The chunks a caller stages verify against the bundle on their own.
- **An expired import.** An expired import is dead state, not a live one. Ordinal zero reclaims it inside the staging transaction — the row becomes `expired` and its chunks are deleted — so the same call starts the sequence again; a later ordinal answers the `expired` conflict instead.
- **A repeated header.** Ordinal zero while a live import already holds this source export's header answers the `duplicate_header` conflict.
- **Order.** `next_ordinal` alone decides what a later chunk may carry, so a repeat and a skipped ordinal take the same `out_of_order` conflict, with `expected_ordinal` and `received_ordinal` in the details. A refusal writes nothing, deletes nothing, and leaves `next_ordinal` where it was.
- **The final chunk.** The last ordinal runs the whole-sequence verification and reports every failed check in one refusal; its outcome is [frozen at staging](../../adr/implemented/architecture/2026-11-22-frozen-import-identities.md).

### Activation

`memory.imports.activate` applies the frozen payload and report.

- Subjects keep their source identity unless the target space already holds it. Under `rename_subject` the collision gets a freshly minted id and the report's `subject_id_map` carries the rename; under `fail` it is the `subject_collision` conflict. A bundle imported into its own source space therefore collides on every subject and renames all of them.
- Records, candidates and reflections always take fresh identities; their source ids stay reachable through `import_ref` and the report's `record_id_map` / `candidate_id_map`. A profile whose subject and kind already exist in the target is the `profile_collision` conflict.
- A record is stamped with this hop's `import_ref` (`import_id`, `source_record_id`, `source_revision`) only when it carries none and is a live record with content: an `import_ref` already on the record is the identity an earlier authority move gave it and is adopted verbatim as historical attribution.
- An imported reflection keeps its pinned `basis` as historical provenance and is stored with the write-time stale hint, because the target's live inputs are not the ones the source derived from.
- Adoption creates only the grants the caller listed in `adopt_grants`, each a permanent (non-expiring) row; a repeated `grant_id` counts in `grants_dropped`, and an id that is not a grant summary in the bundle is the `verification` conflict.
- No adopted grant may be wider than the caller's own current coverage, and a non-expiring adopted grant needs a non-expiring cover. Because an adopted grant is one row with one capability set, one single caller grant must cover every one of its capabilities; requirements never union across grants, so two separately held capabilities cannot be merged into a grant wider than either source. A violation is the `grant_not_subsumed` conflict naming the grant and its capabilities. The cover is re-derived at activation rather than trusted from the frozen import, because the grants become permanent only there.

## Invariants

- An element is never split: a chunk over `export_chunk_bytes` carries exactly one element.
- Every emitted chunk's `bytes` equals its own JCS length, and no measured frame is smaller than the chunk it admits.
- Every cross-reference inside a verified bundle resolves inside it; no identity map holds a duplicate key.
- A staging refusal writes nothing and leaves the import row unchanged; only ordinal zero reclaims an expired import, in the same transaction that opens the new sequence.
- Staging's absence answer names no import, ordinal or expiry, and an export read never discloses whether an unreadable export id exists.
- An adopted grant is never wider than the caller's own coverage, and no grant is adopted that the caller did not list.

## Where it lives

- `crates/mindcore-spoke/src/transfer/bundle.rs` — bundle assembly, sealing and the subject/candidate filters.
- `crates/mindcore-spoke/src/transfer/chunk.rs` — partitioning, frame measurement and the per-chunk digest.
- `crates/mindcore-spoke/src/transfer/stage.rs` — staging, reassembly, verification and the correlation-label screen.
- `crates/mindcore-spoke/src/transfer/activate.rs` — identity mapping, `import_ref`, reflections and the adoption cover.
- `crates/mindcore-spoke/src/transfer/exports.rs` — export creation and page reads.
- Tables: `memory_exports`, `memory_export_chunks`, `memory_imports`, `memory_import_chunks` (`store-schema.md`).
- Tests: `crates/mindcore-spoke/src/transfer/tests.rs`.

## Change obligations

- Changing a bundle member, the array order, the chunk frame or the measurement placeholders updates this page and the chunk round-trip assertions in the same change.
- Changing a staging refusal's code or `details.reason`, the expiry reclaim, or the ordinal rule updates this page and the staging scenario together.
- Changing which identities are kept, minted or stamped at activation, or how the adoption cover is derived, updates this page and the frozen-import-identities record when the freeze itself moves.
