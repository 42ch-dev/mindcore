# ADR: Knowledge documents are written only through the controlled entry point

Status: proposed — no write entry, gate or export exists; the constraint is agreed and unimplemented
Date: 2026-11-27
Scope: Who may write a Knowledge document, through which path, with what validation and attribution, and how a human reviews one
Related: `2026-11-27-dual-store-knowledge-boundary.md`, `2026-11-27-canonical-knowledge-format.md`, `2026-11-27-bare-git-knowledge-store.md`, `2026-11-27-per-document-versioning.md`

## Problem

A working tree invites edits. An editor, a script or a careless `sed` can change a document without passing any validation, and the result is a state that is neither the committed version nor a validated candidate: the store cannot say what is current, and every reader has to be told which copy to trust. Editor temporary files, lock files and partial saves make the ambiguity worse, and a filesystem watcher cannot tell an intentional write from a stray one.

The current write path shows what is missing even without a working tree. `write_entry` stores whatever payload arrives, under the submitted id, with an integer revision check and no format validation (`crates/mindcore-sqlite/src/knowledge.rs:1733-1741`). There is no staging area, no cross-document invariant check, and no record of who decided the content beyond the admitted principal.

If the refactor adds files without deciding this, the first thing that happens is a human editing one, and the gate becomes advisory.

## Proposal

The Knowledge store will have no working tree, no index file and no checkout on any host. It will be created bare and will never be checked out; a human will have no path to write its files.

Every write will pass one controlled entry point, in a fixed order:

1. **Admit.** The caller's granted scope will be resolved to a Mind and a read/write grant, exactly as a served knowledge write is admitted today.
2. **Resolve the fence.** The write will name the versions it read, one per document. A create will name no version and will mean "this id is absent", checked against an absent head ref; it will never continue an existing chain. A deletion will name the version it deletes and append a tombstone. The store will resolve each named version to that document's commit before validation, so validation runs against a known base (`2026-11-27-per-document-versioning.md`).
3. **Validate.** The document will be checked against the accepted format, required metadata, section ids, declared lineage and the namespace claim. The rejection set will be the format rules of `2026-11-27-canonical-knowledge-format.md` plus the lineage rules of `2026-11-27-stable-section-identity-and-drift.md`: duplicate id, reused id, a lineage `from` that is not old-only, a target that is not new-only, a section named twice, a split outside its bound, a move with an unequal fingerprint, a target in a document the operation did not name, a missing or malformed id attribute, and trailing whitespace. A retained id whose heading text or position changed will be accepted without lineage. Validation will be deterministic and will not call a model.
4. **Confirm dependencies.** The write will declare the documents it depends on and, for rules checked at publication, the exact version of each. Immediately before publication the store will re-read each named dependency's head and compare it to the declared version. A mismatch will refuse the affected write before its compare-and-swap.
5. **Stage.** The candidate commit(s) will be written to the object database under staging refs. No head will move.
6. **Publish.** A single-document write will move that document's head by compare-and-swap. A related-document write will create one batch record under one batch ref by a single must-not-exist compare-and-swap — the decision point — then move each named head by its own compare-and-swap. The store will not claim an all-or-none visible publication; it will report a per-document outcome (`2026-11-27-bare-git-knowledge-store.md`).
7. **Project and answer.** The derived projection will advance and the caller will receive the committed versions, the batch id when there is one, the per-document outcomes and a receipt.

A rejection will be total: a document that fails one validation rule writes nothing, and no partially valid document will be stored in a quarantine state. The response will name every violated rule with its byte range.

**Cross-document rules have two classes.** A rule checked at publication will exist only when the writer names exact dependency versions and the rule is a function of those documents' bytes; a moved dependency will refuse the write with a conflict, before any publication. A rule that cannot be decided that way — a corpus-wide uniqueness, or a dependency the writer did not pin — will be checked after publication and reported as a finding. No rule will use a Mind-wide lock, and the residual window between the dependency confirmation and the compare-and-swap is covered by the after-publication class.

**An uncertain outcome will be resolvable.** Every write will carry a caller-supplied request id. The store will record an authoritative write intent, and the committed commits will carry the request id, its digest and a caller proof. A negotiated, caller-authorized receipt lookup will answer `committed` with the stored result when the caller's request matches, `in_flight` while an attempt holds a live reservation, `incomplete` with the exact unpublished documents for a partially published related operation, and `unknown` only when no reservation exists, no commit matches, and no attempt can still publish — which is the only state in which a resend is safe. Another principal's request will answer as unknown, without revealing that it exists. Replaying the put is not the resolution: a repeated create or update is refused before the store runs.

**Activation and legacy occupancy.** The reserved prefix and type will be enabled by one activation transaction: it will insert a `legacy_occupied` claim for every existing `spec.` id in the entry and revision tables and then write an active marker. Until the marker exists, a document write will be refused and every prefixed id stays a dynamic row. After it, a document create will reserve a `spec` claim in the same transaction that reserves its write and will be refused if any claim exists; a dynamic write will be refused the prefix unless the id carries a `legacy_occupied` claim, in which case it continues as the ordinary row it is. A tombstoned document keeps its claim, so its id is never reused, and no existing row is renamed or migrated.

**Attribution.** Each document commit will carry the admitted principal, the session, the request id and its digest, the tool or agent identity, and the compiler version. These trailers will be readable from the commit alone, so a rebuilt projection does not lose them. A client will not be able to set historical or cross-document trailers from its own payload.

**Human review is read-only.** A human will inspect a document through an export of a chosen version — the exact bytes plus a manifest naming document, version, commit and section digests — and through a read-only view. There will be no import path, because an import would be a human write by another name; the only way to change a document will be to submit a new version through the gate.

**One gate for every caller.** Served peers, product tools, workflows and maintenance jobs will all use the same entry point. A maintenance job that repairs a projection will not be allowed to write a head ref by a second route.

## Alternatives considered

- **A normal repository with a working tree, human edits and server-side hooks.** Familiar and cheap to adopt: editors, diffs and reviews already work. Rejected because hooks run after the edit, the tree can hold unvalidated bytes at any moment, and "no human edits" becomes a convention the store cannot verify. The dirty state would also have to be reconciled with the version fence, which is a second source of truth for "current".
- **A writable checkout for operators with an emergency bypass.** Viable operationally. Rejected: a bypass becomes the normal path under pressure, and the gate's rejection evidence disappears exactly when it matters. An emergency repair will instead be a normal write of the corrected document, attributed to the operator.
- **A dirty overlay over the committed documents.** Rejected: it reintroduces the two-state ambiguity the store exists to remove, and every read would need an overlay rule.
- **Documents as SQLite rows with a validation layer.** The current shape, plus the gate. Viable and simpler to deploy. Rejected because the file store is the decision of this refactor: content addressing, per-document history and byte-exact hydration all follow from it, and a validation layer over JSON payloads would still have no section spans or immutable versions.
- **Direct object writes without staging.** Fewer steps. Rejected: an agent would have no sandbox in which to compile a candidate and run cross-document checks, and a failed validation would leave unreachable objects with no lifecycle.
- **Export → edit → re-import.** Rejected as the human write it is: it would create a second authority (the edited file) and a merge question the gate cannot answer.
- **A shared publication lock so cross-document rules can be checked without races.** It would make publication-time checking complete. Rejected: it is the Mind-wide lock the owner forbids, and it would serialise unrelated documents. The two-class rule detects the residual window instead.
- **Treat a replayed put as the receipt.** No new surface. Rejected: a repeated create or update is refused before the store runs, so the replay is indistinguishable from a competing writer's win; the caller cannot learn whether its own write landed.
- **Rename legacy `spec.` rows to free the prefix.** Rejected: the store keys writes on the submitted id, so a rename is a new row; moving live rows, preimages, relations and adoption anchors risks data that works today, and every citation would change. Claims keep them reachable and reserved instead.

## Acceptance criteria

- A host will provide no writable checkout of the Knowledge store; a test will assert that opening the store never creates a working tree or index file.
- A write that violates one format rule, one duplicate section id, one invalid lineage declaration and one moved publication-time dependency will each be refused with the rule name and the offending byte range, and no head ref will move; an undeclared rename will not be refused, because it is classified as a deletion and an addition.
- A write that passes will publish exactly the named documents; a related-document write with one failed validation or one moved named dependency will leave every named document at its previous version.
- A related-document write that fails after its decision point will report exactly the unpublished documents, and every other named document will be at its new version; no document will be reported committed before its head moved.
- A create against an existing id, live or tombstoned, will be refused without moving the head.
- A blank-line-only change outside a fence will pass; a trailing-whitespace document, a duplicate id, a reused id and an invalid lineage target will each be refused with the rule and byte range, and the same list will be asserted in the section-identity record's tests.
- Dropping a create or update response after the commit will be resolved by the receipt lookup as `committed` with the stored result, without replaying the put; another principal's request for that id will answer as unknown.
- After activation, a seeded `spec.` dynamic row will still be read, written and replayed from SQLite, while a document create under its id is refused; before activation, a document write is refused.
- Every committed document will expose principal, session, request id, request digest and compiler version from the commit alone.
- An export of a version will be byte-identical to the blob and will name the version, commit and section digests; no operation will accept exported bytes as a write without going through the gate.
- A grep of the repository will find one write entry: no other module will move a document head ref.

## Risks

- **No hand fix in an outage.** The gate is the only repair path, and a broken gate blocks repairs. Mitigation: the gate is pure and testable, an operator can still submit a corrected document, and the store's own maintenance does not require a document write.
- **Attribution depends on the admitted identity.** A host that admits a caller under a borrowed principal produces a true but unhelpful trailer. Mitigation: the trailer names both principal and tool identity, and the protocol's grant binding remains the authority for who could write.
- **Strictness will block legitimate content.** A writer will produce a document the grammar refuses. Mitigation: rejections are actionable (rule plus byte range), and the format is changed by an ADR, not by a gate exception.
- **Export pressures a re-import shortcut.** Someone will ask to "apply these reviewed bytes". Mitigation: the reviewed bytes are the write's content argument to the gate; there will be no path that skips validation.
- **A dependency can move between the confirmation and the publication.** Mitigation: confirmation is the last read before the write, the affected write is refused when it happens before, and the after-publication finding covers the residual window; closing it would need the forbidden lock.
- **A related-document write can leave a mix visible.** Mitigation: per-document outcomes, the batch id, roll-forward, and a strict capability for callers that require all-or-none (`2026-11-27-per-document-versioning.md`).
- **The receipt is an existence oracle if it is careless.** Mitigation: it requires the caller's own request id and digest, answers another principal as unknown, and never reveals a document id that the caller did not write.
- **Activation is a one-time irreversible reservation.** Mitigation: it is idempotent, it refuses nothing that exists, it runs before any document may exist, and it is not reversible by design — un-reserving an id would let a document be created under a legacy citation.
- **Staging objects accumulate.** Mitigation: staging refs carry a TTL and a sweeper deletes them before releasing a reservation; packing is a maintenance duty (`2026-11-27-bare-git-knowledge-store.md`).
