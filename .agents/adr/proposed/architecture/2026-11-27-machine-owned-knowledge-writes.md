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
2. **Resolve the fence.** The write will name the document versions it read; the store will resolve each to a parent commit before any validation, so validation runs against a known base.
3. **Validate.** The document will be checked against the accepted format, required metadata, unique section ids, declared lineage, the namespace reservation and the declared cross-document invariants. Validation will be deterministic and will not call a model.
4. **Stage.** The candidate commit(s) will be written to the object database under staging refs. No head will move.
5. **Commit.** The head refs of exactly the named documents will move by compare-and-swap, all-or-none for a related-document operation.
6. **Project and answer.** The derived projection will advance and the caller will receive the committed versions and a receipt.

A rejection will be total: a document that fails one rule writes nothing, and no partially valid document will be stored in a quarantine state. The response will name every violated rule with its byte range.

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

## Acceptance criteria

- A host will provide no writable checkout of the Knowledge store; a test will assert that opening the store never creates a working tree or index file.
- A write that violates one format rule, one duplicate section id, one undeclared rename and one declared cross-document invariant will each be refused with the rule name and the offending byte range, and no head ref will move.
- A write that passes will commit exactly the named documents; a related-document write with one failed validation will leave every named document at its previous version.
- Every committed document will expose principal, session, request id, request digest and compiler version from the commit alone.
- An export of a version will be byte-identical to the blob and will name the version, commit and section digests; no operation will accept exported bytes as a write without going through the gate.
- A grep of the repository will find one write entry: no other module will move a document head ref.

## Risks

- **No hand fix in an outage.** The gate is the only repair path, and a broken gate blocks repairs. Mitigation: the gate is pure and testable, an operator can still submit a corrected document, and the store's own maintenance does not require a document write.
- **Attribution depends on the admitted identity.** A host that admits a caller under a borrowed principal produces a true but unhelpful trailer. Mitigation: the trailer names both principal and tool identity, and the protocol's grant binding remains the authority for who could write.
- **Strictness will block legitimate content.** A writer will produce a document the grammar refuses. Mitigation: rejections are actionable (rule plus byte range), and the format is changed by an ADR, not by a gate exception.
- **Export pressures a re-import shortcut.** Someone will ask to "apply these reviewed bytes". Mitigation: the reviewed bytes are the write's content argument to the gate; there will be no path that skips validation.
- **Staging objects accumulate.** Mitigation: staging refs carry a TTL and a sweeper deletes them before releasing a reservation; packing is a maintenance duty (`2026-11-27-bare-git-knowledge-store.md`).
