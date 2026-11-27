# ADR: Versions are per document, never per Mind

Status: proposed — no document store or fence exists; the rule is agreed and unimplemented
Date: 2026-11-27
Scope: Revision semantics for Knowledge documents, conflict detection, and how a multi-document operation coordinates only the documents it names
Related: `2026-11-27-bare-git-knowledge-store.md`, `2026-11-27-derived-knowledge-indexes.md`, `2026-11-27-spoke-knowledge-protocol-gaps.md`, `2026-11-27-dual-store-knowledge-boundary.md`

## Problem

A Mind will hold many documents with independent lifetimes. Any shared counter or lock makes unrelated work interfere: a write to document B advances the shared counter, so a caller holding document A's version is told its base is stale although A never changed. The caller re-reads and resends, which is a wasted round trip attributable to work it did not do. Under load, every writer of a Mind also serialises on that one counter, so a hot document throttles a cold one.

There is a second cost: a Mind-wide counter leaks an internal ordering onto the wire. SPOKE's contract is per entity — `KnowledgeEntry.revision` is an integer fence, and `expected_base_revision` names the base of that one entry (`schemas/data/knowledge-entry.schema.json`). A Mind-wide value in that field would mean every document's revision changes when any document changes, so the field would stop describing the entry it belongs to.

The research report proposed exactly that Mind-wide sequence (§2.6, option a) and justified it on the ground that a per-document fence could not name its parent. That justification does not hold once versions are per document: the version then names the document's own immutable revision, and that revision has exactly one parent.

If no decision is taken, the store inherits a global sequence, and every unrelated write becomes someone else's conflict.

## Proposal

Each Knowledge document will have its own linear version chain. Version `n` of a document will name the commit at `n` in that document's chain, and that commit will have exactly one parent, the commit at `n − 1`.

- **The wire revision will be the document's own version.** For a document-backed entry, `revision` will be the version of that document. `expected_base_revision = n` will mean "the parent is this document's commit at `n`". `expected_base_revision = null` will mean the document must be absent, so the operation is a create and its parent is the current head if one exists.
- **A fence will bind one document.** A stale fence will be answered only for the document it names, with that document's current version. No other document's version will enter the answer.
- **No Mind-wide sequence, lock or conflict fence will exist.** A write to document B will not change, invalidate or serialise a write to document A. There will be no per-Mind counter, no per-Mind lease and no epoch that a document write must take.
- **A related-document operation will coordinate exactly the documents it names.** It will carry one expected version per named document; the store will check all of them and move all named heads in one atomic step. If any fence is stale, the whole operation will be refused and no head will move. On success, each named document will receive its own commit, carrying the same request id, and its own version increment.
- **A read will report versions, not a snapshot token.** A multi-document read will return each document's version. It will not claim cross-document snapshot isolation; a caller that needs a stable pair will name both versions in a related-document read or re-check them.
- **History will be per document.** A version will remain resolvable after any later write, so a citation of "document X at version n" will be permanent (`2026-11-27-layered-knowledge-retrieval.md`).

A batch of documents will therefore share an operation identity and an atomic store step, and will share no version number. Two documents in one operation will each advance by one, from their own bases.

## Alternatives considered

- **One sequence per Mind (the research report's option a).** Viable, and it was the researched recommendation: one counter advances on every commit, every document reports it as its revision, and the counter names the parent. Rejected because an unrelated document write makes every other document's fence stale, so a caller must re-read and resend for work it did not touch; because it serialises all writers of a Mind on one counter; because it puts an internal ordering into a wire field whose contract is per entry; and because it gives every document in a batch the same version, erasing the per-document history granularity the cold path needs.
- **A per-document version in the field, with the parent chosen by the server from the head it happens to read (the report's option b as it was written).** This is close to the proposal but not the same: there the wire value was a fence and the parent was the server's choice, so the value did not name the base. Rejected in that form because two writers could succeed from different unseen bases. Adopted in the corrected form above, where the version names the parent and the server never re-bases.
- **A content-digest fence.** Viable and attractive: the parent is named by immutable bytes, needs no counter, and is verifiable by any provider. Rejected because SPOKE's field is a bounded unsigned integer; a digest fence would need a new wire member and a cohort, and it would lose the ordering that history and "how many revisions" need, so a counter would have to be kept beside it anyway.
- **A snapshot token plus a per-document fence.** Rejected: two fences, two failure answers, and the token reintroduces a Mind-wide value that readers must obtain, which is the rejected sequence in another shape.
- **An optimistic multi-version merge of documents.** Rejected: prose sections have no merge semantics the gate can verify, and a merge would invent text no one authored.
- **Serialising all writes per Mind for safety.** Rejected: it converts a conflict problem into a throughput problem and still leaves cross-document invariants unverified after the fact.

## Acceptance criteria

- Two concurrent writes to different documents from their current versions will both commit, and each document's version will advance by exactly one.
- A write to document B will not change document A's version; a test will read A's version before and after B's commit and compare.
- A write to A with a version one behind will be refused with A's current version; no other document's version will appear in the answer.
- A related-document write naming A and B with one stale fence will leave both heads unchanged; with both fences current it will commit both, and each document's history will contain only its own commit.
- `expected_base_revision` for a document-backed entry will resolve to that document's commit at that version, and a create under `null` will have the current head as its parent when one exists.
- A projection rebuild will reproduce the same version numbers from the commit chains alone, without any per-Mind counter.
- A grep of the store and projection will find no per-Mind sequence, lease or epoch governing document writes.

## Risks

- **Mixed-snapshot reads.** Two documents read at different times will carry different versions, and a caller may assume they were consistent. Mitigation: every read returns versions, the pack carries them, and a related-document read is the only thing that returns a coordinated pair.
- **Cross-document invariants are not prevented, only detected.** No global fence can stop a later single-document write from violating a declared dependency. Mitigation: dependencies are declared at write time, checked against the declared set, and a later violation produces a finding for review; prevention would require the global lock this record rejects.
- **Ref count grows with document count.** One head ref per document is more refs than one per Mind. Mitigation: ref names are encoded, refs are packed by maintenance, and the number is bounded by the document count, which the format's size bound also bounds (`2026-11-27-bare-git-knowledge-store.md`).
- **Atomic related-document steps depend on the provider.** A provider that cannot move several heads atomically could fake it and split a batch. Mitigation: the storage seam declares all-or-none a contract, the provider suite tests it, and a provider that cannot honour it must refuse the operation rather than degrade (`2026-11-27-storage-agnostic-knowledge-seams.md`).
- **A caller may prefer one number for "the Mind".** Some clients will want a single cursor for polling. Mitigation: the read returns a version vector (document → version) and the highest observed version per collection as a change hint, explicitly not a fence.
