# ADR: Versions are per document, never per Mind

Status: proposed — no document store or fence exists; the rule is agreed and unimplemented
Date: 2026-11-27
Scope: Revision semantics for Knowledge documents, their root, deletion and recreation, conflict detection, and what a related-document operation does and does not guarantee
Related: `2026-11-27-bare-git-knowledge-store.md`, `2026-11-27-derived-knowledge-indexes.md`, `2026-11-27-spoke-knowledge-protocol-gaps.md`, `2026-11-27-machine-owned-knowledge-writes.md`

## Problem

A Mind will hold many documents with independent lifetimes. Any shared counter or lock makes unrelated work interfere: a write to document B advances the shared counter, so a caller holding document A's version is told its base is stale although A never changed. The caller re-reads and resends for work it did not do, and every writer in the Mind serialises on one counter.

A Mind-wide counter also puts an internal ordering into a wire field whose contract is per entry: `KnowledgeEntry.revision` is an integer fence and `expected_base_revision` names the base of that one entry (`schemas/data/knowledge-entry.schema.json`). If every document's revision changed when any document changed, the field would stop describing the entry it belongs to.

Two lifecycle questions decide whether the fence is even well defined, and the first draft left both open. A new document has no parent, so "the parent is version n − 1" says nothing about its first version. A `null` fence was described both as "the document is absent" and as "the head, born or not, becomes the parent", which lets a create silently continue an existing chain. Deletion and recreation of the same id were not defined at all.

The first draft also promised that a related-document operation would be all-or-none visible through one multi-ref Git transaction. A ref transaction guarantees rollback during preparation; its publish phase is not a guaranteed all-or-nothing switch, so a crash can leave some heads moved and others not. That promise cannot be kept by an acceptance test, and a false promise is worse than a narrower one.

## Proposal

Each Knowledge document will have its own linear version chain. Version `n` will name the commit at `n` in that document's chain.

- **Root.** A document's first version will be a root commit with no parent, at version 1. A create will be a compare-and-swap against an **absent** head ref. `expected_base_revision = null` will mean exactly "no document with this id exists"; if a head exists, including a tombstoned head, the create will be refused as an existing entry. A `null` fence will never continue an existing chain.
- **Fence.** `expected_base_revision = n` will name that document's commit at `n`. A stale fence will be answered for that document only, with that document's current version. No other document's version will enter the answer.
- **No Mind-wide sequence, lock or fence.** A write to document B will not change, invalidate or serialise a write to document A.
- **Deletion.** Deleting a document will append a **tombstone version**: a commit whose parent is the previous version and whose document state trailer says `deleted`. The head will remain at the tombstone. A current read will answer not found; history will replay every version including the tombstone; the id will remain occupied.
- **Recreation is forbidden.** Because the tombstone keeps the head ref, a create under a deleted id will be refused exactly as a create under a live id. Reusing an id would make a permanent citation ambiguous; a future reuse policy would need its own record, and this record does not provide one.
- **A related-document operation is a sequenced publication, not an atomic one.** It will name its documents and one expected version for each. It will publish each named document as its own version with its own parent, and it will not claim that a reader sees all or none.

The related-document operation will have one **decision point**: an immutable batch record listing, for every named document, the target version, the target commit and the expected parent, referenced by one per-operation batch ref created with a must-not-exist compare-and-swap. That single ref creation will be the only atomic step. After it exists, the operation will move each named head by its own compare-and-swap, in any order.

The guarantees will be exactly these:

1. No document outside the named set will move.
2. Every named document will end at its old version, its new version, or a later version that superseded the new one, never at a partial state.
3. A stale fence will refuse at fence resolution, before validation. At the post-staging dependency check, immediately before the decision point, a named dependency that has **already** moved will refuse the whole operation with nothing published.
4. After the decision point, the store will decide each named document's outcome by **reachability**: a target commit that is the document's current head **or an ancestor of it** is published, even when a later write has superseded it. A target that is not reachable will be rolled forward when its expected parent is still current; otherwise it will be reported as not published. A decided batch will not be abandoned by credential expiry: its credentials will be retained until settlement, so a target whose parent is still current will always be published by recovery. The caller will receive the batch id and a per-document outcome.
5. The settled outcome of every named document will be persisted in the authoritative write intent **before** the batch record and its ref are removed, so a later head advance cannot turn a published target into a failure or an unpublished one into a success. The batch will have exactly two terminal states: `committed` only when every named target is published, `incomplete` otherwise, with the per-document outcomes.
6. A document that was not published will be retryable on its own; the caller will not be told to retry the whole operation.

A read during or after a failed operation may therefore see one named document at its new version and another at its old one. That is stated, not hidden. A caller that needs all-or-none visibility will require the provider capability of `2026-11-27-storage-agnostic-knowledge-seams.md`; such a publication will be refused before any write when the provider cannot supply it.

- **A named dependency is a declaration checked once after staging, not a lock.** The store will re-read each declared dependency's head after staging and immediately before the decision point — before a single write's compare-and-swap, or before a related operation's batch record — and will refuse the write when a dependency has already moved. A dependency that moves after that check is **not** prevented: no lock exists, and preventing it would need the Mind-wide fence this record rejects. It will be detected after publication as a finding naming the document, the dependency versions the writer declared, and the dependency's current version; the declared versions will be recorded on the commit so the finding is recomputable. Rules that cannot be expressed as named versions will be findings only. This is the whole promise, and the write path states the same one (`2026-11-27-machine-owned-knowledge-writes.md`).
- **Reads will report a version vector.** A multi-document read will return each document's version and will not claim cross-document snapshot isolation. A collection's change surface will be that vector, document by document; there will be no collection-level counter or "maximum version" hint, because a low-version document changing would not move it.

## Alternatives considered

- **One sequence per Mind (the research report's option a).** Viable, and it was the researched recommendation: one counter advances on every commit, every document reports it as its revision, and the counter names the parent. Rejected because an unrelated document write makes every other document's fence stale; because it serialises all writers of a Mind on one counter; because it puts an internal ordering into a per-entry wire field; and because it gives every document in a batch the same version, erasing per-document history.
- **One multi-ref Git transaction as the atomic publication (the previous draft).** Attractive because it needs no second mechanism. Rejected: the publish phase of a ref transaction is not guaranteed atomic across refs, so a crash can leave some heads moved and others not; the promise could only be kept by a provider that offers a stronger primitive, which Git does not.
- **A single shared publication log or ref per collection, so one compare-and-swap publishes the batch.** This would give real atomic visibility. Rejected: every writer of the collection would contend on that one ref, which is the shared write lock the owner forbids, and per-document versions would become positions in that log.
- **Publication objects as the sole authority, with SQLite naming each document's current version.** This would make one publication ref per operation the visibility point and derive heads from it. Rejected: the projection would become the authority for which version is current, and a rebuild would have to enumerate every publication ever made.
- **A per-document version whose parent the server chooses from the head it happens to read (the report's option b as written).** Rejected in that form: the wire value would be a fence that does not name the base, so two writers could succeed from different unseen bases. Adopted only in the corrected form above, where the version names the parent and the server never re-bases.
- **A content-digest fence.** Viable: the parent is named by immutable bytes and any provider can verify it. Rejected because the wire field is a bounded unsigned integer; a digest fence would need a new member and a cohort, and it would lose the ordering history needs, so a counter would be kept beside it anyway.
- **A snapshot token plus a per-document fence.** Rejected: two fences, two failure answers, and the token reintroduces a Mind-wide value readers must obtain.
- **An optimistic multi-version merge of documents.** Rejected: prose sections have no merge semantics the gate can verify, and a merge would invent text no one authored.
- **Allowing recreation of a deleted id.** Convenient for a renamed or replaced document. Rejected: a permanent citation of "document X at version n" would become ambiguous, and an old anchor could resolve into a new document. A rename is a new identity with declared lineage (`2026-11-27-stable-section-identity-and-drift.md`), not a reused one.

## Acceptance criteria

- A create on an absent id will produce version 1 with no parent; a create on an existing id, live or tombstoned, will be refused and will leave the head unchanged.
- Two concurrent creates of one id will produce exactly one root; the other will be refused as an existing entry.
- Deleting a document will append a tombstone version; a current read will answer not found; history will replay every version including the tombstone; a create under that id will be refused.
- A write to document B will not change document A's version.
- A write to A with a stale version will be answered with A's current version; no other document's version will appear.
- A related-document operation that fails after the decision point and before its last head move will, after restart, leave every named document at its old or new version; roll-forward will complete the documents whose parent is still current, and the response will list each document's outcome, naming the unpublished ones for retry.
- A related-document operation with a stale fence or a named dependency that had already moved at the post-staging check will publish nothing.
- A write will be refused only for its own fence or a dependency already moved at that check; a dependency that moves after it will not refuse the write, will produce a finding, and will leave the published version standing.
- A decided batch whose credential outlives its TTL will still publish a target whose parent is unchanged, then settle once; a target whose parent moved will settle the batch `incomplete` without asking for the whole operation to be retried.
- A target commit published and then superseded by a later version will still be reported as `published` by the settlement and the receipt; the batch will be `committed` only if every named target is published, and otherwise `incomplete` with that target still listed published and not retryable.
- A batch that settles after a later head advance will answer the same per-document outcome after a restart, because the outcome is persisted before the batch record is removed.
- A strict related-document operation on a provider without atomic multi-version publication will be refused before any write.
- A projection rebuild will reproduce the same versions from the commit chains alone, with no per-Mind counter.
- No batch ref will survive settlement beyond its TTL.

## Risks

- **Mixed visibility across documents.** A caller may observe a partially published related operation. Mitigation: the outcome is per document, the batch id names the operation, roll-forward completes what it can, and a strict capability exists for callers that must not observe a mix.
- **A publish failure leaves an unpublished target commit reachable only from the batch record.** Mitigation: the batch ref is retained until every named target is published (its commit is the head or an ancestor) or explicitly failed, its settled outcome is persisted, and only then is the ref removed; commits carry a batch trailer so the audit survives the ref's deletion.
- **A dependency can move between the decision-point check and the compare-and-swap.** No lock is allowed, so that window cannot be closed. Mitigation: the check is the last read before the write, the declared versions are recorded on the commit, and a move in the window is detected afterwards as a finding rather than being silently accepted or silently refused.
- **Callers may read a related operation as a transaction.** Mitigation: the operation's name and response describe per-document outcomes, and the record states the limit in the proposal rather than only in a risk.
- **Tombstones accumulate refs and claims.** Mitigation: a tombstone is one commit and one head ref per deleted document; there is no compaction, and the cost is bounded by deletions, not by writes.
- **The version vector is a larger change surface than a scalar.** Mitigation: it is truthful; each entry is small, and a caller that cares about one document reads that one.
