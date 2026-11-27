# ADR: SQLite holds disposable projections with a cursor per document

Status: proposed — no projection table, cursor or catch-up path exists; the shape is agreed and unimplemented
Date: 2026-11-27
Scope: Which derived data SQLite may hold, how it is keyed and rebuilt, and how a crash between a commit and its projection is repaired
Related: `2026-11-27-per-document-versioning.md`, `2026-11-27-bare-git-knowledge-store.md`, `2026-11-27-canonical-knowledge-format.md`, `2026-11-27-hybrid-knowledge-index-strategy.md`

## Problem

Ranked retrieval cannot be answered from Git: there is no lexical index, no vector index and no section table. Mindcore has none of these today — no FTS5 table and no vector table in the migrations — and its recall scores a per-record vector cache in Rust over one authorised page, with an IDF computed over that page (`crates/mindcore-sqlite/src/knowledge.rs`). On a page of a few dozen rows that statistic is noise, and the served list refuses more than 512 live entries (`crates/mindcore-sqlite/src/knowledge.rs:34`).

So a projection must exist. The risk is that it becomes a second authority: a row that no commit produced, a cursor that disagrees with its ref, or a reader that ranks a half-applied document and calls the result current. A crash between a head ref moving and the projection updating is normal, not exceptional; without an explicit rule it is indistinguishable from corruption.

The researched design kept one cursor and one sequence per Mind. That is superseded: a per-Mind cursor advances when any document changes, so an unrelated write forces every reader of the Mind to catch up, and the cursor becomes the global sequence that `2026-11-27-per-document-versioning.md` rejects.

## Proposal

SQLite will hold only derived projections of the Knowledge domain, and every row will be rebuildable.

- **Contents.** Document and section rows (path, version, commit, section id, kind, byte range, span digest, fingerprint), outline rows, lexical rows, vector rows with an embedding identity, anchor references, and findings. No authority datum will live here.
- **Per-document cursor.** Each document will have a cursor naming the version it is projected to, that version's commit, the compiler version, and index-readiness flags. The cursor will advance by a conditional update fenced on its predecessor version, its commit and the compiler version.
- **Idempotent apply.** A projection step will be keyed on the exact `(version, commit)` pair. A repeated step will be recognised as already applied; the same version paired with a different commit will be refused as a desync, because it cannot have passed the write gate.
- **Crash windows.** A crash after the ref move and before the projection will leave the cursor behind its head; the next access will catch up, and that state will not be called a desync. A crash inside the projection transaction will roll it back to the same window. A cursor whose commit is neither the head nor one of its ancestors, or a cursor without a head, will be a desync. A related-document write will move its refs atomically but project each document independently, so one document of a batch may be projected and another not; that is lag, not corruption.
- **Catch-up.** A catch-up will walk the document's own first-parent chain from its cursor to its head, compile the changed blobs at the active compiler version, and apply one transaction per step. A compiler-version change will trigger a rebuild of that document at its cursor before any step is applied, so an epoch is never mixed.
- **Read discipline.** A read that names a document will ensure that document is projected to its head before answering. A search will ensure every document of the collection that lags before ranking; it will never rank a partially applied document and present the result as current.
- **No cross-engine transaction.** There will be no claim of atomicity between the file store and SQLite. The file store will be the authority and the projection disposable; a rebuild from the document heads will reproduce it.
- **No double write.** A projection step will not perform Git I/O inside a database transaction, because the pool has one connection and a held writer would stall every other Mind.

## Alternatives considered

- **One cursor and one sequence per Mind (the researched design).** Simpler: one row advances per commit. Rejected because it reintroduces a Mind-wide sequence through the back door: any document's write moves the cursor, every reader of the Mind then catches up on work it did not ask for, and the cursor no longer names a document version.
- **Write SQLite and the file store in one transaction.** Impossible across engines without two-phase commit, and the implementation would hold the single database connection across Git I/O. Rejected.
- **Write SQLite first, then Git.** The document would be visible before it is committed, and a failed Git step would need a compensating delete; the projection would become the authority. Rejected.
- **Query Git per request, no projection.** No drift between stores, but every read pays object traversal and there is no ranked index at all. Rejected for the hot path.
- **A separate event log replayed into projections.** Familiar projection pattern. Rejected: the commit chain already is the log, so a second log would need its own ordering and reconciliation with Git.
- **Rebuild the whole collection on every read.** Correct by construction, unbounded latency. Rejected.

## Acceptance criteria

- A commit followed by a read of that document will return the committed version, with the projection advanced by catch-up.
- Applying the same `(version, commit)` twice will write once; applying a different commit at the same version will be refused as a desync.
- Dropping every derived row and replaying the document heads will reproduce the same section, outline, lexical and vector inputs per document for the same compiler version.
- A crash injected after the ref move and before the projection will require no manual step; a crash inside the projection will leave no partial document rows.
- A search over a collection with one lagging document will catch that document up before ranking, and no response will contain a section from an older version under the current version.
- A compiler-version change will rebuild a document before any new step, and no row will carry two compiler versions.
- No database transaction will be open while a file-store operation runs.

## Risks

- **Projection lag is user-visible.** A read right after a commit pays the catch-up cost. Mitigation: the write path projects before answering, and the read path catches up bounded by the lag, not by the corpus.
- **Epoch rebuild cost.** A compiler change rebuilds every document. Mitigation: the rebuild is one transaction per document, measured, and a compiler change is a planned release event rather than a per-commit one.
- **Desync after external ref surgery.** Someone will delete or reset a ref with a Git tool. Mitigation: heads are never reset by Mindcore, a desync refuses reads and writes for the affected document, and repair is an explicit rebuild from the ref.
- **One connection serialises projection steps.** A long catch-up delays other Minds. Mitigation: steps are per document and batched reads are bounded; Git I/O stays outside the transaction.
- **Storage growth.** Provisional rows are rewritten per version and old projections are deleted with the document version. Mitigation: projection rows are keyed by current version only; history is read from Git, never projected.
- **The vector channel may be absent.** A build without the extension has no vector table. Mitigation: the lexical channel is independent, and readiness flags say which channels a projection supports.
