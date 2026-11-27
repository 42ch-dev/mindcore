# ADR: SQLite holds disposable projections with a cursor per document

Status: proposed — no projection table, cursor or catch-up path exists; the shape is agreed and unimplemented
Date: 2026-11-27
Scope: Which derived data SQLite may hold, which state must survive a rebuild, how a document with no cursor is discovered, and how a crash between a commit and its projection is repaired
Related: `2026-11-27-per-document-versioning.md`, `2026-11-27-bare-git-knowledge-store.md`, `2026-11-27-canonical-knowledge-format.md`, `2026-11-27-hybrid-knowledge-index-strategy.md`, `2026-11-27-stable-section-identity-and-drift.md`

## Problem

Ranked retrieval cannot be answered from Git: there is no lexical index, no vector index and no section table. Mindcore has none of these today — no FTS5 table and no vector table in the migrations — and recall scores a per-record vector cache in Rust over one authorised page, with an IDF computed over that page (`crates/mindcore-sqlite/src/knowledge.rs`). On a page of a few dozen rows that statistic is noise, and the served list refuses more than 512 live entries (`crates/mindcore-sqlite/src/knowledge.rs:34`).

A projection must therefore exist, and the risk is that it becomes an authority: a row no commit produced, a cursor disagreeing with its ref, a reader ranking a half-applied document and calling the result current. The first draft made every finding disposable, including the human decision that dismissed or accepted it; a rebuild would then silently reopen decisions and lose the difference between "dismissed" and "untreated".

A second gap was not a crash window but a discovery window. A document's first head can be published and the process can die before any cursor row exists. Collection search over cursors would never find it, and the layout did not say how it is found or what that costs.

The researched design kept one cursor and one sequence per Mind. That is superseded: a per-Mind cursor advances when any document changes, so an unrelated write forces every reader of the Mind to catch up, and the cursor becomes the shared sequence that `2026-11-27-per-document-versioning.md` rejects.

## Proposal

SQLite will hold derived projections and a named set of authoritative state, and the two will never be confused.

- **Derived and rebuildable.** Document and section rows (path, version, commit, section id, kind, byte range, span digest, fingerprint), document-level and section-level indexes, vector rows with an embedding identity, finding **evidence**, and the projection's compiler stamp. Every one will be a function of commits plus compiler version, and dropping them all will be safe.
- **Authoritative, never rebuilt.** Finding **review decisions** (status, reviewer, timestamp, note), write intents and receipts, and the namespace claims that `2026-11-27-dual-store-knowledge-boundary.md` and `2026-11-27-machine-owned-knowledge-writes.md` define. Anchors on Facts stay on their authoritative rows, not here. A rebuild tool will drop only the derived set by name; no rebuild will delete or reset a review decision.
- **Finding association.** Evidence and a review decision will meet on one stable key: collection, owner kind, owner identity, document identity, section id, finding kind and the version the finding is about. A document-level finding will use the document as its owner. A rebuild will recompute the same keys and join the decisions; a new document version will produce a new key and therefore a new review item, while a dismissed item will stay dismissed for its own key.
- **Per-document cursor.** Each document will have a cursor naming the version it is projected to, that version's commit, the compiler version, and index readiness. The cursor will advance by a conditional update fenced on the predecessor version, its commit and the compiler version.
- **Idempotent apply.** A step will be keyed on the exact `(version, commit)` pair. A repeat will be recognised as already applied; the same version with a different commit will be a desync.
- **Discovery of new documents.** Each collection's catch-up and each collection search will enumerate that collection's head refs. A head with no cursor will be adopted: its chain replayed from its root, which also covers a first version published without a projection row. The cost will be one ref enumeration per collection per round, proportional to the number of documents and independent of document bytes; it will be measured separately. A document published after the enumeration may be missed by that search — it will be found by the next one or by any read that names it — and no document will be silently lost.
- **Crash windows.** A crash after a head moves and before its projection will leave the cursor behind the head; the next access catches up, and that is lag, not corruption. A crash inside the projection transaction rolls it back to the same window. A head with no cursor is adopted as a root. A cursor whose commit is neither the head nor one of its ancestors, or a cursor without a head, is a desync. A related-document operation may leave one named document projected and another lagging; each is repaired on its own, and no batch record is needed to project.
- **Catch-up.** A catch-up will walk the document's own first-parent chain from its cursor to its head, compile the changed blobs at the active compiler version, and apply one transaction per step. A compiler-version change will rebuild that document at its cursor before any step is applied, so two compiler versions are never mixed. The compiler stamp is a rebuild marker, not a version or an epoch in the owner's sense: it never fences a write.
- **Read discipline.** A read that names a document will ensure that document is projected to its head. A search will refresh the collection's head inventory and ensure every lagging document before ranking; it will never rank a partially applied document as current.
- **No cross-engine transaction.** The file store is the authority and the projection disposable; a rebuild from the heads reproduces it.
- **No double write.** A step will not perform store I/O inside a database transaction, because the pool has one connection.

## Alternatives considered

- **One cursor and one sequence per Mind (the researched design).** Simpler: one row advances per commit. Rejected because any document's write moves the cursor, every reader of the Mind catches up on work it did not ask for, and the cursor stops naming a document version.
- **Trust cursor rows as the discovery path.** Cheapest search path. Rejected: a first head published before its cursor exists would be invisible to every collection search, with no error.
- **Enumerate all heads on every read.** Correct and simple. Rejected for the hot path: it pays the collection-wide ref cost per request instead of per catch-up round.
- **Write SQLite and the file store in one transaction.** Impossible across engines without two-phase commit, and it would hold the single connection across store I/O. Rejected.
- **Write SQLite first, then the store.** The document would be visible before it is committed, and a failed step would need a compensating delete; the projection would become the authority. Rejected.
- **Query the store per request, no projection.** No drift between stores, but no ranked index and object traversal per read. Rejected for the hot path.
- **A separate event log replayed into projections.** The commit chain already is the log; a second log would need its own ordering and reconciliation. Rejected.
- **Treat review decisions as derived and re-derive them from evidence.** They cannot be: a human decision is not a function of the bytes. Rejected explicitly, which is why the authoritative set is named.

## Acceptance criteria

- A commit followed by a read of that document will return the committed version, with the projection advanced by catch-up.
- Applying the same `(version, commit)` twice will write once; a different commit at the same version will be refused as a desync.
- Dropping the named derived set and replaying the heads will reproduce the same section, outline and index inputs per document for the same compiler version.
- Dismissing and accepting findings, then dropping and rebuilding the derived set, will preserve both decisions, with no reopened finding and no duplicate review item.
- A first head published with no cursor row will be found by the next collection search and projected from its root; the discovery cost is reported separately from replay cost.
- A crash injected after the head move and before the projection will require no manual step; a crash inside the projection will leave no partial document rows.
- A search over a collection with one lagging document will catch that document up before ranking, and no response will contain a section from an older version under the current version.
- A compiler-version change will rebuild a document before any new step, and no row will carry two compiler versions.
- No database transaction will be open while a file-store operation runs.

## Risks

- **Head enumeration cost grows with the collection.** Mitigation: it is a ref read per document per catch-up round, measured separately; the search path refreshes once per round, not per candidate.
- **Mixed projections after a partially published batch.** Mitigation: each document is caught up on its own; a search ensures the whole collection before ranking, so the mix is not presented as current.
- **Review state could be dropped by a careless rebuild script.** Mitigation: the derived set is named, the authoritative tables are excluded by construction, and the dismissal/acceptance survival is an acceptance test.
- **Desync after external ref surgery.** Mitigation: heads are never reset by Mindcore, a desync refuses the affected document, and repair is an explicit rebuild from the head.
- **One connection serialises steps.** Mitigation: steps are per document, store I/O stays outside the transaction, and catch-up is bounded by lag.
- **Compiler-version rebuilds are periodic work.** Mitigation: a rebuild is a planned release event, applied per document, and never mixed with older rows.
- **Vector readiness varies by build.** Mitigation: readiness flags say which channels a projection serves, and the lexical channel is independent.
