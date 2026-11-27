# ADR: One bare content-addressed repository per host, with one head ref per document

Status: proposed — no repository, provider or ref layout exists; the shape is agreed and unimplemented
Date: 2026-11-27
Scope: Physical and logical layout of the Knowledge file store: objects, refs, the publication decision point, staging, compare-and-swap, library choice, maintenance
Related: `2026-11-27-per-document-versioning.md`, `2026-11-27-machine-owned-knowledge-writes.md`, `2026-11-27-derived-knowledge-indexes.md`, `2026-11-27-storage-agnostic-knowledge-seams.md`

## Problem

Storing documents as SQLite payloads gives no content addressing, no per-document immutable history and no byte-exact section hydration. Storing them in an ordinary filesystem directory gives a working tree a human can edit, editor temporary files, and an ambiguous "current" state; syncing it needs watchers or full scans.

The researched layout put one head ref per Mind (`refs/heads/m/<hex>`, report §3.7). That is superseded: one ref per Mind makes every document's chain advance on an unrelated write, so a reader of document A depends on the timing of document B's commit. The first draft of this record replaced it with one head ref per document but claimed that a multi-ref ref transaction makes a related-document write all-or-none visible. A ref transaction guarantees rollback during preparation; its publish phase is not a guaranteed all-or-nothing switch, so that claim cannot be kept.

Two mechanics also need a home in the store because they are not merely semantic: a new document's first commit has no parent and must be created against an absent ref, and a deletion must leave a version behind rather than remove a ref. Without them the store cannot distinguish "create" from "continue" or "delete" from "never existed".

## Proposal

The Knowledge store will be one bare repository per host, created beside the SQLite database, never checked out, on no path a human can write.

- **Objects.** A live document will be a blob at a canonical path in that document's commit tree. A document commit will hold only that document, and will have exactly one parent, the previous version of the same document. A root commit will have no parent. A tombstone commit will carry no document blob at the path and a `Document-State: deleted` trailer, and its parent will be the previous version.
- **Refs.** There will be one head ref per document, named from the collection and document identity in a total encoding. A head ref's existence will mean the document identity is occupied, live or tombstoned. A collection's document set will be its head refs; there will be no collection snapshot ref.
- **Publication decision point.** A related-document write will first write one immutable batch record listing, for each named document, target version, target commit and expected parent, and will reference it by one batch ref created with a must-not-exist compare-and-swap. That single ref creation will be the only atomic step. Per-document head compare-and-swap will follow, in any order, and may be rolled forward by recovery. The store will not perform, and will not claim, an atomic multi-ref publish.
- **Root and create.** A first version will be published by a compare-and-swap that requires the head ref to be absent. A create against an existing head, including a tombstone, will be refused.
- **Staging.** A write attempt will write its candidate commit(s) under staging refs, one per attempted document, and will leave every head untouched until validation and the decision point have passed. Staging refs will be deleted after publication or abandonment.
- **Batch settlement.** A batch ref will be retained until every named document is at its target or the operation is explicitly abandoned, then deleted by the sweeper. Commits will carry a `Batch-ID` trailer and the write intent will be recorded authoritatively in SQLite (`2026-11-27-machine-owned-knowledge-writes.md`), so the audit survives the ref's deletion.
- **Inventory.** The authoritative discovery path for documents will be head-ref enumeration per collection. A head with no projection cursor will be adopted as a root and replayed from its chain (`2026-11-27-derived-knowledge-indexes.md`), whose cost that record states.
- **Library.** `gix` will be the recommended provider: pure-object-database read and write without a working tree, per-ref compare-and-swap, and a repository handle that can be shared across threads, at the cost of a large pure-Rust dependency tree and a recent minimum Rust version. `git2` is genuinely viable (bare init, blob and tree builders, matching-reference update) but its repository handle is `Send` and not `Sync` and its build needs a C toolchain. Neither will be relied on for an atomic multi-ref publish.
- **Maintenance.** A maintenance job will pack cold objects when no staging ref is younger than its TTL, settle and delete stale batch refs, run integrity checks, and verify the backup. The file store is the authority, so a backup that has never been restore-tested is a single point of loss (`2026-11-27-full-lifecycle-delivery-and-evaluation.md`).
- **Validation is not the store's job.** The store will accept bytes from the gate and move refs; format and lineage rules belong to `2026-11-27-canonical-knowledge-format.md` and `2026-11-27-stable-section-identity-and-drift.md`.

## Alternatives considered

- **One bare repository per Mind (the researched layout).** Viable, and it isolates corruption per Mind. Rejected: objects are no longer shared, every Mind needs its own handle and maintenance, and a per-Mind ref either carries every document again (the rejected shared sequence) or is split per document inside every repository, multiplying ref maintenance for no gain.
- **One repository per document.** Smallest blast radius. Rejected: thousands of repositories, no blob deduplication, per-document handle overhead, and no shared object cache.
- **An ordinary repository with a working tree.** Rejected by `2026-11-27-machine-owned-knowledge-writes.md`: the tree is the editable surface the design forbids.
- **A filesystem directory plus a sidecar manifest.** Cheapest to inspect. Rejected: no compare-and-swap, no immutable history, no deduplication, and sync degrades to watching or scanning directories.
- **A SQLite table of content-addressed blobs.** Attractive: one engine, one backup, one transaction model. Rejected because cross-document publication would then be a table lock or a shared sequence, there is no native pack or history DAG, and it merges the two data domains that `2026-11-27-dual-store-knowledge-boundary.md` keeps apart.
- **Rely on a multi-ref ref transaction for the batch.** Fewer objects and refs. Rejected: the publish phase is not atomic through a crash, so the store would report a success it cannot prove; the batch record gives a real decision point instead.
- **An object store as the primary backend on the host.** Rejected as primary because a host-local POSIX store is cheaper to operate; the object-store shape is kept as a provider contract (`2026-11-27-storage-agnostic-knowledge-seams.md`).
- **Shelling out to the Git CLI.** Rejected: a process and output parse per object, a runtime dependency, and a working-directory model this store does not have.

## Acceptance criteria

- Opening the store will create no working tree and no index file; a test will assert the repository directory contents after init and after a write.
- A first version will be created only against an absent head; a second create on the same id will be refused and will leave the head unchanged.
- A deletion will leave a tombstone commit whose parent is the previous version, with no document blob and a deleted-state trailer; the head will remain.
- Two writers on the same document from the same base will produce exactly one version; two writers on different documents will both publish.
- A failure injected after the batch ref exists and before the last head move will, after restart, show every named document at its old or new version; recovery will complete those whose parent is current; no batch will report success for a document that did not publish.
- A batch ref will not exist after settlement; its `Batch-ID` will remain readable from every published commit.
- A head with no cursor will be discovered by enumerating the collection's heads, adopted and replayed.
- Writing the same bytes twice will store one blob; identical documents across collections will share blobs.

## Risks

- **Ref count grows with documents and batches.** One head per document, plus short-lived batch refs. Mitigation: batch refs settle and are deleted; document heads stay loose; packing targets cold non-head refs; a lock failure is answered as retryable, never as a moved head.
- **Loose objects accumulate.** `gix` has no repository-level repack with delta compression. Mitigation: a scheduled pack of cold objects, measured, with a host `git gc` as an explicit logged fallback.
- **One repository is one blast radius.** Mitigation: integrity checks, a verified backup, a restore drill as a release gate, and no attempt to rebuild the file store from SQLite — the projection is derived, not a backup.
- **Unpublished batch targets become unreachable.** A document that never publishes leaves a commit referenced only by the batch record until abandonment. Mitigation: settle or abandon explicitly, then let packing remove it; the intent row keeps the outcome.
- **Head enumeration costs one ref read per document per catch-up round.** Mitigation: it is a directory read, measured separately from document bytes, and it is the only discovery path that cannot miss a document whose cursor was never created.
- **Ref-name encoding defects.** Mitigation: a total, tested encoding with adversarial identities and a round-trip test.
