# ADR: One bare content-addressed repository per host, with one head ref per document

Status: proposed — no repository, provider or ref layout exists; the shape is agreed and unimplemented
Date: 2026-11-27
Scope: Physical and logical layout of the Knowledge file store: objects, refs, staging, compare-and-swap, library choice, maintenance
Related: `2026-11-27-per-document-versioning.md`, `2026-11-27-machine-owned-knowledge-writes.md`, `2026-11-27-derived-knowledge-indexes.md`, `2026-11-27-storage-agnostic-knowledge-seams.md`

## Problem

Storing documents as SQLite payloads gives no content addressing, no per-document immutable history and no byte-exact section hydration. Storing them in an ordinary filesystem directory gives a working tree a human can edit, editor temporary files, and an ambiguous "current" state; syncing it needs watchers or full scans, which cannot distinguish an intentional write from a stray one.

The researched layout put one head ref per Mind (`refs/heads/m/<hex>`, report §3.7). That layout is superseded here: a single ref per Mind means every document's chain advances on an unrelated write, so a reader of document A depends on the timing of document B's commit — the Mind-wide sequencing that `2026-11-27-per-document-versioning.md` rejects. It also makes a document's history a walk over the Mind's commits filtered by path, so history cost grows with unrelated activity.

The stakes are that the file store is the authority for the Knowledge domain. Its layout decides whether an unrelated write can disturb a reader, whether history is per document, and whether a write can be made atomic across exactly the documents it touches.

## Proposal

The Knowledge store will be one bare repository per host, created beside the SQLite database, never checked out, on no path a human can write.

- **Objects.** A document will be a blob. A commit's tree will hold that document at its canonical path only, so a document's commit chain is its own history and contains nothing else. A commit will have exactly one parent, the previous version of the same document. A revert will be a new version with an older blob, never a ref reset.
- **Refs.** There will be one head ref per document. A ref name will encode the collection and the document identity into a form that is valid for any identity, rather than interpolating raw ids. A collection or Mind view will be derived by reading the document heads; no snapshot ref will exist for it.
- **Staging.** A write attempt will write its candidate commit(s) under staging refs, one per attempted document, and will leave every head untouched until validation and confirmation have passed (`2026-11-27-machine-owned-knowledge-writes.md`). The same ref transaction that moves the heads will delete those staging refs.
- **Compare-and-swap.** A single-document write will move that document's head only if it still holds the expected parent. A related-document operation will perform one ref transaction over exactly the named refs: every expected value is checked, all heads move or none do. There will be no global write lock.
- **No rewrite.** Mindcore will never reset or force a head ref. History will only be appended, so a version remains resolvable after any later write.
- **Library.** `gix` will be the recommended provider: it offers pure-object-database read and write without a working tree, per-ref compare-and-swap transactions, and a repository handle that can be shared across threads, at the cost of a large pure-Rust dependency tree and a recent minimum Rust version. `git2` is genuinely viable (bare init, blob and tree builders, matching-reference update) but its repository handle is `Send` and not `Sync` and its build needs a C toolchain; a shared `Arc<dyn SpecStore>` would then need a mutex around every read.
- **Maintenance.** A maintenance job will pack cold objects when no staging ref is younger than its TTL, run integrity checks, and verify the backup. The file store is the authority, so a backup that has never been restore-tested is a single point of loss, not a convenience.
- **Validation is not the store's job.** The store will not parse or validate a document; it will accept bytes from the gate and move refs. Format rules belong to `2026-11-27-canonical-knowledge-format.md`.

## Alternatives considered

- **One bare repository per Mind (the researched layout).** Viable, and it isolates a Mind's corruption to its own repository. Rejected because objects are no longer shared across Minds, every Mind needs its own handle and maintenance, and a per-Mind ref would either carry every document again (the rejected global sequence) or be split per document inside every repository, multiplying ref maintenance for no gain.
- **One repository per document.** Smallest possible blast radius. Rejected: thousands of repositories, no deduplication of identical blobs, handle and filesystem overhead per document, and no place to keep a shared object cache.
- **An ordinary repository with a working tree.** Rejected by `2026-11-27-machine-owned-knowledge-writes.md`: the tree is the editable surface the design forbids.
- **A filesystem directory plus a sidecar manifest.** Cheapest to inspect. Rejected: no compare-and-swap, no immutable history, no deduplication, and sync degrades to watching or scanning directories.
- **A SQLite table of content-addressed blobs.** Attractive: one engine, one backup, one transaction model. Rejected because cross-document atomicity would then be a table lock or a sequence (the rejected global fence), there is no native pack or history DAG, and it merges the two data domains that `2026-11-27-dual-store-knowledge-boundary.md` keeps apart.
- **An object store as the primary backend on the host.** Rejected as the primary because a host-local POSIX store is available and cheaper to operate; the object-store shape is kept as a provider contract for edge deployments (`2026-11-27-storage-agnostic-knowledge-seams.md`).
- **Shelling out to the Git CLI.** Rejected: a process and output parse per object, a runtime dependency on a system binary, and a working-directory model this store does not have.

## Acceptance criteria

- Opening the store will create no working tree and no index file; a test will assert the repository directory contents after init and after a write.
- Two writers on the same document from the same base will produce exactly one commit; the other will receive a stale or conflict answer. Two writers on different documents will both commit.
- A related-document write whose second ref no longer matches its expected value will leave the first head unchanged.
- A document's chain will contain only that document's versions; no commit will have two parents; a revert will appear as a new version whose blob equals an older one.
- Writing the same bytes twice will store one blob; identical documents across collections will share blobs.
- No staging ref will survive a completed or aborted write beyond its TTL.
- A provider failure injected between two ref updates will show either all named heads moved or none; the provider will not report success for a partial batch.

## Risks

- **Ref count and packed-refs contention.** One ref per document is more refs than one per Mind, and ref transactions contend on the packed-refs file when refs are packed. Mitigation: document heads stay loose, packing targets cold non-head refs, and a lock failure is answered as retryable rather than as a moved head; the write volume is measured.
- **Loose objects accumulate.** `gix` has no repository-level repack with delta compression. Mitigation: a scheduled pack of cold objects, measured against the store's growth, with a host `git gc` as an explicit logged fallback.
- **One repository is one blast radius.** Corruption or accidental deletion affects all Minds on the host. Mitigation: integrity checks, a verified backup, a restore drill as a release gate, and no attempt to rebuild the file store from SQLite — the projection is derived, not a backup.
- **Abandoned staging objects.** A crashed attempt leaves unreachable objects. Mitigation: staging refs carry a TTL, the sweeper deletes them before releasing a reservation, and packing later removes what the refs no longer reach.
- **Ref-name encoding defects.** A raw id may be an invalid ref name. Mitigation: a total encoding that is tested against adversarial identities, and a test that every accepted identity round-trips.
- **Atomic multi-ref depends on the provider.** A future provider may lack a transactional multi-ref update. Mitigation: all-or-none is a declared contract with a provider suite, and a provider that cannot honour it must refuse related-document writes rather than split them.
