# ADR: The Knowledge seams are object-safe traits over DTOs with no storage types

Status: proposed — no trait exists; the contract shape is agreed and unimplemented
Date: 2026-11-27
Scope: The storage, compiler and read contracts; what may appear in their signatures; and how a provider without a POSIX filesystem or a Git binary serves them
Related: `2026-11-27-bare-git-knowledge-store.md`, `2026-11-27-derived-knowledge-indexes.md`, `2026-11-27-per-document-versioning.md`, `2026-11-27-layered-knowledge-retrieval.md`

## Problem

The capability traits in `mindcore-spi` are object-safe, `Send + Sync`, and tested for external implementability (`crates/mindcore-spi/tests/mock_adapter.rs:199`). A Knowledge seam that leaks `gix::Repository`, an `sqlx` transaction, a file handle or an associated transaction type would break that test, make `dyn` composition impossible, and tie every consumer to one driver.

The deployment goal makes this urgent rather than stylistic: an edge runtime has no POSIX filesystem and no Git binary, only an object store with conditional writes and a read-only index. If the contract assumes refs and reflogs, that deployment cannot be served without a fork of the engine. If the contract assumes a working tree, it is the design the refactor forbids.

The current storage surface does not help: the store is one SQLite file with a single connection and synchronous transaction borrows, and the serve path takes concrete ports. Nothing in the Knowledge path can be implemented against an object store without a contract that names only data and versions.

## Proposal

Three traits will define the Knowledge seams, all object-safe, asynchronous, `Send + Sync`, and expressed in data transfer objects only.

- **Document store.** It will open a document's head, read a blob at a version, stage candidate commits, publish one document by compare-and-swap, publish a related set through one batch record and per-document compare-and-swap, discard a staging set, and report changes, log and diff for a document between two versions. It will report outcomes as data: committed, already committed, head moved, staging expired, ref busy, not found, unpublished. It will report a per-document outcome for a related set, never a single success for the set. A provider that can publish many versions atomically will advertise `atomic_multi_publish`; a caller may require it and will be refused before any write when the provider lacks it. The seam will not promise an atomic multi-version publish from a provider that cannot supply one. It will not expose a repository, a ref handle, a lock or a file path.
- **Compiler.** It will accept document bytes and a compiler version and return sections, outline, digests and violations. It will be pure and synchronous at the boundary, with no I/O, no model and no clock.
- **Read binding.** It will ensure a document or collection is projected, open a **current** view pinned to a head and compiler version, run a ranked search that returns handles from the visible candidate set, hydrate a span by pinned handle, list anchored entries, and read findings. Every read will carry the admitted scope. A current view will report `ViewStale` when its head or compiler version has moved, rather than silently mixing. A pinned hydration will not be a view: it will return the bytes of the version it names, or not found when the caller may not see them, and it will never answer `ViewStale`.
- **No storage types.** No `sqlx`, `gix`, filesystem, thread, connection or transaction type will appear in any signature, and no trait will carry an associated transaction type. Errors will be a closed taxonomy at the seam that maps to protocol codes at the boundary.
- **Provider contract suite.** One suite will run against every provider: the Git provider, an in-memory provider, and a read-only provider over prefetched content and a prebuilt index. Git-specific behaviours will be tested with the Git provider, not in the shared suite.
- **Edge provider shape.** A head will be a pointer object, blobs will be content-addressed, a commit will be a manifest object, compare-and-swap will be a conditional put, and history will be prefetched. A provider that cannot publish several versions atomically will advertise that absence; a related set will then publish document by document with per-document outcomes, and a caller that requires atomicity will be refused before any write. A provider that cannot guarantee read-your-write after a successful publish will refuse the publish rather than approximate it.

## Alternatives considered

- **A concrete Git-backed store type behind a thin wrapper.** Fewer abstractions and faster to ship. Rejected: no in-memory tests without a repository, no edge deployment, and the wrapper becomes the contract every caller programs against.
- **An associated transaction type, as `MemoryRepository` uses.** Strong typing over a driver. Rejected: an associated type makes the trait unusable as `dyn`, which the serve composition requires, and it ties the seam to one driver.
- **Synchronous store methods.** Simpler to call. Rejected: a read-only edge provider answers from a remote store and is asynchronous; a synchronous seam would force a blocking wrapper into an async path.
- **Expose the Git repository or the SQL connection.** Maximum power for an implementer. Rejected: it breaks the crate's own boundary rule and the external-implementability test, and it lets a caller depend on loose-object behaviour the store may change.
- **A separate edge implementation of the engine.** No seam at all. Rejected: two engines drift, and the read model would be implemented twice with two sets of bugs.
- **One monolithic trait for store, compile and read.** Fewer names. Rejected: a read-only provider would have to implement writes it never performs, and the compiler would be entangled with storage.
- **A trait method that commits a set and returns one outcome.** Convenient for callers. Rejected: it would force every provider either to fake atomicity it cannot guarantee or to refuse a capability the caller cannot distinguish from a bug; the per-document outcome is the honest shape.
- **A trait per provider capability (history, search, write) with runtime discovery.** Flexible. Rejected as premature: it multiplies the `dyn` combinations before a second provider exists, and the provider suite would have to test a matrix.

## Acceptance criteria

- An external test will implement every trait outside the defining crate, bind each as `&dyn`, and pass the shared contract suite.
- A signature check will find no `sqlx`, `gix`, `std::fs`, connection, transaction or associated-type member in the seam.
- The in-memory provider will pass the shared suite without change; Git-specific cases will not be part of it.
- The suite will include a publish-phase failure: after the batch record exists and one head has moved, the provider must report the remaining documents as unpublished, must not report the set as committed, and must leave every document at its old or new version.
- A strict related-document write on a provider that does not advertise atomic multi-version publication will be refused before any write, with a named capability error.
- A pinned hydration will return its named version's bytes after the document moves and will never answer `ViewStale`; a current view at a moved head will answer `ViewStale`.
- A read-only provider will answer head, blob at version, changes, log, diff and every read-binding method without a compiler and without a vector channel.

## Risks

- **The seam settles on the lowest common denominator.** A Git provider has capabilities an object store lacks, and the trait may hide them. Mitigation: the trait names required behaviour, optional channels are reported as readiness flags, and Git-only behaviour stays in the provider tests.
- **Async trait overhead.** `async_trait` boxes futures on every call. Mitigation: calls are per document version, not per byte; the cost is measured in the read path.
- **Contract-suite drift.** A new provider passes the suite while behaving differently in an untested corner. Mitigation: the suite is the admission test and grows with each provider-specific bug that generalises.
- **Edge providers are weaker.** Conditional puts have no multi-ref transaction and no group commit. Mitigation: atomicity is advertised as a capability and required only by callers that ask; sequenced publication returns per-document outcomes; the edge provider is a named delivery stage, not a silent degradation.
- **The single-connection store constrains the read binding.** Mitigation: no seam method performs store I/O inside a database transaction, and the projection engine owns the transaction boundary.
- **Admission callbacks cannot call the seam.** Mitigation: no Knowledge call is made from an admission callback; the seam is called from the serve path after admission.
