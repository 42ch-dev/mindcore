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

- **Document store.** It will open a document's head, read a blob at a version, stage candidate commits, commit exactly a named set of documents all-or-none, discard a staging set, and report changes, log and diff for a document between two versions. It will report outcomes as data: committed, already committed, head moved, staging expired, ref busy, not found. It will not expose a repository, a ref handle, a lock or a file path.
- **Compiler.** It will accept document bytes and a compiler version and return sections, outline, digests and violations. It will be pure and synchronous at the boundary, with no I/O, no model and no clock.
- **Read binding.** It will ensure a document or collection is projected, open a view pinned to versions, run a ranked search that returns handles, hydrate a span by handle, list anchored entries, and read findings. Every read will carry the admitted scope; every view will report `ViewStale` when its pinned versions are no longer current rather than silently mixing.
- **No storage types.** No `sqlx`, `gix`, filesystem, thread, connection or transaction type will appear in any signature, and no trait will carry an associated transaction type. Errors will be a closed taxonomy at the seam that maps to protocol codes at the boundary.
- **Provider contract suite.** One suite will run against every provider: the Git provider, an in-memory provider, and a read-only provider over prefetched content and a prebuilt index. Git-specific behaviours will be tested with the Git provider, not in the shared suite.
- **Edge provider shape.** A head will be a pointer object, blobs will be content-addressed, a commit will be a manifest object, compare-and-swap will be a conditional put, and history will be prefetched. A provider that cannot move several documents atomically, or cannot guarantee read-your-write, will refuse the affected operation; it will not approximate it.

## Alternatives considered

- **A concrete Git-backed store type behind a thin wrapper.** Fewer abstractions and faster to ship. Rejected: no in-memory tests without a repository, no edge deployment, and the wrapper becomes the contract every caller programs against.
- **An associated transaction type, as `MemoryRepository` uses.** Strong typing over a driver. Rejected: an associated type makes the trait unusable as `dyn`, which the serve composition requires, and it ties the seam to one driver.
- **Synchronous store methods.** Simpler to call. Rejected: a read-only edge provider answers from a remote store and is asynchronous; a synchronous seam would force a blocking wrapper into an async path.
- **Expose the Git repository or the SQL connection.** Maximum power for an implementer. Rejected: it breaks the crate's own boundary rule and the external-implementability test, and it lets a caller depend on loose-object behaviour the store may change.
- **A separate edge implementation of the engine.** No seam at all. Rejected: two engines drift, and the read model would be implemented twice with two sets of bugs.
- **One monolithic trait for store, compile and read.** Fewer names. Rejected: a read-only provider would have to implement writes it never performs, and the compiler would be entangled with storage.
- **A trait per provider capability (history, search, write) with runtime discovery.** Flexible. Rejected as premature: it multiplies the `dyn` combinations before a second provider exists, and the provider suite would have to test a matrix.

## Acceptance criteria

- An external test will implement every trait outside the defining crate, bind each as `&dyn`, and pass the shared contract suite.
- A signature check will find no `sqlx`, `gix`, `std::fs`, connection, transaction or associated-type member in the seam.
- The in-memory provider will pass the shared suite without change; Git-specific cases will not be part of it.
- A provider that refuses related-document writes will fail with a named capability error, not a partial commit.
- A view pinned to versions will answer `ViewStale` after those documents move, and a caller will be able to reopen at most twice.
- A read-only provider will answer head, blob at version, changes, log, diff and every read-binding method without a compiler and without a vector channel.

## Risks

- **The seam settles on the lowest common denominator.** A Git provider has capabilities an object store lacks, and the trait may hide them. Mitigation: the trait names required behaviour, optional channels are reported as readiness flags, and Git-only behaviour stays in the provider tests.
- **Async trait overhead.** `async_trait` boxes futures on every call. Mitigation: calls are per document version, not per byte; the cost is measured in the read path.
- **Contract-suite drift.** A new provider passes the suite while behaving differently in an untested corner. Mitigation: the suite is the admission test and grows with each provider-specific bug that generalises.
- **Edge providers are weaker.** Conditional puts have no multi-ref transaction. Mitigation: all-or-none is required, and a provider without it refuses the operation; the delivery record treats the edge provider as a later stage, not a silent degradation.
- **The single-connection store constrains the read binding.** Mitigation: no seam method performs store I/O inside a database transaction, and the projection engine owns the transaction boundary.
- **Admission callbacks cannot call the seam.** Mitigation: no Knowledge call is made from an admission callback; the seam is called from the serve path after admission.
