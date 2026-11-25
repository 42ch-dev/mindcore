# ADR: A Mind is one memory space

Status: proposed — a Mind's id is its space id, and one persisted record binds its space, knowledge collection, carrier and default subject; the owner settled it on 2026-11-25, and nothing in the workspace persists a Mind yet
Date: 2026-11-25
Class: architecture
Related: `2026-11-19-cognition-carrier.md`, `2026-11-25-create-default-grant-set.md`, `.agents/specs/architecture/mind-model.md`, `.agents/specs/data/store-schema.md`

## Problem

A caller needs five values to mean one Mind, and nothing binds them. Memory is addressed by `space_id` and `subject_id`, with an optional `context_id`. Knowledge is addressed by the collection a port was built on. Cognition is addressed by a carrier `entry_id` and a `holder` the caller names on every call (`crates/mindcore-runtime/src/tools.rs`, `DeliberateArguments`). Workflow routing is a `principal_id` key into `ServeFaces.workflow_routes`, host configuration that is not persisted (`crates/mindcore-runtime/src/lib.rs`). Adoption writes the one collection of `ServeFaces.knowledge`.

`MindId` is defined and used nowhere else (`crates/mindcore-base/src/ids.rs`). `memory.spaces.list` answers the spaces a principal holds a live grant in, not Minds (`crates/mindcore-spoke/src/control.rs`, `list_spaces`). `.agents/specs/architecture/mind-model.md` keeps a Mind a "stable logical actor identity" that is "not a caller, a database file, a collection or a wire type"; that rule stopped a Mind being confused with one coordinate and left nothing that holds the coordinates together.

The cost falls on a downstream's first call. A new Mind's first rows are a hand-written `INSERT INTO memory_spaces` and a repository-level `insert_grant` (`crates/mindcore-sqlite/tests/support/embedded_story.rs`, `provision_space`), and `docs/quickstart.md` leaves them to the host: "production hosts use their own authorization model".

Without a decision every delivery form invents its own Mind. An embedding process keeps its own table of coordinates beside the store, a TypeScript host keeps another, and a SPOKE peer has nothing to name. A registry built later then reconciles three vocabularies, and a Mind created through one form is not the Mind another form finds.

## Proposal

A Mind is one memory space, and its id is that space's `space_id`. `MindId` becomes the entity's id and takes the same value as the `SpaceId` of the Mind's space. There is no second id namespace and no mapping between the two.

The owner's reason: a Mind is what is a mind within one space, its application scenario, so it needs no second id.

One persisted record binds what the caller supplies by hand today: the Mind's space, the knowledge collection its entries and carrier live in, the carrier that holds its current judgment, and its default subject. `create` establishes them in one transaction, and a failure leaves no row.

- **The carrier.** Current judgment stays `modules.cognition` records on the carrier's `KnowledgeEntry` (`2026-11-19-cognition-carrier.md`). What changes is who names the carrier: the Mind's row does, where today every `deliberate` names it by `holder`.
- **The default subject.** A Mind has one default subject, which `create` establishes. `subject_id` stays on the wire: a record's placement is its space, subject and context, and a grant's coverage is subject-scoped (`.agents/specs/architecture/mind-model.md`), so the member keeps its meaning. A Mind without a subject could not be written to at all, because the operations that write a subject's content need the subject row to exist (`crates/mindcore-spoke/src/admission.rs`, `writes_subject_content`).
- **The collection binding.** The binding is stored, not assembled from host configuration. How a served call resolves a Mind to a collection is `2026-11-25-one-surface-serves-many-minds.md`.
- **Storage.** The registry arrives as an appended migration file. `0001_init.sql` is published and does not change, and a database opened by `0.1.0` reopens through the appended file (`docs/storage-compatibility.md`).

A Mind record is not a grant. It binds coordinates and confers no access: every operation still admits the caller under its own live grants in the Mind's space, as it does today.

**Records and pages this changes.** `2026-11-19-cognition-carrier.md` keeps its decision: the carrier stays the holder's `KnowledgeEntry`. It gains a `Related:` line to this record, because the Mind's row is what names that carrier from now on. `.agents/specs/architecture/mind-model.md` gains the Mind record, its id rule and the registry table. `.agents/specs/data/store-schema.md` gains that table and the appended migration that creates it.

## Alternatives considered

- **No persisted Mind (today's state).** Rejected: it is the state this decision replaces. A caller that keeps five coordinates can keep them inconsistent with each other, a new Mind needs hand-written SQL, and a SPOKE peer cannot name a Mind because no wire value stands for one.
- **A separate Mind id namespace.** Rejected: a Mind that is one space would carry two ids for one thing. Every selector would need a translation between them, a listing would carry both, and a caller able to quote either could pair a Mind id with the wrong space id. One id that already rides `space_id` needs no new wire member.
- **No default subject (today's state).** Rejected: `create` would establish a Mind whose memory cannot be written until someone provisions a subject, and each delivery form would pick the subject a new Mind starts with. One default subject fixed by `create` gives every form the same starting Mind.

## Acceptance criteria

- One `create` operation, in a single transaction, records the Mind and establishes its memory space, default subject, knowledge collection binding, cognition carrier and the creating principal's default grants; a failure leaves no row.
- The Mind's id equals its space's `space_id`. No second Mind id exists in storage, in the facade or on the wire.
- `list` returns the Minds the caller may see and no other; `get` returns the same identity after the store is reopened.
- `crates/mindcore-sqlite/migrations/0001_init.sql` is unchanged, the registry is an appended migration file, and a database written by `0.1.0` reopens through it.
- `MindId` (`crates/mindcore-base/src/ids.rs`) has a use beyond its definition.
- No test or example creates a Mind by inserting into `memory_spaces` by hand: `provision_space` (`crates/mindcore-sqlite/tests/support/embedded_story.rs`) is gone.
- `.agents/specs/architecture/mind-model.md` and `.agents/specs/data/store-schema.md` state the Mind record and are updated in the same change.

## Risks

- **A Mind id is a space id, so Mind ids inherit the space's disclosure.** On the memory path an unknown `space_id` answers `NOT_FOUND` where an ungranted one answers `NOT_AUTHORIZED` (`crates/mindcore-spoke/src/admission.rs`, `space` and `refuse_coverage`), so a peer can tell which Mind ids exist. `2026-11-25-one-surface-serves-many-minds.md` closes that gap; this record does not.
- **The registry's shape is fixed once published.** It is an appended migration, and a file in the lock is immutable (`docs/storage-compatibility.md`); a wrong column is corrected by another appended file, never by an edit.
- **Retirement is undecided.** A Mind is retired with its space. The owner has not fixed what retirement does to the space — whether `memory.spaces.freeze` (`crates/mindcore-spoke/src/control.rs`, `freeze_space`) is the mechanism — or whether a retired Mind comes back.
- **How a caller reaches the default subject is undecided.** `subject_id` stays on the wire, and the owner has not said whether the facade substitutes the Mind's default subject for a call that omits it, or requires every caller to name it.
