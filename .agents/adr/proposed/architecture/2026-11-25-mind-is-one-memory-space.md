# ADR: A Mind is one memory space

Status: proposed — a Mind's id is its space id, one persisted record binds its space, knowledge collection and carrier, and a Mind is deactivated only by freezing its space; the owner settled it on 2026-11-25, and nothing in the workspace persists a Mind yet
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

One persisted record binds what the caller supplies by hand today: the Mind's space, the knowledge collection its entries and carrier live in, and the carrier that holds its current judgment. `create` establishes them in one transaction, and a failure leaves no row.

- **The carrier.** Current judgment stays `modules.cognition` records on the carrier's `KnowledgeEntry` (`2026-11-19-cognition-carrier.md`). What changes is who names the carrier: the Mind's row does, where today every `deliberate` names it by `holder`.
- **No subject of its own.** A Mind owns no subject: `create` establishes none, nothing substitutes one, and a write names its own subject. The owner's reason: which subject a memory belongs to is the caller's organization of its memory, so no subject of a Mind is its default, and a Mind's space holds many subjects. A record's placement is its space, subject and context, and a grant's coverage is subject-scoped (`.agents/specs/architecture/mind-model.md`).
- **Deactivation.** A Mind is deactivated by freezing its space and reactivated by unfreezing it. There is no archive and no delete. The freeze is SPOKE's and is not reinvented: a space is `active` or `frozen` (`crates/mindcore-sqlite/migrations/0001_init.sql`, `memory_spaces`); a frozen space answers `SpaceFrozen` to the operations that write a subject's content outside the maintenance allowlist and keeps serving reads (`crates/mindcore-spoke/src/admission.rs`, `freeze_gated`); `memory.spaces.freeze` and `memory.spaces.unfreeze` need an `admin` grant (`crates/mindcore-spoke/src/control.rs`). A frozen Mind therefore answers by one rule: a caller with a grant in its space reads, and its writes outside the allowlist are refused with `SpaceFrozen`; a caller without one is refused as for any Mind it holds no grant in.
- **The collection binding.** The binding is stored, not assembled from host configuration. How a served call resolves a Mind to a collection is `2026-11-25-one-surface-serves-many-minds.md`.
- **Storage.** The registry arrives as an appended migration file. `0001_init.sql` is published and does not change, and a database opened by `0.1.0` reopens through the appended file (`docs/storage-compatibility.md`).

A Mind record is not a grant. It binds coordinates and confers no access: every operation still admits the caller under its own live grants in the Mind's space, as it does today.

**Records and pages this changes.** `2026-11-19-cognition-carrier.md` keeps its decision: the carrier stays the holder's `KnowledgeEntry`. It gains a `Related:` line to this record, because the Mind's row is what names that carrier from now on. `.agents/specs/architecture/mind-model.md` gains the Mind record, its id, subject and deactivation rules and the registry table. `.agents/specs/data/store-schema.md` gains that table and the appended migration that creates it.

## Alternatives considered

- **No persisted Mind (today's state).** Rejected: it is the state this decision replaces. A caller that keeps five coordinates can keep them inconsistent with each other, a new Mind needs hand-written SQL, and a SPOKE peer cannot name a Mind because no wire value stands for one.
- **A separate Mind id namespace.** Rejected: a Mind that is one space would carry two ids for one thing. Every selector would need a translation between them, a listing would carry both, and a caller able to quote either could pair a Mind id with the wrong space id. One id that already rides `space_id` needs no new wire member.
- **A default subject per Mind.** Rejected: the owner's reason is that which subject a memory belongs to is the caller's organization of its memory, so no subject of a Mind is its default, and a Mind's space holds many subjects.
- **Archive.** Rejected: the owner ruled it out. A space is `active` or `frozen` (`crates/mindcore-sqlite/migrations/0001_init.sql`, `memory_spaces`), so an archived Mind would need a state the space does not have, where the frozen state already is the deactivation.
- **Delete.** Rejected: the owner ruled it out. The repository stops serving what it withdraws by a state or a marker and keeps the row: a subject becomes `fenced` or `archived` (`crates/mindcore-sqlite/migrations/0001_init.sql`, `memory_subjects`), and a knowledge entry is superseded by a marker (`.agents/knowledge/architecture-patterns/retire-with-a-marker-not-a-delete.md`).

## Acceptance criteria

- One `create` operation, in a single transaction, records the Mind and establishes its memory space, knowledge collection binding, cognition carrier and the creating principal's default grants; a failure leaves no row.
- A Mind owns no subject: `create` establishes none and no operation substitutes one; a write names its own subject.
- Freezing a Mind's space deactivates the Mind and unfreezing it reactivates the Mind; no operation archives or deletes a Mind. A frozen Mind serves a granted caller's reads and answers that caller's writes outside the maintenance allowlist with `SpaceFrozen`.
- The Mind's id equals its space's `space_id`. No second Mind id exists in storage, in the facade or on the wire.
- `list` returns the Minds the caller may see and no other; `get` returns the same identity after the store is reopened.
- `crates/mindcore-sqlite/migrations/0001_init.sql` is unchanged, the registry is an appended migration file, and a database written by `0.1.0` reopens through it.
- `MindId` (`crates/mindcore-base/src/ids.rs`) has a use beyond its definition.
- No test or example creates a Mind by inserting into `memory_spaces` by hand: `provision_space` (`crates/mindcore-sqlite/tests/support/embedded_story.rs`) is gone.
- `.agents/specs/architecture/mind-model.md` and `.agents/specs/data/store-schema.md` state the Mind record and are updated in the same change.

## Risks

- **A Mind id is a space id, so Mind ids inherit the space's disclosure.** On the memory path an unknown `space_id` answers `NOT_FOUND` where an ungranted one answers `NOT_AUTHORIZED` (`crates/mindcore-spoke/src/admission.rs`, `space` and `refuse_coverage`), so a peer can tell which Mind ids exist. `2026-11-25-one-surface-serves-many-minds.md` closes that gap; this record does not.
- **The registry's shape is fixed once published.** It is an appended migration, and a file in the lock is immutable (`docs/storage-compatibility.md`); a wrong column is corrected by another appended file, never by an edit.
- **Deactivation is the space's frozen state.** A Mind is deactivated by freezing its space and reactivated by unfreezing it; there is no archive and no delete. A frozen Mind keeps its id and its rows, so reactivation brings back the same Mind, and freezing needs an `admin` grant (`crates/mindcore-spoke/src/control.rs`).
- **`deliberate` takes `subject_id` today for its own work.** It uses it for its snapshot recall, its grant re-check and its unsettled-intent check (`crates/mindcore-spoke/src/workflows.rs`). Whether `deliberate` keeps requiring `subject_id` is decided with the facade, not here.
