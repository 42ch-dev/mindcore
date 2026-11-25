# ADR: The `create` default grant set

Status: proposed — `create` grants the creating principal one grant row with `recall`, `capture`, `review`, `records_write`, `knowledge_read`, `knowledge_write` and space-wide `admin`; the owner settled the set on 2026-11-25, and `create` does not exist yet
Date: 2026-11-25
Class: architecture
Related: `2026-11-25-mind-is-one-memory-space.md`, `.agents/specs/architecture/mind-model.md`

## Problem

Nothing creates a Mind's first grant. A new Mind's minimum rows are a hand-written `INSERT INTO memory_spaces` and a repository-level `insert_grant` (`crates/mindcore-sqlite/tests/support/embedded_story.rs`, `provision_space`), and `docs/quickstart.md` leaves them to the host. The SPOKE grant model stays, so every operation on a Mind is admitted under the caller's live grants in that space (`.agents/specs/architecture/mind-model.md`). A Mind with no grant row admits nobody, not the principal that created it.

Without a fixed default set, each delivery form answers who may operate a new Mind in its own way, and the hand-written row stays the only way to start one. The set's shape is not free. `memory.grants.grant` requires a space-wide `admin` grant on the granter (`crates/mindcore-spoke/src/control.rs`, `grant`), so a creator without `admin` can never share the Mind through the one operation that shares. And `memory.candidates.review` admits only a single grant carrying both `review` and `records_write` (`crates/mindcore-spoke/src/content.rs`, `review`; `crates/mindcore-spoke/src/admission.rs`, `one_grant_covers`), so a set of one row per capability leaves the creator unable to review a candidate.

## Proposal

`create` grants the creating principal one grant row on the new Mind's space, with seven capabilities: `recall`, `capture`, `review`, `records_write`, `knowledge_read`, `knowledge_write` and `admin`. The SPOKE grant model stays; the set is what a new Mind starts with.

- **One row, space-wide, unrestricted, with no expiry.** The row has no `subject_id` and no `context_ids`, and has the shape `memory.grants.grant` writes.
- **Why these six beside `admin`.** They are what the three workflow tools check between them (`crates/mindcore-spoke/src/workflows.rs`): `absorb` captures, reads the candidate, recalls, reviews, updates and dismisses; `recall_context` recalls and fills its knowledge slot; `deliberate` recalls, reads and appends the carrier. `tools.mindcore.read_cognition` adds nothing beyond `knowledge_read`.
- **Why space-wide and unrestricted.** `memory.subjects.ensure` needs a grant that already covers a subject that does not exist yet (`crates/mindcore-spoke/src/control.rs`, `ensure_subject`). The `deliberate` write re-checks a `subject_all` read, which only an unrestricted grant passes (`crates/mindcore-spoke/src/workflows.rs`, `revalidate_subject`).
- **Why `admin`.** The owner's reason: without it the creator cannot share a Mind through `memory.grants.grant`. Sharing goes only through that operation, it needs a space-wide `admin` grant on the granter, and an issuance cannot exceed what the granter holds (`crates/mindcore-spoke/src/admission.rs`, `authorize_issuance`).
- **How `create` writes the row.** In its own transaction. It cannot go through `memory.grants.grant`, which requires the granter to hold `admin` already.

**Pages this changes.** No existing record states a default grant set. `.agents/specs/architecture/mind-model.md` states the rule that memory reads authorize per operation under the caller's live grants, and it gains the `create` operation with this set; the facade contract spec carries the operation's shape and is written with the facade.

## Alternatives considered

- **No default set (today's state).** Rejected: it is the state this decision replaces. The first grant of every Mind is whatever the host writes by hand, so a test fixture grants one set of capabilities (`crates/mindcore-sqlite/tests/support/embedded_story.rs`, `provision_space`), a production host grants another, and a Mind created without one admits nobody.
- **A default set without `admin`.** Rejected: without `admin` a new Mind has no admin inside the SPOKE grant model, and a creator could not share it, because `memory.grants.grant` refuses a granter that holds no space-wide `admin`. The set would also leave record promotion, which needs `promote` (`crates/mindcore-spoke/src/content.rs`, `promote_record`), out of the creator's reach.
- **A default set of one row per capability.** Rejected: `memory.candidates.review` admits only a single grant carrying both `review` and `records_write` (`crates/mindcore-spoke/src/admission.rs`, `one_grant_covers`), so one row per capability leaves the creator unable to review a candidate.

## Acceptance criteria

- After `create`, the creating principal holds exactly one grant row on the new Mind's space, and its capabilities are `recall`, `capture`, `review`, `records_write`, `knowledge_read`, `knowledge_write` and `admin`.
- The row has no `subject_id`, no `context_ids` and no expiry, and has the shape `memory.grants.grant` writes.
- With that row alone, the creator runs `recall_context`, `absorb` and `deliberate` on the new Mind, and `memory.grants.grant` accepts a grant it issues to another principal.
- A `create` that fails leaves no grant row behind.

## Risks

- **`admin` is wider than the six it sits beside.** The adapter delegates coverage to SPOKE's `grant_covers` (`crates/mindcore-spoke/src/admission.rs`), in which `admin` implies every capability within the grant's coverage. The creator therefore also holds what the six do not name, such as record promotion (`promote`), profile update (`profile_write`, `crates/mindcore-spoke/src/content.rs`) and reflections (`reflect`, `crates/mindcore-spoke/src/reflection.rs`), and the operations that require space-wide `admin` themselves: `memory.grants.revoke`, `memory.spaces.freeze` and `memory.spaces.unfreeze` (`crates/mindcore-spoke/src/control.rs`). A reader who counts seven names understates what the row grants.
- **The row never expires.** Whoever holds the creating principal keeps `admin` of the Mind for as long as the row stands, and sharing the Mind does not dilute it.
- **A SPOKE peer does not reach `create`.** The adapter's space operations are `list_spaces`, `freeze_space` and `unfreeze_space` (`crates/mindcore-spoke/src/control.rs`); no wire operation creates a Mind, so the set reaches a peer only for a Mind an embedding process created. Whether a peer may create a Mind is undecided.
