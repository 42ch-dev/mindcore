# ADR: Stable section ids with declared lineage, and drift as a fingerprint

Status: proposed — no section model exists; the mechanism is agreed and unimplemented
Date: 2026-11-27
Scope: How a section is identified across versions, how rename, move and split are declared, and how drift between a document version and an anchored Fact is detected
Related: `2026-11-27-canonical-knowledge-format.md`, `2026-11-27-per-document-versioning.md`, `2026-11-27-layered-knowledge-retrieval.md`, `2026-11-27-dual-store-knowledge-boundary.md`

## Problem

Anchoring a Fact or a Belief to a whole document makes every local edit invalidate every anchor in the file. Anchoring to a byte range breaks as soon as an earlier section grows. Anchoring to a hash of the heading breaks on a rename even when the section's meaning is unchanged, and a hash of the raw span changes when a child section is edited or a line is re-indented, so a parent drifts because of its child.

The reverse error is worse. A hash can say "same" or "different"; it cannot say "renamed", "moved" or "split". If the system infers intent from similarity, a copied block and a renamed section look alike, and a Fact may be re-anchored to text it never described. If it never infers, an ordinary rename is reported as one deletion plus one addition, and a reviewer cannot tell a deliberate restructure from a lost section.

Today there is no section model at all: only whole-entry revision preimages (`knowledge_entry_revisions`). Every question above is unanswered, and the layered read has nothing to hydrate by.

## Proposal

Sections will have identity, and intent will be declared.

- **Stable ids.** Every section will carry a document-unique id assigned once and never reused, expressed as a heading attribute in the accepted grammar. The id will survive a rename, a level change and a move.
- **Two proofs.** Each section will carry a **span digest** over its exact bytes, used only to verify hydration, and a **drift fingerprint** over its normalised direct body, used to decide drift. Normalisation will keep fenced content verbatim and keep hard breaks; it will collapse blank-line runs and single trailing spaces outside fences only.
- **Two levels of comparison.** A document whose blob did not change will cost nothing: none of its anchors will be re-examined. A changed document will be compared by section id set and fingerprint, so a local edit touches only its own section and its ancestors are not marked.
- **Intent is declared.** A writer that renames, moves or splits a section will declare it in a lineage manifest on that write. The gate will validate the declarations: each `from` must exist only in the old version, each target only in the new, a section may be named once, a split has two or more targets, and a move preserves the fingerprint.
- **Classification.** Same id and same fingerprint: no event. Same id, different fingerprint: modified. Old-only without lineage: deleted, severity error. Declared rename or move with an equal fingerprint: deleted on the old id with the target as successor and `auto_repairable`. Declared rename whose body also changed: deleted with successor, not auto-repairable. Declared split: one split finding carrying every successor. New-only: added, index only, no finding.
- **Findings are review items.** Each finding will carry the kind, its severity, the document versions compared, the successors when declared, and whether a successor repair is mechanical. Resolution will be a review action: re-anchor, dismiss, or accept. Nothing will be rewritten automatically, and a finding will not be re-opened by a later automatic pass.

## Alternatives considered

- **Hash-only identity (the researched first draft).** Simplest to build: one hash per section decides everything. Rejected because the hash changes on heading, child and whitespace edits — the false-positive rate would be dominated by structural edits — and because a hash cannot distinguish a rename from a delete plus an add.
- **Whole-document anchors only.** No section model at all. Rejected: one edit invalidates every anchor in the document, which is the defect that motivates section identity.
- **Mandatory lineage on every write.** Strongest audit trail. Rejected: an ordinary edit or addition would require a manifest it does not need, and the conservative fallback below gives the right answer without one.
- **Infer a rename by an equal fingerprint.** Tempting: a pure move has an equal fingerprint. Rejected because a copied block is indistinguishable from a rename, and a wrong successor silently re-anchors a Fact to a section it never described.
- **Infer a split by containment or a similarity threshold.** Rejected: no deterministic threshold decides which of three new sections "is" the old one, and a wrong successor is worse than a deletion a reviewer sees.
- **Declare lineage inside the document.** Rejected: the document would carry its own history, and removing the attribute later would itself be a modification the drift check must explain.
- **Let a model classify drift during review.** Rejected as a gate: nondeterministic, unauditable, and unable to explain a classification. A review flow may use a model to help a human decide; it will not produce the finding.

## Acceptance criteria

- A whitespace-only edit outside a fenced block will produce zero findings; the same edit inside a fence will produce a modification.
- Editing one section will not mark its parent or sibling as modified.
- A renamed heading with an unchanged body and a declared lineage entry will produce one deletion finding with the successor and `auto_repairable = true`.
- The same rename without a declaration will produce a deletion and an addition, with no successor.
- A declared split into three sections will produce one split finding with three successors; an undeclared split will produce deletion plus additions.
- A duplicate section id will be refused at the gate; a duplicate found in a stored version will be reported as a desync.
- Hydrating a span will verify its bytes against the recorded span digest and fail on a mismatch.
- No finding will be resolved or a Fact re-anchored by an automatic pass.

## Risks

- **Undeclared restructures are noisy.** Every rename without a manifest becomes a deletion and an addition. Mitigation: `auto_repairable` and successor fields make the mechanical cases cheap to review, and writer tooling proposes a manifest that the writer must still declare.
- **Writers can misdeclare.** A writer may claim a rename where the content changed. Mitigation: the declaration is checked against the section sets, and a changed body makes the finding non-repairable, so a reviewer sees it.
- **Normalisation edge cases.** Fenced content, hard breaks and indentation are easy to get wrong. Mitigation: the normaliser is part of the compiler version, has golden fixtures, and a change triggers a rebuild rather than an in-place reinterpretation.
- **Findings volume.** A large restructure can open many findings at once. Mitigation: findings are grouped by document and compared versions, and the review flow can decide per group.
- **Section id reuse across documents.** Ids are unique per document, not globally, so an anchor must always name the document. Mitigation: every handle and anchor carries the document identity and version.
