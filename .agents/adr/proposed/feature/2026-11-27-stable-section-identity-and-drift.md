# ADR: Stable section ids with declared lineage, and drift as a fingerprint

Status: proposed — no section model exists; the mechanism is agreed and unimplemented
Date: 2026-11-27
Scope: How a section is identified across versions, which edits need declared lineage, how validated lineage is persisted, and how drift between a document version and an anchored Fact is detected
Related: `2026-11-27-canonical-knowledge-format.md`, `2026-11-27-per-document-versioning.md`, `2026-11-27-derived-knowledge-indexes.md`, `2026-11-27-layered-knowledge-retrieval.md`, `2026-11-27-machine-owned-knowledge-writes.md`

## Problem

Anchoring a Fact or a Belief to a whole document makes every local edit invalidate every anchor in the file. Anchoring to a byte range breaks as soon as an earlier section grows. Anchoring to a hash of the heading breaks on a heading rename even when the section is unchanged, and a hash of the raw span changes when a child section is edited or a line is re-indented, so a parent drifts because of its child.

The reverse error is worse. A hash can say "same" or "different"; it cannot say "renamed", "moved" or "split". If the system infers intent from similarity, a copied block and a renamed section look alike, and a Fact may be re-anchored to text it never described.

The first draft then conflated cases that need different answers. It treated a heading rename as a lineage event even though the section id did not change, and it promised zero drift for whitespace changes that the format gate would reject outright — a document with that whitespace can never become a version, so it cannot produce a finding. It also left open how a validated lineage survives a crash before projection.

Today there is no section model at all: only whole-entry revision preimages. Every question above is unanswered, and the layered read has nothing to hydrate by.

## Proposal

A section's identity will be its id, not its heading text, its level or its position. Edits will be classified by whether the id is retained, replaced, removed or added, and intent will be declared only where identity changes.

| Case | Condition | Lineage needed | Classification |
|---|---|---|---|
| Retained id, heading text, level or position changed | id in old and new, fingerprint equal | No | No event; the section is the same section |
| Retained id, body changed | id in old and new, fingerprint different | No | `section_modified` |
| Id replaced, body equal | old id absent, new id present, fingerprints equal | Yes: `rename {from, to}` | `section_deleted` on the old id with successor and `auto_repairable: true` |
| Id replaced, body changed | as above, fingerprints different | Yes: `rename {from, to}` | `section_deleted` with successor, `auto_repairable: false` |
| Same id in another document | id in old and new, different document, fingerprints equal | Yes: `move {from, to}` | `section_deleted` in the source document with the target as successor, `auto_repairable: true` |
| Split | old id absent, two or more new ids present, each named as a target | Yes: `split {from, to[]}` | `section_split` with every successor |
| Declared delete | old id absent, declared `delete {from}` | Yes: `delete` | `section_deleted` with `intentional: true` |
| Undeclared removal | old id absent, no lineage | No | `section_deleted` |
| Addition | new id present, not a lineage target, not previously used | No | `section_added`; index only, no finding |
| Duplicate id | same id twice in one candidate | — | Rejected at the gate |
| Reused id | id absent from the parent but present in the document's recorded lineage history | — | Rejected at the gate |

Two rules make the table deterministic. A `move` requires an equal fingerprint; a move that also edits is two operations. An undeclared cross-document relocation will appear as an undeclared removal in the source document and an addition in the target, with no successor link. And an id may be named by at most one lineage entry, each `from` must be old-only and each target new-only, with a split bounded to a small maximum.

- **Two proofs.** Each section will carry a **span digest** over its exact bytes, used only to verify hydration, and a **drift fingerprint** over its normalised direct body, used to decide drift. The normaliser's equivalence set will contain only compiler-recognised transforms: blank-line runs outside fenced blocks. Indentation will be significant, because it can change block structure; everything inside a fence will be verbatim content.
- **Trailing whitespace is a rejection, not drift.** The format gate will refuse trailing whitespace outside fences (`2026-11-27-canonical-knowledge-format.md`), so such bytes never become a version and never produce a finding. The grammar will provide an explicit hard-break marker, so rejecting two-space breaks loses no expressive power.
- **Lineage will be persisted with the commit.** The validated manifest will be written as an immutable blob referenced by the commit, so a crash before projection is replayed from the commit and a rebuild reproduces the same classification. The projection will not be the only copy of a declared lineage.
- **Findings will separate evidence from review state.** The classification above is recomputable evidence. A human dismiss, accept or acknowledgement will be authoritative SQLite state keyed by a stable association, and will survive a rebuild (`2026-11-27-derived-knowledge-indexes.md`). Nothing will re-anchor automatically.
- **One rule set for gate and classification.** The gate will reject exactly: a duplicate id, a reused id, a lineage entry whose `from` is not old-only or whose target is not new-only, a section named twice, a split outside its bound, a move with an unequal fingerprint, a target in a document the operation did not name, a missing or malformed id attribute, and trailing whitespace outside a fence. A retained id with changed heading text or position will be accepted without lineage. `2026-11-27-machine-owned-knowledge-writes.md` uses this same list.

## Alternatives considered

- **Hash-only identity (the researched first draft).** Simplest: one hash per section decides everything. Rejected because the hash changes on heading, child and whitespace edits, so structural edits would dominate the findings, and because a hash cannot distinguish a rename from a delete plus an add.
- **Whole-document anchors only.** No section model. Rejected: one edit invalidates every anchor in the document.
- **Lineage for every heading change, including a retained id.** The previous draft's rule. Rejected: it makes an ordinary heading edit a declared event, and it contradicts identity being the id.
- **Mandatory lineage on every write.** Strongest trail. Rejected: an ordinary edit or addition would need a manifest it does not need, and the table's conservative row gives the right answer without one.
- **Infer a rename by an equal fingerprint.** Tempting for a pure move. Rejected because a copied block is indistinguishable from a rename, and a wrong successor silently re-anchors a Fact to text it never described.
- **Infer a split by containment or a similarity threshold.** Rejected: no deterministic threshold decides which of three new sections "is" the old one, and a wrong successor is worse than a deletion a reviewer sees.
- **Declare lineage inside the document.** Rejected: the document would carry its own history, and removing the attribute later would itself be a modification the drift check must explain.
- **Normalise trailing whitespace instead of rejecting it.** Rejected: inside a fence trailing whitespace is content, and outside a fence two trailing spaces are a hard break; a normaliser would have to guess which. Rejection keeps the equivalence set exact.

## Acceptance criteria

- A retained id with changed heading text, level or position, and an equal fingerprint, will produce no finding and need no lineage.
- A retained id with a changed body will produce one `section_modified` per anchored owner.
- A declared id replacement with an equal body will produce a deletion with a successor and `auto_repairable: true`; with a changed body, a successor and `auto_repairable: false`; undeclared, a deletion and an addition.
- A declared cross-document move with an equal fingerprint will produce a deletion in the source with the target as successor; undeclared, a deletion and an addition.
- A declared split into three will produce one split finding with three successors; an undeclared split will produce a deletion and additions.
- A declared delete will produce a deletion with `intentional: true`; an undeclared removal will produce a deletion without it.
- A duplicate id and a reused id will each be refused at the gate with its rule and byte range.
- A blank-line run change outside a fence will produce no finding; trailing whitespace will be refused by the gate; an indentation change that alters block structure will be `section_modified`; a whitespace change inside a fence will be `section_modified`.
- A crash after the commit and before projection will be repaired by replaying the commit's lineage, producing the same classification.
- A dismiss or accept decision will survive a projection rebuild, and no finding will be resolved or re-anchored automatically.

## Risks

- **Undeclared restructures are noisy.** Every replaced id without a manifest becomes a deletion and an addition. Mitigation: `auto_repairable` and successor fields make the mechanical cases cheap to review, and writer tooling proposes a manifest that the writer must still declare.
- **Writers can misdeclare intent.** The gate validates structure, not truth. Mitigation: a changed body makes a rename non-repairable, and a `move` with an unequal fingerprint is refused, so a misdeclaration surfaces as an ordinary deletion.
- **A move that also edits must be split into two operations.** Mitigation: the refusal names the fingerprint mismatch and the two-step path.
- **Id reuse detection reads the document's own history.** A reused id is refused only if its earlier use is in the document's lineage history. Mitigation: the check is bounded by the document's versions, not the corpus; an id that never had a recorded section is a new id.
- **Lineage manifests add bytes.** Mitigation: a manifest exists only for a write that changes identity, and most writes carry none.
- **The equivalence set may surprise.** Only blank-line runs outside fences are ignored. Mitigation: the grammar forbids the ambiguous cases, and the rule is stated where writers look.
