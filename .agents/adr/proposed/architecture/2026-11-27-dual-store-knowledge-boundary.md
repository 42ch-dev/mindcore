# ADR: A file-backed Knowledge domain beside the SQLite runtime domain

Status: proposed — the boundary is a direction only; no store, schema or interface implements it yet
Date: 2026-11-27
Scope: Where a knowledge-shaped datum lives, and how one request reads both domains without giving either store authority over the other's data
Related: `2026-11-27-machine-owned-knowledge-writes.md`, `2026-11-27-per-document-versioning.md`, `2026-11-27-derived-knowledge-indexes.md`, `2026-11-25-mind-is-one-memory-space.md`

## Problem

Every knowledge-shaped datum is a SQLite row today: `knowledge_entries` holds a JSON payload, `knowledge_entry_revisions` holds its preimages with a prune floor, and the served list refuses a live set above 512 entries (`crates/mindcore-sqlite/src/knowledge.rs:34`). That row shape suits a discrete claim with an identity and a revision. It does not suit a long-lived document that a reader must quote section by section: there is no section identity, no byte range, no per-document history, and a whole document competes with high-write runtime rows for the single write connection.

Splitting the data across a file store and SQLite creates a risk of its own. If a document is also projected as a Fact, two stores hold the same statement under two revisions, a reader has to decide which copy is true, and every correction has to be applied twice. If a runtime record is automatically turned into document text, the resulting prose has no author, its provenance is a pipeline artefact, and the write gate cannot say who decided it.

The stakes are concrete: without a boundary rule, the refactor produces either a second authority with two answers to "what is current", or an automatic conversion path that manufactures documents nobody approved.

## Proposal

Two domains will exist, each the sole authority for its own data.

- **The runtime domain (SQLite)** will hold Facts (`KnowledgeEntry` rows), Memory, Cognition and `MindState`-derived values. It will keep per-entry integer revisions, the preimage table, tombstone and retirement markers, and transaction semantics.
- **The Knowledge domain (content-addressed file store)** will hold Knowledge documents: long-lived, sectioned prose whose value is being quoted and applied — invariants, protocols, procedures, domain rules. Documents will be machine-owned, versioned per document, and immutable in history.

Membership will be decided by the shape of the datum, not by which subsystem produced it:

| Shape | Domain |
|---|---|
| A discrete claim with an identity, confidence and lifecycle | Fact (runtime) |
| An episode or observation with tombstone and hard-forget semantics | Memory (runtime) |
| A judgment about the Mind's own state | Cognition (runtime) |
| A document a reader must quote by section, and whose history must be auditable | Knowledge document (file store) |

Four rules will keep the stores from becoming each other's duplicate:

1. **No datum is stored in both domains.** A Knowledge document will not be mirrored as a Fact, and a Fact will not be materialised as a document. Derived indexes are projections of documents, not copies of Facts (`2026-11-27-derived-knowledge-indexes.md`).
2. **No internal conversion exists in either direction.** There will be no reflection promotion, no automatic distillation of runtime records into documents, and no import of documents into Facts. An external caller may submit a document through the public write entry; that will be an ordinary attributed write, and the caller will own its content. The research report's proposed reflection-to-document promotion channel (§2.4) is superseded by this rule: no such channel will exist, and no provenance trailer will stand in for an author.
3. **A read composes without merging.** A request that wants both domains will receive items from each, every item carrying its domain, its own version and its authority. The read will not fuse a Fact and a document into one entity, will not copy one into the other, and will perform no cross-domain conflict resolution. Ordering and token budget will be packing decisions, not truth decisions.
4. **References are one-way and versioned.** A Fact or Belief may carry a content anchor naming a document section and the document version it cited. Resolution will report staleness as a finding and will never rewrite the Fact. A document may name a Fact id in prose; nothing will resolve it automatically. Listing the entries anchored to a document or section will check the document version and each actual owner, so a hidden entry is omitted rather than counted.
5. **Id occupancy will be explicit, not inferred from a string.** A `spec.`-prefixed id will be routed by a claim inventory, not by prefix inspection alone. A claim will be one of `spec` (a file document owns the id, live or tombstoned), `legacy_occupied` (an existing SQLite row owns it), or absent. A `spec` claim will route reads to the file store; a `legacy_occupied` claim will route them to SQLite with that row's identity and history unchanged; an unclaimed prefixed id will route to SQLite when a row exists and otherwise to the file store. A new document will be refused an id with any claim, so a legacy Fact can never be shadowed, and a dynamic write will be refused a `spec`-claimed id. Activation of the claim inventory and the enablement gate belong to `2026-11-27-machine-owned-knowledge-writes.md`; the claim is a namespace fact, not content authority.

No request will write both domains atomically. A caller that needs both will issue two writes, and the second will not be implied by the first; there is no cross-engine transaction to build (`2026-11-27-derived-knowledge-indexes.md`).

On the wire, a Knowledge document will be a knowledge entry of the reserved type `spec` with a reserved id prefix. "Spec" will be only that wire label: it will carry no claim that the document is the canonical truth of anything, and no term of the protocol will depend on that reading. The prefix will be reserved by the claim inventory in rule 5, so a pre-existing dynamic row under it keeps its id, its history and its SQLite route.

## Alternatives considered

- **Keep every datum in SQLite, with documents as large payloads.** Genuinely viable and closest to today. Rejected because it preserves exactly the defects that motivate the refactor: no section identity, no per-document history, a prune floor over document versions, and whole documents competing with runtime writes for one connection. It would also make the eventual move to a content-addressed store a rewrite of the read path.
- **Put Memory and Cognition in the file store too.** Viable in principle: it would give every record content addressing and history. Rejected because tombstoned forget must delete, not append an immutable version; the write rate is that of the runtime, not of curated documents; and the store's per-document chain cannot express the transactional multi-record writes Memory already relies on.
- **One store, one schema, a `kind` column.** Simplest to operate. Rejected because it forces a single write path and a single ordering across data with different lifecycles, which is precisely the dual-authority problem: a document revision and a Fact revision would have to be comparable, and one lock or sequence would govern both.
- **An automatic Memory → Knowledge promotion flow.** Rejected by the owner: it produces prose no one authored, and any provenance trailer it writes describes a pipeline, not a decision. An external caller may write a document, which keeps the decision where it belongs.
- **Mirror documents as Facts for search convenience.** Rejected: two authorities, two revisions, and a staleness question that has no answer the protocol can express. Search will read projections of documents instead.

## Acceptance criteria

- A schema and repository check will show no document blob in a SQLite payload column and no Fact, Memory, Cognition or `MindState` row in the file store.
- A read that requests both domains will return each item tagged with its domain and its own version; a test will assert that no item appears in both domains and that no item carries a merged identity.
- A seeded `spec.` Fact will be read, written and replayed from SQLite before and after activation; a document create under its id will be refused; a document with the same id cannot be created and cannot shadow it.
- Listing the entries anchored to a section will omit an entry the caller may not see, without changing the count or revealing its existence.
- A static check will show no call path from a Memory or Cognition write to a document write, and no path from a document write to a dynamic knowledge write other than a refusal of the reserved namespace.
- A Fact anchored to a section whose drift fingerprint changed will gain a drift finding while its payload stays byte-identical.
- A document whose content is a list of discrete claims will still be accepted if it satisfies the format, and no rule will convert those claims into Facts: the boundary will be enforced by reviews and by the format's required section structure, not by content inspection.
- The boundary table above will appear in the specs entry point for Knowledge, so a reviewer of a new datum type has one question to answer.

## Risks

- **Two read paths cost more than one.** A composition that wants documents and Facts will make two reads. Mitigation: the layered read returns a token-priced pack from both, and the cost gate of the delivery record measures the combination, not each read alone.
- **Operators will expect a join.** Someone will ask for "all documents mentioning this Fact". Mitigation: no joined view will be exposed; the answer is an explicit read of each domain, and the boundary rule says a cross-domain reference is a pointer, not a foreign key.
- **The boundary erodes under convenience.** A future feature will be tempted to store a document summary as a Fact. Mitigation: the derived indexes own summaries; a Fact that merely repeats a document is refused at review, and the format requires each document to declare the constraint blocks that make it more than a summary.
- **Two backup and restore regimes.** SQLite and the file store will have separate recovery points, and no joint restore is promised. Mitigation: each domain documents its own recovery, and the delivery record makes file-store backup a first-class stage rather than an afterthought.
- **Anchors create a third kind of staleness.** A Fact can now be stale with respect to a document version. Mitigation: staleness is a finding with a severity and a review flow, never an automatic rewrite.
- **Routing has two occupancy sources.** The file store owns document ids by head ref; SQLite holds the claim inventory for legacy rows. Mitigation: every routing decision reads the claim, activation writes it in one transaction before any document may exist, and no row is ever renamed or migrated, so the two sources cannot disagree about an existing id.
