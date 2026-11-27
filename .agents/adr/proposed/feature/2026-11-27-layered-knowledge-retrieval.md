# ADR: Knowledge is read in three layers, with history as a separate cold path

Status: proposed — no view level, span hydration or layer return shape exists; the model is a fixed core of the refactor
Date: 2026-11-27
Scope: The read model for Knowledge documents, the shape of each layer's return value, handle-based hydration, and the boundary of the history path
Related: `2026-11-27-derived-knowledge-indexes.md`, `2026-11-27-hybrid-knowledge-index-strategy.md`, `2026-11-27-stable-section-identity-and-drift.md`, `2026-11-27-spoke-knowledge-protocol-gaps.md`

## Problem

Knowledge entries enter the context pack whole today, in stored order, cut at a token budget (`recall_context`, `crates/mindcore-sqlite/src/knowledge.rs`). That is affordable while documents are small and the collection is short. It stops being affordable when a Mind holds hundreds of dense documents: the pack either carries documents the task does not need or overflows the budget and truncates the one it does, and a whole document spends most of its tokens on text that no decision depends on.

Shrinking documents into fixed chunks trades one failure for another: BM25 statistics on short chunks are unreliable — the current implementation computes IDF over one authorised page — and a chunk has no outline, so the reader cannot see whether the document as a whole contradicts it.

There is a third problem. History and current text have different semantics: history is immutable, appendix-shaped, and read to answer "why"; current text is what a task must obey. Mixing them in one ranked surface makes a stale revision compete with a live rule, and it puts history into an index that is meant to be disposable.

The three layers are a fixed core of this refactor, not an open question. What is open is their return shape and how a span is addressed, and that is what this record fixes.

## Proposal

A Knowledge read will have three current layers and one separate history path.

- **L0 — navigation card.** A card will carry the document identity, its domain, its declared summary, its declared triggers and symbols, its version, its section count and its size. It will carry no body prose. L0 will be the coarse pruning layer.
- **L1 — outline and constraints.** An outline will carry every section's id, title, kind and position, plus the declared invariants, constraints and interface signatures, and each section's byte size. It will carry no section prose beyond those declared lines. L1 will be the first-pass decision layer, bounded by a declared budget.
- **L2 — exact spans.** A span will be requested by a handle: collection, document, version, section id, byte range and span digest. Hydration will return exactly those bytes and will verify them against the digest. A handle will pin the version it was created from and will never silently resolve to a newer version. A pinned hydration will not be a current view: it will not answer `ViewStale`, and it will return the pinned bytes, or not found when the caller may not see that document version, even after the document has moved or been deleted. `ViewStale` will apply only to a view opened at a head and compiler version that has since moved, which a caller reopens.
- **Visibility is per item and per version.** Every layer's item will be admitted before it is returned: the document's visibility will be decided under the caller's scope for the requested version, a history item at its own revision, and a cross-domain reference for each actual owner. A hidden item will answer exactly as an absent one, so its existence does not leak.
- **L3 — history, separate.** History, log and diff will be a cold path keyed by document and version, read only on request. No historical blob will enter the lexical or vector index, and no history will be mixed with Fact history.
- **Escalation will be caller-driven.** L0 will not imply L1, and L1 will not imply L2. Each response will carry the domain, document, version, section and byte size, so a caller can budget before asking for more. A pack will report what it included and what it truncated.
- **Ordering.** L0 candidates will come from document-level ranking, not from a section list (`2026-11-27-hybrid-knowledge-index-strategy.md`), so a long document cannot crowd out a document whose summary or symbols match strongly. Section search will run only inside the documents L0 and L1 kept, with a per-document cap on returned sections.

## Alternatives considered

- **Keep whole-entry packing (the status quo).** Cheapest to ship, no read path change. Rejected because cost per accepted task grows with the collection, no citation is possible, and a document that is mostly irrelevant displaces one that matters.
- **Flat fixed-size chunks with lexical retrieval.** Simple and cheap. Rejected: chunk statistics are unreliable, a chunk cannot be checked against its document's constraints, and hydration would return fragments no section owns.
- **Model-generated summaries at write time.** Attractive for compression. Rejected: nondeterministic output, a summary that drifts with no event, a per-write model cost, and no stable cache key. Summaries will come from declared metadata.
- **Always hydrate L2.** Maximum fidelity. Rejected: it is the current cost problem with more round trips.
- **One unified surface including history.** Rejected: a historical revision would compete with a live rule in ranking, and the index would stop being disposable because pruning would change results.
- **Fixed-length byte windows over the document.** Rejected: windows cross section boundaries, so a handle cannot name an owner and drift cannot be attributed.
- **A single "view level" that returns a fully packed context.** Rejected: it hides where the tokens went and makes truncation invisible to the caller.

## Acceptance criteria

- An L0 response will contain no document body; an L1 response will contain no prose beyond declared constraint, invariant and signature lines.
- Hydrating a span will return bytes whose hash equals the handle's digest; a tampered byte range will fail rather than return other bytes.
- A handle created at version `n` will return version `n`'s bytes after version `n + 1` exists, and will not answer `ViewStale`; a view opened at a moved head will answer `ViewStale` and be reopenable.
- A handle naming another Mind's collection, a document version the caller may not see, or a deleted document's current view will be refused or answered as not found exactly like an absent item, with no content and no existence signal.
- L0 will be ranked from document-level rows; a long document whose sections dominate a section search will not prevent a document with a matching summary from entering L0; a section search will return sections only from the documents L0/L1 selected, capped per document.
- L0 and L1 for the same bytes will be byte-identical across processes and hosts.
- A packed result will report the tokens used, the layers included, and what was truncated.
- A history read will be reachable only through the history path; no search will return a historical revision.

## Risks

- **L1 may omit the deciding detail.** The reader then hydrates L2, and the saving shrinks. Mitigation: the L1 first-pass closure rate is a measured gate, and the declared constraint blocks exist to put the deciding lines in L1.
- **Handle explosion.** A caller may retain thousands of handles. Mitigation: handles are small value tuples, and a stale handle is answered as not found rather than resolved by search.
- **Deleted documents.** A handle for a deleted document will not hydrate as current text, while its history stays replayable. Mitigation: the two paths answer differently and say so; deletion does not erase history.
- **Over-fetching L2.** A caller can defeat the model by asking for every span. Mitigation: the report counts hydrated bytes per task, so over-fetching shows up in the cost metric rather than hiding.
- **Pinned hydration can outlive a grant.** A caller may hold a handle for a version it could see when the handle was made. Mitigation: a handle is not a capability — every hydration re-checks the caller's current scope for that document and version, so a revocation takes effect immediately and the answer is the same as for an absent item.
- **Token accounting.** The current estimate is a serialised-length approximation. Mitigation: the pack records a tokeniser count for the acceptance gate, and spans are counted where they are hydrated.
