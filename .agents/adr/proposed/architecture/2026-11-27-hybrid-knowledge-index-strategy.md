# ADR: Lexical and vector retrieval over one visible candidate set, fused by rank

Status: proposed — no lexical or vector index exists; the strategy is agreed and unmeasured
Date: 2026-11-27
Scope: The two retrieval channels, the visible candidate set they share, the document-level and section-level indexes, how results are fused and ordered, and when a heavier vector index may be adopted
Related: `2026-11-27-derived-knowledge-indexes.md`, `2026-11-27-layered-knowledge-retrieval.md`, `2026-11-27-full-lifecycle-delivery-and-evaluation.md`

## Problem

A lexical index alone misses paraphrase: a question phrased differently from the document returns nothing, and the reader concludes the rule does not exist. A vector index alone misses exact tokens: identifiers, version strings, error codes and rare terms are exactly the queries where a nearest-neighbour search is least reliable, and the answer must be the section that literally names the symbol.

Mixing both carelessly creates a worse failure. If one index ranks globally and the Mind, domain or subtree filter is applied afterwards, the top results are consumed by rows the caller may not see, and the filtered answer is short or empty even though the corpus is full. Mind and domain filtering is not per-item disclosure: a row can belong to the right Mind and still be one the caller may not read, and filtering for that after `k` hides rows *and* leaks their effect through the count. If term statistics are computed across Minds, one Mind's corpus changes another Mind's ranking, which breaks isolation in a way no access check would catch. And if sections are ranked before documents, one long document can fill every candidate slot, so a document whose summary or symbols match strongly never reaches the navigation layer.

Today none of this exists — there is no index to isolate — so the choice is free, and choosing a heavy index before measuring would spend the cost the refactor is meant to avoid.

## Proposal

Retrieval will use two channels over one visible candidate set, with a document-level index for L0 and a section-level index for L2, every filter applied before ranking, and every statistic computed over that visible set.

- **One visible candidate set.** Both channels will draw from one candidate set: the admitted scope's collection, domain and subtree predicate plus **the same per-item disclosure predicate the read paths apply** — the protocol's entry-visibility rule over the candidate's owning document and the entry that carries it, including the source, entry id, entry type, status and label — not a separate type/id/source filter and not a namespace check. A candidate a get or hydration refuses will be a candidate the search cannot rank or count, because there is one predicate function, not two. The predicate will be evaluated **before scoring**, not only before the limit: the visible document and section rows will be determined first, and both channels will score only those rows. A hidden row will consume no candidate, will change no count, and will answer exactly as an absent row. Post-filtering after `k` will not be implemented.
- **Document index and section index will be separate.** A document-level index will hold the declared summary, triggers, symbols and domain; a section-level index will hold section text. L0 will rank documents from the document index, never from a section list. Section search will run only inside the documents L0 and L1 selected, and a per-document cap will stop one long document from filling the returned sections.
- **Lexical channel.** A full-text index over the document-level and section-level rows, queried with column filters for collection, domain and subtree. Ranking will use document and section statistics computed over the **visible candidate set for the query**; no statistic will be pooled across Minds or include a hidden row, so adding or removing a hidden document changes no visible score or order.
- **Vector channel.** Vectors will live in ordinary tables keyed by collection, item kind and identity. A query will apply the same visible candidate predicate in the statement that orders by distance and limits to `k`; the distance is per row, so the same set and limit rule gives the same independence from hidden rows. Vectors will be optional per deployment.
- **The embedding input will be a projection, not the stored record.** The text embedded for a document or a section will be its **visible layer body plus a stable node or document identity only** — for a document, the declared summary, triggers and symbols with the document id; for a section, its body with the document id and section id. Source, generator, freshness, version, review state and every other internal metadata field will be excluded. The same projection function will be used by a full rebuild and by a vectors-only reindex, so both produce the same input bytes. The recorded input digest will be over the projected text, not over the stored record; the visible body itself is defined by `2026-11-27-layered-knowledge-retrieval.md`.
- **Deterministic fusion.** The channels will be fused by reciprocal rank, with ties broken by document identity and section position. Channel scores will not be added or normalised against each other.
- **Embedding identity.** Vector rows will record the embedding identity and the digest of the text that was embedded. A fill will write only when both still match; an identity switch will fill beside the old rows, switch atomically, then delete the old ones.
- **Accelerator gate.** A partitioned vector index will be adopted only if the ordinary-table form misses a recorded latency gate on the largest measured collection. Until that measurement exists, the scalar form is the index; no second index will be built on expectation.

## Alternatives considered

- **Vector-only.** Good recall on paraphrase, simple pipeline. Rejected: exact identifiers and rare terms are the highest-value queries for a rule corpus, and a nearest-neighbour search answers them unreliably.
- **Lexical-only.** Predictable, no extension needed, cheapest. Rejected: it fails the paraphrase case outright, and adding a synonym layer by hand moves the maintenance cost into a glossary nobody updates.
- **One global index with a post-filter.** Simplest isolation code. Rejected: the top `k` can be entirely outside the caller's Mind, so the answer is truncated before the filter; the failure is silent and load-dependent.
- **A partitioned vector table now.** Better asymptotics on large collections. Rejected as premature: it needs its own delete and identity handling, and its benefit over a filtered scan is unmeasured on this corpus. It stays the documented fallback behind the measured gate.
- **Add raw channel scores after normalisation.** Familiar hybrid. Rejected: BM25 and cosine are not comparable, normalisation depends on the candidate set, and the order becomes unstable across Minds.
- **A model reranker over the fused top set.** Rejected for this decision: nondeterministic, per-query cost, and unnecessary while L1 closes most tasks; it would also make the retrieval quality gate depend on a model version.
- **Pool term statistics across Minds for better BM25.** Rejected: it makes one Mind's corpus a ranking input to another's, which is an isolation breach that access checks cannot see.

## Acceptance criteria

- Every returned section will belong to the requested collection; a test where the global top `k` are all outside the Mind will still return `k` in-Mind rows when the Mind has them.
- Filling the top candidates with hidden same-Mind rows will still return `k` visible rows when the Mind has them, and the count and errors will be identical to a corpus without the hidden rows.
- The same visible rows, scores and order will be produced with and without hidden same-Mind documents at unchanged versions; adding or removing a hidden document will change no visible score, rank or tie.
- The search predicate will be the same function the read paths apply: for every candidate, a test will compare the search's visibility decision with the get or hydration decision on the same entry, and a candidate the read refuses will never be ranked, counted or returned.
- A hidden document, section or history revision will answer exactly as an absent one, with no content and no existence signal.
- Doubling another Mind's corpus will leave the order and the tie order inside a Mind identical.
- A query whose terms match no section lexically but whose vector is near a section will return that section; a query naming an exact identifier will return the section that contains it.
- L0 will be ranked from the document-level index: a document whose summary or symbols match strongly will enter L0 even while a longer document supplies most section hits; a section search will return only sections of the documents L0/L1 selected, capped per document.
- Repeating a query on the same bytes and compiler version will return the same order.
- A fill whose embedded-text digest or embedding identity changed will write no row.
- Changing only excluded metadata — a freshness counter, a generator component, source metadata, review state or a document version — will leave every vector, score and order unchanged.
- A full rebuild and a vectors-only reindex from the same document bytes, compiler version and embedding identity will produce the same embedding input digest for every vector row, and the same vector from the same provider.
- An identity switch will leave no query mixing two identities, and old rows will be removed after the switch.
- No partitioned index will exist before a recorded measurement shows the scalar form missing its gate.

## Risks

- **The two indexes drift from the corpus.** Mitigation: both are rebuilt from the same section rows in one projection step; a rebuild is the repair, not a reconciliation.
- **Embedding model changes are disruptive.** Mitigation: identity numbers, fenced fills and an atomic switch; queries always name the active identity.
- **Prefix context may lower recall or inflate storage.** Mitigation: a short ancestor-path prefix is measured with and without it, and the result decides.
- **The visible predicate and its statistics may be expensive.** Determining the visible set and computing statistics over it costs more than a Mind-wide statistic. Mitigation: both are computed over the filtered candidate set in the same pass, measured on the largest bucket, and never replaced by Mind-wide statistics, which would let a hidden document change a visible order.
- **Document-level and section-level indexes can disagree.** A document enters L0 on its summary while its section text has changed. Mitigation: both indexes are written in the same projection step from the same document version.
- **The measurement corpus may not represent real documents.** Mitigation: the harness accepts a real export as an additional input and reports both.
- **The embedding projection can drift from the visible body.** A rebuild and a reindex could embed different text. Mitigation: one projection function serves both, the input digest is over the projected text, and a parity test compares the digest from a rebuild with the digest from a reindex.
- **The lexical index may be unavailable in a thin build.** Mitigation: readiness flags say which channels a deployment serves, and a search over a build without the vector channel still ranks lexically rather than failing.
