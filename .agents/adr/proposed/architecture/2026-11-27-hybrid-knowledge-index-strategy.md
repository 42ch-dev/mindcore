# ADR: Lexical and vector retrieval over one scoped section set, fused by rank

Status: proposed — no lexical or vector index exists; the strategy is agreed and unmeasured
Date: 2026-11-27
Scope: The two retrieval channels, where isolation and filtering apply, how their results are fused and ordered, and when a heavier vector index may be adopted
Related: `2026-11-27-derived-knowledge-indexes.md`, `2026-11-27-layered-knowledge-retrieval.md`, `2026-11-27-full-lifecycle-delivery-and-evaluation.md`

## Problem

A lexical index alone misses paraphrase: a question phrased differently from the document returns nothing, and the reader concludes the rule does not exist. A vector index alone misses exact tokens: identifiers, version strings, error codes and rare terms are exactly the queries where a nearest-neighbour search is least reliable, and the answer must be the section that literally names the symbol.

Mixing both carelessly creates a worse failure. If one index ranks globally and the Mind, domain or subtree filter is applied afterwards, the top results are consumed by rows the caller may not see, and the filtered answer is short or empty even though the corpus is full. If term statistics are computed across Minds, one Mind's corpus changes another Mind's ranking, which breaks isolation in a way no access check would catch. If fusion adds raw scores, the two channels' incomparable scales decide the order.

Today none of this exists — there is no index to isolate — so the choice is free, and choosing a heavy index before measuring would spend the cost the refactor is meant to avoid.

## Proposal

Retrieval will use two channels over the same projected section rows, with isolation applied before ranking.

- **Lexical channel.** A full-text index over section text and declared metadata, queried with column filters for collection, domain and subtree. Ranking will use document and section statistics computed per Mind; no statistic will be pooled across Minds.
- **Vector channel.** Vectors will live in ordinary tables keyed by collection and identity. A query will filter by collection, domain and subtree in the same statement that orders by cosine distance and limits to `k`, so the limit applies after the filter by SQL semantics. Vectors will be optional per deployment.
- **Isolation before Top-K.** Every predicate — Mind, domain, subtree, document — will be applied before any `k` is taken. Over-fetching and filtering afterwards will not be implemented, because it truncates.
- **Deterministic fusion.** The channels will be fused by reciprocal rank, with ties broken by document identity and section position. Channel scores will not be added or normalised against each other.
- **Embedding identity.** Vector rows will record the embedding identity that produced them and the digest of the text that was embedded. A fill will write only when both still match; an identity switch will fill beside the old rows, switch atomically, then delete the old ones.
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
- Doubling another Mind's corpus will leave the order and the tie order inside a Mind identical.
- A query whose terms match no section lexically but whose vector is near a section will return that section; a query naming an exact identifier will return the section that contains it.
- Repeating a query on the same bytes and compiler version will return the same order.
- A fill whose embedded-text digest or embedding identity changed will write no row.
- An identity switch will leave no query mixing two identities, and old rows will be removed after the switch.
- No partitioned index will exist before a recorded measurement shows the scalar form missing its gate.

## Risks

- **The two indexes drift from the corpus.** Mitigation: both are rebuilt from the same section rows in one projection step; a rebuild is the repair, not a reconciliation.
- **Embedding model changes are disruptive.** Mitigation: identity numbers, fenced fills and an atomic switch; queries always name the active identity.
- **Prefix context may lower recall or inflate storage.** Mitigation: a short ancestor-path prefix is measured with and without it, and the result decides.
- **The measurement corpus may not represent real documents.** Mitigation: the harness accepts a real export as an additional input and reports both.
- **The lexical index may be unavailable in a thin build.** Mitigation: readiness flags say which channels a deployment serves, and a search over a build without the vector channel still ranks lexically rather than failing.
