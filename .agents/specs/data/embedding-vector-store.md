# Data: embedding vector store

## Scope

An optional SQLite cache of live retained Memory record vectors for `recall_context`. The host supplies `Embedder`; Mindcore supplies neither a model nor a vector extension. Scoring is exact over the authorized recall page, not a global index or ANN search.

## Current state

`identity_hash` is the 64 lowercase hex SHA-256 of the RFC 8785 canonical UTF-8 JSON object `{"dim":384,"model_id":"model","model_version":"version","provider_id":"provider"}` with the actual four fields substituted. Use SPOKE's `jcs_sha256_hex`. Keep identifier strings exact (no trimming, case folding or Unicode normalization); dimension is a positive integer whose four-byte vector length is representable on the target — `4 * dim` must fit `usize`, and the identity is rejected otherwise.

`memory_embedding_vectors(record_id TEXT, identity_hash TEXT, record_revision INTEGER, dim INTEGER, vector BLOB)` has non-null columns, primary key `(record_id, identity_hash)`, revision and dimension stored as integers (`typeof` of each column is `integer`, since `INTEGER` affinity alone keeps a fractional value as REAL) and both positive, and `length(vector) = 4 * dim`. The backing Memory schema keys records by `record_id` within a database. A vector hit requires exact identity, current authorized record revision and dimension; the revision is a freshness marker, not part of the key. One row replaces any old revision for that record/identity. Encode each finite `f32` component in little-endian IEEE 754 binary32 without a header or quantization; reject malformed lengths, non-finite components and all-zero vectors. Embed the same `record_text` input that `rank` uses.

Switching identity can never hit the old partition. Retire an identity through the one supported host entry, `MindcoreSurface::retire_embedding_identity(&EmbeddingIdentity)` in `crates/mindcore-runtime/src/lib.rs`: it clears that identity's in-memory query slot and deletes its disk partition in one statement over the `identity_hash` index — `DELETE FROM memory_embedding_vectors WHERE identity_hash = ?1` — returning how many rows went. Call it between recalls: the surface does not drain a recall already in flight, so drain that identity's in-flight vector producers first, and a fill that commits afterwards re-creates rows the call deleted.

Use the trusted retired hash, not an untrusted request value and not `<> active_identity`. Different identities may coexist until retirement; do not sweep or rebuild unrelated rows. Recompute only written records and authorized recall misses.

A record's rows live only while the record is live. `memory.records.delete` scrubs the body onto a tombstone, and the same transaction deletes every row of that record — `DELETE FROM memory_embedding_vectors WHERE record_id = ?1` over the primary-key prefix. A tombstone therefore never keeps a vector, and recall's miss set covers live records only: a tombstoned body is never re-embedded, scored by similarity or filled. The delete is cache-only — its failure leaves a stale row no read can serve, never a refused or rolled-back tombstone.

### Read and fill order

1. Obtain the existing authorized recall page, including the withheld-read check. No vector lookup, cache lookup, staleness probe or provider work happens for a refused page. An empty page/query, absent embedder or `identity() == None` uses adapter lexical order with no vector table I/O.
2. Resolve the query vector first. Keep one recent validated query vector in memory per `SpiMindWorkflows` instance, keyed by serving identity, authenticated principal, space, subject and exact trimmed query. Do not persist queries, hold a cache lock across provider work or fill the query slot from an unrevalidated request. A cold query embeds its own text. If it returns `EmbeddingsUnavailable`, fall back without touching the vector table.
3. After any query provider call, recheck page admission and withholding in the SQLite snapshot used to select vector rows. Compare current live revisions to the authorized page, then look up record vectors only for its authorized `(record_id, revision)` pairs under that identity — one bounded page-shaped statement, never one point lookup per record. Embed only missing/stale live page texts; a tombstone is never a miss and keeps no row. If this later batch returns `EmbeddingsUnavailable`, discard hybrid work, use lexical order and write nothing; a read-only lookup may already have happened, and any warm query slot survives. Other provider errors still propagate on recall.
4. Release and cache fill are one binding boundary: in one transaction the adapter re-reads the authorized page and knowledge set, compares both to the read the recall ranked, and writes the page's missing vectors. A refusal or a moved set aborts the release before anything is written; a fill for a record the transaction no longer serves live at that revision is skipped, and a cache-only storage failure leaves a miss rather than refusing a release. The query slot is installed only after that boundary succeeds, so no awaited cache work can observe a refusal after release. Never release a cache hit or index a record's body after failed revalidation.
5. After a fresh record is retained or its text changes, warm the cache for the committed revision when the host supplies a serving embedder and the record passes the same live/settled guards. Embedding runs outside the settlement transaction, and the write uses the same batch fill operation with one element. A post-commit cache failure leaves a miss; it must not undo, replay or misreport a durable Memory effect. Do not warm on an idempotent replay, unsettled intent, tombstone, missing identity or legacy storage path without a host embedder.

### Ranking

Reuse `hybrid_scores` on the same page and ordering with bit-preserving decoded `f32` values: `0.5 × normalized_BM25 + 0.5 × (1 + cosine)/2`. Stable descending sort preserves adapter order on ties; leave the reasoner and packing stages unchanged. An identical authorized query with a warm query slot and current record vectors needs zero provider calls. A new query embeds its text but does not re-embed current records.

## Invariants

- Storage IO never precedes memory authorization and withholding; identity or revision mismatch is a miss, not a usable approximation. No live provider call occurs under a SQLite settlement lock.
- Ablation: storing the revision prevents reuse after text changes without keeping a row for every revision; the one query slot is needed for zero provider calls on an identical warm recall.
- `NoEmbeddings` leaves the vector table untouched even when rows from an earlier embedding-enabled run exist.

## Where it lives

- `crates/mindcore-spi/src/{lib,workflows}.rs`: host contract, query slot, exact ranking and revalidation sequencing.
- `crates/mindcore-spoke/src/workflows.rs`: authorized SQLite snapshot and cache-fill boundary. `crates/mindcore-sqlite/src/vectors.rs` and `crates/mindcore-sqlite/migrations/`: vector codec, scoped lookup/upsert and bulk retirement.
- `crates/mindcore-runtime/src/lib.rs`: `MindcoreSurface::retire_embedding_identity`, the one host entry that sequences the slot clear and the partition delete.

## Change obligations

Change the storage spec and parity tests together when identity, vector encoding, freshness or ranking changes. Compare cold and persisted paths with a deterministic fixture: equal input bytes, per-item scores and stable tie order; prove lexical, withheld, stale-revision and wrong-identity paths separately.
