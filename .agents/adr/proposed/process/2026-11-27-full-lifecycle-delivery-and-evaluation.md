# ADR: Deliver the whole Knowledge lifecycle in dependency order, with measured gates

Status: proposed — no stage is planned, scoped or costed beyond this record
Date: 2026-11-27
Scope: Delivery staging for the complete Knowledge feature, the stage that owns each promised capability, the acceptance and evaluation gates, and what may not be dropped or redefined as an MVP
Related: `2026-11-27-dual-store-knowledge-boundary.md`, `2026-11-27-layered-knowledge-retrieval.md`, `2026-11-27-spoke-knowledge-protocol-gaps.md`, `2026-11-27-hybrid-knowledge-index-strategy.md`, `2026-11-27-storage-agnostic-knowledge-seams.md`

## Problem

The feature spans four subsystems and one external protocol: a file store, a write gate, a projection and read path, retrieval, history and drift, protocol carriers, and deployments that have no filesystem. Its benefit is only observable at the end: cost per accepted task depends on the layered read, the retrieval funnel and the format together, not on the write path alone.

That shape invites a false economy. A "write path first" plan ships working writes with no cheaper reads, and the cost goal is declared out of scope. A "first phase only" plan drops L2 hydration, history or the protocol carriers, which then have no committed home. A plan staged by subsystem in parallel produces integrations nobody can review. The first draft of this record also left three promised capabilities without a stage or an owner: the diskless read-only provider, the backup and restore drill, and the protocol standardisation delivery; and it named numeric thresholds without saying which stage owns the mechanism, which input measures it, or whether it is a gate or a report.

Any of these produces a feature that cannot be evaluated, because the evaluation needs the whole lifecycle.

The owner's constraint is explicit: staging exists to order dependencies and reduce risk; it must not omit later functionality or redefine the feature as a smaller MVP. The research report's own "beyond this roadmap" section is therefore superseded where it defers capability: deferral here means a later stage of this plan, never a smaller feature.

## Proposal

The complete lifecycle will be delivered in twelve dependency-ordered stages, all retained.

| Stage | Scope | Must precede | Owner of |
|---|---|---|---|
| S1 | Seam traits, DTOs, error taxonomy, provider contract suite including a publish-phase failure and the atomic-publication capability | everything | contract shape |
| S2 | Grammar, deterministic compiler, drift fingerprint, lineage rules, fixtures | gate, projection | format and compile rules |
| S3 | Bare store provider: per-document refs, root and tombstone, batch record decision point, staging, per-document compare-and-swap, head enumeration | write gate | store mechanics, write latency |
| S4 | Write entry: admission, create/delete fences, validation, dependency confirmation and refusal, receipt, activation and claims | projection | write rejections, receipt |
| S5 | Projections: per-document cursor, head-inventory adoption, idempotent apply, crash windows, compiler stamp, authoritative finding review tables | read path | catch-up, discovery cost, review-state survival |
| S6 | Layered read: L0 cards, L1 outlines, L2 pinned hydration, per-item visibility, current-view staleness | retrieval | read shapes, layer closure |
| S7 | Retrieval funnel: document-level index, section-level index, one visible candidate set, fusion, accelerator gate | evaluation | isolation, ordering, recall mechanism |
| S8 | Cold path: history, log, diff, drift findings and the review flow (dismiss, accept, acknowledge, re-anchor) | evaluation | drift mechanism and precision |
| S9 | SPOKE Phase 1: view, span, anchor, feature, receipt and history carriers; direct list/get round trip; conformance; old-client bytes | standardisation | peer round trip, receipt reachability |
| S10 | SPOKE standardisation delivery: promoted members and operations, assemble packet carrier, scoped FFI face, version cohort, SDK parity | evaluation of the peer path | standardisation completeness |
| S11 | Diskless and read-only provider: in-memory store, prefetched object provider, prebuilt read-only index, no compiler and no vector channel | evaluation of the provider seam | provider parity |
| S12 | Backup and restore drill, then the full evaluation run on the complete feature | release | restoration, every numeric gate below |

Each stage will merge on its own, behind the capability it advertises, and will change no caller-visible behaviour before the stage that serves that behaviour. Later stages will be planned work in the same feature, not options to drop.

**Optional model enhancement will never be a baseline gate.** The deterministic compiler, container navigation and baseline ranking are mandatory. A model-generated abstract is an optional derived artifact with model/prompt/input provenance and a deterministic fallback; it is not required for availability, the write gate, compilation, baseline ranking or acceptance. Evaluation will run the mandatory deterministic baseline first and report an enhancement-enabled run separately.

**Metrics will have one owner, one input and a declared class.** A mandatory gate blocks its stage or the release; a reported metric is measured and published without blocking.

| Metric | Owner stage | Input | Class | Condition |
|---|---|---|---|---|
| Cost per accepted task | mechanism S6/S7, measured S12 | three size buckets plus a real export; baseline measured in the same run | Mandatory | at least 60% below the status-quo baseline on every bucket |
| Document Recall@5, MRR@10 | S7, measured S12 | 400 labelled questions | Mandatory | ≥ 0.85 and ≥ 0.75 |
| Section Recall@10 | S7, measured S12 | same questions at the low-noise setting | Mandatory | ≥ 0.70 |
| Drift precision and false-positive rate | S8, measured S12 | labelled change set with and without lineage | Mandatory | precision ≥ 0.95; false positives ≤ 1% of unchanged anchors |
| Write-gate plus commit latency | S3/S4, measured S12 | one 8 KiB document | Mandatory | p99 ≤ 50 ms |
| Catch-up per step | S5, measured S12 | chains of 100, 1 000 and 10 000 one-document commits from 10%, 50% and 100% behind | Mandatory | per-step p99 ≤ 100 ms; 10 000 steps ≤ 30 s |
| Compiler-version rebuild | S5, measured S12 | a 500-document Mind | Mandatory | p99 ≤ 2 s, one transaction per document |
| Rank invariance and isolation | S7, measured S12 | one Mind before and after another Mind's corpus doubles; hidden documents among the top candidates | Mandatory | identical order and ties; `k` visible rows; no visible score or order changes when hidden documents are added or removed |
| Old-client response bytes | S9, measured S12 | a peer without the features; a collection with no document-backed entry | Mandatory | dynamic entries byte-identical; the document-free collection byte-identical |
| Restore from backup | S12 | a completed backup | Mandatory | every read, version and history answer identical on a fresh host |
| L1 first-pass closure | S6, measured S12 | accepted tasks needing no hydration | Reported | target ≥ 70% |
| Head-discovery cost | S5, measured S12 | collection head enumeration on every collection search and catch-up invocation | Reported | measured per search and separately from replay; it is a search-path cost, not avoided by the cursor |
| Cache hit rate | S3/S5, measured S12 | a rename series and a revert series | Reported | target ≥ 90% |
| Index backfill | S5, measured S12 | a 2 000-document collection written before the indexes | Reported | batches of 100; measured |
| Oracle whole-document baseline | S7, measured S12 | the same corpus and questions | Reported | bounds the achievable gain; no gate |
| Optional model enhancement | S2/S6/S7, measured S12 | the same corpus with enhancement enabled and disabled | Reported | deterministic baseline is mandatory; enhancement quality/cost reported separately, with model/prompt/input provenance |

**Stage gates will be as concrete as the metrics.**

- S1: an external implementation compiles and passes the groups its capabilities require — every provider the read and write-refusal groups, a writable one the writable-publication group including a publish-phase failure.
- S2: positive fixtures compile byte-exactly; every rule has a refusal case; a blank-line-only change outside a fence compiles to the same fingerprint; a trailing-whitespace document is refused; an indentation change that alters block structure changes the fingerprint; no model call is on the compile or gate path, and an optional abstract carries model/prompt/input provenance with deterministic fallback.
- S3: a create against an existing head is refused; a tombstone keeps the head; an attempt ref is created before staging and revoked at release, so a paused attempt cannot stage; a batch decision consumes no staging credential; a decided batch retains its credentials and settles once, while an undecided attempt is revoked at expiry; a revoked attempt answers `attempt revoked` even when its staging credential is gone, and only a live attempt answers `staging expired`; a mutual move builds in either order; a publish failure leaves every named document old or new, settles each target by reachability and reports only the truly unpublished ones.
- S4: every rejection in the format and lineage lists refuses with rule and byte range; a dependency already moved at the post-staging check refuses and one that moves afterwards produces a finding; a partial batch answers `incomplete` with each published target's version and commit and only the unpublished ones retryable; a dropped response is resolved by the receipt; a released attempt cannot create its first credential, stage or publish; a cross-document operation writes its lineage record before the batch decision ref, and a failed write leaves no decision ref and no head move; a create retried after a released claim yields exactly one root; a delete through the negotiated tool appends a tombstone and its lost response is resolved by its own request id and digest; a head published before its claim repair is served by a read, and an ordinary update cannot resurrect the id; activation preserves a seeded legacy row. A shared put or delete requires a qualified `document_ref` and a write grant on that exact collection; an unqualified write resolves only the primary Mind collection and cannot target shared, and the Mind's same-id entry and history stay unchanged. A put whose `document_ref.entry_id` differs from the body `entry.entry_id` is refused before any pre-read and mutates nothing.
- S5: each crash window is injected; a head with no cursor is adopted; dropping the derived set preserves review decisions, and rebuilding with changed anchors follows the valid owners; the admitted tenant's SQLite authority database is selected before any Mind, collection, grant, claim, receipt, projection or history lookup, with migration into one authority database per tenant; container navigation is derived from path and visible children with no folder descriptor or parent commit, the visible child set and authorization fingerprint are in the cache identity, and shared and Mind-owned heads are discovered once by stable id in the admitted read set; unqualified reads resolve only the primary Mind collection and shared or non-primary documents carry `document_ref {collection, entry_id}`, so a shared id cannot shadow an existing Fact. The container authorization fingerprint stays in the server-side cache identity and is never returned.
- S6: node-level layers are present: a container L1 is bounded child navigation and key constraints, a document L1 is section outline with a complete handle per section, and L2 is exact leaf content; container views are reachable through `tools.<ns>.read_container_view` with a required validated Mind scope and no default, bounded results and no fabricated container version, and container L2 is refused by name. Visible children and the authorization fingerprint are computed before navigation, summaries or model input; the fingerprint stays in the server-side cache identity and never appears in a response, and high-privilege caches do not serve low-privilege views. No model is required for container availability or baseline ranking. Pinned handles return their version and never answer `ViewStale`, a hidden item answers as absent, and a diff with a hidden side answers as not found.
- S7: hidden rows in the top candidates still yield `k` visible rows and change no visible score or order; the search's visibility decision matches the read predicate item by item; shared and Mind-owned documents enter the admitted read set of Mind collections plus shared collections visible by scope, so two admitted Minds resolve the same shared version through search, get, hydration and the qualified history tool with no per-Mind copy; L0 is document-ranked; sections come only from the selected documents, capped per document; every search result carries `document_ref {collection, entry_id}` and no result is ambiguous; the actual embedding identity `(provider, model, model version, dimensions)` and the projection mode are checked separately, a provider/model/version/dimension change never mixes vectors, and baseline vectors survive enhancement failure with no reindex under the same active embedding identity.
- S8: the classification table holds; a successor attaches only to a published target — the containing commit for a same-document target, the recorded candidate for a cross-document one — independent of publication order; a same-document target stores no commit id; a cross-document binding survives removing staging and batch refs and a garbage collection; a late target publication refreshes the source finding on the target's projection or a source finding read without rebuilding the source; declared lineage replays after a crash; no automatic re-anchor; shared history replays through `tools.<ns>.knowledge_history_get` with a validated Mind scope and `document_ref {collection, entry_id}`, resolved inside that scope's admitted read set and disclosure with a missing scope refused, while unqualified `history.revisions.get` stays primary-Mind-only.
- S9: the direct round trip including L1-carried handles, explicit layer selection with no implicit traversal, the reachable `tools.<ns>.read_container_view` with bounded container navigation and named L2 refusal, shared stable ids resolvable through get, hydration and the qualified history tool under a validated Mind scope from the admitted read set, the peer delete through the negotiated tool with its tombstone replay and its own request id, the receipt and the old-client bytes pass. The container tool requires a validated Mind scope with no default, and the same principal reading one shared container under two scopes receives each scope's authorized children. Admission derives tenant from the authenticated caller/session and selects both the Git repository and the tenant's SQLite authority database before any lookup, a raw extension hint cannot select it, ambiguous or foreign tenants are refused, and same-named Mind ids in different tenants do not collide; no second Mind-routing field is introduced. Shared and non-primary results carry `document_ref {collection, entry_id}`, and no response carries the authorization fingerprint.
- S10: schemas, cohort, assemble carrier, scoped FFI and SDK parity are delivered.
- S11: the read-only provider passes the read and write-refusal groups without a compiler or a vector channel.
- S12: the restore drill passes, then the evaluation runs on the complete feature, including shared visibility and directory navigation acceptance; the deterministic baseline runs first, and any model enhancement is reported separately with provenance, never as a baseline gate.

**If a measured gate misses, the response will be a mechanism fix or a recorded decision — not a dropped layer and not a restated goal.** Selection, fusion, prefixing, index level and caching are the levers; the scope of the feature is not.

## Alternatives considered

- **A write-path MVP, with reads planned later.** Fastest visible progress. Rejected by the owner and by the evaluation: writes alone produce none of the cost saving, and the later read stages would have no committed owner.
- **A vertical slice for one document kind.** Proves the mechanism on real content. Rejected: it leaves the general grammar, the domain boundary and the retrieval model unproven, and the slice's shortcuts become the architecture.
- **Build the read path over SQLite first, then swap the store.** Lower risk per stage. Rejected: the read path would be written twice, and the first version's assumptions would leak into the second.
- **Deliver everything at once.** One integration, one review. Rejected: unreviewable, and a failure anywhere blocks every capability.
- **Stage by subsystem in parallel.** Shorter wall time. Rejected: the seams, the gate and the read path are dependent, and parallel integration would decide the contracts by accident.
- **Defer the diskless provider, the restore drill and the standardisation to a follow-up.** Keeps this plan shorter. Rejected: those are promised capabilities of the feature, and a follow-up without a stage, owner and gate is the omission this record exists to prevent.
- **Gate the protocol work on a consumer count.** Rejected: the gap is an absent capability, and the protocol stages are planned here like every other stage.
- **Treat the cost goal as aspirational and ship the mechanism.** Rejected: the goal is the reason for the refactor, and a mechanism that misses it must be revised, not excused.

## Acceptance criteria

- The plan will contain all twelve stages, each with its own merge criteria and an owner for the capability it promises; no stage will be marked optional or "future".
- Every mandatory and reported metric above will be traceable to an owner stage, an input corpus and a pass condition.
- The restore drill will block release: a backup that has never been restored will not be accepted as a backup.
- The evaluation will run on the complete feature, with the status-quo baseline measured in the same run.
- No stage will merge a caller-visible change before the stage that serves it, and no later stage will be removed to make an earlier one ship.
- A missed gate will produce a recorded mechanism decision and a revised mechanism, not a reduced scope.
- The old-client conformance case will stay green through every stage.
- A shared document will be referenced by id and readable by many admitted Minds in one tenant with one history; an unadmitted or foreign tenant will be refused, and same-named Mind ids in different tenants will not collide.
- Admission will select the tenant's SQLite authority database and Git repository before any Mind, collection, grant, claim, receipt, projection or history lookup; a principal selecting Mind `X` in tenants A and B will see independent grants, reads, writes, searches, history and receipts.
- Two sibling folders will return separate bounded L1 navigation through `tools.<ns>.read_container_view` with no fabricated container version; a container L2 request will return no leaf content and a named refusal.
- A container cache prewarmed at high privilege will not serve a lower-privilege view: path, count, summary, constraints and abstract will be unchanged when high-only children are added or removed, and the cache identity will include the visible child set and authorization fingerprint.
- Folder metadata and declared constraints will derive only from child documents and canonical paths; dropping the projection and rebuilding from document heads will reproduce the same path and constraints.
- Baseline vectors and indexes will be retained alongside enhancement; invalidating the model artifact will restore the exact pre-enhancement baseline ranking without a model call or reindex, under the same active embedding identity.
- Unqualified reads and writes will resolve only the primary Mind collection; shared or non-primary reads and put/delete targets will carry `document_ref {collection, entry_id}`, a shared write will require a grant on that collection, and a shared id cannot shadow an existing Fact, whose history remains.
- A shared update or delete by qualified ref will change the shared document exactly once; the Mind's same-id entry, history and vectors stay unchanged, while scores and ranks are recomputed from the new visible candidate set. An unqualified write cannot target shared.
- A put whose `document_ref.entry_id` differs from the body `entry.entry_id` will be refused before any pre-read, with no history or vector change, while a matching request updates only the target collection.
- `knowledge_history_get` will require a validated Mind scope and resolve a shared version only inside that scope's admitted read set; the same shared version under two scopes returns a snapshot for the admitted scope and absent-like for the other, and a missing scope is refused.
- The container tool will require a validated Mind scope with no default; the same principal reading one shared container under two scopes will receive each scope's authorized children, and no response will carry the authorization fingerprint.
- The actual embedding identity `(provider, model, model version, dimensions)` and the projection mode will be checked separately; changing provider, model, version or dimensions will not mix vectors, and enhancement failure will fall back to the deterministic projection under the same active identity.
- Folder and container views will be composed from document heads and child versions; a folder move will produce new versions for affected documents and no collection-wide commit or sequence.
- No acceptance gate will require a model artifact. The deterministic baseline is mandatory; optional model enhancement is evaluated and reported separately with provenance and a deterministic fallback.

## Risks

- **Long horizon.** Twelve stages is a long commitment. Mitigation: each stage is independently mergeable and useful internally, and stage boundaries are review units.
- **A measured gate may fail on a synthetic corpus.** Mitigation: the harness accepts a real export as an additional corpus and reports both; a miss is a mechanism decision.
- **Stage coupling.** A late discovery in S6 can force a seam change. Mitigation: S1's suite and S2's rule vectors are the early place to discover it, and a seam change is a reviewed revision of that record.
- **Evaluation cost.** Several buckets with a re-measured baseline costs time. Mitigation: the harness reuses existing statement and provider-call instrumentation, and the baseline is measured once per run.
- **The restore drill is slow and unglamorous.** Mitigation: it is a stage gate, not a checklist item; its input is a real backup, and its output is a comparison of every read and history answer.
- **The standardisation cohort depends on an external release window.** Mitigation: S9 delivers the peer path on its own; S10 is scheduled by that window, not by demand, and stays in this plan.
- **Head enumeration adds a discovery cost to search.** Mitigation: it is measured per search (reported), bounded by the document count, and the per-document cursor removes replay of unchanged documents but not the enumeration; this record states it as a search-path cost rather than calling it outside the hot path.
