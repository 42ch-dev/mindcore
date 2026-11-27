# ADR: Deliver the whole Knowledge lifecycle in dependency order, with measured gates

Status: proposed — no stage is planned, scoped or costed beyond this record
Date: 2026-11-27
Scope: Delivery staging for the complete Knowledge feature, the acceptance and evaluation gates, and what may not be dropped or redefined as an MVP
Related: `2026-11-27-dual-store-knowledge-boundary.md`, `2026-11-27-layered-knowledge-retrieval.md`, `2026-11-27-spoke-knowledge-protocol-gaps.md`, `2026-11-27-hybrid-knowledge-index-strategy.md`

## Problem

The feature spans four subsystems and one external protocol: a file store, a write gate, a projection and read path, retrieval, history and drift, and SPOKE carriers. Its benefit is only observable at the end: cost per accepted task depends on the layered read, the retrieval funnel and the format together, not on the write path alone.

That shape invites a false economy. A "write path first" plan ships working writes with no cheaper reads, and the cost goal is declared out of scope. A "first phase only" plan keeps the write path and drops L2 hydration, history or the protocol carriers, which then have no committed home. A plan staged by subsystem in parallel produces integrations nobody can review. Any of these produces a feature that cannot be evaluated, because the evaluation needs the whole lifecycle.

The owner's constraint is explicit: staging exists to order dependencies and reduce risk; it must not omit later functionality or redefine the feature as a smaller MVP. The research report's own "beyond this roadmap" section is therefore superseded where it defers capability: deferral here means a later stage of this plan, never a smaller feature.

## Proposal

The complete lifecycle will be delivered in dependency-ordered stages, all retained.

| Stage | Scope | Why it must precede the next |
|---|---|---|
| S1 | Seam traits, DTOs, error taxonomy and the provider contract suite | Every later stage is written against one contract |
| S2 | Grammar, deterministic compiler, drift fingerprint, lineage rules and fixtures | The gate cannot exist before the rules do |
| S3 | Bare store provider: per-document refs, staging, compare-and-swap, exact related-document commits | The write path needs a store that can move exactly the named documents |
| S4 | Controlled write entry: admission, version fences, validation, staging, commit, receipt, identity reservation | Nothing may enter the store except through this path |
| S5 | Projections: per-document cursor, idempotent apply, crash windows, catch-up, compiler epoch | A read has nothing to query without it |
| S6 | Layered read: L0 cards, L1 outlines, L2 hydration by handle | The cost goal is only measurable with a selectable view |
| S7 | Retrieval funnel: lexical channel, vector channel, isolation before Top-K, deterministic fusion, accelerator gate | L0 pruning needs ranked candidates |
| S8 | Cold path: history, log, diff, drift findings, review and repair flow | Drift has no consumer until anchors and reviews exist |
| S9 | SPOKE Phase 1: view, span, anchor and feature carriers, history entity kind, conformance | A peer can only be served once the local shapes are stable |
| S10 | SPOKE standardisation: promoted members and operations, cohort, SDK and FFI parity | It promotes what Phase 1 validated |

Each stage will merge on its own, behind the capability it advertises, and will change no caller-visible behaviour before the stage that serves that behaviour. Later stages will be planned work in the same feature, not options to drop.

**Acceptance and evaluation will gate the stages, not just the release.**

- Contracts: the provider suite passes for every provider, and an external implementation compiles.
- Grammar and compiler: positive fixtures compile byte-exactly; every rule has a refusal case; compilation is deterministic across hosts.
- Store: same-document races produce one commit; different-document writes do not interfere; a related-document failure moves nothing; no working tree exists.
- Write gate: every violation refuses with rule and byte range; attribution is readable from the commit; no alternate write route exists.
- Projection: each crash window is injected; a rebuild reproduces the projection; no transaction spans store I/O.
- Read: layer shapes as specified; hydration verifies digests; a pinned handle returns its pinned version; a foreign collection is refused; token use is reported.
- Retrieval: isolation before Top-K; rank invariance across Minds; deterministic tie order; no accelerator before its measured gate.
- History and drift: the classification table holds; a whitespace-only edit outside a fence produces no finding; declared lineage is honoured; nothing auto-rewrites an anchor.
- Evaluation: cost per accepted task at least 60% below the status-quo baseline on every size bucket; L1 first-pass closure reported against its target; document and section recall and MRR above their gates; drift precision and false-positive rate within their bounds; write-gate plus commit latency, catch-up, and compiler-epoch rebuild measured against their percentiles; old-client bytes unchanged; no historical blob in a hot index.

**If a measured gate misses, the response will be a mechanism fix or a recorded decision — not a dropped layer and not a restated goal.** Selection, fusion, prefixing and caching are the levers; the scope of the feature is not.

## Alternatives considered

- **A write-path MVP, with reads planned later.** Fastest visible progress. Rejected by the owner and by the evaluation: writes alone produce none of the cost saving, and the later read stages would have no committed owner. Writes are a stage here, not a destination.
- **A vertical slice for one document kind (for example, procedures only).** Proves the mechanism on real content. Rejected: it leaves the general grammar, the domain boundary and the retrieval model unproven, and the slice's shortcuts become the architecture.
- **Build the read path over SQLite first, then swap the store.** Lower risk per stage. Rejected: the read path would be written twice, and the first version's assumptions — whole entries, prune floors — would leak into the second.
- **Deliver everything at once.** One integration, one review. Rejected: unreviewable, and a failure anywhere blocks every capability.
- **Stage by subsystem in parallel.** Shorter wall time. Rejected: the seams, the gate and the read path are dependent, and parallel integration would decide the contracts by accident.
- **Gate the protocol work on a consumer count.** Rejected: the gap is an absent capability, and the protocol stages are planned here like every other stage.
- **Treat the cost goal as aspirational and ship the mechanism.** Rejected: the goal is the reason for the refactor, and a mechanism that misses it must be revised, not excused.

## Acceptance criteria

- The plan will contain all ten stages, each with its own merge criteria, and no stage will be marked optional or "future".
- No stage will merge a caller-visible behaviour change before the stage that serves it, and no later stage will be removed to make an earlier one ship.
- Every gate above will have a test or measurement that names its threshold and the stage that owns it.
- The evaluation will run on the complete feature, with a status-quo baseline measured in the same run.
- A missed gate will produce a recorded mechanism decision and a revised mechanism, not a reduced scope.
- The old-client conformance case will stay green through every stage.

## Risks

- **Long horizon.** Ten stages is a long commitment. Mitigation: each stage is independently mergeable and useful internally, and the stage boundaries are the review units.
- **A measured gate may fail on a synthetic corpus.** Mitigation: the harness accepts a real export as an additional corpus and reports both; a miss is a mechanism decision, not a scope cut.
- **Stage coupling.** A late discovery in S6 can force a change in S2 or S3. Mitigation: S1's contract suite and S2's rule vectors are the early place to discover it, and a contract change is a reviewed revision of the seam record.
- **Evaluation cost is real.** Running the harness on several buckets costs time and tokens. Mitigation: the baseline is measured per run rather than copied, and the harness reuses existing statement and provider-call instrumentation.
- **Protocol standardisation depends on a compatibility window.** Mitigation: S9 delivers value on its own; S10 is scheduled by the protocol's cohort, not by consumer demand, and remains in this plan.
- **The feature may be partially useful and temptation to stop.** Mitigation: the delivery record defines done as the complete lifecycle with the evaluation gates; a partial stop is a recorded decision by the owner, not a silent redefinition.
