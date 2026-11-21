# ADR: `mindcore-spi` — host-facing capability boundary on published SPOKE DTOs

Status: implemented — `crates/mindcore-spi` defines the embedder/reasoner/policy/lens/workflow contracts a host plugs into, bound on `spoke-schemas`/`spoke-operations` types where they exist plus six SPI-owned types for the gaps
Date: 2026-11-20
Class: architecture
Related: `2026-11-20-borrowed-transaction-seam.md`, `2026-11-20-embedded-sqlite-storage.md`

## Problem

Iter004 opened the MIND ladder: Mindcore needs a seam through which an embedding host supplies cognition capabilities (embedder, reasoner, settlement policy, domain lens) and invokes the workflows built on top (`recall_context`, `absorb`, `deliberate`). Two decisions had to be fixed before any workflow code lands:

1. **What the boundary is.** Is the SPI a set of Rust traits the host implements, a service interface it calls into, or a data contract both sides exchange? Each choice fixes a different coupling for every downstream workflow.
2. **What types the boundary carries.** The trait signatures have to name concrete types — either the storage-layer IR (`mindcore-base` types, which are Mindcore-owned and internal) or the published SPOKE DTOs (`spoke-schemas`/`spoke-operations`), or a third, new type layer in between. Whatever is chosen becomes a permanent API surface that every host adapter and every workflow signature shares.

Getting this wrong re-opens the same problem the borrowed-transaction seam solved: a host that can't plug in without depending on Mindcore internals, or a type surface that drifts from the SPOKE contract the rest of the system already speaks.

## Decision

`crates/mindcore-spi` is a contract crate: it declares traits, error and capability types, and workflow signatures, and carries **only** `spoke-schemas`/`spoke-operations` `=0.16.1` types plus serde/async plumbing. It has no storage-crate dependency, no `mindcore-base`/`mindcore-sqlite`/`mindcore-spoke` dep, and no production workflow implementation.

> **Superseded-by (2026-11-21, `2026-11-21-knowledge-lifecycle.md`):** the `mindcore-base` part of that clause no longer holds. The contract crate depends on `mindcore-base` for the adoption-anchor codec, because adoption is a domain-level contract operation and a second codec inside the SPI would be two parsers for one wire shape. The `=0.16.1` pin no longer holds either: the workspace resolves `spoke-schemas`/`spoke-operations` `=0.17.0` (see `2026-11-21-spoke-017-serving-cohort.md`). The rest of the clause stands: the SPI carries no storage-crate dependency, and it hosts no production **workflow** implementation. What lives in it is a domain-level contract operation over types the host already passes in (`adopt_knowledge_entry` takes two ports and never opens a transaction); a `MindWorkflows` implementation or a storage engine would still violate the boundary.

1. **The boundary is a trait set the host implements, plus a capability bundle the host hands in.** `Embedder`, `Reasoner`, `PolicyHook`, `DomainLens` are traits a host provides; `MindCapabilities` is the borrowed `&dyn` bundle those traits are injected through; `MindWorkflows` is the trait the host calls once its `MindCapabilities` are wired. A host that embeds Mindcore supplies capabilities, not a storage engine.

2. **Bound types are published SPOKE DTOs where they exist; SPI-owned types fill the gaps.** `Decision`, `SpokeReject`, `CognitionRecord`, `MemoryCandidate` and the other wire types are `spoke-*`; `SettlementDraft`, `PolicyDecision`, `ContextPack`, `AbsorbCandidateOutcome`, `DeliberateRequest` and `DeliberateOutcome` are declared inside `mindcore-spi` itself because no SPOKE DTO covers them yet. That local declaration *is* the type-layer option (see Alternatives): it is not a deferred third mapping layer, it is the decision — the SPI owns the types no DTO covers, so a host's `Embedder`/`Reasoner`/`PolicyHook` never sees `mindcore-base` IR.

3. **`NoEmbeddings` is a first-class degrade path.** Embedding is optional via `MindCapabilities.embedder: Option<&dyn Embedder>` — a host without embeddings supplies `NoEmbeddings` (or simply `embedder: None`), and the lexical path is the contract, not a fallback. Workflow signatures take `caps: &MindCapabilities<'_>`; no `MindWorkflows` method takes `Option<&dyn Embedder>` directly.

4. **`PromptReasoner` bridges the reasoner without importing Nexus.** A host's reasoner is a `Fn(String) -> Future<Output=Result<String, SpiError>>` closure over whatever it already runs (ACP/`PromptExecutor` or any executor); `PromptReasoner` wraps full-text in the wire envelope and parses the whole reply as JSON — schema enforcement lives outside the bridge.

5. **`DefaultPolicy` is the intended fail-closed invariant.** `DefaultPolicy` returns `RequireHumanReview` for every `SettlementDraft` — the conservative decision a host that injects no policy must get. Today the SPI crate exposes `DefaultPolicy` and `MindCapabilities.policy_hook: Option<&dyn PolicyHook>` separately; the v05 workflow implementations will map `None` → `DefaultPolicy` at composition (documented invariant, not yet a shipped code path).

6. **`MindWorkflows` signatures name the v05 workflows.** `recall_context`, `absorb`, `deliberate` are declared at signature level so a host's capabilities type-check today; the workflow bodies land in v05 (`I-000030`–`I-000033`). `MockWorkflows` in the crate's tests exists only to prove the surface is object-safe and externally implementable — its bodies are stubs, and no test asserts on its behavior.

## Alternatives considered

- **A third type layer between storage IR and SPOKE DTOs.** Accepted in the narrowed form: the six wire types no SPOKE DTO covers are declared inside `mindcore-spi` rather than carried by a future mapping layer, because there is no external consumer for them yet and a separate layer would only rename them. The `spi-*` DTOs can be promoted to `spoke-*` later if another surface needs them; the boundary's `SpiError::Settlement(SpokeReject)` already carries the typed reject, so that promotion is a rename, not a contract change.
- **Service/RPC seam instead of traits.** Rejected: the embedding host is in-process (Nexus guards the SQLite transaction through the borrowed-tx seam); a network seam adds serialization and lifecycle overhead for a boundary that is a Rust-type dependency anyway.
- **Non-object-safe traits / generic params.** Rejected: `MindCapabilities` carries `&dyn` trait objects so a host's capability bundle is runtime-injectable without generics infecting every workflow signature.
- **`policy_hook: None` → auto-settle.** Rejected: the conservative default is `RequireHumanReview` for every draft; a host that wants automation must inject a `PolicyHook` explicitly, so authorization is never the absent-injection path.

## Consequences

- **`PromptReasoner`, `NoEmbeddings` and `DefaultPolicy` are the behavior-bearing implementations shipped in this crate** — `PromptReasoner` builds the wire envelope, makes exactly one prompt call, and parses the whole reply as JSON (malformed text → `InvalidReasonerOutput`); `NoEmbeddings` reports `SpiError::EmbeddingsUnavailable` (which selects the lexical degrade at the workflow layer); `DefaultPolicy` is the conservative decision the v05 composition is intended to apply when `policy_hook` is `None`. A looser default in any of them is a breaking change; `MindCapabilities`/`MindWorkflows` remain the contract surface.
- **`mindcore-base` can refactor freely** so long as the SPOKE DTOs stay stable; the SPI surface does not move with it.
- **v05 workflow signatures are pinned** — `recall_context`/`absorb`/`deliberate` land on `MindWorkflows` as declared; an implementation that needs different inputs forces an SPI revision, not a silent signature drift.
- **The DTO-vs-IR coupling is now on record** — a future `I-000043` withheld-read or remote-spoke change can cite this ADR when deciding whether the SPI binds DTOs or needs the extra type layer.
