# ADR: `mindcore-spi` — host-facing capability boundary on published SPOKE DTOs

Status: implemented — `crates/mindcore-spi` defines the embedder/reasoner/policy/lens/workflow contracts a host plugs into, bound on `spoke-schemas`/`spoke-operations` types only
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

1. **The boundary is a trait set the host implements, plus a capability bundle the host hands in.** `Embedder`, `Reasoner`, `PolicyHook`, `DomainLens` are traits a host provides; `MindCapabilities` is the borrowed `&dyn` bundle those traits are injected through; `MindWorkflows` is the trait the host calls once its `MindCapabilities` are wired. A host that embeds Mindcore supplies capabilities, not a storage engine.

2. **Bound types are published SPOKE DTOs.** `SettlementDraft`, `Decision`, `PolicyDecision`, `SpokeReject`/`Reject`, `ContextPack`, `AbsorbCandidateOutcome`, `DeliberateRequest`/`DeliberateOutcome` are `spoke-*` types, not Mindcore IR. The SPI speaks the same wire the rest of the system already carries — a host's `Embedder`/`Reasoner`/`PolicyHook` never sees `mindcore-base` types.

3. **`NoEmbeddings` is a first-class degrade path.** `Embedder` is optional in every workflow signature (`Option<&dyn Embedder>`); a host without embeddings supplies `NoEmbeddings`, and the lexical path is the contract, not a fallback.

4. **`PromptReasoner` bridges the reasoner without importing Nexus.** A host's reasoner is a `Fn(String) -> Future<Output=Result<String, SpiError>>` closure over whatever it already runs (ACP/`PromptExecutor` or any executor); `PromptReasoner` wraps full-text in the wire envelope and parses the whole reply as JSON — schema enforcement lives outside the bridge.

5. **`DefaultPolicy` fails closed.** `policy_hook: None` yields `DefaultPolicy`, which returns `RequireHumanReview` for every `SettlementDraft` — a host that injects no policy never silently auto-settles.

6. **`MindWorkflows` signatures name the v05 workflows.** `recall_context`, `absorb`, `deliberate` are declared at signature level so a host's capabilities type-check today; the workflow bodies land in v05 (`I-000030`–`I-000033`). `MockWorkflows` in the crate's tests exists only to prove the surface is object-safe and externally implementable — its bodies are stubs, and no test asserts on its behavior.

## Alternatives considered

- **Carry the storage IR (`mindcore-base` types) through the SPI.** Rejected: it couples every host adapter to Mindcore internals and forces the SPI to track every storage refactor. The SPOKE DTOs are the contract the rest of the system already speaks, and they survive a `mindcore-base` refactor without an SPI bump.
- **A third type layer between storage IR and SPOKE DTOs.** Rejected: it adds a mapping layer with no consumer yet; the SPOKE DTOs are already the stable, versioned surface every other side reads. The layer can be added later if the DTOs prove insufficient — the SPI's `SpiError::Settlement(SpokeReject)` already carries the typed reject, so the boundary isn't blocked on that choice.
- **Service/RPC seam instead of traits.** Rejected: the embedding host is in-process (Nexus guards the SQLite transaction through the borrowed-tx seam); a network seam adds serialization and lifecycle overhead for a boundary that is a Rust-type dependency anyway.
- **Non-object-safe traits / generic params.** Rejected: `MindCapabilities` carries `&dyn` trait objects so a host's capability bundle is runtime-injectable without generics infecting every workflow signature.
- **`policy_hook: None` → auto-settle.** Rejected: the conservative default is `RequireHumanReview` for every draft; a host that wants automation must inject a `PolicyHook` explicitly, so authorization is never the absent-injection path.

## Consequences

- **Host coupling is fixed at the trait + `MindCapabilities` level** — an external impl compiles and runs without storage crates (the `mock_adapter` conformance proof), and a host implementing `MindWorkflows` binds to the SPOKE DTO vocabulary, not Mindcore internals.
- **`mindcore-base` can refactor freely** so long as the SPOKE DTOs stay stable; the SPI surface does not move with it.
- **`NoEmbeddings` and `DefaultPolicy` are the two real shipped surfaces** — their degrade/fail-closed semantics are now part of the contract every host reads; a looser default in either is a breaking change.
- **v05 workflow signatures are pinned** — `recall_context`/`absorb`/`deliberate` land on `MindWorkflows` as declared; an implementation that needs different inputs forces an SPI revision, not a silent signature drift.
- **The DTO-vs-IR coupling is now on record** — a future `I-000043` withheld-read or remote-spoke change can cite this ADR when deciding whether the SPI binds DTOs or needs the extra type layer.
