# ADR: Derive the transfer capability from the bound service, not a manifest claim

Status: implemented — advertisement is derived from the service that dispatches the same operation list
Date: 2026-11-22
Class: architecture
Related: `2026-11-22-frozen-import-identities.md`, `2026-11-21-spoke-017-serving-cohort.md`

## Problem

Which `memory.*` operations the bound memory service serves was unknowable. `spoke-connect`'s `RemoteMemoryService` is an opaque trait with one `invoke` method, so the runtime could not ask a host's service whether it dispatches the five transfer operations. The first attempt let the host declare a capability list; review rejected it as a manifest claiming a capability the service might not implement. The second attempt removed the advertisement, which closed the false claim by making five implemented operations unreachable. Either way the manifest and the dispatcher were two independent statements that could disagree.

## Decision

The served operations are carried by a type this crate owns. `pub trait ServedMemory: RemoteMemoryService` adds `fn operations(&self) -> &'static [&'static str]`, defaulting to `&[]` (`crates/mindcore-spoke/src/serve.rs:35`). `TransferService` returns the five transfer constants plus §7.28 receipt polling from its own static (`crates/mindcore-spoke/src/serve.rs:20`), and `invoke` dispatches those same constants. `face_capabilities` advertises `spoke-memory-transfer` exactly when every op in `TRANSFER_OPERATIONS` — `transfer::OPERATIONS`, the five constants (`crates/mindcore-spoke/src/transfer/mod.rs:33`) — is reported (`crates/mindcore-runtime/src/lib.rs:342`). A bare upstream service takes the default and advertises nothing extra; a partial reporter advertises no transfer family.

## Alternatives considered

- **Advertise a host-declared capability list.** Rejected in review as the Critical finding: the manifest becomes a claim the bound service never proves, and a client plans against a capability that fails only at call time.
- **Remove the transfer advertisement.** Rejected: an honest manifest that loses five implemented operations trades a false claim for an unusable surface. The transfer family is exactly what a host installs the service for.
- **Inspect the concrete service at the runtime seam.** Not available: `RemoteMemoryService` is a pinned upstream trait with one `invoke`, and neither it nor any other `spoke-*` type may change. A crate-owned sub-trait is the smallest seam that can carry the answer.

## Consequences

- A surface with this crate's service advertises `spoke-memory-transfer` and serves the five operations over one loopback session; a surface with a bare `RemoteMemoryService` advertises `spoke-memory` only; a service reporting four of five advertises no transfer family (`crates/mindcore-runtime/tests/loopback.rs:1296,1331`; `crates/mindcore-runtime/tests/surface.rs:503-543`).
- The manifest derivation, the service's `operations()`, and the dispatcher all read the five `transfer::OPERATIONS` constants; no advertised op string is spelled a second time (`crates/mindcore-spoke/src/transfer/mod.rs:33`, `crates/mindcore-spoke/src/serve.rs:20`, `crates/mindcore-runtime/src/lib.rs:342`).
- Receipt polling rides the same service, so an unacknowledged transfer commit answers `OUTCOME_UNCERTAIN` with the settled receipt id in `details` and the client resolves it through `memory.receipts.get` (`crates/mindcore-spoke/src/serve.rs:145`).
- A new transfer operation joins two adjacent crate-owned lists: `transfer::OPERATIONS` is the
  advertisement gate, and `SERVED` (`crates/mindcore-spoke/src/serve.rs:20`) is what the service
  reports it dispatches, so the second list is not derived from the first and must be edited with
  it. Neither is a manifest edit.
