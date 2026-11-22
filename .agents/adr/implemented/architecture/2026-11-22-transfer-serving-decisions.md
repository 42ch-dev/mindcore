# ADR: Serve memory transfer honestly and freeze the import at staging

Status: implemented — advertisement is derived from the bound service, and activation applies an identity mapping fixed before the flip
Date: 2026-11-22
Class: architecture

## Problem

Two decisions made while implementing the §7.29–§7.33 transfer family had no record.

1. **Which operations the bound memory service serves was unknowable.** `spoke-connect`'s `RemoteMemoryService` is an opaque trait with one `invoke` method. The runtime therefore could not ask a host's service whether it dispatches the five transfer operations. The first attempt let the host declare a capability list; review rejected it as a manifest claiming a capability the service might not implement. The second attempt removed the advertisement, which closed the false claim by making five implemented operations unreachable. Either way the manifest and the dispatcher were two independent statements that could disagree.
2. **Activation would have to re-derive what staging already verified.** A staged bundle arrives as ordered chunks carrying source identifiers. If activation rebuilt the subject/record/candidate mapping and the translated references from the raw chunks, it would re-run verification on a sequence it had already verified, and the destination identity the caller saw in the staged report would not be the identity the flip wrote. Any re-derivation difference would surface only after the visibility flip, when it cannot be reported as a staging refusal.

## Decision

1. **The served operations are carried by a type this crate owns.** `pub trait ServedMemory: RemoteMemoryService` adds `fn operations(&self) -> &'static [&'static str]`, defaulting to `&[]` (`crates/mindcore-spoke/src/serve.rs:34`). `TransferService` returns the five transfer constants plus §7.28 receipt polling from its own static (`crates/mindcore-spoke/src/serve.rs:20`), and `invoke` dispatches those same constants. `face_capabilities` advertises `spoke-memory-transfer` exactly when every op in `TRANSFER_OPERATIONS` — `transfer::OPERATIONS`, the five constants, at `crates/mindcore-spoke/src/transfer/mod.rs:33` — is reported (`crates/mindcore-runtime/src/lib.rs:334`). A bare upstream service takes the default and advertises nothing extra; a partial reporter advertises no transfer family.
2. **Staging freezes the destinations; activation applies them.** The final staging chunk runs verification, builds the target identity mapping and the translated references, and writes both the translated payload and the report through `finish_import`, then deletes the raw source chunks (`crates/mindcore-spoke/src/transfer/stage.rs:652,665`; `crates/mindcore-sqlite/src/repository.rs:1626`). `activate_import` decodes that frozen payload (`decode_staged`, `crates/mindcore-spoke/src/transfer/activate.rs:688`), rechecks it against the live target, re-derives the caller's adoption cover, and flips visibility without re-reading the source sequence.

## Alternatives considered

- **Advertise a host-declared capability list.** Rejected in review as the Critical finding: the manifest becomes a claim the bound service never proves, and a client plans against a capability that fails only at call time.
- **Remove the transfer advertisement.** Rejected: an honest manifest that loses five implemented operations trades a false claim for an unusable surface. The transfer family is exactly what a host installs the service for.
- **Inspect the concrete service at the runtime seam.** Not available: `RemoteMemoryService` is a pinned upstream trait with one `invoke`, and neither it nor any other `spoke-*` type may change. A crate-owned sub-trait is the smallest seam that can carry the answer.
- **Keep the raw chunks and derive the mapping at activation.** Rejected: it repeats the whole-sequence verification after the caller has already been told the sequence verified, and the identity mapping the staged report publishes would not be the mapping the flip applies. A late failure would be an internal fault after staging instead of a staging refusal.
- **Store the raw chunks beside the frozen payload.** Rejected: the frozen payload is the only thing activation reads and expiry scrubs, so keeping the sequence would duplicate the transfer's largest artifact for no reader.

## Consequences

- A surface with this crate's service advertises `spoke-memory-transfer` and serves the five operations over one loopback session; a surface with a bare `RemoteMemoryService` advertises `spoke-memory` only; a service reporting four of five advertises no transfer family (`crates/mindcore-runtime/tests/loopback.rs:1296,1331`; `crates/mindcore-runtime/tests/surface.rs:503-543`).
- The manifest derivation, the service's `operations()`, and the dispatcher all read the five `transfer::OPERATIONS` constants; no advertised op string is spelled a second time (`crates/mindcore-spoke/src/transfer/mod.rs:33`, `crates/mindcore-spoke/src/serve.rs:20`, `crates/mindcore-runtime/src/lib.rs:342`).
- An unacknowledged commit answers `OUTCOME_UNCERTAIN` with the settled receipt id in `details`, so the client resolves it through the receipt read this same service routes (`crates/mindcore-spoke/src/serve.rs:145`).
- Staging pays one translation pass and one mapping build; activation's work is proportional to the frozen row set. An abandoned import expires after `STAGED_IMPORT_TTL_SECONDS`, and the sweep releases the frozen payload as well as any leftover chunk (`crates/mindcore-sqlite/src/repository.rs:1698`).
- Activation cannot apply a mapping the staged report did not publish: `decode_staged` accepts only the frozen rows and `recheck` refuses a target that moved under them (`crates/mindcore-spoke/src/transfer/activate.rs:688`).
- The frozen payload is trusted only because staging wrote it in the same store on the same transaction seam; a row it cannot decode is an internal fault, never a caller-visible refusal (`crates/mindcore-spoke/src/transfer/activate.rs:688`).
