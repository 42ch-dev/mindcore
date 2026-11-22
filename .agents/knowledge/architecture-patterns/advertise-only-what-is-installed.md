# Advertise only what is installed

## Problem

A served surface derives its signed manifest from configuration. The cheapest derivation trusts the host's claim: pass the capabilities the host says it wants, advertise them, and let dispatch refuse what is missing. A client that reads the manifest then plans against a capability the server cannot deliver, and learns that only at call time — usually as a generic refusal that looks like a bug.

Mindcore hit this adding memory transfer: `spoke-connect`'s `RemoteMemoryService` is an opaque trait with one `invoke` method, so the runtime could not ask a host's service which operations it implements. A caller-declared operation list was the obvious shape and was rejected in review as the Critical finding: a manifest that claims a capability the bound service does not implement is worse than a missing capability.

## Pattern (as implemented in `transfer-surface`)

1. **Carry the answer on a type this crate owns.** Declare a sub-trait with one method that reports the served operations, defaulting to empty: `pub trait ServedMemory: RemoteMemoryService { fn operations(&self) -> &'static [&'static str] { &[] } }`. A host's bare upstream trait keeps working and advertises nothing extra, because the default is honest.
2. **The crate's own implementation reports itself.** The transfer service's `operations()` names the five transfer operation constants plus receipt polling from its own static; the manifest derivation checks those same five constants (`TRANSFER_OPERATIONS`), and `invoke` dispatches the same constants, so no advertised op string is spelled twice.
3. **Derive, do not accept.** `face_capabilities` advertises a family exactly when every one of its op strings is present in `operations()`. Partial coverage advertises nothing from that family, never a subset.
4. **The implementation carries its own transaction story.** `invoke` has no transaction parameter, so the crate-owned service takes the host's pool, begins one transaction per invocation, and commits only on a staged success. Rejects roll back by drop; an unacknowledged commit answers `OUTCOME_UNCERTAIN` and is resolvable through the receipt read the same service routes.

## Why it works

The manifest cannot drift from the implementation because both read the same operation constants, and the drift that would otherwise appear is turned into a compile-time or assembly-time question: a service that does not implement the sub-trait advertises nothing from that family, and a family reported incompletely advertises nothing at all.

## Traps

- **A caller-declared list is not a derivation.** It moves the correctness burden to every host and makes a wrong manifest indistinguishable from an honest one.
- **Removing the advertisement entirely is not honesty.** The first fix deleted the capability, which closed the Critical by making five implemented operations unreachable. The correct fix makes honesty possible, then restores the capability.
- **Terminal states are terminal.** When an operation has a persisted state machine, decide expiry and reachability from the persisted state, never from a wall-clock comparison: a clock rollback misreports a dead import as live, and a deadline that has passed does not un-activate what was activated.
- **One derivation, one place.** A second advertisement list beside the first is the drift you just prevented, relocated.
