# ADR: Socket teardown shuts the write side before the read side
Status: implemented — `shutdown_socket` issues the two calls in this order under one guard.
Date: 2026-11-28

## Problem

Tearing a SPOKE connection down must tell the peer the stream ended and make a parked `write_all`
fail, including when the peer has already half-closed. A single `SHUT_RDWR` after a peer half-close
answers `ENOTCONN` and delivers no EOF, so the one call that was supposed to do both can silently
do neither.

## Decision

`shutdown_socket` issues `SHUT_WR` first, then `SHUT_RD`, each once, guarded by a one-shot flag.
The write step is the one that must never be skipped: it signals the end of the stream and wakes a
parked writer. A `SHUT_RD` refusal after it cannot undo that.

## Alternatives considered

- **One `SHUT_RDWR` call.** Rejected: it fails as a whole on a peer half-close, losing both the
  EOF signal and the parked writer's wake-up.
- **`SHUT_RD` then `SHUT_WR`.** Rejected: the parked writer keeps waiting until the second call,
  and an `SHUT_RD` refusal is indistinguishable from success.
- **Drop the socket halves instead of shutting down.** Rejected: the peer loses the EOF signal and
  the transport loses the distinction between an orderly end and a framing fault.

## Consequences

The two libc results are ignored after the first; the one-shot flag keeps a concurrent close from
repeating the sequence. The raw-descriptor call carries its own safety justification at the site.
