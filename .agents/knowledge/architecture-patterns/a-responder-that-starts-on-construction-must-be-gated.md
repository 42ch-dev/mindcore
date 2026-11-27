# A responder that starts on construction must be gated, not raced

**Problem.** A process hands a transport to a Connect responder and then registers the tool handlers
on the returned handle. The responder object spawns its own task inside the constructor, before
returning: on a multi-threaded runtime that task can read the peer's first frame, negotiate a tool
and dispatch it while the registry is still empty, answering `op_unsupported` to a peer that did
nothing wrong.

**Why an ordering assertion does not fix it.** `let responder = connect(…); surface.register(&responder);`
reads as ordered, and a test that dials and then invokes a tool passes under **both** orderings — the
window is narrower than the test can reach. The test pins the guarantee, not the race.

**What fixes it.** Put the gate inside the transport the responder reads from, not in the caller's
ordering:

- A `GatedTransport` wraps the real transport. `recv()` awaits a gate before delegating; `send()` and
  `close()` delegate immediately, because the responder's hello reply is a `send` and a failed start
  must still be able to close.
- The gate is **three** states, not two: `Closed` (wait), `Open` (read), and `Rejected`. The third
  exists because a start can fail after the responder exists but before any handler is installed —
  a surface that refuses registration. Merely opening the gate there would release the read task
  into a transport the host will never write to, so it would park rather than end; `Rejected` maps
  `recv()` to `Closed`, which is the honest answer, and lets the connection task finish and untrack
  itself. `recv()` re-reads the state after waiting, so a rejection that lands as a `recv` parks is
  not lost.
- The connection task builds the gate, hands it to `connect_responder`, calls `register`, then opens
  the gate. No inbound frame is read before the handlers exist — by construction, for every future
  handler the daemon adds.
- Falsify it by removing the `open()` call: the signed-dialer test must fail with a hello timeout. A
  test that only invokes a tool after the handshake cannot distinguish the orderings.

**The same discipline for the socket name.** The name a process binds is a lifetime obligation:
guard it from `bind` to hand-over so a failure in between unlinks it, remove it in drain only after
its `dev`/`ino` still matches, and report an unremovable name in the refusal instead of exiting 0.
A `Drop`-only guard cannot report; make cleanup an explicit call whose error is merged into the
start failure, and keep `Drop` as the last resort.

**Evidence.** `crates/mindcore-daemon/src/transport.rs` (`GatedTransport`, `GateState`),
`src/serve.rs` (`BoundSocket`, `remove_own_socket`); tests `a_closed_gate_withholds_the_hello`,
`a_rejected_gate_ends_a_parked_recv`, `opening_the_gate_releases_the_recv_into_the_inner_transport`,
`a_signed_dialer_reads_a_stored_entry_over_the_socket` (fails with a hello timeout when the gate is
not opened), and `a_start_that_fails_after_the_bind_leaves_no_socket`.
