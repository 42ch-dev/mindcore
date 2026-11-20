# ADR: Executor-independent deadline timer for the workflow engine

Status: implemented — the deadline bounds provider futures without making the contract crate depend on a runtime
Date: 2026-11-20

## Problem

`SpiMindWorkflows` bounds each `deliberate` call by one request deadline, and the bound must interrupt the provider call itself: a hung reasoner, lens or policy must surface as `SpiError::DeadlineExceeded` instead of blocking the caller forever. A deadline needs a timer, while `mindcore-spi` is the runtime-agnostic contract crate an embedder hosts under whatever executor it chose. Awaiting a timer therefore cannot drag in a runtime this crate is not allowed to depend on.

## Decision

`mindcore-spi` depends on `futures-timer` and races each provider call against `futures_timer::Delay` inside a hand-rolled `poll_fn`: the provider future is polled first, the delay second, and whichever settles first is returned while the loser is dropped. The deadline itself is one `std::time::Instant`, clamped to a saturating maximum before the instant is computed, so the engine carries no runtime handle and no timeout can silently remove the bound.

## Alternatives considered

- **`tokio::time::sleep` / `tokio::select!`.** Rejected: it makes the capability boundary depend on the Tokio runtime and forces every embedder onto it, duplicating the executor choice the SPI exists to abstract.
- **`std::thread::sleep` polling loop.** Rejected: a sleeping or spinning poll occupies an executor thread and wakes on no timer edge, turning the deadline into a busy-wait rather than a bound.
- **Checking `Instant::now()` only between phases.** Rejected: a hung provider call would then never be interrupted; the deadline must bound the call, not only the boundaries between calls.

## Consequences

- `futures-timer` starts one process-global timer thread lazily on first use, shared by every engine instance in the process; creating a `Delay` after that is cheap and allocation-light.
- The race drops a still-running provider future at expiry. That is safe for reasoner, lens and policy calls because they hold no transaction and commit nothing; the one step never raced is a carrier write that already began (`commit_cognition`), whose actual or uncertain outcome is preserved instead.
- The dependency adds one `futures-*` family crate that carries no executor, so the contract surface stays runtime-free.
