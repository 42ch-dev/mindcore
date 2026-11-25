# ADR: Three delivery forms, and the daemon that carries the SPOKE-peer form

Status: proposed — the first release serves three delivery forms and no HTTP API; the owner settled it on 2026-11-25, and of the three only the embedding path exists
Date: 2026-11-25
Class: architecture
Related: `2026-11-21-spoke-017-serving-cohort.md`, `2026-11-24-release-pipeline.md`, `2026-11-20-embedded-sqlite-storage.md`, `2026-11-25-mind-is-one-memory-space.md`, `docs/serving.md`

## Problem

The owner's target is that every application which uses this repository can take on a Mind's memory, knowledge and cognition storage and read it through simple calls. One way in exists today, and it is not simple: a process embeds the library and assembles each operation itself, opening a transaction, building a `Principal`, shaping a wire request body and matching a `MemoryStage` (`docs/quickstart.md`). The workspace has five library crates and no binary (`Cargo.toml`), so no runnable process exists. It has no binding crate, so a TypeScript process cannot link the library. A SPOKE peer reaches what the served surface offers at assembly: six transfer and receipt memory operations (`crates/mindcore-spoke/src/serve.rs`, `SERVED`; every other `memory.*` operation answers `CAPABILITY_PORT_MISSING`), three workflow tools (`crates/mindcore-runtime/src/tools.rs`) and the knowledge, history and adoption faces of one collection.

Without a decision each downstream picks its own way in. The peer form has no process to carry it, so the planned sidecar daemon grows into whatever its first caller asks for, an HTTP API included. A TypeScript caller, having no binding, is answered with that API. And the documents keep describing the parts: `README.md` opens with "No server, no UI, no transport ships here".

## Proposal

The first release serves three delivery forms.

1. **A process embedding the Rust library.** The host embeds the workspace as it does today and reaches memory, knowledge and cognition through an in-process facade over the existing adapter, ports and workflows, by way of a Mind handle. The facade is `crates/mindcore-runtime`: that crate is the library's entry crate and holds the in-process entry points beside the served surface; no crate is added and none is renamed (owner, 2026-11-25). Its entry points `create`, `list` and `get` are built (`mindcore_runtime::minds`); the Mind handle and the memory, knowledge and cognition operations behind it are the facade's remaining work. The facade adds no second write path, moves no validation point, and a Mind handle never widens a principal's grants. Its contract spec is written with the facade.
2. **A TypeScript host through a native binding library.** A native binding wraps the in-process facade, so a TypeScript process links the library into its own process and is not a client of another process.
3. **A SPOKE peer.** Any SPOKE Connect peer operates a Mind through standard SPOKE operations. A SPOKE interface change was proposed for SPOKE-side evaluation (`https://github.com/42ch-dev/spoke/issues/222`); the owner stated on 2026-11-25 that SPOKE settled it and that SPOKE 0.19.0 will carry the three items; what a peer gets without it, and in which stages, is `2026-11-25-one-surface-serves-many-minds.md`.

The three forms reach one kernel, so one authorization rule — live admission of the (principal, Mind) pair before any target is read — holds for each of them.

**What the first release exposes.** Mind-level convenience operations only. The space-level capabilities beneath the facade are an advanced surface that may be exposed later. It also composes none of the operations that are not Mind-routed: until each of `port.knowledge.*`, `history.*` and `knowledge.adopt` is served per Mind, the surface does not compose it, because every operation is to hang off the Mind entity (owner, 2026-11-25).

**An HTTP API is not a delivery form.** The first release ships no HTTP surface over the library: no HTTP listener, route or request type.

**What the sidecar daemon becomes.** The daemon is one process that embeds the Rust library. It is not a fourth form, and no other form passes through it.

- Under the SPOKE-peer form the daemon is the sidecar: it is the process that carries the form. It assembles one served surface with `MindcoreServe::surface` and hands the ports, manifest and handlers to a `ConnectResponder`, which it serves over a Unix socket. The socket is this record's decision for the daemon, not something `docs/serving.md` describes: the runtime opens no transport, and that page leaves it to the host. It is the packaged host of this form, not the only one: an embedding host that assembles its own surface serves peers without it, as `docs/serving.md` describes today.
- Under the embedding form the daemon is one such process and the reference host. Every obligation `docs/serving.md` leaves to the host is the daemon's — transport, identity and peer keys, the peer-to-`Principal` and peer-to-`Scope` admission, the journal publisher, the workflow providers and the manifest metadata — and it loads them from a config file.
- Under the TypeScript form the daemon is not on the path. A TypeScript host that connects to the daemon is using the SPOKE-peer form.
- Its single-instance lock is how one daemon keeps the one-store-per-file obligation (`2026-11-20-embedded-sqlite-storage.md`) and the one-serving-process-per-database obligation (`.agents/specs/architecture/serving-seam.md`). `MindStore::open` is unchanged and still does not detect a second store.

`crates/mindcore-runtime` stays a library crate: it opens no listener and owns no accept loop. The daemon is the host that `2026-11-21-spoke-017-serving-cohort.md` names, and the transport lives in the daemon binary. That record keeps its decision; its rejected "standalone serving binary" alternative gains one sentence of history saying so.

**Pages this changes.** `README.md` and `docs/quickstart.md` lead with the Mind path instead of the parts. `docs/serving.md` names the daemon as the host that takes the obligations it lists. If the facade's crate or the binding is published, `2026-11-24-release-pipeline.md` changes with it. The daemon's crate layout and the operations its first release serves belong to a spec that does not exist yet.

## Alternatives considered

- **Sidecar-only delivery: every caller goes through the daemon.** Rejected: it retires the embedding path the workspace serves today (`docs/quickstart.md`, `docs/serving.md`) and puts a process boundary and a second deployment unit in front of every caller. A TypeScript host able to link the library in-process would be made a client of another process for nothing.
- **Embedded-only delivery: the embedding path alone.** Rejected: a caller that cannot link Rust is left out, and every other host language needs a binding of its own before it can create a Mind. The served surface already delivered by `2026-11-21-spoke-017-serving-cohort.md` would have no product form.
- **An HTTP API over the library.** Rejected: the cross-process contract is already SPOKE Connect, so an HTTP API would be a second operation surface with its own request types, admission and refusal table. The serving seam removed one such second authority when it retired the private adoption tool in favour of `knowledge.adopt` (`.agents/specs/architecture/serving-seam.md`), and an HTTP API brings one back. The three settled forms keep every cross-process call on the one contract.

## Acceptance criteria

- An in-process caller reaches memory, knowledge and cognition through a Mind handle without opening a transaction, building a `Principal`, supplying a wire-shaped request body or matching `MemoryStage`.
- The facade's first release exposes Mind-level convenience operations only; the space-level capabilities beneath it are not exposed before that advanced surface is decided.
- A TypeScript host creates a Mind, lists Minds and operates memory, knowledge and cognition through the native binding. A refusal on the facade surfaces through the binding with its code and details intact, and a Node integration check in the workspace runs the facade's quickstart path through the binding.
- A SPOKE peer lists the Minds it is admitted to and operates one, as the acceptance criteria of `2026-11-25-one-surface-serves-many-minds.md` state.
- A `mindcore-daemon` binary loads a config file, takes a single-instance lock, serves a `ConnectResponder` over its transport, and exits cleanly on `SIGTERM`; an integration test drives one hello and one invoke. Which knowledge operations its first release serves follows the daemon spec and the settled composition rule recorded in `2026-11-25-one-surface-serves-many-minds.md`: the surface composes none of the operations that are not Mind-routed until each is served per Mind.
- `crates/mindcore-runtime` remains a library crate with no listener and no accept loop.
- The repository contains no HTTP surface over the library: no HTTP listener, route or request type.
- `README.md` and `docs/quickstart.md` lead with the Mind path, and the facade, the binding and the crates they need are consumable by a downstream.

## Risks

- **None of the three forms exists in full.** At the decision the facade, the binding and the daemon binary are unbuilt. A first release that claims all three owes all three.
- **The SPOKE-side items wait for a cohort this repository does not yet pin.** SPOKE 0.19.0 will carry them (owner, 2026-11-25); until that cohort is pinned here, a peer gets per-Mind memory and cognition only (`2026-11-25-one-surface-serves-many-minds.md`).
- **Settled for the two crates, open for the binding: how a downstream consumes the facade and the binding.** `mindcore-runtime` and `mindcore-spoke` are published to crates.io (owner, 2026-11-25); today both are `publish = false` (`crates/mindcore-spoke/Cargo.toml`, `crates/mindcore-runtime/Cargo.toml`; `2026-11-24-release-pipeline.md`), so until that lands an embedding downstream reaches them from this repository only. How the binding is packaged is decided with the binding.
- **Undecided: which tool builds the binding,** and how it is packaged for a TypeScript host. The owner fixed a native binding library and nothing about its toolchain.
- **The daemon's boundary is fixed here only in position and transport.** Its crate layout, config file format and admission model are not written down.
- **The daemon is one more artifact to release and to operate,** and it takes in configuration what an embedding host writes for itself: identity, admission, a journal publisher and workflow providers.
- **No release before SPOKE 0.19.0.** Changes merge to `main`; the first Mind-centric release follows SPOKE 0.19.0 and ships the served surface's two stages as one minor (owner, 2026-11-25).
