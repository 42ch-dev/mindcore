# mindcore

[![CI](https://github.com/42ch-dev/mindcore/actions/workflows/ci.yml/badge.svg)](https://github.com/42ch-dev/mindcore/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust: stable](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org/)
[![Status: pre-release](https://img.shields.io/badge/status-pre--release-yellow.svg)](#status-and-limits)

**A settlement layer for memory, knowledge and cognition.**

Mindcore is an embedded Rust library that keeps three things apart: what an agent *heard*, what
it *adopted* as personal knowledge, and what it currently *judges* to be true. Each lands in its
own record, and the provenance linking them survives every revision.

No server, no UI, no transport ships here. A host embeds the store and exposes whatever surface
it wants; `mindcore-runtime` only composes the pieces a SPOKE Connect responder is built from.

> [!NOTE]
> Pre-release. Every crate is `publish = false`, the schema is still mutable between commits,
> and there is no upgrade path yet. Embed from this repository, not from a registry.

**English** | [简体中文](README.zh-CN.md)

## Why "settlement"

| Layer | Record | Question it answers |
| --- | --- | --- |
| Memory | candidate → retained record | What did the agent hear or observe? |
| Knowledge | scoped `KnowledgeEntry` with revisions | What did it adopt for itself, and from where? |
| Cognition | carrier records → `MindState` | What does it currently judge true, and on what basis? |

A memory never becomes a personal belief by being read. An adopted entry never silently changes
because the shared source changed. Only an explicit carrier update changes the current judgment.
That separation is the product.

## Architecture

![Mindcore layers: a host embeds mindcore-runtime, which composes the SPI workflows, the SPOKE adapter and the SQLite store](docs/assets/architecture.svg)

The host owns transport, identity, admission and the store path. Mindcore owns the record model,
the revision and replay rules, the protocol adapter, and the persistence boundary.

## What you get

- **A memory lifecycle with replay.** `capture` stages a candidate; review projects one record,
  several fragments or none, and every mutation is idempotent: a repeated key re-answers the
  recorded outcome instead of repeating the effect.
- **Scoped knowledge with conditional revisions.** Every write binds the collection when the port
  is built; `expected_base_revision` makes each update compare-and-swap, and refusals leave the
  row untouched.
- **Adoption with provenance.** An adopted copy carries an anchor to its source scope, entry and
  revision. The anchor resolves through the source's retirement chain, so a copy names the live
  terminal rather than a retired hop.
- **A cognition carrier.** Current judgment is an append-only record set under one holder entry,
  guarded at the single persistence boundary; `MindState` is derived from it, never stored twice.
- **A served surface that advertises what it serves.** `MindcoreServe` derives capabilities and
  tools from the composed faces and registrations, not from a host-supplied string.
- **One embedded store.** SQLite with WAL, foreign keys and a busy timeout, opened by
  `MindStore::open` over a single connection pool and one embedded migration.

## Crates

| Crate | Responsibility |
| --- | --- |
| `mindcore-base` | Storage-agnostic domain core: ids and trusted boundary, memory protocol types and IR, knowledge scope and adoption provenance, the cognition carrier and `MindState` derivation. No I/O. |
| `mindcore-spi` | The MIND ladder contracts, pinned to the published SPOKE DTOs: recall, absorb and deliberate workflows plus the prompt-reasoner bridge. No storage crate, network or model SDK in the contract surface. |
| `mindcore-sqlite` | SQLite (SQLx) backend: one connection pool, one migration, WAL and a busy timeout; implements the protocol repository behind `MemoryAdapter` and the scoped `KnowledgeEntryPort`, and guards the cognition module at the single persistence boundary. |
| `mindcore-spoke` | SPOKE memory protocol adapter over the borrowed-transaction repository: wire validation, the wire↔IR boundary, reject mapping, admission, replay and staging. It never opens, commits or rolls back a transaction. |
| `mindcore-runtime` | The served surface: `MindcoreServe` assembles the workflow engine, the SQLite knowledge face and the host's memory face into the pieces a Connect responder is built from, and derives the advertised capabilities and tools from what it serves. |

## Quick start

Add the crates from this repository (they are not published yet):

```toml
[dependencies]
mindcore-base = { git = "https://github.com/42ch-dev/mindcore" }
mindcore-sqlite = { git = "https://github.com/42ch-dev/mindcore" }
spoke-operations = "0.18"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Open a store and read through a collection-bound port:

```rust
use mindcore_base::knowledge::KnowledgeScope;
use mindcore_sqlite::MindStore;
use spoke_operations::{KnowledgeEntryPort, SpokeResult};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = MindStore::open(std::path::Path::new("mind.db")).await?;
    let wren = store.knowledge(KnowledgeScope::new("wren"));

    match wren.get_knowledge_entry("ke_north_gate").await {
        SpokeResult::Ok(entry) => println!("entry at revision {:?}", entry.revision),
        SpokeResult::Reject(reject) => println!("{}: {}", reject.code, reject.message),
    }
    Ok(())
}
```

Then run the full first-mind story: hearing, adoption, doubt, evidence, a shared change, reopen
and refusals, with every step asserted.

```sh
cargo run --locked -p mindcore-sqlite --example iteration_01
```

The story is the same code the integration test runs. For the guided version, start with
[docs/quickstart.md](docs/quickstart.md).

## Documentation

| Document | Read it when |
| --- | --- |
| [Quick start](docs/quickstart.md) | You want a store, a captured memory and a derived `MindState` running locally. |
| [Concepts](docs/concepts.md) | You need the record model, revision rules and provenance before designing on it. |
| [Serving a surface](docs/serving.md) | You are wiring `MindcoreServe` into a SPOKE Connect responder. |
| [Migrating from `tools.mindcore.adopt`](docs/migrating-from-tools-adopt.md) | You are moving an existing host to `knowledge.adopt`, or reopening an old database. |
| [For agents](docs/for-agents.md) | You are working in this repository rather than embedding the library. |

## Status and limits

- **Embedded only.** `mindcore-runtime` opens no listener, transport or identity; the host
  supplies them.
- **No data movement yet.** Moving an existing store or provider into Mindcore is not
  implemented, and no migration path exists before the first release.
- **One store per database file.** Each opened `MindStore` owns one pool; two stores on one file
  are unsupported.
- **Mutable pre-release schema.** A database whose applied migrations still match the embedded
  set reopens normally. Otherwise `open` refuses it; see
  [storage compatibility](docs/migrating-from-tools-adopt.md#storage-compatibility).

Repository checks: CI runs formatting, clippy and the full test suite, and additionally verifies the
`.sqlx` offline cache. The exact local command block lives in
[docs/for-agents.md](docs/for-agents.md); contributor conventions live in
[CONTRIBUTING.md](CONTRIBUTING.md).
