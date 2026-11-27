# mindcore

[![CI](https://github.com/42ch-dev/mindcore/actions/workflows/ci.yml/badge.svg)](https://github.com/42ch-dev/mindcore/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust: stable](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org/)
[![crates.io](https://img.shields.io/crates/v/mindcore-base.svg)](https://crates.io/crates/mindcore-base)
[![Status: alpha](https://img.shields.io/badge/status-alpha-yellow.svg)](#status-and-limits)

**A Mind's memory, knowledge and cognition, kept in one embedded store and reached through plain calls.**

Mindcore is a Rust library for an application that needs a Mind: what an agent *heard*, what it
*adopted* as its own knowledge, and what it currently *judges* to be true stay apart, each in its
own record, and the provenance linking them survives every revision.

**English** | [简体中文](README.zh-CN.md)

## What a Mind is

A Mind is one memory space. Its id is the space's id — `mindcore-sqlite` keeps one registry and a
Mind has no second identity, so the registry row, the space and the cognition carrier cannot
disagree. `mindcore_runtime::minds` is the in-process entry: `create` writes the space, the
cognition carrier, the registry row and the owner's default grant in one transaction and leaves no
row on a refusal; `list` and `get` read the registry back under the caller's live grants.

| Record | Held as | Question it answers |
| --- | --- | --- |
| Memory | candidate → retained record | What did it hear or observe? |
| Knowledge | scoped `KnowledgeEntry` with revisions | What did it adopt for itself, and from where? |
| Cognition | append-only carrier records → `MindState` | What does it currently judge true, and on what basis? |

`mindcore-base` defines those record types and their invariants with no I/O; `mindcore-sqlite` is
the one implementation that persists them. A memory never becomes a personal belief by being read,
an adopted entry never silently changes because the shared source changed, and only an explicit
carrier update changes the current judgment. That separation is the product.

`mindcore_runtime::Mind` is the handle over one Mind: `open` is one registry read, and every
operation re-reads the live state on its own transaction — the handle caches no grant and confers
none. `capture`, `recall` and `review` compose the memory adapter; `knowledge_get`,
`knowledge_put` and `knowledge_list` compose the scoped store; `cognition_read` and `deliberate`
reach the current judgment and the workflows behind it. No method takes a collection, an owner or
a viewpoint, and none chooses a subject for you.

## Three ways to reach one

All three reach one kernel, so one authorization rule holds for each: live admission of the
(principal, Mind) pair before any target is read. The cross-process contract is SPOKE Connect, and
an HTTP API is not one of the forms.

| Form | Who it is for | Carried by | Status at this head |
| --- | --- | --- | --- |
| In-process Rust | a Rust host that links the library into its own process | `mindcore-runtime`: `minds::{create, list, get}` and the `Mind` handle | built in this repository, `publish = false` |
| In-process TypeScript | a Node host that links the addon into its own process | `crates/mindcore-binding` (napi `cdylib`), loaded by the `bindings/node` package (`MindcoreClient`, `MindView`, `MindcoreError`) | built from this repository; the package is `private`, not on npm |
| SPOKE peer | any SPOKE Connect peer | `mindcore-daemon` serves one over a Unix socket; a host that assembles `MindcoreServe` itself serves peers without it | built in this repository, `publish = false` |

The binding calls the facade in-process rather than dialling another process, and a TypeScript host
that connects to a daemon is using the SPOKE-peer form, not the TypeScript one. Every form reads
and writes the same SQLite store through `mindcore-sqlite`.

## Quickstart

```toml
[dependencies]
# The facade is unpublished, so the whole mindcore family comes from this repository at one
# revision: a registry `mindcore-base` beside a git `mindcore-runtime` would be a second copy of
# the same crate, and the facade would not accept its types.
mindcore-base = { git = "https://github.com/42ch-dev/mindcore" }
mindcore-sqlite = { git = "https://github.com/42ch-dev/mindcore" }
mindcore-runtime = { git = "https://github.com/42ch-dev/mindcore" }
spoke-operations = "0.19.1"
spoke-schemas = "0.19.1"
sqlx = { version = "0.9", default-features = false, features = ["runtime-tokio", "sqlite"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
serde_json = "1"
```

`mindcore-base`, `mindcore-spi` and `mindcore-sqlite` are on crates.io as the `0.1.0-alpha.1`
pre-release — an exact pin, because Cargo's `"0.1"` does not match a pre-release. The facade is
`publish = false`, so this example takes the whole family from this repository at one revision.

```rust
use mindcore_base::boundary::CallerId;
use mindcore_runtime::minds::{self, CreateMind};
use mindcore_runtime::{Capture, Mind};
use mindcore_sqlite::MindStore;
use serde_json::json;
use spoke_operations::SpokeResult;

/// The facade answers a refusal instead of panicking; a host maps it and keeps the code.
fn ok<T>(result: SpokeResult<T>) -> Result<T, Box<dyn std::error::Error>> {
    match result {
        SpokeResult::Ok(value) => Ok(value),
        SpokeResult::Reject(reject) => Err(format!("{}: {}", reject.code, reject.message).into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = MindStore::open(std::path::Path::new("mind.db")).await?;
    let caller = CallerId::new("caller_wren");

    let row = ok(minds::create(&store, &CreateMind {
        mind_id: None,
        display_name: Some("Wren".to_string()),
        owner: caller.clone(),
    }).await)?;
    let mind = ok(Mind::open(&store, &caller, &row.id).await)?;

    // The host provisions the subject row a memory write needs; `capture` answers NOT_FOUND for
    // an unknown subject.
    sqlx::query(
        "INSERT INTO memory_subjects (space_id, subject_id, status, revision, created_at) \
         VALUES (?, ?, 'active', 1, '2026-01-01T00:00:00.000Z')",
    )
    .bind(row.id.as_str())
    .bind("wren")
    .execute(&store.pool())
    .await?;

    ok(mind.capture(&Capture {
        subject: "wren",
        context: None,
        content: &json!({ "text": "the north gate was closed at dusk" }),
        occurred_at: None,
        kind_hint: None,
        idempotency_key: "capture-north-gate",
    }).await)?;

    Ok(())
}
```

The full story — recall, review of a staged candidate, adoption from a shared collection, doubt,
evidence, a shared change, a reopen and the refusals — is [docs/quickstart.md](docs/quickstart.md),
and the same sequence is asserted by the workspace example:

```sh
cargo run --locked -p mindcore-sqlite --example iteration_01
```

The record model, revision rules and provenance are in [docs/concepts.md](docs/concepts.md).

## Serving a peer

`mindcore-daemon` is the reference host for the peer form: one process, one store file, one Unix
socket. Its TOML config names the store, the socket, the responder's seed file, the journal
directory, the admitted peers (each `public_key` mapped to a `principal`) and the one adoption
route; every table denies unknown keys, so a typo is a start failure rather than a silently
ignored setting. It takes a single-instance lock before it opens the store — so one database file
is served by one process — and on `SIGTERM` or `SIGINT` it drains instead of dropping a live
responder. A second instance on the same store exits with code 2.

```sh
cargo run --locked -p mindcore-daemon -- --config daemon.toml
```

What it serves is `mindcore-runtime`'s. `MindcoreServe` composes the workflow engine, the routed
knowledge face, the host's memory service and the optional history and adoption faces into a
`MindcoreSurface`: the ports, a manifest and the handlers a `ConnectResponder` needs. The
advertised `capabilities` and `tools` are derived from what is actually served, never from a
host-supplied string, and the runtime refuses to invent what only a host can decide — transport,
identity, admission and the journal publisher. An embedding host that supplies the same builds its
own responder and needs no daemon.

The obligations the host takes, the `ServeFaces` fields and the served-operation list are in
[docs/serving.md](docs/serving.md).

## Storage and guarantees

- **One embedded store.** `mindcore-sqlite` opens the SQLite file through `MindStore::open` over
  one connection pool and one embedded migration, with WAL, foreign keys and a busy timeout.
- **Idempotent writes.** A repeated idempotency key re-answers the recorded outcome instead of
  repeating the effect, and a replay reads the receipt rather than re-running the operation.
- **Conditional revisions.** `expected_base_revision` makes every knowledge update a
  compare-and-swap, and a refusal leaves the row untouched.
- **Provenance that resolves.** An adopted copy anchors to its source scope, entry and revision and
  resolves through the source's retirement chain, so it names the live terminal rather than a
  retired hop.
- **Immutable migrations.** A published migration file never changes — the lock records its
  sha256 — and new schema arrives as appended files. A database whose applied migrations still
  match the embedded set reopens; anything else is refused rather than converted.

What `open` accepts and refuses is [storage compatibility](docs/storage-compatibility.md); the
version policy behind it is [SemVer](docs/semver.md).

## Status and limits

- **Alpha, and mostly unpublished.** `mindcore-base`, `mindcore-spi` and `mindcore-sqlite` are on
  crates.io; the registry currently holds an alpha. `mindcore-spoke`, `mindcore-runtime`,
  `mindcore-daemon` and `mindcore-binding` are `publish = false` and come from this repository.
- **Mind-level only.** The facade and the served surface expose Mind-level operations. The
  space-level capabilities beneath them are an advanced surface that is not exposed, and the
  operations not yet served per Mind are not composed.
- **No HTTP surface.** This repository contains no HTTP listener, route or request type; the
  cross-process contract is SPOKE Connect.
- **No data movement.** Moving an existing store or provider into Mindcore is not implemented, and
  there is no upgrade tool.
- **One store per database file.** Each opened `MindStore` owns one pool, and two stores on one
  file are unsupported; the daemon's lock is how one process keeps that.
- **The addon is built, not shipped.** `bindings/node` loads `libmindcore_binding` from
  `target/{debug,release}` or from the path `MINDCORE_BINDING_PATH` names; build it with
  `cargo build -p mindcore-binding`.

## Contributing / Changelog

Contributor conventions and the local checks are in [CONTRIBUTING.md](CONTRIBUTING.md); release
history is in [CHANGELOG.md](CHANGELOG.md). Working in this repository rather than embedding the
library is [docs/for-agents.md](docs/for-agents.md), and the publishing steps are
[docs/releasing.md](docs/releasing.md).
