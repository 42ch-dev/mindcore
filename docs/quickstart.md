# Quick start

This tutorial takes you from an empty directory to a derived `MindState`. The short path is the
facade — one handle whose operations compose everything below — and the sections after it show
what it composes, for a host that needs the manual transaction-level control. It is a guided
path, not a complete API reference.

## What you will do

1. Create a Mind and open its handle.
2. Capture a memory, review it and recall the settled record.
3. Write the carrier with one cognition record and read the derived `MindState`.
4. See the transaction-level assembly the facade performs for you.

## Prerequisites

- A stable Rust toolchain with edition 2021 support.
- A system SQLite library the linker can find. The workspace does not enable `sqlx`'s `bundled`
  feature, so `libsqlite3-sys` links against the system library.
- `sqlite3` CLI and `sqlx-cli` only if you also run the repository checks.

## The facade path: one Mind end to end

The facade is `mindcore_runtime::Mind` — the library's in-process handle over one Mind. You never
open a transaction, shape a wire request or settle a stage yourself: each method composes the
operation for you and admits the caller under its live grants. The complete documented example
is [`crates/mindcore-runtime/examples/mind_facade.rs`](../crates/mindcore-runtime/examples/mind_facade.rs);
run it from a checkout of this repository:

```sh
cargo run --offline -p mindcore-runtime --example mind_facade
```

It prints the recalled record and the derived `MindState`. In order, the example:

1. `create`s a Mind through the registry — the space, the carrier entry, the registry row and the
   owner's grant row land in one transaction — and opens the handle with `Mind::open`:

```rust
use mindcore_base::boundary::CallerId;
use mindcore_runtime::minds::{self, CreateMind};
use mindcore_runtime::{journal_dir, Mind};
use spoke_operations::SpokeResult;

// Every facade call answers a `SpokeResult`: unwrap yours the way your host handles refusals.
fn ok<T>(result: SpokeResult<T>, step: &str) -> T {
    match result {
        SpokeResult::Ok(value) => value,
        SpokeResult::Reject(reject) => panic!("{step} refused: {}: {}", reject.code, reject.message),
    }
}

let owner = CallerId::new("caller_wren");
let created = ok(
    minds::create(
        &store,
        &CreateMind {
            mind_id: None,
            display_name: Some("Wren".to_string()),
            owner: owner.clone(),
        },
    )
    .await,
    "create",
);

// The journal root is the host's: create it and make its creation entry durable before the
// first publish — the publisher refuses a missing root.
std::fs::create_dir_all("journal").expect("create the journal root");
std::fs::File::open(".")
    .and_then(|parent| parent.sync_all())
    .expect("sync the journal root's creation entry");

let mind = ok(Mind::open(&store, &owner, &created.id).await, "open")
    .with_journal_publisher(journal_dir("journal"));
```

`open` is one registry read; a Mind the caller cannot see answers the registry's own `NotFound`.
The journal publisher is the host's input — the facade never invents one — and `journal_dir` is
the one publisher the crate offers. The journal root is the host's to create and make durable
before the first publish: create it, then sync its parent, exactly as the example does. Before memory writes, the host provisions the subject rows it
means to write under, exactly as on the served surface; the example shows the minimal seed.

2. Captures a memory and reviews it, in plain arguments:

```rust
use mindcore_runtime::{Capture, Recall};

let content = serde_json::json!({ "text": "the north gate is shut after dusk" });
let capture = Capture {
    subject: "subj_wren",
    context: None,
    content: &content,
    occurred_at: None,
    kind_hint: None,
    idempotency_key: "cap-north-gate",
};
let captured = ok(mind.capture(&capture).await, "capture");
let candidate_id = serde_json::to_value(&captured).expect("the response serializes")
    ["result"]["candidate_ids"][0]
    .as_str()
    .expect("the capture stages one candidate")
    .to_string();
```

`capture` takes the caller-named subject — never a default — and `review` answers the settled
receipt instead of a stage: the fold (staging, publication, settlement) is the handle's. A failed
publication leaves no record and the same idempotency key can be repeated.

3. Recalls the reviewed record with `mind.recall`, then writes the carrier entry — the one the
   handle names with `carrier_entry_id()`, never a coordinate you compose — with one cognition
   record through `knowledge_put`, the single validation boundary, and reads it back with
   `knowledge_get`. That write is an explicit carrier update, so the last step has a judgment to
   derive.

4. Reads the Mind's judgment with `mind.cognition_read()`: the carrier's current-view records and
   the `MindState` derived from them. A fresh carrier answers `None` — a Mind without a judgment
   is a state, not an error.

`knowledge_list` and `deliberate` sit on the same handle. The facade's `deliberate` takes a
target and evidence only — no subject — and stamps the Mind's id as the assessment context; the
full contract is [`facade.md`](../.agents/specs/architecture/facade.md).

## What the facade composes: the transaction level

The rest of this page is the manual path the facade performs for you — adapter calls on
transactions you own, the wire shapes each takes and the staged settlement of a review. Read it
to understand what the facade composes, or use it when a host needs control the handle does not
expose.

## Add the dependency

`mindcore-base` and `mindcore-sqlite` are on crates.io; let Cargo resolve the version the registry
holds. `[workspace.package].version` in this repository is the source of truth for what a release
contains.

```sh
cargo add mindcore-base mindcore-sqlite
```

`mindcore-spoke`, which the transaction examples below use, is not published; it and the remaining
pins go into `Cargo.toml` directly:

```toml
[dependencies]
mindcore-spoke = { git = "https://github.com/42ch-dev/mindcore" }
# match the SPOKE version pinned in this workspace's Cargo.toml
spoke-operations = ">=0.19.1, <0.20.0"
spoke-schemas = ">=0.19.1, <0.20.0"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
serde_json = "1"
```

## Open a store

```rust
use std::path::Path;
use mindcore_sqlite::MindStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = MindStore::open(Path::new("mind.db")).await?;
    // ... use the store ...
    store.close().await;
    Ok(())
}
```

`open` creates the file when absent, enables WAL, foreign keys and a five-second busy timeout,
and runs the embedded migration. Each opened store owns one single-connection pool; open each
database file once per process.

## Capture a memory

A memory write goes through the SPOKE adapter on a transaction you own. The host must have
provisioned the space, the subject and the caller's grants first; [`tests/support/embedded_story.rs`](../crates/mindcore-sqlite/tests/support/embedded_story.rs)
shows the minimum rows for a test fixture (production hosts use their own authorization model).

```rust
use mindcore_spoke::{MemoryAdapter, MemoryStage};
use serde_json::json;
use spoke_operations::SpokeResult;
use spoke_schemas::Principal;

let adapter = MemoryAdapter::new(false);
let caller: Principal = serde_json::from_value(json!({
    "principal_id": "caller_wren",
    "extensions": {}
}))?;

let mut tx = store.pool().begin().await?;
let stage = adapter
    .capture(
        &mut tx,
        &caller,
        json!({
            "space_id": "wren",
            "subject_id": "wren",
            "capture_scope": { "visibility": "context", "context_id": "tavern" },
            "content": { "text": "A traveller says the north gate is shut after dusk." },
            "occurred_at": "2026-11-19T17:40:00Z",
            "idempotency_key": "cap-wren-hearsay"
        }),
    )
    .await;

let candidate_id = match stage {
    SpokeResult::Ok(MemoryStage::Ready(body)) => {
        body["result"]["candidate_ids"][0].as_str().unwrap().to_string()
    }
    other => panic!("capture did not stage a candidate: {other:?}"),
};
tx.commit().await?;
```

A capture stages a **pending candidate**. It is not recallable, and it is not a record. Review
consumes it into one retained record (`promote`), several (`fragment`) or none (`drop`);
`dismiss` consumes it without a record.

Review is a staged settlement. A review that owes file effects returns `MemoryStage::JournalPending`;
commit that transaction, publish the targets, then settle the receipt in a second transaction.
The helpers `stage_review`, `settle_receipt` and `attempt_retain` in
[`tests/support/embedded_story.rs`](../crates/mindcore-sqlite/tests/support/embedded_story.rs)
are the shortest complete implementation.

Recall reads live records only:

```rust
let mut tx = store.pool().begin().await?;
let _page = adapter
    .recall(
        &mut tx,
        &caller,
        json!({
            "space_id": "wren",
            "subject_id": "wren",
            "selection": { "mode": "selected_contexts", "context_ids": ["tavern"] },
            "budget": { "max_items": 10 }
        }),
    )
    .await;
tx.rollback().await?;
```

## Read and write knowledge

A knowledge port is bound to one collection when it is built. No API accepts a collection,
`owner` or `viewpoint` as a call argument.

```rust
use mindcore_base::knowledge::KnowledgeScope;
use spoke_operations::KnowledgeEntryPort;

let wren = store.knowledge(KnowledgeScope::new("wren"));

match wren.get_knowledge_entry("ke_north_gate").await {
    SpokeResult::Ok(entry) => println!("revision {:?}", entry.revision),
    SpokeResult::Reject(reject) => println!("{}: {}", reject.code, reject.message),
}
```

A create passes `expected_base_revision = None`; an update passes `Some(stored_revision)` and is
refused when the row has moved. The store assigns the next revision itself.

To adopt a shared entry, use the runtime's `knowledge.adopt` operation: it resolves the source's
retirement chain, then writes the copy with an anchor naming the live terminal. A direct
`put_knowledge_entry` with an anchor built by `mindcore_base::knowledge::adoption_anchor` stores
that anchor verbatim — it preserves an existing anchor and refuses a change, but it performs no
retirement resolution. Full rules: [Concepts](concepts.md#knowledge) and
[Migrating from `tools.mindcore.adopt`](migrating-from-tools-adopt.md).

## Derive the current judgment

Cognition lives in a carrier entry: an ordinary `KnowledgeEntry` whose cognition module holds
append-only records, with one head pointer to the current judgment. A fresh store has no carrier, so
`read_carrier` refuses `KnowledgeEntryNotFound` until the holder entry exists. Once it exists, the
read succeeds even before the first append; `derive_mind_state` then answers `NoRecords` until a
record is appended. The story's steps 3–4 create the holder and append the first records.

```rust
use mindcore_base::cognition::derive_mind_state;
use mindcore_sqlite::cognition::read_carrier;

let carrier = match read_carrier(&wren, "ke_wren_cognition").await {
    SpokeResult::Ok(entry) => entry,
    SpokeResult::Reject(reject) => panic!("no carrier: {} {}", reject.code, reject.message),
};

match derive_mind_state(&carrier) {
    Ok(view) => println!("holder {} at revision {}", view.holder_entry_id, view.carrier_revision),
    Err(error) => println!("no derivable state: {error:?}"),
}
```

The carrier is validated at the single persistence boundary, and `MindState` is derived rather
than stored a second time. A catalog read never derives cognition; a shared source change never
moves the carrier.

## Run the full story

Run this from a checkout of this repository; a consumer project does not carry the example.

```sh
cargo run --locked -p mindcore-sqlite --example iteration_01
```

The example prints and asserts the S1–S8 scenario:

1. hearing — a candidate is captured, recall stays empty, no personal entry exists
2. catalog read — the shared entry is unchanged; the derived state is `NoRecords`
3. adoption — a personal copy is anchored to the shared source at its revision
4. doubt — a belief with `Unknown` truth and testimony as its basis
5. evidence — perception replaces doubt in the current judgment set
6. shared change — the source moves to revision 2; the adopted copy stays at 1
7. reopen — the derived state equals the pre-close state
8. refusals — wrong-scope and stale writes leave both scopes unchanged

## Next

- [Concepts](concepts.md) — the record model behind these calls.
- [Serving a surface](serving.md) — expose the same store over SPOKE Connect.
- [For agents](for-agents.md) — checks and repository conventions.
