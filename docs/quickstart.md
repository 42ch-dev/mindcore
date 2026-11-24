# Quick start

This tutorial takes you from an empty directory to a derived `MindState`, using the same code the
repository's integration test runs. It is a guided path, not a complete API reference.

## What you will do

1. Open an embedded SQLite store.
2. Capture a memory and see why it is not recallable yet.
3. Read and write a scoped knowledge entry.
4. Derive the current judgment from a cognition carrier.
5. Run the full first-mind story.

## Prerequisites

- A stable Rust toolchain with edition 2021 support.
- A system SQLite library the linker can find. The workspace does not enable `sqlx`'s `bundled`
  feature, so `libsqlite3-sys` links against the system library.
- `sqlite3` CLI and `sqlx-cli` only if you also run the repository checks.

## Add the dependency

The crates are `publish = false`; depend on this repository directly.

```toml
[dependencies]
mindcore-base = { git = "https://github.com/42ch-dev/mindcore" }
mindcore-sqlite = { git = "https://github.com/42ch-dev/mindcore" }
mindcore-spoke = { git = "https://github.com/42ch-dev/mindcore" }
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
refused when the row has moved. The store assigns the next revision itself. To adopt a shared
entry, copy it into the port's collection with an adoption anchor built by
`mindcore_base::knowledge::adoption_anchor`; the source resolves through its retirement chain
before the write. Full rules: [Concepts](concepts.md#knowledge) and
[Migrating from `tools.mindcore.adopt`](migrating-from-tools-adopt.md).

## Derive the current judgment

Cognition lives in a carrier entry: an ordinary `KnowledgeEntry` whose cognition module holds
append-only records, with one head pointer to the current judgment.

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
