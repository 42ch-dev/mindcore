# Quick start: one Mind in process

This tutorial takes you from an empty directory to a Mind that has heard something, settled it
into a record, recalled it, written one judgment and read the `MindState` derived from it. You
operate the Mind through `mindcore_runtime::Mind`, the facade in `mindcore-runtime` — one of the
three ways to reach a Mind, and the one a Rust host links into its own process. The
[README](../README.md#three-ways-to-reach-one) names the other two; [Serving a peer](serving.md)
covers the SPOKE-peer form.

It is a guided path, not a complete API reference. The record model is in
[Concepts](concepts.md), and the facade's full contract is
[`facade.md`](../.agents/specs/architecture/facade.md).

## What you will do

1. Create a Mind and open its handle.
2. Capture a memory, review it and recall the settled record.
3. Write the carrier with one cognition record and read the derived `MindState`.

## Prerequisites

- A stable Rust toolchain with edition 2021 support.
- A system SQLite library the linker can find. The workspace does not enable `sqlx`'s `bundled`
  feature, so `libsqlite3-sys` links against the system library.
- `sqlite3` CLI and `sqlx-cli` only if you also run the repository checks.

## Run the example first

The shortest path is the documented example, run from a checkout of this repository:

```sh
cargo run --locked -p mindcore-runtime --example mind_facade
```

It prints the recalled record and the derived `MindState`, and its source
([`crates/mindcore-runtime/examples/mind_facade.rs`](../crates/mindcore-runtime/examples/mind_facade.rs))
is the complete, runnable version of steps 1–6. Step 7 needs a provider the example does not
construct; the rest of this page walks through every step.

## Add the dependency

```toml
[dependencies]
# The facade is unpublished, so the whole mindcore family comes from this repository at one
# revision: a registry `mindcore-base` beside a git `mindcore-runtime` would be a second copy of
# the same crate, and the facade would not accept its types.
mindcore-base = { git = "https://github.com/42ch-dev/mindcore" }
mindcore-sqlite = { git = "https://github.com/42ch-dev/mindcore" }
mindcore-runtime = { git = "https://github.com/42ch-dev/mindcore" }
# The SPOKE versions this workspace pins.
spoke-operations = "0.19.1"
spoke-schemas = "0.19.1"
# For the host's own subject provisioning (step 3).
sqlx = { version = "0.9", default-features = false, features = ["runtime-tokio", "sqlite"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
serde_json = "1"
```

`mindcore-base`, `mindcore-spi` and `mindcore-sqlite` are on crates.io at `0.1.0`, the version
`[workspace.package].version` declares at this head, so a `"0.1"` requirement matches. A host that
needs only the record model and the store can take them from the registry. Embedding the facade
needs `mindcore-runtime`, which is `publish = false`, so the family comes from one source: the git
entries above, whose revision `Cargo.lock` records. A registry `mindcore-base` beside a git
`mindcore-runtime` would load two copies of the same types.

## 1. Create a Mind

```rust
use mindcore_base::boundary::CallerId;
use mindcore_runtime::minds::{self, CreateMind};
use mindcore_sqlite::MindStore;
use spoke_operations::SpokeResult;

// Every facade call answers a `SpokeResult`: unwrap yours the way your host handles refusals.
fn ok<T>(result: SpokeResult<T>, step: &str) -> T {
    match result {
        SpokeResult::Ok(value) => value,
        SpokeResult::Reject(reject) => panic!("{step} refused: {}: {}", reject.code, reject.message),
    }
}

let store = MindStore::open(std::path::Path::new("mind.db"))
    .await
    .expect("open the store");
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
```

`create` writes the space, the cognition carrier, the registry row and the owner's default grant
in one transaction, and a refusal leaves no row. `mind_id: None` asks the registry for a fresh id;
a Mind's id is its space id, so the registry row, the space and the carrier cannot disagree.
`CallerId` is the authenticated caller and every later operation re-checks its live grants.

## 2. Open the handle and give it a journal publisher

```rust
use mindcore_runtime::{journal_dir, Mind};

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
The handle caches no grant and confers none: a frozen Mind opens, and every operation re-reads the
live state on its own transaction. The journal publisher is the host's input — the facade never
invents one — and `journal_dir` is the one publisher the crate offers. Creating the root writes an
entry in its parent, and only the host can make that entry durable, so sync the parent exactly as
above.

## 3. Capture a memory

Before a memory write, the host provisions the subject rows it means to write under, exactly as on
the served surface. A single row insert is a complete atomic write in SQLite:

```rust
sqlx::query(
    "INSERT INTO memory_subjects (space_id, subject_id, status, revision, created_at) \
     VALUES (?, ?, 'active', 1, '2026-01-01T00:00:00.000Z')",
)
.bind(created.id.as_str())
.bind("subj_wren")
.execute(&store.pool())
.await
.expect("provision the subject");
```

```rust
use mindcore_runtime::Capture;

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

`capture` takes the caller-named subject — never a default — and stages a **pending candidate**.
The candidate is not recallable and is not a record.

## 4. Review the candidate

```rust
use spoke_schemas::memory_candidates_review_request::Decision;

let promote: Decision = serde_json::from_value(serde_json::json!({ "action": "promote" }))
    .expect("a promote decision");
let reviewed = ok(
    mind.review(&candidate_id, &promote, "rev-north-gate").await,
    "review",
);
let record_id = serde_json::to_value(&reviewed).expect("the response serializes")
    ["result"]["record_ids"][0]
    .as_str()
    .expect("promote retains one record")
    .to_string();
```

`review` answers the settled receipt instead of a stage: the fold — staging, publication,
settlement — is the handle's. `promote` retains one record, `fragment` several and `drop` none. A
failed publication leaves no record and the same idempotency key can be repeated; a failure after
durable staging answers `OUTCOME_UNCERTAIN`, and the caller repeats the same review to settle it.

## 5. Recall the settled record

```rust
use mindcore_runtime::Recall;

let selection = serde_json::json!({ "mode": "subject_all" });
let recall = Recall {
    subject: "subj_wren",
    selection: &selection,
    max_items: 10,
    max_bytes: None,
    query: None,
    cursor: None,
};
let page = ok(mind.recall(&recall).await, "recall");
```

Recall reads live records only. `max_items` is required because every recall the facade sends is a
valid wire request; `selection` is the read-selection member exactly as the wire carries it.

## 6. Write the carrier and read the judgment

The cognition carrier is an ordinary knowledge entry, and `knowledge_put` is its one validation
boundary. The entry id comes from the handle; a host never composes it.

```rust
use mindcore_base::cognition::append_cognition_record;

let carrier = ok(
    mind.knowledge_get(mind.carrier_entry_id(), None).await,
    "read the carrier",
);
let carrier_revision = carrier.revision.expect("a stored carrier has a revision");
let record = serde_json::json!({
    "kind": "belief",
    "order": 1,
    "record_id": "cog_north_gate",
    "holder": {
        "kind": "knowledge_entry",
        "namespace": "mindcore",
        "id": mind.carrier_entry_id(),
    },
    "target": {
        "carrier": {
            "kind": "knowledge_entry",
            "namespace": "mindcore",
            "id": mind.carrier_entry_id(),
        },
        "target_id": "prop_north_gate_shut",
        "kind": "proposition",
    },
    "assessor": {
        "kind": "principal",
        "namespace": "mindcore",
        "id": "caller_wren",
    },
    "assessor_role": "holder_self_report",
    "basis": {
        "kind": "evidence",
        "evidence": [{ "kind": "memory_record", "namespace": "mindcore", "id": record_id }],
    },
    "assessment_context": { "context_id": created.id.as_str() },
    "assessed_at": "2026-01-01T00:30:00Z",
    "effective_at": "2026-01-01T00:30:00Z",
    "view": "current",
    "q": {
        "value": 0.8,
        "assertion": "belief_attribution",
        "method": "two_witness_reports",
        "calibration": "uncalibrated",
    },
    "extensions": {},
});
let updated = append_cognition_record(&carrier, record)
    .expect("one valid record appends to the carrier");
let written = ok(
    mind.knowledge_put(updated, Some(carrier_revision)).await,
    "write the carrier",
);
```

The expected revision makes the write a compare-and-swap: a row that has moved answers
`StoredRevisionStale`, and the refusal leaves it untouched. The carrier, its validation boundary
and the derived state are in [Concepts](concepts.md#cognition).

```rust
let cognition = ok(mind.cognition_read().await, "cognition read")
    .expect("one record on the carrier is a judgment");
println!("{}", serde_json::to_string_pretty(&cognition.mind_state).expect("the state prints"));
```

`cognition_read` answers the carrier's current-view records and the `MindState` derived from them.
A fresh carrier answers `None` — a Mind without a judgment is a state, not an error. `MindState` is
derived on every read, never stored a second time. `knowledge_list` sits on the same handle; it is
bounded, and a collection over 512 live entries answers the store's own
`COLLECTION_OVER_CAPACITY` rather than paging.

## 7. Deliberate, when the host supplies a reasoner

`deliberate` is not in `mind_facade`: the example assembles no providers, and the call needs the
host's `Reasoner` before it can complete. It takes a target and the evidence the verdict may rest
on — no subject — and stamps the Mind's id as the assessment context:

```rust
use std::time::Duration;
use mindcore_runtime::{Deliberation, MindProviders};

// `reasoner` is a host-supplied `Arc<dyn Reasoner>`; `target` and `evidence` come from your host.
let mind = mind.with_providers(MindProviders {
    reasoner: Some(reasoner),
    ..Default::default()
});
let deliberation = Deliberation {
    target: &target,
    evidence: &evidence,
    timeout: Duration::from_secs(10),
};
let outcome = ok(mind.deliberate(&deliberation).await, "deliberate");
```

The providers are the host's, as on the served surface: without a reasoner, `deliberate` answers
`InternalError` with `details.kind = "reasoner_unavailable"`. It settles only under a space-wide
live `recall` grant, and an unsettled intent anywhere in the Mind withholds the commit.

## Run the full story

The facade above is the product path. The workspace also asserts the whole record story — adoption,
doubt, evidence, a shared change, a reopen and the refusals — as a runnable example:

```sh
cargo run --locked -p mindcore-sqlite --example iteration_01
```

It prints and asserts the S1–S8 scenario: a candidate is heard and recall stays empty; the catalog
read is unchanged; a personal copy is anchored to the shared source at its revision; a belief with
`Unknown` truth and testimony as its basis is recorded; perception replaces doubt; the source moves
to revision 2 and the adopted copy stays at 1; reopening derives the same state; wrong-scope and
stale writes leave both scopes unchanged.

## Next

- [Concepts](concepts.md) — the record model behind these calls.
- [Serving a peer](serving.md) — reach the same store over SPOKE Connect with `mindcore-daemon`.
- [`bindings/node`](../bindings/node/index.d.ts) — the in-process TypeScript form; its exported
  surface is documented in `index.d.ts`.
- [For agents](for-agents.md) — checks and repository conventions.
