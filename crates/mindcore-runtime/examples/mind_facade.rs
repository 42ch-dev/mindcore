//! The documented facade example: one Mind end to end through `mindcore_runtime::Mind`.
//!
//! In order the example `create`s a Mind, opens its handle, captures a memory, reviews it,
//! recalls the settled record, writes the carrier with one cognition record through the one
//! validation boundary, reads it back, and reads the derived `MindState`. None of the assembly
//! a served caller performs is here: no transaction of its own, no wire principal, no wire
//! request bodies, no stage fold — the handle composes each of those.
//!
//! Host provisioning: the subject row a memory write needs is the host's job, as on the served
//! surface. A single row insert is a complete atomic write in SQLite, so the seed runs on the
//! pool without an explicit transaction.

use mindcore_base::boundary::CallerId;
use mindcore_base::cognition::append_cognition_record;
use mindcore_runtime::minds::{self, CreateMind};
use mindcore_runtime::{journal_dir, Capture, Mind, Recall};
use mindcore_sqlite::MindStore;
use serde_json::{json, Value};
use spoke_operations::SpokeResult;
use spoke_schemas::memory_candidates_review_request::Decision;

const OWNER: &str = "caller_wren";
const SUBJECT: &str = "subj_wren";

/// The value of a successful call; a refusal aborts the example with its code and message.
fn ok<T>(result: SpokeResult<T>, step: &str) -> T {
    match result {
        SpokeResult::Ok(value) => value,
        SpokeResult::Reject(reject) => panic!("{step} refused: {}: {}", reject.code, reject.message),
    }
}

fn value_of<T: serde::Serialize>(response: &T) -> Value {
    serde_json::to_value(response).expect("a response serializes")
}

#[tokio::main]
async fn main() {
    let dir = tempfile::TempDir::new().expect("a scratch directory");
    let store = MindStore::open(&dir.path().join("mind.db"))
        .await
        .expect("open the store");

    // 1. Create the Mind. The registry writes the space, the carrier entry in the Mind's
    //    collection, the registry row and the owner's grant row in one transaction.
    let owner = CallerId::new(OWNER);
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
    println!("mind created: {}", created.id.as_str());

    // Host provisioning, as the embedding host performs before capture: the subject row the
    // memory writes need. This is the host's setup, not a facade operation.
    sqlx::query(
        "INSERT INTO memory_subjects (space_id, subject_id, status, revision, created_at) \
         VALUES (?, ?, 'active', 1, '2026-01-01T00:00:00.000Z')",
    )
    .bind(created.id.as_str())
    .bind(SUBJECT)
    .execute(&store.pool())
    .await
    .expect("provision the subject");

    // 2. Open the handle. The crate's one journal publisher writes the review's file effects
    //    under the scratch root at their own paths — the host creates the root first, because
    //    the publisher never does: the entry that names it is the host's to make durable.
    let journal_root = dir.path().join("journal");
    std::fs::create_dir_all(&journal_root).expect("the host creates the journal root");
    // The host, not the publisher, owns the root's creation entry: creating the root writes an
    // entry in its parent, and that entry is durable only once the parent is synced.
    std::fs::File::open(journal_root.parent().expect("the journal root has a parent"))
        .and_then(|parent| parent.sync_all())
        .expect("sync the journal root's creation entry");
    let mind = ok(Mind::open(&store, &owner, &created.id).await, "open")
        .with_journal_publisher(journal_dir(journal_root));

    // 3. Capture a memory: plain arguments, never a wire request body.
    let content_text = "the north gate is shut after dusk";
    let content = json!({ "text": content_text });
    let capture = Capture {
        subject: SUBJECT,
        context: None,
        content: &content,
        occurred_at: None,
        kind_hint: None,
        idempotency_key: "cap-north-gate",
    };
    let captured = ok(mind.capture(&capture).await, "capture");
    let candidate_id = value_of(&captured)["result"]["candidate_ids"][0]
        .as_str()
        .expect("the capture stages one candidate")
        .to_string();

    // 4. Review the candidate. The answer is the settled receipt: the fold — staging,
    //    publication, settlement — is the handle's, and the caller never sees it.
    let promote: Decision = serde_json::from_value(json!({ "action": "promote" }))
        .expect("a promote decision");
    let reviewed = ok(
        mind.review(&candidate_id, &promote, "rev-north-gate").await,
        "review",
    );
    let record_id = value_of(&reviewed)["result"]["record_ids"][0]
        .as_str()
        .expect("promote retains one record")
        .to_string();

    // 5. Recall: the reviewed record is live.
    let selection = json!({ "mode": "subject_all" });
    let recall = Recall {
        subject: SUBJECT,
        selection: &selection,
        max_items: Some(10),
        max_bytes: None,
        query: None,
        cursor: None,
    };
    let page = value_of(&ok(mind.recall(&recall).await, "recall"));
    let record = page["records"]
        .as_array()
        .expect("a records page")
        .iter()
        .find(|record| record["record_id"] == record_id)
        .expect("the reviewed record is recallable")
        .clone();
    assert_eq!(
        record["content"]["text"],
        json!(content_text),
        "the record carries the captured content"
    );
    println!(
        "recalled record: {}",
        serde_json::to_string_pretty(&record).expect("the record prints")
    );

    // 6. Write the carrier with one cognition record: an explicit carrier update through the
    //    one validation boundary, so the next step has a judgment to derive. The entry id
    //    comes from the handle; the example never names `ke_carrier`.
    let carrier = ok(
        mind.knowledge_get(mind.carrier_entry_id(), None).await,
        "read the carrier",
    );
    let carrier_revision = carrier.revision.expect("a stored carrier has a revision");
    let record_one = json!({
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
            "id": OWNER,
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
    let updated = append_cognition_record(&carrier, record_one)
        .expect("one valid record appends to the carrier");
    let written = ok(
        mind.knowledge_put(updated, Some(carrier_revision)).await,
        "write the carrier",
    );
    assert_eq!(
        written.revision,
        Some(carrier_revision + 1),
        "the carrier update is one revision step"
    );
    let read_back = ok(
        mind.knowledge_get(mind.carrier_entry_id(), None).await,
        "read the written carrier",
    );
    assert_eq!(
        read_back.revision,
        Some(carrier_revision + 1),
        "the read answers the written revision"
    );

    // 7. Read the judgment: the carrier's one current-view record and the `MindState` derived
    //    from it, for the Mind the handle names.
    let cognition = ok(mind.cognition_read().await, "cognition read")
        .expect("one record on the carrier is a judgment");
    assert_eq!(cognition.records.len(), 1, "the one current-view record");
    assert_eq!(
        cognition.mind_state["mind_state_id"],
        json!(created.id.as_str()),
        "the Mind's id names its derived state"
    );
    println!(
        "derived MindState: {}",
        serde_json::to_string_pretty(&cognition.mind_state).expect("the state prints")
    );

    store.close().await;
    println!("facade example complete: capture, review, recall, carrier, MindState");
}
