//! One end-to-end lifecycle over one store: history replay, adoption provenance and supersede.
//!
//! Each capability is proven alone elsewhere. This target proves they still agree when they run
//! in sequence over the same entry, and that every refusal leaves the stored state untouched.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use mindcore_base::cognition::CARRIER_NAMESPACE;
use mindcore_base::knowledge::{adoption_anchor_parts, KnowledgeScope};
use mindcore_spi::{adopt_knowledge_entry, AdoptionOutcome};
use mindcore_sqlite::MindStore;
use serde_json::{json, Map, Value};
use spoke_operations::{KnowledgeEntryPort, SpokeRejectCode, SpokeResult};
use spoke_schemas::knowledge_entry::{
    KnowledgeEntryBody, KnowledgeEntryCanonicalName, KnowledgeEntryDisclosure,
    KnowledgeEntryModulesKey, KnowledgeEntryModulesValue, KnowledgeEntryOwner,
};
use spoke_schemas::KnowledgeEntry;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn database_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before the epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "mindcore-lifecycle-{}-{label}-{nanos}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

// WAL leaves siblings next to the database; a rerun must start from an empty store.
fn remove_database(path: &Path) {
    let _ = std::fs::remove_file(path);
    for suffix in ["-wal", "-shm"] {
        let mut sibling = path.as_os_str().to_os_string();
        sibling.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(sibling));
    }
}

fn entry_with(entry_id: &str, summary: &str) -> KnowledgeEntry {
    KnowledgeEntry {
        body: KnowledgeEntryBody {
            summary: Some(summary.to_string()),
            ..KnowledgeEntryBody::default()
        },
        canonical_name: KnowledgeEntryCanonicalName::try_from(format!("{entry_id} name"))
            .expect("non-empty canonical name"),
        created_at: None,
        disclosure: None,
        entry_id: entry_id.to_string(),
        entry_type: "location".to_string(),
        extensions: HashMap::new(),
        modules: HashMap::new(),
        owner: None,
        revision: None,
        schema_version: NonZeroU64::new(1).expect("non-zero wire baseline"),
        source_anchor: None,
        status: "confirmed".to_string(),
        updated_at: None,
    }
}

/// An entry submitting the cognition envelope with `records` inline.
fn cognition_entry(entry_id: &str, summary: &str, records: Vec<Value>) -> KnowledgeEntry {
    let mut entry = entry_with(entry_id, summary);
    entry.modules.insert(
        cognition_module_key(),
        knowledge_module(json!({
            "records": records,
            "world_facts": [],
            "extensions": {},
        })),
    );
    entry
}

/// An owner-private entry: the viewpoint-free disclosure predicate hides it.
fn private_entry(entry_id: &str, summary: &str) -> KnowledgeEntry {
    let mut entry = entry_with(entry_id, summary);
    entry.disclosure =
        Some(KnowledgeEntryDisclosure::from_str("owner-private").expect("non-empty disclosure"));
    entry.owner = Some(KnowledgeEntryOwner::try_from("kb_mira").expect("non-empty owner"));
    entry
}

/// One current-view belief record held by `holder`: the shape a carrier write accepts.
fn belief(holder: &str, record_id: &str) -> Value {
    json!({
        "kind": "belief",
        "order": 1,
        "record_id": record_id,
        "holder": {
            "kind": "knowledge_entry",
            "id": holder,
            "namespace": CARRIER_NAMESPACE,
        },
        "target": {
            "carrier": {
                "kind": "knowledge_entry",
                "id": holder,
                "namespace": CARRIER_NAMESPACE,
            },
            "target_id": "prop_north_gate_closed",
            "kind": "proposition",
        },
        "assessor": {
            "kind": "knowledge_entry",
            "id": holder,
            "namespace": CARRIER_NAMESPACE,
        },
        "assessor_role": "holder_self_report",
        "basis": {
            "kind": "evidence",
            "evidence": [{
                "kind": "memory_record",
                "id": "mem_hearsay",
                "namespace": CARRIER_NAMESPACE,
            }],
        },
        "assessment_context": {"context_id": "ctx_dusk_at_the_gate"},
        "effective_at": "2026-11-19T18:00:00Z",
        "assessed_at": "2026-11-19T18:05:00Z",
        "view": "current",
        "q": {
            "value": 0.4,
            "assertion": "belief_attribution",
            "method": "single_self_report",
            "calibration": "uncalibrated",
        },
        "extensions": {},
    })
}

fn cognition_module_key() -> KnowledgeEntryModulesKey {
    KnowledgeEntryModulesKey::try_from("cognition").expect("valid module key")
}

fn knowledge_module(module: Value) -> KnowledgeEntryModulesValue {
    KnowledgeEntryModulesValue::from(
        module
            .as_object()
            .expect("module fixture is an object")
            .clone(),
    )
}

fn module_of(entry: &KnowledgeEntry) -> Option<&Map<String, Value>> {
    match entry.modules.get(&cognition_module_key()) {
        Some(KnowledgeEntryModulesValue::Object(module)) => Some(module),
        _ => None,
    }
}

fn record_ids(entry: &KnowledgeEntry) -> Vec<String> {
    module_of(entry)
        .and_then(|module| module.get("records"))
        .and_then(Value::as_array)
        .map(|records| {
            records
                .iter()
                .filter_map(|record| {
                    record
                        .get("record_id")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn summary_of(entry: &KnowledgeEntry) -> Option<&str> {
    entry.body.summary.as_deref()
}

fn as_json(entry: &KnowledgeEntry) -> Value {
    serde_json::to_value(entry).expect("entry serializes")
}

fn expect_ok<T>(result: SpokeResult<T>, context: &str) -> T {
    match result {
        SpokeResult::Ok(value) => value,
        SpokeResult::Reject(reject) => panic!("{context}: unexpectedly rejected with {reject:?}"),
    }
}

fn expect_reject<T: std::fmt::Debug>(result: SpokeResult<T>, context: &str) -> SpokeRejectCode {
    match result {
        SpokeResult::Ok(value) => panic!("{context}: unexpectedly accepted with {value:?}"),
        SpokeResult::Reject(reject) => reject.code,
    }
}

/// The stored payload of one row, read without going through the port: adoption must not rewrite
/// even one byte of its source.
async fn stored_payload(store: &MindStore, collection: &str, entry_id: &str) -> String {
    sqlx::query_scalar(
        "SELECT payload FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
    )
    .bind(collection)
    .bind(entry_id)
    .fetch_one(&store.pool())
    .await
    .expect("stored payload")
}

fn expect_revision_absent<T: std::fmt::Debug>(result: SpokeResult<T>, context: &str) {
    match result {
        SpokeResult::Ok(value) => panic!("{context}: unexpectedly returned {value:?}"),
        SpokeResult::Reject(reject) => {
            assert_eq!(
                reject.code,
                SpokeRejectCode::KnowledgeEntryNotFound,
                "{context}"
            );
            assert_eq!(
                reject.message, "knowledge entry revision not found",
                "{context}"
            );
            assert!(
                reject.details.is_none(),
                "{context}: an absent revision carries no details"
            );
        }
    }
}

fn expect_source_absent(result: SpokeResult<AdoptionOutcome>, context: &str) {
    match result {
        SpokeResult::Ok(outcome) => panic!("{context}: unexpectedly adopted {outcome:?}"),
        SpokeResult::Reject(reject) => {
            assert_eq!(
                reject.code,
                SpokeRejectCode::KnowledgeEntryNotFound,
                "{context}"
            );
            assert_eq!(
                reject.message, "source knowledge entry not found",
                "{context}"
            );
            assert!(
                reject.details.is_none(),
                "{context}: a generic absence carries no details"
            );
        }
    }
}

const REVISION_ONE: &str = "the north gate shuts after dusk";
const REVISION_TWO: &str = "the north gate shuts after dusk; a keeper walks it";
const REVISION_THREE: &str =
    "the north gate shuts after dusk; a keeper walks it; the key hangs at the mill";

#[tokio::test]
async fn knowledge_lifecycle_preserves_history_and_adopted_provenance() {
    let path = database_path("lifecycle");
    let store = MindStore::open(&path).await.expect("open store");
    let shared = store.knowledge(KnowledgeScope::new("shared"));
    let personal = store.knowledge(KnowledgeScope::new("wren"));

    // Stage 1: create, then two updates, one appended cognition record each.
    let created = expect_ok(
        shared
            .put_knowledge_entry(entry_with("entry-a", REVISION_ONE), None)
            .await,
        "create entry-a",
    );
    assert_eq!(created.revision, Some(1));
    assert!(
        record_ids(&created).is_empty(),
        "the first revision carries no cognition record"
    );

    let second = expect_ok(
        shared
            .put_knowledge_entry(
                cognition_entry("entry-a", REVISION_TWO, vec![belief("entry-a", "cog_one")]),
                Some(1),
            )
            .await,
        "second revision",
    );
    assert_eq!(second.revision, Some(2));

    let third = expect_ok(
        shared
            .put_knowledge_entry(
                cognition_entry(
                    "entry-a",
                    REVISION_THREE,
                    vec![belief("entry-a", "cog_one"), belief("entry-a", "cog_two")],
                ),
                Some(2),
            )
            .await,
        "third revision",
    );
    assert_eq!(third.revision, Some(3));

    // Negatives: the id refuses a second create, and a stale conditional write is refused without
    // moving the entry. The stale payload repeats every committed record, so only the CAS can
    // refuse it.
    assert_eq!(
        expect_reject(
            shared
                .put_knowledge_entry(entry_with("entry-a", "a second gate"), None)
                .await,
            "a second create over the same id",
        ),
        SpokeRejectCode::KnowledgeEntryAlreadyExists
    );
    assert_eq!(
        expect_reject(
            shared
                .put_knowledge_entry(
                    cognition_entry(
                        "entry-a",
                        "a stale edit",
                        vec![belief("entry-a", "cog_one"), belief("entry-a", "cog_two"),],
                    ),
                    Some(1),
                )
                .await,
            "a stale conditional write",
        ),
        SpokeRejectCode::StoredRevisionStale
    );
    let head = expect_ok(
        shared.get_knowledge_entry("entry-a").await,
        "read the head after two refusals",
    );
    assert_eq!(head.revision, Some(3));
    assert_eq!(summary_of(&head), Some(REVISION_THREE));
    assert_eq!(record_ids(&head), vec!["cog_one", "cog_two"]);

    // Stage 2: adopt at revision 3. The copy anchors back, the source row is untouched and a
    // repeat returns the same copy instead of writing a second one.
    let source_before = stored_payload(&store, "shared", "entry-a").await;
    let copy = match expect_ok(
        adopt_knowledge_entry(&shared, &personal, "entry-a", 3).await,
        "adoption at revision 3",
    ) {
        AdoptionOutcome::Created(copy) => copy,
        other => panic!("the first adoption must create the copy, got {other:?}"),
    };
    assert_eq!(copy.entry_id, "entry-a");
    assert_eq!(copy.revision, Some(1));
    assert_eq!(summary_of(&copy), Some(REVISION_THREE));
    assert!(
        record_ids(&copy).is_empty(),
        "the copy carries none of the source's judgments"
    );
    assert_eq!(
        adoption_anchor_parts(&copy),
        Some(("entry-a", Some(3))),
        "the anchor records the id and the exact copied revision"
    );
    assert_eq!(
        expect_ok(
            shared.resolve_adoption_anchor(&copy).await,
            "the anchor of a live source"
        ),
        "entry-a"
    );
    assert_eq!(
        stored_payload(&store, "shared", "entry-a").await,
        source_before,
        "adoption only reads its source row"
    );

    let repeat = match expect_ok(
        adopt_knowledge_entry(&shared, &personal, "entry-a", 3).await,
        "repeat adoption at the adopted revision",
    ) {
        AdoptionOutcome::AlreadyPresent(existing) => existing,
        other => panic!("a repeat must return the stored copy, got {other:?}"),
    };
    assert_eq!(as_json(&repeat), as_json(&copy));
    assert_eq!(
        expect_ok(
            personal.list_entries().await,
            "list the personal collection"
        )
        .len(),
        1,
        "no repeat claimed the destination slot twice"
    );

    // Stage 3: supersede entry-a with entry-b. The default read omits it, the explicit read
    // returns it with its marker, and a write after the supersede is refused.
    expect_ok(
        shared
            .put_knowledge_entry(
                entry_with("entry-b", "the mill door replaces the gate"),
                None,
            )
            .await,
        "seed entry-b",
    );
    assert_eq!(
        expect_ok(
            shared.supersede_entry("entry-a", 3, "entry-b").await,
            "supersede entry-a with entry-b"
        ),
        4,
        "the source advances one revision for its marker"
    );

    match shared.get_knowledge_entry("entry-a").await {
        SpokeResult::Ok(value) => panic!("default read of a superseded entry returned {value:?}"),
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::KnowledgeEntryNotFound);
            assert_eq!(
                reject.message,
                "no knowledge entry entry-a in collection shared"
            );
            assert!(reject.details.is_none());
        }
    }
    let listed = expect_ok(shared.list_entries().await, "list after the supersede");
    assert_eq!(
        listed
            .iter()
            .map(|entry| entry.entry_id.as_str())
            .collect::<Vec<_>>(),
        ["entry-b"],
        "the list omits the superseded row"
    );

    let marked = expect_ok(
        shared.get_entry_including_superseded("entry-a").await,
        "explicit read of a superseded entry",
    );
    assert_eq!(marked.superseded_by.as_deref(), Some("entry-b"));
    assert_eq!(marked.entry.revision, Some(4));
    assert_eq!(summary_of(&marked.entry), Some(REVISION_THREE));
    assert_eq!(record_ids(&marked.entry), vec!["cog_one", "cog_two"]);

    // A write naming the pre-marker revision never lands: the superseded guard answers first and
    // leaves no fifth revision behind.
    match shared
        .put_knowledge_entry(
            cognition_entry(
                "entry-a",
                "an edit after the supersede",
                vec![belief("entry-a", "cog_one"), belief("entry-a", "cog_two")],
            ),
            Some(3),
        )
        .await
    {
        SpokeResult::Ok(value) => panic!("a write to a superseded entry returned {value:?}"),
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::InvalidInput);
            let details = reject
                .details
                .as_ref()
                .expect("the superseded refusal names its marker");
            assert_eq!(details.get("superseded_by"), Some(&Value::from("entry-b")));
        }
    }
    assert_eq!(
        expect_ok(
            shared.get_entry_including_superseded("entry-a").await,
            "explicit read after the refused write"
        )
        .entry
        .revision,
        Some(4)
    );

    // Stage 4: every revision replays its own payload and only the records it had. Revision 4 is
    // the superseded head, so its payload and history are the third revision's.
    for (revision, summary, records) in [
        (1, REVISION_ONE, Vec::<String>::new()),
        (2, REVISION_TWO, vec!["cog_one".to_string()]),
        (
            3,
            REVISION_THREE,
            vec!["cog_one".to_string(), "cog_two".to_string()],
        ),
        (
            4,
            REVISION_THREE,
            vec!["cog_one".to_string(), "cog_two".to_string()],
        ),
    ] {
        let replayed = expect_ok(
            shared.get_entry_at_revision("entry-a", revision).await,
            "replay an assigned revision",
        );
        assert_eq!(replayed.revision, Some(revision));
        assert_eq!(
            summary_of(&replayed),
            Some(summary),
            "revision {revision} replays its own payload"
        );
        assert_eq!(
            record_ids(&replayed),
            records,
            "revision {revision} truncates the record history it did not have"
        );
    }

    // Stage 5: the adopted copy still identifies the exact revision it copied, and its anchor now
    // resolves through the supersede to the live successor.
    assert_eq!(
        adoption_anchor_parts(&copy),
        Some(("entry-a", Some(3))),
        "the supersede does not rewrite the anchor's recorded revision"
    );
    assert_eq!(
        expect_ok(
            shared.resolve_adoption_anchor(&copy).await,
            "the anchor after the supersede"
        ),
        "entry-b"
    );
    assert_eq!(
        as_json(&expect_ok(
            personal.get_knowledge_entry("entry-a").await,
            "read the adopted copy back"
        )),
        as_json(&copy),
        "the copy itself never moves with its source"
    );
    assert_eq!(
        expect_ok(personal.list_entries().await, "list after the supersede").len(),
        1,
        "the personal collection still holds exactly the one copy"
    );

    // Stage 6: an owner-private source answers the same generic absence as a missing one, and a
    // revision that never existed is one absence.
    expect_ok(
        shared
            .put_knowledge_entry(private_entry("entry-p", "mira's private notes"), None)
            .await,
        "seed an owner-private entry",
    );
    expect_source_absent(
        adopt_knowledge_entry(&shared, &personal, "entry-p", 1).await,
        "adopting an owner-private entry",
    );
    assert_eq!(
        expect_ok(
            personal.list_entries().await,
            "list after the refused adoption"
        )
        .len(),
        1,
        "a refused adoption writes no personal row"
    );
    expect_revision_absent(
        shared.get_entry_at_revision("entry-a", 5).await,
        "a revision the entry never held",
    );
    expect_revision_absent(
        shared.get_entry_at_revision("entry-b", 2).await,
        "a revision of a live entry that never existed",
    );

    store.close().await;
    remove_database(&path);
}
