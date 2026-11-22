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
use mindcore_sqlite::{append_cognition, MindStore, PruneReport};
use serde_json::{json, Map, Value};
use spoke_operations::{KnowledgeEntryPort, SpokeRejectCode, SpokeResult};
use spoke_schemas::knowledge_entry::{
    KnowledgeEntryBody, KnowledgeEntryCanonicalName, KnowledgeEntryDisclosure,
    KnowledgeEntryModulesKey, KnowledgeEntryModulesValue, KnowledgeEntryOwner,
};
use spoke_schemas::KnowledgeEntry;
use sqlx::SqlitePool;

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

// why: WAL leaves siblings next to the database; a rerun must start from an empty store.
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

fn expect_history_truncated<T: std::fmt::Debug>(result: SpokeResult<T>, context: &str) {
    match result {
        SpokeResult::Ok(value) => panic!("{context}: unexpectedly returned {value:?}"),
        SpokeResult::Reject(reject) => {
            assert_eq!(
                reject.code,
                SpokeRejectCode::KnowledgeEntryNotFound,
                "{context}"
            );
            assert_eq!(
                reject.message, "knowledge entry revision was pruned",
                "{context}"
            );
            assert_eq!(
                reject
                    .details
                    .as_ref()
                    .and_then(|details| details.get("reason")),
                Some(&Value::from("HISTORY_TRUNCATED")),
                "{context}: the truncation refusal names its class"
            );
        }
    }
}

async fn history_floor(pool: &SqlitePool, collection: &str, entry_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT history_floor_revision FROM knowledge_entries \
         WHERE collection = ? AND entry_id = ?",
    )
    .bind(collection)
    .bind(entry_id)
    .fetch_one(pool)
    .await
    .expect("stored history floor")
}

// note: seed a floor directly, without a prune, so the read gate is provable on its own.
async fn seed_history_floor(pool: &SqlitePool, collection: &str, entry_id: &str, floor: i64) {
    sqlx::query(
        "UPDATE knowledge_entries SET history_floor_revision = ? \
         WHERE collection = ? AND entry_id = ?",
    )
    .bind(floor)
    .bind(collection)
    .bind(entry_id)
    .execute(pool)
    .await
    .expect("seed a history floor");
}

async fn checkpoint_revisions(pool: &SqlitePool, collection: &str, entry_id: &str) -> Vec<i64> {
    sqlx::query_scalar(
        "SELECT revision FROM knowledge_entry_revisions \
         WHERE collection = ? AND entry_id = ? ORDER BY revision ASC",
    )
    .bind(collection)
    .bind(entry_id)
    .fetch_all(pool)
    .await
    .expect("stored checkpoint revisions")
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

    // note: stage 1 — create, then two updates, one appended cognition record each.
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

    // why: negatives — the id refuses a second create, and a stale conditional write is refused without
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

    // note: stage 2 — adopt at revision 3. The copy anchors back, the source row is untouched and a
    // repeat returns the same copy instead of writing a second one.
    // The whole source row through the public live read: adoption must leave every field alone.
    let source_before = expect_ok(
        shared.get_knowledge_entry("entry-a").await,
        "read the source before adoption",
    );
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
        as_json(&expect_ok(
            shared.get_knowledge_entry("entry-a").await,
            "read the source after adoption"
        )),
        as_json(&source_before),
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
    // why: the repeat answered from the destination too: the source row is still the one both
    // adoptions read.
    assert_eq!(
        as_json(&expect_ok(
            shared.get_knowledge_entry("entry-a").await,
            "read the source after the repeat adoption"
        )),
        as_json(&source_before),
        "a repeat adoption only reads its source row"
    );

    // note: stage 3 — supersede entry-a with entry-b. The default read omits it, the explicit read
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

    // why: a write naming the pre-marker revision never lands: the superseded guard answers first and
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

    // why: adoption against the superseded source answers the same generic absence as a missing one,
    // and neither attempt moves the source row. The default read hides the source now, so the
    // explicit superseded read supplies both sides of that comparison; the destination copy is
    // compared once, after the last refusal in this scenario.
    expect_source_absent(
        adopt_knowledge_entry(&shared, &personal, "entry-a", 3).await,
        "adopting a superseded source",
    );
    expect_source_absent(
        adopt_knowledge_entry(&shared, &personal, "entry-never-seeded", 1).await,
        "adopting a missing source",
    );
    let marked_after = expect_ok(
        shared.get_entry_including_superseded("entry-a").await,
        "explicit read after the refused adoption attempts",
    );
    assert_eq!(marked_after.entry.entry_id, "entry-a");
    assert_eq!(
        as_json(&marked_after.entry),
        as_json(&marked.entry),
        "a refused adoption never touches its superseded source"
    );
    assert_eq!(
        marked_after.superseded_by, marked.superseded_by,
        "a refused adoption never moves the source's marker"
    );

    // note: stage 4 — after the refused write, every revision still replays its whole payload and every
    // cognition record it held, and the refused write left no fifth revision. Revision 4 is the
    // superseded head, so its payload and history are the third revision's with only the revision
    // advanced.
    let mut marked_head = third.clone();
    marked_head.revision = Some(4);
    for (revision, expected) in [
        (1, as_json(&created)),
        (2, as_json(&second)),
        (3, as_json(&third)),
        (4, as_json(&marked_head)),
    ] {
        let replayed = expect_ok(
            shared.get_entry_at_revision("entry-a", revision).await,
            "replay an assigned revision",
        );
        assert_eq!(
            as_json(&replayed),
            expected,
            "revision {revision} replays its whole payload and every cognition record"
        );
    }
    expect_revision_absent(
        shared.get_entry_at_revision("entry-a", 5).await,
        "a revision the refused write never created",
    );

    // note: stage 5 — the adopted copy still identifies the exact revision it copied, and its anchor now
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

    // note: stage 6 — an owner-private source answers the same generic absence as a missing one, and a
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
    // why: every refusal above is covered here — the destination copy's content, its anchor and the
    // collection's row count are asserted once, after the last refusal.
    let copy_after = expect_ok(
        personal.get_knowledge_entry("entry-a").await,
        "read the adopted copy after every refusal",
    );
    assert_eq!(
        as_json(&copy_after),
        as_json(&copy),
        "a refused adoption leaves the destination copy unchanged"
    );
    assert_eq!(
        adoption_anchor_parts(&copy_after),
        Some(("entry-a", Some(3))),
        "a refused adoption leaves the destination anchor unchanged"
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

#[tokio::test]
async fn knowledge_history_floor_refuses_pruned_revisions_across_reopen() {
    let path = database_path("floor");
    remove_database(&path);
    let store = MindStore::open(&path).await.expect("open store");
    let shared = store.knowledge(KnowledgeScope::new("shared"));

    // note: three revisions — the first carries no record, the second one, the third two.
    let created = expect_ok(
        shared
            .put_knowledge_entry(entry_with("entry-f", REVISION_ONE), None)
            .await,
        "create entry-f",
    );
    let second = expect_ok(
        shared
            .put_knowledge_entry(
                cognition_entry("entry-f", REVISION_TWO, vec![belief("entry-f", "cog_one")]),
                Some(1),
            )
            .await,
        "second revision",
    );
    let third = expect_ok(
        shared
            .put_knowledge_entry(
                cognition_entry(
                    "entry-f",
                    REVISION_THREE,
                    vec![belief("entry-f", "cog_one"), belief("entry-f", "cog_two")],
                ),
                Some(2),
            )
            .await,
        "third revision",
    );

    // why: the column default starts a fresh row at 1: creation never names it, and neither update
    // moved it.
    let pool = store.pool();
    assert_eq!(history_floor(&pool, "shared", "entry-f").await, 1);

    // why: at or above floor 1 every revision replays as stored: 1 and 2 from a saved checkpoint, 3
    // from the live row.
    for (revision, expected) in [
        (1, as_json(&created)),
        (2, as_json(&second)),
        (3, as_json(&third)),
    ] {
        assert_eq!(
            as_json(&expect_ok(
                shared.get_entry_at_revision("entry-f", revision).await,
                "replay at the default floor"
            )),
            expected,
            "revision {revision} replays unchanged at the default floor"
        );
    }

    // note: seed the floor directly, without a prune, so the read gate is proven on its own.
    seed_history_floor(&pool, "shared", "entry-f", 2).await;

    // why: below the floor the preimage was pruned: that is not the generic absence.
    expect_history_truncated(
        shared.get_entry_at_revision("entry-f", 1).await,
        "a revision below the seeded floor",
    );

    // why: at or above the floor replay is byte-identical to the pre-seed result: revision 2 is a
    // checkpoint hit and revision 3 the live fallback.
    for (revision, expected) in [(2, as_json(&second)), (3, as_json(&third))] {
        assert_eq!(
            as_json(&expect_ok(
                shared.get_entry_at_revision("entry-f", revision).await,
                "replay at the seeded floor"
            )),
            expected,
            "revision {revision} replays unchanged above the floor"
        );
    }

    // why: a later full write leaves the floor alone: the column is storage-owned, not payload-derived.
    let fourth = expect_ok(
        shared
            .put_knowledge_entry(
                cognition_entry(
                    "entry-f",
                    "the mill door is barred",
                    vec![belief("entry-f", "cog_one"), belief("entry-f", "cog_two")],
                ),
                Some(3),
            )
            .await,
        "fourth revision",
    );
    assert_eq!(fourth.revision, Some(4));
    assert_eq!(history_floor(&pool, "shared", "entry-f").await, 2);

    // why: generic absence still covers zero, the unrepresentable range, a missing entry and a revision
    // above the live one; a floor never turns one of those into a refusal that claims a prune.
    expect_revision_absent(
        shared.get_entry_at_revision("entry-f", 0).await,
        "revision zero",
    );
    expect_revision_absent(
        shared.get_entry_at_revision("entry-f", u64::MAX).await,
        "a revision outside the representable range",
    );
    expect_revision_absent(
        shared.get_entry_at_revision("entry-never-seeded", 2).await,
        "a missing entry",
    );
    expect_revision_absent(
        shared.get_entry_at_revision("entry-f", 5).await,
        "a revision above the live one",
    );

    // why: the floor lives on the entry row, so the truncation refusal survives a close and reopen of
    // the same file instead of being reconstructed in memory.
    drop(pool);
    store.close().await;
    let store = MindStore::open(&path).await.expect("reopen the store");
    let shared = store.knowledge(KnowledgeScope::new("shared"));
    assert_eq!(history_floor(&store.pool(), "shared", "entry-f").await, 2);
    expect_history_truncated(
        shared.get_entry_at_revision("entry-f", 1).await,
        "a pruned revision after reopen",
    );
    assert_eq!(
        as_json(&expect_ok(
            shared.get_entry_at_revision("entry-f", 2).await,
            "a checkpoint above the floor after reopen"
        )),
        as_json(&second)
    );
    assert_eq!(
        as_json(&expect_ok(
            shared.get_entry_at_revision("entry-f", 4).await,
            "the live revision after reopen"
        )),
        as_json(&fourth)
    );
    expect_revision_absent(
        shared.get_entry_at_revision("entry-f", 5).await,
        "a revision above the live one after reopen",
    );

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn knowledge_prune_bounds_history_and_keeps_replay_above_the_floor() {
    let path = database_path("prune");
    remove_database(&path);
    let store = MindStore::open(&path).await.expect("open store");
    let shared = store.knowledge(KnowledgeScope::new("shared"));

    // note: create, then three full rewrites: revisions 1..4 and one checkpoint for each replaced
    // revision, so 1, 2 and 3 are on disk and 4 is the live row.
    expect_ok(
        shared
            .put_knowledge_entry(entry_with("entry-f", REVISION_ONE), None)
            .await,
        "create entry-f",
    );
    expect_ok(
        shared
            .put_knowledge_entry(
                cognition_entry("entry-f", REVISION_TWO, vec![belief("entry-f", "cog_one")]),
                Some(1),
            )
            .await,
        "second revision",
    );
    expect_ok(
        shared
            .put_knowledge_entry(
                cognition_entry(
                    "entry-f",
                    REVISION_THREE,
                    vec![belief("entry-f", "cog_one"), belief("entry-f", "cog_two")],
                ),
                Some(2),
            )
            .await,
        "third revision",
    );
    expect_ok(
        shared
            .put_knowledge_entry(
                cognition_entry(
                    "entry-f",
                    "the north gate reopens at dawn",
                    vec![belief("entry-f", "cog_one"), belief("entry-f", "cog_two")],
                ),
                Some(3),
            )
            .await,
        "fourth revision",
    );

    let pool = store.pool();
    assert_eq!(
        checkpoint_revisions(&pool, "shared", "entry-f").await,
        [1, 2, 3]
    );
    assert_eq!(history_floor(&pool, "shared", "entry-f").await, 1);

    // why: the prune keeps revisions 2 and up. Replay every one of them now, before the prune, so the
    // comparison after it is replay-to-replay rather than write-to-replay.
    let mut replay_before = Vec::new();
    for revision in 2..=4 {
        replay_before.push((
            revision,
            as_json(&expect_ok(
                shared.get_entry_at_revision("entry-f", revision).await,
                "replay before the prune",
            ))
            .to_string(),
        ));
    }

    let pruned: PruneReport = expect_ok(
        shared.prune_history("entry-f", 2).await,
        "prune to the two newest checkpoints",
    );
    assert_eq!(pruned.deleted, 1);
    assert_eq!(pruned.floor, Some(2));
    assert_eq!(history_floor(&pool, "shared", "entry-f").await, 2);
    assert_eq!(
        checkpoint_revisions(&pool, "shared", "entry-f").await,
        [2, 3],
        "the prune deletes exactly the checkpoints below the new floor"
    );

    // why: at or above the floor the replay is byte-identical to the replay snapshotted before it.
    for (revision, expected) in replay_before {
        assert_eq!(
            as_json(&expect_ok(
                shared.get_entry_at_revision("entry-f", revision).await,
                "replay above the new floor"
            ))
            .to_string(),
            expected,
            "revision {revision} replays exactly as it did before the prune"
        );
    }
    // why: the revision just below the floor is the one the prune removed.
    expect_history_truncated(
        shared.get_entry_at_revision("entry-f", 1).await,
        "the revision just below the new floor",
    );

    // why: a second prune with the same window has nothing to delete and leaves the floor alone.
    let repeated = expect_ok(
        shared.prune_history("entry-f", 2).await,
        "a repeated prune with the same window",
    );
    assert_eq!(repeated.deleted, 0);
    assert_eq!(repeated.floor, None);
    assert_eq!(history_floor(&pool, "shared", "entry-f").await, 2);
    assert_eq!(
        checkpoint_revisions(&pool, "shared", "entry-f").await,
        [2, 3]
    );

    // why: emptying history is a different action: a zero window is refused before any transaction.
    assert_eq!(
        expect_reject(
            shared.prune_history("entry-f", 0).await,
            "a prune that keeps no checkpoint"
        ),
        SpokeRejectCode::InvalidInput
    );
    assert_eq!(history_floor(&pool, "shared", "entry-f").await, 2);
    assert_eq!(
        checkpoint_revisions(&pool, "shared", "entry-f").await,
        [2, 3]
    );

    // why: a missing entry answers the current-entry absence, not a prune-shaped refusal.
    match shared.prune_history("entry-never-seeded", 1).await {
        SpokeResult::Ok(report) => panic!("a missing entry was pruned with {report:?}"),
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::KnowledgeEntryNotFound);
            assert_eq!(
                reject.message,
                "no knowledge entry entry-never-seeded in collection shared"
            );
            assert!(reject.details.is_none());
        }
    }

    // why: a retired entry keeps its history and answers the same generic absence: the live-only
    // predicate refuses it before any checkpoint is read or deleted.
    expect_ok(
        shared
            .put_knowledge_entry(
                entry_with("entry-g", "the mill door replaces the gate"),
                None,
            )
            .await,
        "seed entry-g",
    );
    expect_ok(
        shared.supersede_entry("entry-f", 4, "entry-g").await,
        "supersede entry-f with entry-g",
    );
    match shared.prune_history("entry-f", 1).await {
        SpokeResult::Ok(report) => panic!("a retired entry was pruned with {report:?}"),
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::KnowledgeEntryNotFound);
            assert_eq!(
                reject.message,
                "no knowledge entry entry-f in collection shared"
            );
            assert!(reject.details.is_none());
        }
    }
    assert_eq!(
        checkpoint_revisions(&pool, "shared", "entry-f").await,
        [2, 3, 4],
        "the refused prune leaves the retired entry's history untouched"
    );
    assert_eq!(history_floor(&pool, "shared", "entry-f").await, 2);

    // why: a later prune with a *smaller* window cuts again: the window's K-th newest is not yet the
    // floor, so the delete must still remove rows. This is the case a floor-only prune would fail.
    expect_ok(
        shared
            .put_knowledge_entry(entry_with("entry-h", REVISION_ONE), None)
            .await,
        "seed entry-h",
    );
    for (expected, summary) in [
        (1, "the mill door opens at dawn"),
        (2, "the mill door opens at dawn; a keeper sweeps it"),
        (3, "the mill door opens at dawn; the miller sweeps it"),
    ] {
        expect_ok(
            shared
                .put_knowledge_entry(entry_with("entry-h", summary), Some(expected))
                .await,
            "rewrite entry-h",
        );
    }
    assert_eq!(
        checkpoint_revisions(&pool, "shared", "entry-h").await,
        [1, 2, 3]
    );

    let first = expect_ok(
        shared.prune_history("entry-h", 2).await,
        "prune entry-h to the two newest checkpoints",
    );
    assert_eq!((first.deleted, first.floor), (1, Some(2)));

    let second = expect_ok(
        shared.prune_history("entry-h", 1).await,
        "prune entry-h again with a smaller window",
    );
    assert!(
        second.deleted >= 1,
        "a smaller window deletes at least one checkpoint, got {second:?}"
    );
    assert_eq!(second.floor, Some(3));
    assert_eq!(history_floor(&pool, "shared", "entry-h").await, 3);
    assert_eq!(checkpoint_revisions(&pool, "shared", "entry-h").await, [3]);

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn knowledge_prune_keeps_revisions_derived_across_a_gap_replayable() {
    let path = database_path("append-gap");
    remove_database(&path);
    let store = MindStore::open(&path).await.expect("open store");
    let shared = store.knowledge(KnowledgeScope::new("shared"));

    // note: alternate full rewrites and dedicated appends: an append advances the revision without
    // writing a preimage, so the checkpoints sit at 1, 3 and 5 and revisions 2 and 4 are derived,
    // replaying only from the checkpoint above them.
    expect_ok(
        shared
            .put_knowledge_entry(entry_with("entry-p", REVISION_ONE), None)
            .await,
        "create entry-p",
    );
    expect_ok(
        shared
            .put_knowledge_entry(
                cognition_entry("entry-p", REVISION_TWO, vec![belief("entry-p", "cog_one")]),
                Some(1),
            )
            .await,
        "second revision",
    );
    let mut appended = store
        .pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("begin the first dedicated append");
    match append_cognition(
        &mut appended,
        "shared",
        "entry-p",
        2,
        &belief("entry-p", "cog_two"),
    )
    .await
    {
        SpokeResult::Ok(assigned) => assert_eq!(assigned, 3),
        SpokeResult::Reject(reject) => panic!("first dedicated append rejected: {reject:?}"),
    }
    appended
        .commit()
        .await
        .expect("commit the first dedicated append");
    expect_ok(
        shared
            .put_knowledge_entry(
                cognition_entry(
                    "entry-p",
                    REVISION_THREE,
                    vec![belief("entry-p", "cog_one"), belief("entry-p", "cog_two")],
                ),
                Some(3),
            )
            .await,
        "fourth revision",
    );
    let mut appended = store
        .pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("begin the second dedicated append");
    match append_cognition(
        &mut appended,
        "shared",
        "entry-p",
        4,
        &belief("entry-p", "cog_three"),
    )
    .await
    {
        SpokeResult::Ok(assigned) => assert_eq!(assigned, 5),
        SpokeResult::Reject(reject) => panic!("second dedicated append rejected: {reject:?}"),
    }
    appended
        .commit()
        .await
        .expect("commit the second dedicated append");
    expect_ok(
        shared
            .put_knowledge_entry(
                cognition_entry(
                    "entry-p",
                    "the north gate reopens at dawn",
                    vec![
                        belief("entry-p", "cog_one"),
                        belief("entry-p", "cog_two"),
                        belief("entry-p", "cog_three"),
                    ],
                ),
                Some(5),
            )
            .await,
        "sixth revision",
    );

    let pool = store.pool();
    assert_eq!(
        checkpoint_revisions(&pool, "shared", "entry-p").await,
        [1, 3, 5]
    );

    let mut replay_before = Vec::new();
    for revision in 2..=6 {
        replay_before.push((
            revision,
            as_json(&expect_ok(
                shared.get_entry_at_revision("entry-p", revision).await,
                "replay before the prune",
            ))
            .to_string(),
        ));
    }
    let expected_derived = replay_before
        .iter()
        .find(|(revision, _)| *revision == 2)
        .map(|(_, expected)| expected.clone())
        .expect("revision 2 was replayed before the prune");

    // why: keeping two checkpoints deletes only checkpoint 1. Revisions 2 and 4 lose their preimage but
    // not their answer: the next checkpoint plus the crossed appends replays them exactly.
    let pruned: PruneReport = expect_ok(
        shared.prune_history("entry-p", 2).await,
        "prune to the two newest checkpoints",
    );
    assert_eq!((pruned.deleted, pruned.floor), (1, Some(2)));
    assert_eq!(history_floor(&pool, "shared", "entry-p").await, 2);
    assert_eq!(
        checkpoint_revisions(&pool, "shared", "entry-p").await,
        [3, 5]
    );

    for (revision, expected) in &replay_before {
        assert_eq!(
            as_json(&expect_ok(
                shared.get_entry_at_revision("entry-p", *revision).await,
                "replay a revision at or above the new floor",
            ))
            .to_string(),
            *expected,
            "revision {revision} answers exactly after the prune"
        );
    }
    expect_history_truncated(
        shared.get_entry_at_revision("entry-p", 1).await,
        "the revision whose preimage the prune removed",
    );

    // why: the derived replay survives a reopen: it is the surviving checkpoint, not the deleted row,
    // that keeps revision 2 answerable.
    drop(pool);
    store.close().await;
    let store = MindStore::open(&path).await.expect("reopen the store");
    let shared = store.knowledge(KnowledgeScope::new("shared"));
    assert_eq!(
        as_json(&expect_ok(
            shared.get_entry_at_revision("entry-p", 2).await,
            "replay the derived revision after reopen",
        ))
        .to_string(),
        expected_derived,
        "revision 2 still answers exactly after the prune and a reopen"
    );

    store.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn knowledge_history_survives_writes_and_reopen_until_an_explicit_prune() {
    let path = database_path("retention");
    remove_database(&path);
    let store = MindStore::open(&path).await.expect("open store");
    let shared = store.knowledge(KnowledgeScope::new("shared"));

    // note: create plus three full rewrites: one checkpoint per replaced revision, none removed.
    expect_ok(
        shared
            .put_knowledge_entry(entry_with("entry-r", REVISION_ONE), None)
            .await,
        "create entry-r",
    );
    for (expected, summary) in [
        (1, REVISION_TWO),
        (2, REVISION_THREE),
        (3, "the north gate reopens at dawn"),
    ] {
        let written = expect_ok(
            shared
                .put_knowledge_entry(entry_with("entry-r", summary), Some(expected))
                .await,
            "rewrite entry-r",
        );
        assert_eq!(written.revision, Some(expected + 1));
    }

    let pool = store.pool();
    assert_eq!(
        checkpoint_revisions(&pool, "shared", "entry-r").await,
        [1, 2, 3],
        "ordinary writes keep one checkpoint per replaced revision"
    );
    assert_eq!(history_floor(&pool, "shared", "entry-r").await, 1);

    // why: reopening the same file removes nothing: the checkpoints and the floor survive it.
    drop(pool);
    store.close().await;
    let store = MindStore::open(&path).await.expect("reopen the store");
    assert_eq!(
        checkpoint_revisions(&store.pool(), "shared", "entry-r").await,
        [1, 2, 3],
        "reopening a store keeps every checkpoint"
    );
    assert_eq!(history_floor(&store.pool(), "shared", "entry-r").await, 1);

    // why: only an explicit prune removes history, and only what its window leaves behind.
    let shared = store.knowledge(KnowledgeScope::new("shared"));
    let pruned: PruneReport = expect_ok(
        shared.prune_history("entry-r", 2).await,
        "prune to the two newest checkpoints",
    );
    assert_eq!(pruned.deleted, 1);
    assert_eq!(pruned.floor, Some(2));
    assert_eq!(
        checkpoint_revisions(&store.pool(), "shared", "entry-r").await,
        [2, 3],
        "the explicit prune leaves exactly the kept window"
    );
    assert_eq!(history_floor(&store.pool(), "shared", "entry-r").await, 2);

    store.close().await;
    remove_database(&path);
}
