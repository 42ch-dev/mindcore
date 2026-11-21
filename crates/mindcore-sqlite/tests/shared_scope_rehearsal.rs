//! The first five I-000009 live-scope facts on one isolated shared scope.
//!
//! One `shared:world` collection, one personal collection and two admitted viewpoints on the same
//! shared route: revision replay, the adoption anchor and its retirement chain, holder disclosure
//! before and after retirement, a personal write that leaves the shared row alone, and the carrier
//! head settling with its entry inside one `BEGIN IMMEDIATE`. Nothing here changes product code;
//! the rehearsal is the deliverable. Each block names the item it covers, so a failure maps to an
//! acceptance line.

use std::str::FromStr;

use mindcore_base::cognition::CARRIER_NAMESPACE;
use mindcore_base::knowledge::{adoption_anchor_parts, KnowledgeScope};
use mindcore_spi::{adopt_knowledge_entry, AdoptionOutcome};
use mindcore_sqlite::cognition::read_records;
use mindcore_sqlite::{read_carrier_header, write_entry, MindStore};
use serde_json::{json, Value};
use spoke_operations::{KnowledgeEntryPort, ScopedKnowledgePort, SpokeRejectCode};
use spoke_schemas::knowledge_entry::{
    KnowledgeEntryDisclosure, KnowledgeEntryModulesKey, KnowledgeEntryModulesValue,
    KnowledgeEntryOwner,
};
use spoke_schemas::{KnowledgeEntry, Scope};
use sqlx::SqlitePool;

#[allow(dead_code)]
#[path = "support/embedded_story.rs"]
mod embedded_story;

use embedded_story::{entry, expect_ok, expect_reject, TempDb};

const SHARED: &str = "shared:world";
const PERSONAL: &str = "wren";
/// The one route the served face admits for the shared collection.
const ROUTE: &str = "shared-route";
const OWNER_VIEWPOINT: &str = "holder_one";
const FOREIGN_VIEWPOINT: &str = "holder_two";

const GATE_ONE: &str = "the north gate shuts after dusk";
const GATE_TWO: &str = "the north gate shuts after dusk; a keeper walks it";
const PERSONAL_NOTE: &str = "wren's own note: the keeper carries the key";
const CARRIER_SUMMARY: &str = "the keeper's beliefs";
/// The attempted rewrite's summary differs from the committed one, so the rollback probe can tell
/// a leaked payload from the committed payload.
const CARRIER_REWRITE_SUMMARY: &str = "the keeper's amended beliefs";

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

/// A carrier entry holding `records` inline, the shape a full put accepts.
fn carrier(entry_id: &str, records: usize, summary: &str) -> KnowledgeEntry {
    let mut entry = entry(entry_id, "character", summary);
    let records: Vec<Value> = (1..=records)
        .map(|index| belief(entry_id, &format!("cog_{index}")))
        .collect();
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

/// One current-view belief record held by `holder`: the shape a carrier write accepts.
fn belief(holder: &str, record_id: &str) -> Value {
    let holder_reference = json!({
        "kind": "knowledge_entry",
        "id": holder,
        "namespace": CARRIER_NAMESPACE,
    });
    json!({
        "kind": "belief",
        "order": 1,
        "record_id": record_id,
        "holder": holder_reference,
        "target": {
            "carrier": {
                "kind": "knowledge_entry",
                "id": "ke_gate",
                "namespace": CARRIER_NAMESPACE,
            },
            "target_id": "prop_north_gate_closed",
            "kind": "proposition",
        },
        "assessor": holder_reference,
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

/// An owner-private entry: only its owner's viewpoint reaches the disclosure predicate.
fn owner_private(entry_id: &str, summary: &str, owner: &str) -> KnowledgeEntry {
    let mut entry = entry(entry_id, "note", summary);
    entry.disclosure =
        Some(KnowledgeEntryDisclosure::from_str("owner-private").expect("non-empty disclosure"));
    entry.owner = Some(KnowledgeEntryOwner::try_from(owner).expect("non-empty owner"));
    entry
}

fn viewpoint_scope(viewpoint: &str) -> Scope {
    serde_json::from_value(json!({ "scope_id": ROUTE, "viewpoint": viewpoint }))
        .expect("scope JSON matches the schema")
}

/// The shared source row as stored: revision and payload bytes, read from the column because no
/// product read hands out the bytes promotion must leave alone.
async fn source_row(pool: &SqlitePool) -> (i64, String) {
    sqlx::query_as(
        "SELECT revision, payload FROM knowledge_entries \
         WHERE collection = ? AND entry_id = ?",
    )
    .bind(SHARED)
    .bind("ke_gate")
    .fetch_one(pool)
    .await
    .expect("read the shared source row")
}

async fn carrier_header(pool: &SqlitePool) -> (u64, Option<i64>) {
    let header = expect_ok(
        read_carrier_header(pool, SHARED, "ke_keeper_log").await,
        "item 5: read the carrier header",
    );
    (header.revision, header.head_seq)
}

#[tokio::test]
async fn shared_scope_holds_the_first_five_live_facts() {
    let temp = TempDb::new("shared-scope-rehearsal");
    let path = temp.database_path();
    let store = MindStore::open(&path).await.expect("open store");
    let pool = store.pool();
    let shared = store.knowledge(KnowledgeScope::new(SHARED));
    let personal = store.knowledge(KnowledgeScope::new(PERSONAL));
    let served = store.served_knowledge(ROUTE, KnowledgeScope::new(SHARED));

    // I-000009 item 1 — revisions: a read as of an earlier revision returns that revision's
    // payload, not the head's.
    expect_ok(
        shared
            .put_knowledge_entry(entry("ke_gate", "location", GATE_ONE), None)
            .await,
        "item 1: create ke_gate",
    );
    expect_ok(
        shared
            .put_knowledge_entry(entry("ke_gate", "location", GATE_TWO), Some(1))
            .await,
        "item 1: second revision",
    );
    assert_eq!(
        expect_ok(
            shared.get_entry_at_revision("ke_gate", 1).await,
            "item 1: replay revision 1",
        )
        .body
        .summary
        .as_deref(),
        Some(GATE_ONE),
        "I-000009 item 1 (revisions): revision 1 must replay its own payload"
    );
    assert_eq!(
        expect_ok(
            shared.get_entry_at_revision("ke_gate", 2).await,
            "item 1: replay revision 2",
        )
        .body
        .summary
        .as_deref(),
        Some(GATE_TWO),
        "I-000009 item 1 (revisions): revision 2 must replay its own payload"
    );

    // I-000009 item 2 — references: the personal copy carries an adoption anchor naming its shared
    // source id and adopted revision; resolving from that copy outward through the shared
    // collection follows retirement markers to the first live entry.
    let copy = match expect_ok(
        adopt_knowledge_entry(&shared, &personal, "ke_gate", 2).await,
        "item 2: adopt ke_gate at revision 2",
    ) {
        AdoptionOutcome::Created(copy) => copy,
        other => panic!("I-000009 item 2 (references): the first adoption must create the copy, got {other:?}"),
    };
    assert_eq!(
        adoption_anchor_parts(&copy),
        Some(("ke_gate", Some(2))),
        "I-000009 item 2 (references): the copy anchors to its shared source id and adopted revision"
    );
    for (entry_id, summary) in [
        ("ke_gate_rail", "the rail replaces the gate"),
        ("ke_gate_tower", "the tower replaces the rail"),
    ] {
        expect_ok(
            shared
                .put_knowledge_entry(entry(entry_id, "location", summary), None)
                .await,
            "item 2: seed the retirement successor",
        );
    }
    assert_eq!(
        expect_ok(
            shared.supersede_entry("ke_gate", 2, "ke_gate_rail").await,
            "item 2: retire the adopted source",
        ),
        3,
        "item 2: the source advances one revision for its marker"
    );
    assert_eq!(
        expect_ok(
            shared
                .supersede_entry("ke_gate_rail", 1, "ke_gate_tower")
                .await,
            "item 2: retire the first successor",
        ),
        2
    );
    assert_eq!(
        expect_ok(
            shared.resolve_adoption_anchor(&copy).await,
            "item 2: resolve the anchor through the retirement chain",
        ),
        "ke_gate_tower",
        "I-000009 item 2 (references): resolution must follow same-collection markers to the first live entry"
    );

    // I-000009 item 3 — holder/binding disclosure: two admitted viewpoints get different answers
    // for the same live entry; once it is retired, ordinary reads hide it from both, while the
    // explicit lifecycle read exposes the marker with no viewpoint input.
    expect_ok(
        shared
            .put_knowledge_entry(
                owner_private("ke_gate_note", "the keeper keeps a private note", OWNER_VIEWPOINT),
                None,
            )
            .await,
        "item 3: seed an owner-private entry",
    );
    let owner_view = viewpoint_scope(OWNER_VIEWPOINT);
    let foreign_view = viewpoint_scope(FOREIGN_VIEWPOINT);
    assert_eq!(
        expect_ok(
            served
                .get_scoped_knowledge_entry(&owner_view, "ke_gate_note")
                .await,
            "item 3: owner viewpoint get of the live entry",
        )
        .revision,
        Some(1),
        "I-000009 item 3 (holder disclosure): the owner viewpoint reads the live entry"
    );
    assert_eq!(
        expect_reject(
            served
                .get_scoped_knowledge_entry(&foreign_view, "ke_gate_note")
                .await,
            "item 3: foreign viewpoint get of the live entry",
        ),
        SpokeRejectCode::KnowledgeEntryNotFound,
        "I-000009 item 3 (holder disclosure): a foreign viewpoint gets a different answer for the same live entry"
    );
    expect_ok(
        shared
            .put_knowledge_entry(
                entry("ke_gate_note_successor", "note", "the note moved to the mill"),
                None,
            )
            .await,
        "item 3: seed the note's successor",
    );
    expect_ok(
        shared
            .supersede_entry("ke_gate_note", 1, "ke_gate_note_successor")
            .await,
        "item 3: retire the note",
    );
    for (viewpoint, scope) in [("owner", &owner_view), ("foreign", &foreign_view)] {
        assert_eq!(
            expect_reject(
                served.get_scoped_knowledge_entry(scope, "ke_gate_note").await,
                "item 3: ordinary get of the retired entry",
            ),
            SpokeRejectCode::KnowledgeEntryNotFound,
            "I-000009 item 3 (holder disclosure): a retired entry is absent from ordinary get \
             for the {viewpoint} viewpoint"
        );
        let listed = expect_ok(
            served.list_scoped_knowledge_entries(scope).await,
            "item 3: ordinary list after retirement",
        );
        assert!(
            !listed
                .iter()
                .any(|listed| listed.entry_id == "ke_gate_note"),
            "I-000009 item 3 (holder disclosure): a retired entry is absent from ordinary list \
             for the {viewpoint} viewpoint"
        );
    }
    let marked = expect_ok(
        shared.get_entry_including_superseded("ke_gate_note").await,
        "item 3: explicit lifecycle read of the retired entry",
    );
    assert_eq!(
        marked.superseded_by.as_deref(),
        Some("ke_gate_note_successor"),
        "I-000009 item 3 (holder disclosure): the explicit lifecycle read takes no viewpoint and \
         still returns the marker"
    );
    assert_eq!(
        marked.entry.revision,
        Some(2),
        "I-000009 item 3 (holder disclosure): the explicit lifecycle read returns the retired row"
    );

    // I-000009 item 4 — promotion: writing the personal copy leaves the shared source's revision
    // and stored payload bytes untouched.
    let source_before = source_row(&pool).await;
    let mut promoted_copy = copy.clone();
    promoted_copy.body.summary = Some(PERSONAL_NOTE.to_string());
    let promoted = expect_ok(
        personal
            .put_knowledge_entry(promoted_copy, Some(1))
            .await,
        "item 4: write the personal copy",
    );
    assert_eq!(
        promoted.revision,
        Some(2),
        "item 4: the personal write lands on the copy"
    );
    assert_eq!(
        promoted.body.summary.as_deref(),
        Some(PERSONAL_NOTE),
        "item 4: the personal write lands its own payload"
    );
    assert_eq!(
        adoption_anchor_parts(&promoted),
        Some(("ke_gate", Some(2))),
        "item 4: the personal write keeps the copy's anchor"
    );
    assert_eq!(
        source_row(&pool).await,
        source_before,
        "I-000009 item 4 (promotion): the personal write must leave the shared row's revision and \
         payload bytes unchanged"
    );

    // I-000009 item 5 — carrier transaction invariants: the carrier head and the entry land in one
    // `BEGIN IMMEDIATE`, and a rollback leaves neither visible.
    let mut write = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("item 5: begin immediate");
    expect_ok(
        write_entry(
            &mut write,
            SHARED,
            carrier("ke_keeper_log", 2, CARRIER_SUMMARY),
            None,
        )
        .await,
        "item 5: create the carrier inside the reservation",
    );
    write.commit().await.expect("item 5: commit");
    assert_eq!(
        carrier_header(&pool).await,
        (1, Some(2)),
        "I-000009 item 5 (carrier transaction): the committed carrier holds revision 1 and head 2 \
         together"
    );
    assert_eq!(
        expect_ok(
            shared.get_knowledge_entry("ke_keeper_log").await,
            "item 5: read the committed carrier",
        )
        .revision,
        Some(1),
        "I-000009 item 5 (carrier transaction): the committed entry is revision 1"
    );
    assert_eq!(
        expect_ok(
            read_records(&pool, SHARED, "ke_keeper_log").await,
            "item 5: read the committed records",
        )
        .len(),
        2,
        "I-000009 item 5 (carrier transaction): the committed head names the two stored records"
    );

    let mut rewrite = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("item 5: begin immediate for the rolled-back write");
    expect_ok(
        write_entry(
            &mut rewrite,
            SHARED,
            carrier("ke_keeper_log", 3, CARRIER_REWRITE_SUMMARY),
            Some(1),
        )
        .await,
        "item 5: extend the carrier inside the reservation",
    );
    rewrite.rollback().await.expect("item 5: rollback");
    assert_eq!(
        carrier_header(&pool).await,
        (1, Some(2)),
        "I-000009 item 5 (carrier transaction): a rollback leaves the revision and head unchanged"
    );
    let after = expect_ok(
        shared.get_knowledge_entry("ke_keeper_log").await,
        "item 5: read the carrier after the rollback",
    );
    assert_eq!(
        after.revision,
        Some(1),
        "I-000009 item 5 (carrier transaction): the rolled-back entry never became visible"
    );
    assert_eq!(
        after.body.summary.as_deref(),
        Some(CARRIER_SUMMARY),
        "I-000009 item 5 (carrier transaction): the rollback kept the committed payload, not the \
         attempted one",
    );
    assert_eq!(
        expect_ok(
            read_records(&pool, SHARED, "ke_keeper_log").await,
            "item 5: read the records after the rollback",
        )
        .len(),
        2,
        "I-000009 item 5 (carrier transaction): the rolled-back record never became visible"
    );

    store.close().await;
}
