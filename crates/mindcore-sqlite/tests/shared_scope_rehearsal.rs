//! The seven I-000009 facts on one isolated shared scope.
//!
//! One `shared:world` collection, one personal collection and two admitted viewpoints on the same
//! shared route: revision replay, the adoption anchor and its retirement chain, holder disclosure
//! before and after retirement, a personal write that leaves the shared row alone, the carrier
//! head settling with its entry inside one `BEGIN IMMEDIATE`, a closed store copied as a file and
//! reopened with every earlier fact intact, and two stores on that file with one write reservation
//! between them. Nothing here changes product code; the rehearsal is the deliverable. Each block
//! names the item it covers, so a failure maps to an acceptance line.

use std::str::FromStr;

use mindcore_base::cognition::CARRIER_NAMESPACE;
use mindcore_base::knowledge::{adoption_anchor_parts, KnowledgeScope};
use mindcore_spi::{adopt_knowledge_entry, AdoptionOutcome};
use mindcore_sqlite::cognition::read_records;
use mindcore_sqlite::{
    read_carrier_header, read_entry, write_entry, MindStore, RoutedKnowledgeStore,
};
use serde_json::{json, Value};
use spoke_operations::{KnowledgeEntryPort, ScopedKnowledgePort, SpokeRejectCode, SpokeResult};
use spoke_schemas::knowledge_entry::{
    KnowledgeEntryDisclosure, KnowledgeEntryModulesKey, KnowledgeEntryModulesValue,
    KnowledgeEntryOwner,
};
use spoke_schemas::{KnowledgeEntry, Scope};
use sqlx::{Sqlite, SqlitePool, Transaction};

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
/// The write item 7 first attempts while refused and then repeats after the reservation is gone.
const SWITCH_PROBE: &str = "the mill opens at dawn";

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
async fn source_row(pool: &SqlitePool, collection: &str, entry_id: &str) -> (i64, String) {
    sqlx::query_as(
        "SELECT revision, payload FROM knowledge_entries \
         WHERE collection = ? AND entry_id = ?",
    )
    .bind(collection)
    .bind(entry_id)
    .fetch_one(pool)
    .await
    .expect("read the shared source row")
}

async fn carrier_header(pool: &SqlitePool, collection: &str, entry_id: &str) -> (u64, Option<i64>) {
    let header = expect_ok(
        read_carrier_header(pool, collection, entry_id).await,
        "read the carrier header",
    );
    (header.revision, header.head_seq)
}

/// The pending rewrite as written inside its own reservation: revision, payload summary, record
/// rows and carrier head must all have moved before the rollback, or the post-rollback assertions
/// prove only that the old state is still there. Reads through the caller's transaction, so it
/// sees the uncommitted rewrite.
async fn expect_rewrite_written(tx: &mut Transaction<'_, Sqlite>, entry_id: &str) {
    let written = expect_ok(
        read_entry(tx, SHARED, entry_id).await,
        "carrier transaction: read the pending rewrite inside its transaction",
    );
    assert_eq!(
        written.revision,
        Some(2),
        "I-000009 (carrier transaction): the pending rewrite must be revision 2 before the rollback"
    );
    assert_eq!(
        written.body.summary.as_deref(),
        Some(CARRIER_REWRITE_SUMMARY),
        "I-000009 (carrier transaction): the pending rewrite must hold its own payload before the \
         rollback"
    );
    assert_eq!(
        expect_ok(
            read_records(&mut **tx, SHARED, entry_id).await,
            "carrier transaction: read the pending rewrite's records",
        )
        .len(),
        3,
        "I-000009 (carrier transaction): the pending rewrite must carry the appended record before \
         the rollback"
    );
    let header = expect_ok(
        read_carrier_header(&mut **tx, SHARED, entry_id).await,
        "carrier transaction: read the pending rewrite's head",
    );
    assert_eq!(
        (header.revision, header.head_seq),
        (2, Some(3)),
        "I-000009 (carrier transaction): the pending rewrite must move the revision and head \
         before the rollback"
    );
}

/// Positive control for a disclosure probe: both viewpoints read and list a live, non-private
/// entry, the owner lists its own private entry, and the foreign list omits it. Without this, a
/// list that always answers empty or a viewpoint refused every read would pass the negative
/// assertions below.
async fn expect_live_disclosure(
    served: &RoutedKnowledgeStore,
    owner: &Scope,
    foreign: &Scope,
    live_id: &str,
    private_id: &str,
) {
    for (viewpoint, scope) in [("owner", owner), ("foreign", foreign)] {
        assert_eq!(
            expect_ok(
                served.get_scoped_knowledge_entry(scope, live_id).await,
                "disclosure control: ordinary get of a live entry",
            )
            .revision,
            Some(1),
            "I-000009 (holder disclosure): the {viewpoint} viewpoint reads the live entry {live_id}"
        );
        let listed = expect_ok(
            served.list_scoped_knowledge_entries(scope).await,
            "disclosure control: ordinary list of the live collection",
        );
        assert!(
            listed.iter().any(|listed| listed.entry_id == live_id),
            "I-000009 (holder disclosure): the {viewpoint} viewpoint lists the live entry {live_id}"
        );
    }
    let owner_listed = expect_ok(
        served.list_scoped_knowledge_entries(owner).await,
        "disclosure control: the owner's list of the private entry",
    );
    assert!(
        owner_listed
            .iter()
            .any(|listed| listed.entry_id == private_id),
        "I-000009 (holder disclosure): the owner viewpoint lists its own private entry {private_id}"
    );
    let foreign_listed = expect_ok(
        served.list_scoped_knowledge_entries(foreign).await,
        "disclosure control: the foreign list of the private entry",
    );
    assert!(
        !foreign_listed
            .iter()
            .any(|listed| listed.entry_id == private_id),
        "I-000009 (holder disclosure): the foreign viewpoint's list omits the private entry \
         {private_id}"
    );
}

#[tokio::test]
async fn shared_scope_holds_the_seven_named_facts() {
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
    match expect_ok(
        adopt_knowledge_entry(&shared, &personal, "ke_gate", 2).await,
        "item 2: adopt ke_gate at revision 2",
    ) {
        AdoptionOutcome::Created(_) => {}
        other => panic!(
            "I-000009 item 2 (references): the first adoption must create the copy, got {other:?}"
        ),
    }
    // The anchor is a persisted property of the copy, so it is read back from the personal
    // collection: a later write that added the missing anchor would pass an assertion on the
    // adoption return value alone.
    let adopted = expect_ok(
        personal.get_knowledge_entry("ke_gate").await,
        "item 2: re-read the adopted copy from the personal collection",
    );
    assert_eq!(
        adoption_anchor_parts(&adopted),
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
            shared.resolve_adoption_anchor(&adopted).await,
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
                owner_private(
                    "ke_gate_note",
                    "the keeper keeps a private note",
                    OWNER_VIEWPOINT,
                ),
                None,
            )
            .await,
        "item 3: seed an owner-private entry",
    );
    let owner_view = viewpoint_scope(OWNER_VIEWPOINT);
    let foreign_view = viewpoint_scope(FOREIGN_VIEWPOINT);
    expect_live_disclosure(
        &served,
        &owner_view,
        &foreign_view,
        "ke_gate_tower",
        "ke_gate_note",
    )
    .await;
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
                entry(
                    "ke_gate_note_successor",
                    "note",
                    "the note moved to the mill",
                ),
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
                served
                    .get_scoped_knowledge_entry(scope, "ke_gate_note")
                    .await,
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
    let source_before = source_row(&pool, SHARED, "ke_gate").await;
    let mut promoted_copy = adopted.clone();
    promoted_copy.body.summary = Some(PERSONAL_NOTE.to_string());
    let promoted = expect_ok(
        personal.put_knowledge_entry(promoted_copy, Some(1)).await,
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
        source_row(&pool, SHARED, "ke_gate").await,
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
        carrier_header(&pool, SHARED, "ke_keeper_log").await,
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
    expect_rewrite_written(&mut rewrite, "ke_keeper_log").await;
    rewrite.rollback().await.expect("item 5: rollback");
    assert_eq!(
        carrier_header(&pool, SHARED, "ke_keeper_log").await,
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

    // I-000009 item 6 — backup / restore: a closed store, copied as a file and reopened, carries
    // items 1–5's persisted results, and items 3 and 5 run again on the copy.
    //
    // The copy happens only after `close`: a copy of a live WAL database's main file is not a
    // consistent snapshot, because committed frames may still sit in `-wal` rather than the main
    // file. Closing the last connection checkpoints them, so the copied file is the whole store.
    store.close().await;
    let restored_path = temp.root().join("restored.db");
    std::fs::copy(&path, &restored_path).expect("copy the closed store as a file");
    let restored = MindStore::open(&restored_path)
        .await
        .expect("reopen the copied store");
    // The same bindings against the copy, so the earlier probes run unchanged on it.
    let pool = restored.pool();
    let shared = restored.knowledge(KnowledgeScope::new(SHARED));
    let personal = restored.knowledge(KnowledgeScope::new(PERSONAL));
    let served = restored.served_knowledge(ROUTE, KnowledgeScope::new(SHARED));

    // Persisted facts are read off the copy before any setup write lands on it.
    assert_eq!(
        expect_ok(
            shared.get_entry_at_revision("ke_gate", 1).await,
            "item 6: replay revision 1 on the copy",
        )
        .body
        .summary
        .as_deref(),
        Some(GATE_ONE),
        "I-000009 item 6 (backup/restore): the copy replays revision 1's own payload"
    );
    assert_eq!(
        expect_ok(
            shared.get_entry_at_revision("ke_gate", 2).await,
            "item 6: replay revision 2 on the copy",
        )
        .body
        .summary
        .as_deref(),
        Some(GATE_TWO),
        "I-000009 item 6 (backup/restore): the copy replays revision 2's own payload"
    );
    let restored_copy = expect_ok(
        personal.get_knowledge_entry("ke_gate").await,
        "item 6: read the personal copy on the reopened store",
    );
    assert_eq!(
        adoption_anchor_parts(&restored_copy),
        Some(("ke_gate", Some(2))),
        "I-000009 item 6 (backup/restore): the personal copy keeps its anchor to the shared source \
         id and adopted revision"
    );
    assert_eq!(
        restored_copy.revision,
        Some(2),
        "I-000009 item 6 (backup/restore): the personal copy keeps the revision it was promoted to"
    );
    assert_eq!(
        restored_copy.body.summary.as_deref(),
        Some(PERSONAL_NOTE),
        "I-000009 item 6 (backup/restore): the personal copy keeps the payload it was promoted to"
    );
    assert_eq!(
        expect_ok(
            shared.resolve_adoption_anchor(&restored_copy).await,
            "item 6: resolve the anchor through the retirement chain on the copy",
        ),
        "ke_gate_tower",
        "I-000009 item 6 (backup/restore): the retirement chain survives the copy"
    );
    assert_eq!(
        expect_ok(
            shared.get_entry_including_superseded("ke_gate_note").await,
            "item 6: explicit lifecycle read on the copy",
        )
        .superseded_by
        .as_deref(),
        Some("ke_gate_note_successor"),
        "I-000009 item 6 (backup/restore): the retired row keeps its marker on the copy"
    );
    assert_eq!(
        source_row(&pool, SHARED, "ke_gate").await,
        source_before,
        "I-000009 item 6 (backup/restore): the shared row's revision and payload bytes survive the \
         copy"
    );
    assert_eq!(
        carrier_header(&pool, SHARED, "ke_keeper_log").await,
        (1, Some(2)),
        "I-000009 item 6 (backup/restore): the original carrier keeps its revision and head on the \
         copy"
    );
    assert_eq!(
        expect_ok(
            shared.get_knowledge_entry("ke_keeper_log").await,
            "item 6: read the original carrier on the copy",
        )
        .body
        .summary
        .as_deref(),
        Some(CARRIER_SUMMARY),
        "I-000009 item 6 (backup/restore): the original carrier keeps its committed payload on the \
         copy"
    );
    assert_eq!(
        expect_ok(
            read_records(&pool, SHARED, "ke_keeper_log").await,
            "item 6: read the original carrier's records on the copy",
        )
        .iter()
        .filter_map(|record| record["record_id"].as_str())
        .collect::<Vec<_>>(),
        ["cog_1", "cog_2"],
        "I-000009 item 6 (backup/restore): the original carrier keeps its committed records on the \
         copy"
    );

    // Item 3 is re-probed on the copy under ids this file has not written yet: the same viewpoint
    // split for a live entry, the same ordinary absence for both viewpoints once retired, and the
    // same viewpoint-free explicit marker read.
    expect_ok(
        shared
            .put_knowledge_entry(
                owner_private(
                    "ke_gate_note_copy",
                    "the keeper's copied note",
                    OWNER_VIEWPOINT,
                ),
                None,
            )
            .await,
        "item 6: seed a live owner-private entry on the copy",
    );
    expect_live_disclosure(
        &served,
        &owner_view,
        &foreign_view,
        "ke_gate_tower",
        "ke_gate_note_copy",
    )
    .await;
    assert_eq!(
        expect_ok(
            served
                .get_scoped_knowledge_entry(&owner_view, "ke_gate_note_copy")
                .await,
            "item 6: owner viewpoint get on the copy",
        )
        .revision,
        Some(1),
        "I-000009 item 6 (backup/restore): a live entry still discloses to its owner on the copy"
    );
    assert_eq!(
        expect_reject(
            served
                .get_scoped_knowledge_entry(&foreign_view, "ke_gate_note_copy")
                .await,
            "item 6: foreign viewpoint get on the copy",
        ),
        SpokeRejectCode::KnowledgeEntryNotFound,
        "I-000009 item 6 (backup/restore): a foreign viewpoint still gets the same absence on the \
         copy"
    );
    expect_ok(
        shared
            .put_knowledge_entry(
                entry(
                    "ke_gate_note_copy_successor",
                    "note",
                    "the copied note moved on",
                ),
                None,
            )
            .await,
        "item 6: seed the copied note's successor",
    );
    expect_ok(
        shared
            .supersede_entry("ke_gate_note_copy", 1, "ke_gate_note_copy_successor")
            .await,
        "item 6: retire the copied note",
    );
    for (viewpoint, scope) in [("owner", &owner_view), ("foreign", &foreign_view)] {
        assert_eq!(
            expect_reject(
                served.get_scoped_knowledge_entry(scope, "ke_gate_note_copy").await,
                "item 6: ordinary get of the retired copy note",
            ),
            SpokeRejectCode::KnowledgeEntryNotFound,
            "I-000009 item 6 (backup/restore): a retired entry is absent from ordinary get for the \
             {viewpoint} viewpoint on the copy"
        );
        let listed = expect_ok(
            served.list_scoped_knowledge_entries(scope).await,
            "item 6: ordinary list after retirement on the copy",
        );
        assert!(
            !listed
                .iter()
                .any(|listed| listed.entry_id == "ke_gate_note_copy"),
            "I-000009 item 6 (backup/restore): a retired entry is absent from ordinary list for the \
             {viewpoint} viewpoint on the copy"
        );
    }
    let marked = expect_ok(
        shared
            .get_entry_including_superseded("ke_gate_note_copy")
            .await,
        "item 6: explicit lifecycle read of the retired copy note",
    );
    assert_eq!(
        marked.superseded_by.as_deref(),
        Some("ke_gate_note_copy_successor"),
        "I-000009 item 6 (backup/restore): the explicit lifecycle read returns the marker without \
         a viewpoint on the copy"
    );

    // Item 5 is re-probed on the copy with a carrier id this file has not written yet.
    let mut write = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("item 6: begin immediate on the copy");
    expect_ok(
        write_entry(
            &mut write,
            SHARED,
            carrier("ke_keeper_log_copy", 2, CARRIER_SUMMARY),
            None,
        )
        .await,
        "item 6: create the carrier on the copy inside the reservation",
    );
    write.commit().await.expect("item 6: commit on the copy");
    assert_eq!(
        carrier_header(&pool, SHARED, "ke_keeper_log_copy").await,
        (1, Some(2)),
        "I-000009 item 6 (backup/restore): the carrier head and entry commit together on the copy"
    );
    assert_eq!(
        expect_ok(
            read_records(&pool, SHARED, "ke_keeper_log_copy").await,
            "item 6: read the committed records on the copy",
        )
        .len(),
        2,
        "I-000009 item 6 (backup/restore): the committed head names the two records on the copy"
    );
    let mut rewrite = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("item 6: begin immediate for the rolled-back write on the copy");
    expect_ok(
        write_entry(
            &mut rewrite,
            SHARED,
            carrier("ke_keeper_log_copy", 3, CARRIER_REWRITE_SUMMARY),
            Some(1),
        )
        .await,
        "item 6: extend the carrier on the copy inside the reservation",
    );
    expect_rewrite_written(&mut rewrite, "ke_keeper_log_copy").await;
    rewrite
        .rollback()
        .await
        .expect("item 6: rollback on the copy");
    assert_eq!(
        carrier_header(&pool, SHARED, "ke_keeper_log_copy").await,
        (1, Some(2)),
        "I-000009 item 6 (backup/restore): a rollback on the copy leaves revision and head unchanged"
    );
    let after = expect_ok(
        shared.get_knowledge_entry("ke_keeper_log_copy").await,
        "item 6: read the carrier after the rollback on the copy",
    );
    assert_eq!(
        after.body.summary.as_deref(),
        Some(CARRIER_SUMMARY),
        "I-000009 item 6 (backup/restore): the rollback on the copy kept the committed payload, not \
         the attempted one"
    );
    assert_eq!(
        expect_ok(
            read_records(&pool, SHARED, "ke_keeper_log_copy").await,
            "item 6: read the records after the rollback on the copy",
        )
        .len(),
        2,
        "I-000009 item 6 (backup/restore): the rolled-back record never became visible on the copy"
    );

    // I-000009 item 7 — single-writer switch: two stores opened on the same file before any
    // reservation is taken. The second store's valid write is refused while the first holds the
    // write reservation, leaves no row, and succeeds once the first closes. This asserts SQLite
    // exclusion, not supported simultaneous writers: `MindStore`'s invariant is one store per
    // file, and two stores on one file are unsupported rather than refused.
    let second = MindStore::open(&restored_path)
        .await
        .expect("open a second store on the restored file");
    // Only the second store's sole connection gets a short timeout, so contention fails fast
    // instead of paying the production five-second busy handler.
    let mut connection = second
        .pool()
        .acquire()
        .await
        .expect("acquire the second store's single connection");
    sqlx::query("PRAGMA busy_timeout = 0")
        .execute(&mut *connection)
        .await
        .expect("shorten the second store's busy timeout");
    drop(connection);
    let second_shared = second.knowledge(KnowledgeScope::new(SHARED));

    let reservation = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("item 7: hold the write reservation on the first store");
    let refusal = match second_shared
        .put_knowledge_entry(entry("ke_switch_probe", "note", SWITCH_PROBE), None)
        .await
    {
        SpokeResult::Ok(written) => panic!(
            "I-000009 item 7 (single-writer switch): the second store wrote while the first held \
             the write reservation: {written:?}"
        ),
        SpokeResult::Reject(reject) => reject,
    };
    assert_eq!(
        refusal.code,
        SpokeRejectCode::InternalError,
        "I-000009 item 7 (single-writer switch): the refusal is a storage failure, not a \
         validation answer"
    );
    assert!(
        refusal.message.contains("knowledge write could not begin"),
        "I-000009 item 7 (single-writer switch): the write must be refused at the reservation, \
         got: {}",
        refusal.message
    );
    assert!(
        refusal.message.contains("locked"),
        "I-000009 item 7 (single-writer switch): the refusal must carry SQLite's lock condition, \
         got: {}",
        refusal.message
    );
    assert_eq!(
        expect_reject(
            second_shared.get_knowledge_entry("ke_switch_probe").await,
            "item 7: read the refused entry",
        ),
        SpokeRejectCode::KnowledgeEntryNotFound,
        "I-000009 item 7 (single-writer switch): the refused write left no partial row"
    );

    reservation
        .rollback()
        .await
        .expect("item 7: release the write reservation");
    restored.close().await;
    expect_ok(
        second_shared
            .put_knowledge_entry(entry("ke_switch_probe", "note", SWITCH_PROBE), None)
            .await,
        "item 7: the same write after the first store closes",
    );
    // Read through the second store: the put's return value is the submission, not the row.
    let landed = expect_ok(
        second_shared.get_knowledge_entry("ke_switch_probe").await,
        "item 7: read the switched entry through the second store",
    );
    assert_eq!(
        landed.revision,
        Some(1),
        "I-000009 item 7 (single-writer switch): the same write succeeds once the first store \
         closes"
    );
    assert_eq!(
        landed.body.summary.as_deref(),
        Some(SWITCH_PROBE),
        "I-000009 item 7 (single-writer switch): the successful write landed its own payload"
    );
    second.close().await;
}
