//! The served knowledge port's per-operation authorization: the `I-000137` regression.
//!
//! The port is driven the way a composed face drives it — with scopes the test binds through
//! `mindcore_sqlite::bind_caller` — over the production `RoutedKnowledgeStore`. Before the guard,
//! `put_scoped_knowledge_entry` appended cognition records for any admitted scope; these tests
//! prove the refusal happens before any record is written, and that a caller holding one grant
//! covering both capabilities still appends. The wire path that writes the binding is layer 2's.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use serde_json::{json, Value};
use sqlx::SqlitePool;
use tempfile::TempDir;

use mindcore_base::boundary::CallerId;
use mindcore_base::cognition::CARRIER_NAMESPACE;
use mindcore_base::ids::MindId;
use mindcore_base::knowledge::KnowledgeScope;
use mindcore_spoke::MindKnowledgePort;
use mindcore_sqlite::minds::{knowledge_collection_of, CreateMind};
use mindcore_sqlite::{
    bind_caller, bound_caller, MindStore, RoutedKnowledgeStore, CALLER_NAMESPACE,
};
use spoke_operations::{ScopedKnowledgePort, SpokeReject, SpokeRejectCode, SpokeResult};
use spoke_schemas::knowledge_entry::{
    KnowledgeEntryBody, KnowledgeEntryCanonicalName, KnowledgeEntryModulesKey,
    KnowledgeEntryModulesValue,
};
use spoke_schemas::{KnowledgeEntry, Scope};

const MIND: &str = "spc_wren";
const OWNER: &str = "p_wren";
const WRITER: &str = "p_writer";
const READER: &str = "p_reader";
const HALF_WRITER: &str = "p_half_writer";
const STRANGER: &str = "p_stranger";
const NOW: &str = "2026-11-26T09:00:00.000Z";

/// One Mind created through the library's own entry point, so the space, carrier and the owner's
/// default grant are exactly what a served face would find.
struct Harness {
    _dir: TempDir,
    pool: SqlitePool,
    port: MindKnowledgePort,
    routed: Arc<RoutedKnowledgeStore>,
}

impl Harness {
    async fn open() -> Self {
        let dir = TempDir::new().expect("temporary directory");
        let store = MindStore::open(&dir.path().join("served_knowledge.db"))
            .await
            .expect("open store");
        let mut tx = store.pool().begin().await.expect("begin");
        let created = mindcore_sqlite::minds::create(
            &mut tx,
            &CreateMind {
                mind_id: Some(MindId::new(MIND)),
                display_name: None,
                owner: CallerId::new(OWNER),
            },
        )
        .await;
        match created {
            SpokeResult::Ok(_) => {}
            SpokeResult::Reject(reject) => panic!("seed the mind: {reject:?}"),
        }
        tx.commit().await.expect("commit mind");

        let collection = KnowledgeScope::new(knowledge_collection_of(&MindId::new(MIND)));
        let routed = Arc::new(store.served_knowledge(MIND, collection));
        let port = MindKnowledgePort::new(store.pool(), routed.clone());
        Self {
            _dir: dir,
            pool: store.pool(),
            port,
            routed,
        }
    }

    async fn seed_grant(&self, principal: &str, capabilities: &[&str]) {
        sqlx::query(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
              shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES (?, ?, NULL, ?, ?, '[]', 0, NULL, NULL, 1, ?)",
        )
        .bind(format!("grt_{principal}"))
        .bind(MIND)
        .bind(principal)
        .bind(serde_json::to_string(capabilities).expect("capabilities"))
        .bind(NOW)
        .execute(&self.pool)
        .await
        .expect("seed grant");
    }

    /// Rows the entry holds in each table a served put would write.
    async fn rows_of(&self, entry_id: &str) -> (i64, i64, i64) {
        let entries: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
        )
        .bind(collection_of(MIND))
        .bind(entry_id)
        .fetch_one(&self.pool)
        .await
        .expect("count entries");
        let records: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM cognition_records WHERE collection = ? AND entry_id = ?",
        )
        .bind(collection_of(MIND))
        .bind(entry_id)
        .fetch_one(&self.pool)
        .await
        .expect("count records");
        let revisions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM knowledge_entry_revisions WHERE collection = ? AND entry_id = ?",
        )
        .bind(collection_of(MIND))
        .bind(entry_id)
        .fetch_one(&self.pool)
        .await
        .expect("count revisions");
        (entries, records, revisions)
    }
}

fn collection_of(mind: &str) -> String {
    knowledge_collection_of(&MindId::new(mind))
}

/// The admitted scope, with the caller bound the way layer 2's callback will bind it.
fn bound_scope(principal: &str) -> Scope {
    let mut scope: Scope =
        serde_json::from_value(json!({ "scope_id": MIND })).expect("scope JSON matches the schema");
    bind_caller(&mut scope, principal);
    scope
}

fn entry_of(entry_id: &str, records: Vec<Value>) -> KnowledgeEntry {
    let mut modules = HashMap::new();
    modules.insert(
        KnowledgeEntryModulesKey::try_from("cognition").expect("valid module key"),
        KnowledgeEntryModulesValue::from(
            json!({ "records": records, "world_facts": [], "extensions": {} })
                .as_object()
                .expect("module fixture is an object")
                .clone(),
        ),
    );
    KnowledgeEntry {
        adoption_anchor: None,
        body: KnowledgeEntryBody {
            summary: Some("served entry".to_string()),
            ..KnowledgeEntryBody::default()
        },
        canonical_name: KnowledgeEntryCanonicalName::try_from(format!("{entry_id} name"))
            .expect("non-empty canonical name"),
        created_at: None,
        disclosure: None,
        entry_id: entry_id.to_string(),
        entry_type: "character".to_string(),
        extensions: HashMap::new(),
        modules,
        owner: None,
        revision: None,
        schema_version: NonZeroU64::new(1).expect("non-zero wire baseline"),
        source_anchor: None,
        status: "confirmed".to_string(),
        updated_at: None,
    }
}

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
                "id": "ke_shared_north_gate",
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

fn expect_ok<T>(result: SpokeResult<T>, context: &str) -> T {
    match result {
        SpokeResult::Ok(value) => value,
        SpokeResult::Reject(reject) => panic!("{context}: unexpectedly rejected with {reject:?}"),
    }
}

fn expect_refused<T: std::fmt::Debug>(result: SpokeResult<T>, context: &str) -> SpokeReject {
    match result {
        SpokeResult::Ok(value) => panic!("{context}: unexpectedly accepted with {value:?}"),
        SpokeResult::Reject(reject) => reject,
    }
}

fn assert_fixed_refusal(reject: &SpokeReject, context: &str) {
    assert_eq!(
        reject.code,
        SpokeRejectCode::NotAuthorized,
        "{context}: refused on the fixed answer"
    );
    assert_eq!(
        reject.message, "knowledge scope not authorized",
        "{context}: Connect's own admission-deny message"
    );
    assert!(
        reject.details.is_none(),
        "{context}: the refusal carries no details: {reject:?}"
    );
}

/// The path that was unguarded: an admitted scope's put appended cognition records for a caller
/// holding no `knowledge_write`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_caller_without_knowledge_write_is_refused_before_any_record_is_written() {
    let harness = Harness::open().await;
    harness
        .seed_grant(READER, &["recall", "knowledge_read"])
        .await;
    let scope = bound_scope(READER);
    let entry = entry_of("ke_reader", vec![belief("ke_reader", "cog_reader")]);

    let refused = expect_refused(
        harness
            .port
            .put_scoped_knowledge_entry(&scope, entry, None)
            .await,
        "a put by a caller without knowledge_write",
    );
    assert_fixed_refusal(&refused, "put without knowledge_write");

    assert_eq!(
        harness.rows_of("ke_reader").await,
        (0, 0, 0),
        "the refusal landed before any entry, record or revision row was written"
    );
}

/// The control that shows where the guard is: the same put straight to the storage port still
/// appends, because the routed store is the storage port and carries no grant rule.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_unwrapped_store_appends_without_a_grant() {
    let harness = Harness::open().await;
    let scope = bound_scope(STRANGER);
    let entry = entry_of("ke_stranger", vec![belief("ke_stranger", "cog_stranger")]);

    let stored = expect_ok(
        harness
            .routed
            .put_scoped_knowledge_entry(&scope, entry, None)
            .await,
        "the unwrapped store's put",
    );
    assert_eq!(stored.entry_id, "ke_stranger");
    assert_eq!(
        harness.rows_of("ke_stranger").await.1,
        1,
        "the storage port appended the record, which is the behaviour the port now guards"
    );
}

/// A caller holding the covering grant still appends: the cognition append is guarded, not
/// removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_caller_with_knowledge_write_appends_cognition_records() {
    let harness = Harness::open().await;
    harness
        .seed_grant(WRITER, &["knowledge_read", "knowledge_write"])
        .await;
    let scope = bound_scope(WRITER);
    let entry = entry_of("ke_writer", vec![belief("ke_writer", "cog_writer")]);

    let stored = expect_ok(
        harness
            .port
            .put_scoped_knowledge_entry(&scope, entry, None)
            .await,
        "a put by a caller holding both capabilities",
    );
    assert_eq!(stored.entry_id, "ke_writer");
    assert_eq!(stored.revision, Some(1));
    assert_eq!(
        harness.rows_of("ke_writer").await,
        (1, 1, 0),
        "the entry and its one cognition record are written"
    );
}

/// One grant must cover both capabilities: `knowledge_write` alone is refused, because Connect
/// reads the entry through `get` before it writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_served_put_requires_one_grant_covering_write_and_read() {
    let harness = Harness::open().await;
    harness
        .seed_grant(HALF_WRITER, &["knowledge_read", "recall"])
        .await;
    harness
        .seed_grant("p_write_only", &["knowledge_write"])
        .await;

    for (principal, label) in [
        (HALF_WRITER, "knowledge_read only"),
        ("p_write_only", "knowledge_write only"),
    ] {
        let scope = bound_scope(principal);
        let entry = entry_of(
            &format!("ke_{principal}"),
            vec![belief(
                &format!("ke_{principal}"),
                &format!("cog_{principal}"),
            )],
        );
        let refused = expect_refused(
            harness
                .port
                .put_scoped_knowledge_entry(&scope, entry, None)
                .await,
            label,
        );
        assert_fixed_refusal(&refused, label);
        assert_eq!(
            harness.rows_of(&format!("ke_{principal}")).await,
            (0, 0, 0),
            "{label}: nothing was written"
        );
    }
}

/// `get` and `list` require `knowledge_read`, and `knowledge_write` alone is not a read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_and_list_require_knowledge_read() {
    let harness = Harness::open().await;
    harness
        .seed_grant(WRITER, &["knowledge_read", "knowledge_write"])
        .await;
    harness
        .seed_grant("p_write_only", &["knowledge_write"])
        .await;
    expect_ok(
        harness
            .port
            .put_scoped_knowledge_entry(
                &bound_scope(WRITER),
                entry_of("ke_shared", vec![belief("ke_shared", "cog_shared")]),
                None,
            )
            .await,
        "seed an entry through the port",
    );

    expect_ok(
        harness
            .port
            .get_scoped_knowledge_entry(&bound_scope(WRITER), "ke_shared")
            .await,
        "a reader gets the entry",
    );
    expect_ok(
        harness
            .port
            .list_scoped_knowledge_entries(&bound_scope(WRITER))
            .await,
        "a reader lists the collection",
    );

    let write_only = bound_scope("p_write_only");
    assert_fixed_refusal(
        &expect_refused(
            harness
                .port
                .get_scoped_knowledge_entry(&write_only, "ke_shared")
                .await,
            "get with knowledge_write alone",
        ),
        "get with knowledge_write alone",
    );
    assert_fixed_refusal(
        &expect_refused(
            harness
                .port
                .list_scoped_knowledge_entries(&write_only)
                .await,
            "list with knowledge_write alone",
        ),
        "list with knowledge_write alone",
    );
}

/// A scope the admission never bound has no caller to authorize, so every method refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scope_without_a_bound_caller_is_refused() {
    let harness = Harness::open().await;
    harness
        .seed_grant(WRITER, &["knowledge_read", "knowledge_write"])
        .await;
    let scope: Scope =
        serde_json::from_value(json!({ "scope_id": MIND })).expect("scope JSON matches the schema");
    assert_eq!(bound_caller(&scope), None);

    assert_fixed_refusal(
        &expect_refused(
            harness
                .port
                .put_scoped_knowledge_entry(
                    &scope,
                    entry_of("ke_unbound", vec![belief("ke_unbound", "cog_unbound")]),
                    None,
                )
                .await,
            "an unbound put",
        ),
        "an unbound put",
    );
    assert_fixed_refusal(
        &expect_refused(
            harness
                .port
                .get_scoped_knowledge_entry(&scope, "ke_shared")
                .await,
            "an unbound get",
        ),
        "an unbound get",
    );
    assert_fixed_refusal(
        &expect_refused(
            harness.port.list_scoped_knowledge_entries(&scope).await,
            "an unbound list",
        ),
        "an unbound list",
    );
    assert_eq!(
        harness.rows_of("ke_unbound").await,
        (0, 0, 0),
        "an unbound put wrote nothing"
    );
}

/// No refusal names the caller, the capability or the space: the whole answer serializes without
/// either the principal id or the caller namespace.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refusal_names_no_caller() {
    let harness = Harness::open().await;
    harness.seed_grant(READER, &["knowledge_read"]).await;
    let scope = bound_scope(READER);
    let refused = expect_refused(
        harness
            .port
            .put_scoped_knowledge_entry(
                &scope,
                entry_of("ke_reader", vec![belief("ke_reader", "cog_reader")]),
                None,
            )
            .await,
        "a put refused for want of a capability",
    );
    let rendered = format!("{refused:?}");
    for secret in [READER, CALLER_NAMESPACE, "knowledge_write", MIND, "p_wren"] {
        assert!(
            !rendered.contains(secret),
            "the refusal must not name {secret}: {rendered}"
        );
    }

    let unbound: Scope =
        serde_json::from_value(json!({ "scope_id": MIND })).expect("scope JSON matches the schema");
    let refused = expect_refused(
        harness.port.list_scoped_knowledge_entries(&unbound).await,
        "a list refused for want of a binding",
    );
    let rendered = format!("{refused:?}");
    for secret in [CALLER_NAMESPACE, MIND] {
        assert!(
            !rendered.contains(secret),
            "the refusal must not name {secret}: {rendered}"
        );
    }
}
