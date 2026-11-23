//! The knowledge list hydrates a page in one statement: a collection whose entries carry
//! cognition records costs the same statement count at one entry as at four, instead of one
//! records statement per entry.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicUsize, Ordering};

use mindcore_base::cognition::CARRIER_NAMESPACE;
use mindcore_base::knowledge::KnowledgeScope;
use mindcore_sqlite::{MindStore, ScopedKnowledgeStore};
use serde_json::{json, Value};
use spoke_operations::{KnowledgeEntryPort, SpokeResult};
use spoke_schemas::knowledge_entry::{
    KnowledgeEntryBody, KnowledgeEntryCanonicalName, KnowledgeEntryModulesKey,
    KnowledgeEntryModulesValue,
};
use spoke_schemas::KnowledgeEntry;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{ConnectOptions, SqlitePool};
use tempfile::TempDir;

const COLLECTION: &str = "wren";

static STATEMENTS: AtomicUsize = AtomicUsize::new(0);

/// Counts the driver's per-statement `sqlx::query` events: sqlx gives no other
/// statement-level counter.
struct StatementCounter;

impl tracing::Subscriber for StatementCounter {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == "sqlx::query"
    }

    fn event(&self, _event: &tracing::Event<'_>) {
        STATEMENTS.fetch_add(1, Ordering::Relaxed);
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

/// One current-view belief record held by `holder`, the shape an append accepts.
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

/// An entry carrying exactly one stored cognition record.
fn carrier(entry_id: &str) -> KnowledgeEntry {
    let mut entry = KnowledgeEntry {
        adoption_anchor: None,
        body: KnowledgeEntryBody::default(),
        canonical_name: KnowledgeEntryCanonicalName::try_from(format!("{entry_id} name"))
            .expect("non-empty canonical name"),
        created_at: None,
        disclosure: None,
        entry_id: entry_id.to_string(),
        entry_type: "character".to_string(),
        extensions: HashMap::new(),
        modules: HashMap::new(),
        owner: None,
        revision: Some(1),
        schema_version: NonZeroU64::new(1).expect("non-zero wire baseline"),
        source_anchor: None,
        status: "confirmed".to_string(),
        updated_at: None,
    };
    entry.modules.insert(
        KnowledgeEntryModulesKey::try_from("cognition").expect("valid module key"),
        KnowledgeEntryModulesValue::from(
            json!({
                "records": [belief(entry_id, &format!("cog_{entry_id}"))],
                "world_facts": [],
                "extensions": {},
            })
            .as_object()
            .expect("module fixture is an object")
            .clone(),
        ),
    );
    entry
}

/// Store one entry through the port's write boundary.
async fn put(port: &ScopedKnowledgeStore, entry_id: &str) {
    match port.put_knowledge_entry(carrier(entry_id), None).await {
        SpokeResult::Ok(_) => {}
        SpokeResult::Reject(reject) => panic!("{entry_id} rejected: {reject:?}"),
    }
}

/// The statement count one `list_entries` costs, and the records it hydrated per entry.
async fn list_cost(port: &ScopedKnowledgeStore) -> (usize, Vec<usize>) {
    let before = STATEMENTS.load(Ordering::Relaxed);
    let listed = port.list_entries().await;
    let statements = STATEMENTS.load(Ordering::Relaxed) - before;
    match listed {
        SpokeResult::Ok(entries) => {
            let records = entries
                .iter()
                .map(|entry| {
                    entry
                        .modules
                        .get(&KnowledgeEntryModulesKey::try_from("cognition").expect("module key"))
                        .and_then(|module| match module {
                            KnowledgeEntryModulesValue::Object(module) => module
                                .get("records")
                                .and_then(Value::as_array)
                                .map(Vec::len),
                            KnowledgeEntryModulesValue::Array(_) => None,
                        })
                        .unwrap_or_default()
                })
                .collect();
            (statements, records)
        }
        SpokeResult::Reject(reject) => panic!("list rejected: {reject:?}"),
    }
}

#[tokio::test]
async fn the_knowledge_list_hydrates_a_page_in_one_statement() {
    tracing::subscriber::set_global_default(StatementCounter).expect("install statement counter");
    let dir = TempDir::new().expect("temporary directory");
    let path = dir.path().join("mindcore.db");

    let store = MindStore::open(&path).await.expect("open store");
    store.close().await;
    let pool: SqlitePool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&path)
                .foreign_keys(true)
                .log_statements(log::LevelFilter::Debug),
        )
        .await
        .expect("open logging pool");
    let port = ScopedKnowledgeStore::new(pool.clone(), KnowledgeScope::new(COLLECTION));

    put(&port, "ke_1").await;
    let (one, hydrated_one) = list_cost(&port).await;
    assert_eq!(hydrated_one, [1], "the listing hydrates the stored record");
    assert_eq!(
        one, 2,
        "one payload statement and one records statement list an entry"
    );

    for index in 2..=4 {
        put(&port, &format!("ke_{index}")).await;
    }
    let (four, hydrated_four) = list_cost(&port).await;
    assert_eq!(hydrated_four, [1, 1, 1, 1]);
    assert_eq!(
        one, four,
        "the page hydrates in one records statement at any size: {one} at one entry, {four} at four"
    );

    pool.close().await;
}
