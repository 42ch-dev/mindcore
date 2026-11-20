//! I-000011 cost evidence: appending one cognition record and reading the current
//! judgment cost the same statements and rows — and the same number of SQLite
//! virtual-machine instructions — with 1 record of history as with 1000. The append
//! never touches earlier rows and the current lookup never scans them. Counted via
//! the driver's per-statement tracing events, SQLite's `total_changes()`, and a
//! counting VDBE progress handler (`sqlite3_progress_handler`), not wall-clock.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use mindcore_base::cognition::CARRIER_NAMESPACE;
use mindcore_sqlite::cognition::read_current_record;
use mindcore_sqlite::{append_cognition, read_carrier_header, write_entry, MindStore};
use serde_json::{json, Value};
use spoke_operations::SpokeResult;
use spoke_schemas::knowledge_entry::{KnowledgeEntryBody, KnowledgeEntryCanonicalName};
use spoke_schemas::KnowledgeEntry;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqlitePoolOptions};
use sqlx::{ConnectOptions, Connection, SqlitePool};
use tempfile::TempDir;

const COLLECTION: &str = "wren";
const CARRIER: &str = "ke_wren";

static STATEMENTS: AtomicUsize = AtomicUsize::new(0);
static VDBE_STEPS: AtomicU64 = AtomicU64::new(0);

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

/// Counts every SQLite virtual-machine instruction on this connection via the
/// progress handler: one callback per VDBE opcode, never interrupting. A scan over
/// N history rows adds ~N steps; an indexed lookup adds none of them. The handler
/// lives on the connection (locked out of the worker thread only for the call).
async fn install_step_counter(connection: &mut SqliteConnection) {
    let mut handle = connection.lock_handle().await.expect("lock the sqlite handle");
    // sqlx's callback contract is inverted from SQLite's: returning `true` continues.
    handle.set_progress_handler(1, || {
        VDBE_STEPS.fetch_add(1, Ordering::Relaxed);
        true
    });
}

/// Detaches the counter so unmeasured statements on the same pooled connection
/// (filler appends, header reads) are never counted.
async fn remove_step_counter(connection: &mut SqliteConnection) {
    let mut handle = connection.lock_handle().await.expect("lock the sqlite handle");
    handle.remove_progress_handler();
}

/// The pooled connection holds at most one connection, so acquiring it pins the one
/// the pool will reuse; the measurement brackets a statement-bounded operation on it.
async fn measured_connection(pool: &SqlitePool) -> sqlx::pool::PoolConnection<sqlx::Sqlite> {
    pool.acquire().await.expect("acquire the pooled connection")
}

/// The pinned belief record shape (the CAS suite's fixture), held by `ke_wren`.
fn belief(record_id: &str) -> Value {
    json!({
        "kind": "belief",
        "order": 1,
        "record_id": record_id,
        "holder": {
            "kind": "knowledge_entry",
            "id": CARRIER,
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
            "id": CARRIER,
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

/// One measured storage append: statements issued, rows written and VDBE steps
/// executed, counted on the single pooled connection, plus the revision the append
/// assigned.
async fn measured_append(
    pool: &SqlitePool,
    expected: u64,
    record_id: &str,
) -> (usize, i64, u64, u64) {
    let mut connection = measured_connection(pool).await;
    install_step_counter(&mut *connection).await;
    let before_steps = VDBE_STEPS.load(Ordering::Relaxed);
    let mut tx = (&mut *connection)
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("begin append transaction");
    let before_rows: i64 = sqlx::query_scalar::<_, i64>("SELECT total_changes()")
        .fetch_one(&mut *tx)
        .await
        .expect("total changes before the append");
    let before_statements = STATEMENTS.load(Ordering::Relaxed);
    let assigned =
        match append_cognition(&mut tx, COLLECTION, CARRIER, expected, &belief(record_id)).await {
            SpokeResult::Ok(assigned) => assigned,
            SpokeResult::Reject(reject) => panic!("append {record_id} rejected: {reject:?}"),
        };
    let statements = STATEMENTS.load(Ordering::Relaxed) - before_statements;
    let after_rows: i64 = sqlx::query_scalar::<_, i64>("SELECT total_changes()")
        .fetch_one(&mut *tx)
        .await
        .expect("total changes after the append");
    tx.commit().await.expect("commit the measured append");
    let steps = VDBE_STEPS.load(Ordering::Relaxed) - before_steps;
    remove_step_counter(&mut *connection).await;
    (statements, after_rows - before_rows, assigned, steps)
}

/// One measured current-judgment lookup: statements issued, VDBE steps executed, and
/// the record id the holder's head points at.
async fn measured_lookup(pool: &SqlitePool) -> (usize, u64, Option<String>) {
    let mut connection = measured_connection(pool).await;
    install_step_counter(&mut *connection).await;
    let before_steps = VDBE_STEPS.load(Ordering::Relaxed);
    let before_statements = STATEMENTS.load(Ordering::Relaxed);
    let record = match read_current_record(&mut *connection, COLLECTION, CARRIER).await {
        SpokeResult::Ok(record) => record,
        SpokeResult::Reject(reject) => panic!("current lookup rejected: {reject:?}"),
    };
    let statements = STATEMENTS.load(Ordering::Relaxed) - before_statements;
    let steps = VDBE_STEPS.load(Ordering::Relaxed) - before_steps;
    remove_step_counter(&mut *connection).await;
    let head = record.and_then(|record| {
        record
            .get("record_id")
            .and_then(Value::as_str)
            .map(str::to_string)
    });
    (statements, steps, head)
}

/// The append's and the current lookup's statement, row and VDBE-step work is the
/// same with one record of history as with one thousand, and every append lands: one
/// row inserted, one carrier row advanced, revision and head moving to the newest
/// record. The step budget keeps both paths out of history-scan territory even if a
/// regression scanned a constant (size-independent) number of rows at every scale.
const LOOKUP_STEP_BUDGET: u64 = 200;

#[tokio::test]
async fn append_and_current_lookup_cost_the_same_at_one_and_at_one_thousand_records() {
    tracing::subscriber::set_global_default(StatementCounter).expect("install statement counter");
    let dir = TempDir::new().expect("temporary directory");
    let path = dir.path().join("mindcore.db");

    // The migrator owns the schema; the logging pool below owns the measurement.
    let store = MindStore::open(&path).await.expect("open store");
    store.close().await;
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&path)
                .foreign_keys(true)
                .log_statements(log::LevelFilter::Debug),
        )
        .await
        .expect("open logging pool");

    // The empty carrier row: revision 1, no records, no head.
    let carrier = KnowledgeEntry {
        body: KnowledgeEntryBody::default(),
        canonical_name: KnowledgeEntryCanonicalName::try_from("ke_wren name")
            .expect("non-empty canonical name"),
        created_at: None,
        disclosure: None,
        entry_id: CARRIER.to_string(),
        entry_type: "character".to_string(),
        extensions: HashMap::new(),
        modules: HashMap::new(),
        owner: None,
        revision: None,
        schema_version: NonZeroU64::new(1).expect("non-zero wire baseline"),
        source_anchor: None,
        status: "confirmed".to_string(),
        updated_at: None,
    };
    let mut tx = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("begin carrier write");
    match write_entry(&mut tx, COLLECTION, carrier, None).await {
        SpokeResult::Ok(entry) => assert_eq!(entry.revision, Some(1)),
        SpokeResult::Reject(reject) => panic!("carrier creation rejected: {reject:?}"),
    }
    tx.commit().await.expect("commit the carrier row");

    // Nothing to point at yet: the lookup is still one statement over no record.
    let (headless_lookup_statements, _, headless) = measured_lookup(&pool).await;
    assert_eq!(
        headless, None,
        "a carrier without records has no current judgment"
    );
    assert_eq!(
        headless_lookup_statements, 1,
        "the headless lookup is one statement"
    );

    // History of one: the first append is unmeasured setup, the second is measured
    // while the scope holds one record.
    let mut tx = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("begin first append");
    match append_cognition(&mut tx, COLLECTION, CARRIER, 1, &belief("cog_first")).await {
        SpokeResult::Ok(assigned) => assert_eq!(assigned, 2),
        SpokeResult::Reject(reject) => panic!("first append rejected: {reject:?}"),
    }
    tx.commit().await.expect("commit the first append");
    let (lookup_at_1, steps_at_1, current_at_1) = measured_lookup(&pool).await;
    assert_eq!(current_at_1.as_deref(), Some("cog_first"));
    let (append_at_1, rows_at_1, assigned_at_1, append_steps_at_1) =
        measured_append(&pool, 2, "cog_second").await;
    assert_eq!(assigned_at_1, 3);

    // History of one thousand: filler appends 3..=1000 in one transaction.
    let mut tx = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("begin filler appends");
    for seq in 3..=1000u64 {
        let assigned = match append_cognition(
            &mut tx,
            COLLECTION,
            CARRIER,
            seq,
            &belief(&format!("cog_fill_{seq}")),
        )
        .await
        {
            SpokeResult::Ok(assigned) => assigned,
            SpokeResult::Reject(reject) => panic!("filler append {seq} rejected: {reject:?}"),
        };
        assert_eq!(
            assigned,
            seq + 1,
            "append {seq} must assign the next revision"
        );
    }
    tx.commit().await.expect("commit the filler appends");

    // The measured operations now run while the scope holds one thousand records.
    let (lookup_at_1000, steps_at_1000, current_at_1000) = measured_lookup(&pool).await;
    assert_eq!(
        current_at_1000.as_deref(),
        Some("cog_fill_1000"),
        "the head is the newest row whatever the history"
    );
    let (append_at_1000, rows_at_1000, assigned_at_1000, append_steps_at_1000) =
        measured_append(&pool, 1001, "cog_last").await;
    assert_eq!(assigned_at_1000, 1002);

    println!("append at 1 record: {append_at_1} statements, {rows_at_1} rows, {append_steps_at_1} VDBE steps");
    println!(
        "append at 1000 records: {append_at_1000} statements, {rows_at_1000} rows, {append_steps_at_1000} VDBE steps"
    );
    println!("current lookup at 1 record: {lookup_at_1} statements, {steps_at_1} VDBE steps");
    println!(
        "current lookup at 1000 records: {lookup_at_1000} statements, {steps_at_1000} VDBE steps"
    );

    assert_eq!(
        rows_at_1, 2,
        "an append writes one record row and advances one carrier row (at 1 record)"
    );
    assert_eq!(
        rows_at_1000, 2,
        "an append writes one record row and advances one carrier row (at 1000 records)"
    );
    assert_eq!(
        append_at_1, append_at_1000,
        "the append issued {append_at_1} statements with 1 record of history and \
         {append_at_1000} with 1000"
    );
    assert_eq!(
        lookup_at_1, lookup_at_1000,
        "the current lookup issued {lookup_at_1} statements with 1 record of history and \
         {lookup_at_1000} with 1000"
    );
    assert_eq!(
        append_steps_at_1, append_steps_at_1000,
        "the append executed {append_steps_at_1} VDBE steps with 1 record of history and \
         {append_steps_at_1000} with 1000"
    );
    assert_eq!(
        steps_at_1, steps_at_1000,
        "the current lookup executed {steps_at_1} VDBE steps with 1 record of history and \
         {steps_at_1000} with 1000"
    );
    assert!(
        steps_at_1000 <= LOOKUP_STEP_BUDGET,
        "the current lookup must stay in indexed-lookup territory: {steps_at_1000} VDBE steps"
    );
    assert!(
        append_steps_at_1000 > 0,
        "the counter must see the append itself: {append_steps_at_1000}"
    );
    assert!(
        append_at_1 > 0,
        "the counter must see the append itself: {append_at_1}"
    );
    assert!(
        lookup_at_1 > 0,
        "the counter must see the lookup itself: {lookup_at_1}"
    );

    // Every append really landed and the head points at the newest row.
    let header = match read_carrier_header(&pool, COLLECTION, CARRIER).await {
        SpokeResult::Ok(header) => header,
        SpokeResult::Reject(reject) => panic!("header read rejected: {reject:?}"),
    };
    assert_eq!((header.revision, header.head_seq), (1002, Some(1001)));
}
