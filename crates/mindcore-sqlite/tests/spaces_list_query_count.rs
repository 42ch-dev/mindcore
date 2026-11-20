//! I-000060: `spaces.list` costs the same statement count for one space and
//! for five — the principal's grants are read once, not once per space — and a
//! grant whose space row is gone cannot fail the listing.

use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{ConnectOptions, SqlitePool};
use tempfile::TempDir;

use mindcore_spoke::{MemoryAdapter, MemoryStage};
use spoke_operations::SpokeResult;
use spoke_schemas::Principal;

const DDL: &str = include_str!("fixtures/settlement-prepare.sql");
const NOW: &str = "2026-11-20T09:20:01.123Z";
const PRINCIPAL: &str = "ctr_list";

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

async fn seed_space(pool: &SqlitePool, index: usize) {
    let space = format!("spc_{index}");
    sqlx::query(
        "INSERT INTO memory_spaces \
         (space_id, display_name, authority_host_id, authority_generation, status, revision, \
          created_at) \
         VALUES (?, ?, 'host_1', 1, 'active', 1, ?)",
    )
    .bind(&space)
    .bind(&space)
    .bind(NOW)
    .execute(pool)
    .await
    .expect("seed space");
    sqlx::query(
        "INSERT INTO memory_grants \
         (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
          shared_access, expires_at, revoked_at, revision, created_at) \
         VALUES (?, ?, NULL, ?, '[\"admin\"]', '[]', 1, NULL, NULL, 1, ?)",
    )
    .bind(format!("grt_{index}"))
    .bind(&space)
    .bind(PRINCIPAL)
    .bind(NOW)
    .execute(pool)
    .await
    .expect("seed grant");
}

/// The statement count one `spaces.list` page costs, and the page's space count.
async fn list_cost(pool: &SqlitePool) -> (usize, usize) {
    let adapter = MemoryAdapter::new(true);
    let principal: Principal = serde_json::from_value(json!({
        "principal_id": PRINCIPAL,
        "extensions": {},
    }))
    .expect("principal");
    let mut tx = pool.begin().await.expect("begin");
    let before = STATEMENTS.load(Ordering::Relaxed);
    let stage = adapter.list_spaces(&mut tx, &principal, json!({})).await;
    let statements = STATEMENTS.load(Ordering::Relaxed) - before;
    tx.rollback().await.expect("rollback");
    let body = match stage {
        SpokeResult::Ok(MemoryStage::Ready(body)) => body,
        other => panic!("spaces.list did not return a ready stage: {other:?}"),
    };
    let listed = body["spaces"].as_array().expect("spaces array").len();
    (statements, listed)
}

/// One batch read whatever the space count; an orphan grant (no space row,
/// unconstrained payload) is skipped instead of decoded and fails nothing.
#[tokio::test]
async fn list_spaces_reads_grants_once_and_skips_orphan_grants() {
    tracing::subscriber::set_global_default(StatementCounter).expect("install counter");
    let dir = TempDir::new().expect("temporary directory");
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(dir.path().join("spaces.db"))
                .create_if_missing(true)
                .foreign_keys(false)
                .log_statements(log::LevelFilter::Debug),
        )
        .await
        .expect("open pool");
    sqlx::raw_sql(DDL).execute(&pool).await.expect("apply DDL");

    seed_space(&pool, 1).await;
    let (one_space, listed_one) = list_cost(&pool).await;
    assert_eq!(listed_one, 1, "one seeded space is listed");

    for index in 2..=5 {
        seed_space(&pool, index).await;
    }
    let (five_spaces, listed_five) = list_cost(&pool).await;
    assert_eq!(listed_five, 5, "every seeded space is listed");

    // The counter has to see the read pair itself, or equality proves nothing.
    assert!(
        one_space > 1,
        "spaces.list statement count not observed: {one_space}",
    );
    assert_eq!(
        one_space, five_spaces,
        "spaces.list issued {one_space} statements for one space and {five_spaces} for five",
    );

    // A grant whose space row is gone still carries an unconstrained payload;
    // decoding it would fail the whole page.
    sqlx::query(
        "INSERT INTO memory_grants \
         (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
          shared_access, expires_at, revoked_at, revision, created_at) \
         VALUES ('grt_orphan', 'spc_missing', NULL, ?, '{not json', '[]', 0, NULL, NULL, 1, ?)",
    )
    .bind(PRINCIPAL)
    .bind(NOW)
    .execute(&pool)
    .await
    .expect("seed orphan grant");
    let (with_orphan, listed_with_orphan) = list_cost(&pool).await;
    assert_eq!(
        listed_with_orphan, 5,
        "the valid spaces still list beside an orphan grant",
    );
    assert_eq!(
        with_orphan, five_spaces,
        "the orphan grant added a statement: {with_orphan} against {five_spaces}",
    );
}
