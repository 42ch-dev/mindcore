//! I-000060 / I-000066: `spaces.list` streams the principal's grants and
//! projects the visible revisions, so it costs exactly three SELECTs — the
//! scan, the revision projection and the spaces — whatever the space count,
//! and it never fails on a grant whose space row is gone.

use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{json, Number, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{ConnectOptions, SqlitePool};
use tempfile::TempDir;

use mindcore_base::ids::SpaceId;
use mindcore_base::ir::MemorySpaceRow;
use mindcore_spoke::{MemoryAdapter, MemoryStage};
use mindcore_sqlite::{MindStore, SqliteMemoryRepository};
use spoke_operations::{SpokeRejectCode, SpokeResult};
use spoke_schemas::Principal;

const NOW: &str = "2026-11-20T09:20:01.123Z";
const PRINCIPAL: &str = "ctr_list";
const EXPIRED_AT: &str = "2020-01-01T00:00:00.000Z";

static SELECTS: AtomicUsize = AtomicUsize::new(0);

/// Counts the driver's per-statement `sqlx::query` events for SELECTs only:
/// sqlx gives no other statement-level counter, and transaction statements
/// share the target.
struct StatementCounter;

struct StatementText(Option<String>);

impl tracing::field::Visit for StatementText {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "db.statement" {
            self.0 = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}

impl tracing::Subscriber for StatementCounter {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == "sqlx::query"
    }

    fn event(&self, event: &tracing::Event<'_>) {
        let mut text = StatementText(None);
        event.record(&mut text);
        if text
            .0
            .as_deref()
            .is_some_and(|sql| sql.trim_start().starts_with("SELECT"))
        {
            SELECTS.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

/// One `memory_grants` row; `capabilities_json` stays raw so a test can seed a
/// corrupt payload, and both liveness stamps stay optional so it can seed a
/// grant that grants nothing.
struct Grant<'a> {
    grant_id: &'a str,
    space_id: &'a str,
    principal_id: &'a str,
    capabilities_json: &'a str,
    expires_at: Option<&'a str>,
    revoked_at: Option<&'a str>,
    revision: i64,
}

async fn seed_space_row(pool: &SqlitePool, space_id: &str) {
    let mut tx = pool.begin().await.expect("begin");
    let space = MemorySpaceRow {
        space_id: SpaceId::new(space_id),
        display_name: Some(space_id.to_string()),
        authority_host_id: "host_1".to_string(),
        authority_generation: 1,
        status: "active".to_string(),
        revision: Number::from(1),
        created_at: NOW.to_string(),
        pending_receipts: 0,
    };
    SqliteMemoryRepository
        .insert_space(&mut tx, &space)
        .await
        .expect("seed space");
    tx.commit().await.expect("commit space");
}

async fn seed_grant(pool: &SqlitePool, grant: Grant<'_>) {
    sqlx::query(
        "INSERT INTO memory_grants \
         (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
          shared_access, expires_at, revoked_at, revision, created_at) \
         VALUES (?, ?, NULL, ?, ?, '[]', 1, ?, ?, ?, ?)",
    )
    .bind(grant.grant_id)
    .bind(grant.space_id)
    .bind(grant.principal_id)
    .bind(grant.capabilities_json)
    .bind(grant.expires_at)
    .bind(grant.revoked_at)
    .bind(grant.revision)
    .bind(NOW)
    .execute(pool)
    .await
    .expect("seed grant");
}

/// A space the principal can see through one live grant.
async fn seed_space(pool: &SqlitePool, index: usize) {
    let space = format!("spc_{index}");
    let grant = format!("grt_{index}");
    seed_space_row(pool, &space).await;
    seed_grant(
        pool,
        Grant {
            grant_id: &grant,
            space_id: &space,
            principal_id: PRINCIPAL,
            capabilities_json: "[\"admin\"]",
            expires_at: None,
            revoked_at: None,
            revision: 1,
        },
    )
    .await;
}

/// One `spaces.list` call: the SELECT count it cost and its outcome.
async fn list_call(pool: &SqlitePool, request: Value) -> (usize, SpokeResult<MemoryStage>) {
    let adapter = MemoryAdapter::new(true);
    let principal: Principal = serde_json::from_value(json!({
        "principal_id": PRINCIPAL,
        "extensions": {},
    }))
    .expect("principal");
    let mut tx = pool.begin().await.expect("begin");
    let before = SELECTS.load(Ordering::Relaxed);
    let stage = adapter.list_spaces(&mut tx, &principal, request).await;
    let selects = SELECTS.load(Ordering::Relaxed) - before;
    tx.rollback().await.expect("rollback");
    (selects, stage)
}

/// A ready page's space ids in served order and its next cursor.
fn page_of(stage: &SpokeResult<MemoryStage>) -> (Vec<String>, Option<String>) {
    let SpokeResult::Ok(MemoryStage::Ready(body)) = stage else {
        panic!("spaces.list did not return a ready stage: {stage:?}");
    };
    let ids = body["spaces"]
        .as_array()
        .expect("spaces array")
        .iter()
        .map(|space| space["space_id"].as_str().expect("space_id").to_string())
        .collect();
    let cursor = body["next_cursor"].as_str().map(str::to_string);
    (ids, cursor)
}

/// One full listing's SELECT count and served ids.
async fn listing(pool: &SqlitePool) -> (usize, Vec<String>) {
    let (selects, stage) = list_call(pool, json!({})).await;
    let (ids, _) = page_of(&stage);
    (selects, ids)
}

fn cursor_invalid(stage: &SpokeResult<MemoryStage>) -> bool {
    matches!(stage, SpokeResult::Reject(reject) if reject.code == SpokeRejectCode::CursorInvalid)
}

fn five_spaces() -> Vec<String> {
    (1..=5).map(|index| format!("spc_{index}")).collect()
}

/// Three SELECTs whatever the space count, no retained invisible history, and
/// the same order, visibility and cursor contract as the batched read.
#[tokio::test]
async fn list_spaces_streams_grants_and_costs_three_selects() {
    tracing::subscriber::set_global_default(StatementCounter).expect("install counter");
    let dir = TempDir::new().expect("temporary directory");
    let path = dir.path().join("spaces.db");
    MindStore::open(&path).await.expect("open store");
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true)
                .foreign_keys(false)
                .log_statements(log::LevelFilter::Debug),
        )
        .await
        .expect("open pool");

    assert_eq!(
        listing(&pool).await,
        (3, vec![]),
        "zero spaces still cost three SELECTs",
    );

    seed_space(&pool, 1).await;
    assert_eq!(
        listing(&pool).await,
        (3, vec!["spc_1".to_string()]),
        "one space costs the same three SELECTs",
    );

    for index in 2..=5 {
        seed_space(&pool, index).await;
    }
    let five = five_spaces();
    assert_eq!(
        listing(&pool).await,
        (3, five.clone()),
        "five spaces cost the same three SELECTs",
    );

    seed_grant(
        &pool,
        Grant {
            grant_id: "grt_orphan",
            space_id: "spc_missing",
            principal_id: PRINCIPAL,
            capabilities_json: "{not json",
            expires_at: None,
            revoked_at: None,
            revision: 1,
        },
    )
    .await;
    assert_eq!(
        listing(&pool).await,
        (3, five.clone()),
        "an orphan grant is skipped instead of decoded",
    );

    seed_space_row(&pool, "spc_hidden").await;
    seed_grant(
        &pool,
        Grant {
            grant_id: "grt_hidden_other",
            space_id: "spc_hidden",
            principal_id: "ctr_other",
            capabilities_json: "[\"admin\"]",
            expires_at: None,
            revoked_at: None,
            revision: 1,
        },
    )
    .await;
    seed_grant(
        &pool,
        Grant {
            grant_id: "grt_hidden_expired",
            space_id: "spc_hidden",
            principal_id: PRINCIPAL,
            capabilities_json: "[\"admin\"]",
            expires_at: Some(EXPIRED_AT),
            revoked_at: None,
            revision: 1,
        },
    )
    .await;
    assert_eq!(
        listing(&pool).await,
        (3, five.clone()),
        "invisible grant history neither lists a space nor costs a statement",
    );

    let (selects, stage) = list_call(&pool, json!({ "limit": 2 })).await;
    assert_eq!(selects, 3, "a partial page costs the same three SELECTs");
    let (first, cursor) = page_of(&stage);
    assert_eq!(first, ["spc_1", "spc_2"], "visible spaces keep their order");
    let cursor = cursor.expect("the first page advances");
    let (_, stage) = list_call(&pool, json!({ "limit": 2, "cursor": cursor })).await;
    let (second, _) = page_of(&stage);
    assert_eq!(
        second,
        ["spc_3", "spc_4"],
        "the same snapshot serves the second page",
    );

    sqlx::query("UPDATE memory_grants SET revision = 2 WHERE grant_id = 'grt_3'")
        .execute(&pool)
        .await
        .expect("bump a visible grant");
    let (_, stage) = list_call(&pool, json!({ "limit": 2, "cursor": cursor })).await;
    assert!(
        cursor_invalid(&stage),
        "a visible-space grant revision must invalidate the cursor: {stage:?}",
    );

    seed_grant(
        &pool,
        Grant {
            grant_id: "grt_5_expired",
            space_id: "spc_5",
            principal_id: PRINCIPAL,
            capabilities_json: "[\"admin\"]",
            expires_at: Some(EXPIRED_AT),
            revoked_at: None,
            revision: 1,
        },
    )
    .await;
    seed_grant(
        &pool,
        Grant {
            grant_id: "grt_5_revoked",
            space_id: "spc_5",
            principal_id: PRINCIPAL,
            capabilities_json: "[\"admin\"]",
            expires_at: None,
            revoked_at: Some(NOW),
            revision: 1,
        },
    )
    .await;
    assert_eq!(
        listing(&pool).await,
        (3, five.clone()),
        "expired and revoked grants list no extra space",
    );
    let (_, stage) = list_call(&pool, json!({ "limit": 2 })).await;
    let cursor = page_of(&stage).1.expect("the first page advances");
    sqlx::query("UPDATE memory_grants SET revision = 2 WHERE grant_id = 'grt_5_revoked'")
        .execute(&pool)
        .await
        .expect("bump a revoked grant");
    let (_, stage) = list_call(&pool, json!({ "limit": 2, "cursor": cursor })).await;
    assert!(
        cursor_invalid(&stage),
        "a revoked grant in a visible space must invalidate the cursor: {stage:?}",
    );

    seed_grant(
        &pool,
        Grant {
            grant_id: "grt_1_bad",
            space_id: "spc_1",
            principal_id: PRINCIPAL,
            capabilities_json: "{not json",
            expires_at: None,
            revoked_at: None,
            revision: 1,
        },
    )
    .await;
    let (_, stage) = list_call(&pool, json!({})).await;
    assert!(
        matches!(&stage, SpokeResult::Reject(reject) if reject.code == SpokeRejectCode::InternalError),
        "a malformed grant payload in a visible space must fail the listing: {stage:?}",
    );
}
