//! The storage contract `docs/storage-compatibility.md` states, proven against `MindStore::open`:
//! a database the embedded set produced reopens, and a database whose `_sqlx_migrations` row set
//! does not match the embedded set is refused by the migrator rather than served.

use std::path::Path;

use mindcore_sqlite::MindStore;
use sqlx::migrate::MigrateError;
use sqlx::{Error, SqlitePool};

/// A pool on the same file without the migrator, so a test can inspect or doctor the applied rows.
async fn raw_pool(path: &Path) -> SqlitePool {
    sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true),
        )
        .await
        .expect("open a raw pool on the store file")
}

/// The refusal path is SQLx's: `open` returns `Error::Migrate` carrying the `MigrateError`.
fn migrate_error(error: Error) -> MigrateError {
    match error {
        Error::Migrate(error) => *error,
        other => panic!("expected a migration refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn embedded_set_reopens() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");

    MindStore::open(&path)
        .await
        .expect("first open")
        .close()
        .await;
    MindStore::open(&path)
        .await
        .expect("a database the embedded set produced reopens")
        .close()
        .await;
}

#[tokio::test]
async fn applied_version_outside_the_embedded_set_is_refused() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    MindStore::open(&path)
        .await
        .expect("open store")
        .close()
        .await;

    const ABSENT: i64 = 9999;
    let pool = raw_pool(&path).await;
    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) \
         VALUES (?1, 'absent from the embedded set', TRUE, X'00', -1)",
    )
    .bind(ABSENT)
    .execute(&pool)
    .await
    .expect("record the foreign migration");
    pool.close().await;

    let error = MindStore::open(&path)
        .await
        .err()
        .expect("an applied version outside the embedded set is refused");
    match migrate_error(error) {
        MigrateError::VersionMissing(version) => assert_eq!(version, ABSENT),
        other => panic!("expected VersionMissing({ABSENT}), got {other:?}"),
    }
}

#[tokio::test]
async fn applied_migration_with_a_changed_checksum_is_refused() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    MindStore::open(&path)
        .await
        .expect("open store")
        .close()
        .await;

    let pool = raw_pool(&path).await;
    sqlx::query("UPDATE _sqlx_migrations SET checksum = X'00' WHERE version = 1")
        .execute(&pool)
        .await
        .expect("diverge the recorded checksum");
    pool.close().await;

    let error = MindStore::open(&path)
        .await
        .err()
        .expect("an edited migration is refused");
    match migrate_error(error) {
        MigrateError::VersionMismatch(version) => assert_eq!(version, 1),
        other => panic!("expected VersionMismatch(1), got {other:?}"),
    }
}

#[tokio::test]
async fn applied_set_missing_a_recorded_version_is_refused() {
    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");
    MindStore::open(&path)
        .await
        .expect("open store")
        .close()
        .await;

    let pool = raw_pool(&path).await;
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 1")
        .execute(&pool)
        .await
        .expect("drop the applied record");
    pool.close().await;

    let error = MindStore::open(&path)
        .await
        .err()
        .expect("an applied set that lost a version is refused");
    match migrate_error(error) {
        MigrateError::ExecuteMigration(_, version) => assert_eq!(version, 1),
        other => panic!("expected ExecuteMigration for version 1, got {other:?}"),
    }
}

#[tokio::test]
async fn a_database_written_by_the_published_set_reopens_through_the_appended_file() {
    use mindcore_base::boundary::CallerId;
    use mindcore_base::ids::MindId;
    use mindcore_sqlite::minds::CreateMind;

    let temp = tempfile::tempdir().expect("private temporary root");
    let path = temp.path().join("mindcore.db");

    let embedded = sqlx::migrate!("./migrations");
    let published = sqlx::migrate::Migrator {
        migrations: std::borrow::Cow::Owned(
            embedded
                .migrations
                .iter()
                .filter(|m| m.version == 1)
                .cloned()
                .collect(),
        ),
        ..embedded
    };
    let pool = raw_pool(&path).await;
    published.run(&pool).await.expect("write a 0.1.0 database");
    sqlx::query(
        "INSERT INTO memory_spaces \
         (space_id, display_name, authority_host_id, authority_generation, status, revision, \
          created_at) \
         VALUES ('spc_legacy', 'Legacy', 'host-1', 1, 'active', 1, '2026-11-20T09:20:01.123Z')",
    )
    .execute(&pool)
    .await
    .expect("write the legacy space");
    pool.close().await;

    let store = MindStore::open(&path)
        .await
        .expect("a 0.1.0 database reopens");
    let legacy: (Option<String>, String, i64, String, i64, String) = sqlx::query_as(
        "SELECT display_name, authority_host_id, authority_generation, status, revision, \
         created_at FROM memory_spaces WHERE space_id = 'spc_legacy'",
    )
    .fetch_one(&store.pool())
    .await
    .expect("read the legacy space");
    assert_eq!(
        legacy,
        (
            Some("Legacy".to_string()),
            "host-1".to_string(),
            1,
            "active".to_string(),
            1,
            "2026-11-20T09:20:01.123Z".to_string()
        ),
        "the legacy row is unchanged"
    );
    let registry = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM minds")
        .fetch_one(&store.pool())
        .await
        .expect("count the registry");
    assert_eq!(registry, 0, "a pre-registry space is not registered");
    let listed = store.list_minds(&CallerId::new("caller_wren")).await;
    match listed {
        spoke_operations::SpokeResult::Ok(listed) => assert!(
            listed.is_empty(),
            "the legacy space is not listed as a Mind"
        ),
        spoke_operations::SpokeResult::Reject(reject) => {
            panic!("list on a reopened database: {reject:?}")
        }
    }

    let created = store
        .create_mind(&CreateMind {
            mind_id: Some(MindId::new("wren")),
            display_name: None,
            owner: CallerId::new("caller_wren"),
        })
        .await;
    let mind = match created {
        spoke_operations::SpokeResult::Ok(mind) => mind,
        spoke_operations::SpokeResult::Reject(reject) => {
            panic!("create on a reopened database: {reject:?}")
        }
    };
    let seen = store
        .get_mind(&CallerId::new("caller_wren"), &MindId::new("wren"))
        .await;
    match seen {
        spoke_operations::SpokeResult::Ok(seen) => assert_eq!(mind, seen),
        spoke_operations::SpokeResult::Reject(reject) => {
            panic!("get on a reopened database: {reject:?}")
        }
    }
    store.close().await;
    MindStore::open(&path)
        .await
        .expect("the reopened database reopens again")
        .close()
        .await;
}
