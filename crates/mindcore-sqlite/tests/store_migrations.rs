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

    // A database that applied a version the embedded set does not contain — the shape a database
    // migrated by a set this release does not ship has. The version must stay far above any real
    // migration prefix, or the row stops being foreign as soon as that migration is appended.
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

    // The shape a database has after the embedded 0001 was edited in place: its row still records
    // version 1, but not with the bytes the embedded file now hashes to.
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

    // The schema exists but the row that records it does not: the migrator retries the pending
    // migration over the existing tables instead of treating the database as current.
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
    // The set 0.1.0 shipped: the first file alone, with the checksum sqlx records for it.
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
    pool.close().await;

    let store = MindStore::open(&path)
        .await
        .expect("a 0.1.0 database reopens");
    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&store.pool())
            .await
            .expect("read the applied versions");
    assert_eq!(versions, vec![1, 2], "the appended file applied on open");
    let registry = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM minds")
        .fetch_one(&store.pool())
        .await
        .expect("count the registry");
    assert_eq!(registry, 0);

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
