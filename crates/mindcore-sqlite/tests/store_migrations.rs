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
    // migrated by a set this release does not ship has.
    let pool = raw_pool(&path).await;
    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) \
         VALUES (2, 'absent from the embedded set', TRUE, X'00', -1)",
    )
    .execute(&pool)
    .await
    .expect("record the foreign migration");
    pool.close().await;

    let error = MindStore::open(&path)
        .await
        .err()
        .expect("an applied version outside the embedded set is refused");
    match migrate_error(error) {
        MigrateError::VersionMissing(version) => assert_eq!(version, 2),
        other => panic!("expected VersionMissing(2), got {other:?}"),
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
