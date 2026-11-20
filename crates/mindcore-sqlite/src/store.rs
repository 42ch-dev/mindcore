//! One local database file, one pool, one embedded migrator.

use std::path::Path;
use std::time::Duration;

use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub struct MindStore {
    pub(crate) pool: SqlitePool,
}

impl MindStore {
    /// Creates the database file when absent and applies any pending embedded migration.
    ///
    /// Lock contention may enter SQLite's busy handler for up to five seconds, but SQLite can
    /// still return `SQLITE_BUSY` sooner — the handler is not consulted for every case, such as a
    /// write whose WAL read snapshot another connection's commit already invalidated. The
    /// timeout is a cap, not a wait guarantee.
    ///
    /// Pre-release migrations are mutable: the schema files change in place between commits, so
    /// there is no upgrade path until the first release. An applied migration whose file has since
    /// changed is refused here with a checksum mismatch; a database whose applied migrations still
    /// match the embedded files opens normally, whatever commit created it — cross-commit
    /// compatibility is unsupported and untested.
    pub async fn open(path: &Path) -> Result<Self, sqlx::Error> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true)
            // Cap the busy-handler wait; paths that bypass it can return SQLITE_BUSY sooner.
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn close(self) {
        self.pool.close().await;
    }

    /// A handle to this store's pool, for adapters that bind over the same connection.
    pub fn pool(&self) -> SqlitePool {
        self.pool.clone()
    }
}
