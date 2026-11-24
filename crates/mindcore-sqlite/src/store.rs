//! One local database file, one pool, one embedded migrator.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub struct MindStore {
    pub(crate) pool: SqlitePool,
    /// The read-projection seam every port of this store shares.
    pub(crate) seam: Arc<SnapshotSeam>,
}

/// The seam a unit test uses to hold one snapshot read between its two statements and commit a
/// write in that window, mirroring the append that must not be paired with an older payload.
///
/// One seam belongs to one store and travels with its ports, so a hydration running on another
/// store in the same test binary can never consume it. Production builds carry the empty type:
/// nothing can be installed and `interleave` returns at once.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct SnapshotSeam {
    slot: std::sync::Mutex<
        Option<(
            tokio::sync::mpsc::UnboundedSender<&'static str>,
            tokio::sync::mpsc::UnboundedReceiver<&'static str>,
        )>,
    >,
}

#[cfg(test)]
impl SnapshotSeam {
    /// Install the test's channels: the next read on a port of this store suspends at the seam.
    pub(crate) fn install(
        &self,
        to_test: tokio::sync::mpsc::UnboundedSender<&'static str>,
        from_test: tokio::sync::mpsc::UnboundedReceiver<&'static str>,
    ) {
        *self.slot.lock().expect("snapshot seam lock") = Some((to_test, from_test));
    }

    pub(crate) async fn interleave(&self) {
        let installed = self.slot.lock().expect("snapshot seam lock").take();
        let (to_test, mut from_test) = match installed {
            Some(pair) => pair,
            None => return,
        };
        if to_test.send("at_records_seam").is_err() {
            return;
        }
        let _ = from_test.recv().await;
    }
}

#[cfg(not(test))]
#[derive(Default)]
pub(crate) struct SnapshotSeam {}

#[cfg(not(test))]
impl SnapshotSeam {
    pub(crate) async fn interleave(&self) {}
}

impl MindStore {
    /// Creates the database file when absent and applies any pending embedded migration.
    ///
    /// Lock contention may enter SQLite's busy handler for up to five seconds, but SQLite can
    /// still return `SQLITE_BUSY` sooner — the handler is not consulted for every case, such as a
    /// write whose WAL read snapshot another connection's commit already invalidated. The
    /// timeout is a cap, not a wait guarantee.
    ///
    /// The embedded migration set is append-only from the first published release: a published
    /// file is immutable and a schema change appends a new one, so a database recorded at any
    /// published set reopens here and the appended files are the only pending work. The contract is
    /// `docs/storage-compatibility.md`, and `tooling/migrations/check.mjs` enforces the lock it
    /// reads.
    ///
    /// `open` accepts an applied set that is a subset of the embedded one — the remaining files are
    /// then applied — and refuses a database that diverges from it: SQLx's `VersionMissing` when an
    /// applied version is not embedded, `VersionMismatch` when an applied version's SQL has since
    /// changed, and `ExecuteMigration` when a pending file reruns over tables that already exist.
    /// There is no upgrade, downgrade or data-movement path: a refused database is recreated, not
    /// converted.
    pub async fn open(path: &Path) -> Result<Self, sqlx::Error> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self {
            pool,
            seam: Arc::new(SnapshotSeam::default()),
        })
    }

    pub async fn close(self) {
        self.pool.close().await;
    }

    /// A handle to this store's pool, for adapters that bind over the same connection.
    pub fn pool(&self) -> SqlitePool {
        self.pool.clone()
    }
}
