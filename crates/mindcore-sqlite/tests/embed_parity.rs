//! v02-embed-parity Task 1 — Mindcore-side parity probes and fixture guard.
//!
//! The paired-selector run needs Mindcore's storage effect inside the transaction
//! Nexus's `begin_admission` already holds. `MindStore` cannot do that: `open`
//! is the only constructor (its own pool plus embedded migrator) and
//! `MemoryStore` methods stage over that owned pool — `retain` even begins its
//! own `BEGIN IMMEDIATE` (`src/memory.rs:163-166`). The borrowed-transaction
//! trait `mindcore_base::ir::MemoryRepository` has no implementation here.
//! The paired matrix therefore stops; these tests keep the runnable part of
//! the task — the fixture guard and the observable surface Mindcore does serve
//! across close/reopen — green.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::Utc;
use mindcore_base::ids::{ContextId, SpaceId, SubjectId};
use mindcore_base::memory::{MemoryCandidate, MemoryError, MemoryStore, RecallQuery};
use mindcore_sqlite::MindStore;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;

/// Harness-owned fixture root plus a path check: the only sanctioned database
/// is `parity-fixture.db` directly inside a private `TempDir`. Because the
/// root is a unique temporary directory the harness itself created, no path
/// walking is needed — the only adversarial state is the leaf itself.
struct Fixture {
    _tmp: TempDir,
    root: PathBuf,
}

impl Fixture {
    fn fresh() -> Self {
        let tmp = TempDir::new().expect("temp fixture root");
        let root = tmp.path().to_path_buf();
        Self { _tmp: tmp, root }
    }

    fn db_path(&self) -> PathBuf {
        let leaf = self.root.join("parity-fixture.db");
        Self::guard(&leaf).unwrap_or_else(|reason| panic!("fixture leaf refused: {reason}"));
        leaf
    }

    /// Leaf-only check: `symlink_metadata` on the leaf must be `NotFound`.
    /// Anything else — an existing file, directory, or symlink, or a metadata
    /// failure that isn't `NotFound` — refuses the path before a connection
    /// opens. That suffices because the parent is a private `TempDir` the
    /// harness controls: a caller cannot plant a `..` or symlink ancestor in
    /// its name, and no other process owns the root.
    fn guard(path: &Path) -> Result<(), String> {
        match path.symlink_metadata() {
            Ok(_) => Err(format!("{} already exists", path.display())),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(format!("cannot stat {}: {e}", path.display())),
        }
    }
}

fn candidate(candidate_id: &str, context: &str, content: &str) -> MemoryCandidate {
    MemoryCandidate {
        candidate_id: candidate_id.to_string(),
        space: SpaceId::new("spc_local_default"),
        subject: SubjectId::new("sbj_mira"),
        context: ContextId::new(context),
        content: content.to_string(),
        observed_at: Utc::now(),
    }
}

/// The guard only inspects the leaf: with a private `TempDir` root the
/// harness owns the parent, so no ancestor/`..`/symlink-component state can
/// exist. Anything present at the leaf — file, directory, or symlink,
/// dangling included — refuses.
#[test]
fn fixture_guard_refuses_existing_leaf() {
    let fixture = Fixture::fresh();
    let leaf = fixture.root.join("parity-fixture.db");
    assert!(Fixture::guard(&leaf).is_ok());
    std::fs::write(&leaf, b"").unwrap();
    assert!(Fixture::guard(&leaf).is_err());
    std::fs::remove_file(&leaf).unwrap();
    std::fs::create_dir(&leaf).unwrap();
    assert!(Fixture::guard(&leaf).is_err());
}

#[test]
fn fixture_guard_refuses_dangling_symlink() {
    let fixture = Fixture::fresh();
    let foreign = std::env::temp_dir().join("mindcore-parity-foreign-target.db");
    let link = fixture.db_path();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&foreign, &link).expect("symlink");
    #[cfg(windows)]
    std::os::windows::fs::symlink_file(&foreign, &link).expect("symlink");
    // The target does not exist: `Path::exists()` reports false, so a
    // metadata-only-on-real-target check would pass this. `symlink_metadata`
    // sees the link itself.
    assert!(!link.exists());
    assert!(Fixture::guard(&link).is_err());
}

/// True only for a real SQLite lock refusal (`SQLITE_BUSY` or
/// `SQLITE_BUSY_TIMEOUT`), never for a pool-acquisition timeout.
fn is_sqlite_lock(error: &MemoryError) -> bool {
    let MemoryError::Store(inner) = error else {
        return false;
    };
    let Some(sqlx::Error::Database(db)) = inner.downcast_ref::<sqlx::Error>() else {
        return false;
    };
    db.code()
        .and_then(|c| c.parse::<i64>().ok())
        .is_some_and(|code| code & 0xFF == 5 || code & 0xFF == 6)
}

/// The observable surface Mindcore serves for the baseline's read-side rows:
/// `memory.capture` → `memory.candidates.review` → `memory.recall`, then the
/// reopen observation the parity run asks for. Recorded here so the report can
/// quote real Mindcore values next to the Nexus expectations — the differences
/// (id shapes, receipt contents, classification) classify the gap, they do not
/// pass as parity.
#[tokio::test]
async fn mindcore_surface_observables_and_reopen() {
    let fixture = Fixture::fresh();
    let db = fixture.db_path();

    {
        let store = MindStore::open(&db).await.expect("open");
        let cand_a = candidate("cand_a", "ctx-a", "The northern gate.");
        store.capture(cand_a.clone()).await.expect("capture");
        // Exact same payload replays to Ok, matching Nexus's capture replay
        // shape only at the bool level — Mindcore returns no receipt object.
        store.capture(cand_a).await.expect("capture replay");
        let record = store.retain("cand_a", "req_1").await.expect("retain");
        assert_eq!(record.record_id, "rec_cand_a");
        store.close().await;
    }

    // Reopen: read-side observables persist through a fresh open.
    {
        let store = MindStore::open(&db).await.expect("reopen");
        let candidate = store
            .candidate("cand_a")
            .await
            .expect("candidate")
            .expect("candidate row");
        assert_eq!(candidate.content, "The northern gate.");
        let recalled = store
            .recall(&RecallQuery {
                space: SpaceId::new("spc_local_default"),
                subject: SubjectId::new("sbj_mira"),
                context: Some(ContextId::new("ctx-a")),
            })
            .await
            .expect("recall");
        assert_eq!(recalled.len(), 1);
        assert_eq!(recalled[0].record_id, "rec_cand_a");
        store.close().await;
    }
}

/// The transaction seam: `MemoryStore::retain` is the only effect path and it
/// stages on a transaction it opens itself. While a caller holds
/// `BEGIN IMMEDIATE` on the file, `retain`'s own begin is refused with a real
/// SQLite lock error, and nothing the attempt tried to write exists after the
/// caller rolls back.
#[tokio::test]
async fn mindstore_blocked_by_caller_held_transaction() {
    let fixture = Fixture::fresh();
    let db = fixture.db_path();
    let store = MindStore::open(&db).await.expect("open");
    store
        .capture(candidate("cand_b", "ctx-b", "seed"))
        .await
        .expect("capture");

    let caller_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&db)
                .busy_timeout(Duration::from_secs(1)),
        )
        .await
        .expect("caller pool");
    let caller_tx = sqlx::SqlitePool::begin_with(&caller_pool, "BEGIN IMMEDIATE")
        .await
        .expect("caller begin immediate");

    // `retain` cannot stage on the caller's transaction: it takes `&self` and
    // begins another `BEGIN IMMEDIATE` on the Mindcore pool
    // (src/memory.rs:163-166), which busy-waits behind the caller until SQLite
    // refuses it. The refusal must be the lock class itself, not a pool
    // acquisition timeout.
    let retained =
        tokio::time::timeout(Duration::from_secs(8), store.retain("cand_b", "req_2")).await;
    let error = match retained {
        Ok(Ok(_)) => panic!("mindcore retain committed inside the caller transaction"),
        Ok(Err(e)) => e,
        Err(elapsed) => panic!("mindcore retain did not settle: {elapsed}"),
    };
    assert!(
        is_sqlite_lock(&error),
        "expected a SQLite lock refusal (SQLITE_BUSY/BUSY_TIMEOUT), got {error}"
    );
    caller_tx.rollback().await.expect("caller rollback");

    // Named, not counted: the blocked attempt's own identities are absent
    // after the caller's rollback, while the earlier capture — committed
    // before the caller began, outside its boundary — is untouched.
    let cand_b: Option<String> = sqlx::query_scalar(
        "SELECT candidate_id FROM memory_candidates WHERE candidate_id = 'cand_b'",
    )
    .fetch_optional(&caller_pool)
    .await
    .expect("cand_b lookup");
    assert!(cand_b.is_some(), "capture was committed before the lock");
    let rec_from_b: Option<String> =
        sqlx::query_scalar("SELECT record_id FROM memory_records WHERE candidate_id = 'cand_b'")
            .fetch_optional(&caller_pool)
            .await
            .expect("rec_b lookup");
    assert!(rec_from_b.is_none(), "blocked retain left no record");
    let receipt_2: Option<String> =
        sqlx::query_scalar("SELECT record_id FROM review_receipts WHERE request_id = 'req_2'")
            .fetch_optional(&caller_pool)
            .await
            .expect("req_2 lookup");
    assert!(receipt_2.is_none(), "blocked retain left no receipt");
}
