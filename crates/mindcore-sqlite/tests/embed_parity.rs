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

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::Utc;
use mindcore_base::ids::{ContextId, SpaceId, SubjectId};
use mindcore_base::memory::{MemoryCandidate, MemoryError, MemoryStore, RecallQuery};
use mindcore_sqlite::MindStore;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Harness-owned fixture root plus a path guard: the only sanctioned database
/// is a fresh file directly inside this root; anything else is refused before
/// a connection opens.
struct Fixture {
    root: PathBuf,
}

impl Fixture {
    /// Exclusive create: refuses to adopt a pre-existing directory, so `Drop`
    /// can only ever remove a root this harness itself created.
    fn open(root: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir(&root)?;
        // `/var` is a symlink to `/private/var` on macOS; canonicalize once so
        // the guard compares canonical parents against a canonical root.
        let root = root.canonicalize()?;
        Ok(Self { root })
    }

    fn fresh() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before the epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "mindcore-parity-{}-{nanos}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        Self::open(root).expect("fresh fixture root")
    }

    fn db_path(&self) -> PathBuf {
        let candidate = self.root.join("parity-fixture.db");
        Self::guard(&self.root, &candidate)
            .unwrap_or_else(|reason| panic!("fixture guard refused its own path: {reason}"));
        candidate
    }

    fn guard(root: &Path, path: &Path) -> Result<(), String> {
        // `symlink_metadata` inspects the final component itself, so a file,
        // directory or symlink — including a dangling link that `exists()`
        // would report as absent — is refused before a connection opens.
        if path.symlink_metadata().is_ok() {
            return Err(format!("{} already exists", path.display()));
        }
        // Walk up from the path: any existing symlink component is refused,
        // and the walk ends in one of two verdicts — reaching the fixture root
        // (inside) or canonicalizing an ancestor that sits outside it.
        let mut cursor = path.parent();
        while let Some(dir) = cursor {
            match dir.symlink_metadata() {
                Ok(meta) if meta.file_type().is_symlink() => {
                    return Err(format!("{} crosses a symlink", dir.display()));
                }
                Ok(_) => {
                    if let Ok(real) = dir.canonicalize() {
                        if real == root {
                            return Ok(());
                        }
                        if !real.starts_with(root) {
                            return Err(format!(
                                "{} resolves outside the fixture workspace",
                                path.display()
                            ));
                        }
                    }
                }
                Err(_) => {}
            }
            cursor = dir.parent();
        }
        Err(format!("{} has no resolvable parent", path.display()))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
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

#[test]
fn fixture_guard_refuses_foreign_paths() {
    let fixture = Fixture::fresh();
    let root = &fixture.root;
    // Nested paths inside the workspace are allowed; `..` and foreign roots
    // are refused.
    assert!(Fixture::guard(root, &root.join("nested").join("x.db")).is_ok());
    assert!(Fixture::guard(root, &root.join("..").join("x.db")).is_err());
    assert!(Fixture::guard(root, Path::new("/tmp/foreign-parity.db")).is_err());
    let existing = fixture.db_path();
    std::fs::write(&existing, b"").unwrap();
    assert!(Fixture::guard(root, &existing).is_err());
}

#[test]
fn fixture_guard_refuses_dangling_symlink() {
    let fixture = Fixture::fresh();
    let foreign = std::env::temp_dir().join("mindcore-parity-foreign-target.db");
    let link = fixture.root.join("parity-link.db");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&foreign, &link).expect("symlink");
    #[cfg(windows)]
    std::os::windows::fs::symlink_file(&foreign, &link).expect("symlink");
    // The target does not exist: `Path::exists()` reports false, so the old
    // guard passed this path. `symlink_metadata` sees the link itself.
    assert!(!link.exists());
    assert!(Fixture::guard(&fixture.root, &link).is_err());
}

#[test]
fn fixture_root_refuses_to_adopt_existing_directory() {
    let preexisting = Fixture::fresh();
    let claimed = Fixture::open(preexisting.root.clone());
    assert!(claimed.is_err(), "adopting a pre-existing root must fail");
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
    if let Some(code) = db.code().and_then(|c| c.parse::<i64>().ok()) {
        return code & 0xFF == 5 || code & 0xFF == 6;
    }
    db.message().contains("database is locked")
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

    // The blocked attempt left nothing behind, and the caller's rollback could
    // not touch the earlier capture: Mindcore's commits are durable outside
    // the caller's transaction boundary.
    let candidates: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM memory_candidates")
        .fetch_one(&caller_pool)
        .await
        .expect("candidate count");
    assert_eq!(candidates, 1);
    let records: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM memory_records")
        .fetch_one(&caller_pool)
        .await
        .expect("record count");
    assert_eq!(records, 0);
    let receipts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM review_receipts")
        .fetch_one(&caller_pool)
        .await
        .expect("receipt count");
    assert_eq!(receipts, 0);
}
