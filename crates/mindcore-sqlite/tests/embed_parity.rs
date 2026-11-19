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
        // Fail closed, component by component. The path must literally start
        // with the fixture root's components — so `..`, `.` and foreign
        // prefixes refuse — and every existing element between the root and
        // the final component must be a directory: a regular file
        // (`root/file/x`) or a symlink (`root/alias/x` where alias -> root)
        // cannot be walked through, and neither can a symlinked tail.
        let mut tail = path.components();
        for expected in root.components() {
            match tail.next() {
                Some(actual) if actual == expected => {}
                _ => {
                    return Err(format!(
                        "{} resolves outside the fixture workspace",
                        path.display()
                    ));
                }
            }
        }
        let mut probe = root.to_path_buf();
        for component in tail {
            match component {
                std::path::Component::Normal(name) => probe.push(name),
                _ => {
                    return Err(format!("{} contains a non-path component", path.display()));
                }
            }
            match probe.symlink_metadata() {
                Ok(meta) if meta.file_type().is_symlink() => {
                    return Err(format!("{} crosses a symlink", probe.display()));
                }
                Ok(meta) if !meta.is_dir() => {
                    return Err(format!("{} is not a directory", probe.display()));
                }
                Ok(_) => {}
                // Beyond the last existing component nothing can mislead the
                // open: a missing tail is created under a verified prefix.
                Err(_) => break,
            }
        }
        Ok(())
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
fn fixture_guard_refuses_alias_dotdot_and_file_components() {
    let fixture = Fixture::fresh();
    // `alias -> root`: `root/alias/child/../absent.db` lexically stays inside,
    // but a prefix-only check misses that `alias` is a symlink, and the `..`
    // then escapes by resolution.
    let alias = fixture.root.join("alias");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&fixture.root, &alias).expect("alias symlink");
    #[cfg(windows)]
    std::os::windows::fs::symlink_dir(&fixture.root, &alias).expect("alias symlink");
    assert!(Fixture::guard(
        &fixture.root,
        &alias.join("child").join("..").join("absent.db")
    )
    .is_err());
    // A regular file is not a directory: `root/file/absent.db` cannot be a
    // fixture path even though its components stay under the root.
    let file = fixture.root.join("file");
    std::fs::write(&file, b"").unwrap();
    assert!(Fixture::guard(&fixture.root, &file.join("absent.db")).is_err());
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
