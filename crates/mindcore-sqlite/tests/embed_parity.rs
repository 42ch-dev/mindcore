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
use mindcore_base::memory::{MemoryCandidate, MemoryStore, RecallQuery};
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
        std::fs::create_dir_all(&root).expect("fixture root");
        // `/var` is a symlink to `/private/var` on macOS; canonicalize once so
        // the guard compares canonical parents against a canonical root.
        let root = root.canonicalize().expect("canonical fixture root");
        Self { root }
    }

    fn db_path(&self) -> PathBuf {
        let candidate = self.root.join("parity-fixture.db");
        Self::guard(&self.root, &candidate)
            .unwrap_or_else(|reason| panic!("fixture guard refused its own path: {reason}"));
        candidate
    }

    fn guard(root: &Path, path: &Path) -> Result<(), String> {
        if path.exists() {
            return Err(format!("{} already exists", path.display()));
        }
        let parent = path
            .parent()
            .and_then(|p| p.canonicalize().ok())
            .ok_or_else(|| format!("{} has no resolvable parent", path.display()))?;
        if parent != root {
            return Err(format!(
                "{} resolves outside the fixture workspace",
                path.display()
            ));
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
    assert!(Fixture::guard(root, &root.join("nested").join("x.db")).is_err());
    assert!(Fixture::guard(root, &root.join("..").join("x.db")).is_err());
    assert!(Fixture::guard(root, Path::new("/tmp/foreign-parity.db")).is_err());
    let existing = fixture.db_path();
    std::fs::write(&existing, b"").unwrap();
    assert!(Fixture::guard(root, &existing).is_err());
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
/// stages on a transaction it opens itself. A caller holding `BEGIN IMMEDIATE`
/// on the file blocks it outright — Mindcore can never borrow the caller's
/// transaction, so a caller rollback cannot cover Mindcore's rows.
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
    // (src/memory.rs:163-166), which busy-waits behind the caller until its
    // timeout refuses it.
    let retained =
        tokio::time::timeout(Duration::from_secs(8), store.retain("cand_b", "req_2")).await;
    let error = match retained {
        Ok(Ok(_)) => panic!("mindcore retain committed inside the caller transaction"),
        Ok(Err(e)) => e,
        Err(elapsed) => panic!("mindcore retain did not settle: {elapsed}"),
    };
    assert!(
        error.to_string().contains("locked") || error.to_string().contains("timed out"),
        "expected a lock refusal, got {error}"
    );
    caller_tx.rollback().await.expect("caller rollback");
}
