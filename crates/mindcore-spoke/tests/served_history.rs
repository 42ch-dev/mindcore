//! The served history port's per-operation authorization: compass D10.
//!
//! `serving-seam.md` says history grants no access the knowledge face does not. The port is driven
//! directly, as the composed face drives it, over a recording double: each refusal is shown before
//! the wrapped port runs, and each allowed call is shown to reach it.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;
use sqlx::SqlitePool;
use tempfile::TempDir;

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::MindId;
use mindcore_spoke::MindHistoryPort;
use mindcore_sqlite::minds::CreateMind;
use mindcore_sqlite::MindStore;
use spoke_operations::{
    spoke_reject, HistoryPort, HistoryPruneResult, SpokeReject, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::{Principal, RevisionSnapshot, Scope};

const MIND: &str = "spc_wren";
const OWNER: &str = "p_wren";
const RECALL_ONLY: &str = "p_recall";
const READER: &str = "p_reader";
const WRITER: &str = "p_writer";
/// One caller that holds `knowledge_read` and `knowledge_write` on two separate live grants.
const SPLIT: &str = "p_split";
const NOW: &str = "2026-11-26T09:00:00.000Z";

/// The wrapped port's own answer, distinct from any refusal the guard gives, so reaching it is
/// observable on the result as well as in the recording.
const REACHED: &str = "recorded port reached";

/// One Mind created through the library's own entry point, so the space and the owner's default
/// grant are exactly what a served face would find.
struct Harness {
    _dir: TempDir,
    pool: SqlitePool,
    port: MindHistoryPort,
    reached: Arc<Mutex<Vec<&'static str>>>,
}

impl Harness {
    async fn open() -> Self {
        let dir = TempDir::new().expect("temporary directory");
        let store = MindStore::open(&dir.path().join("served_history.db"))
            .await
            .expect("open store");
        let mut tx = store.pool().begin().await.expect("begin");
        let created = mindcore_sqlite::minds::create(
            &mut tx,
            &CreateMind {
                mind_id: Some(MindId::new(MIND)),
                display_name: None,
                owner: CallerId::new(OWNER),
            },
        )
        .await;
        match created {
            SpokeResult::Ok(_) => {}
            SpokeResult::Reject(reject) => panic!("seed the mind: {reject:?}"),
        }
        tx.commit().await.expect("commit mind");

        let reached = Arc::new(Mutex::new(Vec::new()));
        let port = MindHistoryPort::new(
            store.pool(),
            Arc::new(RecordingPort {
                calls: reached.clone(),
            }),
        );
        Self {
            _dir: dir,
            pool: store.pool(),
            port,
            reached,
        }
    }

    async fn seed_grant(&self, principal: &str, capabilities: &[&str]) {
        self.seed_grant_with_id(&format!("grt_{principal}"), principal, capabilities)
            .await;
    }

    async fn seed_grant_with_id(&self, grant_id: &str, principal: &str, capabilities: &[&str]) {
        sqlx::query(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
              shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES (?, ?, NULL, ?, ?, '[]', 0, NULL, NULL, 1, ?)",
        )
        .bind(grant_id)
        .bind(MIND)
        .bind(principal)
        .bind(serde_json::to_string(capabilities).expect("capabilities"))
        .bind(NOW)
        .execute(&self.pool)
        .await
        .expect("seed grant");
    }

    /// How many live grant rows `principal` holds in the Mind.
    async fn grants_of(&self, principal: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM memory_grants \
             WHERE space_id = ? AND principal_id = ? AND revoked_at IS NULL",
        )
        .bind(MIND)
        .bind(principal)
        .fetch_one(&self.pool)
        .await
        .expect("count the principal's grants")
    }

    fn calls(&self) -> Vec<&'static str> {
        self.reached.lock().expect("recording").clone()
    }

    async fn replay(&self, principal: &str) -> SpokeResult<RevisionSnapshot> {
        self.port
            .get_history_snapshot(
                &principal_of(principal),
                &scope(),
                "knowledge_entry",
                "entry-1",
                1,
            )
            .await
    }

    async fn prune(&self, principal: &str) -> SpokeResult<HistoryPruneResult> {
        self.port
            .prune_history(
                &principal_of(principal),
                &scope(),
                "knowledge_entry",
                "entry-1",
                1,
            )
            .await
    }
}

/// The double the wrapper wraps: it records that it ran and answers its own text.
struct RecordingPort {
    calls: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl HistoryPort for RecordingPort {
    async fn get_history_snapshot(
        &self,
        _principal: &Principal,
        _scope: &Scope,
        _entity_kind: &str,
        _entity_id: &str,
        _revision: u64,
    ) -> SpokeResult<RevisionSnapshot> {
        self.calls.lock().expect("recording").push("replay");
        reached()
    }

    async fn prune_history(
        &self,
        _principal: &Principal,
        _scope: &Scope,
        _entity_kind: &str,
        _entity_id: &str,
        _keep: u64,
    ) -> SpokeResult<HistoryPruneResult> {
        self.calls.lock().expect("recording").push("prune");
        reached()
    }
}

fn reached<T>() -> SpokeResult<T> {
    spoke_reject(SpokeRejectCode::HistoryEntityUnsupported, REACHED, None)
}

fn scope() -> Scope {
    serde_json::from_value(json!({ "scope_id": MIND })).expect("scope JSON matches the schema")
}

fn principal_of(principal_id: &str) -> Principal {
    serde_json::from_value(json!({ "principal_id": principal_id, "extensions": {} }))
        .expect("principal JSON matches the schema")
}

fn expect_refused<T: std::fmt::Debug>(result: SpokeResult<T>, context: &str) -> SpokeReject {
    match result {
        SpokeResult::Ok(value) => panic!("{context}: unexpectedly accepted with {value:?}"),
        SpokeResult::Reject(reject) => reject,
    }
}

fn assert_fixed_refusal(reject: &SpokeReject, context: &str) {
    assert_eq!(
        reject.code,
        SpokeRejectCode::NotAuthorized,
        "{context}: refused on the fixed answer"
    );
    assert_eq!(
        reject.message, "knowledge scope not authorized",
        "{context}: Connect's own admission-deny message"
    );
    assert!(
        reject.details.is_none(),
        "{context}: the refusal carries no details: {reject:?}"
    );
}

fn assert_reached<T: std::fmt::Debug>(result: SpokeResult<T>, context: &str) {
    let reject = expect_refused(result, context);
    assert_eq!(
        reject.message, REACHED,
        "{context}: the wrapped port answered"
    );
}

/// A principal holding only a memory capability gets neither history op: `recall` reaches no
/// knowledge capability.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_principal_holding_only_recall_is_refused_before_the_port_runs() {
    let harness = Harness::open().await;
    harness.seed_grant(RECALL_ONLY, &["recall"]).await;

    assert_fixed_refusal(
        &expect_refused(harness.replay(RECALL_ONLY).await, "replay"),
        "replay",
    );
    assert_fixed_refusal(
        &expect_refused(harness.prune(RECALL_ONLY).await, "prune"),
        "prune",
    );
    assert!(
        harness.calls().is_empty(),
        "the wrapped port was not reached"
    );
}

/// A replay needs `knowledge_read`, and `knowledge_read` alone is enough for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_needs_knowledge_read() {
    let harness = Harness::open().await;
    harness.seed_grant(READER, &["knowledge_read"]).await;

    assert_reached(harness.replay(READER).await, "replay under knowledge_read");
    assert_eq!(harness.calls(), vec!["replay"], "the wrapped port ran once");
}

/// A replay is not a write: `knowledge_write` without `knowledge_read` is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_is_refused_for_knowledge_write_without_knowledge_read() {
    let harness = Harness::open().await;
    harness.seed_grant(WRITER, &["knowledge_write"]).await;

    assert_fixed_refusal(
        &expect_refused(harness.replay(WRITER).await, "replay"),
        "replay",
    );
    assert!(
        harness.calls().is_empty(),
        "the wrapped port was not reached"
    );
}

/// A prune needs one grant covering both capabilities: the write alone is refused, and so is the
/// read alone, because the prune reads the entry's permission before it deletes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prune_needs_one_grant_covering_write_and_read() {
    let harness = Harness::open().await;
    harness.seed_grant(WRITER, &["knowledge_write"]).await;
    harness.seed_grant(READER, &["knowledge_read"]).await;

    assert_fixed_refusal(
        &expect_refused(harness.prune(WRITER).await, "prune"),
        "prune",
    );
    assert_fixed_refusal(
        &expect_refused(harness.prune(READER).await, "prune"),
        "prune",
    );
    assert!(
        harness.calls().is_empty(),
        "the wrapped port was not reached"
    );
}

/// Requirements never union: ONE caller holding `knowledge_read` and `knowledge_write` on two
/// separate live grants in the same Mind is refused, because the prune's one grant must cover both.
///
/// This is the invariant the two-principal case above cannot establish: two callers each missing a
/// capability would still pass if the guard made two independent single-capability reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prune_is_refused_when_one_caller_holds_read_and_write_on_separate_grants() {
    let harness = Harness::open().await;
    harness
        .seed_grant_with_id("grt_split_read", SPLIT, &["knowledge_read"])
        .await;
    harness
        .seed_grant_with_id("grt_split_write", SPLIT, &["knowledge_write"])
        .await;

    assert_eq!(
        harness.grants_of(SPLIT).await,
        2,
        "the caller holds two live grants, one capability each"
    );
    assert_fixed_refusal(
        &expect_refused(harness.prune(SPLIT).await, "a split-grant prune"),
        "a split-grant prune",
    );
    assert!(
        harness.calls().is_empty(),
        "the wrapped port was not reached"
    );

    assert_reached(harness.replay(SPLIT).await, "the split-grant replay");
    assert_eq!(harness.calls(), vec!["replay"], "only the replay ran");
}

/// The creator's default grant covers both operations, so both reach the wrapped port.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_creators_default_grant_reaches_the_port_for_both_operations() {
    let harness = Harness::open().await;

    assert_reached(harness.replay(OWNER).await, "owner replay");
    assert_reached(harness.prune(OWNER).await, "owner prune");
    assert_eq!(
        harness.calls(),
        vec!["replay", "prune"],
        "both reached the port"
    );
}
