//! Competing handles on one database: the retain and settlement paths are exercised by two
//! independent stores, so a double apply or a torn journal/receipt pair fails here instead of
//! passing on a sequential simulation.
//!
//! One store's pool holds a single connection (`MindStore::open`), so genuine competition needs
//! two handles over the same file; `cross_handle_cas_lets_exactly_one_writer_win` in
//! `tests/knowledge.rs` establishes the same shape for knowledge writes. Settlement contention is
//! constructed rather than scheduled: both caller transactions read the pending pair before
//! either settles, so the loser cannot be a replay of the winner's commit.
//!
//! What a settlement loser observes is SQLite's writer refusal, never the settlement's row
//! predicate: the loser's read pins a snapshot the winner's commit invalidates, and SQLite refuses
//! that connection's first write before any statement runs, so the `rows_affected == 0` branch in
//! `settle_journal` cannot answer on a second connection. It cannot answer on one connection
//! either — settling reads and writes the pair inside one transaction, so nothing else can move
//! the intent between them. The predicate's reachable twin is the sequential pre-read guard
//! asserted by `mindcore-spoke`'s `settle_journal_reports_mismatch_as_uncertain`: the same
//! `OUTCOME_UNCERTAIN` refusal for an intent that moved before the settlement read it.

/// The refusal SQLite's writer produces when it refuses a transaction whose snapshot a competing
/// commit invalidated. `InternalError`/`retryable` is also what an unrelated pre-commit driver
/// failure produces, so the contention path is asserted by this detail, not by the class.
const REFUSED_BEFORE_COMMIT: &str = "a concurrent writer refused this transaction before commit";

#[allow(dead_code)]
#[path = "support/embedded_story.rs"]
mod embedded_story;

use mindcore_base::ids::SpaceId;
use mindcore_spoke::MemoryAdapter;
use mindcore_sqlite::MindStore;
use serde_json::{json, Value};
use spoke_operations::{SpokeReject, SpokeRejectCode, SpokeResult};
use sqlx::{Sqlite, Transaction};

use embedded_story::{
    attempt_retain, capture, ensure_subject, expect_ok, principal, provision_space, recalled,
    record_ids, settle_receipt, stage_review, TempDb,
};

fn hearing(key: &str) -> serde_json::Value {
    json!({
        "space_id": "wren",
        "subject_id": "wren",
        "capture_scope": { "visibility": "context", "context_id": "tavern" },
        "content": { "text": "Two handles hear the same news." },
        "occurred_at": "2026-11-19T17:40:00Z",
        "idempotency_key": key,
    })
}

/// The retry class a refusal reports: the protocol promises every reject names its `retry_class`,
/// so a refusal without one is itself a defect.
fn retry_class(reject: &SpokeReject) -> &str {
    reject
        .details
        .as_ref()
        .and_then(|details| details.get("retry_class"))
        .and_then(Value::as_str)
        .expect("every reject names its retry_class")
}

/// The pair a settlement reads before it writes, asserted inside the caller transaction so that
/// transaction's snapshot is fixed on the pending state: a later write from it is a contended
/// settlement, not a replay of a commit that arrived in between.
async fn assert_pending_pair(tx: &mut Transaction<'_, Sqlite>, receipt_id: &str) {
    let (status, state): (String, String) = sqlx::query_as(
        "SELECT r.status, j.state FROM memory_receipts r \
         JOIN memory_journal j ON j.receipt_id = r.receipt_id WHERE r.receipt_id = ?",
    )
    .bind(receipt_id)
    .fetch_one(&mut **tx)
    .await
    .expect("the pending pair");
    assert_eq!(
        status, "pending",
        "receipt pending before either settlement"
    );
    assert_eq!(state, "pending", "intent pending before either settlement");
}

#[tokio::test]
async fn competing_handles_retain_one_candidate() {
    let temp = TempDb::new("retain-race");
    let path = temp.database_path();
    let left = MindStore::open(&path).await.expect("open the left handle");
    let left_pool = left.pool();
    provision_space(&left_pool, "wren", "caller_wren").await;
    let right = MindStore::open(&path).await.expect("open the right handle");
    let right_pool = right.pool();
    let caller = principal("caller_wren");
    let adapter = MemoryAdapter::new(false);
    ensure_subject(&left_pool, &adapter, &caller).await;
    let candidate = capture(&left_pool, &adapter, &caller, hearing("cap-race")).await;

    // Both handles review, publish and settle the same candidate on their own connections.
    let (left_result, right_result) = tokio::join!(
        attempt_retain(
            &left_pool,
            &adapter,
            &caller,
            temp.root(),
            &candidate,
            "race-left"
        ),
        attempt_retain(
            &right_pool,
            &adapter,
            &caller,
            temp.root(),
            &candidate,
            "race-right"
        ),
    );
    let (_, winner) = match (left_result, right_result) {
        (SpokeResult::Ok(retained), SpokeResult::Reject(reject))
        | (SpokeResult::Reject(reject), SpokeResult::Ok(retained)) => {
            // The loser's refusal depends on how far its transaction got before the winner
            // committed: a consumed candidate, a lost revision CAS, or a write SQLite refused
            // once the sibling's commit invalidated the loser's WAL snapshot. The last case is
            // contention, and a retain loser never commits, so it is a retryable pre-commit
            // failure — never `OUTCOME_UNCERTAIN`, which would ask the caller to resolve an
            // outcome this transaction provably does not have. Only the message separates that
            // contention from any other pre-commit driver failure, so it is asserted.
            assert!(
                matches!(
                    reject.code,
                    SpokeRejectCode::CandidateConsumed | SpokeRejectCode::RevisionConflict
                ) || (reject.code == SpokeRejectCode::InternalError
                    && retry_class(&reject) == "retryable"
                    && reject.message == REFUSED_BEFORE_COMMIT),
                "a competing retention must lose with a competition outcome or the write-lock \
                 refusal, not {reject:?}"
            );
            retained
        }
        _ => panic!("exactly one concurrent retention must win"),
    };

    // One candidate, one record: the loser's staged record rolled back with its transaction.
    for pool in [left_pool.clone(), right_pool.clone()] {
        let records = recalled(
            &pool,
            &adapter,
            &caller,
            "wren",
            json!({ "mode": "subject_all" }),
        )
        .await;
        assert_eq!(
            record_ids(&records),
            vec![winner.as_str()],
            "the race must leave exactly the winner's record"
        );
        let applied: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM memory_journal WHERE state = 'applied'")
                .fetch_one(&pool)
                .await
                .expect("count applied intents");
        assert_eq!(applied, 1, "exactly one journal intent is applied");
        let pending: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM memory_receipts WHERE status = 'pending'")
                .fetch_one(&pool)
                .await
                .expect("count pending receipts");
        assert_eq!(pending, 0, "no receipt is left pending");
    }

    left.close().await;
    right.close().await;
}

#[tokio::test]
async fn competing_settlements_keep_the_pending_pair_atomic() {
    let temp = TempDb::new("settle-race");
    let path = temp.database_path();
    let left = MindStore::open(&path).await.expect("open the left handle");
    let left_pool = left.pool();
    provision_space(&left_pool, "wren", "caller_wren").await;
    let right = MindStore::open(&path).await.expect("open the right handle");
    let right_pool = right.pool();
    let caller = principal("caller_wren");
    let adapter = MemoryAdapter::new(false);
    ensure_subject(&left_pool, &adapter, &caller).await;
    let candidate = capture(&left_pool, &adapter, &caller, hearing("cap-settle")).await;

    // Stage one intent through the left handle; the receipt stays pending for both to settle.
    let receipt = expect_ok(
        stage_review(
            &left_pool,
            &adapter,
            &caller,
            temp.root(),
            &candidate,
            "settle-left",
        )
        .await,
        "stage the review",
    );
    // Both handles open their settlement transaction and read the pending pair before either
    // writes. Each read is awaited in turn, so the overlap is built, not sampled: the winner only
    // commits after both snapshots hold the pending receipt over the pending intent.
    let mut winner_tx = left_pool.begin().await.expect("begin the left settlement");
    let mut loser_tx = right_pool
        .begin()
        .await
        .expect("begin the right settlement");
    for tx in [&mut winner_tx, &mut loser_tx] {
        assert_pending_pair(tx, &receipt).await;
    }
    let space = SpaceId::new("wren");

    let settled = expect_ok(
        adapter
            .settle_journal(&mut winner_tx, &space, &receipt)
            .await,
        "settle on the left handle",
    );
    assert_eq!(settled["status"], json!("committed"));
    assert_eq!(settled["receipt_id"], json!(receipt));
    winner_tx
        .commit()
        .await
        .expect("commit the winning settlement");

    // The loser already observed the pending pair, so its settlement is a write against that
    // closed snapshot rather than a read of the winner's commit. The serialized writer refuses it
    // before any statement runs — the `rows_affected == 0` predicate below never answers — and the
    // protocol reports uncertainty; any successful reply here means the pending pair was replayed
    // as a second settlement.
    //
    // This loser differs from the retain race above: the staged intent and its pending receipt are
    // already durable, so the pre-commit refusal lands on `unsettled` and becomes `OUTCOME_UNCERTAIN`
    // for the caller to resolve. A retain loser never committed anything, so its refusal stays the
    // retryable pre-commit class.
    match adapter
        .settle_journal(&mut loser_tx, &space, &receipt)
        .await
    {
        SpokeResult::Reject(reject) => {
            assert_eq!(
                reject.code,
                SpokeRejectCode::OutcomeUncertain,
                "a contended settlement loses without a terminal second outcome: {reject:?}"
            );
            assert_eq!(
                reject.message, REFUSED_BEFORE_COMMIT,
                "the write lock answered, not the settlement's row predicate: {reject:?}"
            );
        }
        SpokeResult::Ok(body) => panic!(
            "a settlement that observed the pending pair must not replay the winner's commit: \
             {body}"
        ),
    }
    loser_tx
        .rollback()
        .await
        .expect("rollback the losing settlement");

    // A settlement from a fresh snapshot replays the one committed outcome.
    let replay = expect_ok(
        settle_receipt(&left_pool, &adapter, "wren", &receipt).await,
        "replay the settlement",
    );
    assert_eq!(replay["status"], json!("committed"));
    assert_eq!(replay["receipt_id"], json!(receipt));

    // The contended settlement left the pair single-applied: applied intent, scrubbed targets,
    // committed receipt.
    let (state, targets, status): (String, String, String) = sqlx::query_as(
        "SELECT j.state, j.targets_json, r.status FROM memory_journal j \
         JOIN memory_receipts r ON r.receipt_id = j.receipt_id WHERE j.receipt_id = ?",
    )
    .bind(&receipt)
    .fetch_one(&left_pool)
    .await
    .expect("settled pair");
    assert_eq!(state, "applied");
    assert_eq!(targets, "[]");
    assert_eq!(status, "committed");
    let records = recalled(
        &left_pool,
        &adapter,
        &caller,
        "wren",
        json!({ "mode": "subject_all" }),
    )
    .await;
    assert_eq!(records.len(), 1, "settlement applies the retention once");

    left.close().await;
    right.close().await;
}
