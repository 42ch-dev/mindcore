//! Competing handles on one database: the retain and settlement paths are exercised by two
//! independent stores, so a double apply or a torn journal/receipt pair fails here instead of
//! passing on a sequential simulation.
//!
//! One store's pool holds a single connection (`MindStore::open`), so genuine competition needs
//! two handles over the same file; `cross_handle_cas_lets_exactly_one_writer_win` in
//! `tests/knowledge.rs` establishes the same shape for knowledge writes. Settlement contention is
//! constructed rather than scheduled: both caller transactions read the pending pair before
//! either settles, so the loser cannot be a replay of the winner's commit.

#[allow(dead_code)]
#[path = "support/embedded_story.rs"]
mod embedded_story;

use mindcore_base::ids::SpaceId;
use mindcore_spoke::MemoryAdapter;
use mindcore_sqlite::MindStore;
use serde_json::json;
use spoke_operations::{SpokeRejectCode, SpokeResult};
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
    assert_eq!(status, "pending", "receipt pending before either settlement");
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
            // committed: a consumed candidate, a lost revision CAS, or SQLite's pre-commit
            // write refusal once the sibling's commit invalidated the loser's WAL snapshot.
            assert!(
                matches!(
                    reject.code,
                    SpokeRejectCode::CandidateConsumed
                        | SpokeRejectCode::RevisionConflict
                        | SpokeRejectCode::InternalError
                ),
                "a competing retention must lose with a competition outcome, not {reject:?}"
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
        let applied: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM memory_journal WHERE state = 'applied'",
        )
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
    let mut loser_tx = right_pool.begin().await.expect("begin the right settlement");
    for tx in [&mut winner_tx, &mut loser_tx] {
        assert_pending_pair(tx, &receipt).await;
    }
    let space = SpaceId::new("wren");

    let settled = expect_ok(
        adapter.settle_journal(&mut winner_tx, &space, &receipt).await,
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
    // and the protocol reports uncertainty; any successful reply here means the pending pair was
    // replayed as a second settlement.
    match adapter.settle_journal(&mut loser_tx, &space, &receipt).await {
        SpokeResult::Reject(reject) => assert_eq!(
            reject.code,
            SpokeRejectCode::OutcomeUncertain,
            "a contended settlement loses without a terminal second outcome: {reject:?}"
        ),
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
