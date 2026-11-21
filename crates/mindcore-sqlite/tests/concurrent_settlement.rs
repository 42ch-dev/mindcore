//! Competing handles on one database: the retain and settlement paths are exercised by two
//! independent stores whose transactions overlap, so a lost CAS, a double apply or a torn
//! journal/receipt pair fails here instead of passing on a sequential simulation.
//!
//! One store's pool holds a single connection (`MindStore::open`), so genuine competition needs
//! two handles over the same file; `cross_handle_cas_lets_exactly_one_writer_win` in
//! `tests/knowledge.rs` establishes the same shape for knowledge writes.

#[allow(dead_code)]
#[path = "support/embedded_story.rs"]
mod embedded_story;

use mindcore_spoke::MemoryAdapter;
use mindcore_sqlite::MindStore;
use serde_json::json;
use spoke_operations::{SpokeRejectCode, SpokeResult};

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
    let (left_result, right_result) = tokio::join!(
        settle_receipt(&left_pool, &adapter, "wren", &receipt),
        settle_receipt(&right_pool, &adapter, "wren", &receipt),
    );

    let mut committed = 0;
    for result in [left_result, right_result] {
        match result {
            // A settlement that read the committed receipt replays it; both sides then report the
            // same durable outcome, never a second one.
            SpokeResult::Ok(body) => {
                committed += 1;
                assert_eq!(body["status"], json!("committed"));
                assert_eq!(body["receipt_id"], json!(receipt));
            }
            // The losing settlement must report uncertainty, never a terminal second outcome.
            SpokeResult::Reject(reject) => assert_eq!(
                reject.code,
                SpokeRejectCode::OutcomeUncertain,
                "a losing settlement reports uncertainty: {reject:?}"
            ),
        }
    }
    assert!(committed > 0, "at least one competing settlement commits");

    // Whichever interleaving happened, the pair is single-applied: applied intent, scrubbed
    // targets, committed receipt.
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
