//! Caller-owned transaction boundary: the repository stages reads and writes inside the
//! caller's open `sqlx::Transaction`; commit/rollback stay with the caller. Uses the Task-1
//! prepare fixture DDL (`settlement-prepare.sql`) on a temporary database.

use serde_json::{json, Map, Number};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use tempfile::TempDir;

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::{ContextId, SpaceId, SubjectId};
use mindcore_base::ir::{
    CandidateState, Classification, Consumption, MemoryCandidateIr, MemoryContent,
    MemoryJournalRow, MemoryRecordIr, MemoryRepository, Placement, Provenance, ReceiptRead,
    ReceiptState, RecordState, ReviewReceiptIr,
};
use mindcore_sqlite::SqliteMemoryRepository;

const DDL: &str = include_str!("fixtures/settlement-prepare.sql");
const TIME: &str = "2026-11-20T09:20:01.123Z";
const LATER: &str = "2026-11-20T09:24:00.456Z";
const DIGEST: &str = "034a5c6d74a6c8fa3fa25d0e41dd2b10d1a110464c5e9e3f56e7c1a22cf48696";

async fn open(path: &std::path::Path) -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true)
                .foreign_keys(false),
        )
        .await
        .expect("open pool")
}

async fn prepared(path: &std::path::Path) -> SqlitePool {
    let pool = open(path).await;
    sqlx::raw_sql(DDL).execute(&pool).await.expect("apply DDL");
    sqlx::query!(
        "INSERT INTO memory_spaces \
         (space_id, display_name, authority_host_id, authority_generation, status, revision, \
          created_at) \
         VALUES ('spc_workshop', 'Workshop', 'host-1', 1, 'active', 1, ?)",
        TIME,
    )
    .execute(&pool)
    .await
    .expect("seed space");
    sqlx::query!(
        "INSERT INTO memory_subjects \
         (space_id, subject_id, status, revision, created_at) \
         VALUES ('spc_workshop', 'sub_mira', 'active', 1, ?)",
        TIME,
    )
    .execute(&pool)
    .await
    .expect("seed subject");
    pool
}

fn pending_candidate() -> MemoryCandidateIr {
    MemoryCandidateIr {
        candidate_id: "cand_1".into(),
        space_id: "spc_workshop".into(),
        subject_id: "sub_mira".into(),
        capture_scope: Placement::Context("ctx_a".into()),
        classification: Classification {
            disposition: "promote".into(),
            rationale_codes: vec!["r1".into()],
            confidence: None,
        },
        provenance: Provenance {
            source_context_ids: vec!["ctx_a".into()],
            anchors: vec![],
            captured_by: "prn_writer".into(),
            captured_at: TIME.into(),
        },
        state: CandidateState::Pending {
            kind: "local_note".into(),
            content: MemoryContent {
                text: "Harbor closes at dusk".into(),
                structured: Some(json!({"rank": 1})),
                cognition: None,
            },
            expires_at: None,
        },
        occurred_at: Some(TIME.into()),
        created_at: TIME.into(),
        revision: Number::from(1),
        extensions: Map::new(),
    }
}

fn live_record() -> MemoryRecordIr {
    MemoryRecordIr {
        record_id: "rec_1".into(),
        space_id: "spc_workshop".into(),
        subject_id: "sub_mira".into(),
        kind: "local_note".into(),
        scope: Placement::Context("ctx_a".into()),
        provenance: Provenance {
            source_context_ids: vec!["ctx_a".into()],
            anchors: vec![],
            captured_by: "prn_writer".into(),
            captured_at: TIME.into(),
        },
        state: RecordState::Live {
            content: MemoryContent {
                text: "Harbor closes at dusk".into(),
                structured: Some(json!({"rank": 1})),
                cognition: None,
            },
            import_ref: None,
        },
        occurred_at: Some(TIME.into()),
        created_at: TIME.into(),
        updated_at: TIME.into(),
        revision: Number::from(1),
        extensions: Map::new(),
    }
}

fn committed_receipt() -> ReviewReceiptIr {
    ReviewReceiptIr {
        receipt_id: "rcp_1".into(),
        space_id: "spc_workshop".into(),
        requester: "prn_mira".into(),
        operation: "memory.candidates.review".into(),
        idempotency_key: Some("attempt-1".into()),
        payload_digest: DIGEST.into(),
        created_at: TIME.into(),
        state: ReceiptState::Committed {
            completed_at: LATER.into(),
            result: json!({"record_ids": ["rec_1"]}),
        },
        extensions: Map::new(),
    }
}

fn receipt_key() -> mindcore_base::ir::ReceiptKey {
    mindcore_base::ir::ReceiptKey {
        space: SpaceId::new("spc_workshop"),
        requester: CallerId::new("prn_mira"),
        operation: "memory.candidates.review".into(),
        idempotency_key: "attempt-1".into(),
    }
}

/// A read and a mutation staged on the caller's open transaction are visible inside it,
/// and caller commit makes both durable across a reopen.
#[tokio::test]
async fn caller_commit_persists_effect_and_receipt() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;

    {
        let pool = prepared(&path).await;
        let mut tx = pool.begin().await.unwrap();
        // Read in the open transaction: the seeded subject is visible.
        let subject = repo
            .subject(&mut tx, &SpaceId::new("spc_workshop"), "sub_mira")
            .await
            .unwrap()
            .expect("subject inside tx");
        assert_eq!(subject.status, "active");

        repo.insert_review(&mut tx, &live_record(), &committed_receipt())
            .await
            .unwrap();
        // Reads in the same open transaction observe the staged effect and receipt.
        assert!(repo
            .record(&mut tx, &SpaceId::new("spc_workshop"), "rec_1")
            .await
            .unwrap()
            .is_some());
        let key = receipt_key();
        assert!(repo.review_receipt(&mut tx, &key).await.unwrap().is_some());
        tx.commit().await.unwrap();
        pool.close().await;
    }

    let pool = open(&path).await;
    let mut tx = pool.begin().await.unwrap();
    let record = repo
        .record(&mut tx, &SpaceId::new("spc_workshop"), "rec_1")
        .await
        .unwrap()
        .expect("record persisted after reopen");
    assert_eq!(record.kind, "local_note");
    assert_eq!(record.extensions.len(), 0);
    let key = receipt_key();
    match repo
        .review_receipt(&mut tx, &key)
        .await
        .unwrap()
        .expect("receipt persisted after reopen")
    {
        ReceiptRead::Retained(receipt) => {
            assert_eq!(receipt.operation, "memory.candidates.review")
        }
        ReceiptRead::Evicted { .. } => panic!("receipt body must be retained"),
    }
    tx.rollback().await.unwrap();
    pool.close().await;
}

/// Forced caller rollback removes everything the repository staged on that transaction.
#[tokio::test]
async fn caller_rollback_drops_effect_and_receipt() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;

    {
        let pool = prepared(&path).await;
        let mut tx = pool.begin().await.unwrap();
        repo.insert_review(&mut tx, &live_record(), &committed_receipt())
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        pool.close().await;
    }

    let pool = open(&path).await;
    let mut tx = pool.begin().await.unwrap();
    assert!(repo
        .record(&mut tx, &SpaceId::new("spc_workshop"), "rec_1")
        .await
        .unwrap()
        .is_none());
    let key = receipt_key();
    assert!(repo.review_receipt(&mut tx, &key).await.unwrap().is_none());
    tx.rollback().await.unwrap();
    pool.close().await;
}

/// Candidate insert + CAS consume on the borrowed transaction.
#[tokio::test]
async fn candidate_insert_and_guarded_consume() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let mut tx = pool.begin().await.unwrap();
    repo.insert_candidate(&mut tx, &pending_candidate())
        .await
        .unwrap();
    let staged = repo
        .candidate(&mut tx, &SpaceId::new("spc_workshop"), "cand_1")
        .await
        .unwrap()
        .expect("staged candidate");
    assert!(matches!(staged.state, CandidateState::Pending { .. }));
    assert_eq!(staged.extensions.len(), 0);

    let mut consumed = staged.clone();
    consumed.state = CandidateState::Consumed {
        proposed_kind: Some("local_note".into()),
        consumed_by: Consumption {
            operation: "memory.candidates.review".into(),
            receipt_id: "rcp_1".into(),
            outcome: "promoted".into(),
            record_ids: vec!["rec_1".into()],
        },
        content_digest: DIGEST.into(),
    };
    consumed.revision = Number::from(2);
    assert!(repo
        .update_candidate(&mut tx, &consumed, &Number::from(1))
        .await
        .unwrap());
    // A stale observation does not match.
    assert!(!repo
        .update_candidate(&mut tx, &consumed, &Number::from(1))
        .await
        .unwrap());
    let read = repo
        .candidate(&mut tx, &SpaceId::new("spc_workshop"), "cand_1")
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        read.state,
        CandidateState::Consumed { ref consumed_by, .. } if consumed_by.receipt_id == "rcp_1"
    ));
    tx.commit().await.unwrap();
    pool.close().await;
}

/// Leased pending receipt staged, then settled by the same owner; a different owner is
/// refused while the lease is still live.
#[tokio::test]
async fn leased_receipt_settle_guarded_by_owner() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let pending = ReviewReceiptIr {
        receipt_id: "rcp_gen".into(),
        space_id: "spc_workshop".into(),
        requester: "prn_mira".into(),
        operation: "memory.reflections.generate".into(),
        idempotency_key: Some("gen-1".into()),
        payload_digest: DIGEST.into(),
        created_at: TIME.into(),
        state: ReceiptState::Pending,
        extensions: Map::new(),
    };
    // A lease that stays live for the test's lifetime keeps the wrong-owner guard intact.
    let lease = mindcore_base::ir::ReceiptLease {
        owner_id: "instance-1".into(),
        expires_at: "9999-01-01T00:00:00.000Z".into(),
    };

    let mut tx = pool.begin().await.unwrap();
    repo.insert_receipt(&mut tx, &pending, Some(&lease))
        .await
        .unwrap();

    let settled = ReviewReceiptIr {
        state: ReceiptState::Committed {
            completed_at: LATER.into(),
            result: json!({"reflection_id": "rfl_1", "cost": {"unit_kind": "reflection", "units": 1.0}}),
        },
        ..pending.clone()
    };
    // Wrong owner cannot settle while the lease is live.
    assert!(!repo
        .update_receipt(&mut tx, &settled, "pending", Some("instance-2"))
        .await
        .unwrap());
    // The observed owner can.
    assert!(repo
        .update_receipt(&mut tx, &settled, "pending", Some("instance-1"))
        .await
        .unwrap());
    match repo
        .receipt(&mut tx, &SpaceId::new("spc_workshop"), "rcp_gen")
        .await
        .unwrap()
        .unwrap()
    {
        ReceiptRead::Retained(receipt) => {
            assert!(matches!(receipt.state, ReceiptState::Committed { .. }))
        }
        ReceiptRead::Evicted { .. } => panic!("receipt body must be retained"),
    }
    tx.commit().await.unwrap();
    pool.close().await;
}

/// §7.0.3 liveness: a crashed generation leaves a pending receipt under its lease; past the
/// lease instant a different owner claims it, and only the persisted new owner settles.
#[tokio::test]
async fn expired_lease_pending_receipt_taken_over() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let pending = ReviewReceiptIr {
        receipt_id: "rcp_gen_crash".into(),
        space_id: "spc_workshop".into(),
        requester: "prn_mira".into(),
        operation: "memory.reflections.generate".into(),
        idempotency_key: Some("gen-crash".into()),
        payload_digest: DIGEST.into(),
        created_at: TIME.into(),
        state: ReceiptState::Pending,
        extensions: Map::new(),
    };
    // The crashed owner leased this receipt long enough ago that its lease is expired.
    let lease = mindcore_base::ir::ReceiptLease {
        owner_id: "instance-crashed".into(),
        expires_at: "2020-01-01T00:00:00.000Z".into(),
    };

    let mut tx = pool.begin().await.unwrap();
    repo.insert_receipt(&mut tx, &pending, Some(&lease))
        .await
        .unwrap();

    // A different owner claims the abandoned pending row: the CAS persists the new
    // owner and expiry inside this caller-owned transaction. The expiry is measured
    // from the claim instant (30s lease), not from when the transaction began.
    let claim_start = chrono::Utc::now();
    assert!(repo
        .claim_receipt_lease(
            &mut tx,
            &SpaceId::new("spc_workshop"),
            "rcp_gen_crash",
            "instance-2",
            30
        )
        .await
        .unwrap());
    let claim_end = chrono::Utc::now();
    // The stamped lease is in the future: a takeover stamped from the request
    // instant could already be expired and re-leasable on the very next retry.
    let stamped = repo
        .receipt_lease(&mut tx, &SpaceId::new("spc_workshop"), "rcp_gen_crash")
        .await
        .unwrap()
        .expect("claimed row carries a lease");
    let stamped_at = chrono::DateTime::parse_from_rfc3339(&stamped).expect("lease stamp");
    // The stamp is millisecond-truncated, so its instant can sit a millisecond
    // below the full-precision claim start.
    let earliest = claim_start + chrono::Duration::seconds(30) - chrono::Duration::milliseconds(1);
    let latest = claim_end + chrono::Duration::seconds(30);
    assert!(
        stamped_at >= earliest && stamped_at <= latest,
        "stamped lease {stamped} must sit inside the measured claim interval +30s \
         ({earliest}..={latest})"
    );
    // The claim is lost when the row no longer qualifies — already re-leased here, so the
    // second claimant gets no work and the crashed owner still cannot settle.
    assert!(!repo
        .claim_receipt_lease(
            &mut tx,
            &SpaceId::new("spc_workshop"),
            "rcp_gen_crash",
            "instance-3",
            30
        )
        .await
        .unwrap());

    let settled = ReviewReceiptIr {
        state: ReceiptState::Committed {
            completed_at: LATER.into(),
            result: json!({"reflection_id": "rfl_2", "cost": {"unit_kind": "reflection", "units": 1.0}}),
        },
        ..pending.clone()
    };
    // Neither the crashed nor the losing owner can settle: owner matching is exact, and
    // the expiry bypass is gone.
    for stale_owner in ["instance-crashed", "instance-3"] {
        assert!(!repo
            .update_receipt(&mut tx, &settled, "pending", Some(stale_owner))
            .await
            .unwrap());
    }
    // The persisted takeover owner settles.
    assert!(repo
        .update_receipt(&mut tx, &settled, "pending", Some("instance-2"))
        .await
        .unwrap());
    match repo
        .receipt(&mut tx, &SpaceId::new("spc_workshop"), "rcp_gen_crash")
        .await
        .unwrap()
        .unwrap()
    {
        ReceiptRead::Retained(receipt) => {
            assert!(matches!(receipt.state, ReceiptState::Committed { .. }))
        }
        ReceiptRead::Evicted { .. } => panic!("receipt body must be retained"),
    }
    tx.commit().await.unwrap();
    pool.close().await;
}

/// A settled row is never taken over: even past lease expiry the recorded outcome stands.
#[tokio::test]
async fn settled_receipt_not_taken_over_past_expiry() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let pending = ReviewReceiptIr {
        receipt_id: "rcp_gen_done".into(),
        space_id: "spc_workshop".into(),
        requester: "prn_mira".into(),
        operation: "memory.reflections.generate".into(),
        idempotency_key: Some("gen-done".into()),
        payload_digest: DIGEST.into(),
        created_at: TIME.into(),
        state: ReceiptState::Pending,
        extensions: Map::new(),
    };
    let lease = mindcore_base::ir::ReceiptLease {
        owner_id: "instance-1".into(),
        expires_at: "2020-01-01T00:00:00.000Z".into(),
    };

    let mut tx = pool.begin().await.unwrap();
    repo.insert_receipt(&mut tx, &pending, Some(&lease))
        .await
        .unwrap();
    let settled = ReviewReceiptIr {
        state: ReceiptState::Committed {
            completed_at: LATER.into(),
            result: json!({"reflection_id": "rfl_1", "cost": {"unit_kind": "reflection", "units": 1.0}}),
        },
        ..pending.clone()
    };
    assert!(repo
        .update_receipt(&mut tx, &settled, "pending", Some("instance-1"))
        .await
        .unwrap());

    // Past expiry another owner still cannot rewrite the committed receipt.
    let forged = ReviewReceiptIr {
        state: ReceiptState::Committed {
            completed_at: LATER.into(),
            result: json!({"reflection_id": "rfl_forged", "cost": {"unit_kind": "reflection", "units": 1.0}}),
        },
        ..pending.clone()
    };
    assert!(!repo
        .update_receipt(&mut tx, &forged, "committed", Some("instance-2"))
        .await
        .unwrap());
    match repo
        .receipt(&mut tx, &SpaceId::new("spc_workshop"), "rcp_gen_done")
        .await
        .unwrap()
        .unwrap()
    {
        ReceiptRead::Retained(receipt) => match receipt.state {
            ReceiptState::Committed { ref result, .. } => {
                assert_eq!(result["reflection_id"], "rfl_1")
            }
            _ => panic!("receipt must stay committed"),
        },
        ReceiptRead::Evicted { .. } => panic!("receipt body must be retained"),
    }
    tx.commit().await.unwrap();
    pool.close().await;
}

/// Insert collision returns `Conflict` without overwriting.
#[tokio::test]
async fn insert_conflict_reports_without_overwrite() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let mut tx = pool.begin().await.unwrap();
    repo.insert_record(&mut tx, &live_record()).await.unwrap();
    let mut different = live_record();
    different.kind = "other_kind".into();
    let outcome = repo.insert_record(&mut tx, &different).await;
    assert!(matches!(
        outcome,
        Err(mindcore_sqlite::SqliteRepositoryError::Conflict(_))
    ));
    // The first row survives the collision.
    let kept = repo
        .record(&mut tx, &SpaceId::new("spc_workshop"), "rec_1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(kept.kind, "local_note");
    tx.commit().await.unwrap();
    pool.close().await;
}

/// Journal intent staging plus provider-meta markers round-trip on the borrowed path.
#[tokio::test]
async fn journal_and_provider_meta_roundtrip() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let mut tx = pool.begin().await.unwrap();
    let intent = mindcore_base::ir::MemoryJournalRow {
        seq: 0,
        receipt_id: "rcp_1".into(),
        space_id: SpaceId::new("spc_workshop"),
        subject_id: SubjectId::new("sub_mira"),
        requester: CallerId::new("prn_mira"),
        operation: "memory.records.create".into(),
        idempotency_key: Some("attempt-1".into()),
        state: "pending".into(),
        targets: vec![json!({"path": "spc_workshop/sub_mira/records/rec_1.md", "digest": DIGEST})],
        result: Some(json!({"record_id": "rec_1"})),
        created_at: TIME.into(),
        updated_at: TIME.into(),
    };
    repo.insert_journal_intent(&mut tx, &intent).await.unwrap();
    let read = repo
        .journal_intent(&mut tx, &SpaceId::new("spc_workshop"), "rcp_1")
        .await
        .unwrap()
        .expect("intent");
    assert_eq!(read.state, "pending");
    assert_eq!(read.targets.len(), 1);

    let mut applied = read.clone();
    applied.state = "applied".into();
    applied.targets = Vec::new();
    applied.updated_at = LATER.into();
    assert!(repo
        .update_journal_intent(&mut tx, &applied, "pending")
        .await
        .unwrap());
    assert!(!repo
        .update_journal_intent(&mut tx, &applied, "pending")
        .await
        .unwrap());

    let marker = mindcore_base::ir::MemoryProviderMetaRow {
        key: "soul_invalidation:spc_workshop:rec_1".into(),
        value: "ctx_a".into(),
        updated_at: TIME.into(),
    };
    repo.put_provider_meta(&mut tx, &marker).await.unwrap();
    let read = repo
        .provider_meta(&mut tx, &SpaceId::new("spc_workshop"), &marker.key)
        .await
        .unwrap()
        .expect("marker");
    assert_eq!(read.value, "ctx_a");
    assert!(repo
        .delete_provider_meta(&mut tx, &SpaceId::new("spc_workshop"), &marker.key)
        .await
        .unwrap());
    tx.commit().await.unwrap();
    pool.close().await;
}

/// `recall` returns live rows scoped to space/subject; a context query admits the
/// context's rows plus nothing else, `None` admits shared and every context.
#[tokio::test]
async fn recall_scopes_live_records() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let mut tx = pool.begin().await.unwrap();
    let mut shared = live_record();
    shared.record_id = "rec_shared".into();
    shared.scope = Placement::Shared;
    repo.insert_record(&mut tx, &shared).await.unwrap();
    repo.insert_record(&mut tx, &live_record()).await.unwrap();
    let mut tombstoned = live_record();
    tombstoned.record_id = "rec_dead".into();
    tombstoned.state = RecordState::Tombstoned {
        deleted_at: LATER.into(),
        deleted_by: "prn_writer".into(),
        reason: None,
    };
    repo.insert_record(&mut tx, &tombstoned).await.unwrap();

    let all = repo
        .recall(
            &mut tx,
            &mindcore_base::memory::RecallQuery {
                space: SpaceId::new("spc_workshop"),
                subject: SubjectId::new("sub_mira"),
                context: None,
            },
        )
        .await
        .unwrap();
    let mut ids: Vec<&str> = all.iter().map(|record| record.record_id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, ["rec_1", "rec_shared"]);

    let scoped = repo
        .recall(
            &mut tx,
            &mindcore_base::memory::RecallQuery {
                space: SpaceId::new("spc_workshop"),
                subject: SubjectId::new("sub_mira"),
                context: Some(ContextId::new("ctx_a")),
            },
        )
        .await
        .unwrap();
    assert_eq!(scoped.len(), 1);
    assert_eq!(scoped[0].record_id, "rec_1");
    tx.commit().await.unwrap();
    pool.close().await;
}

/// F1: `list_candidates` applies the adapter-resolved placement scope in SQL before
/// returning — a selection limited to `ctx_a` must not see a `ctx_b` candidate.
#[tokio::test]
async fn candidate_list_filters_by_admitted_placement() {
    use mindcore_base::ir::{CandidateSelection, PlacementSelection};

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let mut tx = pool.begin().await.unwrap();
    let mut ctx_b = pending_candidate();
    ctx_b.candidate_id = "cand_b".into();
    ctx_b.capture_scope = Placement::Context("ctx_b".into());
    ctx_b.created_at = LATER.into();
    let mut shared = pending_candidate();
    shared.candidate_id = "cand_shared".into();
    shared.capture_scope = Placement::Shared;
    shared.created_at = "2026-11-20T09:30:00Z".into();
    repo.insert_candidate(&mut tx, &pending_candidate())
        .await
        .unwrap();
    repo.insert_candidate(&mut tx, &ctx_b).await.unwrap();
    repo.insert_candidate(&mut tx, &shared).await.unwrap();

    let admitted = CandidateSelection {
        placement: PlacementSelection::SelectedContexts {
            context_ids: vec![ContextId::new("ctx_a")],
            include_shared: false,
        },
        status: Some("pending".into()),
    };
    let rows = repo
        .list_candidates(
            &mut tx,
            &SpaceId::new("spc_workshop"),
            "sub_mira",
            &admitted,
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].candidate_id, "cand_1");

    // With shared included the shared row appears; ctx_b stays invisible.
    let admitted = CandidateSelection {
        placement: PlacementSelection::SelectedContexts {
            context_ids: vec![ContextId::new("ctx_a")],
            include_shared: true,
        },
        status: None,
    };
    let rows = repo
        .list_candidates(
            &mut tx,
            &SpaceId::new("spc_workshop"),
            "sub_mira",
            &admitted,
        )
        .await
        .unwrap();
    let mut ids: Vec<&str> = rows.iter().map(|c| c.candidate_id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, ["cand_1", "cand_shared"]);

    // SharedOnly yields exactly the shared row.
    let admitted = CandidateSelection {
        placement: PlacementSelection::SharedOnly,
        status: None,
    };
    let rows = repo
        .list_candidates(
            &mut tx,
            &SpaceId::new("spc_workshop"),
            "sub_mira",
            &admitted,
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].candidate_id, "cand_shared");
    tx.commit().await.unwrap();
    pool.close().await;
}

/// F2: a guarded update whose replacement row carries a different space misses — no
/// mutation, not a false success.
#[tokio::test]
async fn guarded_updates_do_not_cross_spaces() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let mut tx = pool.begin().await.unwrap();
    sqlx::query!(
        "INSERT INTO memory_spaces \
         (space_id, authority_host_id, authority_generation, status, revision, created_at) \
         VALUES ('spc_other', 'host-2', 1, 'active', 1, ?)",
        TIME,
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query!(
        "INSERT INTO memory_subjects \
         (space_id, subject_id, status, revision, created_at) \
         VALUES ('spc_other', 'sub_mira', 'active', 1, ?)",
        TIME,
    )
    .execute(&mut *tx)
    .await
    .unwrap();

    let grant = mindcore_base::ir::MemoryGrantRow {
        grant_id: "gnt_1".into(),
        space_id: SpaceId::new("spc_workshop"),
        subject_id: Some(SubjectId::new("sub_mira")),
        principal_id: CallerId::new("prn_mira"),
        capabilities: vec!["read".into()],
        context_ids: vec![ContextId::new("ctx_a")],
        shared_access: true,
        expires_at: None,
        revoked_at: None,
        revision: Number::from(1),
        created_at: TIME.into(),
        extensions: Map::new(),
    };
    repo.insert_grant(&mut tx, &grant).await.unwrap();

    // A replacement row claiming the wrong space must not mutate the real row.
    let mut forged = grant.clone();
    forged.space_id = SpaceId::new("spc_other");
    forged.revoked_at = Some(LATER.into());
    forged.revision = Number::from(2);
    assert!(!repo
        .update_grant(&mut tx, &forged, &Number::from(1))
        .await
        .unwrap());
    let kept = repo
        .grant(&mut tx, &SpaceId::new("spc_workshop"), "gnt_1")
        .await
        .unwrap()
        .unwrap();
    assert!(kept.revoked_at.is_none());

    // Same scoping for receipts, including the unleased (NULL owner) path.
    repo.insert_receipt(&mut tx, &committed_receipt(), None)
        .await
        .unwrap();
    let mut forged_receipt = committed_receipt();
    forged_receipt.space_id = "spc_other".into();
    forged_receipt.state = ReceiptState::Pending;
    assert!(!repo
        .update_receipt(&mut tx, &forged_receipt, "committed", None)
        .await
        .unwrap());
    match repo
        .receipt(&mut tx, &SpaceId::new("spc_workshop"), "rcp_1")
        .await
        .unwrap()
        .unwrap()
    {
        ReceiptRead::Retained(receipt) => {
            assert!(matches!(receipt.state, ReceiptState::Committed { .. }))
        }
        ReceiptRead::Evicted { .. } => panic!("receipt body must be retained"),
    }
    tx.commit().await.unwrap();
    pool.close().await;
}

/// F4: malformed persisted reflection selections fail `InvalidRow`, never silently
/// nonmatch or widen.
#[tokio::test]
async fn malformed_reflection_selections_fail_invalid_row() {
    use mindcore_base::ir::PlacementSelection;

    // Each malformed shape gets its own record and its own evaluation so every
    // `InvalidRow` cause is asserted independently of row order.
    for selection in [
        json!({"mode": "everywhere"}),
        json!({"mode": "selected_contexts", "context_ids": ["ctx_a", 3], "include_shared": false}),
        json!({"mode": "selected_contexts", "context_ids": ["ctx_a"], "include_shared": 1}),
    ] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settlement.db");
        let repo = SqliteMemoryRepository;
        let pool = prepared(&path).await;

        let mut tx = pool.begin().await.unwrap();
        sqlx::query!(
            "INSERT INTO memory_reflections \
             (seq, reflection_id, space_id, subject_id, requester_principal_id, \
              selection_json, basis_json, engine_policy_version, schema_version, revision, \
              extensions_json, created_at, body_text, body_format, stale_cache) \
             VALUES ((SELECT COALESCE(MAX(seq),0)+1 FROM memory_reflections), \
               'rfl_bad', 'spc_workshop', 'sub_mira', 'prn_mira', ?, '{\"digest\":\"d\"}', \
               'v1', 1, 1, '{}', ?, 'text', 'markdown', 0)",
            selection.to_string(),
            TIME,
        )
        .execute(&mut *tx)
        .await
        .unwrap();

        let outcome = repo
            .list_reflections(
                &mut tx,
                &SpaceId::new("spc_workshop"),
                "sub_mira",
                Some(&PlacementSelection::SubjectAll),
            )
            .await;
        assert!(
            matches!(
                outcome,
                Err(mindcore_sqlite::SqliteRepositoryError::InvalidRow(_))
            ),
            "selection {selection} must fail InvalidRow, got {outcome:?}"
        );
        tx.commit().await.unwrap();
        pool.close().await;
    }
}

/// F5: the replacement row's own revision is stored under the observed-revision guard;
/// a stale observation misses and writes nothing.
#[tokio::test]
async fn update_persists_supplied_revision() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let mut tx = pool.begin().await.unwrap();
    repo.insert_record(&mut tx, &live_record()).await.unwrap();

    // observed=1, supplied revision=7 → stored revision is 7, not 2.
    let mut bumped = live_record();
    bumped.kind = "renamed_kind".into();
    bumped.revision = Number::from(7);
    bumped.updated_at = LATER.into();
    assert!(repo
        .update_record(&mut tx, &bumped, &Number::from(1))
        .await
        .unwrap());
    let stored = repo
        .record(&mut tx, &SpaceId::new("spc_workshop"), "rec_1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.revision, Number::from(7));
    assert_eq!(stored.kind, "renamed_kind");

    // A stale observed revision (1 against stored 7) misses and writes nothing.
    let mut stale = live_record();
    stale.kind = "stale_kind".into();
    stale.revision = Number::from(2);
    assert!(!repo
        .update_record(&mut tx, &stale, &Number::from(1))
        .await
        .unwrap());
    let kept = repo
        .record(&mut tx, &SpaceId::new("spc_workshop"), "rec_1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(kept.kind, "renamed_kind");
    assert_eq!(kept.revision, Number::from(7));
    tx.commit().await.unwrap();
    pool.close().await;
}

/// C-1: a settled receipt whose payload body retention evicted reads as
/// `ReceiptRead::Evicted` with the stored digest and last status on both the keyed
/// dedup lookup and the by-id read — never `InvalidRow`.
#[tokio::test]
async fn evicted_receipt_distinguishes_expiry_from_conflict() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let mut tx = pool.begin().await.unwrap();
    repo.insert_receipt(&mut tx, &committed_receipt(), None)
        .await
        .unwrap();
    // Retention reclaims both payload columns in place (dedup tombstone shape).
    sqlx::query!(
        "UPDATE memory_receipts SET result_json = NULL, error_json = NULL \
         WHERE receipt_id = 'rcp_1'",
    )
    .execute(&mut *tx)
    .await
    .unwrap();

    let key = receipt_key();
    match repo.review_receipt(&mut tx, &key).await.unwrap() {
        Some(ReceiptRead::Evicted {
            requester,
            operation,
            status,
            payload_digest,
        }) => {
            assert_eq!(requester, "prn_mira");
            assert_eq!(operation, "memory.candidates.review");
            assert_eq!(status, "committed");
            assert_eq!(payload_digest, DIGEST);
        }
        other => panic!("expected evicted dedup hit, got {other:?}"),
    }
    match repo
        .receipt(&mut tx, &SpaceId::new("spc_workshop"), "rcp_1")
        .await
        .unwrap()
    {
        Some(ReceiptRead::Evicted {
            status,
            payload_digest,
            ..
        }) => {
            assert_eq!(status, "committed");
            assert_eq!(payload_digest, DIGEST);
        }
        other => panic!("expected evicted receipt row, got {other:?}"),
    }
    tx.rollback().await.unwrap();
    pool.close().await;
}

/// C-2: a consume without `proposed_kind` preserves the stored kind, and a fresh
/// kindless candidate is rejected before it can be persisted unreadably.
#[tokio::test]
async fn consumed_candidate_without_kind_preserves_stored_kind() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let mut tx = pool.begin().await.unwrap();
    repo.insert_candidate(&mut tx, &pending_candidate())
        .await
        .unwrap();
    let mut consumed = pending_candidate();
    consumed.state = CandidateState::Consumed {
        proposed_kind: None,
        consumed_by: Consumption {
            operation: "memory.candidates.dismiss".into(),
            receipt_id: "rcp_1".into(),
            outcome: "dismissed".into(),
            record_ids: vec![],
        },
        content_digest: DIGEST.into(),
    };
    consumed.revision = Number::from(2);
    assert!(repo
        .update_candidate(&mut tx, &consumed, &Number::from(1))
        .await
        .unwrap());
    let read = repo
        .candidate(&mut tx, &SpaceId::new("spc_workshop"), "cand_1")
        .await
        .unwrap()
        .expect("consumed candidate readable");
    assert!(matches!(
        &read.state,
        CandidateState::Consumed {
            proposed_kind: Some(kind),
            ..
        } if kind == "local_note"
    ));

    // The same shape on a fresh insert is rejected instead of written.
    let mut kindless = pending_candidate();
    kindless.candidate_id = "cand_kindless".into();
    kindless.state = consumed.state.clone();
    let outcome = repo.insert_candidate(&mut tx, &kindless).await;
    assert!(matches!(
        outcome,
        Err(mindcore_sqlite::SqliteRepositoryError::InvalidRow(_))
    ));
    assert!(repo
        .candidate(&mut tx, &SpaceId::new("spc_workshop"), "cand_kindless")
        .await
        .unwrap()
        .is_none());
    tx.rollback().await.unwrap();
    pool.close().await;
}

/// C-3: the native writer's tombstone — explicit JSON `null` reason and a
/// surviving import ref — still reads as a tombstoned record.
#[tokio::test]
async fn tombstoned_record_with_null_reason_and_import_ref_reads() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let mut tx = pool.begin().await.unwrap();
    let mut record = live_record();
    record.state = RecordState::Live {
        content: MemoryContent {
            text: "Harbor closes at dusk".into(),
            structured: None,
            cognition: None,
        },
        import_ref: Some(mindcore_base::ir::ImportReference {
            import_id: "imp_1".into(),
            source_record_id: "src_9".into(),
            source_revision: Number::from(3),
        }),
    };
    repo.insert_record(&mut tx, &record).await.unwrap();
    // The pinned delete scrubs content, writes `reason: null` and leaves
    // `import_ref_json` in place.
    sqlx::query!(
        "UPDATE memory_records \
         SET content_json = '{\"text\":\"\"}', tombstone_json = ?, updated_at = ?, \
             revision = revision + 1 \
         WHERE record_id = 'rec_1'",
        json!({"deleted_at": LATER, "deleted_by": "prn_writer", "reason": null}).to_string(),
        LATER,
    )
    .execute(&mut *tx)
    .await
    .unwrap();

    let read = repo
        .record(&mut tx, &SpaceId::new("spc_workshop"), "rec_1")
        .await
        .unwrap()
        .expect("tombstoned record readable");
    assert!(matches!(
        &read.state,
        RecordState::Tombstoned {
            deleted_by,
            reason: None,
            ..
        } if deleted_by == "prn_writer"
    ));
    let listed = repo
        .list_records(
            &mut tx,
            &SpaceId::new("spc_workshop"),
            "sub_mira",
            &mindcore_base::ir::RecordSelection {
                include_tombstoned: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    tx.rollback().await.unwrap();
    pool.close().await;
}

/// C-4: a persisted column that cannot decode (non-UTF-8 bytes in a TEXT column)
/// fails `InvalidRow` on the point read and the list read alike — never panics.
#[tokio::test]
async fn malformed_persisted_column_returns_invalid_row() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let mut tx = pool.begin().await.unwrap();
    repo.insert_record(&mut tx, &live_record()).await.unwrap();
    sqlx::query!("UPDATE memory_records SET content_json = X'80' WHERE record_id = 'rec_1'")
        .execute(&mut *tx)
        .await
        .unwrap();

    let point = repo
        .record(&mut tx, &SpaceId::new("spc_workshop"), "rec_1")
        .await;
    assert!(
        matches!(
            point,
            Err(mindcore_sqlite::SqliteRepositoryError::InvalidRow(_))
        ),
        "undecodable column must fail InvalidRow, got {point:?}"
    );
    let list = repo
        .list_records(
            &mut tx,
            &SpaceId::new("spc_workshop"),
            "sub_mira",
            &Default::default(),
        )
        .await;
    assert!(
        matches!(
            list,
            Err(mindcore_sqlite::SqliteRepositoryError::InvalidRow(_))
        ),
        "undecodable column must fail InvalidRow on the list read too, got {list:?}"
    );
    tx.rollback().await.unwrap();
    pool.close().await;
}

/// C-5: `list_reflections(..., selection: None)` validates the pinned selection
/// on every row, so a malformed persisted selection still fails `InvalidRow`.
#[tokio::test]
async fn malformed_selection_with_none_filter_fails_invalid_row() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let mut tx = pool.begin().await.unwrap();
    sqlx::query!(
        "INSERT INTO memory_reflections \
         (seq, reflection_id, space_id, subject_id, requester_principal_id, \
          selection_json, basis_json, engine_policy_version, schema_version, revision, \
          extensions_json, created_at, body_text, body_format, stale_cache) \
         VALUES ((SELECT COALESCE(MAX(seq),0)+1 FROM memory_reflections), \
           'rfl_bad', 'spc_workshop', 'sub_mira', 'prn_mira', ?, '{\"digest\":\"d\"}', \
           'v1', 1, 1, '{}', ?, 'text', 'markdown', 0)",
        json!({"mode": "everywhere"}).to_string(),
        TIME,
    )
    .execute(&mut *tx)
    .await
    .unwrap();

    let outcome = repo
        .list_reflections(&mut tx, &SpaceId::new("spc_workshop"), "sub_mira", None)
        .await;
    assert!(
        matches!(
            outcome,
            Err(mindcore_sqlite::SqliteRepositoryError::InvalidRow(_))
        ),
        "malformed selection with None filter must fail InvalidRow, got {outcome:?}"
    );
    tx.rollback().await.unwrap();
    pool.close().await;
}
/// QC1-01: the fixture is non-STRICT, so a persisted BLOB of valid UTF-8 JSON
/// must decode identically on the point read and the list read — both compile-
/// checked paths share the macro's unchecked UTF-8 contract. `X'80'` stays
/// `InvalidRow` on both.
#[tokio::test]
async fn utf8_blob_column_decodes_identically_on_point_and_list_reads() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let mut tx = pool.begin().await.unwrap();
    repo.insert_record(&mut tx, &live_record()).await.unwrap();
    sqlx::query!(
        "UPDATE memory_records SET content_json = CAST(? AS BLOB) WHERE record_id = 'rec_1'",
        r#"{"text":"Harbor closes at dusk","structured":{"rank":1}}"#,
    )
    .execute(&mut *tx)
    .await
    .unwrap();

    let point = repo
        .record(&mut tx, &SpaceId::new("spc_workshop"), "rec_1")
        .await;
    let list = repo
        .list_records(
            &mut tx,
            &SpaceId::new("spc_workshop"),
            "sub_mira",
            &Default::default(),
        )
        .await;
    assert_eq!(
        point.is_ok(),
        list.is_ok(),
        "point and list reads must agree on a UTF-8 BLOB: {point:?} vs {list:?}"
    );
    assert_eq!(list.unwrap().len(), 1);
    tx.rollback().await.unwrap();
    pool.close().await;
}

/// S-1: D-1 — non-empty `extensions` on candidates, records and receipts are
/// dropped on write and reconstructed empty on read; a reflection keeps its map
/// as the positive control.
#[tokio::test]
async fn nonempty_extensions_dropped_on_write() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let mut tx = pool.begin().await.unwrap();
    let mut candidate = pending_candidate();
    candidate
        .extensions
        .insert("scratch".into(), json!({"note": "ephemeral"}));
    let mut record = live_record();
    record
        .extensions
        .insert("scratch".into(), json!({"note": "ephemeral"}));
    let mut receipt = committed_receipt();
    receipt
        .extensions
        .insert("scratch".into(), json!({"note": "ephemeral"}));
    repo.insert_candidate(&mut tx, &candidate).await.unwrap();
    repo.insert_review(&mut tx, &record, &receipt)
        .await
        .unwrap();

    let read = repo
        .candidate(&mut tx, &SpaceId::new("spc_workshop"), "cand_1")
        .await
        .unwrap()
        .unwrap();
    assert!(read.extensions.is_empty());
    let read = repo
        .record(&mut tx, &SpaceId::new("spc_workshop"), "rec_1")
        .await
        .unwrap()
        .unwrap();
    assert!(read.extensions.is_empty());
    match repo.review_receipt(&mut tx, &receipt_key()).await.unwrap() {
        Some(ReceiptRead::Retained(receipt)) => assert!(receipt.extensions.is_empty()),
        other => panic!("expected retained receipt, got {other:?}"),
    }

    // Positive control: the only persisted extension map survives intact.
    let mut extensions = Map::new();
    extensions.insert("kept".into(), json!({"model": "sonnet"}));
    let reflection = mindcore_base::ir::MemoryReflectionRow {
        seq: 0,
        reflection_id: "rfl_1".into(),
        space_id: SpaceId::new("spc_workshop"),
        subject_id: SubjectId::new("sub_mira"),
        requester: CallerId::new("prn_mira"),
        selection: json!({"mode": "subject_all"}),
        filter: None,
        cognition_filter: None,
        basis: json!({"digest": "d"}),
        engine_policy_version: "v1".into(),
        model: None,
        schema_version: 1,
        revision: Number::from(1),
        extensions,
        created_at: TIME.into(),
        body_text: "text".into(),
        body_format: "markdown".into(),
        cost: None,
        stale_cache: false,
    };
    repo.insert_reflection(&mut tx, &reflection).await.unwrap();
    let rows = repo
        .list_reflections(&mut tx, &SpaceId::new("spc_workshop"), "sub_mira", None)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].extensions.len(), 1);
    tx.rollback().await.unwrap();
    pool.close().await;
}

/// BUG-01 row path: opaque extension values (number, array, string) inside persisted
/// provenance anchors survive the write/read round trip; a non-namespace key is refused
/// by the reader instead of being silently dropped.
#[tokio::test]
async fn persisted_candidate_extension_values_are_opaque() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;
    let mut tx = pool.begin().await.unwrap();

    let mut candidate = pending_candidate();
    candidate.provenance.anchors = [json!(5), json!([1, "two", null]), json!("text")]
        .into_iter()
        .map(|value| {
            json!({"schema_version": 1, "source_id": "src", "extensions": {"md_note": value}})
        })
        .collect();
    repo.insert_candidate(&mut tx, &candidate).await.unwrap();
    let read = repo
        .candidate(&mut tx, &SpaceId::new("spc_workshop"), "cand_1")
        .await
        .unwrap()
        .expect("staged candidate");
    assert_eq!(read.provenance.anchors, candidate.provenance.anchors);

    let mut bad = pending_candidate();
    bad.candidate_id = "cand_2".into();
    bad.provenance.anchors =
        vec![json!({"schema_version": 1, "source_id": "src", "extensions": {"md.note": 1}})];
    repo.insert_candidate(&mut tx, &bad).await.unwrap();
    assert!(matches!(
        repo.candidate(&mut tx, &SpaceId::new("spc_workshop"), "cand_2")
            .await,
        Err(mindcore_sqlite::SqliteRepositoryError::InvalidRow(_))
    ));
    tx.rollback().await.unwrap();
    pool.close().await;
}

/// I-000053/I-000054: a persisted `selected_contexts` row that omits
/// `include_shared` decodes as `false` — and so hides shared rows — while a
/// present non-boolean still fails `InvalidRow`; a stored literal `null`
/// filter reads as absent rather than poisoning the row.
#[tokio::test]
async fn absent_include_shared_and_null_filter_decode() {
    use mindcore_base::ir::PlacementSelection;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;
    let mut tx = pool.begin().await.unwrap();

    // Legacy row: `include_shared` omitted entirely, `filter_json` holds the
    // literal text 'null'.
    sqlx::query(
        "INSERT INTO memory_reflections \
         (seq, reflection_id, space_id, subject_id, requester_principal_id, \
          selection_json, filter_json, basis_json, engine_policy_version, \
          schema_version, revision, extensions_json, created_at, body_text, \
          body_format, stale_cache) \
         VALUES ((SELECT COALESCE(MAX(seq),0)+1 FROM memory_reflections), \
           'rfl_legacy', 'spc_workshop', 'sub_mira', 'prn_mira', ?, 'null', \
           '{\"digest\":\"d\"}', 'v1', 1, 1, '{}', ?, 'text', 'markdown', 0)",
    )
    .bind("{\"mode\":\"selected_contexts\",\"context_ids\":[\"ctx_a\"]}")
    .bind(TIME)
    .execute(&mut *tx)
    .await
    .unwrap();

    // The omitted flag defaults to false: a requested selection that covers
    // the pinned context does not widen into shared visibility.
    let rows = repo
        .list_reflections(
            &mut tx,
            &SpaceId::new("spc_workshop"),
            "sub_mira",
            Some(&PlacementSelection::SelectedContexts {
                context_ids: vec![ContextId::new("ctx_a")],
                include_shared: true,
            }),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].reflection_id, "rfl_legacy");
    assert_eq!(rows[0].filter, None);
    // A request pinning the same context with `include_shared: false` still
    // covers the row: the stored flag decodes as `false` (had it decoded as
    // `true`, this request would see nothing — unlike `SharedOnly`, which
    // rejects a pinned selected-contexts row either way).
    let covered = repo
        .list_reflections(
            &mut tx,
            &SpaceId::new("spc_workshop"),
            "sub_mira",
            Some(&PlacementSelection::SelectedContexts {
                context_ids: vec![ContextId::new("ctx_a")],
                include_shared: false,
            }),
        )
        .await
        .unwrap();
    assert_eq!(covered.len(), 1);

    // A present non-boolean `include_shared` still fails strict decode.
    sqlx::query(
        "INSERT INTO memory_reflections \
         (seq, reflection_id, space_id, subject_id, requester_principal_id, \
          selection_json, basis_json, engine_policy_version, schema_version, \
          revision, extensions_json, created_at, body_text, body_format, \
          stale_cache) \
         VALUES ((SELECT COALESCE(MAX(seq),0)+1 FROM memory_reflections), \
           'rfl_bad', 'spc_workshop', 'sub_mira', 'prn_mira', ?, \
           '{\"digest\":\"d\"}', 'v1', 1, 1, '{}', ?, 'text', 'markdown', 0)",
    )
    .bind("{\"mode\":\"selected_contexts\",\"context_ids\":[\"ctx_a\"],\"include_shared\":\"yes\"}")
    .bind(TIME)
    .execute(&mut *tx)
    .await
    .unwrap();
    assert!(matches!(
        repo.list_reflections(&mut tx, &SpaceId::new("spc_workshop"), "sub_mira", None)
            .await,
        Err(mindcore_sqlite::SqliteRepositoryError::InvalidRow(_))
    ));
    tx.rollback().await.unwrap();
    pool.close().await;
}

/// `list_unsettled_intents` returns exactly the non-`applied` intents of the subject in
/// the space — `pending`, `deferred` and `quarantined` — in `created_at` order; `applied`
/// rows and intents of another subject or space stay out.
#[tokio::test]
async fn list_unsettled_intents_covers_non_applied_states() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("settlement.db");
    let repo = SqliteMemoryRepository;
    let pool = prepared(&path).await;

    let intent = |receipt_id: &str, space: &str, subject: &str, state: &str, created: &str| {
        MemoryJournalRow {
            seq: 0,
            receipt_id: receipt_id.into(),
            space_id: SpaceId::new(space),
            subject_id: SubjectId::new(subject),
            requester: CallerId::new("prn_mira"),
            operation: "memory.records.create".into(),
            idempotency_key: Some(receipt_id.into()),
            state: state.into(),
            targets: vec![
                json!({"path": "spc_workshop/sub_mira/records/rec_1.md", "digest": DIGEST}),
            ],
            result: None,
            created_at: created.into(),
            updated_at: created.into(),
        }
    };

    let mut tx = pool.begin().await.unwrap();
    let rows = [
        intent(
            "rcp_pending",
            "spc_workshop",
            "sub_mira",
            "pending",
            "2026-11-20T09:21:00.000Z",
        ),
        intent(
            "rcp_applied",
            "spc_workshop",
            "sub_mira",
            "applied",
            "2026-11-20T09:21:30.000Z",
        ),
        intent(
            "rcp_deferred",
            "spc_workshop",
            "sub_mira",
            "deferred",
            "2026-11-20T09:22:00.000Z",
        ),
        intent(
            "rcp_quarantined",
            "spc_workshop",
            "sub_mira",
            "quarantined",
            "2026-11-20T09:23:00.000Z",
        ),
        intent(
            "rcp_other_subj",
            "spc_workshop",
            "sub_ava",
            "pending",
            "2026-11-20T09:21:00.000Z",
        ),
        intent(
            "rcp_other_space",
            "spc_attic",
            "sub_mira",
            "pending",
            "2026-11-20T09:21:00.000Z",
        ),
    ];
    for row in &rows {
        repo.insert_journal_intent(&mut tx, row).await.unwrap();
    }

    let unsettled = repo
        .list_unsettled_intents(
            &mut tx,
            &SpaceId::new("spc_workshop"),
            &SubjectId::new("sub_mira"),
        )
        .await
        .unwrap();
    let receipt_ids: Vec<&str> = unsettled
        .iter()
        .map(|row| row.receipt_id.as_str())
        .collect();
    assert_eq!(
        receipt_ids,
        ["rcp_pending", "rcp_deferred", "rcp_quarantined"]
    );
    assert_eq!(
        unsettled
            .iter()
            .map(|row| row.state.as_str())
            .collect::<Vec<_>>(),
        ["pending", "deferred", "quarantined"]
    );

    // A subject with no unsettled intents reads empty rather than erroring.
    assert!(repo
        .list_unsettled_intents(
            &mut tx,
            &SpaceId::new("spc_workshop"),
            &SubjectId::new("sub_ava_new")
        )
        .await
        .unwrap()
        .is_empty());
    tx.rollback().await.unwrap();
    pool.close().await;
}
