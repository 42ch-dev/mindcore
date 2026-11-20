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
    CandidateState, Classification, Consumption, MemoryCandidateIr, MemoryContent, MemoryRecordIr,
    MemoryRepository, Placement, Provenance, ReceiptRead, ReceiptState, RecordState,
    ReviewReceiptIr,
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
    sqlx::query(
        "INSERT INTO memory_spaces \
         (space_id, display_name, authority_host_id, authority_generation, status, revision, \
          created_at) \
         VALUES ('spc_workshop', 'Workshop', 'host-1', 1, 'active', 1, ?)",
    )
    .bind(TIME)
    .execute(&pool)
    .await
    .expect("seed space");
    sqlx::query(
        "INSERT INTO memory_subjects \
         (space_id, subject_id, status, revision, created_at) \
         VALUES ('spc_workshop', 'sub_mira', 'active', 1, ?)",
    )
    .bind(TIME)
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
    let receipt = repo
        .review_receipt(&mut tx, &key)
        .await
        .unwrap()
        .expect("receipt persisted after reopen");
    assert_eq!(receipt.operation, "memory.candidates.review");
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

/// Leased pending receipt staged, then settled by the same owner; an owner mismatch fails.
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
    let lease = mindcore_base::ir::ReceiptLease {
        owner_id: "instance-1".into(),
        expires_at: "2026-11-20T10:00:00Z".into(),
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
    // Wrong owner cannot settle.
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
