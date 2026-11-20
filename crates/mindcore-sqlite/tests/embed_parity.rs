//! settle-parity Task 1 — Mindcore-side parity harness on the borrowed
//! transaction seam.
//!
//! Every selector runs through `mindcore_spoke::MemoryAdapter` +
//! `SqliteMemoryRepository` on a caller-held `&mut sqlx::Transaction`. The
//! fixture pool belongs to the test; the adapter only stages rows inside the
//! transaction it is handed, so the caller's commit is what makes an effect,
//! its receipt and a journaled intent durable — and the caller's rollback
//! erases all three. No `MindStore` and no adapter-owned transaction enters
//! the parity path (D11).
//!
//! The selector list is the v02 baseline's full adapter surface: the 33 named
//! `MemoryAdapter` operations (5 of the 38 wire ops are the transfer family,
//! exercised here as `CAPABILITY_PORT_MISSING` refusals; `settle_journal` /
//! `complete_reflection` are caller-phase methods, not selectors).

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use spoke_operations::{SpokeRejectCode, SpokeResult};
use spoke_schemas::Principal;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{AssertSqlSafe, SqlitePool, Transaction};
use tempfile::TempDir;

use mindcore_spoke::{MemoryAdapter, MemoryStage};

const DDL: &str = include_str!("fixtures/settlement-prepare.sql");
const SPACE: &str = "spc_parity";
const SUBJECT: &str = "sbj_mira";
const PRINCIPAL: &str = "ctr_admin";
const CTX_A: &str = "ctx-a";

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

/// Caller harness: a pool the test owns, the Nexus-shaped memory schema the
/// borrowed-transaction repository targets, and the space/subject/grant rows
/// the selector calls need — seeded by the caller, never by the adapter.
struct Harness {
    _fixture: Fixture,
    db: PathBuf,
    pool: SqlitePool,
}

impl Harness {
    async fn open() -> Self {
        let fixture = Fixture::fresh();
        let db = fixture.db_path();
        let pool = Self::connect(&db).await;
        sqlx::raw_sql(DDL).execute(&pool).await.expect("apply DDL");
        let harness = Self {
            _fixture: fixture,
            db,
            pool,
        };
        sqlx::query(
            "INSERT INTO memory_spaces \
             (space_id, display_name, authority_host_id, authority_generation, status, revision, \
              created_at) \
             VALUES ('spc_parity', 'parity', 'host_1', 1, 'active', 1, '2026-11-20T09:20:01.123Z')",
        )
        .execute(&harness.pool)
        .await
        .expect("seed space");
        sqlx::query(
            "INSERT INTO memory_subjects \
             (space_id, subject_id, kind_ns, kind_id, status, fenced_contexts_json, revision, \
              created_at) \
             VALUES ('spc_parity', 'sbj_mira', 'nexus', 'character', 'active', '[]', 1, \
              '2026-11-20T09:20:01.123Z')",
        )
        .execute(&harness.pool)
        .await
        .expect("seed subject");
        // Two seeded grants: a space-wide admin grant (no subject_id) for the
        // control-plane ops, plus a subject-scoped grant carrying the whole
        // subject-side capability set for content/profile selectors.
        sqlx::query(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
              shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES ('grt_admin', 'spc_parity', NULL, 'ctr_admin', '[\"admin\"]', \
              '[]', 1, NULL, NULL, 1, '2026-11-20T09:20:01.123Z')",
        )
        .execute(&harness.pool)
        .await
        .expect("seed admin grant");
        sqlx::query(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
              shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES ('grt_mira', 'spc_parity', 'sbj_mira', 'ctr_admin', \
              '[\"admin\",\"capture\",\"review\",\"promote\",\"records_read\",\"records_write\",\
                \"recall\",\"reflect\",\"profile_write\"]', \
              '[]', 1, NULL, NULL, 1, '2026-11-20T09:20:01.123Z')",
        )
        .execute(&harness.pool)
        .await
        .expect("seed subject grant");
        harness
    }

    async fn connect(db: &Path) -> SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(db)
                    .create_if_missing(true)
                    .foreign_keys(false),
            )
            .await
            .expect("open pool")
    }

    /// Drop the current pool and attach a fresh one to the same fixture file
    /// — the close/reopen boundary durable state must survive.
    async fn reopen(&mut self) {
        self.pool.close().await;
        self.pool = Self::connect(&self.db).await;
    }
    async fn begin(&self) -> Transaction<'_, sqlx::Sqlite> {
        self.pool.begin().await.expect("begin caller transaction")
    }

    async fn scalar<T>(&self, sql: &str) -> T
    where
        T: for<'r> sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite> + Send + Unpin,
    {
        sqlx::query_scalar::<_, T>(AssertSqlSafe(sql.to_string()))
            .fetch_one(&self.pool)
            .await
            .expect("scalar")
    }

    async fn count(&self, table: &str) -> i64 {
        self.scalar::<i64>(&format!("SELECT COUNT(*) FROM {table}"))
            .await
    }
}

fn principal(id: &str) -> Principal {
    serde_json::from_value(json!({
        "principal_id": id,
        "extensions": {},
    }))
    .expect("principal")
}

fn ready(result: SpokeResult<MemoryStage>, op: &str) -> Value {
    match result {
        SpokeResult::Ok(MemoryStage::Ready(body)) => body,
        other => panic!("{op} did not return a ready stage: {other:?}"),
    }
}

/// A staged success — the body is `Ready` unless the op is journaled, in
/// which case it names the pending receipt either way.
fn staged(result: SpokeResult<MemoryStage>, op: &str) -> Option<String> {
    match result {
        SpokeResult::Ok(MemoryStage::Ready(_)) => None,
        SpokeResult::Ok(MemoryStage::JournalPending { receipt_id, .. }) => Some(receipt_id),
        other => panic!("{op} did not stage cleanly: {other:?}"),
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
    let foreign = fixture.root.join("foreign-target.db");
    let link = fixture.db_path();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&foreign, &link).expect("symlink");
    #[cfg(windows)]
    std::os::windows::fs::symlink_file(&foreign, &link).expect("symlink");
    assert!(!link.exists());
    assert!(Fixture::guard(&link).is_err());
}

/// The 33 named adapter operations, each driven by the caller's own
/// `&mut Transaction` — the full v02 selector surface on the embedded arm.
/// Reads must reach a `Ready` stage; writes may stage `Ready` or
/// `JournalPending`; the transfer family refuses `CAPABILITY_PORT_MISSING`.
/// One caller transaction carries every selector and commits at the end.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn every_selector_takes_the_caller_transaction() {
    let harness = Harness::open().await;
    let adapter = MemoryAdapter::new(true);
    let admin = principal(PRINCIPAL);
    let mut tx = harness.begin().await;

    // C0 describe — no request members needed.
    let body = ready(
        adapter.describe(&mut tx, &admin, json!({})).await,
        "describe",
    );
    assert_eq!(
        body["capabilities"],
        json!(["spoke-memory", "spoke-memory-reflection"])
    );

    // C1 list spaces (empty object is the list-everything request).
    let body = ready(
        adapter.list_spaces(&mut tx, &admin, json!({})).await,
        "spaces.list",
    );
    assert_eq!(body["spaces"][0]["space_id"], SPACE);

    // C2 freeze: expected_revision = 1 -> revision 2.
    let body = ready(
        adapter
            .freeze_space(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "expected_revision": 1,
                    "idempotency_key": "par-freeze",
                }),
            )
            .await,
        "spaces.freeze",
    );
    assert_eq!(body["result"]["status"], "frozen");

    // C3 unfreeze back to active revision 3 so later ops are not gated.
    let body = ready(
        adapter
            .unfreeze_space(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "expected_revision": 2,
                    "idempotency_key": "par-thaw",
                }),
            )
            .await,
        "spaces.unfreeze",
    );
    assert_eq!(body["result"]["status"], "active");

    // C4 ensure a second subject (created=true) next to the seeded one.
    let body = ready(
        adapter
            .ensure_subject(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": "sbj_ensured",
                    "kind": { "ns": "nexus", "id": "character-other" },
                }),
            )
            .await,
        "subjects.ensure",
    );
    assert_eq!(body["result"]["created"], true);

    // C5 get the seeded subject.
    let body = ready(
        adapter
            .get_subject(
                &mut tx,
                &admin,
                json!({ "space_id": SPACE, "subject_id": SUBJECT }),
            )
            .await,
        "subjects.get",
    );
    assert_eq!(body["subject"]["subject_id"], SUBJECT);

    // C6 fence ctx-a on the subject (revision 1 -> 2).
    let body = ready(
        adapter
            .fence_subject(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "action": "fence",
                    "target": { "type": "context", "context_id": CTX_A },
                    "expected_revision": 1,
                    "idempotency_key": "par-fence",
                }),
            )
            .await,
        "subjects.fence",
    );
    assert_eq!(body["result"]["fenced_contexts"], json!([CTX_A]));

    // C7 grant a review-only coverage for a second principal.
    let body = ready(
        adapter
            .grant(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "principal_id": "ctr_reviewer",
                    "capabilities": ["review"],
                    "context_ids": [CTX_A],
                    "shared_access": false,
                    "idempotency_key": "par-grant",
                }),
            )
            .await,
        "grants.grant",
    );
    let grant_id = body["result"]["grant_id"]
        .as_str()
        .expect("grant id")
        .to_string();

    // C8 list grants for the space.
    let body = ready(
        adapter
            .list_grants(&mut tx, &admin, json!({ "space_id": SPACE }))
            .await,
        "grants.list",
    );
    assert_eq!(body["grants"].as_array().expect("grants").len(), 3);

    // C9 revoke the grant at revision 1.
    let body = ready(
        adapter
            .revoke_grant(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "grant_id": grant_id,
                    "expected_revision": 1,
                    "idempotency_key": "par-revoke",
                }),
            )
            .await,
        "grants.revoke",
    );
    assert_eq!(body["result"]["revoked"], true);

    // D1 capture a candidate (Ready stage, receipt staged in the caller tx).
    let body = ready(
        adapter
            .capture(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "capture_scope": { "visibility": "shared" },
                    "content": { "text": "the northern gate opens before dawn" },
                    "idempotency_key": "par-cap-a",
                }),
            )
            .await,
        "capture",
    );
    let cand_a = body["result"]["candidate_ids"][0]
        .as_str()
        .expect("candidate id")
        .to_string();

    // A second pending candidate for the dismiss selector.
    let body = ready(
        adapter
            .capture(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "capture_scope": { "visibility": "shared" },
                    "content": { "text": "a second candidate body" },
                    "idempotency_key": "par-cap-b",
                }),
            )
            .await,
        "capture-b",
    );
    let cand_b = body["result"]["candidate_ids"][0]
        .as_str()
        .expect("candidate id")
        .to_string();

    // D2 list pending candidates.
    let body = ready(
        adapter
            .list_candidates(
                &mut tx,
                &admin,
                json!({ "space_id": SPACE, "subject_id": SUBJECT }),
            )
            .await,
        "candidates.list",
    );
    assert_eq!(body["candidates"].as_array().expect("candidates").len(), 2);

    // D3 get the first candidate.
    let body = ready(
        adapter
            .get_candidate(
                &mut tx,
                &admin,
                json!({ "space_id": SPACE, "candidate_id": cand_a }),
            )
            .await,
        "candidates.get",
    );
    assert_eq!(body["candidate"]["status"], "pending");

    // D4 review cand_a to promote — a journaled effect: pending receipt,
    // intent and file targets are staged, not committed.
    let review_receipt = staged(
        adapter
            .review_candidate(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "candidate_id": cand_a,
                    "expected_revision": 1,
                    "decision": { "action": "promote" },
                    "idempotency_key": "par-review",
                }),
            )
            .await,
        "candidates.review",
    )
    .expect("review is journaled");

    // D5 dismiss cand_b — also journaled.
    staged(
        adapter
            .dismiss_candidate(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "candidate_id": cand_b,
                    "expected_revision": 1,
                    "reason": "duplicate",
                    "idempotency_key": "par-dismiss",
                }),
            )
            .await,
        "candidates.dismiss",
    );

    // The review's promoted record, visible inside the caller transaction.
    let ra: String = sqlx::query_scalar("SELECT record_id FROM memory_records")
        .fetch_one(&mut *tx)
        .await
        .expect("promoted record staged");

    // D6 create an authored record — journaled.
    let create_receipt = staged(
        adapter
            .create_record(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "kind": "fact",
                    "scope": { "visibility": "shared" },
                    "content": { "text": "an authored record body" },
                    "idempotency_key": "par-create",
                }),
            )
            .await,
        "records.create",
    )
    .expect("create is journaled");
    let rn: String =
        sqlx::query_scalar("SELECT record_id FROM memory_records WHERE record_id != ?1")
            .bind(&ra)
            .fetch_one(&mut *tx)
            .await
            .expect("created record staged");

    // D7 get the promoted record.
    let body = ready(
        adapter
            .get_record(
                &mut tx,
                &admin,
                json!({ "space_id": SPACE, "record_id": ra }),
            )
            .await,
        "records.get",
    );
    assert_eq!(body["record"]["record_id"], ra);

    // D8 list the subject's records (the promoted row and the created row).
    let body = ready(
        adapter
            .list_records(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" },
                }),
            )
            .await,
        "records.list",
    );
    assert_eq!(body["records"].as_array().expect("records").len(), 2);

    // D9 update the created record at revision 1.
    let update_receipt = staged(
        adapter
            .update_record(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "record_id": rn,
                    "expected_revision": 1,
                    "content": { "text": "the edited record body" },
                    "idempotency_key": "par-update",
                }),
            )
            .await,
        "records.update",
    );
    assert!(update_receipt.is_some(), "record update is journaled");

    // D10 promote moves the record into context scope ctx-b — the baseline's
    // scope-change case: the row's scope and revision update inside the
    // caller's transaction and the change is journaled (pending receipt +
    // intent staged, not committed).
    let promote_receipt = staged(
        adapter
            .promote_record(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "record_id": rn,
                    "expected_revision": 2,
                    "target_scope": { "visibility": "context", "context_id": "ctx-b" },
                    "idempotency_key": "par-promote",
                }),
            )
            .await,
        "records.promote",
    );
    assert!(
        promote_receipt.is_some(),
        "scope-change promote is journaled"
    );
    let (scope_visibility, scope_context_id, promote_rev): (String, Option<String>, i64) =
        sqlx::query_as(
            "SELECT scope_visibility, scope_context_id, revision \
             FROM memory_records WHERE record_id = ?1",
        )
        .bind(&rn)
        .fetch_one(&mut *tx)
        .await
        .expect("promoted row");
    assert_eq!(scope_visibility, "context");
    assert_eq!(scope_context_id.as_deref(), Some("ctx-b"));
    assert_eq!(promote_rev, 3);

    // D11 delete at the live revision (update bumped to 2, promote to 3).
    staged(
        adapter
            .delete_record(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "record_id": rn,
                    "expected_revision": 3,
                    "idempotency_key": "par-delete",
                }),
            )
            .await,
        "records.delete",
    );

    // D12 recall the subject's remaining rows.
    ready(
        adapter
            .recall(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" },
                    "budget": { "max_items": 10 },
                }),
            )
            .await,
        "recall",
    );

    // D13 update the personality profile — journaled.
    staged(
        adapter
            .update_profile(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "profile_kind": "personality",
                    "expected_revision": null,
                    "document": { "format": "markdown", "body": "# Parity subject" },
                    "idempotency_key": "par-profile",
                }),
            )
            .await,
        "profile.update",
    );

    // D14 read the profile back.
    let body = ready(
        adapter
            .get_profile(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "profile_kinds": ["personality"],
                }),
            )
            .await,
        "profile.get",
    );
    assert_eq!(body["profiles"].as_array().expect("profiles").len(), 1);

    // D15 poll the capture receipt by id.
    let cap_receipt: String =
        sqlx::query_scalar("SELECT receipt_id FROM memory_receipts WHERE op = 'memory.capture'")
            .fetch_one(&mut *tx)
            .await
            .expect("capture receipt staged");
    let body = ready(
        adapter
            .get_receipt(
                &mut tx,
                &admin,
                json!({ "space_id": SPACE, "receipt_id": cap_receipt }),
            )
            .await,
        "receipts.get",
    );
    assert_eq!(body["receipt"]["receipt_id"], cap_receipt);

    // F1 reflection state — a read; the digest feeds F2.
    let body = ready(
        adapter
            .get_reflection_state(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" },
                }),
            )
            .await,
        "reflections.state",
    );
    let digest = body["state_digest"]
        .as_str()
        .expect("state digest")
        .to_string();

    // F2 generate — this fixture is complete (personality profile + live
    // records + matching state digest), so the expected path is a leased
    // ReflectionPending: a pending receipt carrying the lease is staged
    // inside the caller's transaction for transaction-2 completion.
    let generate = adapter
        .generate_reflection(
            &mut tx,
            &admin,
            json!({
                "space_id": SPACE,
                "subject_id": SUBJECT,
                "selection": { "mode": "subject_all" },
                "expected_state_digest": digest,
                "idempotency_key": "par-generate",
            }),
        )
        .await;
    let work = match generate {
        SpokeResult::Ok(MemoryStage::ReflectionPending(work)) => work,
        other => panic!("generate did not stage a reflection lease: {other:?}"),
    };
    let lease_status: String =
        sqlx::query_scalar("SELECT status FROM memory_receipts WHERE receipt_id = ?1")
            .bind(&work.receipt_id)
            .fetch_one(&mut *tx)
            .await
            .expect("leased receipt staged");
    assert_eq!(lease_status, "pending");

    // F3 list reflections.
    ready(
        adapter
            .list_reflections(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" },
                }),
            )
            .await,
        "reflections.list",
    );

    // O2 transfer family: all five refuse CAPABILITY_PORT_MISSING on the
    // supplied transaction.
    let transfer = [
        adapter
            .create_export(&mut tx, &admin, json!({ "space_id": SPACE }))
            .await,
        adapter
            .get_export(&mut tx, &admin, json!({ "space_id": SPACE }))
            .await,
        adapter
            .stage_import(&mut tx, &admin, json!({ "space_id": SPACE }))
            .await,
        adapter
            .activate_import(&mut tx, &admin, json!({ "space_id": SPACE }))
            .await,
        adapter
            .discard_import(&mut tx, &admin, json!({ "space_id": SPACE }))
            .await,
    ];
    // Compare refusal code, retry class and details.op against the O2 contract.
    let transfer_ops = [
        "memory.exports.create",
        "memory.exports.get",
        "memory.imports.stage",
        "memory.imports.activate",
        "memory.imports.discard",
    ];
    for (result, op) in transfer.into_iter().zip(transfer_ops) {
        match result {
            SpokeResult::Reject(reject) => {
                assert_eq!(reject.code, SpokeRejectCode::CapabilityPortMissing, "{op}");
                let details = reject.details.as_ref().expect("refusal details");
                assert_eq!(details["retry_class"], "terminal", "{op}");
                assert_eq!(details["op"], op, "{op}");
            }
            SpokeResult::Ok(_) => panic!("{op} must refuse"),
        }
    }

    // Everything above sits in this one transaction; commit and confirm.
    tx.commit().await.expect("caller commit");
    assert_eq!(harness.count("memory_candidates").await, 2);
    assert_eq!(harness.count("memory_records").await, 2);
    assert_eq!(harness.count("memory_profiles").await, 1);
    // Staged receipts exist for every mutating selector; staged journal
    // intents exist for the journaled ones (review, dismiss, create, update,
    // delete, profile update).
    assert!(harness.count("memory_receipts").await >= 8);
    assert!(harness.count("memory_journal").await >= 5);
    let _ = review_receipt;
    let _ = create_receipt;
}

/// D11: the caller's commit is what makes the staged effect, its receipt and
/// a journaled effect's pending intent durable — the adapter commits nothing.
#[tokio::test]
async fn caller_commit_persists_effect_receipt_and_intent() {
    let harness = Harness::open().await;
    let adapter = MemoryAdapter::new(true);
    let admin = principal(PRINCIPAL);

    // Capture: Ready stage, but the candidate + receipt live only in the tx.
    let mut tx = harness.begin().await;
    let body = ready(
        adapter
            .capture(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "capture_scope": { "visibility": "shared" },
                    "content": { "text": "the northern gate opens before dawn" },
                    "idempotency_key": "commit-cap",
                }),
            )
            .await,
        "capture",
    );
    let candidate_id = body["result"]["candidate_ids"][0]
        .as_str()
        .expect("candidate id")
        .to_string();
    tx.commit().await.expect("caller commit");

    // A journaled effect: record + pending receipt + pending intent, all
    // durable only after the caller commits.
    let mut tx = harness.begin().await;
    let receipt_id = staged(
        adapter
            .create_record(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "kind": "fact",
                    "scope": { "visibility": "shared" },
                    "content": { "text": "a journaled record body" },
                    "idempotency_key": "commit-create",
                }),
            )
            .await,
        "records.create",
    )
    .expect("create is journaled");
    tx.commit().await.expect("caller commit");

    let persisted_candidate: Option<String> =
        sqlx::query_scalar("SELECT candidate_id FROM memory_candidates WHERE candidate_id = ?1")
            .bind(&candidate_id)
            .fetch_optional(&harness.pool)
            .await
            .expect("candidate lookup");
    assert_eq!(persisted_candidate.as_deref(), Some(candidate_id.as_str()));
    // (a) the journaled operation's record row persisted.
    let record_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM memory_records")
        .fetch_one(&harness.pool)
        .await
        .expect("record count");
    assert_eq!(record_count, 1, "the journaled record must persist");
    // (b) capture's own committed receipt persisted beside its effect.
    let (cap_status, cap_receipt_id): (String, String) = sqlx::query_as(
        "SELECT status, receipt_id FROM memory_receipts WHERE op = 'memory.capture'",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("capture receipt");
    assert_eq!(cap_status, "committed");
    assert!(cap_receipt_id.starts_with("rct_"), "{cap_receipt_id}");
    // (c) the journaled effect's pending receipt + pending intent persisted.
    let (status, journal_state): (String, String) = sqlx::query_as(
        "SELECT r.status, j.state \
         FROM memory_receipts r JOIN memory_journal j ON j.receipt_id = r.receipt_id \
         WHERE r.receipt_id = ?1",
    )
    .bind(&receipt_id)
    .fetch_one(&harness.pool)
    .await
    .expect("receipt + intent");
    assert_eq!(status, "pending");
    assert_eq!(journal_state, "pending");
}

/// D11: a pre-commit rollback erases everything the adapter staged — no
/// candidate, record, receipt or intent survives because none was committed.
#[tokio::test]
async fn caller_rollback_persists_nothing() {
    let harness = Harness::open().await;
    let adapter = MemoryAdapter::new(true);
    let admin = principal(PRINCIPAL);
    let mut tx = harness.begin().await;
    ready(
        adapter
            .capture(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "capture_scope": { "visibility": "shared" },
                    "content": { "text": "an uncommitted capture" },
                    "idempotency_key": "rollback-cap",
                }),
            )
            .await,
        "capture",
    );
    staged(
        adapter
            .create_record(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "kind": "fact",
                    "scope": { "visibility": "shared" },
                    "content": { "text": "an uncommitted record" },
                    "idempotency_key": "rollback-create",
                }),
            )
            .await,
        "records.create",
    );
    tx.rollback().await.expect("caller rollback");

    for table in [
        "memory_candidates",
        "memory_records",
        "memory_receipts",
        "memory_journal",
    ] {
        assert_eq!(harness.count(table).await, 0, "{table} must stay empty");
    }
}

/// Baseline §5 Y04: the second-grant receipt controls. A retained receipt
/// stays readable by its requester while any live grant in the space stands —
/// including one confined to an unrelated context — while an unparticipating
/// caller and the space admin split as NOT_AUTHORIZED vs success. A replay
/// under a revoked-context grant refuses before the dedup probe can serve it.
#[tokio::test]
async fn receipt_y04_second_grant_selector() {
    let mut harness = Harness::open().await;
    let adapter = MemoryAdapter::new(true);
    let admin = principal(PRINCIPAL);
    let p = principal("ctr_p");
    let q = principal("ctr_q");
    let mut tx = harness.begin().await;

    // Y04.0: O grants P capture on ctx-a and ctx-b, Q capture on ctx-b only.
    let mut grant_ids = Vec::new();
    for (key, context) in [("y04-grant-a", "ctx-a"), ("y04-grant-b", "ctx-b")] {
        let body = ready(
            adapter
                .grant(
                    &mut tx,
                    &admin,
                    json!({
                        "space_id": SPACE,
                        "subject_id": SUBJECT,
                        "principal_id": "ctr_p",
                        "capabilities": ["capture"],
                        "context_ids": [context],
                        "shared_access": false,
                        "idempotency_key": key,
                    }),
                )
                .await,
            "grants.grant",
        );
        grant_ids.push(
            body["result"]["grant_id"]
                .as_str()
                .expect("grant id")
                .to_string(),
        );
    }
    let [grant_a, grant_b] = grant_ids.as_slice() else {
        unreachable!()
    };
    ready(
        adapter
            .grant(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "principal_id": "ctr_q",
                    "capabilities": ["capture"],
                    "context_ids": ["ctx-b"],
                    "shared_access": false,
                    "idempotency_key": "y04-grant-q",
                }),
            )
            .await,
        "grants.grant",
    );

    // Y04.0 continued: P captures under ctx-a; the committed receipt carries
    // the result but never the captured text.
    let cap_request = json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "capture_scope": { "visibility": "context", "context_id": "ctx-a" },
        "content": { "text": "the northern gate opens before dawn" },
        "idempotency_key": "y04-capture-a",
    });
    let body = ready(
        adapter.capture(&mut tx, &p, cap_request.clone()).await,
        "capture",
    );
    let receipt_id = body["receipt_id"].as_str().expect("receipt id").to_string();
    assert_eq!(body["status"], "committed");
    assert_eq!(body["result"]["classified"], 1);
    let body_json = body.to_string();
    assert!(
        !body_json.contains("northern gate"),
        "receipt envelope must be content-free: {body_json}"
    );

    // Y04.1: revoking only the ctx-a grant leaves P a live participant via
    // ctx-b; polling the receipt still returns the same retained body.
    let body = ready(
        adapter
            .revoke_grant(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "grant_id": grant_a,
                    "expected_revision": 1,
                    "idempotency_key": "y04-revoke-a",
                }),
            )
            .await,
        "grants.revoke",
    );
    assert_eq!(body["result"]["revoked"], true);
    let body = ready(
        adapter
            .get_receipt(
                &mut tx,
                &p,
                json!({ "space_id": SPACE, "receipt_id": receipt_id }),
            )
            .await,
        "receipts.get",
    );
    let retained = &body["receipt"];
    assert_eq!(retained["status"], "committed");
    assert_eq!(retained["result"]["classified"], 1);
    assert!(
        !body.to_string().contains("northern gate"),
        "poll must not disclose captured content: {body}"
    );

    // Y04.2: P replays the identical capture while only ctx-b is live — the
    // placement requirement binds ctx-a, so authorization refuses before the
    // dedup probe can serve the replay.
    match adapter.capture(&mut tx, &p, cap_request).await {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
            assert_eq!(
                reject.details.as_ref().expect("details")["retry_class"],
                "terminal"
            );
        }
        other => panic!("revoked-context replay must refuse: {other:?}"),
    }

    // Y04.3: Q (live on ctx-b but not the requester) is refused; the space
    // admin reads the identical content-free body.
    match adapter
        .get_receipt(
            &mut tx,
            &q,
            json!({ "space_id": SPACE, "receipt_id": receipt_id }),
        )
        .await
    {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
            assert_eq!(
                reject.details.as_ref().expect("details")["retry_class"],
                "terminal"
            );
        }
        other => panic!("non-requester poll must refuse: {other:?}"),
    }
    let body = ready(
        adapter
            .get_receipt(
                &mut tx,
                &admin,
                json!({ "space_id": SPACE, "receipt_id": receipt_id }),
            )
            .await,
        "receipts.get",
    );
    assert_eq!(body["receipt"], *retained);

    // Y04.4: P's last grant is revoked; both its known-id and unissued-id
    // polls refuse NOT_AUTHORIZED/terminal identically (no enumeration), and
    // the admin still reads the retained receipt.
    ready(
        adapter
            .revoke_grant(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "grant_id": grant_b,
                    "expected_revision": 1,
                    "idempotency_key": "y04-revoke-b",
                }),
            )
            .await,
        "grants.revoke",
    );
    for id in [receipt_id.as_str(), "rcp_y04_unknown"] {
        match adapter
            .get_receipt(&mut tx, &p, json!({ "space_id": SPACE, "receipt_id": id }))
            .await
        {
            SpokeResult::Reject(reject) => {
                assert_eq!(reject.code, SpokeRejectCode::NotAuthorized, "{id}");
                assert_eq!(
                    reject.details.as_ref().expect("details")["retry_class"],
                    "terminal",
                    "{id}"
                );
            }
            other => panic!("non-participant poll must refuse {id}: {other:?}"),
        }
    }
    let body = ready(
        adapter
            .get_receipt(
                &mut tx,
                &admin,
                json!({ "space_id": SPACE, "receipt_id": receipt_id }),
            )
            .await,
        "receipts.get",
    );
    assert_eq!(body["receipt"], *retained);
    tx.commit().await.expect("caller commit");

    // The retained receipt and the revoked grant rows are durable across the
    // close/reopen boundary — authorization survives reopen.
    harness.reopen().await;
    let mut tx = harness.begin().await;
    let body = ready(
        adapter
            .get_receipt(
                &mut tx,
                &admin,
                json!({ "space_id": SPACE, "receipt_id": receipt_id }),
            )
            .await,
        "receipts.get",
    );
    assert_eq!(body["receipt"]["status"], "committed");
}

/// L3/reopen on the embedded arm: a journaled effect committed by the caller
/// keeps its row, pending receipt and pending intent durable across a
/// close/reopen, and `settle_journal` — the caller's publication-complete
/// phase — marks the intent applied, scrubs its targets and commits the
/// receipt under one transaction.
#[tokio::test]
async fn journaled_effect_survives_reopen_and_settles() {
    let mut harness = Harness::open().await;
    let adapter = MemoryAdapter::new(true);
    let admin = principal(PRINCIPAL);
    let mut tx = harness.begin().await;
    let pending = staged(
        adapter
            .create_record(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "kind": "fact",
                    "scope": { "visibility": "shared" },
                    "content": { "text": "a journaled record body" },
                    "idempotency_key": "reopen-create",
                }),
            )
            .await,
        "records.create",
    )
    .expect("create is journaled");
    tx.commit().await.expect("caller commit");

    // After close/reopen the row, the pending receipt and the pending intent
    // are all still there; the intent still owes its publication targets.
    harness.reopen().await;
    let (status, state, targets): (String, String, String) = sqlx::query_as(
        "SELECT r.status, j.state, j.targets_json \
         FROM memory_receipts r JOIN memory_journal j ON j.receipt_id = r.receipt_id \
         WHERE r.receipt_id = ?1",
    )
    .bind(&pending)
    .fetch_one(&harness.pool)
    .await
    .expect("receipt + intent after reopen");
    assert_eq!(status, "pending");
    assert_eq!(state, "pending");
    assert_ne!(targets, "[]", "an unsettled intent keeps its targets");
    assert_eq!(harness.count("memory_records").await, 1);

    // Phase 2: settle_journal in a fresh caller transaction commits the
    // receipt and applies the intent; before it commits a poll reports
    // OUTCOME_UNCERTAIN, after it the same id returns the committed body.
    let mut tx = harness.begin().await;
    match adapter
        .get_receipt(
            &mut tx,
            &admin,
            json!({ "space_id": SPACE, "receipt_id": pending }),
        )
        .await
    {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::OutcomeUncertain);
        }
        other => panic!("a pending receipt must poll uncertain: {other:?}"),
    }
    let body = match adapter
        .settle_journal(&mut tx, &pending_space(), &pending)
        .await
    {
        SpokeResult::Ok(body) => body,
        other => panic!("settle_journal failed: {other:?}"),
    };
    assert_eq!(body["status"], "committed");
    tx.commit().await.expect("settle commit");

    let (status, state, targets): (String, String, String) = sqlx::query_as(
        "SELECT r.status, j.state, j.targets_json \
         FROM memory_receipts r JOIN memory_journal j ON j.receipt_id = r.receipt_id \
         WHERE r.receipt_id = ?1",
    )
    .bind(&pending)
    .fetch_one(&harness.pool)
    .await
    .expect("settled receipt + intent");
    assert_eq!(status, "committed");
    assert_eq!(state, "applied");
    assert_eq!(targets, "[]", "settlement scrubs the targets");
}

/// O1: the unadvertised build refuses every Reflection selector — the same
/// CAPABILITY_PORT_MISSING/terminal shape with the exact details.op, matching
/// the pinned provider's open_unadvertised observable. `describe` keeps the
/// base capability and omits spoke-memory-reflection.
#[tokio::test]
async fn optional_reflection_refusal() {
    let harness = Harness::open().await;
    let adapter = MemoryAdapter::new(false);
    let admin = principal(PRINCIPAL);
    let mut tx = harness.begin().await;

    let body = ready(
        adapter.describe(&mut tx, &admin, json!({})).await,
        "describe",
    );
    let capabilities = body["capabilities"].as_array().expect("capabilities");
    assert!(capabilities.iter().any(|c| c == "spoke-memory"));
    assert!(!capabilities.iter().any(|c| c == "spoke-memory-reflection"));

    let selection = json!({ "mode": "subject_all" });
    let ops: [(&'static str, SpokeResult<MemoryStage>); 3] = [
        (
            "memory.reflections.state",
            adapter
                .get_reflection_state(
                    &mut tx,
                    &admin,
                    json!({ "space_id": SPACE, "subject_id": SUBJECT, "selection": selection }),
                )
                .await,
        ),
        (
            "memory.reflections.list",
            adapter
                .list_reflections(
                    &mut tx,
                    &admin,
                    json!({ "space_id": SPACE, "subject_id": SUBJECT, "selection": selection }),
                )
                .await,
        ),
        (
            "memory.reflections.generate",
            adapter
                .generate_reflection(
                    &mut tx,
                    &admin,
                    json!({
                        "space_id": SPACE,
                        "subject_id": SUBJECT,
                        "selection": selection,
                        "expected_state_digest": "0".repeat(64),
                        "idempotency_key": "opt-reflection",
                    }),
                )
                .await,
        ),
    ];
    for (op, result) in ops {
        match result {
            SpokeResult::Reject(reject) => {
                assert_eq!(reject.code, SpokeRejectCode::CapabilityPortMissing, "{op}");
                let details = reject.details.as_ref().expect("refusal details");
                assert_eq!(details["retry_class"], "terminal", "{op}");
                assert_eq!(details["op"], op, "{op}");
            }
            SpokeResult::Ok(_) => panic!("{op} must refuse"),
        }
    }
    // A refused optional family writes nothing: no receipt, no reflection row.
    tx.commit().await.expect("caller commit");
    assert_eq!(harness.count("memory_receipts").await, 0);
    assert_eq!(harness.count("memory_reflections").await, 0);
}

fn pending_space() -> mindcore_base::ids::SpaceId {
    mindcore_base::ids::SpaceId::new(SPACE)
}

/// F2 terminal arm: transaction 1 stages the personality profile, a live
/// record and the leased generation receipt; transaction 2 — a fresh caller
/// transaction — completes the lease: the envelope lands in
/// `memory_reflections` and the receipt commits with the pinned
/// `result.reflection_id`/`result.cost` shape.
#[tokio::test]
async fn reflection_lease_completes_in_second_transaction() {
    let harness = Harness::open().await;
    let adapter = MemoryAdapter::new(true);
    let admin = principal(PRINCIPAL);

    let mut tx = harness.begin().await;
    staged(
        adapter
            .update_profile(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "profile_kind": "personality",
                    "expected_revision": null,
                    "document": { "format": "markdown", "body": "# Parity subject" },
                    "idempotency_key": "f2-profile",
                }),
            )
            .await,
        "profile.update",
    );
    staged(
        adapter
            .create_record(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "kind": "fact",
                    "scope": { "visibility": "shared" },
                    "content": { "text": "basis record" },
                    "idempotency_key": "f2-record",
                }),
            )
            .await,
        "records.create",
    );
    let body = ready(
        adapter
            .get_reflection_state(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" },
                }),
            )
            .await,
        "reflections.state",
    );
    let digest = body["state_digest"]
        .as_str()
        .expect("state digest")
        .to_string();
    let work = match adapter
        .generate_reflection(
            &mut tx,
            &admin,
            json!({
                "space_id": SPACE,
                "subject_id": SUBJECT,
                "selection": { "mode": "subject_all" },
                "expected_state_digest": digest,
                "idempotency_key": "f2-generate",
            }),
        )
        .await
    {
        SpokeResult::Ok(MemoryStage::ReflectionPending(work)) => work,
        other => panic!("generate did not stage a lease: {other:?}"),
    };
    tx.commit().await.expect("transaction 1 commit");

    // Transaction 2: the composed deterministic effect settles the lease.
    let mut tx = harness.begin().await;
    let body = match adapter.complete_reflection(&mut tx, work.compose()).await {
        SpokeResult::Ok(body) => body,
        other => panic!("complete_reflection failed: {other:?}"),
    };
    assert_eq!(body["status"], "committed");
    let reflection_id = body["result"]["reflection_id"]
        .as_str()
        .expect("reflection id")
        .to_string();
    tx.commit().await.expect("transaction 2 commit");

    let body_text: String =
        sqlx::query_scalar("SELECT body_text FROM memory_reflections WHERE reflection_id = ?1")
            .bind(&reflection_id)
            .fetch_one(&harness.pool)
            .await
            .expect("envelope row");
    assert!(!body_text.is_empty());
    // The now-visible reflection lists and reports a fresh state.
    let mut tx = harness.begin().await;
    let body = ready(
        adapter
            .list_reflections(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" },
                }),
            )
            .await,
        "reflections.list",
    );
    assert_eq!(
        body["reflections"].as_array().expect("reflections").len(),
        1
    );
}
