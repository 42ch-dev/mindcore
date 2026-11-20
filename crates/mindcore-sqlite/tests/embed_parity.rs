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
    pool: SqlitePool,
}

impl Harness {
    async fn open() -> Self {
        let fixture = Fixture::fresh();
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(fixture.db_path())
                    .create_if_missing(true)
                    .foreign_keys(false),
            )
            .await
            .expect("open pool");
        sqlx::raw_sql(DDL).execute(&pool).await.expect("apply DDL");
        sqlx::query(
            "INSERT INTO memory_spaces \
             (space_id, display_name, authority_host_id, authority_generation, status, revision, \
              created_at) \
             VALUES ('spc_parity', 'parity', 'host_1', 1, 'active', 1, '2026-11-20T09:20:01.123Z')",
        )
        .execute(&pool)
        .await
        .expect("seed space");
        sqlx::query(
            "INSERT INTO memory_subjects \
             (space_id, subject_id, kind_ns, kind_id, status, fenced_contexts_json, revision, \
              created_at) \
             VALUES ('spc_parity', 'sbj_mira', 'nexus', 'character', 'active', '[]', 1, \
              '2026-11-20T09:20:01.123Z')",
        )
        .execute(&pool)
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
        .execute(&pool)
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
        .execute(&pool)
        .await
        .expect("seed subject grant");
        Self {
            _fixture: fixture,
            pool,
        }
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

    // D10 promote is a same-scope no-op here (shared -> shared), still staged
    // on the caller's transaction; expected revision is now 2.
    staged(
        adapter
            .promote_record(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "record_id": rn,
                    "expected_revision": 2,
                    "target_scope": { "visibility": "shared" },
                    "idempotency_key": "par-promote",
                }),
            )
            .await,
        "records.promote",
    );

    // D11 delete at the live revision (update bumped to 2, promote no-ops).
    staged(
        adapter
            .delete_record(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "record_id": rn,
                    "expected_revision": 2,
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

    // F2 generate — leased reflection or an honest reject; either way the
    // adapter answered on the caller's transaction.
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
    match &generate {
        SpokeResult::Ok(_) | SpokeResult::Reject(_) => {}
    }

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
    for result in transfer {
        match result {
            SpokeResult::Reject(reject) => {
                assert_eq!(reject.code, SpokeRejectCode::CapabilityPortMissing);
            }
            SpokeResult::Ok(_) => panic!("a transfer selector must refuse"),
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
