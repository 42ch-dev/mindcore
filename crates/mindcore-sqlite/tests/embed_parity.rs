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
//! driven end to end on the same borrowed transaction; `settle_journal` /
//! `complete_reflection` are caller-phase methods, not selectors).

use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use spoke_operations::{jcs_sha256_hex, SpokeRejectCode, SpokeResult};
use spoke_schemas::Principal;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{AssertSqlSafe, SqlitePool, Transaction};
use tempfile::TempDir;

use mindcore_spoke::{MemoryAdapter, MemoryStage};
use mindcore_sqlite::MindStore;

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

/// Caller harness: a pool the test owns, the protocol schema `MindStore::open`
/// installs, and the space/subject/grant rows the selector calls need — seeded
/// by the caller, never by the adapter.
struct Harness {
    _fixture: Fixture,
    db: PathBuf,
    pool: SqlitePool,
}

impl Harness {
    async fn open() -> Self {
        let fixture = Fixture::fresh();
        let db = fixture.db_path();
        let pool = MindStore::open(&db).await.expect("open store").pool();
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
        // note: two seeded grants — a space-wide admin grant (no subject_id) for the
        // control-plane ops, plus a subject-scoped grant carrying the whole
        // subject-side capability set for content/profile selectors.
        sqlx::query(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
              shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES ('grt_admin', 'spc_parity', NULL, 'ctr_admin', '[\"admin\",\"transfer\"]', \
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

/// Settle one staged journaled intent inside the same transaction — the
/// shape a reconciled production caller takes: `settle_journal` marks the
/// intent `applied` and the receipt `committed`, so the withheld-read guard
/// (`UNRECONCILED`) finds no unsettled intent when a later guarded subject
/// read runs in this fixture.
async fn settle(
    adapter: &MemoryAdapter,
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    receipt_id: &str,
    op: &str,
) {
    match adapter
        .settle_journal(tx, &pending_space(), receipt_id)
        .await
    {
        SpokeResult::Ok(body) => {
            assert_eq!(body["status"], "committed", "{op} settlement");
        }
        other => panic!("{op} settlement failed: {other:?}"),
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

/// Symlink creation is the only platform-specific step; without it the
/// dangling leaf never exists and the guard assertions would prove nothing.
#[cfg(unix)]
#[test]
fn fixture_guard_refuses_dangling_symlink() {
    let fixture = Fixture::fresh();
    let foreign = fixture.root.join("foreign-target.db");
    let link = fixture.db_path();
    std::os::unix::fs::symlink(&foreign, &link).expect("symlink");
    assert!(!link.exists());
    assert!(Fixture::guard(&link).is_err());
}

/// The 33 named adapter operations, each driven by the caller's own
/// `&mut Transaction` — the full v02 selector surface on the embedded arm.
/// Reads must reach a `Ready` stage and writes `Ready` or `JournalPending`.
/// The five transfer operations run the same way: their rows are visible only
/// inside the caller's transaction, the caller's rollback erases them and the
/// caller's commit makes them durable, so none opens one of its own.
/// One caller transaction carries every baseline selector and commits at the end.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn every_selector_takes_the_caller_transaction() {
    let harness = Harness::open().await;
    let adapter = MemoryAdapter::new(true);
    let admin = principal(PRINCIPAL);
    let mut tx = harness.begin().await;

    // note: C0 describe — no request members needed.
    let body = ready(
        adapter.describe(&mut tx, &admin, json!({})).await,
        "describe",
    );
    assert_eq!(
        body["capabilities"],
        json!([
            "spoke-memory",
            "spoke-memory-reflection",
            "spoke-memory-transfer"
        ])
    );

    // note: C1 list spaces (empty object is the list-everything request).
    let body = ready(
        adapter.list_spaces(&mut tx, &admin, json!({})).await,
        "spaces.list",
    );
    assert_eq!(body["spaces"][0]["space_id"], SPACE);

    // note: C2 freeze — expected_revision = 1 -> revision 2.
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

    // note: C3 unfreeze back to active revision 3 so later ops are not gated.
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

    // note: C4 ensure a second subject (created=true) next to the seeded one.
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

    // note: C5 get the seeded subject.
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

    // note: C6 fence ctx-a on the subject (revision 1 -> 2).
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

    // note: C7 grant a review-only coverage for a second principal.
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

    // note: C8 list grants for the space.
    let body = ready(
        adapter
            .list_grants(&mut tx, &admin, json!({ "space_id": SPACE }))
            .await,
        "grants.list",
    );
    assert_eq!(body["grants"].as_array().expect("grants").len(), 3);

    // note: C9 revoke the grant at revision 1.
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

    // note: D1 capture a candidate (Ready stage, receipt staged in the caller tx).
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

    // note: a second pending candidate for the dismiss selector.
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

    // note: D2 list pending candidates.
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

    // note: D3 get the first candidate.
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

    // note: D4 review cand_a to promote — a journaled effect: receipt and intent
    // are staged, then settled in-tx so later guarded reads reconcile.
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
    settle(&adapter, &mut tx, &review_receipt, "candidates.review").await;

    // note: D5 dismiss cand_b — also journaled and settled in-tx.
    let dismiss_receipt = staged(
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
    settle(
        &adapter,
        &mut tx,
        &dismiss_receipt.expect("dismiss is journaled"),
        "candidates.dismiss",
    )
    .await;

    // why: the review's promoted record, visible inside the caller transaction.
    let ra: String = sqlx::query_scalar("SELECT record_id FROM memory_records")
        .fetch_one(&mut *tx)
        .await
        .expect("promoted record staged");

    // note: D6 create an authored record — journaled and settled in-tx.
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
    settle(&adapter, &mut tx, &create_receipt, "records.create").await;
    let rn: String =
        sqlx::query_scalar("SELECT record_id FROM memory_records WHERE record_id != ?1")
            .bind(&ra)
            .fetch_one(&mut *tx)
            .await
            .expect("created record staged");

    // note: D7 get the promoted record.
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

    // note: D8 list the subject's records (the promoted row and the created row).
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

    // note: D9 update the created record at revision 1.
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
    settle(
        &adapter,
        &mut tx,
        &update_receipt.unwrap(),
        "records.update",
    )
    .await;

    // note: D10 promote moves the record into context scope ctx-b — the baseline's
    // scope-change case: the row's scope and revision update inside the
    // caller's transaction and the change is journaled (receipt + intent
    // staged, then settled in-tx so later guarded reads reconcile).
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
    settle(
        &adapter,
        &mut tx,
        &promote_receipt.unwrap(),
        "records.promote",
    )
    .await;
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

    // note: D11 delete at the live revision (update bumped to 2, promote to 3).
    let delete_receipt = staged(
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
    settle(
        &adapter,
        &mut tx,
        &delete_receipt.expect("delete is journaled"),
        "records.delete",
    )
    .await;

    // note: D12 recall the subject's remaining rows.
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

    // note: D13 update the personality profile — journaled.
    let profile_receipt = staged(
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
    settle(
        &adapter,
        &mut tx,
        &profile_receipt.expect("profile update is journaled"),
        "profile.update",
    )
    .await;

    // note: D14 read the profile back.
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

    // note: D15 poll the capture receipt by id.
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

    // note: F1 reflection state — a read; the digest feeds F2.
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

    // note: F2 generate — this fixture is complete (personality profile + live
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

    // note: F3 list reflections.
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

    // note: everything above sits in this one transaction; commit and confirm.
    tx.commit().await.expect("caller commit");
    assert_eq!(harness.count("memory_candidates").await, 2);
    assert_eq!(harness.count("memory_records").await, 2);
    assert_eq!(harness.count("memory_profiles").await, 1);
    // why: staged receipts exist for every mutating selector; staged journal
    // intents exist for the journaled ones (review, dismiss, create, update,
    // delete, profile update).
    assert!(harness.count("memory_receipts").await >= 8);
    assert!(harness.count("memory_journal").await >= 5);
    let _ = review_receipt;
    let _ = create_receipt;

    // note: O2 the transfer family runs on the caller's transaction too. The export is only
    // readable from inside the transaction that wrote it, the import it feeds is staged and
    // discarded on that same transaction, and the caller's rollback erases every row they wrote
    // — so none of the five opens, commits or rolls back a transaction of its own.
    let mut transfer = harness.begin().await;
    let created = ready(
        adapter
            .create_export(
                &mut transfer,
                &admin,
                json!({ "space_id": SPACE, "idempotency_key": "par-export" }),
            )
            .await,
        "exports.create",
    );
    assert_eq!(created["status"], json!("committed"));
    let export_id = created["result"]["export_id"]
        .as_str()
        .expect("export id")
        .to_string();
    let chunk_count = created["result"]["chunk_count"]
        .as_u64()
        .expect("chunk count");
    let written: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM memory_exports")
        .fetch_one(&mut *transfer)
        .await
        .expect("the export header is readable through the caller's transaction");
    assert_eq!(written, 1);

    // note: exports.get reads that uncommitted export — a transaction of its own could not see it.
    let page = ready(
        adapter
            .get_export(
                &mut transfer,
                &admin,
                json!({ "space_id": SPACE, "export_id": &export_id, "chunk_ordinal": 0 }),
            )
            .await,
        "exports.get",
    );
    let chunk = page["chunk"].clone();
    assert_eq!(chunk["export_id"], json!(export_id));
    assert_eq!(chunk["chunk_count"], json!(chunk_count));

    // note: imports.stage takes ordinal zero of that export. The parity space exports as one
    // chunk, so staging the final ordinal verifies the sequence, fixes the translated rows
    // and reaches `staged`. The import targets the same space, so the subject collision the
    // final chunk decides is resolved by a rename.
    assert_eq!(chunk_count, 1, "the parity space exports as one chunk");
    let staged_body = ready(
        adapter
            .stage_import(
                &mut transfer,
                &admin,
                json!({
                    "space_id": SPACE,
                    "chunk": chunk,
                    "conflict_policy": "rename_subject",
                    "idempotency_key": "par-stage",
                }),
            )
            .await,
        "imports.stage",
    );
    assert_eq!(staged_body["result"]["chunks_received"], json!(1));
    assert!(
        staged_body["result"]["staged_report"].is_object(),
        "the final chunk freezes the identity report"
    );
    let import_id = staged_body["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();
    let state: String = sqlx::query_scalar("SELECT state FROM memory_imports WHERE import_id = ?1")
        .bind(&import_id)
        .fetch_one(&mut *transfer)
        .await
        .expect("the staged import is readable through the caller's transaction");
    assert_eq!(state, "staged");

    // note: imports.discard releases the staged payload inside the same transaction.
    let discarded = ready(
        adapter
            .discard_import(
                &mut transfer,
                &admin,
                json!({
                    "space_id": SPACE,
                    "import_id": &import_id,
                    "idempotency_key": "par-discard",
                }),
            )
            .await,
        "imports.discard",
    );
    assert_eq!(discarded["result"]["discarded"], json!(true));

    // note: imports.activate resolves the import this transaction just discarded; a separate
    // transaction would not see it at all and answer NOT_FOUND instead of the state conflict.
    match adapter
        .activate_import(
            &mut transfer,
            &admin,
            json!({
                "space_id": SPACE,
                "import_id": &import_id,
                "idempotency_key": "par-activate",
            }),
        )
        .await
    {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::ImportConflict);
            let details = reject.details.as_ref().expect("refusal details");
            assert_eq!(details["state"], json!("discarded"));
        }
        SpokeResult::Ok(stage) => {
            panic!("an import that left `staged` cannot activate: {stage:?}")
        }
    }

    // why: a transfer call that committed or rolled back on its own would leave rows here.
    transfer.rollback().await.expect("caller rollback");
    assert_eq!(harness.count("memory_exports").await, 0);
    assert_eq!(harness.count("memory_export_chunks").await, 0);
    assert_eq!(harness.count("memory_imports").await, 0);
    assert_eq!(harness.count("memory_import_chunks").await, 0);

    // note: the caller's commit is what makes a transfer effect durable.
    let mut committed = harness.begin().await;
    ready(
        adapter
            .create_export(
                &mut committed,
                &admin,
                json!({ "space_id": SPACE, "idempotency_key": "par-export-committed" }),
            )
            .await,
        "exports.create",
    );
    committed.commit().await.expect("caller commit");
    assert_eq!(harness.count("memory_exports").await, 1);
}

/// D11: the caller's commit is what makes the staged effect, its receipt and
/// a journaled effect's pending intent durable — the adapter commits nothing.
#[tokio::test]
async fn caller_commit_persists_effect_receipt_and_intent() {
    let harness = Harness::open().await;
    let adapter = MemoryAdapter::new(true);
    let admin = principal(PRINCIPAL);

    // why: capture is a Ready stage, but the candidate + receipt live only in the tx.
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

    // why: a journaled effect — record + pending receipt + pending intent, all
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
    // note: (a) the journaled operation's record row persisted.
    let record_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM memory_records")
        .fetch_one(&harness.pool)
        .await
        .expect("record count");
    assert_eq!(record_count, 1, "the journaled record must persist");
    // note: (b) capture's own committed receipt persisted beside its effect.
    let (cap_status, cap_receipt_id): (String, String) = sqlx::query_as(
        "SELECT status, receipt_id FROM memory_receipts WHERE op = 'memory.capture'",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("capture receipt");
    assert_eq!(cap_status, "committed");
    assert!(cap_receipt_id.starts_with("rct_"), "{cap_receipt_id}");
    // note: (c) the journaled effect's pending receipt + pending intent persisted.
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

    // note: Y04.0 — O grants P capture on ctx-a and ctx-b, Q capture on ctx-b only.
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

    // note: Y04.0 continued — P captures under ctx-a; the committed receipt carries
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

    // note: Y04.1 — revoking only the ctx-a grant leaves P a live participant via
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

    // note: Y04.2 — P replays the identical capture while only ctx-b is live; the
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

    // note: Y04.3 — Q (live on ctx-b but not the requester) is refused; the space
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

    // note: Y04.4 — P's last grant is revoked; both its known-id and unissued-id
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

    // why: the retained receipt and the revoked grant rows are durable across the
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

    // note: baseline L3 pre-COMMIT fault — the caller drops its transaction before
    // committing, so no record, receipt, intent or file target is durable —
    // including across the pool close/reopen.
    let mut tx = harness.begin().await;
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
                    "content": { "text": "a rolled-back record body" },
                    "idempotency_key": "j-rollback",
                }),
            )
            .await,
        "records.create",
    );
    tx.rollback().await.expect("pre-commit fault");
    harness.reopen().await;
    for table in ["memory_records", "memory_receipts", "memory_journal"] {
        assert_eq!(harness.count(table).await, 0, "{table} must stay empty");
    }

    // note: baseline L3 normal path — journaled create committed by the caller.
    let create_request = json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "kind": "fact",
        "scope": { "visibility": "shared" },
        "content": { "text": "a journaled record body" },
        "idempotency_key": "reopen-create",
    });
    let mut tx = harness.begin().await;
    let pending = staged(
        adapter
            .create_record(&mut tx, &admin, create_request.clone())
            .await,
        "records.create",
    )
    .expect("create is journaled");
    tx.commit().await.expect("caller commit");

    // why: after close/reopen the row, the pending receipt and the pending intent
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
    let record_id: String = sqlx::query_scalar("SELECT record_id FROM memory_records")
        .fetch_one(&harness.pool)
        .await
        .expect("staged record row");

    // note: post-COMMIT fault leg — replaying the identical request while the
    // receipt is pending reports OUTCOME_UNCERTAIN — the dedup probe finds
    // the pending receipt and refuses before a second effect can stage.
    let mut tx = harness.begin().await;
    match adapter
        .create_record(&mut tx, &admin, create_request.clone())
        .await
    {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::OutcomeUncertain);
        }
        other => panic!("pending replay must report uncertain: {other:?}"),
    }
    tx.rollback().await.expect("read-only replay tx");
    assert_eq!(harness.count("memory_records").await, 1);
    assert_eq!(harness.count("memory_journal").await, 1);

    // note: file leg — every target the intent owes is a digest-matched file the
    // caller publishes into `memory-store/` beside the fixture DB. The
    // digest is JCS/SHA-256 over {"content": <bytes>} — recomputed the same
    // way the adapter stamped it.
    let store_dir = harness
        .db
        .parent()
        .expect("fixture root")
        .join("memory-store");
    for target in serde_json::from_str::<Vec<Value>>(&targets).expect("targets json") {
        let rel = target["path"].as_str().expect("target path");
        let digest = target["digest"].as_str().expect("target digest");
        let content = target["content"].as_str().expect("target content");
        let file = store_dir.join(rel);
        std::fs::create_dir_all(file.parent().expect("target dir")).expect("mkdir");
        std::fs::write(&file, content).expect("publish target");
        let landed = std::fs::read_to_string(&file).expect("read published target");
        let recomputed = match jcs_sha256_hex(&json!({ "content": landed })) {
            SpokeResult::Ok(digest) => digest,
            SpokeResult::Reject(reject) => panic!("digest recompute rejected: {reject:?}"),
        };
        assert_eq!(recomputed, digest, "digest-matched file for {rel}");
    }

    // note: post-COMMIT / settle-path fault — publication has already landed, then
    // a storage failure inside the settlement transaction (journal table
    // renamed out from under the read) makes the outcome genuinely unknown —
    // the adapter reports OUTCOME_UNCERTAIN with the `uncertain` retry class,
    // never a retryable pre-commit reject.
    let published_files = file_set(&store_dir);
    let mut tx = harness.begin().await;
    sqlx::query("ALTER TABLE memory_journal RENAME TO memory_journal_broken")
        .execute(&mut *tx)
        .await
        .expect("inject storage failure");
    match adapter
        .settle_journal(&mut tx, &pending_space(), &pending)
        .await
    {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::OutcomeUncertain);
            assert_eq!(
                reject.details.as_ref().expect("details")["retry_class"],
                "uncertain"
            );
        }
        other => panic!("a storage-faulted settle must report uncertain: {other:?}"),
    }
    tx.rollback().await.expect("rollback restores the table");

    // why: the caller's rollback leaves nothing new durable — the intent and its
    // receipt stay pending, and the renamed table reverts with them.
    let (status, state): (String, String) = sqlx::query_as(
        "SELECT r.status, j.state \
         FROM memory_receipts r JOIN memory_journal j ON j.receipt_id = r.receipt_id \
         WHERE r.receipt_id = ?1",
    )
    .bind(&pending)
    .fetch_one(&harness.pool)
    .await
    .expect("receipt + intent after fault rollback");
    assert_eq!(status, "pending");
    assert_eq!(state, "pending");
    assert_eq!(file_set(&store_dir), published_files);
    assert_eq!(
        harness.count("memory_journal").await,
        1,
        "the faulted settle must not create a second intent"
    );

    // note: poll before settlement reports OUTCOME_UNCERTAIN; republish the owed
    // targets (idempotent) and the retry settle commits receipt + intent.
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
    republish(&store_dir, &targets);
    let body = match adapter
        .settle_journal(&mut tx, &pending_space(), &pending)
        .await
    {
        SpokeResult::Ok(body) => body,
        other => panic!("retry settle failed: {other:?}"),
    };
    assert_eq!(body["status"], "committed");
    tx.commit().await.expect("settle commit");
    let settled_files = file_set(&store_dir);
    assert_eq!(
        settled_files, published_files,
        "settle must not rewrite published files"
    );

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

    // why: replay-does-not-duplicate-publish — a second settle returns the stored
    // outcome and replaying the original op request serves the committed
    // receipt — no second publish: the file set is byte-identical to the
    // post-publish snapshot and no new record or intent appears.
    let mut tx = harness.begin().await;
    let body = match adapter
        .settle_journal(&mut tx, &pending_space(), &pending)
        .await
    {
        SpokeResult::Ok(body) => body,
        other => panic!("re-settle must return the stored outcome: {other:?}"),
    };
    assert_eq!(body["status"], "committed");
    let body = ready(
        adapter.create_record(&mut tx, &admin, create_request).await,
        "records.create replay",
    );
    assert_eq!(body["status"], "committed");
    assert_eq!(body["result"]["record_id"], record_id);
    tx.commit().await.expect("replay tx commit");
    assert_eq!(harness.count("memory_records").await, 1);
    assert_eq!(harness.count("memory_journal").await, 1);
    assert_eq!(
        file_set(&store_dir),
        published_files,
        "replay must not republish or touch the file set"
    );
}

/// Relative path → file bytes for everything under `dir` (empty when the
/// directory does not exist) — the publish-set snapshot the replay legs
/// compare.
fn file_set(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                files.insert(
                    path.strip_prefix(&dir).unwrap_or(&path).to_path_buf(),
                    std::fs::read(&path).expect("read published file"),
                );
            }
        }
    }
    files
}

/// Re-confirm every staged target on disk — the idempotent republish the
/// caller runs after a settlement attempt whose outcome stayed uncertain.
fn republish(store_dir: &Path, targets_json: &str) {
    for target in serde_json::from_str::<Vec<Value>>(targets_json).expect("targets json") {
        let rel = target["path"].as_str().expect("target path");
        let content = target["content"].as_str().expect("target content");
        let file = store_dir.join(rel);
        std::fs::create_dir_all(file.parent().expect("target dir")).expect("mkdir");
        std::fs::write(&file, content).expect("republish target");
    }
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
    // why: a refused optional family writes nothing — no receipt, no reflection row.
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
    let profile_receipt = staged(
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
    settle(
        &adapter,
        &mut tx,
        &profile_receipt.expect("profile update is journaled"),
        "profile.update",
    )
    .await;
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
                    "content": { "text": "basis record" },
                    "idempotency_key": "f2-record",
                }),
            )
            .await,
        "records.create",
    );
    settle(
        &adapter,
        &mut tx,
        &create_receipt.expect("create is journaled"),
        "records.create",
    )
    .await;
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

    // note: transaction 2 — the composed deterministic effect settles the lease.
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
    // why: the now-visible reflection lists and reports a fresh state.
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

/// Baseline D3: candidate visibility tracks the caller's coverage, not the
/// caller's identity. A principal granted `review` on U restricted to ctx-a
/// reads the ctx-a candidate but gets `NOT_FOUND` for the ctx-b one; after
/// the review consumes the in-scope candidate, the same restricted read
/// serves the digest-bearing shell with its receipt link and no proposed
/// body.
#[tokio::test]
async fn restricted_caller_candidate_visibility() {
    let harness = Harness::open().await;
    let adapter = MemoryAdapter::new(true);
    let admin = principal(PRINCIPAL);
    let reviewer = principal("ctr_p");
    let mut tx = harness.begin().await;

    // note: grant P review coverage on U confined to ctx-a, no shared access.
    ready(
        adapter
            .grant(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "principal_id": "ctr_p",
                    "capabilities": ["review"],
                    "context_ids": [CTX_A],
                    "shared_access": false,
                    "idempotency_key": "d3-grant",
                }),
            )
            .await,
        "grants.grant",
    );

    let mut candidates = Vec::new();
    for (context, key) in [(CTX_A, "d3-cap-a"), ("ctx-b", "d3-cap-b")] {
        let body = ready(
            adapter
                .capture(
                    &mut tx,
                    &admin,
                    json!({
                        "space_id": SPACE,
                        "subject_id": SUBJECT,
                        "capture_scope": { "visibility": "context", "context_id": context },
                        "content": { "text": "a scoped candidate body" },
                        "idempotency_key": key,
                    }),
                )
                .await,
            "capture",
        );
        candidates.push(
            body["result"]["candidate_ids"][0]
                .as_str()
                .expect("candidate id")
                .to_string(),
        );
    }
    let [cand_a, cand_b] = candidates.as_slice() else {
        unreachable!()
    };

    // note: in-scope — the restricted caller reads its own pending candidate.
    let body = ready(
        adapter
            .get_candidate(
                &mut tx,
                &reviewer,
                json!({ "space_id": SPACE, "candidate_id": cand_a }),
            )
            .await,
        "candidates.get",
    );
    assert_eq!(body["candidate"]["status"], "pending");

    // note: out of scope — the ctx-b candidate hides exactly as an absent id.
    match adapter
        .get_candidate(
            &mut tx,
            &reviewer,
            json!({ "space_id": SPACE, "candidate_id": cand_b }),
        )
        .await
    {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::NotFound);
            assert_eq!(
                reject.details.as_ref().expect("details")["retry_class"],
                "terminal"
            );
        }
        other => panic!("out-of-scope candidate must hide: {other:?}"),
    }

    // note: admin consumes cand_a by review-promote; the post-consume restricted
    // read serves the consumed shell — digest + consuming receipt, no body.
    let receipt_id = staged(
        adapter
            .review_candidate(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "candidate_id": cand_a,
                    "expected_revision": 1,
                    "decision": { "action": "promote" },
                    "idempotency_key": "d3-review",
                }),
            )
            .await,
        "candidates.review",
    )
    .expect("review is journaled");
    settle(&adapter, &mut tx, &receipt_id, "candidates.review").await;
    let body = ready(
        adapter
            .get_candidate(
                &mut tx,
                &reviewer,
                json!({ "space_id": SPACE, "candidate_id": cand_a }),
            )
            .await,
        "candidates.get post-consume",
    );
    let candidate = &body["candidate"];
    assert_eq!(candidate["status"], "consumed");
    assert!(
        candidate["content_digest"]
            .as_str()
            .is_some_and(|d| d.len() == 64),
        "consumed shell carries a content digest: {candidate}"
    );
    assert_eq!(candidate["consumed_by"]["receipt_id"], receipt_id);
    assert!(
        candidate["proposed"]["content"].is_null(),
        "consumed shell must not carry the proposed body: {candidate}"
    );

    tx.commit().await.expect("caller commit");
}

/// Baseline D10: `records.promote` INTO shared scope — the context-scoped
/// record lands in `shared` at the bumped revision, its provenance survives
/// the move, and list membership flips: shared gains the row, ctx-a loses
/// it. The change is journaled (receipt + intent staged and settled in-tx).
#[tokio::test]
async fn promotion_into_shared_scope() {
    let harness = Harness::open().await;
    let adapter = MemoryAdapter::new(true);
    let admin = principal(PRINCIPAL);
    let mut tx = harness.begin().await;

    // note: baseline carrier — a record authored into context scope ctx-a.
    let create_receipt = staged(
        adapter
            .create_record(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "kind": "fact",
                    "scope": { "visibility": "context", "context_id": CTX_A },
                    "source_context_ids": ["ctx-b"],
                    "content": { "text": "a context-scoped record body" },
                    "idempotency_key": "d10-create",
                }),
            )
            .await,
        "records.create",
    );
    settle(
        &adapter,
        &mut tx,
        &create_receipt.expect("create is journaled"),
        "records.create",
    )
    .await;
    let rn: String = sqlx::query_scalar("SELECT record_id FROM memory_records")
        .fetch_one(&mut *tx)
        .await
        .expect("created record staged");
    let provenance: String =
        sqlx::query_scalar("SELECT provenance_json FROM memory_records WHERE record_id = ?1")
            .bind(&rn)
            .fetch_one(&mut *tx)
            .await
            .expect("provenance before promote");

    // note: before — ctx-a sees the record, shared does not.
    let body = ready(
        adapter
            .list_records(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "selected_contexts", "context_ids": [CTX_A] },
                }),
            )
            .await,
        "records.list ctx-a",
    );
    assert_eq!(body["records"].as_array().expect("records").len(), 1);
    let body = ready(
        adapter
            .list_records(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "shared_only" },
                }),
            )
            .await,
        "records.list shared",
    );
    assert!(body["records"].as_array().expect("records").is_empty());

    // note: promote into shared scope; the row updates in the caller's tx.
    let promote_receipt = staged(
        adapter
            .promote_record(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "record_id": rn,
                    "expected_revision": 1,
                    "target_scope": { "visibility": "shared" },
                    "idempotency_key": "d10-promote",
                }),
            )
            .await,
        "records.promote",
    )
    .expect("promote is journaled");
    settle(&adapter, &mut tx, &promote_receipt, "records.promote").await;
    let (visibility, context_id, revision, provenance_after): (
        String,
        Option<String>,
        i64,
        String,
    ) = sqlx::query_as(
        "SELECT scope_visibility, scope_context_id, revision, provenance_json \
         FROM memory_records WHERE record_id = ?1",
    )
    .bind(&rn)
    .fetch_one(&mut *tx)
    .await
    .expect("promoted row");
    assert_eq!(visibility, "shared");
    assert_eq!(context_id, None);
    assert_eq!(revision, 2);
    assert_eq!(
        provenance_after, provenance,
        "provenance survives promotion"
    );

    // note: after — membership flips: shared serves the record, ctx-a no longer.
    let body = ready(
        adapter
            .list_records(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "shared_only" },
                }),
            )
            .await,
        "records.list shared after",
    );
    let records = body["records"].as_array().expect("records");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["record_id"], rn);
    let body = ready(
        adapter
            .list_records(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "selected_contexts", "context_ids": [CTX_A] },
                }),
            )
            .await,
        "records.list ctx-a after",
    );
    assert!(body["records"].as_array().expect("records").is_empty());

    tx.commit().await.expect("caller commit");
    assert_eq!(harness.count("memory_journal").await, 2);
}

/// Baseline D1/D2: distinct bearer kinds (U `nexus/character`, V
/// `nexus/character-other`), context-scoped captures, exact-replay and
/// altered-content controls, and scoped candidate paging — O pages U/A and
/// U/B in order, the V/A candidate never appears, and a `review`@ctx-a
/// principal pages only its own context's candidates.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_capture_baseline_d1_d2() {
    let harness = Harness::open().await;
    let adapter = MemoryAdapter::new(true);
    let admin = principal(PRINCIPAL);
    let reviewer = principal("ctr_p");
    let mut tx = harness.begin().await;

    // note: ensure V with a distinct bearer kind — the second creation is valid
    // only because its kind.id differs from U's `character`.
    let body = ready(
        adapter
            .ensure_subject(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": "sbj_other",
                    "kind": { "ns": "nexus", "id": "character-other" },
                }),
            )
            .await,
        "subjects.ensure",
    );
    assert_eq!(body["result"]["created"], true);

    // note: grant P review coverage on U confined to ctx-a (baseline D2's
    // restricted lister).
    ready(
        adapter
            .grant(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "principal_id": "ctr_p",
                    "capabilities": ["review"],
                    "context_ids": [CTX_A],
                    "shared_access": false,
                    "idempotency_key": "d2-review-a",
                }),
            )
            .await,
        "grants.grant",
    );

    // note: CAP(U,A,d-a) → ca, CAP(U,B,d-b) → cb, CAP(U,A,d-c) → cc, CAP(V,A,d-v).
    let capture = |key: &str, subject: &str, context: &str| {
        json!({
            "space_id": SPACE,
            "subject_id": subject,
            "capture_scope": { "visibility": "context", "context_id": context },
            "content": { "text": "the northern gate opens before dawn" },
            "idempotency_key": key,
        })
    };
    let mut ids = Vec::new();
    for (key, context) in [("d-a", CTX_A), ("d-b", "ctx-b"), ("d-c", CTX_A)] {
        let body = ready(
            adapter
                .capture(&mut tx, &admin, capture(key, SUBJECT, context))
                .await,
            "capture",
        );
        assert_eq!(body["result"]["classified"], 1);
        assert!(
            !body.to_string().contains("northern gate"),
            "result must be content-free: {body}"
        );
        ids.push(
            body["result"]["candidate_ids"][0]
                .as_str()
                .expect("candidate id")
                .to_string(),
        );
    }
    let [cand_a, cand_b, cand_c] = ids.as_slice() else {
        unreachable!()
    };
    let body = ready(
        adapter
            .capture(&mut tx, &admin, capture("d-v", "sbj_other", CTX_A))
            .await,
        "capture-v",
    );
    let cand_v = body["result"]["candidate_ids"][0]
        .as_str()
        .expect("v candidate id")
        .to_string();

    // note: exact replay of d-a returns the same committed receipt/candidate id;
    // the same key with changed text is IDEMPOTENCY_CONFLICT/terminal.
    let body = ready(
        adapter
            .capture(&mut tx, &admin, capture("d-a", SUBJECT, CTX_A))
            .await,
        "capture replay",
    );
    assert_eq!(body["result"]["candidate_ids"][0], *cand_a);
    let mut changed = capture("d-a", SUBJECT, CTX_A);
    changed["content"]["text"] = json!("Changed northern gate evening.");
    match adapter.capture(&mut tx, &admin, changed).await {
        SpokeResult::Reject(reject) => {
            assert_eq!(reject.code, SpokeRejectCode::IdempotencyConflict);
            assert_eq!(
                reject.details.as_ref().expect("details")["retry_class"],
                "terminal"
            );
        }
        other => panic!("altered replay must conflict: {other:?}"),
    }

    // note: D2 — O pages U's pending candidates limit=1 — [ca], [cb], [cc], no gap,
    // and cv is excluded from U's listing entirely.
    let mut cursor = Value::Null;
    let mut pages = Vec::new();
    for _ in 0..4 {
        let body = ready(
            adapter
                .list_candidates(
                    &mut tx,
                    &admin,
                    json!({
                        "space_id": SPACE,
                        "subject_id": SUBJECT,
                        "status": "pending",
                        "limit": 1,
                        "cursor": cursor,
                    }),
                )
                .await,
            "candidates.list",
        );
        pages.push(
            body["candidates"].as_array().expect("candidates")[0]["candidate_id"]
                .as_str()
                .expect("id")
                .to_string(),
        );
        match body["next_cursor"].as_str() {
            Some(next) => cursor = json!(next),
            None => break,
        }
    }
    assert_eq!(
        pages.as_slice(),
        [cand_a.as_str(), cand_b.as_str(), cand_c.as_str()],
        "pages must serve the candidates in declared capture order: {pages:?}"
    );
    assert!(
        !pages.contains(&cand_v),
        "V candidate must not list under U"
    );

    // note: the restricted principal pages only its granted context — [ca], [cc],
    // skipping the ctx-b candidate without a gap.
    let mut cursor = Value::Null;
    let mut pages = Vec::new();
    for _ in 0..4 {
        let body = ready(
            adapter
                .list_candidates(
                    &mut tx,
                    &reviewer,
                    json!({
                        "space_id": SPACE,
                        "subject_id": SUBJECT,
                        "status": "pending",
                        "limit": 1,
                        "cursor": cursor,
                    }),
                )
                .await,
            "candidates.list restricted",
        );
        pages.push(
            body["candidates"].as_array().expect("candidates")[0]["candidate_id"]
                .as_str()
                .expect("id")
                .to_string(),
        );
        match body["next_cursor"].as_str() {
            Some(next) => cursor = json!(next),
            None => break,
        }
    }
    assert_eq!(
        pages.as_slice(),
        [cand_a.as_str(), cand_c.as_str()],
        "restricted pages must be ca, cc with no gap: {pages:?}"
    );

    tx.commit().await.expect("caller commit");
}

/// Baseline D12: recall pages ranked results under the item budget —
/// `max_items=1` yields one record per page with `total_authorized=2`, only
/// page 1 truncates, and tombstoned rows never appear. The byte-budget arm
/// names the overflowing record without fabricating content.
///
/// Tombstone exclusion is query-shape independent, proven against the shape
/// the shipped code has: `records.delete` scrubs the body to `{"text":""}` and
/// keeps only the deletion mark, and recall's candidate query admits live rows
/// only, so no query word can reach a tombstoned row. The test therefore takes
/// a positive control with the deleted record's own words while it is live
/// (they rank it first), then reruns the same words after the delete: no
/// surviving record carries them, yet the zero-score siblings still come back
/// and the tombstone does not — its absence is the tombstone filter, not a
/// query that could never have matched it.
#[tokio::test]
async fn recall_pagination_d12() {
    let harness = Harness::open().await;
    let adapter = MemoryAdapter::new(true);
    let admin = principal(PRINCIPAL);
    let mut tx = harness.begin().await;

    let mut records = Vec::new();
    for (key, text) in [
        ("d12-create-a", "the northern gate opens before dawn"),
        ("d12-create-b", "the northern gate closes at dusk"),
        (
            "d12-create-del",
            "the northern gate ledger is sealed and unreadable",
        ),
    ] {
        let create_receipt = staged(
            adapter
                .create_record(
                    &mut tx,
                    &admin,
                    json!({
                        "space_id": SPACE,
                        "subject_id": SUBJECT,
                        "kind": "fact",
                        "scope": { "visibility": "context", "context_id": CTX_A },
                        "source_context_ids": [CTX_A],
                        "content": { "text": text },
                        "idempotency_key": key,
                    }),
                )
                .await,
            "records.create",
        );
        settle(
            &adapter,
            &mut tx,
            &create_receipt.expect("create is journaled"),
            "records.create",
        )
        .await;
        records.push(
            sqlx::query_scalar::<_, String>(
                "SELECT record_id FROM memory_records WHERE content_json LIKE ?1",
            )
            .bind(format!("%{text}%"))
            .fetch_one(&mut *tx)
            .await
            .expect("record id"),
        );
    }
    let [r1, r2, r_del] = records.as_slice() else {
        unreachable!()
    };
    // note: positive control while the record is live — its own words match it and
    // rank it first, while the two siblings score zero and are still served.
    // The same words are rerun after the delete below, so their later failure
    // to produce this record cannot be blamed on the query shape.
    let body = ready(
        adapter
            .recall(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" },
                    "query": "sealed unreadable",
                    "budget": { "max_items": 10 },
                }),
            )
            .await,
        "recall before delete",
    );
    assert_eq!(body["total_authorized"], 3);
    let matches = body["records"].as_array().expect("records");
    assert_eq!(matches.len(), 3, "recall admits zero-score rows too");
    assert_eq!(
        matches[0]["record_id"],
        json!(r_del),
        "the deleted record's words rank it first"
    );
    let delete_receipt = staged(
        adapter
            .delete_record(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "record_id": r_del,
                    "expected_revision": 1,
                    "idempotency_key": "d12-delete",
                }),
            )
            .await,
        "records.delete",
    );
    settle(
        &adapter,
        &mut tx,
        &delete_receipt.expect("delete is journaled"),
        "records.delete",
    )
    .await;

    // why: tombstoning scrubs the body and keeps the deletion mark — the row below
    // still exists, but carries no searchable text for any query to rank.
    let tombstone: Option<String> =
        sqlx::query_scalar("SELECT tombstone_json FROM memory_records WHERE record_id = ?1")
            .bind(r_del)
            .fetch_one(&mut *tx)
            .await
            .expect("deleted record row");
    assert!(
        tombstone.is_some(),
        "r_del must persist as a tombstoned row"
    );
    let content: String =
        sqlx::query_scalar("SELECT content_json FROM memory_records WHERE record_id = ?1")
            .bind(r_del)
            .fetch_one(&mut *tx)
            .await
            .expect("deleted record content");
    assert_eq!(
        serde_json::from_str::<Value>(&content).expect("content json"),
        json!({ "text": "" }),
        "the tombstone retains no searchable text"
    );

    // note: same words as the positive control, now that no surviving record carries
    // them: both zero-score siblings are still served and the tombstone is not,
    // so the exclusion is the tombstone filter rather than the query shape.
    let body = ready(
        adapter
            .recall(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" },
                    "query": "sealed unreadable",
                    "budget": { "max_items": 10 },
                }),
            )
            .await,
        "recall after delete",
    );
    assert_eq!(body["total_authorized"], 2);
    let mut survived: Vec<&str> = body["records"]
        .as_array()
        .expect("records")
        .iter()
        .map(|record| record["record_id"].as_str().expect("record id"))
        .collect();
    survived.sort_unstable();
    let mut expected = vec![r1.as_str(), r2.as_str()];
    expected.sort_unstable();
    assert_eq!(
        survived, expected,
        "zero-score live rows stay served; the tombstone does not"
    );

    // note: page 1 — one record, truncated, cursor continues; page 2 serves the
    // remaining record with no further cursor. A second query shape reaches
    // the same two live rows and still never the tombstone.
    let body = ready(
        adapter
            .recall(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" },
                    "query": "northern gate",
                    "budget": { "max_items": 1 },
                }),
            )
            .await,
        "recall page 1",
    );
    assert_eq!(body["records"].as_array().expect("records").len(), 1);
    let page1_id = body["records"].as_array().expect("records")[0]["record_id"]
        .as_str()
        .expect("page 1 record id")
        .to_string();
    assert!(
        [r1.as_str(), r2.as_str()].contains(&page1_id.as_str()),
        "page 1 must serve a live expected record: {page1_id}"
    );
    assert_ne!(page1_id, r_del.as_str(), "tombstone must not appear");
    assert_eq!(body["total_authorized"], 2);
    assert_eq!(body["truncated"], true);
    let cursor = body["next_cursor"].clone();
    assert!(!cursor.is_null(), "page 1 must continue");

    let body = ready(
        adapter
            .recall(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" },
                    "query": "northern gate",
                    "budget": { "max_items": 1 },
                    "cursor": cursor,
                }),
            )
            .await,
        "recall page 2",
    );
    assert_eq!(body["records"].as_array().expect("records").len(), 1);
    let page2_id = body["records"].as_array().expect("records")[0]["record_id"]
        .as_str()
        .expect("page 2 record id")
        .to_string();
    assert!(
        [r1.as_str(), r2.as_str()].contains(&page2_id.as_str()),
        "page 2 must serve a live expected record: {page2_id}"
    );
    assert_ne!(page2_id, r_del.as_str(), "tombstone must not appear");
    assert_ne!(
        page1_id, page2_id,
        "the two pages must serve distinct records"
    );
    assert_eq!(body["total_authorized"], 2);
    assert_eq!(body["truncated"], false);
    assert!(body["next_cursor"].is_null());

    // note: byte budget below one record's size — the budget names the overflowing
    // record and keeps the page honest — no fabricated partial body.
    let body = ready(
        adapter
            .recall(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "selected_contexts", "context_ids": [CTX_A] },
                    "filter": { "source_context_ids": [CTX_A] },
                    "budget": { "max_items": 10, "max_bytes": 1 },
                }),
            )
            .await,
        "recall byte budget",
    );
    assert_eq!(body["total_authorized"], 2);
    assert_eq!(body["truncated"], true);
    assert!(body["overflow_record_id"].is_string());

    tx.commit().await.expect("caller commit");
}

/// Baseline F3: `reflections.list` pages newest-first through its cursor —
/// after a second generation the updated envelope leads page 1 while the
/// first follows on page 2, and each row reports its own recomputed stale
/// flag.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn reflection_pagination_f3() {
    let harness = Harness::open().await;
    let adapter = MemoryAdapter::new(true);
    let admin = principal(PRINCIPAL);

    // note: transaction 1 — profile + basis record + first generation lease.
    let mut tx = harness.begin().await;
    let profile_receipt = staged(
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
                    "idempotency_key": "f3-profile",
                }),
            )
            .await,
        "profile.update",
    );
    settle(
        &adapter,
        &mut tx,
        &profile_receipt.expect("profile update is journaled"),
        "profile.update",
    )
    .await;
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
                    "content": { "text": "basis record" },
                    "idempotency_key": "f3-record",
                }),
            )
            .await,
        "records.create",
    );
    settle(
        &adapter,
        &mut tx,
        &create_receipt.expect("create is journaled"),
        "records.create",
    )
    .await;
    let record_id: String = sqlx::query_scalar("SELECT record_id FROM memory_records")
        .fetch_one(&mut *tx)
        .await
        .expect("record id");
    let digest = ready(
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
    )["state_digest"]
        .as_str()
        .expect("digest")
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
                "idempotency_key": "f3-generate",
            }),
        )
        .await
    {
        SpokeResult::Ok(MemoryStage::ReflectionPending(work)) => work,
        other => panic!("generate did not stage a lease: {other:?}"),
    };
    tx.commit().await.expect("tx1 commit");

    // note: transaction 2 — complete the first lease.
    let mut tx = harness.begin().await;
    let body = match adapter.complete_reflection(&mut tx, work.compose()).await {
        SpokeResult::Ok(body) => body,
        other => panic!("complete_reflection failed: {other:?}"),
    };
    let first_id = body["result"]["reflection_id"]
        .as_str()
        .expect("first reflection")
        .to_string();
    tx.commit().await.expect("tx2 commit");

    // note: transaction 3 — update the basis record, re-read the state digest and
    // stage the second generation.
    let mut tx = harness.begin().await;
    let update_receipt = staged(
        adapter
            .update_record(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "record_id": record_id,
                    "expected_revision": 1,
                    "content": { "text": "A changed basis." },
                    "idempotency_key": "f3-change",
                }),
            )
            .await,
        "records.update",
    );
    settle(
        &adapter,
        &mut tx,
        &update_receipt.expect("update is journaled"),
        "records.update",
    )
    .await;
    let digest = ready(
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
        "reflections.state updated",
    )["state_digest"]
        .as_str()
        .expect("updated digest")
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
                "idempotency_key": "f3-generate-updated",
            }),
        )
        .await
    {
        SpokeResult::Ok(MemoryStage::ReflectionPending(work)) => work,
        other => panic!("updated generate did not stage a lease: {other:?}"),
    };
    tx.commit().await.expect("tx3 commit");

    // note: transaction 4 — complete the updated lease.
    let mut tx = harness.begin().await;
    let body = match adapter.complete_reflection(&mut tx, work.compose()).await {
        SpokeResult::Ok(body) => body,
        other => panic!("complete_reflection failed: {other:?}"),
    };
    let updated_id = body["result"]["reflection_id"]
        .as_str()
        .expect("updated reflection")
        .to_string();
    tx.commit().await.expect("tx4 commit");

    // note: list limit=1 — the updated (newest) envelope leads, the stale first
    // envelope follows on the cursor page — each flag recomputed at read.
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
                    "limit": 1,
                }),
            )
            .await,
        "reflections.list page 1",
    );
    let page1 = body["reflections"].as_array().expect("reflections");
    assert_eq!(page1.len(), 1);
    assert_eq!(page1[0]["reflection_id"], updated_id);
    assert_eq!(page1[0]["stale"], false);
    let cursor = body["next_cursor"].clone();
    assert!(!cursor.is_null(), "page 1 must continue");

    let body = ready(
        adapter
            .list_reflections(
                &mut tx,
                &admin,
                json!({
                    "space_id": SPACE,
                    "subject_id": SUBJECT,
                    "selection": { "mode": "subject_all" },
                    "limit": 1,
                    "cursor": cursor,
                }),
            )
            .await,
        "reflections.list page 2",
    );
    let page2 = body["reflections"].as_array().expect("reflections");
    assert_eq!(page2.len(), 1);
    assert_eq!(page2[0]["reflection_id"], first_id);
    assert_eq!(
        page2[0]["stale"], true,
        "the pinned-basis reflection is stale"
    );
    assert!(body["next_cursor"].is_null());
    tx.commit().await.expect("read commit");
}
