//! The export builder against seeded storage and the §6.13 boundary cases.

use serde_json::{json, Map, Value};

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::{ContextId, SpaceId, SubjectId};
use mindcore_base::ir::{
    CandidateState, Classification, Consumption, MemoryCandidateIr, MemoryContent, MemoryGrantRow,
    MemoryJournalRow, MemoryProfileRow, MemoryRecordIr, MemoryReflectionRow, MemoryRepository,
    MemorySubjectRow, Placement, PlacementSelection, Provenance, ReceiptState, RecordState,
    ReviewReceiptIr,
};
use mindcore_sqlite::{MindStore, SqliteMemoryRepository};
use sqlx::SqlitePool;
use tempfile::TempDir;

use crate::policy;

use super::bundle::digest_excluding;
use super::chunk::BUNDLE_ARRAY_FIELDS;
use super::{build, ExportSpec};

const SPACE: &str = "spc_1";
const NOW: &str = "2026-11-22T09:00:00.000Z";

fn spec(subject_id: Option<&str>, selection: PlacementSelection) -> ExportSpec {
    ExportSpec {
        export_id: "exp_1".to_string(),
        space_id: SpaceId::new(SPACE),
        subject_id: subject_id.map(SubjectId::new),
        selection,
        include_tombstoned: false,
        include_reflections: false,
        created_at: chrono::DateTime::parse_from_rfc3339(NOW)
            .expect("timestamp")
            .with_timezone(&chrono::Utc),
    }
}

fn placement(context_id: Option<&str>) -> Placement {
    context_id.map_or(Placement::Shared, |id| Placement::Context(id.to_string()))
}

fn record(
    record_id: &str,
    subject_id: &str,
    context_id: Option<&str>,
    tombstoned: bool,
) -> MemoryRecordIr {
    MemoryRecordIr {
        record_id: record_id.to_string(),
        space_id: SPACE.to_string(),
        subject_id: subject_id.to_string(),
        kind: "note".to_string(),
        scope: placement(context_id),
        provenance: Provenance {
            source_context_ids: context_id.map(str::to_string).into_iter().collect(),
            anchors: Vec::new(),
            captured_by: "p_1".to_string(),
            captured_at: NOW.to_string(),
        },
        state: if tombstoned {
            RecordState::Tombstoned {
                deleted_at: NOW.to_string(),
                deleted_by: "p_1".to_string(),
                reason: None,
            }
        } else {
            RecordState::Live {
                content: MemoryContent {
                    text: format!("ledger for {record_id}"),
                    structured: None,
                    cognition: None,
                },
                import_ref: None,
            }
        },
        occurred_at: None,
        created_at: NOW.to_string(),
        updated_at: NOW.to_string(),
        revision: serde_json::Number::from(1),
        extensions: Map::new(),
    }
}

fn subject(subject_id: &str, status: &str, fenced: &[&str]) -> MemorySubjectRow {
    MemorySubjectRow {
        space_id: SpaceId::new(SPACE),
        subject_id: SubjectId::new(subject_id),
        kind: None,
        status: status.to_string(),
        fenced_contexts: fenced.iter().map(|id| ContextId::new(*id)).collect(),
        revision: serde_json::Number::from(1),
        created_at: NOW.to_string(),
        extensions: Map::new(),
    }
}

fn grant(
    grant_id: &str,
    subject_id: Option<&str>,
    principal: &str,
    contexts: &[&str],
    shared_access: bool,
    revoked: bool,
) -> MemoryGrantRow {
    MemoryGrantRow {
        grant_id: grant_id.to_string(),
        space_id: SpaceId::new(SPACE),
        subject_id: subject_id.map(SubjectId::new),
        principal_id: CallerId::new(principal),
        capabilities: vec!["transfer".to_string(), "records_read".to_string()],
        context_ids: contexts.iter().map(|id| ContextId::new(*id)).collect(),
        shared_access,
        expires_at: None,
        revoked_at: revoked.then(|| NOW.to_string()),
        revision: serde_json::Number::from(1),
        created_at: NOW.to_string(),
        extensions: Map::new(),
    }
}

fn candidate(
    candidate_id: &str,
    context_id: Option<&str>,
    state: CandidateState,
) -> MemoryCandidateIr {
    MemoryCandidateIr {
        candidate_id: candidate_id.to_string(),
        space_id: SPACE.to_string(),
        subject_id: "s_1".to_string(),
        capture_scope: placement(context_id),
        classification: Classification {
            disposition: "promote".to_string(),
            rationale_codes: Vec::new(),
            confidence: None,
        },
        provenance: Provenance {
            source_context_ids: context_id.map(str::to_string).into_iter().collect(),
            anchors: Vec::new(),
            captured_by: "p_1".to_string(),
            captured_at: NOW.to_string(),
        },
        state,
        occurred_at: None,
        created_at: NOW.to_string(),
        revision: serde_json::Number::from(1),
        extensions: Map::new(),
    }
}

struct Fixture {
    _dir: TempDir,
    pool: SqlitePool,
    repository: SqliteMemoryRepository,
}

impl Fixture {
    async fn open() -> Self {
        let dir = TempDir::new().expect("temporary directory");
        let pool = MindStore::open(&dir.path().join("memory.db"))
            .await
            .expect("open store")
            .pool();
        let fixture = Self {
            _dir: dir,
            pool,
            repository: SqliteMemoryRepository,
        };
        fixture.seed().await;
        fixture
    }

    async fn seed(&self) {
        let mut tx = self.pool.begin().await.expect("begin");
        sqlx::query(
            "INSERT INTO memory_spaces \
             (space_id, display_name, authority_host_id, authority_generation, status, revision, \
              created_at) VALUES (?, 'space', 'host_1', 3, 'active', 1, ?)",
        )
        .bind(SPACE)
        .bind(NOW)
        .execute(&mut *tx)
        .await
        .expect("seed space");
        for row in [
            subject("s_1", "active", &["ctx_1", "ctx_2"]),
            subject("s_2", "archived", &[]),
        ] {
            self.repository
                .insert_subject(&mut tx, &row)
                .await
                .expect("seed subject");
        }
        for row in [
            grant("g_1", None, "p_1", &[], false, false),
            grant("g_2", Some("s_1"), "p_2", &["ctx_1"], false, false),
            grant("g_3", Some("s_1"), "p_3", &[], false, true),
        ] {
            self.repository
                .insert_grant(&mut tx, &row)
                .await
                .expect("seed grant");
        }
        for row in [
            record("r_1", "s_1", None, false),
            record("r_2", "s_1", Some("ctx_1"), false),
            record("r_3", "s_1", None, true),
            record("r_4", "s_2", None, false),
        ] {
            self.repository
                .insert_record(&mut tx, &row)
                .await
                .expect("seed record");
        }
        for row in [
            candidate(
                "c_1",
                None,
                CandidateState::Pending {
                    kind: "note".to_string(),
                    content: MemoryContent {
                        text: "pending".to_string(),
                        structured: None,
                        cognition: None,
                    },
                    expires_at: None,
                },
            ),
            candidate(
                "c_2",
                Some("ctx_1"),
                CandidateState::Consumed {
                    proposed_kind: Some("note".to_string()),
                    consumed_by: Consumption {
                        operation: "memory.candidates.review".to_string(),
                        receipt_id: "rc_1".to_string(),
                        outcome: "promoted".to_string(),
                        record_ids: vec!["r_2".to_string(), "r_1".to_string()],
                    },
                    content_digest: "a".repeat(64),
                },
            ),
        ] {
            self.repository
                .insert_candidate(&mut tx, &row)
                .await
                .expect("seed candidate");
        }
        let profile = MemoryProfileRow {
            space_id: SpaceId::new(SPACE),
            subject_id: SubjectId::new("s_1"),
            profile_kind: "personality".to_string(),
            document: json!({ "body": "steady", "format": "markdown" }),
            updated_by: CallerId::new("p_1"),
            revision: serde_json::Number::from(1),
            created_at: NOW.to_string(),
            updated_at: NOW.to_string(),
            extensions: Map::new(),
        };
        self.repository
            .insert_profile(&mut tx, &profile)
            .await
            .expect("seed profile");
        let receipt = ReviewReceiptIr {
            receipt_id: "rc_1".to_string(),
            space_id: SPACE.to_string(),
            requester: "p_1".to_string(),
            operation: "memory.candidates.review".to_string(),
            idempotency_key: Some("key_1".to_string()),
            payload_digest: "b".repeat(64),
            created_at: NOW.to_string(),
            state: ReceiptState::Committed {
                completed_at: NOW.to_string(),
                result: json!({}),
            },
            extensions: Map::new(),
        };
        self.repository
            .insert_receipt(&mut tx, &receipt, None)
            .await
            .expect("seed receipt");
        tx.commit().await.expect("commit seed");
    }

    async fn begin(&self) -> sqlx::Transaction<'_, sqlx::Sqlite> {
        self.pool.begin().await.expect("begin")
    }
}

/// Concatenate a chunk sequence back into the logical bundle it carries.
fn reassemble(chunks: &[Value]) -> Value {
    let mut arrays: Map<String, Value> = Map::new();
    let mut header = None;
    for chunk in chunks {
        for field in BUNDLE_ARRAY_FIELDS {
            if let Some(items) = chunk.get(field).and_then(Value::as_array) {
                arrays
                    .entry(field.to_string())
                    .or_insert_with(|| Value::Array(Vec::new()))
                    .as_array_mut()
                    .expect("array")
                    .extend(items.iter().cloned());
            }
        }
        if let Some(value) = chunk.get("header") {
            header = Some(value.clone());
        }
    }
    let mut bundle = header
        .expect("a header chunk")
        .as_object()
        .expect("header")
        .clone();
    for (field, items) in arrays {
        bundle.insert(field, items);
    }
    Value::Object(bundle)
}

fn hex_digest_excluding(value: &Value, field: &str) -> String {
    let mut stripped = value.clone();
    stripped.as_object_mut().expect("object").remove(field);
    match spoke_operations::jcs_sha256_hex(&stripped) {
        spoke_operations::SpokeResult::Ok(digest) => digest,
        spoke_operations::SpokeResult::Reject(reject) => panic!("digest: {reject:?}"),
    }
}

fn ok<T>(result: crate::reject::AResult<T>, what: &str) -> T {
    match result {
        Ok(value) => value,
        Err(reject) => panic!("{what}: {} ({:?})", reject.message, reject.code),
    }
}

fn decode_bundle(bundle: &Value) -> spoke_schemas::MemoryExportBundle {
    serde_json::from_value(bundle.clone()).expect("bundle decodes against its schema")
}

#[tokio::test]
async fn whole_space_bundle_round_trips_and_digests() {
    let fixture = Fixture::open().await;
    let mut tx = fixture.begin().await;
    let artifact = ok(
        build(
            &fixture.repository,
            &mut tx,
            &spec(None, PlacementSelection::SubjectAll),
        )
        .await,
        "build export",
    );
    tx.rollback().await.expect("rollback");

    let bundle = decode_bundle(&artifact.bundle);
    assert_eq!(bundle.source.authority_generation.get(), 3);
    assert_eq!(bundle.subjects.len(), 2);
    assert_eq!(
        bundle.records.len(),
        3,
        "the tombstone is excluded by default"
    );
    assert_eq!(bundle.candidates.len(), 2);
    assert_eq!(bundle.profiles.len(), 1);
    assert_eq!(bundle.grants.len(), 2, "the revoked grant is excluded");
    assert_eq!(bundle.receipt_summaries.len(), 1);
    assert!(bundle.reflections.is_empty(), "not requested");
    assert_eq!(artifact.digest.len(), 64);

    assert_eq!(
        hex_digest_excluding(&artifact.bundle, "digest"),
        artifact.digest
    );

    let chunk_count = artifact.chunks.len() as u64;
    assert_eq!(
        artifact.chunks[0]["chunk_count"].as_u64(),
        Some(chunk_count)
    );
    for (ordinal, chunk) in artifact.chunks.iter().enumerate() {
        assert_eq!(chunk["chunk_ordinal"].as_u64(), Some(ordinal as u64));
        assert_eq!(
            chunk["chunk_digest"].as_str(),
            Some(hex_digest_excluding(chunk, "chunk_digest").as_str())
        );
        assert_eq!(
            chunk["bytes"].as_u64(),
            Some(spoke_operations::content_serialized_bytes(chunk) as u64),
            "bytes is the JCS length of the whole chunk"
        );
        let decoded = serde_json::from_value::<spoke_schemas::MemoryExportChunk>(chunk.clone())
            .expect("chunk decodes against its schema");
        assert_eq!(
            matches!(decoded, spoke_schemas::MemoryExportChunk::HeaderChunk(_)),
            ordinal == 0
        );
        assert_eq!(chunk["header"].is_object(), ordinal == 0);
    }

    let reassembled = reassemble(&artifact.chunks);
    let mut expected = artifact.bundle.clone();
    expected.as_object_mut().expect("object").remove("digest");
    let mut actual = reassembled.clone();
    actual.as_object_mut().expect("object").remove("digest");
    assert_eq!(actual, expected);
    assert_eq!(
        hex_digest_excluding(&reassembled, "digest"),
        artifact.digest
    );
}

#[tokio::test]
async fn restricted_subject_export_scopes_every_member() {
    let fixture = Fixture::open().await;
    let mut tx = fixture.begin().await;
    let artifact = ok(
        build(
            &fixture.repository,
            &mut tx,
            &spec(
                Some("s_1"),
                PlacementSelection::SelectedContexts {
                    context_ids: vec![ContextId::new("ctx_1")],
                    include_shared: false,
                },
            ),
        )
        .await,
        "build export",
    );
    tx.rollback().await.expect("rollback");

    let bundle = decode_bundle(&artifact.bundle);
    assert_eq!(bundle.subjects.len(), 1);
    assert_eq!(
        bundle.subjects[0].fenced_contexts,
        vec!["ctx_1".to_string()],
        "only the selected fence id is exposed"
    );
    assert_eq!(bundle.records.len(), 1);
    assert_eq!(bundle.candidates.len(), 1, "only the ctx_1 candidate");
    let expected_candidate = json!({
        "schema_version": 1,
        "candidate_id": "c_2",
        "space_id": SPACE,
        "subject_id": "s_1",
        "capture_scope": { "visibility": "context", "context_id": "ctx_1" },
        "status": "consumed",
        "proposed": { "kind": "note" },
        "classification": { "disposition": "promote", "rationale_codes": [] },
        "provenance": {
            "source_context_ids": ["ctx_1"],
            "anchors": [],
            "captured_by": "p_1",
            "captured_at": "2026-11-22T09:00:00Z",
        },
        "created_at": "2026-11-22T09:00:00Z",
        "revision": 1,
        "extensions": {},
        "consumed_by": {
            "op": "memory.candidates.review",
            "receipt_id": "rc_1",
            "outcome": "promoted",
            "record_ids": ["r_2"],
        },
        "content_digest": "a".repeat(64),
    });
    assert_eq!(
        serde_json::to_value(&bundle.candidates[0]).expect("candidate"),
        expected_candidate,
        "an excluded record id is neither disclosed nor left dangling"
    );
    assert!(
        bundle.profiles.is_empty(),
        "a restricted export carries no profiles"
    );
    assert!(
        bundle.receipt_summaries.is_empty(),
        "no summary for a subject export"
    );
    assert_eq!(
        bundle.grants.len(),
        1,
        "only the ctx_1 grant is inside the scope"
    );
    let grant = &artifact.bundle["grants"][0];
    assert_eq!(grant["grant_id"], json!("g_2"));
    let label = grant["principal_ref"].as_str().expect("principal ref");
    assert!(label.starts_with("prl_"), "the label is opaque");
    assert_ne!(label, "p_2", "the raw principal id never crosses");
}

#[tokio::test]
async fn reflection_with_an_unverifiable_basis_is_withheld() {
    let fixture = Fixture::open().await;
    seed_reflection(&fixture, "ref_1").await;
    let mut request = spec(Some("s_1"), PlacementSelection::SubjectAll);
    request.include_reflections = true;

    let mut tx = fixture.begin().await;
    let artifact = ok(
        build(&fixture.repository, &mut tx, &request).await,
        "build export",
    );
    tx.rollback().await.expect("rollback");
    let bundle = decode_bundle(&artifact.bundle);
    assert_eq!(bundle.reflections.len(), 1);
    assert!(
        !bundle.reflections[0].stale,
        "the pinned basis still verifies"
    );

    let mut tx = fixture.begin().await;
    let mut moved = record("r_1", "s_1", None, true);
    moved.revision = serde_json::Number::from(2);
    fixture
        .repository
        .update_record(&mut tx, &moved, &serde_json::Number::from(1))
        .await
        .expect("tombstone a source");
    let artifact = ok(
        build(&fixture.repository, &mut tx, &request).await,
        "build export",
    );
    tx.rollback().await.expect("rollback");
    let bundle = decode_bundle(&artifact.bundle);
    assert!(bundle.reflections.is_empty());
}

#[tokio::test]
async fn a_subject_with_an_unsettled_intent_contributes_nothing() {
    let fixture = Fixture::open().await;
    seed_reflection(&fixture, "ref_1").await;
    let mut tx = fixture.begin().await;
    fixture
        .repository
        .insert_journal_intent(
            &mut tx,
            &MemoryJournalRow {
                seq: 0,
                receipt_id: "rc_pending".to_string(),
                space_id: SpaceId::new(SPACE),
                subject_id: SubjectId::new("s_1"),
                requester: CallerId::new("p_1"),
                operation: "memory.records.create".to_string(),
                idempotency_key: None,
                state: "pending".to_string(),
                targets: Vec::new(),
                result: None,
                created_at: NOW.to_string(),
                updated_at: NOW.to_string(),
            },
        )
        .await
        .expect("seed an unsettled intent");
    tx.commit().await.expect("commit intent");

    let mut request = spec(None, PlacementSelection::SubjectAll);
    request.include_reflections = true;
    let mut tx = fixture.begin().await;
    let artifact = ok(
        build(&fixture.repository, &mut tx, &request).await,
        "build export",
    );
    tx.rollback().await.expect("rollback");

    let bundle = decode_bundle(&artifact.bundle);
    let subject_ids: Vec<&str> = artifact.bundle["subjects"]
        .as_array()
        .expect("array")
        .iter()
        .map(|subject| subject["subject_id"].as_str().expect("subject id"))
        .collect();
    assert_eq!(
        subject_ids,
        vec!["s_2"],
        "a withheld subject contributes nothing, its subject row included"
    );
    let record_ids: Vec<&str> = artifact.bundle["records"]
        .as_array()
        .expect("array")
        .iter()
        .map(|record| record["record_id"].as_str().expect("record id"))
        .collect();
    assert_eq!(
        record_ids,
        vec!["r_4"],
        "only the unaffected subject's record crosses"
    );
    assert!(bundle.candidates.is_empty(), "no withheld candidate");
    assert!(bundle.profiles.is_empty(), "no withheld profile");
    assert!(bundle.reflections.is_empty(), "no withheld reflection");
    let grants: Vec<&Value> = artifact.bundle["grants"]
        .as_array()
        .expect("array")
        .iter()
        .collect();
    assert_eq!(
        grants
            .iter()
            .map(|grant| grant["grant_id"].as_str().expect("grant id"))
            .collect::<Vec<_>>(),
        vec!["g_1"],
        "only the space-wide grant survives the withholding"
    );
    assert!(
        !artifact.bundle.to_string().contains("s_1"),
        "the bundle names no trace of the withheld subject"
    );

    elevate(&fixture).await;
    target_space(&fixture).await;
    let chunks = sealed_partition(&artifact.bundle);
    for (ordinal, chunk) in chunks.iter().enumerate() {
        let staged = ready(
            stage_into(
                &fixture,
                TARGET,
                chunk.clone(),
                &format!("withheld-stage-{ordinal}"),
                None,
            )
            .await,
        );
        assert_eq!(staged["result"]["chunks_received"], json!(ordinal + 1));
    }
}

async fn seed_reflection(fixture: &Fixture, reflection_id: &str) {
    let live = vec![("r_1".to_string(), 1i64), ("r_2".to_string(), 1i64)];
    let profiles = vec![("personality".to_string(), 1i64)];
    let source_set_digest = ok(
        policy::reflection_source_set_digest(&live),
        "source set digest",
    );
    let state_digest = ok(
        policy::reflection_state_digest(&live, &profiles, &PlacementSelection::SubjectAll, None),
        "state digest",
    );
    let mut tx = fixture.begin().await;
    let row = MemoryReflectionRow {
        seq: 0,
        reflection_id: reflection_id.to_string(),
        space_id: SpaceId::new(SPACE),
        subject_id: SubjectId::new("s_1"),
        requester: CallerId::new("p_1"),
        selection: json!({ "mode": "subject_all" }),
        filter: None,
        cognition_filter: None,
        basis: json!({
            "record_count": live.len(),
            "source_set_digest": source_set_digest,
            "state_digest": state_digest,
            "profile_revisions": { "personality": 1 },
        }),
        engine_policy_version: policy::ENGINE_POLICY_VERSION.to_string(),
        model: None,
        schema_version: 1,
        revision: serde_json::Number::from(1),
        extensions: Map::new(),
        created_at: NOW.to_string(),
        body_text: "derived".to_string(),
        body_format: "markdown".to_string(),
        cost: None,
        stale_cache: false,
    };
    fixture
        .repository
        .insert_reflection(&mut tx, &row)
        .await
        .expect("seed reflection");
    tx.commit().await.expect("commit reflection");
}

/// A sealed bundle carrying the required members and the given arrays.
fn synthetic_bundle(arrays: Value) -> Value {
    let mut members = json!({
        "schema_version": 1,
        "bundle_format_version": 1,
        "memory_protocol_version": 1,
        "export_id": "exp_synthetic",
        "space_id": SPACE,
        "created_at": NOW,
        "source": {
            "authority_host_id": "host_1",
            "authority_generation": 1,
            "engine_policy_version": policy::ENGINE_POLICY_VERSION,
        },
        "subjects": [],
        "records": [],
        "profiles": [],
        "candidates": [],
        "receipt_summaries": [],
        "grants": [],
        "extensions": {},
    })
    .as_object()
    .expect("object")
    .clone();
    for (field, value) in arrays.as_object().expect("arrays") {
        members.insert(field.clone(), value.clone());
    }
    let mut bundle = Value::Object(members);
    let digest = ok(digest_excluding(&bundle, "digest"), "digest");
    bundle
        .as_object_mut()
        .expect("object")
        .insert("digest".to_string(), json!(digest));
    bundle
}

/// The seal a bundle publishes; partition takes it rather than recomputing it.
fn sealed_digest(bundle: &Value) -> &str {
    bundle["digest"].as_str().expect("the bundle is sealed")
}

fn synthetic_record(record_id: &str, text_length: usize) -> Value {
    json!({
        "schema_version": 1,
        "record_id": record_id,
        "space_id": SPACE,
        "subject_id": "s_1",
        "kind": "note",
        "scope": { "visibility": "shared" },
        "provenance": {
            "source_context_ids": [],
            "anchors": [],
            "captured_by": "p_1",
            "captured_at": NOW,
        },
        "created_at": NOW,
        "updated_at": NOW,
        "revision": 1,
        "extensions": {},
        "content": { "text": "x".repeat(text_length) },
    })
}

#[test]
fn a_partitioned_bundle_must_carry_the_seal_it_was_given() {
    let bundle = synthetic_bundle(json!({ "records": [synthetic_record("r_1", 16)] }));
    match super::chunk::partition(&bundle, &"0".repeat(64), policy::EXPORT_CHUNK_BYTES) {
        Ok(chunks) => panic!("a mismatched seal emitted {} chunks", chunks.len()),
        Err(error) => assert!(
            error.message.contains("digest does not verify"),
            "{}",
            error.message
        ),
    }
}

#[test]
fn one_oversized_element_travels_alone() {
    let limit = policy::EXPORT_CHUNK_BYTES;
    let bundle = synthetic_bundle(json!({
        "records": [
            synthetic_record("r_big", 300_000),
            synthetic_record("r_1", 16),
            synthetic_record("r_2", 16),
        ],
        "reflections": [],
    }));
    let chunks = ok(
        super::chunk::partition(&bundle, sealed_digest(&bundle), limit),
        "partition",
    );
    assert!(
        chunks.len() >= 2,
        "the small records still fill their own chunk"
    );
    let carrier = chunks
        .iter()
        .find(|chunk| {
            chunk["records"]
                .as_array()
                .is_some_and(|records| records.iter().any(|record| record["record_id"] == "r_big"))
        })
        .expect("the oversized record travels");
    assert_eq!(
        carrier["records"].as_array().expect("array").len(),
        1,
        "the oversized element is never split or packed with another"
    );
    assert!(carrier["bytes"].as_u64().expect("bytes") > limit);

    assert_eq!(
        chunks[0]["reflections"],
        json!([]),
        "a present-but-empty array stays visible on ordinal zero"
    );
    for (ordinal, chunk) in chunks.iter().enumerate() {
        assert_eq!(chunk["chunk_ordinal"].as_u64(), Some(ordinal as u64));
        assert_eq!(
            chunk["chunk_digest"].as_str(),
            Some(hex_digest_excluding(chunk, "chunk_digest").as_str())
        );
        let singleton = chunk
            .get("records")
            .and_then(Value::as_array)
            .is_some_and(|records| records.len() == 1);
        if !(singleton && chunk["bytes"].as_u64().expect("bytes") > limit) {
            assert!(chunk["bytes"].as_u64().expect("bytes") <= limit);
        }
    }
    let reassembled = reassemble(&chunks);
    assert_eq!(reassembled["records"], bundle["records"]);
}

#[test]
fn elements_are_never_split_at_a_small_limit() {
    let limit = 2_048;
    let records: Vec<Value> = (0..40)
        .map(|index| synthetic_record(&format!("r_{index:02}"), 256))
        .collect();
    let bundle = synthetic_bundle(json!({ "records": records.clone() }));
    let chunks = ok(
        super::chunk::partition(&bundle, sealed_digest(&bundle), limit),
        "partition",
    );
    assert!(chunks.len() > 1, "forty records split at a 2 KiB limit");
    let ordinals: Vec<u64> = chunks
        .iter()
        .map(|chunk| chunk["chunk_ordinal"].as_u64().expect("ordinal"))
        .collect();
    assert_eq!(ordinals, (0..chunks.len() as u64).collect::<Vec<_>>());
    for chunk in &chunks {
        let count = chunk
            .get("records")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        if count > 1 {
            assert!(chunk["bytes"].as_u64().expect("bytes") <= limit);
        }
    }
    let reassembled = reassemble(&chunks);
    assert_eq!(reassembled["records"], json!(records));
    assert_eq!(
        hex_digest_excluding(&reassembled, "digest"),
        bundle["digest"].as_str().expect("digest")
    );
}

#[test]
fn header_only_bundle_is_one_chunk() {
    let bundle = synthetic_bundle(json!({ "reflections": [] }));
    let chunks = ok(
        super::chunk::partition(&bundle, sealed_digest(&bundle), policy::EXPORT_CHUNK_BYTES),
        "partition",
    );
    assert_eq!(
        chunks.len(),
        1,
        "the header alone still travels as one chunk"
    );
    assert_eq!(chunks[0]["chunk_ordinal"], json!(0));
    assert_eq!(chunks[0]["chunk_count"], json!(1));
    assert_eq!(chunks[0]["header"]["export_id"], json!("exp_synthetic"));
    for field in BUNDLE_ARRAY_FIELDS {
        assert_eq!(
            chunks[0][field],
            json!([]),
            "{field} stays visible when empty"
        );
    }
    assert_eq!(
        hex_digest_excluding(&reassemble(&chunks), "digest"),
        bundle["digest"].as_str().expect("digest")
    );
}

#[test]
fn an_element_the_header_cannot_hold_starts_ordinal_one() {
    let limit = 2_048;
    let header_bytes = {
        let header_only = synthetic_bundle(json!({ "reflections": [] }));
        let chunks = ok(
            super::chunk::partition(&header_only, sealed_digest(&header_only), limit),
            "partition",
        );
        assert_eq!(chunks.len(), 1, "the header alone is one chunk");
        chunks[0]["bytes"].as_u64().expect("bytes")
    };

    let target = limit - header_bytes + 32;
    let base = spoke_operations::content_serialized_bytes(&synthetic_record("r_edge", 0)) as u64;
    let record = synthetic_record("r_edge", (target - base) as usize);
    let element_bytes = spoke_operations::content_serialized_bytes(&record) as u64;
    assert_eq!(element_bytes, target, "the record is sized as intended");
    assert!(element_bytes <= limit, "the element fits an ordinary chunk");
    assert!(
        header_bytes + element_bytes > limit,
        "the header plus the element overflows"
    );

    let bundle = synthetic_bundle(json!({ "records": [record], "reflections": [] }));
    let chunks = ok(
        super::chunk::partition(&bundle, sealed_digest(&bundle), limit),
        "partition",
    );
    assert_eq!(chunks.len(), 2, "the header moves to a chunk of its own");
    assert!(
        chunks[0].get("records").is_none(),
        "ordinal zero carries the header alone"
    );
    assert_eq!(
        chunks[1]["records"].as_array().expect("array").len(),
        1,
        "the lone element starts ordinal one"
    );
    assert!(chunks[0]["bytes"].as_u64().expect("bytes") <= limit);
    assert!(
        chunks[1]["bytes"].as_u64().expect("bytes") <= limit,
        "the element alone fits its chunk"
    );
    assert_eq!(reassemble(&chunks)["records"], bundle["records"]);
}

fn principal(id: &str) -> spoke_schemas::Principal {
    serde_json::from_value(json!({ "principal_id": id, "extensions": {} })).expect("principal")
}

fn ready(result: spoke_operations::SpokeResult<crate::MemoryStage>) -> Value {
    match result {
        spoke_operations::SpokeResult::Ok(crate::MemoryStage::Ready(body)) => body,
        other => panic!("expected a ready stage, got {other:?}"),
    }
}

fn rejected<T: std::fmt::Debug>(
    result: &spoke_operations::SpokeResult<T>,
) -> &spoke_operations::SpokeReject {
    match result {
        spoke_operations::SpokeResult::Reject(reject) => reject,
        spoke_operations::SpokeResult::Ok(_) => panic!("expected a reject, got {result:?}"),
    }
}

/// Create a whole-space export as `p_1` and return the committed receipt body.
async fn create_export(fixture: &Fixture, key: &str) -> Value {
    let adapter = crate::MemoryAdapter::new(false);
    let mut tx = fixture.begin().await;
    let body = ready(
        adapter
            .create_export(
                &mut tx,
                &principal("p_1"),
                json!({ "space_id": SPACE, "idempotency_key": key }),
            )
            .await,
    );
    tx.commit().await.expect("commit create");
    body
}

async fn read_page(fixture: &Fixture, export_id: &str, ordinal: u64, as_principal: &str) -> Value {
    let adapter = crate::MemoryAdapter::new(false);
    let mut tx = fixture.begin().await;
    let result = adapter
        .get_export(
            &mut tx,
            &principal(as_principal),
            json!({ "space_id": SPACE, "export_id": export_id, "chunk_ordinal": ordinal }),
        )
        .await;
    tx.rollback().await.expect("rollback read");
    ready(result)
}

#[tokio::test]
async fn create_then_page_reassembles_the_same_bundle() {
    let fixture = Fixture::open().await;
    let created = create_export(&fixture, "export-key-1").await;
    assert_eq!(created["status"], json!("committed"));
    let result = &created["result"];
    assert_eq!(result["counts"]["subjects"], json!(2));
    assert_eq!(result["counts"]["records"], json!(3));
    let export_id = result["export_id"].as_str().expect("export id").to_string();
    let chunk_count = result["chunk_count"].as_u64().expect("chunk count");

    let mut chunks = Vec::new();
    for ordinal in 0..chunk_count {
        let chunk = read_page(&fixture, &export_id, ordinal, "p_1").await;
        let again = read_page(&fixture, &export_id, ordinal, "p_1").await;
        assert_eq!(chunk, again, "an export page is stable across reads");
        chunks.push(chunk["chunk"].clone());
    }

    let reassembled = reassemble(&chunks);
    assert_eq!(
        hex_digest_excluding(&reassembled, "digest"),
        result["bundle_digest"].as_str().expect("bundle digest"),
        "the pages reassemble to the sealed bundle"
    );
    let total: u64 = chunks
        .iter()
        .map(|chunk| chunk["bytes"].as_u64().expect("bytes"))
        .sum();
    assert_eq!(result["total_bytes"], json!(total));

    let replay = create_export(&fixture, "export-key-1").await;
    assert_eq!(replay["result"]["export_id"], json!(export_id));
    let stored: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM memory_exports WHERE space_id = ?")
        .bind(SPACE)
        .fetch_one(&fixture.pool)
        .await
        .expect("count exports");
    assert_eq!(stored, 1);
}

async fn expire(fixture: &Fixture, export_id: &str) {
    sqlx::query("UPDATE memory_exports SET expires_at = ? WHERE export_id = ?")
        .bind("2000-01-01T00:00:00.000Z")
        .bind(export_id)
        .execute(&fixture.pool)
        .await
        .expect("expire the export");
}

#[tokio::test]
async fn a_read_past_the_retention_window_answers_expired() {
    let fixture = Fixture::open().await;
    let created = create_export(&fixture, "export-key-2").await;
    let export_id = created["result"]["export_id"]
        .as_str()
        .expect("export id")
        .to_string();
    expire(&fixture, &export_id).await;

    let adapter = crate::MemoryAdapter::new(false);
    let mut tx = fixture.begin().await;
    let result = adapter
        .get_export(
            &mut tx,
            &principal("p_1"),
            json!({ "space_id": SPACE, "export_id": export_id, "chunk_ordinal": 0 }),
        )
        .await;
    tx.rollback().await.expect("rollback read");
    assert_eq!(
        rejected(&result).code,
        spoke_operations::SpokeRejectCode::ExportExpired
    );
}

/// Retention reclaims an expired export's payload on the next successful transfer
/// write of its space, the way expired imports are reclaimed; the metadata row stays
/// so a later read still answers `EXPORT_EXPIRED`.
#[tokio::test]
async fn the_next_transfer_write_reclaims_an_expired_export_payload() {
    let fixture = Fixture::open().await;
    let expired = create_export(&fixture, "sweep-key-1").await;
    let expired_id = expired["result"]["export_id"]
        .as_str()
        .expect("export id")
        .to_string();
    expire(&fixture, &expired_id).await;
    assert!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_export_chunks WHERE export_id = ?",
            &[expired_id.as_str()],
        )
        .await
            > 0,
        "the expired export still holds its payload"
    );

    create_export(&fixture, "sweep-key-2").await;

    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_export_chunks WHERE export_id = ?",
            &[expired_id.as_str()],
        )
        .await,
        0,
        "the next transfer write released the expired payload"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_exports WHERE export_id = ?",
            &[expired_id.as_str()],
        )
        .await,
        1,
        "the metadata row stays so the expiry answer still resolves"
    );

    sqlx::query("UPDATE memory_exports SET expires_at = ? WHERE export_id = ?")
        .bind("2999-01-01T00:00:00.000Z")
        .bind(&expired_id)
        .execute(&fixture.pool)
        .await
        .expect("roll the clock back");
    let adapter = crate::MemoryAdapter::new(false);
    let mut tx = fixture.begin().await;
    let result = adapter
        .get_export(
            &mut tx,
            &principal("p_1"),
            json!({ "space_id": SPACE, "export_id": expired_id, "chunk_ordinal": 0 }),
        )
        .await;
    tx.rollback().await.expect("rollback read");
    assert_eq!(
        rejected(&result).code,
        spoke_operations::SpokeRejectCode::ExportExpired
    );
}

#[tokio::test]
async fn a_scope_the_caller_cannot_read_answers_absence_rather_than_expiry() {
    let fixture = Fixture::open().await;
    let created = create_export(&fixture, "export-key-3").await;
    let export_id = created["result"]["export_id"]
        .as_str()
        .expect("export id")
        .to_string();
    expire(&fixture, &export_id).await;

    let adapter = crate::MemoryAdapter::new(false);
    let mut tx = fixture.begin().await;
    let result = adapter
        .get_export(
            &mut tx,
            &principal("p_2"),
            json!({ "space_id": SPACE, "export_id": export_id, "chunk_ordinal": 0 }),
        )
        .await;
    tx.rollback().await.expect("rollback read");
    assert_eq!(
        rejected(&result).code,
        spoke_operations::SpokeRejectCode::NotFound
    );
}

/// Stage admission needs `admin` beside `transfer` on the target space.
async fn elevate(fixture: &Fixture) {
    sqlx::query("UPDATE memory_grants SET capabilities_json = ? WHERE grant_id = 'g_1'")
        .bind(r#"["admin","records_read","transfer"]"#)
        .execute(&fixture.pool)
        .await
        .expect("elevate the space-wide grant");
}

/// One element larger than the chunk limit travels alone, so the fixture's
/// export splits into a header chunk, the oversized record, and the trailing
/// arrays — three ordinals.
async fn seed_big_record(fixture: &Fixture) {
    let mut row = record("r_big", "s_1", None, false);
    if let RecordState::Live { content, .. } = &mut row.state {
        content.text = "x".repeat(300_000);
    }
    let mut tx = fixture.begin().await;
    fixture
        .repository
        .insert_record(&mut tx, &row)
        .await
        .expect("seed oversized record");
    tx.commit().await.expect("commit oversized record");
}

/// Stage one chunk as `p_1`, committing on success and rolling back a refusal so
/// the fixture observes exactly what the caller's transaction would.
async fn stage_chunk(
    fixture: &Fixture,
    chunk: Value,
    key: &str,
) -> spoke_operations::SpokeResult<crate::MemoryStage> {
    stage_into(fixture, SPACE, chunk, key, None).await
}

/// Stage one chunk into `space`; ordinal zero may carry extra header members
/// (adopted grants, reference translations).
async fn stage_into(
    fixture: &Fixture,
    space: &str,
    chunk: Value,
    key: &str,
    header: Option<Value>,
) -> spoke_operations::SpokeResult<crate::MemoryStage> {
    stage_as(fixture, space, "p_1", chunk, key, header).await
}

/// Stage one chunk as `as_principal`; a refusal rolls back so the fixture observes
/// exactly what that caller's transaction would.
async fn stage_as(
    fixture: &Fixture,
    space: &str,
    as_principal: &str,
    chunk: Value,
    key: &str,
    header: Option<Value>,
) -> spoke_operations::SpokeResult<crate::MemoryStage> {
    let adapter = crate::MemoryAdapter::new(false);
    let mut tx = fixture.begin().await;
    let ordinal = chunk["chunk_ordinal"].as_u64().expect("ordinal");
    let mut request = if ordinal == 0 {
        json!({
            "space_id": space,
            "chunk": chunk,
            // why: these stages import a space into itself, so the subject collision the
            // final chunk now decides is resolved by a rename rather than a refusal.
            "conflict_policy": "rename_subject",
            "idempotency_key": key,
        })
    } else {
        json!({ "space_id": space, "chunk": chunk, "idempotency_key": key })
    };
    if let Some(header) = header {
        let request = request.as_object_mut().expect("request object");
        for (member, value) in header.as_object().expect("header object") {
            request.insert(member.clone(), value.clone());
        }
    }
    let result = adapter
        .stage_import(&mut tx, &principal(as_principal), request)
        .await;
    if matches!(result, spoke_operations::SpokeResult::Ok(_)) {
        tx.commit().await.expect("commit stage");
    } else {
        tx.rollback().await.expect("rollback stage");
    }
    result
}

async fn export_chunks(fixture: &Fixture, export_id: &str) -> Vec<Value> {
    let first = read_page(fixture, export_id, 0, "p_1").await;
    let count = first["chunk"]["chunk_count"].as_u64().expect("chunk count");
    let mut chunks = vec![first["chunk"].clone()];
    for ordinal in 1..count {
        let page = read_page(fixture, export_id, ordinal, "p_1").await;
        chunks.push(page["chunk"].clone());
    }
    chunks
}

/// Create a whole-space export as `p_1` and return its wire chunks.
async fn staged_source(fixture: &Fixture, key: &str) -> Vec<Value> {
    let created = create_export(fixture, key).await;
    let export_id = created["result"]["export_id"]
        .as_str()
        .expect("export id")
        .to_string();
    export_chunks(fixture, &export_id).await
}

async fn import_state(fixture: &Fixture, import_id: &str) -> (String, i64) {
    sqlx::query_as("SELECT state, next_ordinal FROM memory_imports WHERE import_id = ?")
        .bind(import_id)
        .fetch_one(&fixture.pool)
        .await
        .expect("import row")
}

async fn staged_chunk_count(fixture: &Fixture, import_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM memory_import_chunks WHERE import_id = ?")
        .bind(import_id)
        .fetch_one(&fixture.pool)
        .await
        .expect("staged chunk count")
}

fn detail<'a>(reject: &'a spoke_operations::SpokeReject, key: &str) -> &'a Value {
    reject
        .details
        .as_ref()
        .expect("refusal details")
        .get(key)
        .expect("named detail")
}

#[tokio::test]
async fn a_valid_single_chunk_sequence_reaches_staged() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    let chunks = staged_source(&fixture, "stage-key-1").await;
    assert_eq!(chunks.len(), 1, "the fixture export is one chunk");

    let body = ready(stage_chunk(&fixture, chunks[0].clone(), "stage-1-0").await);
    assert_eq!(body["status"], json!("committed"));
    assert_eq!(body["result"]["chunks_received"], json!(1));
    let import_id = body["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();
    assert_eq!(
        import_state(&fixture, &import_id).await,
        ("staged".to_string(), 1)
    );
    assert_eq!(staged_chunk_count(&fixture, &import_id).await, 0);
    let report = &body["result"]["staged_report"];
    assert_eq!(report["counts"]["subjects"], json!(2));
    assert_eq!(report["counts"]["records"], json!(3));
    assert_eq!(report["verification"]["bundle_digest_ok"], json!(true));
    assert_eq!(
        report["subject_id_map"]
            .as_object()
            .expect("subject map")
            .len(),
        2,
        "the report fixes a target identity for every subject"
    );
    assert_eq!(
        report["record_id_map"]
            .as_object()
            .expect("record map")
            .len(),
        3,
        "every transferred record has a fixed identity"
    );
    let (payload, stored_report): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT staged_payload_json, staged_report_json FROM memory_imports WHERE import_id = ?",
    )
    .bind(&import_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("import row");
    assert!(payload.is_some(), "the translated rows are durable");
    assert!(stored_report.is_some(), "the identity report is durable");
}

/// The fixture's subjects carry no `kind`. The sealed bundle must omit the member
/// rather than seal `null`, because the typed client drops an absent optional on the
/// wire — the null would make its chunk disagree with the digest this store sealed.
#[tokio::test]
async fn a_kindless_subject_survives_the_typed_client_round_trip() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    let chunks = staged_source(&fixture, "kindless-export").await;
    assert_eq!(chunks.len(), 1, "the fixture export is one chunk");

    let sealed = chunks[0]["subjects"].as_array().expect("subjects");
    assert!(!sealed.is_empty());
    for subject in sealed {
        assert!(
            subject.get("kind").is_none(),
            "an absent kind is omitted, not null"
        );
    }
    let typed: spoke_schemas::MemoryExportChunk =
        serde_json::from_value(chunks[0].clone()).expect("chunk decodes against its schema");
    let sent = serde_json::to_value(typed).expect("chunk re-encodes");
    for subject in sent["subjects"].as_array().expect("subjects") {
        assert!(
            subject.get("kind").is_none(),
            "the typed client's wire shape agrees"
        );
    }

    let body = ready(stage_chunk(&fixture, sent, "kindless-0").await);
    assert_eq!(
        body["result"]["staged_report"]["counts"]["subjects"],
        json!(2)
    );
}

/// A stored document spells a present, null `structured` value as `"structured": null`. The pinned
/// content type carries no present null, so the typed seal would omit the member and the transfer
/// would carry a different document; the export refuses instead of changing it.
#[tokio::test]
async fn an_explicit_null_document_is_refused_at_export() {
    let fixture = Fixture::open().await;
    let mut row = record("r_null", "s_1", None, false);
    if let RecordState::Live { content, .. } = &mut row.state {
        content.structured = Some(Value::Null);
    }
    let mut tx = fixture.begin().await;
    fixture
        .repository
        .insert_record(&mut tx, &row)
        .await
        .expect("seed the null-structured record");
    tx.commit().await.expect("commit the seeded record");

    let adapter = crate::MemoryAdapter::new(false);
    let mut tx = fixture.begin().await;
    let result = adapter
        .create_export(
            &mut tx,
            &principal("p_1"),
            json!({ "space_id": SPACE, "idempotency_key": "null-content-export" }),
        )
        .await;
    tx.rollback().await.expect("rollback a refused export");
    let reject = rejected(&result);
    assert_eq!(reject.code, spoke_operations::SpokeRejectCode::InvalidInput);
    assert_eq!(
        detail(reject, "reason"),
        &json!("explicit_null_content_not_transferable"),
        "the refusal names what it could not carry: {reject:?}"
    );
}

/// A `null` nested inside `structured` is what the pinned content type carries unchanged, so the
/// export seals it and a receiving store re-imports the document. Only a present `null`
/// occupying `structured` itself is lossy.
#[tokio::test]
async fn a_nested_null_document_exports_and_reimports() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    target_space(&fixture).await;
    let structured = json!({ "product": [false, 0, null, []] });
    let mut row = record("r_nested", "s_1", None, false);
    if let RecordState::Live { content, .. } = &mut row.state {
        content.structured = Some(structured.clone());
    }
    let mut tx = fixture.begin().await;
    fixture
        .repository
        .insert_record(&mut tx, &row)
        .await
        .expect("seed the nested-null record");
    tx.commit().await.expect("commit the seeded record");

    let chunks = staged_source(&fixture, "nested-null-export").await;
    assert_eq!(chunks.len(), 1, "the fixture export is one chunk");
    let exported = chunks[0]["records"]
        .as_array()
        .expect("chunk records")
        .iter()
        .find(|record| record["record_id"] == json!("r_nested"))
        .expect("the nested-null record travels");
    assert_eq!(
        exported["content"]["structured"], structured,
        "the seal carries the nested null unchanged: {exported}"
    );

    let staged = ready(
        stage_into(
            &fixture,
            TARGET,
            chunks[0].clone(),
            "nested-null-stage-0",
            None,
        )
        .await,
    );
    assert_eq!(staged["status"], json!("committed"));
    let report = &staged["result"]["staged_report"];
    assert_eq!(report["counts"]["records"], json!(4));
    assert!(
        report["record_id_map"].get("r_nested").is_some(),
        "the nested-null document is re-imported, not refused: {report}"
    );
}

/// Defence in depth: a bundle whose bytes verify but whose content member spells an explicit null
/// is refused at final verification with the same reason, so no arrival can be activated as a
/// document the sender's seal changed.
#[tokio::test]
async fn a_bundle_carrying_an_explicit_null_is_refused_at_final_verification() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    target_space(&fixture).await;
    let mut tx = fixture.begin().await;
    let artifact = ok(
        build(
            &fixture.repository,
            &mut tx,
            &spec(None, PlacementSelection::SubjectAll),
        )
        .await,
        "build export",
    );
    tx.rollback().await.expect("rollback build");

    let mut bundle = artifact.bundle.clone();
    bundle["records"][0]["content"]["structured"] = Value::Null;
    reseal(&mut bundle);
    let chunks = sealed_partition(&bundle);
    let result = stage_into(
        &fixture,
        TARGET,
        chunks[0].clone(),
        "null-content-stage-0",
        None,
    )
    .await;
    let reject = rejected(&result);
    assert_eq!(
        detail(reject, "reason"),
        &json!("explicit_null_content_not_transferable"),
        "final verification refuses the document it cannot carry: {reject:?}"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_imports WHERE space_id = ?",
            &[TARGET],
        )
        .await,
        0,
        "a refused final chunk stages nothing"
    );
}

/// §7.0.5: the same key and payload replays the recorded outcome and writes
/// nothing; the same key with a different payload is a conflict at any age.
#[tokio::test]
async fn stage_import_replays_on_the_same_key_and_conflicts_on_a_new_digest() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    let chunks = staged_source(&fixture, "stage-key-replay").await;
    assert_eq!(chunks.len(), 1, "the fixture export is one chunk");

    let first = ready(stage_chunk(&fixture, chunks[0].clone(), "stage-replay").await);
    let import_id = first["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();
    let replay = ready(stage_chunk(&fixture, chunks[0].clone(), "stage-replay").await);
    assert_eq!(replay["status"], json!("committed"));
    assert_eq!(replay["result"], first["result"]);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_imports WHERE space_id = ?",
            &[SPACE]
        )
        .await,
        1,
        "a replay stages no second import"
    );
    assert_eq!(
        staged_chunk_count(&fixture, &import_id).await,
        0,
        "the final chunk already released the raw sequence"
    );

    let changed = stage_into(
        &fixture,
        SPACE,
        chunks[0].clone(),
        "stage-replay",
        Some(json!({ "conflict_policy": "fail" })),
    )
    .await;
    let reject = rejected(&changed);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::IdempotencyConflict
    );
    assert_eq!(staged_chunk_count(&fixture, &import_id).await, 0);
}

#[tokio::test]
async fn repeated_ordinals_conflict_without_ending_the_import() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    seed_big_record(&fixture).await;
    let chunks = staged_source(&fixture, "stage-key-2").await;
    assert_eq!(chunks.len(), 3, "the oversized record splits the export");

    let body = ready(stage_chunk(&fixture, chunks[0].clone(), "stage-2-0").await);
    let import_id = body["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();

    let repeated_header = stage_chunk(&fixture, chunks[0].clone(), "stage-2-0b").await;
    assert_eq!(
        detail(rejected(&repeated_header), "reason"),
        &json!("duplicate_header")
    );
    assert_eq!(staged_chunk_count(&fixture, &import_id).await, 1);

    ready(stage_chunk(&fixture, chunks[1].clone(), "stage-2-1").await);
    let repeated_tail = stage_chunk(&fixture, chunks[1].clone(), "stage-2-1b").await;
    let reject = rejected(&repeated_tail);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "reason"), &json!("out_of_order"));
    assert_eq!(staged_chunk_count(&fixture, &import_id).await, 2);
    assert_eq!(
        import_state(&fixture, &import_id).await,
        ("staging".to_string(), 2)
    );

    let body = ready(stage_chunk(&fixture, chunks[2].clone(), "stage-2-2b").await);
    assert_eq!(body["result"]["chunks_received"], json!(3));
    assert_eq!(
        import_state(&fixture, &import_id).await,
        ("staged".to_string(), 3)
    );
}

/// A repeated header names the ordinal it refused, so a new idempotency key can tell a
/// header collision from a chunk rejection and learn where the live import stands.
#[tokio::test]
async fn a_repeated_header_names_the_ordinal_it_refused() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    seed_big_record(&fixture).await;
    let chunks = staged_source(&fixture, "ordinal-source").await;
    assert_eq!(chunks.len(), 3, "the oversized record splits the export");
    let body = ready(stage_chunk(&fixture, chunks[0].clone(), "ordinal-0").await);
    let import_id = body["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();

    let repeated = stage_chunk(&fixture, chunks[0].clone(), "ordinal-0b").await;
    let reject = rejected(&repeated);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "reason"), &json!("duplicate_header"));
    assert_eq!(detail(reject, "received_ordinal"), &json!(0));
    assert_eq!(detail(reject, "expected_ordinal"), &json!(1));
    assert_eq!(
        import_state(&fixture, &import_id).await,
        ("staging".to_string(), 1),
        "the refusal writes nothing"
    );
    assert_eq!(staged_chunk_count(&fixture, &import_id).await, 1);
}

/// `next_ordinal` alone decides what is acceptable, so an ordinal ahead of the import
/// is the same refusal as a repeat: nothing is written, nothing is deleted, and the
/// sequence still completes once the right chunk arrives.
#[tokio::test]
async fn an_ordinal_ahead_of_the_import_is_refused_and_the_sequence_still_completes() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    seed_big_record(&fixture).await;
    let chunks = staged_source(&fixture, "stage-key-gap").await;
    assert_eq!(chunks.len(), 3, "the oversized record splits the export");

    let body = ready(stage_chunk(&fixture, chunks[0].clone(), "gap-0").await);
    let import_id = body["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();

    let gap = stage_chunk(&fixture, chunks[2].clone(), "gap-2").await;
    let reject = rejected(&gap);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "reason"), &json!("out_of_order"));
    assert_eq!(detail(reject, "expected_ordinal"), &json!(1));
    assert_eq!(detail(reject, "received_ordinal"), &json!(2));
    assert_eq!(
        import_state(&fixture, &import_id).await,
        ("staging".to_string(), 1),
        "the refusal writes nothing and leaves next_ordinal where it was"
    );
    assert_eq!(
        staged_chunk_count(&fixture, &import_id).await,
        1,
        "the refusal deletes nothing"
    );

    let repeated = stage_chunk(&fixture, chunks[2].clone(), "gap-2b").await;
    assert_eq!(
        detail(rejected(&repeated), "reason"),
        &json!("out_of_order")
    );

    ready(stage_chunk(&fixture, chunks[1].clone(), "gap-1").await);
    let body = ready(stage_chunk(&fixture, chunks[2].clone(), "gap-2c").await);
    assert_eq!(body["result"]["chunks_received"], json!(3));
    assert_eq!(
        import_state(&fixture, &import_id).await,
        ("staged".to_string(), 3)
    );
}

/// A final chunk altered and re-sealed: its own digest verifies, so only the
/// whole-sequence verification can catch it, and nothing further is staged.
#[tokio::test]
async fn a_corrupt_final_chunk_conflicts_and_leaves_the_import_staging() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    seed_big_record(&fixture).await;
    let mut chunks = staged_source(&fixture, "stage-key-3").await;
    assert_eq!(chunks.len(), 3, "the oversized record splits the export");

    let body = ready(stage_chunk(&fixture, chunks[0].clone(), "stage-3-0").await);
    let import_id = body["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();
    ready(stage_chunk(&fixture, chunks[1].clone(), "stage-3-1").await);

    let last = chunks.last_mut().expect("a final chunk");
    let profile = last["profiles"]
        .as_array_mut()
        .and_then(|profiles| profiles.first_mut())
        .expect("the final chunk carries the profile");
    profile["document"]["body"] = json!("tampered");
    last["bytes"] = json!(spoke_operations::content_serialized_bytes(last));
    let digest = hex_digest_excluding(last, "chunk_digest");
    last["chunk_digest"] = json!(digest);

    let result = stage_chunk(&fixture, chunks[2].clone(), "stage-3-2").await;
    let reject = rejected(&result);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "reason"), &json!("verification"));
    assert!(!detail(reject, "errors")
        .as_array()
        .expect("errors")
        .is_empty());
    assert_eq!(
        import_state(&fixture, &import_id).await,
        ("staging".to_string(), 2)
    );
    assert_eq!(staged_chunk_count(&fixture, &import_id).await, 2);
}

/// An expired import keeps no payload and no staging lookup key: the next stage of the
/// same source export reclaims it — a committed success, not a refusal — and starts a
/// fresh sequence, while a late chunk of the dead import is still refused.
#[tokio::test]
async fn an_expired_import_is_reclaimed_by_the_next_stage_of_its_source_export() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    seed_big_record(&fixture).await;
    let chunks = staged_source(&fixture, "stage-key-4").await;
    assert_eq!(chunks.len(), 3, "the oversized record splits the export");

    let body = ready(stage_chunk(&fixture, chunks[0].clone(), "stage-4-0").await);
    let import_id = body["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();
    sqlx::query("UPDATE memory_imports SET expires_at = ? WHERE import_id = ?")
        .bind("2000-01-01T00:00:00.000Z")
        .bind(&import_id)
        .execute(&fixture.pool)
        .await
        .expect("expire the import");

    let result = stage_chunk(&fixture, chunks[1].clone(), "stage-4-1").await;
    let reject = rejected(&result);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "reason"), &json!("expired"));
    assert_eq!(
        import_state(&fixture, &import_id).await,
        ("staging".to_string(), 1)
    );
    assert_eq!(staged_chunk_count(&fixture, &import_id).await, 1);

    let restarted = ready(stage_chunk(&fixture, chunks[0].clone(), "stage-4-0b").await);
    let restarted_id = restarted["result"]["import_id"]
        .as_str()
        .expect("restarted import id")
        .to_string();
    assert_ne!(restarted_id, import_id);
    let (state, payload, report): (String, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT state, staged_payload_json, staged_report_json \
         FROM memory_imports WHERE import_id = ?",
    )
    .bind(&import_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("reclaimed import row");
    assert_eq!(
        state, "expired",
        "the dead import keeps a content-free marker"
    );
    assert!(payload.is_none(), "the expired payload is released");
    assert!(report.is_none());
    assert_eq!(
        staged_chunk_count(&fixture, &import_id).await,
        0,
        "the expired chunks are released"
    );
    assert_eq!(
        import_state(&fixture, &restarted_id).await,
        ("staging".to_string(), 1)
    );
}

/// An abandoned import is reclaimed by any later successful transfer write of its space,
/// not only by a stage of the same source export: the expired row keeps no payload, no
/// report and no chunks — and it still names its own expiry. A late chunk of the dead
/// import is refused as expired, not as a forgotten import, while a fresh header for the
/// same source export still starts a new sequence.
#[tokio::test]
async fn an_unrelated_reclaim_names_the_dead_import_and_frees_its_source_export() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    seed_big_record(&fixture).await;
    let chunks = staged_source(&fixture, "sweep-source").await;
    assert_eq!(chunks.len(), 3, "the oversized record splits the export");
    let mut import_id = String::new();
    for (ordinal, chunk) in chunks.iter().enumerate() {
        let body = ready(stage_chunk(&fixture, chunk.clone(), &format!("sweep-{ordinal}")).await);
        import_id = body["result"]["import_id"]
            .as_str()
            .expect("import id")
            .to_string();
    }
    assert_eq!(
        import_state(&fixture, &import_id).await,
        ("staged".to_string(), 3),
        "the abandoned import froze its payload"
    );
    sqlx::query("UPDATE memory_imports SET expires_at = ? WHERE import_id = ?")
        .bind("2000-01-01T00:00:00.000Z")
        .bind(&import_id)
        .execute(&fixture.pool)
        .await
        .expect("expire the import");

    create_export(&fixture, "sweep-other").await;

    let (state, payload, report): (String, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT state, staged_payload_json, staged_report_json \
         FROM memory_imports WHERE import_id = ?",
    )
    .bind(&import_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("reclaimed import row");
    assert_eq!(
        state, "expired",
        "the dead import keeps a content-free marker"
    );
    assert!(payload.is_none(), "the expired payload is released");
    assert!(report.is_none(), "the expired report is released");
    assert_eq!(
        staged_chunk_count(&fixture, &import_id).await,
        0,
        "the expired chunks are released"
    );

    let late = stage_chunk(&fixture, chunks[1].clone(), "sweep-late").await;
    let reject = rejected(&late);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "reason"), &json!("expired"));
    assert_eq!(detail(reject, "import_id"), &json!(import_id));
    assert_eq!(
        detail(reject, "expires_at"),
        &json!("2000-01-01T00:00:00.000Z")
    );

    let restarted = ready(stage_chunk(&fixture, chunks[0].clone(), "sweep-0b").await);
    let restarted_id = restarted["result"]["import_id"]
        .as_str()
        .expect("restarted import id")
        .to_string();
    assert_ne!(restarted_id, import_id);
    assert_eq!(
        import_state(&fixture, &restarted_id).await,
        ("staging".to_string(), 1)
    );
    assert_eq!(
        import_state(&fixture, &import_id).await,
        ("expired".to_string(), 3),
        "the dead import stays a content-free marker"
    );
}

/// A reclaimed import is decided by its persisted `expired` state, not the clock: a
/// backward clock step that leaves `expires_at` in the future must neither revive the
/// dead header as a live one nor accept its next chunk.
#[tokio::test]
async fn a_reclaimed_import_stays_dead_after_the_clock_moves_back() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    seed_big_record(&fixture).await;
    let chunks = staged_source(&fixture, "clock-source").await;
    assert_eq!(chunks.len(), 3, "the oversized record splits the export");

    let body = ready(stage_chunk(&fixture, chunks[0].clone(), "clock-0").await);
    let import_id = body["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();

    sqlx::query("UPDATE memory_imports SET expires_at = ? WHERE import_id = ?")
        .bind("2000-01-01T00:00:00.000Z")
        .bind(&import_id)
        .execute(&fixture.pool)
        .await
        .expect("expire the import");
    create_export(&fixture, "clock-other").await;
    assert_eq!(
        import_state(&fixture, &import_id).await,
        ("expired".to_string(), 1),
        "retention reclaimed the abandoned import"
    );
    sqlx::query("UPDATE memory_imports SET expires_at = ? WHERE import_id = ?")
        .bind("2999-01-01T00:00:00.000Z")
        .bind(&import_id)
        .execute(&fixture.pool)
        .await
        .expect("move the expiry past the clock");

    let late = stage_chunk(&fixture, chunks[1].clone(), "clock-1").await;
    let reject = rejected(&late);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "reason"), &json!("expired"));
    assert_eq!(detail(reject, "import_id"), &json!(import_id));
    assert_eq!(
        detail(reject, "expires_at"),
        &json!("2999-01-01T00:00:00.000Z")
    );
    assert_eq!(
        staged_chunk_count(&fixture, &import_id).await,
        0,
        "the refusal writes no chunk into the dead import"
    );

    let restarted = ready(stage_chunk(&fixture, chunks[0].clone(), "clock-0b").await);
    let restarted_id = restarted["result"]["import_id"]
        .as_str()
        .expect("restarted import id")
        .to_string();
    assert_ne!(restarted_id, import_id);
    assert_eq!(
        import_state(&fixture, &restarted_id).await,
        ("staging".to_string(), 1)
    );
}

/// `activated` is terminal, so the deadline its staging ran against no longer decides
/// anything: a repeat activation past it names the state the import has left, not `expired`.
#[tokio::test]
async fn an_activated_import_past_its_old_deadline_answers_its_state() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    target_space(&fixture).await;
    let chunks = staged_source(&fixture, "terminal-export-1").await;
    let staged = ready(
        stage_into(
            &fixture,
            TARGET,
            chunks[0].clone(),
            "terminal-stage-0",
            None,
        )
        .await,
    );
    let import_id = staged["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();
    ready(activate(&fixture, &import_id, "terminal-activate-1").await);
    sqlx::query("UPDATE memory_imports SET expires_at = ? WHERE import_id = ?")
        .bind("2000-01-01T00:00:00.000Z")
        .bind(&import_id)
        .execute(&fixture.pool)
        .await
        .expect("backdate the activated import");

    let again = activate(&fixture, &import_id, "terminal-activate-2").await;
    let reject = rejected(&again);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "reason"), &json!("import_state"));
    assert_eq!(detail(reject, "state"), &json!("activated"));
}

/// One live import per source export: once the first reaches `staged`, a second
/// idempotency key cannot open a second import that would activate the same export twice.
#[tokio::test]
async fn a_staged_import_holds_its_source_export_against_a_second_key() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    let chunks = staged_source(&fixture, "one-live-export").await;
    assert_eq!(chunks.len(), 1, "the fixture export is one chunk");

    let first = ready(stage_chunk(&fixture, chunks[0].clone(), "live-0").await);
    let import_id = first["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();
    assert_eq!(
        import_state(&fixture, &import_id).await,
        ("staged".to_string(), 1)
    );

    let second = stage_chunk(&fixture, chunks[0].clone(), "live-0b").await;
    let reject = rejected(&second);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "reason"), &json!("duplicate_header"));
    assert_eq!(detail(reject, "import_id"), &json!(import_id));
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_imports WHERE space_id = ?",
            &[SPACE],
        )
        .await,
        1,
        "a second key opened no second import"
    );
}

/// A local export the caller cannot page and an export this store never held are
/// indistinguishable at staging: both are the caller's own durable state, and both
/// stage the chunks the caller supplies. Refusing only the first would let a guessed
/// export id probe which exports exist.
#[tokio::test]
async fn an_unreadable_local_source_stages_exactly_like_a_foreign_export() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    target_space(&fixture).await;
    {
        let mut tx = fixture.begin().await;
        let mut row = grant("g_target_p2", None, "p_2", &[], false, false);
        row.space_id = SpaceId::new(TARGET);
        row.capabilities = vec![
            "admin".to_string(),
            "records_read".to_string(),
            "transfer".to_string(),
        ];
        fixture
            .repository
            .insert_grant(&mut tx, &row)
            .await
            .expect("seed p_2 target grant");
        tx.commit().await.expect("commit p_2 target grant");
    }
    let local = staged_source(&fixture, "stage-key-read").await;
    assert_eq!(local.len(), 1, "the fixture export is one chunk");

    let mut foreign_spec = spec(None, PlacementSelection::SubjectAll);
    foreign_spec.export_id = "exp_never_held".to_string();
    let mut tx = fixture.begin().await;
    let foreign = ok(
        build(&fixture.repository, &mut tx, &foreign_spec).await,
        "build foreign export",
    );
    tx.rollback().await.expect("rollback build");
    assert_eq!(foreign.chunks.len(), 1);

    let unreadable =
        ready(stage_as(&fixture, TARGET, "p_2", local[0].clone(), "read-0", None).await);
    let absent = ready(
        stage_as(
            &fixture,
            TARGET,
            "p_2",
            foreign.chunks[0].clone(),
            "read-0f",
            None,
        )
        .await,
    );
    assert_eq!(unreadable["result"]["chunks_received"], json!(1));
    assert_eq!(absent["result"]["chunks_received"], json!(1));
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_imports WHERE space_id = ?",
            &[TARGET],
        )
        .await,
        2,
        "neither answer discloses whether the named export exists locally"
    );
}

fn reseal(bundle: &mut Value) {
    let digest = hex_digest_excluding(bundle, "digest");
    bundle
        .as_object_mut()
        .expect("bundle")
        .insert("digest".to_string(), json!(digest));
}

fn sealed_partition(bundle: &Value) -> Vec<Value> {
    ok(
        super::chunk::partition(bundle, sealed_digest(bundle), policy::EXPORT_CHUNK_BYTES),
        "partition",
    )
}

#[tokio::test]
async fn sequence_verification_rejects_versions_references_and_adoptions() {
    let fixture = Fixture::open().await;
    let mut tx = fixture.begin().await;
    let artifact = ok(
        build(
            &fixture.repository,
            &mut tx,
            &spec(None, PlacementSelection::SubjectAll),
        )
        .await,
        "build export",
    );
    tx.rollback().await.expect("rollback build");

    let verify = |bundle: &Value, adopted: Value| {
        let chunks = sealed_partition(bundle);
        let digest = bundle["digest"].as_str().expect("digest");
        super::stage::verification_errors(&chunks, chunks.len() as u64, digest, "exp_1", &adopted)
    };
    assert!(
        verify(&artifact.bundle, json!([])).is_empty(),
        "the export's own chunks verify"
    );

    let mut versioned = artifact.bundle.clone();
    versioned["bundle_format_version"] = json!(2);
    reseal(&mut versioned);
    let errors = verify(&versioned, json!([]));
    assert!(
        errors
            .iter()
            .any(|error| error.contains("bundle_format_version")),
        "{errors:?}"
    );

    let mut dangling = artifact.bundle.clone();
    let consumed = dangling["candidates"]
        .as_array_mut()
        .expect("candidates")
        .iter_mut()
        .find(|candidate| candidate.get("consumed_by").is_some())
        .expect("a consumed candidate");
    consumed["consumed_by"]["record_ids"] = json!(["r_missing"]);
    reseal(&mut dangling);
    let errors = verify(&dangling, json!([]));
    assert!(
        errors
            .iter()
            .any(|error| error.contains("record r_missing")),
        "{errors:?}"
    );

    let adopted = json!([{ "grant_id": "g_missing", "principal_id": "p_9" }]);
    let errors = verify(&artifact.bundle, adopted);
    assert!(
        errors
            .iter()
            .any(|error| error.contains("adopted grant g_missing")),
        "{errors:?}"
    );
}

/// Duplicate identity keys would pass verification and then break activation's
/// mapping, so final verification refuses them like any other reference error.
#[tokio::test]
async fn duplicate_subject_record_or_candidate_ids_fail_final_verification() {
    let fixture = Fixture::open().await;
    let mut tx = fixture.begin().await;
    let artifact = ok(
        build(
            &fixture.repository,
            &mut tx,
            &spec(None, PlacementSelection::SubjectAll),
        )
        .await,
        "build export",
    );
    tx.rollback().await.expect("rollback build");

    for (array, noun) in [
        ("subjects", "subject"),
        ("records", "record"),
        ("candidates", "candidate"),
    ] {
        let mut bundle = artifact.bundle.clone();
        let first = bundle[array]
            .as_array()
            .and_then(|items| items.first())
            .expect("the fixture exports the array")
            .clone();
        bundle[array].as_array_mut().expect("array").push(first);
        reseal(&mut bundle);
        let chunks = sealed_partition(&bundle);
        let errors = super::stage::verification_errors(
            &chunks,
            chunks.len() as u64,
            sealed_digest(&bundle),
            "exp_1",
            &json!([]),
        );
        assert!(
            errors
                .iter()
                .any(|error| error.contains(noun) && error.contains("more than once")),
            "{array}: {errors:?}"
        );
    }
}

/// `GrantSummary.principal_ref` is an opaque correlation label. A value that carries
/// key material — here a bearer token — is refused by final verification without being
/// echoed; a foreign exporter's own opaque spelling is accepted, because this crate's
/// `prl_…` grammar is its own convention, not a protocol rule.
#[tokio::test]
async fn a_grant_summary_principal_ref_is_screened_for_credentials_not_for_our_grammar() {
    let fixture = Fixture::open().await;
    let mut tx = fixture.begin().await;
    let artifact = ok(
        build(
            &fixture.repository,
            &mut tx,
            &spec(None, PlacementSelection::SubjectAll),
        )
        .await,
        "build export",
    );
    tx.rollback().await.expect("rollback build");

    let mut bundle = artifact.bundle.clone();
    let grant = bundle["grants"]
        .as_array_mut()
        .and_then(|grants| grants.first_mut())
        .expect("the fixture exports a grant summary");
    let label = grant["principal_ref"].as_str().expect("principal ref");
    assert!(label.starts_with("prl_"), "the export labels the principal");

    grant["principal_ref"] = json!("prn_label_9f2c");
    reseal(&mut bundle);
    let chunks = sealed_partition(&bundle);
    let errors = super::stage::verification_errors(
        &chunks,
        chunks.len() as u64,
        sealed_digest(&bundle),
        "exp_1",
        &json!([]),
    );
    assert!(errors.is_empty(), "{errors:?}");

    let token = "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwXzEifQ";
    bundle["grants"][0]["principal_ref"] = json!(token);
    reseal(&mut bundle);
    let chunks = sealed_partition(&bundle);
    let errors = super::stage::verification_errors(
        &chunks,
        chunks.len() as u64,
        sealed_digest(&bundle),
        "exp_1",
        &json!([]),
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("not an opaque correlation label")),
        "{errors:?}"
    );
    assert!(
        !errors.iter().any(|error| error.contains(token)),
        "the refusal never echoes the value it refused: {errors:?}"
    );
}

/// The screen refuses a credential's whole shape, not a bare prefix: a short foreign
/// label that happens to start with a known key prefix stays an opaque label, while a
/// value carrying the provider's token length and alphabet is refused. Every JWT
/// compact serialisation is refused even though a dotless `eyJ…` label is not.
#[tokio::test]
async fn the_credential_screen_keeps_plain_prefixed_labels() {
    let fixture = Fixture::open().await;
    let mut tx = fixture.begin().await;
    let artifact = ok(
        build(
            &fixture.repository,
            &mut tx,
            &spec(None, PlacementSelection::SubjectAll),
        )
        .await,
        "build export",
    );
    tx.rollback().await.expect("rollback build");

    let screen = |label: &str| {
        let mut bundle = artifact.bundle.clone();
        bundle["grants"][0]["principal_ref"] = json!(label);
        reseal(&mut bundle);
        let chunks = sealed_partition(&bundle);
        super::stage::verification_errors(
            &chunks,
            chunks.len() as u64,
            sealed_digest(&bundle),
            "exp_1",
            &json!([]),
        )
    };

    for label in [
        "sk_invoice_42",
        "ghp_batch_7",
        "pk_project_1",
        "xoxb-staging",
        // A dotless `eyJ` value is a prefix, not a JWT's segment structure.
        "eyJreport_42",
        // Three segments are not enough either: a JWT's header segment decodes to a JSON object,
        // and this versioned label's does not.
        "eyJreport_42.v1",
    ] {
        let errors = screen(label);
        assert!(
            errors.is_empty(),
            "a plain opaque label {label} is not a credential: {errors:?}"
        );
    }
    // Every compact serialisation of a JWT, built here so the fixture source never carries a
    // token-shaped literal: a JWS, the same with an empty signature, a stripped signature, and a
    // JWE.
    let header = "eyJhbGciOiJIUzI1NiJ9";
    let claims = "eyJzdWIiOiIxIn0";
    for label in [
        format!("{header}.{claims}.{}", "c2lnbmF0dXJl"),
        format!("{header}.{claims}."),
        format!("{header}.{claims}"),
        format!("{header}.{claims}.a.b.c"),
    ] {
        let errors = screen(&label);
        assert!(
            errors
                .iter()
                .any(|error| error.contains("not an opaque correlation label")),
            "{label} is a JWT: {errors:?}"
        );
    }
    // Prefix plus the provider's own token length and alphabet, built here so the
    // fixture source never carries a credential-shaped literal.
    let shaped = [
        format!("sk_live_{}", "9fK2mQ8xT4vR7bN3cL6pW1sD"),
        format!("ghp_{}", "aB3dE6gH9jK2mN5pQ8sT1vW4yZ7bC0eF3hJ6"),
        format!("AKIA{}", "Q7XK4M9P2R5T8V3W"),
    ];
    for label in &shaped {
        let errors = screen(label);
        assert!(
            errors
                .iter()
                .any(|error| error.contains("not an opaque correlation label")),
            "{label} carries key material: {errors:?}"
        );
    }
}

/// The wire header omits an empty `extensions`; reassembly restores `{}`, so a
/// header without the member still decodes as a `MemoryExportBundle`.
#[tokio::test]
async fn a_header_that_omits_empty_extensions_still_reassembles() {
    let fixture = Fixture::open().await;
    let mut tx = fixture.begin().await;
    let artifact = ok(
        build(
            &fixture.repository,
            &mut tx,
            &spec(None, PlacementSelection::SubjectAll),
        )
        .await,
        "build export",
    );
    tx.rollback().await.expect("rollback build");

    let mut chunks = sealed_partition(&artifact.bundle);
    chunks[0]["header"]
        .as_object_mut()
        .expect("ordinal zero carries the header")
        .remove("extensions");
    let bytes = spoke_operations::content_serialized_bytes(&chunks[0]);
    chunks[0]["bytes"] = json!(bytes);
    let chunk_digest = hex_digest_excluding(&chunks[0], "chunk_digest");
    chunks[0]["chunk_digest"] = json!(chunk_digest);

    let errors = super::stage::verification_errors(
        &chunks,
        chunks.len() as u64,
        artifact.digest.as_str(),
        "exp_1",
        &json!([]),
    );
    assert!(errors.is_empty(), "{errors:?}");
}

const TARGET: &str = "spc_target";

/// A second, empty space where `p_1` holds `transfer` and `admin`.
async fn target_space(fixture: &Fixture) {
    let mut tx = fixture.begin().await;
    sqlx::query(
        "INSERT INTO memory_spaces \
         (space_id, display_name, authority_host_id, authority_generation, status, revision, \
          created_at) VALUES (?, 'target', 'host_1', 1, 'active', 1, ?)",
    )
    .bind(TARGET)
    .bind(NOW)
    .execute(&mut *tx)
    .await
    .expect("seed target space");
    let mut row = grant("g_target", None, "p_1", &[], false, false);
    row.space_id = SpaceId::new(TARGET);
    row.capabilities = vec![
        "admin".to_string(),
        "records_read".to_string(),
        "transfer".to_string(),
    ];
    fixture
        .repository
        .insert_grant(&mut tx, &row)
        .await
        .expect("seed target grant");
    tx.commit().await.expect("commit target space");
}

async fn count(fixture: &Fixture, sql: &'static str, binds: &[&str]) -> i64 {
    let mut query = sqlx::query_scalar::<_, i64>(sql);
    for bind in binds {
        query = query.bind(*bind);
    }
    query.fetch_one(&fixture.pool).await.expect("count")
}

async fn activate(
    fixture: &Fixture,
    import_id: &str,
    key: &str,
) -> spoke_operations::SpokeResult<crate::MemoryStage> {
    run_import(fixture, TARGET, import_id, key, true).await
}

async fn discard(
    fixture: &Fixture,
    import_id: &str,
    key: &str,
) -> spoke_operations::SpokeResult<crate::MemoryStage> {
    run_import(fixture, TARGET, import_id, key, false).await
}

async fn run_import(
    fixture: &Fixture,
    space: &str,
    import_id: &str,
    key: &str,
    activation: bool,
) -> spoke_operations::SpokeResult<crate::MemoryStage> {
    let adapter = crate::MemoryAdapter::new(false);
    let mut tx = fixture.begin().await;
    let request = json!({ "space_id": space, "import_id": import_id, "idempotency_key": key });
    let result = if activation {
        adapter
            .activate_import(&mut tx, &principal("p_1"), request)
            .await
    } else {
        adapter
            .discard_import(&mut tx, &principal("p_1"), request)
            .await
    };
    if matches!(result, spoke_operations::SpokeResult::Ok(_)) {
        tx.commit().await.expect("commit import mutation");
    } else {
        tx.rollback().await.expect("rollback import mutation");
    }
    result
}

#[tokio::test]
async fn activation_applies_the_staged_bundle_exactly_once() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    target_space(&fixture).await;
    let chunks = staged_source(&fixture, "activate-export-1").await;
    assert_eq!(chunks.len(), 1, "the fixture export is one chunk");
    let staged = ready(
        stage_into(
            &fixture,
            TARGET,
            chunks[0].clone(),
            "activate-stage-0",
            None,
        )
        .await,
    );
    let import_id = staged["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();

    {
        let adapter = crate::MemoryAdapter::new(false);
        let mut tx = fixture.begin().await;
        ready(
            adapter
                .activate_import(
                    &mut tx,
                    &principal("p_1"),
                    json!({ "space_id": TARGET, "import_id": import_id, "idempotency_key": "activate-1" }),
                )
                .await,
        );
        tx.rollback().await.expect("rollback activation");
    }
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_records WHERE space_id = ?",
            &[TARGET]
        )
        .await,
        0,
        "a rolled-back activation applies nothing"
    );
    assert_eq!(
        import_state(&fixture, &import_id).await,
        ("staged".to_string(), 1)
    );

    let body = ready(activate(&fixture, &import_id, "activate-1").await);
    assert_eq!(body["status"], json!("committed"));
    let result = &body["result"];
    assert_eq!(
        result["activated"],
        json!({
            "candidates": 2,
            "profiles": 1,
            "receipt_summaries": 1,
            "records": 3,
            "reflections": 0,
            "subjects": 2,
        })
    );
    assert_eq!(result["authority_generation"], json!(2));
    assert_eq!(result["import_id"], json!(import_id));

    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_records WHERE space_id = ?",
            &[TARGET]
        )
        .await,
        3
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_candidates WHERE space_id = ?",
            &[TARGET]
        )
        .await,
        2
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_subjects WHERE space_id = ?",
            &[TARGET]
        )
        .await,
        2
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_profiles WHERE space_id = ?",
            &[TARGET]
        )
        .await,
        1
    );
    let (generation, revision): (i64, i64) = sqlx::query_as(
        "SELECT authority_generation, revision FROM memory_spaces WHERE space_id = ?",
    )
    .bind(TARGET)
    .fetch_one(&fixture.pool)
    .await
    .expect("space row");
    assert_eq!((generation, revision), (2, 2));

    let row: (String, Option<String>, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT state, activated_at, staged_payload_json, staged_report_json \
         FROM memory_imports WHERE import_id = ?",
    )
    .bind(&import_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("import row");
    assert_eq!(row.0, "activated");
    assert!(row.1.is_some(), "activation stamps activated_at");
    assert!(
        row.2.is_none(),
        "the content-bearing payload it applied is released"
    );
    assert!(
        row.3.is_some(),
        "the identity report stays as content-free provenance"
    );
    assert_eq!(staged_chunk_count(&fixture, &import_id).await, 0);

    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_records WHERE space_id = ? \
             AND json_extract(import_ref_json, '$.import_id') = ?",
            &[TARGET, &import_id],
        )
        .await,
        3
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_candidates c, \
             json_each(json_extract(c.consumed_by_json, '$.record_ids')) j \
             WHERE c.space_id = ? AND NOT EXISTS (SELECT 1 FROM memory_records r \
               WHERE r.space_id = c.space_id AND r.record_id = j.value)",
            &[TARGET],
        )
        .await,
        0
    );

    let replay = ready(activate(&fixture, &import_id, "activate-1").await);
    assert_eq!(replay["result"], *result);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_records WHERE space_id = ?",
            &[TARGET]
        )
        .await,
        3
    );

    let again = activate(&fixture, &import_id, "activate-2").await;
    let reject = rejected(&again);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "state"), &json!("activated"));
}

#[tokio::test]
async fn discard_deletes_staging_and_refuses_an_activated_import() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    target_space(&fixture).await;

    let chunks = staged_source(&fixture, "discard-export-1").await;
    let staged =
        ready(stage_into(&fixture, TARGET, chunks[0].clone(), "discard-stage-0", None).await);
    let import_id = staged["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();

    let body = ready(discard(&fixture, &import_id, "discard-1").await);
    assert_eq!(
        body["result"],
        json!({ "discarded": true, "import_id": import_id })
    );
    assert_eq!(
        import_state(&fixture, &import_id).await,
        ("discarded".to_string(), 1)
    );
    assert_eq!(staged_chunk_count(&fixture, &import_id).await, 0);
    let payload: Option<String> =
        sqlx::query_scalar("SELECT staged_payload_json FROM memory_imports WHERE import_id = ?")
            .bind(&import_id)
            .fetch_one(&fixture.pool)
            .await
            .expect("discarded import row");
    assert!(
        payload.is_none(),
        "a discarded import keeps no content-bearing payload"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_records WHERE space_id = ?",
            &[TARGET]
        )
        .await,
        0,
        "discarding applies nothing"
    );

    let again = discard(&fixture, &import_id, "discard-2").await;
    let reject = rejected(&again);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "state"), &json!("discarded"));

    let chunks = staged_source(&fixture, "discard-export-2").await;
    let staged =
        ready(stage_into(&fixture, TARGET, chunks[0].clone(), "discard-stage-1", None).await);
    let activated_id = staged["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();
    ready(activate(&fixture, &activated_id, "discard-activate-1").await);

    let refused = discard(&fixture, &activated_id, "discard-3").await;
    let reject = rejected(&refused);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "state"), &json!("activated"));
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_records WHERE space_id = ?",
            &[TARGET]
        )
        .await,
        3,
        "a refused discard leaves the activated rows alone"
    );
}

/// A belief record whose typed references can be translated: one external
/// carrier reference and one reference to an imported record.
fn translated_belief() -> Value {
    let reference = |kind: &str, namespace: &str, id: &str| json!({ "kind": kind, "namespace": namespace, "id": id });
    json!({
        "kind": "belief",
        "order": 1,
        "record_id": "belief_tr",
        "holder": reference("knowledge_entry", "fixture", "ke_mira"),
        "target": {
            "carrier": reference("external", "src_realm", "ext_1"),
            "target_id": "prop_harbor_closed",
            "kind": "proposition"
        },
        "assessor": reference("knowledge_entry", "fixture", "ke_mira"),
        "assessor_role": "holder_self_report",
        "basis": {
            "kind": "evidence",
            "evidence": [reference("memory_record", "fixture", "r_1")]
        },
        "assessment_context": { "context_id": "ctx_dusk" },
        "effective_at": NOW,
        "assessed_at": NOW,
        "view": "recorded_at",
        "q": {
            "value": 0.9,
            "assertion": "belief_attribution",
            "method": "self_report",
            "calibration": "uncalibrated"
        },
        "extensions": {}
    })
}

async fn seed_translated_record(fixture: &Fixture) {
    let mut row = record("r_tr", "s_1", None, false);
    if let RecordState::Live { content, .. } = &mut row.state {
        content.text = "translated anchor".to_string();
        content.cognition = Some(vec![translated_belief()]);
    }
    let mut tx = fixture.begin().await;
    fixture
        .repository
        .insert_record(&mut tx, &row)
        .await
        .expect("seed cognition record");
    tx.commit().await.expect("commit cognition record");
}

#[tokio::test]
async fn ordinal_zero_translations_and_id_maps_reach_transferred_content() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    target_space(&fixture).await;
    seed_translated_record(&fixture).await;
    let chunks = staged_source(&fixture, "translate-export-1").await;

    let translations = json!([{
        "from": { "kind": "external", "namespace": "src_realm", "id": "ext_1" },
        "to": { "kind": "external", "namespace": "dst_realm", "id": "ext_2" },
    }]);
    let staged = ready(
        stage_into(
            &fixture,
            TARGET,
            chunks[0].clone(),
            "translate-stage-0",
            Some(json!({ "reference_translations": translations.clone() })),
        )
        .await,
    );
    let import_id = staged["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();
    let stored: String = sqlx::query_scalar(
        "SELECT reference_translations_json FROM memory_imports WHERE import_id = ?",
    )
    .bind(&import_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("stored translations");
    assert_eq!(
        serde_json::from_str::<Value>(&stored).expect("translations json"),
        translations,
        "ordinal zero's translations are durable on the import"
    );

    ready(activate(&fixture, &import_id, "translate-activate-1").await);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_records WHERE space_id = ? \
             AND json_extract(content_json, '$.cognition[0].target.carrier.namespace') = 'dst_realm' \
             AND json_extract(content_json, '$.cognition[0].target.carrier.id') = 'ext_2'",
            &[TARGET],
        )
        .await,
        1,
        "the explicit translation pair reached the imported content"
    );

    let records: Vec<String> =
        sqlx::query_scalar("SELECT record_id FROM memory_records WHERE space_id = ?")
            .bind(TARGET)
            .fetch_all(&fixture.pool)
            .await
            .expect("target record ids");
    let evidence: String = sqlx::query_scalar(
        "SELECT json_extract(content_json, '$.cognition[0].basis.evidence[0].id') \
         FROM memory_records WHERE space_id = ? \
         AND json_extract(content_json, '$.cognition') IS NOT NULL",
    )
    .bind(TARGET)
    .fetch_one(&fixture.pool)
    .await
    .expect("translated evidence reference");
    assert!(
        records.contains(&evidence),
        "the id map rewrote the record reference to a row the target actually holds"
    );
}

#[tokio::test]
async fn activation_creates_only_the_listed_grants_the_caller_covers() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    target_space(&fixture).await;
    let chunks = staged_source(&fixture, "adopt-export-1").await;
    let staged = ready(
        stage_into(
            &fixture,
            TARGET,
            chunks[0].clone(),
            "adopt-stage-0",
            Some(json!({ "adopt_grants": [{ "grant_id": "g_2", "principal_id": "p_2" }] })),
        )
        .await,
    );
    let import_id = staged["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();
    ready(activate(&fixture, &import_id, "adopt-activate-1").await);

    let row: (String, String, Option<String>, String, String, i64) = sqlx::query_as(
        "SELECT grant_id, principal_id, subject_id, capabilities_json, context_ids_json, \
         revision FROM memory_grants WHERE space_id = ? AND principal_id = 'p_2'",
    )
    .bind(TARGET)
    .fetch_one(&fixture.pool)
    .await
    .expect("adopted grant");
    assert!(
        row.0.starts_with("grt_"),
        "an adopted grant takes a fresh store-wide identity"
    );
    assert_eq!(row.1, "p_2", "the listed target principal owns it");
    assert_eq!(row.2.as_deref(), Some("s_1"), "the subject id is preserved");
    assert_eq!(
        serde_json::from_str::<Value>(&row.3).expect("capabilities"),
        json!(["transfer", "records_read"])
    );
    assert_eq!(
        serde_json::from_str::<Value>(&row.4).expect("contexts"),
        json!(["ctx_1"])
    );
    assert_eq!(row.5, 1, "an imported grant starts at revision one");
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_grants WHERE space_id = ? \
             AND grant_id IN ('g_1', 'g_3')",
            &[TARGET],
        )
        .await,
        0,
        "unlisted and revoked grants are never created"
    );
}

#[tokio::test]
async fn adoption_refuses_a_cover_that_would_outlive_the_caller_grant() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    target_space(&fixture).await;
    sqlx::query("UPDATE memory_grants SET expires_at = ? WHERE grant_id = 'g_target'")
        .bind("2030-01-01T00:00:00.000Z")
        .execute(&fixture.pool)
        .await
        .expect("expire the target grant");

    let chunks = staged_source(&fixture, "adopt-export-2").await;
    let refused = stage_into(
        &fixture,
        TARGET,
        chunks[0].clone(),
        "adopt-stage-1",
        Some(json!({ "adopt_grants": [{ "grant_id": "g_1", "principal_id": "p_1" }] })),
    )
    .await;
    let reject = rejected(&refused);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "reason"), &json!("grant_not_subsumed"));
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_imports WHERE space_id = ?",
            &[TARGET]
        )
        .await,
        0,
        "a refused adoption stages nothing"
    );
}

/// An adopted grant is one row with one capability set. Capabilities held in two
/// separate caller grants never merge into it: no single grant must cover fewer than
/// the adopted grant's whole set.
#[tokio::test]
async fn adoption_refuses_capabilities_that_only_separate_grants_cover() {
    let fixture = Fixture::open().await;
    target_space(&fixture).await;
    sqlx::query("UPDATE memory_grants SET expires_at = ? WHERE grant_id = 'g_target'")
        .bind("2030-01-01T00:00:00.000Z")
        .execute(&fixture.pool)
        .await
        .expect("expire the target admin cover");
    for (grant_id, capability) in [
        ("g_target_transfer", "transfer"),
        ("g_target_records", "records_read"),
    ] {
        let mut row = grant(grant_id, None, "p_1", &[], false, false);
        row.space_id = SpaceId::new(TARGET);
        row.capabilities = vec![capability.to_string()];
        let mut tx = fixture.begin().await;
        fixture
            .repository
            .insert_grant(&mut tx, &row)
            .await
            .expect("seed a split grant");
        tx.commit().await.expect("commit the split grant");
    }

    let chunks = staged_source(&fixture, "split-cover-export").await;
    let refused = stage_into(
        &fixture,
        TARGET,
        chunks[0].clone(),
        "split-cover-stage",
        Some(json!({ "adopt_grants": [{ "grant_id": "g_2", "principal_id": "p_2" }] })),
    )
    .await;
    let reject = rejected(&refused);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "reason"), &json!("grant_not_subsumed"));
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_imports WHERE space_id = ?",
            &[TARGET],
        )
        .await,
        0,
        "a split cover stages nothing"
    );
}

/// Adoption becomes permanent only at activation, so the cover that justified it at staging
/// is re-derived from the activating caller's current grants: a cover that now expires
/// refuses the flip and installs nothing.
#[tokio::test]
async fn activation_refuses_adoption_when_the_callers_cover_now_expires() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    target_space(&fixture).await;
    let chunks = staged_source(&fixture, "adopt-export-3").await;
    let staged = ready(
        stage_into(
            &fixture,
            TARGET,
            chunks[0].clone(),
            "adopt-stage-2",
            Some(json!({ "adopt_grants": [{ "grant_id": "g_2", "principal_id": "p_2" }] })),
        )
        .await,
    );
    let import_id = staged["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();

    sqlx::query("UPDATE memory_grants SET expires_at = ? WHERE grant_id = 'g_target'")
        .bind("2030-01-01T00:00:00.000Z")
        .execute(&fixture.pool)
        .await
        .expect("expire the target cover");

    let result = activate(&fixture, &import_id, "adopt-activate-2").await;
    let reject = rejected(&result);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "reason"), &json!("grant_not_subsumed"));
    assert_eq!(
        import_state(&fixture, &import_id).await,
        ("staged".to_string(), 1),
        "a refused activation leaves the import staged"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_grants WHERE space_id = ? AND principal_id = 'p_2'",
            &[TARGET],
        )
        .await,
        0,
        "a refused activation installs no adopted grant"
    );
}

/// The frozen rows are rechecked against the live target: a subject the target gained
/// after staging refuses the activation, and nothing is applied.
#[tokio::test]
async fn activation_rechecks_a_subject_the_target_gained_after_staging() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    target_space(&fixture).await;
    let chunks = staged_source(&fixture, "recheck-export-1").await;
    let staged =
        ready(stage_into(&fixture, TARGET, chunks[0].clone(), "recheck-stage-0", None).await);
    let import_id = staged["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();

    let mut row = subject("s_1", "active", &[]);
    row.space_id = SpaceId::new(TARGET);
    let mut tx = fixture.begin().await;
    fixture
        .repository
        .insert_subject(&mut tx, &row)
        .await
        .expect("ensure the subject");
    tx.commit().await.expect("commit subject");

    let result = activate(&fixture, &import_id, "recheck-activate-1").await;
    let reject = rejected(&result);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "reason"), &json!("subject_collision"));
    assert_eq!(
        import_state(&fixture, &import_id).await,
        ("staged".to_string(), 1),
        "a refused recheck leaves the import staged"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_records WHERE space_id = ?",
            &[TARGET]
        )
        .await,
        0,
        "a refused recheck applies nothing"
    );
}

#[tokio::test]
async fn activating_a_tombstone_inclusive_import_keeps_the_scrubbed_envelope() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    target_space(&fixture).await;
    let adapter = crate::MemoryAdapter::new(false);
    let mut tx = fixture.begin().await;
    let created = ready(
        adapter
            .create_export(
                &mut tx,
                &principal("p_1"),
                json!({
                    "space_id": SPACE,
                    "idempotency_key": "tombstone-export-1",
                    "include_tombstoned": true,
                }),
            )
            .await,
    );
    tx.commit().await.expect("commit export");
    let export_id = created["result"]["export_id"]
        .as_str()
        .expect("export id")
        .to_string();
    let chunks = export_chunks(&fixture, &export_id).await;

    let staged = ready(
        stage_into(
            &fixture,
            TARGET,
            chunks[0].clone(),
            "tombstone-stage-0",
            None,
        )
        .await,
    );
    let import_id = staged["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();
    let body = ready(activate(&fixture, &import_id, "tombstone-activate-1").await);
    assert_eq!(body["result"]["activated"]["records"], json!(4));

    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_records WHERE space_id = ? \
             AND tombstone_json IS NOT NULL \
             AND json_extract(content_json, '$.text') = ''",
            &[TARGET],
        )
        .await,
        1,
        "the scrubbed tombstone envelope crosses the import without regaining content"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_records WHERE space_id = ? AND import_ref_json IS NULL",
            &[TARGET],
        )
        .await,
        1,
        "a tombstoned row carries no import reference"
    );
}

/// §7.31: an `import_ref` already carried by a record is historical attribution and
/// survives a later move; the new hop is recorded in that import's own report instead
/// of overwriting the identity the first move gave the content.
#[tokio::test]
async fn a_second_import_keeps_the_original_import_provenance() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    target_space(&fixture).await;

    let chunks = staged_source(&fixture, "lineage-export-1").await;
    let staged =
        ready(stage_into(&fixture, TARGET, chunks[0].clone(), "lineage-stage-0", None).await);
    let first = staged["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();
    ready(activate(&fixture, &first, "lineage-activate-1").await);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_records WHERE space_id = ? \
             AND json_extract(import_ref_json, '$.import_id') = ?",
            &[TARGET, &first],
        )
        .await,
        3,
        "the first hop stamps its own identity"
    );

    let adapter = crate::MemoryAdapter::new(false);
    let mut tx = fixture.begin().await;
    let created = ready(
        adapter
            .create_export(
                &mut tx,
                &principal("p_1"),
                json!({ "space_id": TARGET, "idempotency_key": "lineage-export-2" }),
            )
            .await,
    );
    tx.commit().await.expect("commit second export");
    let export_id = created["result"]["export_id"]
        .as_str()
        .expect("export id")
        .to_string();
    let chunk_count = created["result"]["chunk_count"]
        .as_u64()
        .expect("chunk count");
    let mut chunks = Vec::new();
    for ordinal in 0..chunk_count {
        let mut tx = fixture.begin().await;
        let page = ready(
            adapter
                .get_export(
                    &mut tx,
                    &principal("p_1"),
                    json!({ "space_id": TARGET, "export_id": export_id, "chunk_ordinal": ordinal }),
                )
                .await,
        );
        tx.rollback().await.expect("rollback page read");
        chunks.push(page["chunk"].clone());
    }
    let mut staged = None;
    for (ordinal, chunk) in chunks.iter().enumerate() {
        let body = ready(
            stage_into(
                &fixture,
                TARGET,
                chunk.clone(),
                &format!("lineage-hop2-{ordinal}"),
                Some(json!({ "conflict_policy": "rename_subject" })),
            )
            .await,
        );
        staged = Some(body);
    }
    let staged = staged.expect("the second export stages");
    let second = staged["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();
    ready(activate(&fixture, &second, "lineage-activate-2").await);

    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_records WHERE space_id = ? \
             AND json_extract(import_ref_json, '$.import_id') = ?",
            &[TARGET, &first],
        )
        .await,
        6,
        "the second move keeps the original provenance on both copies"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_records WHERE space_id = ? \
             AND json_extract(import_ref_json, '$.import_id') = ?",
            &[TARGET, &second],
        )
        .await,
        0,
        "the later import never overwrites an existing import_ref"
    );
    let report: String =
        sqlx::query_scalar("SELECT staged_report_json FROM memory_imports WHERE import_id = ?")
            .bind(&second)
            .fetch_one(&fixture.pool)
            .await
            .expect("second import report");
    let report: Value = serde_json::from_str(&report).expect("report json");
    assert_eq!(
        report["record_id_map"]
            .as_object()
            .map(serde_json::Map::len),
        Some(3),
        "the new hop is recorded in its own identity report"
    );
}

#[tokio::test]
async fn a_colliding_subject_is_refused_by_fail_and_renamed_on_request() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;

    let chunks = staged_source(&fixture, "rename-export-1").await;
    let refused = stage_into(
        &fixture,
        SPACE,
        chunks[0].clone(),
        "rename-stage-0",
        Some(json!({ "conflict_policy": "fail" })),
    )
    .await;
    let reject = rejected(&refused);
    assert_eq!(
        reject.code,
        spoke_operations::SpokeRejectCode::ImportConflict
    );
    assert_eq!(detail(reject, "reason"), &json!("subject_collision"));
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_imports WHERE space_id = ?",
            &[SPACE]
        )
        .await,
        0,
        "a refused final chunk stages nothing"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_records WHERE space_id = ?",
            &[SPACE]
        )
        .await,
        4,
        "a refused final chunk applies nothing"
    );

    let chunks = staged_source(&fixture, "rename-export-2").await;
    let staged = ready(
        stage_into(
            &fixture,
            SPACE,
            chunks[0].clone(),
            "rename-stage-1",
            Some(json!({ "conflict_policy": "rename_subject" })),
        )
        .await,
    );
    let import_id = staged["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();
    ready(run_import(&fixture, SPACE, &import_id, "rename-activate-2", true).await);

    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_subjects WHERE space_id = ?",
            &[SPACE]
        )
        .await,
        4,
        "two preserved subjects plus two renamed copies"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM memory_records WHERE space_id = ? \
             AND subject_id NOT IN ('s_1', 's_2')",
            &[SPACE],
        )
        .await,
        3,
        "every imported record is rebased on its renamed subject"
    );
}

#[tokio::test]
async fn an_imported_reflection_arrives_stale_with_its_basis() {
    let fixture = Fixture::open().await;
    elevate(&fixture).await;
    target_space(&fixture).await;
    seed_reflection(&fixture, "rfl_1").await;

    let adapter = crate::MemoryAdapter::new(false);
    let mut tx = fixture.begin().await;
    let created = ready(
        adapter
            .create_export(
                &mut tx,
                &principal("p_1"),
                json!({
                    "space_id": SPACE,
                    "idempotency_key": "reflection-export-1",
                    "include_reflections": true,
                }),
            )
            .await,
    );
    tx.commit().await.expect("commit export");
    let export_id = created["result"]["export_id"]
        .as_str()
        .expect("export id")
        .to_string();
    let chunks = export_chunks(&fixture, &export_id).await;

    let staged = ready(
        stage_into(
            &fixture,
            TARGET,
            chunks[0].clone(),
            "reflection-stage-0",
            None,
        )
        .await,
    );
    let import_id = staged["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();
    let body = ready(activate(&fixture, &import_id, "reflection-activate-1").await);
    assert_eq!(body["result"]["activated"]["reflections"], json!(1));

    let row: (String, String, String, i64) = sqlx::query_as(
        "SELECT reflection_id, body_text, selection_json, stale_cache \
         FROM memory_reflections WHERE space_id = ? AND subject_id = 's_1'",
    )
    .bind(TARGET)
    .fetch_one(&fixture.pool)
    .await
    .expect("imported reflection");
    assert!(
        row.0.starts_with("rfl_"),
        "the imported reflection takes a fresh identity"
    );
    assert_eq!(row.1, "derived");
    assert_eq!(row.2, json!({ "mode": "subject_all" }).to_string());
    assert_eq!(row.3, 1, "an imported reflection is stale by construction");
}
