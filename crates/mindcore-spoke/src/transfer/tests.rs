//! The export builder against seeded storage and the §6.13 boundary cases.

use serde_json::{json, Map, Value};

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::{ContextId, SpaceId, SubjectId};
use mindcore_base::ir::{
    CandidateState, Classification, Consumption, MemoryCandidateIr, MemoryContent, MemoryGrantRow,
    MemoryProfileRow, MemoryRecordIr, MemoryReflectionRow, MemoryRepository, MemorySubjectRow,
    Placement, PlacementSelection, Provenance, ReceiptState, RecordState, ReviewReceiptIr,
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

    // The digest is the JCS SHA-256 of the bundle with `digest` removed, computed
    // here through the plain helper rather than the builder's excluding one.
    assert_eq!(
        hex_digest_excluding(&artifact.bundle, "digest"),
        artifact.digest
    );

    // Chunks are contiguous from zero and verify independently.
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

    // Reassembly rebuilds the sealed bundle.
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

    // A source tombstoned after generation changes the live set; the derived body
    // may encode deleted content, so the envelope is withheld.
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
    let chunks = ok(super::chunk::partition(&bundle, limit), "partition");
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
    let chunks = ok(super::chunk::partition(&bundle, limit), "partition");
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
        super::chunk::partition(&bundle, policy::EXPORT_CHUNK_BYTES),
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
