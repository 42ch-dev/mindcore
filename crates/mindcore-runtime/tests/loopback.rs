//! Signed in-process loopback over the composed surface against a real SQLite store.
//!
//! A remote consumer dials a `ConnectResponder` built from [`MindcoreServe::surface`] on a
//! throwaway Ed25519 pair over the in-process transport: signed hellos, a signed session
//! snapshot, a fail-closed allowlist, then the Mind tools. Handlers are installed before the
//! dialer connects, exactly as a host must.
//!
//! The runtime serves a store a production `MindStore::open` produced: the embedded migration
//! installs the protocol schema, and the fixture seeds only rows into it.
//!
//! Advertisement honesty is the negative side: a face the host did not attach advertises nothing
//! and denies its ops on the wire, a family the runtime composes no face for answers
//! `CAPABILITY_PORT_MISSING`, and a tool the runtime does not serve answers `op_unsupported`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ed25519_dalek::SigningKey;
use serde_json::{json, Map, Number, Value};
use tempfile::TempDir;

use mindcore_base::ids::SpaceId;
use mindcore_base::ir::MemorySpaceRow;
use mindcore_runtime::tools::{ABSORB, DELIBERATE, READ_COGNITION, RECALL_CONTEXT};
use mindcore_runtime::{
    MindProviders, MindcoreServe, MindcoreSurface, ServeFaces, ToolRegistration,
};
use mindcore_spi::{
    CognitiveTaskKind, Embedder, EmbeddingIdentity, PolicyDecision, PolicyHook, Reasoner,
    SettlementDraft, SpiError, StructuredReasonRequest,
};
use mindcore_spoke::{MemoryAdapter, MemoryStage, ServedMemory, TransferService};
use mindcore_sqlite::{MindStore, SqliteMemoryRepository};
use spoke_connect::core::derive_peer_id_from_ed25519_pubkey;
use spoke_connect::remote::{
    connect_remote_adapter, connect_responder, loopback_transport_pair, ConnectResponder,
    ConnectResponderOptions, LoopbackTransport, MemoryAdmission, RemoteAdapter,
    RemoteAdapterOptions, RemoteAdapterState, RemoteIdentity, RemoteMemoryService, ToolHandler,
    Transport, TransportError,
};
use spoke_operations::{
    spoke_ok, spoke_reject, validate_judgment, AdoptionPort, HistoryPort, KnowledgeEntryPort,
    KnowledgeQueryPort, MemoryPort, ScopeQueryPort, SpokeReject, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::host_capability_manifest::ToolDescriptor;
use spoke_schemas::knowledge_entry::{
    KnowledgeEntryBody, KnowledgeEntryCanonicalName, KnowledgeEntryModulesKey,
    KnowledgeEntryModulesValue,
};
use spoke_schemas::{
    HostCapabilityManifest, KnowledgeAdoptRequest, KnowledgeEntry, MemoryGrantsListRequest,
    MemoryGrantsListResponse, MemoryRecallRequest, Principal, Scope,
};

const SCOPE_ID: &str = "spoke-scope-001";
const COLLECTION: &str = "wren";
/// The carrier entry id every Mind is created with; the wire never names it.
const CARRIER: &str = "ke_carrier";
/// The second Mind's own collection.
const COLLECTION_TWO: &str = "mora";
/// The second Mind's carrier entry id: deliberately not the production constant, so this fixture
/// is also the regression that a Mind whose row names a different carrier id settles onto that
/// id — and so the two Minds' knowledge content can never be interchangeable.
const CARRIER_TWO: &str = "ke_carrier_mora";
const SPACE: &str = "spc_loopback";
/// The second Mind's space, admitted to the second peer only.
const SPACE_TWO: &str = "spc_loopback_two";
const SUBJECT: &str = "subj_loopback";
const PRINCIPAL: &str = "prn_loopback";
/// why: a transfer chunk travels back through the typed wire DTOs, which canonicalize a whole
/// second to no fraction and drop a null `kind`; this fixture seeds the shape they carry unchanged.
const NOW: &str = "2026-11-21T00:00:00.500Z";
const UNSERVED_TOOL: &str = "tools.mindcore.unserved";
/// A host-owned tool the runtime serves beside its own three.
const HOST_TOOL: &str = "tools.mindcore.host_tool";
/// The knowledge face's three flags: a session that serves scoped knowledge negotiates them
/// together, exactly as the composed manifest advertises them.
const KNOWLEDGE_CAPABILITIES: [&str; 3] =
    ["spoke-knowledge", "ke-ownership", "spoke-knowledge-scoped"];

const DIALER_SEED: [u8; 32] = [0x42u8; 32];
const RESPONDER_SEED: [u8; 32] = [0xA7u8; 32];
/// A second peer the host admits to its own principal, never the carrier route's caller.
const PEER_TWO_SEED: [u8; 32] = [0x5Cu8; 32];
const PEER_TWO_PRINCIPAL: &str = "prn_peer_two";
/// A third peer, for the grants that do not cover a whole operation.
const PEER_THREE_SEED: [u8; 32] = [0x6Du8; 32];
const PEER_THREE_PRINCIPAL: &str = "prn_peer_three";

/// The database file the fixture opens; a test may reopen the same path.
const DB_FILE: &str = "loopback.db";

/// One store the runtime serves: the protocol memory shape with two retained records, the
/// grants the workflows read, and the knowledge collection the face serves.
struct Fixture {
    dir: TempDir,
    store: MindStore,
}

impl Fixture {
    /// Open a throwaway database through the production `MindStore::open` path (its embedded
    /// migration installs the protocol schema), then seed the rows the faces read.
    async fn open() -> Self {
        let dir = TempDir::new().expect("temporary directory");
        let store = MindStore::open(&dir.path().join(DB_FILE))
            .await
            .expect("open store");
        let pool = store.pool();
        let mut tx = pool.begin().await.expect("begin");
        let space = MemorySpaceRow {
            space_id: SpaceId::new(SPACE),
            display_name: Some("loopback space".to_string()),
            authority_host_id: "host_1".to_string(),
            authority_generation: 1,
            status: "active".to_string(),
            revision: Number::from(1),
            created_at: NOW.to_string(),
            pending_receipts: 0,
        };
        SqliteMemoryRepository
            .insert_space(&mut tx, &space)
            .await
            .expect("seed space");
        tx.commit().await.expect("commit space");
        sqlx::query(
            "INSERT INTO memory_subjects \
             (space_id, subject_id, kind_ns, kind_id, status, fenced_contexts_json, revision, \
              created_at) \
             VALUES (?, ?, 'kind', 'belief', 'active', '[]', 1, ?)",
        )
        .bind(SPACE)
        .bind(SUBJECT)
        .bind(NOW)
        .execute(&pool)
        .await
        .expect("seed subject");
        for (grant_id, capabilities) in [
            ("grt_recall", json!(["recall"])),
            (
                "grt_knowledge",
                json!(["knowledge_read", "knowledge_write"]),
            ),
        ] {
            sqlx::query(
                "INSERT INTO memory_grants \
                 (grant_id, space_id, subject_id, principal_id, capabilities_json, \
                  context_ids_json, shared_access, expires_at, revoked_at, revision, created_at) \
                 VALUES (?, ?, NULL, ?, ?, '[]', 0, NULL, NULL, 1, ?)",
            )
            .bind(grant_id)
            .bind(SPACE)
            .bind(PRINCIPAL)
            .bind(capabilities.to_string())
            .bind(NOW)
            .execute(&pool)
            .await
            .expect("seed grant");
        }
        // The carrier entry the registry row references: `minds` keys it foreign to
        // `knowledge_entries`, so the entry exists before the row names it.
        sqlx::query(
            "INSERT INTO knowledge_entries \
             (collection, entry_id, revision, payload, created_at, updated_at) \
             VALUES (?, ?, 1, ?, ?, ?)",
        )
        .bind(COLLECTION)
        .bind(CARRIER)
        .bind(
            json!({
                "schema_version": 1,
                "entry_id": CARRIER,
                "entry_type": "note",
                "canonical_name": format!("{CARRIER}'s carrier"),
                "status": "confirmed",
                "revision": 1,
                "body": { "summary": "what this holder currently holds" },
                "extensions": {},
            })
            .to_string(),
        )
        .bind(NOW)
        .bind(NOW)
        .execute(&pool)
        .await
        .expect("seed the carrier entry");
        // The Mind registry row of this fixture's space: the workflow binding resolves the
        // collection it serves from it, and a space with no row answers nothing.
        sqlx::query(
            "INSERT INTO minds (space_id, knowledge_collection, carrier_entry_id) VALUES (?, ?, ?)",
        )
        .bind(SPACE)
        .bind(COLLECTION)
        .bind(CARRIER)
        .execute(&pool)
        .await
        .expect("seed the mind registry row");

        seed_record(&pool, "rec_north_gate", "the north gate is closed").await;
        seed_record(&pool, "rec_market", "the market opens at dawn").await;
        Self { dir, store }
    }

    /// The second Mind: its own space, subject, records, carrier entry and registry row, ready
    /// for a peer admitted to it alone.
    async fn seed_second_mind(&self) {
        let pool = self.store.pool();
        for statement in [
            format!(
                "INSERT INTO memory_spaces \
                 (space_id, display_name, authority_host_id, authority_generation, status, \
                  revision, created_at) \
                 VALUES ('{SPACE_TWO}', 'second mind', 'host_1', 1, 'active', 1, '{NOW}')"
            ),
            format!(
                "INSERT INTO memory_subjects \
                 (space_id, subject_id, kind_ns, kind_id, status, fenced_contexts_json, revision, \
                  created_at) \
                 VALUES ('{SPACE_TWO}', '{SUBJECT}', 'kind', 'belief', 'active', '[]', 1, '{NOW}')"
            ),
        ] {
            sqlx::query(sqlx::AssertSqlSafe(statement))
                .execute(&pool)
                .await
                .expect("seed the second mind");
        }
        // The carrier entry of the second Mind, before the registry row references it.
        sqlx::query(
            "INSERT INTO knowledge_entries \
             (collection, entry_id, revision, payload, created_at, updated_at) \
             VALUES (?, ?, 1, ?, ?, ?)",
        )
        .bind(COLLECTION_TWO)
        .bind(CARRIER_TWO)
        .bind(
            json!({
                "schema_version": 1,
                "entry_id": CARRIER_TWO,
                "entry_type": "note",
                "canonical_name": "the second mind's carrier",
                "status": "confirmed",
                "revision": 1,
                "body": { "summary": "the knowledge of the second mind, not the first" },
                "extensions": {},
            })
            .to_string(),
        )
        .bind(NOW)
        .bind(NOW)
        .execute(&pool)
        .await
        .expect("seed the second mind's carrier entry");
        sqlx::query(
            "INSERT INTO minds (space_id, knowledge_collection, carrier_entry_id) VALUES (?, ?, ?)",
        )
        .bind(SPACE_TWO)
        .bind(COLLECTION_TWO)
        .bind(CARRIER_TWO)
        .execute(&pool)
        .await
        .expect("seed the second mind's registry row");
        seed_record_in(
            &pool,
            SPACE_TWO,
            "rec_north_gate_two",
            "the north gate is closed",
        )
        .await;
        seed_record_in(
            &pool,
            SPACE_TWO,
            "rec_market_two",
            "the market opens at dawn",
        )
        .await;
    }

    /// One space-wide grant for `principal` over the second Mind's space.
    async fn grant_in_second(&self, grant_id: &str, principal: &str, capabilities: &[&str]) {
        sqlx::query(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, \
              context_ids_json, shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES (?, ?, NULL, ?, ?, '[]', 0, NULL, NULL, 1, ?)",
        )
        .bind(grant_id)
        .bind(SPACE_TWO)
        .bind(principal)
        .bind(json!(capabilities).to_string())
        .bind(NOW)
        .execute(&self.store.pool())
        .await
        .expect("seed grant");
    }

    /// One space-wide grant for `principal` over this fixture's space.
    async fn grant(&self, grant_id: &str, principal: &str, capabilities: &[&str]) {
        sqlx::query(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, \
              context_ids_json, shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES (?, ?, NULL, ?, ?, '[]', 0, NULL, NULL, 1, ?)",
        )
        .bind(grant_id)
        .bind(SPACE)
        .bind(principal)
        .bind(json!(capabilities).to_string())
        .bind(NOW)
        .execute(&self.store.pool())
        .await
        .expect("seed grant");
    }

    /// Drop one seeded grant, as a host revoking it would.
    async fn revoke(&self, grant_id: &str) {
        sqlx::query("DELETE FROM memory_grants WHERE grant_id = ?")
            .bind(grant_id)
            .execute(&self.store.pool())
            .await
            .expect("drop the grant");
    }

    /// The store and its directory part company: the engine takes the store, the caller keeps the
    /// directory alive for the session's lifetime.
    fn serve(self) -> (TempDir, MindcoreServe) {
        let Self { dir, store } = self;
        (dir, MindcoreServe::new(store, false, |_targets| Ok(())))
    }
}

async fn seed_record(pool: &sqlx::SqlitePool, record_id: &str, text: &str) {
    seed_record_in(pool, SPACE, record_id, text).await;
}

async fn seed_record_in(pool: &sqlx::SqlitePool, space: &str, record_id: &str, text: &str) {
    sqlx::query(
        "INSERT INTO memory_records \
         (record_id, space_id, subject_id, kind, content_json, scope_visibility, \
          scope_context_id, provenance_json, import_ref_json, occurred_at, tombstone_json, \
          revision, created_at, updated_at) \
         VALUES (?, ?, ?, 'fact', ?, 'shared', NULL, ?, NULL, NULL, NULL, 1, ?, ?)",
    )
    .bind(record_id)
    .bind(space)
    .bind(SUBJECT)
    .bind(json!({ "text": text }).to_string())
    .bind(
        json!({
            "source_context_ids": [],
            "anchors": [],
            "captured_by": "loopback",
            "captured_at": NOW,
        })
        .to_string(),
    )
    .bind(NOW)
    .bind(NOW)
    .execute(pool)
    .await
    .expect("seed record");
}

/// One signed hello's manifest with exactly the capabilities its side claims.
fn hello(host_id: &str, capabilities: &[&str]) -> HostCapabilityManifest {
    serde_json::from_value(json!({
        "schema_version": 1,
        "host_id": host_id,
        "roles": ["data-store"],
        "capabilities": capabilities,
        "namespaces": ["toy_world"],
        "extensions": {},
    }))
    .expect("manifest JSON matches the schema")
}

/// The host tool's descriptor, as a host could build one.
fn host_tool_descriptor() -> ToolDescriptor {
    serde_json::from_value(json!({
        "schema_version": 1,
        "capability_id": HOST_TOOL,
        "op": HOST_TOOL,
        "description": "The host's own tool.",
        "input": { "type": "object" },
        "output": { "type": "object" },
    }))
    .expect("tool descriptor JSON matches the schema")
}

fn capabilities(surface: &MindcoreSurface) -> Vec<&str> {
    surface
        .manifest
        .capabilities
        .iter()
        .map(|capability| capability.as_str())
        .collect()
}

fn scope_of(scope_id: &str) -> Scope {
    serde_json::from_value(json!({ "scope_id": scope_id })).expect("scope JSON matches the schema")
}

fn principal_for(principal_id: &str) -> Principal {
    serde_json::from_value(json!({ "principal_id": principal_id, "extensions": {} }))
        .expect("principal JSON matches the schema")
}

fn principal() -> Principal {
    principal_for(PRINCIPAL)
}

fn peer_id(seed: &[u8; 32]) -> String {
    derive_peer_id_from_ed25519_pubkey(&SigningKey::from_bytes(seed).verifying_key().to_bytes())
}

fn dialer_peer_id() -> String {
    peer_id(&DIALER_SEED)
}

/// A host admission mapping two authenticated peers to their own principals: the carrier route's
/// caller and a second peer that holds only its own grants.
fn two_principal_admission() -> MemoryAdmission {
    let dialer = dialer_peer_id();
    let second = peer_id(&PEER_TWO_SEED);
    Arc::new(move |peer: &str| {
        let principal_id = if peer == dialer {
            PRINCIPAL
        } else if peer == second {
            PEER_TWO_PRINCIPAL
        } else {
            return spoke_reject(
                SpokeRejectCode::NotAuthorized,
                format!("peer {peer} holds no workflow admission"),
                None,
            );
        };
        spoke_ok(principal_for(principal_id))
    })
}

/// The two-peer mapping plus a third authenticated peer holding only `knowledge_write`: the grant
/// split the per-operation matrix needs.
fn three_principal_admission() -> MemoryAdmission {
    let dialer = dialer_peer_id();
    let second = peer_id(&PEER_TWO_SEED);
    let third = peer_id(&PEER_THREE_SEED);
    Arc::new(move |peer: &str| {
        let principal_id = if peer == dialer {
            PRINCIPAL
        } else if peer == second {
            PEER_TWO_PRINCIPAL
        } else if peer == third {
            PEER_THREE_PRINCIPAL
        } else {
            return spoke_reject(
                SpokeRejectCode::NotAuthorized,
                format!("peer {peer} holds no workflow admission"),
                None,
            );
        };
        spoke_ok(principal_for(principal_id))
    })
}

/// Both host admissions admit the dialer's authenticated peer id and nothing else, so a served
/// call proves the responder took the peer from its session rather than from the arguments.
fn principal_admission() -> MemoryAdmission {
    let peer_id = dialer_peer_id();
    Arc::new(move |peered: &str| match peered == peer_id {
        true => spoke_ok(principal()),
        false => spoke_reject(
            SpokeRejectCode::NotAuthorized,
            format!("peer {peered} holds no workflow admission"),
            None,
        ),
    })
}

/// The host's memory family service; a test attaches it only where it means to serve it.
struct MemoryFace;

#[async_trait]
impl RemoteMemoryService for MemoryFace {
    async fn invoke(&self, peer_id: &str, op: &str, _payload: Value) -> SpokeResult<Value> {
        if peer_id != dialer_peer_id() {
            return spoke_reject(
                SpokeRejectCode::NotAuthorized,
                format!("peer {peer_id} holds no memory admission"),
                None,
            );
        }
        if op != "memory.recall" {
            return spoke_reject(
                SpokeRejectCode::CapabilityPortMissing,
                format!("the fixture memory face does not serve {op}"),
                None,
            );
        }
        spoke_ok(json!({
            "engine_policy_version": "loopback-memory-face",
            "records": [],
            "total_authorized": 7,
            "truncated": false,
        }))
    }
}

impl ServedMemory for MemoryFace {}

/// The host's reasoner: one `none` verdict over the evidence the deliberation supplied.
struct NoChangeReasoner;

#[async_trait]
impl Reasoner for NoChangeReasoner {
    async fn reason_structured(
        &self,
        request: StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        assert_eq!(request.task_kind, CognitiveTaskKind::Deliberate);
        let payload: Value =
            serde_json::from_str(request.user_payload).expect("the deliberation payload is JSON");
        assert_eq!(payload["evidence"][0]["evidence_id"], "ev_gate");
        assert!(
            payload["records"]
                .as_array()
                .is_some_and(|records| records.iter().any(|record| record["record_id"]
                    .as_str()
                    .is_some_and(|id| id.starts_with("rec_north_gate")))),
            "the authorized memory page reached the reasoner: {}",
            request.user_payload
        );
        Ok(json!({
            "recommendation": { "kind": "none" },
            "rationale": "the evidence carries nothing the carrier does not already hold",
            "evidence_ids": ["ev_gate"],
            "record_ids": ["rec_north_gate"],
        }))
    }
}

/// The rationale the settling reasoner states, so a test can restate the verdict without
/// changing the branch.
const SETTLING_RATIONALE: &str = "the cited record carries the north gate state";

/// The host's reasoner when a deliberation must write: one promotion over the evidence, so the
/// settlement policy's `AutoSettle` appends a cognition record to the routed carrier.
struct SettlingReasoner(&'static str);

#[async_trait]
impl Reasoner for SettlingReasoner {
    async fn reason_structured(
        &self,
        request: StructuredReasonRequest<'_>,
    ) -> Result<Value, SpiError> {
        assert_eq!(request.task_kind, CognitiveTaskKind::Deliberate);
        let payload: Value =
            serde_json::from_str(request.user_payload).expect("the deliberation payload is JSON");
        assert!(
            payload["records"]
                .as_array()
                .is_some_and(|records| records.iter().any(|record| record["record_id"]
                    .as_str()
                    .is_some_and(|id| id.starts_with("rec_north_gate")))),
            "the authorized memory page reached the reasoner: {}",
            request.user_payload
        );
        let gate_record = payload["records"]
            .as_array()
            .and_then(|records| {
                records.iter().find(|record| {
                    record["record_id"]
                        .as_str()
                        .is_some_and(|id| id.starts_with("rec_north_gate"))
                })
            })
            .and_then(|record| record["record_id"].as_str())
            .expect("the page carries the north-gate record")
            .to_string();
        Ok(json!({
            "recommendation": { "kind": "promote", "target_kind": "belief" },
            "rationale": self.0,
            "evidence_ids": ["ev_gate"],
            "record_ids": [gate_record],
            "record": {
                "kind": "belief",
                "order": 1,
                "q": {
                    "value": 0.8,
                    "assertion": "belief_attribution",
                    "method": "two_witness_reports",
                    "calibration": "uncalibrated"
                },
                "assessor_role": "holder_self_report",
                "basis": {
                    "kind": "evidence",
                    "evidence": [
                        { "kind": "memory_record", "namespace": "mindcore", "id": gate_record }
                    ]
                }
            }
        }))
    }
}

/// The host's settlement policy: settle every validated draft, so a promotion reaches the carrier.
struct AutoPolicy;

#[async_trait]
impl PolicyHook for AutoPolicy {
    async fn evaluate_settlement(
        &self,
        _draft: SettlementDraft<'_>,
    ) -> Result<PolicyDecision, SpiError> {
        Ok(PolicyDecision::AutoSettle)
    }
}

/// The host's settlement policy that holds every valid draft: a promotion reaches `needs_review`
/// and writes nothing.
struct HoldingPolicy;

#[async_trait]
impl PolicyHook for HoldingPolicy {
    async fn evaluate_settlement(
        &self,
        _draft: SettlementDraft<'_>,
    ) -> Result<PolicyDecision, SpiError> {
        Ok(PolicyDecision::RequireHumanReview {
            reason: "the host withholds automatic settlement".to_string(),
        })
    }
}

/// The host's side of the surface.
fn faces(
    manifest: HostCapabilityManifest,
    memory: Option<Arc<dyn ServedMemory>>,
    providers: MindProviders,
) -> ServeFaces {
    ServeFaces {
        manifest,
        memory,
        knowledge: false,
        history: false,
        principals: principal_admission(),
        providers,
        tools: Vec::new(),
    }
}

fn surface(serve: &MindcoreServe, faces: ServeFaces) -> MindcoreSurface {
    expect_ok(serve.surface(faces), "assemble the served surface")
}

struct Session {
    adapter: Arc<RemoteAdapter>,
    responder: Arc<ConnectResponder>,
    /// Every envelope the responder sent back on this session, verbatim.
    wire: Arc<WireLog>,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.adapter.close();
        self.responder.close();
    }
}

/// The raw envelopes of one session, in order: what the responder sent back, and what the dialer
/// put on the wire.
///
/// A typed response is not the wire: `invoke_mapped` decodes into the op's DTO, so a member the
/// DTO does not declare is gone before a test can see it. These are the bytes. The sent side is
/// here for the one thing only the request carries: a value the dialer built and transmitted.
#[derive(Default)]
struct WireLog {
    frames: Mutex<Vec<String>>,
    sent: Mutex<Vec<String>>,
}

impl WireLog {
    fn drain(&self) -> Vec<String> {
        std::mem::take(&mut *self.frames.lock().expect("wire log lock"))
    }

    /// Every envelope the dialer sent since the last call.
    fn drain_sent(&self) -> Vec<String> {
        std::mem::take(&mut *self.sent.lock().expect("wire log lock"))
    }
}

/// The dialer end of the loopback, recording every envelope it receives.
struct RecordingTransport {
    inner: LoopbackTransport,
    log: Arc<WireLog>,
}

#[async_trait]
impl Transport for RecordingTransport {
    async fn send(&self, envelope: &[u8]) -> Result<(), TransportError> {
        self.log
            .sent
            .lock()
            .expect("wire log lock")
            .push(String::from_utf8_lossy(envelope).into_owned());
        self.inner.send(envelope).await
    }

    async fn recv(&self) -> Result<Vec<u8>, TransportError> {
        let frame = self.inner.recv().await?;
        self.log
            .frames
            .lock()
            .expect("wire log lock")
            .push(String::from_utf8_lossy(&frame).into_owned());
        Ok(frame)
    }

    async fn close(&self) -> Result<(), TransportError> {
        self.inner.close().await
    }
}

/// One step into a decoded frame: an object member by its literal key, or an array element by its
/// index. A key that happens to contain a dot stays one step, so it can never be read as two
/// nesting levels.
#[derive(Clone, Copy)]
enum Step<'a> {
    Key(&'a str),
    Index(usize),
}

/// Render a path for a failure message. Every key is quoted, so a key that contains a dot reads as
/// the one member it is rather than as a path.
fn rendered(path: &[Step<'_>]) -> String {
    let mut out = String::from("$");
    for step in path {
        match step {
            Step::Key(key) => out.push_str(&format!("[\"{key}\"]")),
            Step::Index(index) => out.push_str(&format!("[{index}]")),
        }
    }
    out
}

/// Whether `path` is an object this suite's answers put a receipt body at: the response `payload`
/// itself, and `payload.receipt` for `memory.receipts.get`.
///
/// The comparison is segment by segment, so a member whose literal key is `payload.receipt` is one
/// key at one level and is not this path.
fn is_receipt_path(path: &[Step<'_>]) -> bool {
    matches!(
        path,
        [Step::Key("payload")] | [Step::Key("payload"), Step::Key("receipt")]
    )
}

/// Whether the answer under inspection may carry a receipt at all.
///
/// The receipt exception is a property of the answer's context, not of its shape: the same body
/// that is a committed receipt on a create is a disclosure on a refusal that staged nothing.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ExpectsReceipt {
    /// A create, its replay, the fresh-key repeat, or `memory.receipts.get`: the answer is a
    /// receipt body, or holds one at `payload.receipt`.
    Yes,
    /// A refusal that staged nothing — the terminal refusals this suite asserts, which answer no
    /// `details` at all. No receipt body and no attribution may appear.
    ///
    /// not this class: an `OUTCOME_UNCERTAIN` answer. It is a refusal too, but it names the staged
    /// receipt in `details.receipt_id` so the caller can resolve the outcome through
    /// `memory.receipts.get`; that receipt is real and is not a leak. No call site applies this
    /// walk to an uncertain answer.
    No,
}

/// Every occurrence of a caller in one decoded wire frame that must not be there, as
/// `path: needle`; empty when the frame carries it only in a receipt's own attribution.
///
/// `principals` are the principal ids the session must not disclose — every caller the answer
/// could name, not only this session's own, because a response that echoed a request value would
/// carry the other peer's id.
///
/// This walks the raw frame rather than the typed response, because `invoke_mapped` decodes into
/// the op's DTO and drops every member the DTO does not declare: a leak under an unmodelled key
/// is invisible after decoding but present here. Object keys are walked too: a principal used as
/// a key discloses it as surely as a value does.
fn caller_leaks<'a>(
    value: &'a Value,
    principals: &[&str],
    expects: ExpectsReceipt,
    path: &mut Vec<Step<'a>>,
) -> Vec<String> {
    let mut leaks = Vec::new();
    match value {
        Value::Object(members) => {
            for (key, member) in members {
                path.push(Step::Key(key.as_str()));
                if key.contains("mindcore_caller")
                    || principals.iter().any(|principal| key.contains(principal))
                {
                    leaks.push(format!("{}: the caller as a key", rendered(path)));
                    path.pop();
                    continue;
                }
                // A refusal names no receipt: one here would be a staged outcome the operation is
                // supposed to have left unmentioned.
                if key == "receipt_id" && expects == ExpectsReceipt::No {
                    leaks.push(format!("{}: a refusal carries a receipt", rendered(path)));
                    path.pop();
                    continue;
                }
                // A receipt's own `requester_principal_id` is the caller's attribution of the
                // request it made, required by the receipt polling contract, and is not the
                // admission binding. It is the one permitted occurrence: this receipt object, at
                // the receipt's own path, holding the expected scalar. Any other position,
                // nesting or value leaks — and on an answer that expects no receipt at all, every
                // position does.
                if key == "requester_principal_id" {
                    // `path` ends with this member; the exemption is a property of the object
                    // holding it.
                    let attributed = expects == ExpectsReceipt::Yes
                        && is_receipt_path(&path[..path.len() - 1])
                        && members.contains_key("receipt_id")
                        && member
                            .as_str()
                            .is_some_and(|value| principals.contains(&value));
                    if !attributed {
                        leaks.push(format!(
                            "{}: the caller outside a receipt's attribution",
                            rendered(path)
                        ));
                    }
                    path.pop();
                    continue;
                }
                leaks.extend(caller_leaks(member, principals, expects, path));
                path.pop();
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                path.push(Step::Index(index));
                leaks.extend(caller_leaks(item, principals, expects, path));
                path.pop();
            }
        }
        Value::String(text) => {
            for needle in std::iter::once(&"mindcore_caller").chain(principals) {
                if text.contains(needle) {
                    leaks.push(format!("{}: {needle}", rendered(path)));
                }
            }
        }
        _ => {}
    }
    leaks
}

/// Assert that every frame the responder answered since the last drain carries the caller
/// nowhere, and that the call was answered at all. `expects` says whether the answer may hold a
/// receipt; a refusal passes [`ExpectsReceipt::No`], so the attribution exception cannot excuse
/// one. A frame that is not JSON fails: nothing this suite's own faces send is anything else.
fn assert_caller_absent(session: &Session, context: &str, expects: ExpectsReceipt) {
    assert_caller_absent_for(session, &[PRINCIPAL], context, expects);
}

/// [`assert_caller_absent`] over a caller set the session must not name: the knowledge loopbacks
/// serve two principals and assert every answer against both.
fn assert_caller_absent_for(
    session: &Session,
    principals: &[&str],
    context: &str,
    expects: ExpectsReceipt,
) {
    let frames = session.wire.drain();
    assert!(!frames.is_empty(), "{context}: the call was answered");
    for frame in &frames {
        let parsed: Value = serde_json::from_str(frame)
            .unwrap_or_else(|error| panic!("{context}: the wire frame is JSON ({error}): {frame}"));
        let leaks = caller_leaks(&parsed, principals, expects, &mut Vec::new());
        assert!(
            leaks.is_empty(),
            "{context}: the answer carries the caller: {leaks:?}"
        );
    }
}

/// Assert that exactly one request envelope the dialer sent carries `scope` at the op's payload
/// path, that its `extensions.mindcore_caller.principal` is `principal`, and return the frames so
/// the caller can keep them for a later failure message.
///
/// Read from the request bytes rather than from the typed call: a dialer that stopped copying
/// `scope.extensions` onto its payload would send a scope without the forged namespace, and the
/// store-side refusal would then prove nothing about Connect's merge.
fn assert_sent_forgery(frames: &[String], op: &str, entry: &str, principal: &str) {
    let request = frames
        .iter()
        .filter_map(|frame| serde_json::from_str::<Value>(frame).ok())
        .find(|frame| frame.get("op").and_then(Value::as_str) == Some(op))
        .unwrap_or_else(|| panic!("{op} reached the wire: {frames:?}"));
    if !entry.is_empty() {
        assert_eq!(
            request
                .pointer("/payload/entry_id")
                .or_else(|| request.pointer("/payload/entry/entry_id"))
                .and_then(Value::as_str),
            Some(entry),
            "{op} carried the entry: {request}"
        );
    }
    assert_eq!(
        request
            .pointer("/payload/scope/extensions/mindcore_caller/principal")
            .and_then(Value::as_str),
        Some(principal),
        "{op} carried the forged caller at the scope path: {request}"
    );
    assert_eq!(
        request
            .pointer("/payload/scope/scope_id")
            .and_then(Value::as_str),
        Some(SPACE),
        "{op} named the admitted Mind, not the forged caller: {request}"
    );
}

/// The host admission for the raw-wire sessions: `raw_wire`'s own dialer key, mapped to the
/// fixture's principal. The admission decides the peer, so a raw handshake with any other key is
/// refused before the gates this suite asserts.
fn raw_wire_admission() -> MemoryAdmission {
    let peer_id = raw_wire::dialer_peer_id();
    Arc::new(move |peer: &str| match peer == peer_id {
        true => spoke_ok(principal()),
        false => spoke_reject(
            SpokeRejectCode::NotAuthorized,
            format!("peer {peer} holds no workflow admission"),
            None,
        ),
    })
}

/// Establish a real signed loopback session over `surface`, the dialer claiming exactly
/// `dialer_capabilities` in its hello. Handlers are installed before the dialer connects.
async fn connect(surface: &MindcoreSurface, dialer_capabilities: &[&str]) -> Session {
    connect_as(surface, &DIALER_SEED, dialer_capabilities).await
}

/// As [`connect`], with the dialer's identity chosen by `seed`: the host's admissions decide
/// whether the resulting peer is served, not the transport.
async fn connect_as(
    surface: &MindcoreSurface,
    seed: &[u8; 32],
    dialer_capabilities: &[&str],
) -> Session {
    let pair = loopback_transport_pair();
    let wire = Arc::new(WireLog::default());
    let dialer = peer_id(seed);
    let responder_pubkey = SigningKey::from_bytes(&RESPONDER_SEED)
        .verifying_key()
        .to_bytes();
    let responder_peer_id = derive_peer_id_from_ed25519_pubkey(&responder_pubkey);

    let responder = connect_responder(ConnectResponderOptions {
        transport: Arc::new(pair.server),
        identity: RemoteIdentity {
            seed: RESPONDER_SEED,
        },
        manifest: surface.manifest.clone(),
        allowlist: vec![dialer.clone()],
        peer_keys: HashMap::from([(
            dialer.clone(),
            SigningKey::from_bytes(seed).verifying_key().to_bytes(),
        )]),
        ports: Some(Arc::clone(&surface.ports)),
        invoke_timeout_ms: Some(10_000),
    })
    .await;
    expect_ok(surface.register(&responder), "install the served handlers");

    let adapter = connect_remote_adapter(RemoteAdapterOptions {
        transport: Arc::new(RecordingTransport {
            inner: pair.client,
            log: Arc::clone(&wire),
        }),
        local_identity: RemoteIdentity { seed: *seed },
        local_manifest: hello("loopback-dialer", dialer_capabilities),
        remote_pubkey: responder_pubkey,
        allowlist: vec![responder_peer_id.clone()],
        invoke_timeout_ms: Some(10_000),
        capability_token: None,
    })
    .await
    .expect("signed loopback handshake must succeed");

    assert_eq!(adapter.state(), RemoteAdapterState::Established);
    assert_eq!(
        adapter.remote_peer_id().as_deref(),
        Some(responder_peer_id.as_str())
    );
    assert_eq!(
        responder.remote_peer_id().as_deref(),
        Some(dialer.as_str()),
        "the responder serves the authenticated peer, not a claimed one"
    );
    Session {
        adapter,
        responder,
        wire,
    }
}

fn recall_arguments() -> Value {
    recall_arguments_in(SPACE)
}

fn recall_arguments_in(space: &str) -> Value {
    json!({
        "request": {
            "space_id": space,
            "subject_id": SUBJECT,
            "selection": { "mode": "subject_all" },
            "budget": { "max_items": 10 },
        },
        "max_tokens": 1_000,
    })
}

fn recall_request() -> MemoryRecallRequest {
    serde_json::from_value(json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "selection": { "mode": "subject_all" },
        "budget": { "max_items": 10 },
    }))
    .expect("recall request JSON matches the schema")
}

fn deliberate_arguments() -> Value {
    deliberate_arguments_in(SPACE)
}

/// The same deliberation inside `space`: the wire never names a carrier — the Mind's registry row
/// decides it — so only the target reference varies per Mind.
fn deliberate_arguments_in(space: &str) -> Value {
    json!({
        "space_id": space,
        "subject_id": SUBJECT,
        "target": {
            "carrier": { "kind": "knowledge_entry", "namespace": "mindcore", "id": CARRIER },
            "target_id": "prop_north_gate",
            "kind": "proposition",
        },
        "evidence": [{ "id": "ev_gate", "text": "the north gate is closed at dusk" }],
        "timeout_ms": 5_000,
    })
}

/// One served capture inside `space`: the freeze-gated write the frozen-Mind case needs.
fn absorb_arguments_in(space: &str) -> Value {
    json!({
        "request": {
            "space_id": space,
            "subject_id": SUBJECT,
            "capture_scope": { "visibility": "shared" },
            "content": { "text": "a capture into the Mind" },
            "idempotency_key": "cap_loopback",
        }
    })
}

/// The cognition read's arguments: the Mind id alone, the same id its memory requests carry.
fn cognition_arguments(space: &str) -> Value {
    json!({ "space_id": space })
}

/// The record ids one recall pack carries, sorted: the pack promises no order of its own.
fn packed_record_ids(result: &Value) -> Vec<&str> {
    let mut ids: Vec<&str> = result["records"]
        .as_array()
        .expect("the pack carries a records array")
        .iter()
        .map(|record| {
            record["record_id"]
                .as_str()
                .expect("a packed record carries its id")
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// The knowledge entry ids one recall pack carries, in pack order.
fn packed_knowledge_ids(result: &Value) -> Vec<&str> {
    result["knowledge"]
        .as_array()
        .expect("the pack carries a knowledge array")
        .iter()
        .map(|entry| {
            entry["entry_id"]
                .as_str()
                .expect("a knowledge entry carries its id")
        })
        .collect()
}

/// The stored revision of one carrier entry, wherever its collection lives.
async fn carrier_revision(pool: &sqlx::SqlitePool, collection: &str, entry_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT revision FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
    )
    .bind(collection)
    .bind(entry_id)
    .fetch_one(pool)
    .await
    .expect("read the carrier revision")
}

/// The assessor ids of one carrier's committed cognition rows, in append order.
async fn carrier_assessors(
    pool: &sqlx::SqlitePool,
    collection: &str,
    entry_id: &str,
) -> Vec<String> {
    let payloads: Vec<String> = sqlx::query_scalar(
        "SELECT payload FROM cognition_records \
         WHERE collection = ? AND entry_id = ? ORDER BY append_seq ASC",
    )
    .bind(collection)
    .bind(entry_id)
    .fetch_all(pool)
    .await
    .expect("read the carrier's cognition rows");
    payloads
        .iter()
        .map(|payload| {
            let record: Value = serde_json::from_str(payload).expect("cognition row payload");
            record["assessor"]["id"]
                .as_str()
                .expect("a cognition record names its assessor")
                .to_string()
        })
        .collect()
}

/// One collection's observable state: its entry and cognition row counts, plus one named carrier's
/// revision and update stamp.
#[derive(Debug, PartialEq, Eq)]
struct CollectionState {
    entries: i64,
    cognition: i64,
    revision: i64,
    updated_at: String,
}

async fn collection_state(
    pool: &sqlx::SqlitePool,
    collection: &str,
    carrier: &str,
) -> CollectionState {
    let entries: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM knowledge_entries WHERE collection = ?")
            .bind(collection)
            .fetch_one(pool)
            .await
            .expect("count the collection's entries");
    let cognition: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM cognition_records WHERE collection = ?")
            .bind(collection)
            .fetch_one(pool)
            .await
            .expect("count the collection's cognition rows");
    let (revision, updated_at): (i64, String) = sqlx::query_as(
        "SELECT revision, updated_at FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
    )
    .bind(collection)
    .bind(carrier)
    .fetch_one(pool)
    .await
    .expect("read the carrier's revision and update stamp");
    CollectionState {
        entries,
        cognition,
        revision,
        updated_at,
    }
}

fn expect_ok<T>(result: SpokeResult<T>, context: &str) -> T {
    match result {
        SpokeResult::Ok(value) => value,
        SpokeResult::Reject(reject) => panic!("{context}: unexpectedly rejected with {reject:?}"),
    }
}

fn expect_reject<T: std::fmt::Debug>(result: SpokeResult<T>, context: &str) -> SpokeReject {
    match result {
        SpokeResult::Ok(value) => panic!("{context}: unexpectedly accepted with {value:?}"),
        SpokeResult::Reject(reject) => reject,
    }
}

/// The dispatch-deny branch: the client sees `CapabilityPortMissing` and the pinned raw wire code.
fn assert_wire_deny<T: std::fmt::Debug>(result: SpokeResult<T>, op: &str, wire_code: &str) {
    let reject = expect_reject(result, op);
    assert_eq!(
        reject.code,
        SpokeRejectCode::CapabilityPortMissing,
        "{op} must answer the dispatch-deny row: {reject:?}"
    );
    assert_eq!(
        reject
            .details
            .as_ref()
            .and_then(|details| details.get("wire_code"))
            .and_then(Value::as_str),
        Some(wire_code),
        "pinned raw reject code for {op}"
    );
    assert!(
        reject.message.contains(op),
        "reject message names the refused op: {}",
        reject.message
    );
}

/// A remote consumer reaches the store: `recall_context` runs the real workflow over the fixture's
/// protocol memory tables, admitted from the responder's authenticated peer, and the manifest
/// advertises exactly the tools it installs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_consumer_recalls_context_through_the_composed_surface() {
    let fixture = Fixture::open().await;
    let (_dir, serve) = fixture.serve();
    let surface = surface(
        &serve,
        faces(
            hello("runtime", &["spoke-baseline"]),
            None,
            MindProviders::default(),
        ),
    );

    assert_eq!(
        capabilities(&surface),
        [
            "spoke-judgment",
            RECALL_CONTEXT,
            ABSORB,
            DELIBERATE,
            READ_COGNITION
        ],
        "the manifest advertises the judgment family and every workflow tool"
    );
    assert_eq!(
        surface
            .manifest
            .tools
            .iter()
            .map(|tool| tool.capability_id.as_str())
            .collect::<Vec<_>>(),
        [RECALL_CONTEXT, ABSORB, DELIBERATE, READ_COGNITION],
        "the registry row routes every Mind, so the deliberation tool is always installed"
    );

    let session = connect(&surface, &[RECALL_CONTEXT]).await;
    let result = expect_ok(
        session
            .adapter
            .invoke_tool(RECALL_CONTEXT, recall_arguments())
            .await,
        "the served recall_context tool",
    );

    assert_eq!(packed_record_ids(&result), ["rec_market", "rec_north_gate"]);
    assert!(
        result["tokens_used"]
            .as_u64()
            .is_some_and(|tokens| tokens > 0),
        "the pack reports the tokens it used: {result}"
    );
    assert_eq!(
        result["knowledge"],
        json!([]),
        "the workflow binding serves no knowledge slot of its own"
    );
}

/// The knowledge slot keys to the Mind the request names: a second admitted peer recalling its own
/// Mind's space reads that Mind's collection entries under its own grant, never the first Mind's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn another_admitted_mind_recalls_its_own_knowledge_slot() {
    let fixture = Fixture::open().await;
    fixture.seed_second_mind().await;
    fixture
        .grant_in_second("grt_recall_peer_two", PEER_TWO_PRINCIPAL, &["recall"])
        .await;
    fixture
        .grant_in_second(
            "grt_knowledge_peer_two",
            PEER_TWO_PRINCIPAL,
            &["knowledge_read"],
        )
        .await;
    let (_dir, serve) = fixture.serve();
    let mut faces = faces(hello("runtime", &[]), None, MindProviders::default());
    faces.principals = two_principal_admission();
    let surface = surface(&serve, faces);

    let session = connect_as(&surface, &PEER_TWO_SEED, &[RECALL_CONTEXT]).await;
    let mut arguments = recall_arguments_in(SPACE_TWO);
    arguments["max_tokens"] = json!(4_000);
    let result = expect_ok(
        session.adapter.invoke_tool(RECALL_CONTEXT, arguments).await,
        "recall by a peer admitted to the second Mind",
    );

    assert_eq!(
        packed_record_ids(&result),
        ["rec_market_two", "rec_north_gate_two"]
    );
    assert_eq!(
        packed_knowledge_ids(&result),
        [CARRIER_TWO],
        "the peer's own knowledge_read grant fills the slot from its own Mind: {result}"
    );
    assert_eq!(
        result["knowledge"][0]["canonical_name"], "the second mind's carrier",
        "the slot carries the second Mind's own content: {result}"
    );
    assert_eq!(
        result["knowledge"][0]["body"]["summary"],
        "the knowledge of the second mind, not the first",
        "the slot carries the second Mind's own summary: {result}"
    );
}

/// A configured route never narrows recall's authorization: the route's caller keeps the memory
/// grant but loses `knowledge_read`, and still reads its records — the slot is best effort and
/// degrades to empty exactly as it does with no collection bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_route_caller_without_knowledge_read_recalls_with_an_empty_slot() {
    let fixture = Fixture::open().await;
    fixture.revoke("grt_knowledge").await;
    let (_dir, serve) = fixture.serve();
    let surface = surface(
        &serve,
        faces(hello("runtime", &[]), None, MindProviders::default()),
    );
    assert!(
        capabilities(&surface).contains(&DELIBERATE),
        "the carrier route is wired: {:?}",
        capabilities(&surface)
    );

    let session = connect(&surface, &[RECALL_CONTEXT]).await;
    let result = expect_ok(
        session
            .adapter
            .invoke_tool(RECALL_CONTEXT, recall_arguments())
            .await,
        "recall by the route's caller without a knowledge grant",
    );

    assert_eq!(
        packed_record_ids(&result),
        ["rec_market", "rec_north_gate"],
        "the caller's memory records are served: {result}"
    );
    assert_eq!(
        result["knowledge"],
        json!([]),
        "a caller without knowledge_read gets an empty slot, never a refused recall"
    );
}

/// A remote consumer reaches the carrier: `deliberate` reads the real memory page and the real
/// carrier entry through the composed surface and returns the reasoner's validated verdict beside
/// a judgment the released validator accepts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_consumer_deliberates_through_the_composed_surface() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    let (_dir, serve) = fixture.serve();
    let providers = MindProviders {
        reasoner: Some(Arc::new(NoChangeReasoner)),
        ..MindProviders::default()
    };
    let surface = surface(&serve, faces(hello("runtime", &[]), None, providers));
    assert!(
        capabilities(&surface).contains(&DELIBERATE),
        "a wired deliberation route is advertised: {:?}",
        capabilities(&surface)
    );

    let session = connect(&surface, &[DELIBERATE]).await;
    let result = expect_ok(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "the served deliberate tool",
    );

    assert_eq!(result["status"], "no_change");
    assert_eq!(
        result["verdict"]["rationale"],
        "the evidence carries nothing the carrier does not already hold"
    );
    assert_eq!(result["verdict"]["record_ids"], json!(["rec_north_gate"]));
    assert_judgment(&result, "mindcore:no_change");
    let revision: i64 = sqlx::query_scalar(
        "SELECT revision FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
    )
    .bind(COLLECTION)
    .bind(CARRIER)
    .fetch_one(&pool)
    .await
    .expect("read the carrier revision");
    assert_eq!(
        revision, 1,
        "a no_change verdict leaves the carrier untouched"
    );
}

/// A judgment's identity as the served response carries it.
fn judgment_id(response: &Value) -> &str {
    response["judgment"]["judgment_id"]
        .as_str()
        .expect("the served judgment carries its id")
}

/// One signed session serving `deliberate` over a fresh fixture: the seeded rows give every call
/// the same attempt content, so the calls differ only in what the host's policy does.
async fn deliberation_session(
    rationale: &'static str,
    policy: Arc<dyn PolicyHook>,
) -> (TempDir, MindcoreServe, Session) {
    let fixture = Fixture::open().await;
    let (dir, serve) = fixture.serve();
    let surface = surface(
        &serve,
        faces(
            hello("runtime", &[]),
            None,
            MindProviders {
                reasoner: Some(Arc::new(SettlingReasoner(rationale))),
                policy_hook: Some(policy),
                ..MindProviders::default()
            },
        ),
    );
    let session = connect(&surface, &[DELIBERATE]).await;
    (dir, serve, session)
}

/// The judgment identity is the attempt content, not the clock: equal content answers an equal id
/// on a repeated call, while another verdict or another branch answers another id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deliberation_judgment_identity_follows_the_attempt_content() {
    let (_held_dir, _held_serve, held) =
        deliberation_session(SETTLING_RATIONALE, Arc::new(HoldingPolicy)).await;
    let first = expect_ok(
        held.adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "the first served deliberation",
    );
    let again = expect_ok(
        held.adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "the same served deliberation again",
    );
    assert_eq!(first["status"], "needs_review");
    assert_eq!(again["status"], "needs_review");
    assert_eq!(
        judgment_id(&first),
        judgment_id(&again),
        "equal attempt content answers the same judgment identity: {first} / {again}"
    );

    // A second store answers the same attempt content: nothing the store generates — the draft's
    // own record id, the carrier revision, the clock — reaches the identity.
    let (_peer_dir, _peer_serve, peer) =
        deliberation_session(SETTLING_RATIONALE, Arc::new(HoldingPolicy)).await;
    let peer = expect_ok(
        peer.adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "the same deliberation over another store",
    );
    assert_eq!(peer["status"], "needs_review");
    assert_eq!(
        judgment_id(&peer),
        judgment_id(&first),
        "equal attempt content answers the same identity over another store: {peer} / {first}"
    );

    let (_settled_dir, _settled_serve, settled) =
        deliberation_session(SETTLING_RATIONALE, Arc::new(AutoPolicy)).await;
    let settled = expect_ok(
        settled
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "the settling served deliberation",
    );
    assert_eq!(settled["status"], "settled");
    assert_ne!(
        judgment_id(&settled),
        judgment_id(&first),
        "the same verdict on another branch answers another identity: {settled} / {first}"
    );

    let (_restated_dir, _restated_serve, restated) = deliberation_session(
        "the cited record leaves the north gate undecided",
        Arc::new(HoldingPolicy),
    )
    .await;
    let restated = expect_ok(
        restated
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "the restated served deliberation",
    );
    assert_eq!(restated["status"], "needs_review");
    assert_ne!(
        judgment_id(&restated),
        judgment_id(&first),
        "another verdict on the same branch answers another identity: {restated} / {first}"
    );
}

/// The `needs_review` branch over a signed session: the host's holding policy keeps the promotion
/// as a draft, so the response carries `draft` and writes nothing, beside the same valid judgment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_consumer_holds_a_deliberation_with_a_valid_judgment() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    let (_dir, serve) = fixture.serve();
    let providers = MindProviders {
        reasoner: Some(Arc::new(SettlingReasoner(SETTLING_RATIONALE))),
        policy_hook: Some(Arc::new(HoldingPolicy)),
        ..MindProviders::default()
    };
    let surface = surface(&serve, faces(hello("runtime", &[]), None, providers));

    let session = connect(&surface, &[DELIBERATE]).await;
    let result = expect_ok(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "the served deliberate tool under a holding policy",
    );

    assert_eq!(result["status"], "needs_review");
    assert_eq!(
        result["verdict"]["recommendation"],
        json!({ "kind": "promote", "target_kind": "belief" }),
        "the held verdict keeps the reasoner's recommendation: {result}"
    );
    assert_eq!(result["verdict"]["rationale"], SETTLING_RATIONALE);
    assert_eq!(result["verdict"]["evidence_ids"], json!(["ev_gate"]));
    assert_eq!(result["verdict"]["record_ids"], json!(["rec_north_gate"]));
    assert_eq!(result["draft"]["kind"], "belief");
    assert!(
        result.get("record").is_none(),
        "a held draft settles nothing: {result}"
    );
    assert_judgment(&result, "mindcore:needs_review");
    assert_eq!(
        carrier_revision(&pool, COLLECTION, CARRIER).await,
        1,
        "a needs_review verdict writes no cognition record"
    );
}

/// The `settled` branch over a signed session: the host's policy settles the promotion, so the
/// response carries `record` and `mind_state_version` beside the same valid judgment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_consumer_settles_a_deliberation_with_a_valid_judgment() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    let (_dir, serve) = fixture.serve();
    let providers = MindProviders {
        reasoner: Some(Arc::new(SettlingReasoner(SETTLING_RATIONALE))),
        policy_hook: Some(Arc::new(AutoPolicy)),
        ..MindProviders::default()
    };
    let surface = surface(&serve, faces(hello("runtime", &[]), None, providers));

    let session = connect(&surface, &[DELIBERATE]).await;
    let result = expect_ok(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "the served deliberate tool under an automatic policy",
    );

    assert_eq!(result["status"], "settled");
    assert_eq!(
        result["verdict"]["recommendation"],
        json!({ "kind": "promote", "target_kind": "belief" }),
        "the settled verdict keeps the reasoner's recommendation: {result}"
    );
    assert_eq!(result["verdict"]["rationale"], SETTLING_RATIONALE);
    assert_eq!(result["verdict"]["evidence_ids"], json!(["ev_gate"]));
    assert_eq!(result["verdict"]["record_ids"], json!(["rec_north_gate"]));
    assert_eq!(result["record"]["assessor"]["id"], PRINCIPAL);
    assert_eq!(result["mind_state_version"], 2);
    assert_judgment(&result, "mindcore:settled");
    assert_eq!(
        carrier_revision(&pool, COLLECTION, CARRIER).await,
        2,
        "the settled branch commits the cognition record"
    );
}

/// The served cognition read over a signed session: a Mind that has judged nothing answers
/// `records: []` and a null `mind_state`; after a settlement the same Mind answers the committed
/// record and its derived `MindState`; a Mind the caller holds no grant in answers exactly the
/// refusal a space that is no Mind at all answers; and a caller admitted to a Mind without
/// `knowledge_read` answers the distinct capability-coverage refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_cognition_serves_the_judgment_and_refuses_an_unadmitted_mind() {
    let fixture = Fixture::open().await;
    fixture.seed_second_mind().await;
    // The second peer is admitted to the second Mind through `recall` alone: a live grant, so
    // admission passes, but no `knowledge_read` coverage. It is granted before `serve` consumes
    // the fixture, and it cannot affect the first principal, who holds nothing in that Mind.
    fixture
        .grant_in_second("grt_recall_only", PEER_TWO_PRINCIPAL, &["recall"])
        .await;
    let (_dir, serve) = fixture.serve();
    let providers = MindProviders {
        reasoner: Some(Arc::new(SettlingReasoner(SETTLING_RATIONALE))),
        policy_hook: Some(Arc::new(AutoPolicy)),
        ..MindProviders::default()
    };
    let mut faces = faces(hello("runtime", &[]), None, providers);
    // Two principals: the dialer reaches the first Mind, the second peer reaches only what it is
    // granted in the second.
    faces.principals = two_principal_admission();
    let surface = surface(&serve, faces);

    let session = connect(&surface, &[DELIBERATE, READ_COGNITION]).await;

    let fresh = expect_ok(
        session
            .adapter
            .invoke_tool(READ_COGNITION, cognition_arguments(SPACE))
            .await,
        "the cognition read before any judgment",
    );
    assert_eq!(fresh["carrier_revision"], 1);
    assert_eq!(fresh["records"], json!([]));
    assert_eq!(
        fresh["mind_state"],
        Value::Null,
        "a Mind with no judgment answers a null state: {fresh}"
    );

    let settled = expect_ok(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "the settling deliberation",
    );
    assert_eq!(settled["status"], "settled");

    let read = expect_ok(
        session
            .adapter
            .invoke_tool(READ_COGNITION, cognition_arguments(SPACE))
            .await,
        "the cognition read after a judgment",
    );
    assert_eq!(
        read["carrier_revision"], 2,
        "the read serves the revision the settlement wrote: {read}"
    );
    let records = read["records"]
        .as_array()
        .expect("the read serves a records array");
    assert_eq!(records.len(), 1, "one committed judgment is served: {read}");
    assert_eq!(records[0]["record_id"], settled["record"]["record_id"]);
    assert_eq!(records[0]["assessor"]["id"], PRINCIPAL);
    assert_eq!(records[0]["holder"]["id"], CARRIER);
    assert!(
        read["mind_state"].is_object(),
        "the derived MindState is served beside the record: {read}"
    );
    assert_eq!(read["mind_state"]["mind_state_id"], SPACE);
    assert_eq!(read["mind_state"]["holder_entry_id"], CARRIER);

    // The second Mind exists and carries a registry row, yet this peer holds no grant in it: the
    // refusal must be the byte-same answer a space that is no Mind at all gives.
    let unadmitted = expect_reject(
        session
            .adapter
            .invoke_tool(READ_COGNITION, cognition_arguments(SPACE_TWO))
            .await,
        "the cognition read of a Mind the caller holds no grant in",
    );
    let absent = expect_reject(
        session
            .adapter
            .invoke_tool(READ_COGNITION, cognition_arguments("spc_no_such_mind"))
            .await,
        "the cognition read of a space that is no Mind at all",
    );
    assert_eq!(unadmitted.code, SpokeRejectCode::NotAuthorized);
    assert!(
        unadmitted.details.is_none(),
        "the refusal carries no details: {unadmitted:?}"
    );
    assert_eq!(
        unadmitted, absent,
        "an unadmitted Mind and an absent Mind must be the byte-same refusal"
    );

    // A peer admitted to the second Mind, but only through `recall`: admission passes and the
    // missing `knowledge_read` coverage is what answers. The plan keeps those two refusals
    // distinct — the coverage refusal may name the capability and the Mind, because the caller is
    // already known to hold a live grant here.
    let peer = connect_as(&surface, &PEER_TWO_SEED, &[READ_COGNITION]).await;
    let uncovered = expect_reject(
        peer.adapter
            .invoke_tool(READ_COGNITION, cognition_arguments(SPACE_TWO))
            .await,
        "the cognition read of a Mind the caller is admitted to without knowledge_read",
    );
    assert_eq!(uncovered.code, SpokeRejectCode::NotAuthorized);
    assert!(
        uncovered.message.contains("knowledge_read"),
        "the coverage refusal names the capability the caller lacks: {uncovered:?}"
    );
    assert!(
        uncovered
            .details
            .as_ref()
            .and_then(|details| details.get("retry_class"))
            .and_then(Value::as_str)
            == Some("terminal"),
        "a coverage gap cannot be retried away: {uncovered:?}"
    );
    assert_ne!(
        uncovered, unadmitted,
        "an admitted caller short of one capability is not an unadmitted caller"
    );
    let peer_absent = expect_reject(
        peer.adapter
            .invoke_tool(READ_COGNITION, cognition_arguments("spc_no_such_mind"))
            .await,
        "the second peer reading a space that is no Mind at all",
    );
    assert_eq!(
        peer_absent, unadmitted,
        "every unadmitted caller answers the same target-free refusal"
    );
}

/// One deliberation response's judgment, held to the additive contract on every branch: the
/// released validator accepts the wire value, it carries the request target and the cited record's
/// stored revision, its outcome follows the response branch and its basis is the verdict rationale.
fn assert_judgment(response: &Value, outcome: &str) {
    let judgment = &response["judgment"];
    expect_ok(
        validate_judgment(judgment),
        "the released validator accepts the served judgment",
    );
    assert!(
        judgment["judgment_id"]
            .as_str()
            .is_some_and(|id| id.starts_with("jd_")),
        "the judgment carries its deterministic identity: {judgment}"
    );
    assert_eq!(judgment["judgment_kind"], "mindcore:deliberate");
    assert_eq!(
        judgment["target"],
        json!({ "kind": "proposition", "id": "prop_north_gate", "namespace": "mindcore" }),
        "the judgment carries the request target and no revision: {judgment}"
    );
    assert_eq!(
        judgment["evidence"],
        json!([{
            "kind": "memory_record",
            "namespace": "mindcore",
            "id": "rec_north_gate",
            "revision": 1,
        }]),
        "the evidence pins the cited record's stored revision: {judgment}"
    );
    assert_eq!(
        judgment["verdict"]["outcome"], outcome,
        "the outcome follows the response branch: {judgment}"
    );
    assert!(
        judgment["verdict"].get("confidence").is_none(),
        "no confidence is invented: {judgment}"
    );
    assert_eq!(
        judgment["basis"], response["verdict"]["rationale"],
        "the basis is the verdict's rationale: {judgment}"
    );
    assert!(
        judgment["created_at"]
            .as_str()
            .is_some_and(|stamp| stamp.len() >= 20 && stamp.ends_with('Z')),
        "created_at is a UTC RFC 3339 instant: {judgment}"
    );
}

/// One surface, two admitted Minds: each peer's call names its own space id and settles the
/// cognition onto its own carrier entry, and neither call writes the other Mind's carrier. The
/// `space_id` the request carries decides the collection, so the registry row is the only route.
/// The second Mind's row names `ke_carrier_mora` — deliberately not the production constant — so
/// settling onto it is also the regression that the carrier id comes from the registry row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_minds_settle_onto_their_own_carriers() {
    let fixture = Fixture::open().await;
    fixture.seed_second_mind().await;
    fixture
        .grant_in_second(
            "grt_settle_peer_two",
            PEER_TWO_PRINCIPAL,
            &["recall", "knowledge_read", "knowledge_write"],
        )
        .await;
    let pool = fixture.store.pool();
    let (_dir, serve) = fixture.serve();
    let mut faces = faces(
        hello("runtime", &[]),
        None,
        MindProviders {
            reasoner: Some(Arc::new(SettlingReasoner(SETTLING_RATIONALE))),
            policy_hook: Some(Arc::new(AutoPolicy)),
            ..MindProviders::default()
        },
    );
    faces.principals = two_principal_admission();
    let surface = surface(&serve, faces);

    let first = connect(&surface, &[DELIBERATE, RECALL_CONTEXT]).await;
    let untouched = collection_state(&pool, COLLECTION_TWO, CARRIER_TWO).await;
    let settled = expect_ok(
        first
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "deliberate by the first principal",
    );
    assert_eq!(settled["status"], "settled");
    assert_eq!(settled["record"]["holder"]["id"], CARRIER);
    assert_eq!(settled["record"]["assessor"]["id"], PRINCIPAL);
    assert_eq!(
        settled["mind_state_version"], 2,
        "the write descends from its own carrier"
    );
    assert_eq!(carrier_revision(&pool, COLLECTION, CARRIER).await, 2);
    assert_eq!(
        carrier_revision(&pool, COLLECTION_TWO, CARRIER_TWO).await,
        1,
        "the second principal's carrier keeps its seeded revision"
    );
    assert_eq!(
        carrier_assessors(&pool, COLLECTION, CARRIER).await,
        [PRINCIPAL]
    );
    assert!(
        carrier_assessors(&pool, COLLECTION_TWO, CARRIER_TWO)
            .await
            .is_empty(),
        "the second collection carries no assessment"
    );
    assert_eq!(
        collection_state(&pool, COLLECTION_TWO, CARRIER_TWO).await,
        untouched,
        "the first call leaves the second collection's rows, carrier revision and update stamp unchanged"
    );

    let second = connect_as(&surface, &PEER_TWO_SEED, &[DELIBERATE, RECALL_CONTEXT]).await;
    let untouched = collection_state(&pool, COLLECTION, CARRIER).await;
    let settled = expect_ok(
        second
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments_in(SPACE_TWO))
            .await,
        "deliberate by the second principal",
    );
    assert_eq!(settled["status"], "settled");
    assert_eq!(settled["record"]["holder"]["id"], CARRIER_TWO);
    assert_eq!(settled["record"]["assessor"]["id"], PEER_TWO_PRINCIPAL);
    assert_eq!(
        settled["mind_state_version"], 2,
        "the second write descends from its own carrier, never the first's"
    );
    assert_eq!(
        carrier_revision(&pool, COLLECTION_TWO, CARRIER_TWO).await,
        2
    );
    assert_eq!(
        carrier_revision(&pool, COLLECTION, CARRIER).await,
        2,
        "the first principal's carrier keeps the revision its own call left"
    );
    assert_eq!(
        carrier_assessors(&pool, COLLECTION_TWO, CARRIER_TWO).await,
        [PEER_TWO_PRINCIPAL]
    );
    assert_eq!(
        carrier_assessors(&pool, COLLECTION, CARRIER).await,
        [PRINCIPAL],
        "no second assessment landed on the first principal's carrier"
    );
    assert_eq!(
        collection_state(&pool, COLLECTION, CARRIER).await,
        untouched,
        "the second call leaves the first collection's rows, carrier revision and update stamp unchanged"
    );

    let mut recall = recall_arguments();
    recall["max_tokens"] = json!(4_000);
    let first_pack = expect_ok(
        first
            .adapter
            .invoke_tool(RECALL_CONTEXT, recall.clone())
            .await,
        "recall by the first principal",
    );
    assert_eq!(
        packed_record_ids(&first_pack),
        ["rec_market", "rec_north_gate"],
        "the first principal's memory page is served: {first_pack}"
    );
    assert_eq!(
        packed_knowledge_ids(&first_pack),
        [CARRIER],
        "the first principal's slot lists only its own routed collection: {first_pack}"
    );
    assert_eq!(
        first_pack["knowledge"][0]["canonical_name"], "ke_carrier's carrier",
        "the first slot is the first Mind's entry: {first_pack}"
    );
    let mut second_recall = recall_arguments_in(SPACE_TWO);
    second_recall["max_tokens"] = json!(4_000);
    let second_pack = expect_ok(
        second
            .adapter
            .invoke_tool(RECALL_CONTEXT, second_recall)
            .await,
        "recall by the second principal",
    );
    assert_eq!(
        packed_record_ids(&second_pack),
        ["rec_market_two", "rec_north_gate_two"],
        "the second principal's memory page is served: {second_pack}"
    );
    assert_eq!(
        packed_knowledge_ids(&second_pack),
        [CARRIER_TWO],
        "the second Mind's slot names its own carrier entry — the regression that the carrier id          comes from the registry row, not the production constant: {second_pack}"
    );
    assert_eq!(
        second_pack["knowledge"][0]["canonical_name"], "the second mind's carrier",
        "the second slot carries the second Mind's entry, never the first's: {second_pack}"
    );
    assert_eq!(
        second_pack["knowledge"][0]["body"]["summary"],
        "the knowledge of the second mind, not the first",
        "the content distinguishes the two Minds: {second_pack}"
    );
    let contamination: Vec<&str> = first_pack["knowledge"]
        .as_array()
        .expect("first knowledge")
        .iter()
        .filter_map(|entry| entry["entry_id"].as_str())
        .filter(|id| *id == CARRIER_TWO)
        .collect();
    assert!(
        contamination.is_empty(),
        "the first Mind's recall never carries the second Mind's entry: {first_pack}"
    );

    let first_before = collection_state(&pool, COLLECTION, CARRIER).await;
    let second_before = collection_state(&pool, COLLECTION_TWO, CARRIER_TWO).await;
    // The retired `holder` member: the closed argument object refuses it before any carrier access.
    let mut with_holder = deliberate_arguments();
    with_holder["holder"] =
        json!({ "kind": "knowledge_entry", "namespace": "mindcore", "id": CARRIER });
    let reject = expect_reject(
        first.adapter.invoke_tool(DELIBERATE, with_holder).await,
        "deliberate still naming the retired holder member",
    );
    assert_eq!(
        reject.code,
        SpokeRejectCode::InvalidInput,
        "a retired holder member is the closed object's unknown-field refusal: {reject:?}"
    );
    assert_eq!(
        collection_state(&pool, COLLECTION, CARRIER).await,
        first_before,
        "a refused write settles nothing in either collection"
    );
    assert_eq!(
        collection_state(&pool, COLLECTION_TWO, CARRIER_TWO).await,
        second_before,
    );
}

/// A space that is no Mind — grants live, no registry row — reaches no carrier: a peer holding
/// `recall` and both knowledge grants in it is still refused, target-free, and nothing is written;
/// the same peer still recalls its memory there, with an empty knowledge slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_space_with_no_registry_row_reaches_no_carrier() {
    let fixture = Fixture::open().await;
    // The second space without its Mind: the space, subjects and grants live, but no registry row.
    fixture.seed_second_mind().await;
    sqlx::query("DELETE FROM minds WHERE space_id = ?")
        .bind(SPACE_TWO)
        .execute(&fixture.store.pool())
        .await
        .expect("drop the registry row");
    fixture
        .grant_in_second(
            "grt_route_peer_two",
            PEER_TWO_PRINCIPAL,
            &["recall", "knowledge_read", "knowledge_write"],
        )
        .await;
    let pool = fixture.store.pool();
    let (_dir, serve) = fixture.serve();
    let mut faces = faces(
        hello("runtime", &[]),
        None,
        MindProviders {
            reasoner: Some(Arc::new(NoChangeReasoner)),
            ..MindProviders::default()
        },
    );
    faces.principals = two_principal_admission();
    let surface = surface(&serve, faces);

    let session = connect_as(&surface, &PEER_TWO_SEED, &[DELIBERATE, RECALL_CONTEXT]).await;
    let reject = expect_reject(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments_in(SPACE_TWO))
            .await,
        "deliberate by a peer in a space with no registry row",
    );
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
    assert!(
        reject
            .message
            .contains("the caller is not admitted to the named mind"),
        "the refusal is the admission rule: {reject:?}"
    );
    let refusal = format!("{reject:?}");
    assert!(
        !refusal.contains(COLLECTION),
        "the refusal names no collection: {refusal}"
    );
    let revision: i64 = sqlx::query_scalar(
        "SELECT revision FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
    )
    .bind(COLLECTION)
    .bind(CARRIER)
    .fetch_one(&pool)
    .await
    .expect("read the carrier revision");
    assert_eq!(
        revision, 1,
        "a refused carrier write leaves the entry untouched"
    );

    let mut arguments = recall_arguments_in(SPACE_TWO);
    arguments["max_tokens"] = json!(4_000);
    let recalled = expect_ok(
        session.adapter.invoke_tool(RECALL_CONTEXT, arguments).await,
        "recall by a peer in a space with no registry row",
    );
    assert_eq!(
        packed_record_ids(&recalled),
        ["rec_market_two", "rec_north_gate_two"]
    );
    assert_eq!(
        recalled["knowledge"],
        json!([]),
        "a space with no registry row reads its memory with an empty knowledge slot"
    );
}

/// A frozen Mind, through the composed surface: a granted caller is served every operation that
/// is not freeze-gated — its recall and the cognition read — while a freeze-gated one outside
/// SPOKE's maintenance allowlist answers `SpaceFrozen`, having passed admission and coverage, and
/// stages nothing. The freeze check follows admission, so only a granted caller ever reaches it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_frozen_mind_serves_reads_and_refuses_a_freeze_gated_write() {
    let fixture = Fixture::open().await;
    fixture.grant("grt_capture", PRINCIPAL, &["capture"]).await;
    let pool = fixture.store.pool();
    sqlx::query("UPDATE memory_spaces SET status = 'frozen' WHERE space_id = ?")
        .bind(SPACE)
        .execute(&pool)
        .await
        .expect("freeze the Mind");
    let (_dir, serve) = fixture.serve();
    let surface = surface(
        &serve,
        faces(hello("runtime", &[]), None, MindProviders::default()),
    );
    let session = connect(&surface, &[RECALL_CONTEXT, ABSORB, READ_COGNITION]).await;

    let mut recall = recall_arguments();
    recall["max_tokens"] = json!(4_000);
    let pack = expect_ok(
        session.adapter.invoke_tool(RECALL_CONTEXT, recall).await,
        "recall in a frozen Mind",
    );
    assert_eq!(
        packed_record_ids(&pack),
        ["rec_market", "rec_north_gate"],
        "a frozen Mind still serves its granted caller's recall: {pack}"
    );
    assert_eq!(
        packed_knowledge_ids(&pack),
        [CARRIER],
        "the frozen Mind's knowledge slot is served too: {pack}"
    );

    let read = expect_ok(
        session
            .adapter
            .invoke_tool(READ_COGNITION, cognition_arguments(SPACE))
            .await,
        "the cognition read in a frozen Mind",
    );
    assert_eq!(read["carrier_revision"], 1);
    assert_eq!(read["records"], json!([]));

    let rejection = expect_reject(
        session
            .adapter
            .invoke_tool(ABSORB, absorb_arguments_in(SPACE))
            .await,
        "a capture into a frozen Mind",
    );
    assert_eq!(
        rejection.code,
        SpokeRejectCode::SpaceFrozen,
        "a freeze-gated operation outside the allowlist is the frozen refusal: {rejection:?}"
    );
    let staged: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM memory_candidates")
        .fetch_one(&pool)
        .await
        .expect("count the candidates");
    assert_eq!(staged, 0, "a frozen Mind stages nothing");
}

/// Existence privacy: a peer admitted to nothing — no grant in either space — cannot tell an
/// existing Mind from an absent one. Both `space_id` variations are answered with the same
/// refusal, byte-equal, and neither call reaches the reasoner: the reasoner is absent entirely
/// here, so a call that escaped the admission gate would surface `ReasonerUnavailable` instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deliberate_cannot_distinguish_an_absent_mind_from_an_ungranted_one() {
    let fixture = Fixture::open().await;
    fixture.seed_second_mind().await;
    let (_dir, serve) = fixture.serve();
    // No reasoner: any path that reaches the engine before admission is a different answer.
    let mut faces = faces(hello("runtime", &[]), None, MindProviders::default());
    faces.principals = two_principal_admission();
    let surface = surface(&serve, faces);

    let session = connect_as(&surface, &PEER_TWO_SEED, &[DELIBERATE]).await;
    let existing = expect_reject(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments_in(SPACE_TWO))
            .await,
        "deliberate into an existing Mind the peer holds no grant in",
    );
    let absent = expect_reject(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments_in("spc_no_such_mind"))
            .await,
        "deliberate into a space that is no Mind at all",
    );
    assert_eq!(existing.code, SpokeRejectCode::NotAuthorized);
    assert_eq!(
        existing, absent,
        "an ungranted Mind and an absent Mind must be the byte-same refusal"
    );
    assert!(
        existing.details.is_none(),
        "the refusal carries no details either way: {existing:?}"
    );
}

/// Pre-authorization reads only the caller's own rows: another principal's malformed grant row
/// in an existing Mind must not leak the space's existence through a storage error. The peer
/// holds no grants anywhere; the malformed row belongs to a stranger in `SPACE_TWO`, and the
/// answer for that space and for a space that is no Mind at all are byte-equal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_strangers_malformed_grant_row_cannot_leak_the_minds_existence() {
    let fixture = Fixture::open().await;
    fixture.seed_second_mind().await;
    sqlx::query(
        "INSERT INTO memory_grants \
         (grant_id, space_id, subject_id, principal_id, capabilities_json, \
          context_ids_json, shared_access, expires_at, revoked_at, revision, created_at) \
         VALUES ('grt_malformed_stranger', ?, NULL, 'prn_stranger', 'not-json', '[]', 0, NULL, NULL, 1, ?)",
    )
    .bind(SPACE_TWO)
    .bind(NOW)
    .execute(&fixture.store.pool())
    .await
    .expect("seed the stranger's malformed grant");
    let (_dir, serve) = fixture.serve();
    // No reasoner: a path that reaches the engine would answer differently again.
    let mut faces = faces(hello("runtime", &[]), None, MindProviders::default());
    faces.principals = two_principal_admission();
    let surface = surface(&serve, faces);

    let session = connect_as(&surface, &PEER_TWO_SEED, &[DELIBERATE]).await;
    let poisoned = expect_reject(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments_in(SPACE_TWO))
            .await,
        "deliberate into a Mind carrying a stranger's malformed row",
    );
    let absent = expect_reject(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments_in("spc_no_such_mind"))
            .await,
        "deliberate into a space that is no Mind at all",
    );
    assert_eq!(
        poisoned, absent,
        "a stranger's malformed row and an absent Mind must be the byte-same refusal"
    );
    assert!(
        poisoned.details.is_none(),
        "the refusal carries no details either way: {poisoned:?}"
    );
}

/// Liveness is the adapter's one grant rule: an expiry that does not parse fails closed, so a
/// dead row covers nothing. With the caller's `recall` grant still live the Mind admits it and
/// the dead `knowledge_read` grant is the coverage gap that answers; once every row is dead the
/// answer is the one target-free admission refusal, and no carrier is read either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_grant_with_an_unparseable_expiry_is_dead_to_deliberate() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    // Corrupt the seeded knowledge grant's expiry in place: it can no longer be read as a time.
    sqlx::query(
        "UPDATE memory_grants SET expires_at = 'not-a-time' WHERE grant_id = 'grt_knowledge'",
    )
    .execute(&pool)
    .await
    .expect("corrupt the expiry");
    let (_dir, serve) = fixture.serve();
    // A reasoner is configured so the workflow reaches its snapshot: the refusal must come from
    // admission/coverage, never from the engine's missing capability.
    let mut faces = faces(
        hello("runtime", &[]),
        None,
        MindProviders {
            reasoner: Some(Arc::new(NoChangeReasoner)),
            ..MindProviders::default()
        },
    );
    faces.principals = principal_admission();
    let surface = surface(&serve, faces);

    let session = connect(&surface, &[DELIBERATE]).await;
    let uncovered = expect_reject(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "deliberate with a dead knowledge_read grant",
    );
    assert_eq!(uncovered.code, SpokeRejectCode::NotAuthorized);
    assert!(
        uncovered.message.contains("knowledge_read"),
        "the dead row covers nothing, so the coverage refusal answers: {uncovered:?}"
    );
    assert_eq!(
        uncovered
            .details
            .as_ref()
            .and_then(|details| details.get("retry_class"))
            .and_then(Value::as_str),
        Some("terminal"),
        "a dead grant cannot be retried away: {uncovered:?}"
    );

    // Every row dead now: no live grant admits the caller, so the answer is target-free.
    sqlx::query("UPDATE memory_grants SET expires_at = 'not-a-time' WHERE grant_id = 'grt_recall'")
        .execute(&pool)
        .await
        .expect("corrupt the recall expiry");
    let rejected = expect_reject(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "deliberate with every grant dead",
    );
    assert_eq!(rejected.code, SpokeRejectCode::NotAuthorized);
    assert!(
        rejected.details.is_none(),
        "an unparseable expiry fails closed as the plain admission refusal: {rejected:?}"
    );
}

/// Revocation is final at call time, and the refusal split is the plan's: the admission reads its
/// state once, on its own transaction, and never a cached yes. A revoked `knowledge_read` grant
/// on a caller the Mind still admits is the coverage gap; with every grant revoked the answer is
/// the one target-free admission refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_grant_revoked_before_the_call_refuses_deliberate() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    let (_dir, serve) = fixture.serve();
    // A reasoner is configured so the workflow reaches its snapshot: the refusal must come from
    // admission/coverage, never from the engine's missing capability.
    let mut faces = faces(
        hello("runtime", &[]),
        None,
        MindProviders {
            reasoner: Some(Arc::new(NoChangeReasoner)),
            ..MindProviders::default()
        },
    );
    faces.principals = principal_admission();
    let surface = surface(&serve, faces);

    let session = connect(&surface, &[DELIBERATE]).await;
    sqlx::query(
        "UPDATE memory_grants SET revoked_at = '2026-11-21T01:00:00Z' WHERE grant_id = 'grt_knowledge'",
    )
    .execute(&pool)
    .await
    .expect("revoke the grant");
    let uncovered = expect_reject(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "deliberate after the knowledge grant is revoked",
    );
    assert_eq!(
        uncovered.code,
        SpokeRejectCode::NotAuthorized,
        "the revoked knowledge grant is the coverage answer: {uncovered:?}"
    );

    sqlx::query(
        "UPDATE memory_grants SET revoked_at = '2026-11-21T01:00:00Z' WHERE grant_id = 'grt_recall'",
    )
    .execute(&pool)
    .await
    .expect("revoke the recall grant");
    let reject = expect_reject(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "deliberate after every grant is revoked",
    );
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
    assert!(
        reject.details.is_none(),
        "a revoked grant is the plain admission refusal: {reject:?}"
    );
}

/// The plan's refusal order on the served `deliberate`: admission, then the workflow's subject
/// page and its revalidation, and only then the `knowledge_read` coverage. A caller whose recall
/// grant does not cover the named subject answers that refusal even though it also lacks
/// `knowledge_read`; once the subject is covered, the capability gap answers. No reasoner is
/// configured, so any path that reached the engine would answer differently again, and nothing is
/// written either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deliberate_refuses_the_subject_before_the_knowledge_coverage() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    // The caller keeps one recall grant, scoped to a subject it is not asking about, and no
    // knowledge grant at all: both refusals apply, and the subject's must answer first.
    fixture.revoke("grt_recall").await;
    fixture.revoke("grt_knowledge").await;
    sqlx::query(
        "INSERT INTO memory_grants \
         (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
          shared_access, expires_at, revoked_at, revision, created_at) \
         VALUES ('grt_recall_elsewhere', ?, 'subj_elsewhere', ?, '[\"recall\"]', '[]', 0, NULL, NULL, 1, ?)",
    )
    .bind(SPACE)
    .bind(PRINCIPAL)
    .bind(NOW)
    .execute(&pool)
    .await
    .expect("seed the elsewhere recall grant");
    let (_dir, serve) = fixture.serve();
    // A reasoner is configured so the workflow reaches its snapshot: both refusals below are
    // decided there, before any provider work.
    let surface = surface(
        &serve,
        faces(
            hello("runtime", &[]),
            None,
            MindProviders {
                reasoner: Some(Arc::new(NoChangeReasoner)),
                ..MindProviders::default()
            },
        ),
    );
    let session = connect(&surface, &[DELIBERATE]).await;

    let subject_first = expect_reject(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "deliberate whose recall grant covers another subject",
    );
    assert_eq!(
        subject_first.code,
        SpokeRejectCode::NotAuthorized,
        "the subject refusal answers: {subject_first:?}"
    );
    assert!(
        subject_first.message.contains("recall"),
        "the subject page's own refusal answers first: {subject_first:?}"
    );
    assert!(
        !subject_first.message.contains("knowledge_read"),
        "the capability gap must not pre-empt the subject refusal: {subject_first:?}"
    );

    // Covering the subject leaves only the capability gap; it answers now, from the workflow.
    sqlx::query(
        "INSERT INTO memory_grants \
         (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
          shared_access, expires_at, revoked_at, revision, created_at) \
         VALUES ('grt_recall_subject', ?, ?, ?, '[\"recall\"]', '[]', 0, NULL, NULL, 1, ?)",
    )
    .bind(SPACE)
    .bind(SUBJECT)
    .bind(PRINCIPAL)
    .bind(NOW)
    .execute(&pool)
    .await
    .expect("seed the subject recall grant");
    let coverage = expect_reject(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "deliberate whose subject is covered but whose knowledge_read is missing",
    );
    assert_eq!(coverage.code, SpokeRejectCode::NotAuthorized);
    assert!(
        coverage.message.contains("knowledge_read"),
        "the coverage refusal answers once the subject is admitted: {coverage:?}"
    );
    assert_ne!(
        coverage, subject_first,
        "the two refusals are different answers in the plan's order"
    );
    assert_eq!(
        carrier_revision(&pool, COLLECTION, CARRIER).await,
        1,
        "neither refusal writes a judgment"
    );
}

/// One principal admitted to both Minds: each recall names its own `space_id` and answers that
/// Mind's content — entry id, canonical name and body summary — never the other's. Two different
/// principals would not pin this: a route keyed to the principal would pass either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_principal_recalls_each_mind_by_its_own_space_id() {
    let fixture = Fixture::open().await;
    fixture.seed_second_mind().await;
    fixture
        .grant_in_second("grt_shared_peer", PRINCIPAL, &["recall", "knowledge_read"])
        .await;
    let (_dir, serve) = fixture.serve();
    let mut faces = faces(hello("runtime", &[]), None, MindProviders::default());
    faces.principals = principal_admission();
    let surface = surface(&serve, faces);

    let session = connect(&surface, &[RECALL_CONTEXT]).await;
    for (space, carrier, name, summary) in [
        (
            SPACE,
            CARRIER,
            "ke_carrier's carrier",
            "what this holder currently holds",
        ),
        (
            SPACE_TWO,
            CARRIER_TWO,
            "the second mind's carrier",
            "the knowledge of the second mind, not the first",
        ),
    ] {
        let mut arguments = recall_arguments_in(space);
        arguments["max_tokens"] = json!(4_000);
        let pack = expect_ok(
            session.adapter.invoke_tool(RECALL_CONTEXT, arguments).await,
            &format!("recall {space} by the shared principal"),
        );
        assert_eq!(
            packed_knowledge_ids(&pack),
            [carrier],
            "{space}: the slot names this Mind's carrier"
        );
        assert_eq!(
            pack["knowledge"][0]["canonical_name"], name,
            "{space}: the slot is this Mind's canonical name"
        );
        assert_eq!(
            pack["knowledge"][0]["body"]["summary"], summary,
            "{space}: the slot is this Mind's content"
        );
    }
    let first_pack = {
        let mut arguments = recall_arguments_in(SPACE);
        arguments["max_tokens"] = json!(4_000);
        expect_ok(
            session.adapter.invoke_tool(RECALL_CONTEXT, arguments).await,
            "recall the first Mind",
        )
    };
    assert!(
        !first_pack["knowledge"]
            .as_array()
            .expect("first knowledge")
            .iter()
            .any(|entry| entry["entry_id"] == CARRIER_TWO),
        "the first Mind's recall never carries the second Mind's entry: {first_pack}"
    );
}

/// Advertisement honesty for the optional face: the dialer claims `spoke-memory` in both
/// sessions, yet the op is denied while the host attaches no memory service and served verbatim
/// once it does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_ops_are_served_only_when_the_host_attaches_the_face() {
    let fixture = Fixture::open().await;
    let (_dir, serve) = fixture.serve();
    let absent = surface(
        &serve,
        faces(
            hello("runtime", &["spoke-memory"]),
            None,
            MindProviders::default(),
        ),
    );
    assert!(
        !capabilities(&absent).contains(&"spoke-memory"),
        "an unattached memory face is never advertised: {:?}",
        capabilities(&absent)
    );

    let session = connect(&absent, &["spoke-memory"]).await;
    assert_wire_deny(
        MemoryPort::recall(&*session.adapter, &principal(), recall_request()).await,
        "memory.recall",
        "capability_missing",
    );
    drop(session);

    let attached = surface(
        &serve,
        faces(
            hello("runtime", &["spoke-memory"]),
            Some(Arc::new(MemoryFace)),
            MindProviders::default(),
        ),
    );
    assert!(
        capabilities(&attached).contains(&"spoke-memory"),
        "an attached memory face is advertised: {:?}",
        capabilities(&attached)
    );
    assert!(
        !capabilities(&attached).contains(&"spoke-memory-transfer"),
        "a bare service reports no operation and cannot claim the family: {:?}",
        capabilities(&attached)
    );
    let session = connect(&attached, &["spoke-memory"]).await;
    let body = expect_ok(
        MemoryPort::recall(&*session.adapter, &principal(), recall_request()).await,
        "the attached memory face",
    );
    let body = serde_json::to_value(body).expect("the memory response serializes");
    assert_eq!(
        body["engine_policy_version"], "loopback-memory-face",
        "the host's memory service answered the authenticated peer"
    );
    assert_eq!(body["total_authorized"], 7);
}

/// A host memory service that serves `memory.grants.list` through the adapter over one pool, so
/// the admin view of the listing can be driven on a signed session.
struct GrantsListFace {
    pool: sqlx::SqlitePool,
    admission: MemoryAdmission,
}

#[async_trait]
impl RemoteMemoryService for GrantsListFace {
    async fn invoke(&self, peer_id: &str, op: &str, payload: Value) -> SpokeResult<Value> {
        if op != "memory.grants.list" {
            return spoke_reject(
                SpokeRejectCode::CapabilityPortMissing,
                format!("the fixture grants face does not serve {op}"),
                None,
            );
        }
        let principal = match (self.admission)(peer_id) {
            SpokeResult::Ok(principal) => principal,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(error) => {
                return spoke_reject(
                    SpokeRejectCode::InternalError,
                    format!("the fixture grants face could not begin: {error}"),
                    None,
                )
            }
        };
        let stage = MemoryAdapter::new(true)
            .list_grants(&mut tx, &principal, payload)
            .await;
        let _ = tx.rollback().await;
        match stage {
            SpokeResult::Ok(MemoryStage::Ready(body)) => SpokeResult::Ok(body),
            SpokeResult::Ok(_) => spoke_reject(
                SpokeRejectCode::InternalError,
                "grants.list staged an outcome the fixture face cannot finish",
                None,
            ),
            SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
        }
    }
}

impl ServedMemory for GrantsListFace {}

/// The admin view of `memory.grants.list` over a signed session: an admin naming another principal
/// receives that principal's grant rows (not an empty page), the same admin naming none receives
/// the whole space's decodable set, a non-admin naming another principal still receives only its
/// own rows, and another principal's grant change moves the cursor marker an admin's page is
/// checked against.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_admin_grant_view_reads_the_space_and_moves_its_marker() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    fixture.grant("grt_admin_view", PRINCIPAL, &["admin"]).await;
    fixture
        .grant("grt_peer_two_recall", PEER_TWO_PRINCIPAL, &["recall"])
        .await;
    fixture
        .grant("grt_peer_two_capture", PEER_TWO_PRINCIPAL, &["capture"])
        .await;
    // A stranger's undecodable row is invisible to the view, never fatal.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO memory_grants \
         (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
          shared_access, expires_at, revoked_at, revision, created_at) \
         VALUES ('grant_a_bad', '{SPACE}', NULL, 'prn_stranger', CAST(X'80' AS TEXT), '[]', 0, \
          NULL, NULL, 1, '{NOW}')"
    )))
    .execute(&pool)
    .await
    .expect("seed the undecodable row");
    let (_dir, serve) = fixture.serve();
    let mut faces = faces(
        hello("runtime", &[]),
        Some(Arc::new(GrantsListFace {
            pool: pool.clone(),
            admission: two_principal_admission(),
        })),
        MindProviders::default(),
    );
    faces.principals = two_principal_admission();
    let surface = surface(&serve, faces);

    let request = |payload: Value| -> MemoryGrantsListRequest {
        serde_json::from_value(payload).expect("the grants list request JSON matches the schema")
    };
    let granted = |result: SpokeResult<MemoryGrantsListResponse>| -> (Vec<String>, Value) {
        let response = expect_ok(result, "grants.list");
        let value = serde_json::to_value(response).expect("the grants list response serializes");
        let ids = value["grants"]
            .as_array()
            .expect("a grants array")
            .iter()
            .map(|grant| {
                grant["grant_id"]
                    .as_str()
                    .expect("every served grant names its id")
                    .to_string()
            })
            .collect();
        (ids, value["next_cursor"].clone())
    };

    let admin_session = connect(&surface, &["spoke-memory"]).await;
    let (named, _) = granted(
        MemoryPort::list_grants(
            &*admin_session.adapter,
            &principal(),
            request(json!({ "space_id": SPACE, "principal_id": PEER_TWO_PRINCIPAL })),
        )
        .await,
    );
    assert_eq!(
        named,
        ["grt_peer_two_capture", "grt_peer_two_recall"],
        "an admin naming another principal receives that principal's rows, not an empty page"
    );

    let (all, _) = granted(
        MemoryPort::list_grants(
            &*admin_session.adapter,
            &principal(),
            request(json!({ "space_id": SPACE })),
        )
        .await,
    );
    assert_eq!(
        all,
        [
            "grt_admin_view",
            "grt_knowledge",
            "grt_peer_two_capture",
            "grt_peer_two_recall",
            "grt_recall",
        ],
        "an admin naming no principal receives the whole space's decodable set: {all:?}"
    );
    assert!(
        !all.contains(&"grant_a_bad".to_string()),
        "the undecodable row is invisible to the admin page: {all:?}"
    );

    let peer_session = connect_as(&surface, &PEER_TWO_SEED, &["spoke-memory"]).await;
    let (own, _) = granted(
        MemoryPort::list_grants(
            &*peer_session.adapter,
            &principal_for(PEER_TWO_PRINCIPAL),
            request(json!({ "space_id": SPACE, "principal_id": PRINCIPAL })),
        )
        .await,
    );
    assert_eq!(
        own,
        ["grt_peer_two_capture", "grt_peer_two_recall"],
        "a non-admin naming another principal still receives only its own rows"
    );

    // The cursor marker covers the set the admin page was built from, so another principal's
    // grant change invalidates a cursor the admin already holds.
    let (page, cursor) = granted(
        MemoryPort::list_grants(
            &*admin_session.adapter,
            &principal(),
            request(json!({ "space_id": SPACE, "limit": 1 })),
        )
        .await,
    );
    assert_eq!(page.len(), 1);
    let cursor = cursor
        .as_str()
        .unwrap_or_else(|| panic!("a bounded page advances: {cursor}"))
        .to_string();
    sqlx::query(
        "UPDATE memory_grants SET revision = 2, capabilities_json = '[\"recall\",\"capture\"]' \
         WHERE grant_id = 'grt_peer_two_recall'",
    )
    .execute(&pool)
    .await
    .expect("change another principal's grant");
    let stale = expect_reject(
        MemoryPort::list_grants(
            &*admin_session.adapter,
            &principal(),
            request(json!({ "space_id": SPACE, "cursor": cursor })),
        )
        .await,
        "an admin cursor issued before another principal's grant changed",
    );
    assert_eq!(
        stale.code,
        SpokeRejectCode::CursorInvalid,
        "another principal's grant change moves the admin's marker: {stale:?}"
    );
}

/// The five §7.29–§7.33 operations over the real store. A service that reports all five earns
/// `spoke-memory-transfer`, and every op reaches the adapter through the signed session. The
/// service owns each op's transaction, so the export one create commits is the artifact the
/// later page, stage and activation read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_is_advertised_and_served_when_the_service_reports_all_five() {
    let fixture = Fixture::open().await;
    fixture
        .grant("grt_transfer", PRINCIPAL, &["transfer", "admin"])
        .await;
    let service = Arc::new(TransferService::new(
        fixture.store.pool(),
        principal_admission(),
    ));
    let (_dir, serve) = fixture.serve();
    let surface = surface(
        &serve,
        faces(
            hello("runtime", &["spoke-memory", "spoke-memory-transfer"]),
            Some(service),
            MindProviders::default(),
        ),
    );
    assert!(
        capabilities(&surface).contains(&"spoke-memory-transfer"),
        "a service that reports the whole family earns it: {:?}",
        capabilities(&surface)
    );
    let session = connect(&surface, &["spoke-memory", "spoke-memory-transfer"]).await;

    let created = wire(expect_ok(
        MemoryPort::create_export(
            &*session.adapter,
            &principal(),
            typed(json!({ "space_id": SPACE, "idempotency_key": "t7-export" })),
        )
        .await,
        "memory.exports.create",
    ));
    assert_eq!(created["status"], "committed");
    let export_id = created["result"]["export_id"]
        .as_str()
        .expect("export id")
        .to_string();
    assert_eq!(
        created["result"]["chunk_count"],
        json!(1),
        "the loopback space exports as one chunk"
    );

    let page = wire(expect_ok(
        MemoryPort::get_export(
            &*session.adapter,
            &principal(),
            typed(json!({ "space_id": SPACE, "export_id": export_id, "chunk_ordinal": 0 })),
        )
        .await,
        "memory.exports.get",
    ));
    let chunk = page["chunk"].clone();
    assert_eq!(chunk["export_id"], json!(export_id));

    let staged = wire(expect_ok(
        MemoryPort::stage_import(
            &*session.adapter,
            &principal(),
            typed(json!({
                "space_id": SPACE,
                "chunk": chunk.clone(),
                "conflict_policy": "rename_subject",
                "idempotency_key": "t7-stage",
            })),
        )
        .await,
        "memory.imports.stage",
    ));
    assert_eq!(staged["result"]["chunks_received"], json!(1));
    let import_id = staged["result"]["import_id"]
        .as_str()
        .expect("import id")
        .to_string();

    let discarded = wire(expect_ok(
        MemoryPort::discard_import(
            &*session.adapter,
            &principal(),
            typed(json!({
                "space_id": SPACE,
                "import_id": &import_id,
                "idempotency_key": "t7-discard",
            })),
        )
        .await,
        "memory.imports.discard",
    ));
    assert_eq!(discarded["result"]["discarded"], json!(true));

    let restaged = wire(expect_ok(
        MemoryPort::stage_import(
            &*session.adapter,
            &principal(),
            typed(json!({
                "space_id": SPACE,
                "chunk": chunk,
                "conflict_policy": "rename_subject",
                "idempotency_key": "t7-stage-again",
            })),
        )
        .await,
        "memory.imports.stage",
    ));
    let activated = wire(expect_ok(
        MemoryPort::activate_import(
            &*session.adapter,
            &principal(),
            typed(json!({
                "space_id": SPACE,
                "import_id": restaged["result"]["import_id"].as_str().expect("import id"),
                "idempotency_key": "t7-activate",
            })),
        )
        .await,
        "memory.imports.activate",
    ));
    assert_eq!(activated["status"], "committed");
}

/// The receipt a transfer commit returns is pollable on the same signed session: an
/// `OUTCOME_UNCERTAIN` outcome resolves through `memory.receipts.get`, which the service
/// reports beside the five transfer operations and dispatches over the same store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transfer_receipt_is_polled_on_the_same_signed_session() {
    let fixture = Fixture::open().await;
    fixture
        .grant("grt_transfer", PRINCIPAL, &["transfer", "admin"])
        .await;
    let service = Arc::new(TransferService::new(
        fixture.store.pool(),
        principal_admission(),
    ));
    assert!(
        service.operations().contains(&"memory.receipts.get"),
        "the service reports the op an uncertain commit is resolved through: {:?}",
        service.operations()
    );
    let (_dir, serve) = fixture.serve();
    let surface = surface(
        &serve,
        faces(
            hello("runtime", &["spoke-memory", "spoke-memory-transfer"]),
            Some(service),
            MindProviders::default(),
        ),
    );
    let session = connect(&surface, &["spoke-memory", "spoke-memory-transfer"]).await;
    session.wire.drain();

    let created = wire(expect_ok(
        MemoryPort::create_export(
            &*session.adapter,
            &principal(),
            typed(json!({ "space_id": SPACE, "idempotency_key": "t7-poll-export" })),
        )
        .await,
        "memory.exports.create",
    ));
    let receipt_id = created["receipt_id"]
        .as_str()
        .expect("receipt id")
        .to_string();

    let polled = wire(expect_ok(
        MemoryPort::get_receipt(
            &*session.adapter,
            &principal(),
            typed(json!({ "space_id": SPACE, "receipt_id": receipt_id })),
        )
        .await,
        "memory.receipts.get",
    ));
    // The poll answers a receipt at `payload.receipt`: that is the one other place the caller may
    // appear, and only as the receipt's own `requester_principal_id` attribution.
    assert_caller_absent(&session, "the receipt poll answer", ExpectsReceipt::Yes);
    assert_eq!(polled["receipt"]["op"], json!("memory.exports.create"));
    assert_eq!(polled["receipt"]["status"], json!("committed"));
    assert_eq!(polled["receipt"]["receipt_id"], json!(receipt_id));
    assert_eq!(
        polled["receipt"]["requester_principal_id"],
        json!(PRINCIPAL),
        "the poll answers the caller's own attribution of the request it made"
    );
}

fn typed<T: serde::de::DeserializeOwned>(value: Value) -> T {
    serde_json::from_value(value).expect("the request JSON matches its schema")
}

fn wire(value: impl serde::Serialize) -> Value {
    serde_json::to_value(value).expect("the response serializes")
}

/// The baseline face is never composed: the dialer claims `spoke-baseline`, the responder still
/// advertises none, and a real baseline op answers `op_unsupported`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_baseline_face_is_never_composed_or_advertised() {
    let fixture = Fixture::open().await;
    let (_dir, serve) = fixture.serve();
    let surface = surface(
        &serve,
        faces(
            hello("runtime", &["spoke-baseline"]),
            None,
            MindProviders::default(),
        ),
    );
    assert!(
        !capabilities(&surface).contains(&"spoke-baseline"),
        "no baseline provider is composed: {:?}",
        capabilities(&surface)
    );

    let session = connect(&surface, &["spoke-baseline"]).await;
    assert_wire_deny(
        ScopeQueryPort::list_timeline_events(&*session.adapter, &scope_of(SCOPE_ID)).await,
        "port.scope.list_timeline_events",
        "op_unsupported",
    );
}

/// A tool the runtime does not serve: the dialer claims the id, the responder advertises neither
/// the capability nor a handler, and the invoke answers `op_unsupported`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unserved_tool_answers_op_unsupported() {
    let fixture = Fixture::open().await;
    let (_dir, serve) = fixture.serve();
    let surface = surface(
        &serve,
        faces(hello("runtime", &[]), None, MindProviders::default()),
    );
    assert!(!capabilities(&surface).contains(&UNSERVED_TOOL));

    let session = connect(&surface, &[UNSERVED_TOOL]).await;
    assert_wire_deny(
        session.adapter.invoke_tool(UNSERVED_TOOL, json!({})).await,
        UNSERVED_TOOL,
        "op_unsupported",
    );
}

/// The un-routed families leave composition and advertisement together: the manifest names none of
/// them, no face is composed, and a dialer that claims them is served a missing port.
///
/// The knowledge half of the Stage 1 assertion moved to the composed-face tests below: the face is
/// now served when `ServeFaces.knowledge` asks for it, and absent — unadvertised and unserved —
/// when it does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_un_routed_face_answers_a_missing_port() {
    let fixture = Fixture::open().await;
    let (_dir, serve) = fixture.serve();
    let claimed = ["spoke-history", "spoke-knowledge-adoption"];
    let surface = surface(
        &serve,
        faces(hello("runtime", &claimed), None, MindProviders::default()),
    );
    for capability in claimed {
        assert!(
            !capabilities(&surface).contains(&capability),
            "the un-routed face {capability} is not advertised: {:?}",
            capabilities(&surface)
        );
    }
    assert!(surface.ports.as_knowledge().is_none());
    assert!(surface.ports.as_scoped_knowledge().is_none());
    assert!(surface.ports.as_history().is_none());
    assert!(surface.ports.as_adoption().is_none());

    let session = connect(&surface, &claimed).await;
    let dialer = &*session.adapter;
    assert_wire_deny(
        HistoryPort::get_history_snapshot(
            dialer,
            &principal(),
            &scope_of(SCOPE_ID),
            "knowledge_entry",
            "ke_absent",
            1,
        )
        .await,
        "history.revisions.get",
        "capability_missing",
    );
    let adopt: KnowledgeAdoptRequest = serde_json::from_value(json!({
        "source_scope_id": SCOPE_ID,
        "source_entry_id": "ke_absent",
        "source_revision": 1,
        "destination_scope_id": SCOPE_ID,
        "idempotency_key": "un-routed",
    }))
    .expect("request JSON matches the schema");
    assert_wire_deny(
        AdoptionPort::adopt_knowledge_entry(dialer, &principal(), &scope_of(SCOPE_ID), &adopt)
            .await,
        "knowledge.adopt",
        "capability_missing",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_registration_reaches_its_handler_over_the_signed_session() {
    let fixture = Fixture::open().await;
    let (_dir, serve) = fixture.serve();
    let mut faces = faces(hello("runtime", &[]), None, MindProviders::default());
    faces.tools = vec![ToolRegistration::new(
        host_tool_descriptor(),
        |_responder| {
            let handler: ToolHandler = Arc::new(|arguments: Value| {
                Box::pin(async move {
                    spoke_ok(json!({ "answered_by": HOST_TOOL, "arguments": arguments }))
                })
            });
            handler
        },
    )];
    let surface = surface(&serve, faces);
    assert!(
        capabilities(&surface).contains(&HOST_TOOL),
        "the host registration is advertised: {:?}",
        capabilities(&surface)
    );

    let session = connect(&surface, &[HOST_TOOL]).await;
    let result = expect_ok(
        session
            .adapter
            .invoke_tool(HOST_TOOL, json!({ "probe": 1 }))
            .await,
        "the host's own tool",
    );
    assert_eq!(
        result["answered_by"], HOST_TOOL,
        "the installed host handler answered: {result}"
    );
    assert_eq!(result["arguments"], json!({ "probe": 1 }));
}

/// An embedder whose serving identity the test rotates: it records every text it embeds, so a
/// recall's provider cost is a count rather than an inference.
struct RotatingEmbedder {
    first: EmbeddingIdentity,
    second: EmbeddingIdentity,
    second_active: AtomicBool,
    embedded: Mutex<Vec<String>>,
}

impl RotatingEmbedder {
    fn new(first: EmbeddingIdentity, second: EmbeddingIdentity) -> Self {
        Self {
            first,
            second,
            second_active: AtomicBool::new(false),
            embedded: Mutex::new(Vec::new()),
        }
    }

    /// The host rotating its embedder to the other serving identity.
    fn rotate(&self) {
        let active = self.second_active.load(Ordering::SeqCst);
        self.second_active.store(!active, Ordering::SeqCst);
    }

    fn embedded(&self) -> Vec<String> {
        self.embedded.lock().expect("embedded texts").clone()
    }
}

#[async_trait]
impl Embedder for RotatingEmbedder {
    fn identity(&self) -> Option<&EmbeddingIdentity> {
        Some(match self.second_active.load(Ordering::SeqCst) {
            true => &self.second,
            false => &self.first,
        })
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, SpiError> {
        self.embedded
            .lock()
            .expect("embedded texts")
            .extend(texts.iter().cloned());
        Ok(texts
            .iter()
            .map(|text| match text.contains("market") {
                true => vec![0.0, 1.0],
                false => vec![1.0, 0.0],
            })
            .collect())
    }
}

/// A serving identity over the same model, distinguished only by its version pin.
fn axes_identity(model_version: &str) -> EmbeddingIdentity {
    EmbeddingIdentity {
        provider_id: "local".to_string(),
        model_id: "axes".to_string(),
        model_version: model_version.to_string(),
        dim: 2,
    }
}

/// How many stored vectors one identity's partition holds.
async fn vector_rows(pool: &sqlx::SqlitePool, model_version: &str) -> i64 {
    let hash = expect_ok(
        mindcore_sqlite::vectors::identity_hash("local", "axes", model_version, 2),
        "hash the serving identity",
    );
    sqlx::query_scalar("SELECT COUNT(*) FROM memory_embedding_vectors WHERE identity_hash = ?")
        .bind(hash)
        .fetch_one(pool)
        .await
        .expect("count one identity's vectors")
}

/// The fixture recall with a query: only the query path writes a query slot and record vectors.
fn query_recall_arguments() -> Value {
    let mut arguments = recall_arguments();
    arguments["request"]["query"] = json!("north gate");
    arguments
}

/// Identity rotation has one supported host entry: retiring through the surface clears that
/// identity's query slot and deletes its stored partition, leaving another identity's slot and
/// rows warm and making a later recall under the retired identity recompute.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retiring_an_identity_clears_its_slot_and_partition() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    let (_dir, serve) = fixture.serve();
    let embedder = Arc::new(RotatingEmbedder::new(
        axes_identity("1"),
        axes_identity("2"),
    ));
    let surface = surface(
        &serve,
        faces(
            hello("runtime", &[]),
            None,
            MindProviders {
                embedder: Some(embedder.clone()),
                ..MindProviders::default()
            },
        ),
    );
    let session = connect(&surface, &[RECALL_CONTEXT]).await;

    expect_ok(
        session
            .adapter
            .invoke_tool(RECALL_CONTEXT, query_recall_arguments())
            .await,
        "recall under identity 1",
    );
    assert_eq!(
        vector_rows(&pool, "1").await,
        2,
        "the serving identity stores one row per live record"
    );

    embedder.rotate();
    expect_ok(
        session
            .adapter
            .invoke_tool(RECALL_CONTEXT, query_recall_arguments())
            .await,
        "recall under identity 2",
    );
    assert_eq!(vector_rows(&pool, "2").await, 2);

    let deleted = expect_ok(
        surface.retire_embedding_identity(&axes_identity("1")).await,
        "retire identity 1 through the surface",
    );
    assert_eq!(deleted, 2, "retirement reports the rows it deleted");
    assert_eq!(
        vector_rows(&pool, "1").await,
        0,
        "the retired partition is empty"
    );
    assert_eq!(
        vector_rows(&pool, "2").await,
        2,
        "another identity's rows survive"
    );

    let before = embedder.embedded().len();
    expect_ok(
        session
            .adapter
            .invoke_tool(RECALL_CONTEXT, query_recall_arguments())
            .await,
        "recall under identity 2 after the retirement",
    );
    assert_eq!(
        embedder.embedded().len(),
        before,
        "another identity's query slot survives the retirement"
    );

    embedder.rotate();
    let before = embedder.embedded().len();
    expect_ok(
        session
            .adapter
            .invoke_tool(RECALL_CONTEXT, query_recall_arguments())
            .await,
        "recall under the retired identity",
    );
    let mut reembedded = embedder.embedded()[before..].to_vec();
    reembedded.sort();
    assert_eq!(
        reembedded,
        [
            "north gate",
            "the market opens at dawn",
            "the north gate is closed",
        ],
        "the retired identity re-embeds its query and its rows"
    );
    assert_eq!(
        vector_rows(&pool, "1").await,
        2,
        "the retired identity fills its partition again"
    );
}

/// Retiring the identity whose query slot is warm clears that slot: the next recall under it
/// re-embeds its query instead of reusing a vector its deleted rows no longer back. A different
/// identity's warm slot outlives a retirement that names another identity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retiring_a_warm_identity_forces_its_next_query_embed() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    let (_dir, serve) = fixture.serve();
    let embedder = Arc::new(RotatingEmbedder::new(
        axes_identity("1"),
        axes_identity("2"),
    ));
    let surface = surface(
        &serve,
        faces(
            hello("runtime", &[]),
            None,
            MindProviders {
                embedder: Some(embedder.clone()),
                ..MindProviders::default()
            },
        ),
    );
    let session = connect(&surface, &[RECALL_CONTEXT]).await;

    expect_ok(
        session
            .adapter
            .invoke_tool(RECALL_CONTEXT, query_recall_arguments())
            .await,
        "recall under identity 1",
    );
    expect_ok(
        session
            .adapter
            .invoke_tool(RECALL_CONTEXT, query_recall_arguments())
            .await,
        "second recall under identity 1",
    );
    let warm = embedder.embedded().len();
    assert_eq!(
        vector_rows(&pool, "1").await,
        2,
        "identity 1 stores one row per live record"
    );

    let deleted = expect_ok(
        surface.retire_embedding_identity(&axes_identity("1")).await,
        "retire the warm identity 1",
    );
    assert_eq!(deleted, 2, "retirement reports the rows it deleted");
    expect_ok(
        session
            .adapter
            .invoke_tool(RECALL_CONTEXT, query_recall_arguments())
            .await,
        "recall under the retired identity 1",
    );
    let mut reembedded = embedder.embedded()[warm..].to_vec();
    reembedded.sort();
    assert_eq!(
        reembedded,
        [
            "north gate",
            "the market opens at dawn",
            "the north gate is closed",
        ],
        "the retired identity re-embeds its query instead of reusing the cleared slot"
    );

    embedder.rotate();
    expect_ok(
        session
            .adapter
            .invoke_tool(RECALL_CONTEXT, query_recall_arguments())
            .await,
        "recall under identity 2",
    );
    expect_ok(
        session
            .adapter
            .invoke_tool(RECALL_CONTEXT, query_recall_arguments())
            .await,
        "second recall under identity 2",
    );
    let warm = embedder.embedded().len();
    let deleted = expect_ok(
        surface.retire_embedding_identity(&axes_identity("1")).await,
        "retire identity 1 while identity 2's slot is warm",
    );
    assert_eq!(
        deleted, 2,
        "the refilled identity 1 partition is retired again"
    );
    expect_ok(
        session
            .adapter
            .invoke_tool(RECALL_CONTEXT, query_recall_arguments())
            .await,
        "recall under identity 2 after retiring identity 1",
    );
    assert_eq!(
        embedder.embedded().len(),
        warm,
        "another identity's query slot survives the retirement"
    );
    assert_eq!(
        vector_rows(&pool, "2").await,
        2,
        "another identity's rows survive the retirement"
    );
}

/// A host memory service that routes `memory.spaces.create` to the adapter over one pool, so a
/// peer creates a Mind on a signed session. The service is the caller of the transaction, as
/// `TransferService` is: `BEGIN IMMEDIATE`, the adapter, then commit only after `Ready`.
struct CreateSpaceFace {
    pool: sqlx::SqlitePool,
    admission: MemoryAdmission,
}

/// The create face's lost-acknowledgement answer: the commit was not confirmed, so the caller
/// cannot know whether the Mind was written — only the receipt names which outcome to resolve.
///
/// The receipt id comes from the `Ready` body the adapter staged, which is the committed receipt
/// the transaction wrote; the answer claims no commit outcome either way.
fn create_commit_uncertain(body: &Value, error: impl std::fmt::Display) -> SpokeReject {
    let mut details = Map::new();
    details.insert("retry_class".to_string(), json!("uncertain"));
    if let Some(receipt_id) = body.get("receipt_id").cloned() {
        details.insert("receipt_id".to_string(), receipt_id);
    }
    SpokeReject {
        code: SpokeRejectCode::OutcomeUncertain,
        message: format!("memory.spaces.create commit was not confirmed: {error}"),
        details: Some(details),
    }
}

/// A lost commit acknowledgement names the receipt and claims no outcome: the caller resolves it
/// through `memory.receipts.get` rather than reading the answer as a create or a refusal.
#[test]
fn a_lost_create_commit_names_the_receipt_and_claims_no_outcome() {
    let body = json!({
        "schema_version": 1,
        "receipt_id": "rct_create",
        "status": "committed",
        "result": { "space_id": "spc_wire", "created": true, "status": "active", "revision": 1 },
    });
    let reject = create_commit_uncertain(&body, "the connection went away");
    assert_eq!(reject.code, SpokeRejectCode::OutcomeUncertain);
    let details = reject
        .details
        .expect("the uncertain answer carries details");
    assert_eq!(details["receipt_id"], json!("rct_create"));
    assert_eq!(details["retry_class"], json!("uncertain"));
    assert!(
        !reject.message.contains("committed") && !reject.message.contains("not committed"),
        "the answer claims no commit outcome: {}",
        reject.message
    );
}

#[async_trait]
impl RemoteMemoryService for CreateSpaceFace {
    async fn invoke(&self, peer_id: &str, op: &str, payload: Value) -> SpokeResult<Value> {
        if op != "memory.spaces.create" {
            return spoke_reject(
                SpokeRejectCode::CapabilityPortMissing,
                format!("the fixture create face does not serve {op}"),
                None,
            );
        }
        let principal = match (self.admission)(peer_id) {
            SpokeResult::Ok(principal) => principal,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        let mut tx = match self.pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(tx) => tx,
            Err(error) => {
                return spoke_reject(
                    SpokeRejectCode::InternalError,
                    format!("the fixture create face could not begin: {error}"),
                    None,
                )
            }
        };
        let stage = MemoryAdapter::new(false)
            .create_space(&mut tx, &principal, payload)
            .await;
        match stage {
            SpokeResult::Ok(MemoryStage::Ready(body)) => match tx.commit().await {
                Ok(()) => SpokeResult::Ok(body),
                Err(error) => SpokeResult::Reject(create_commit_uncertain(&body, error)),
            },
            SpokeResult::Ok(_) => spoke_reject(
                SpokeRejectCode::InternalError,
                "create staged an outcome the fixture face cannot finish",
                None,
            ),
            SpokeResult::Reject(reject) => {
                let _ = tx.rollback().await;
                SpokeResult::Reject(reject)
            }
        }
    }
}

impl ServedMemory for CreateSpaceFace {}

/// The signed create path: a peer creates a Mind over the wire, replays under the same key, and
/// repeats under a fresh key. The create writes one default grant row carrying the seven
/// capabilities; the replay writes nothing; the repeat changes no space, grant or revision. No
/// answer carries the caller binding the served knowledge port reads from a scope.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_creates_a_mind_through_the_signed_loopback() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    let (_dir, serve) = fixture.serve();
    let surface = surface(
        &serve,
        faces(
            hello("runtime", &["spoke-memory"]),
            Some(Arc::new(CreateSpaceFace {
                pool: pool.clone(),
                admission: principal_admission(),
            })),
            MindProviders::default(),
        ),
    );
    assert!(
        capabilities(&surface).contains(&"spoke-memory"),
        "the attached create face advertises the memory family: {:?}",
        capabilities(&surface)
    );
    let session = connect(&surface, &["spoke-memory"]).await;
    // The handshake's own envelopes are not this test's subject; every drain from here covers
    // exactly one create call.
    session.wire.drain();

    let grants = |pool: sqlx::SqlitePool, space: &'static str| async move {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory_grants WHERE space_id = ?")
            .bind(space)
            .fetch_one(&pool)
            .await
            .expect("count the grants")
    };
    let receipts = |pool: sqlx::SqlitePool, space: &'static str| async move {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory_receipts WHERE space_id = ?")
            .bind(space)
            .fetch_one(&pool)
            .await
            .expect("count the receipts")
    };
    // The persisted Mind row by row — space, registry, carrier, grants — projected whole, so a
    // write to any column of any of them shows up as a difference; an in-place grant edit too.
    let mind_rows = |pool: sqlx::SqlitePool, space: &'static str| async move {
        let space_row = sqlx::query_scalar::<_, String>(
            "SELECT json_object('space_id', space_id, 'display_name', display_name, \
             'authority_host_id', authority_host_id, 'authority_generation', \
             authority_generation, 'status', status, 'revision', revision, \
             'created_at', created_at) FROM memory_spaces WHERE space_id = ?",
        )
        .bind(space)
        .fetch_one(&pool)
        .await
        .expect("the space row");
        let registry = sqlx::query_scalar::<_, String>(
            "SELECT json_object('space_id', space_id, 'knowledge_collection', \
             knowledge_collection, 'carrier_entry_id', carrier_entry_id) \
             FROM minds WHERE space_id = ?",
        )
        .bind(space)
        .fetch_one(&pool)
        .await
        .expect("the registry row");
        let carrier = sqlx::query_scalar::<_, String>(
            "SELECT json_object('entry_id', entry_id, 'revision', revision, 'payload', payload, \
             'history_floor_revision', history_floor_revision, \
             'cognition_head_seq', cognition_head_seq, 'superseded_by', superseded_by, \
             'created_at', created_at, 'updated_at', updated_at) \
             FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
        )
        .bind(format!("mind:{space}"))
        .bind(CARRIER)
        .fetch_one(&pool)
        .await
        .expect("the carrier entry");
        let grants: Vec<String> = sqlx::query_scalar(
            "SELECT json_object('grant_id', grant_id, 'principal_id', principal_id, \
             'subject_id', subject_id, 'capabilities_json', capabilities_json, \
             'context_ids_json', context_ids_json, 'shared_access', shared_access, \
             'expires_at', expires_at, 'revoked_at', revoked_at, 'revision', revision, \
             'created_at', created_at) \
             FROM memory_grants WHERE space_id = ? ORDER BY grant_id",
        )
        .bind(space)
        .fetch_all(&pool)
        .await
        .expect("the grant rows");
        let row = |json: String| serde_json::from_str::<Value>(&json).expect("the row's json");
        (
            row(space_row),
            row(registry),
            row(carrier),
            grants
                .iter()
                .map(|grant| row(grant.clone()))
                .collect::<Vec<Value>>(),
        )
    };

    let created = wire(expect_ok(
        MemoryPort::create_space(
            &*session.adapter,
            &principal(),
            typed(json!({ "space_id": "spc_wire", "idempotency_key": "create-wire-1" })),
        )
        .await,
        "memory.spaces.create",
    ));
    assert_caller_absent(&session, "the create answer", ExpectsReceipt::Yes);
    assert_eq!(created["result"]["space_id"], json!("spc_wire"));
    assert_eq!(created["result"]["created"], json!(true));
    assert_eq!(created["result"]["status"], json!("active"));
    assert_eq!(created["result"]["revision"], json!(1));
    let first_receipt = created["receipt_id"].clone();
    let grants_after_create = grants(pool.clone(), "spc_wire").await;
    assert_eq!(
        grants_after_create, 1,
        "a create writes one default grant row"
    );
    assert_eq!(
        receipts(pool.clone(), "spc_wire").await,
        1,
        "a create records one committed receipt"
    );

    // The creator's own grant row: the seven capabilities, and the ownership that makes them
    // reachable — space-wide, subject-free, context-free, never expiring.
    let (granted, grantee, subject, contexts, expires): (
        String,
        String,
        Option<String>,
        String,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT capabilities_json, principal_id, subject_id, context_ids_json, expires_at \
         FROM memory_grants WHERE space_id = 'spc_wire'",
    )
    .fetch_one(&pool)
    .await
    .expect("the creator's grant row");
    assert_eq!(
        serde_json::from_str::<Value>(&granted).expect("capabilities json"),
        json!([
            "recall",
            "capture",
            "review",
            "records_write",
            "knowledge_read",
            "knowledge_write",
            "admin"
        ]),
        "the served create writes the same default grant set as the library"
    );
    assert_eq!(
        grantee, PRINCIPAL,
        "the default grant is issued to the creating principal"
    );
    assert_eq!(subject, None, "the default grant is space-wide");
    assert_eq!(contexts, "[]", "the default grant restricts no context");
    assert_eq!(expires, None, "the default grant never expires");

    // The Mind as the create left it: the space row, the registry row, the carrier entry and the
    // creator's one grant row.
    let mind_before = mind_rows(pool.clone(), "spc_wire").await;
    assert_eq!(
        mind_before.1,
        json!({
            "space_id": "spc_wire",
            "knowledge_collection": "mind:spc_wire",
            "carrier_entry_id": CARRIER,
        }),
        "the create's registry row names the Mind's own collection and carrier"
    );
    assert_eq!(
        mind_before.3.len(),
        1,
        "the served create wrote exactly one grant: {:?}",
        mind_before.3
    );
    assert_eq!(mind_before.3[0]["principal_id"], json!(PRINCIPAL));
    assert_eq!(mind_before.3[0]["subject_id"], Value::Null);
    assert_eq!(mind_before.3[0]["context_ids_json"], json!("[]"));
    assert_eq!(mind_before.3[0]["expires_at"], Value::Null);
    assert_eq!(
        mind_before.3[0]["capabilities_json"],
        json!(granted),
        "the snapshot holds the same default capability set the query above pins"
    );

    let replay = wire(expect_ok(
        MemoryPort::create_space(
            &*session.adapter,
            &principal(),
            typed(json!({ "space_id": "spc_wire", "idempotency_key": "create-wire-1" })),
        )
        .await,
        "the replay of a create",
    ));
    assert_caller_absent(&session, "the replay answer", ExpectsReceipt::Yes);
    assert_eq!(replay["receipt_id"], first_receipt);
    assert_eq!(replay["result"], created["result"]);
    assert_eq!(
        grants(pool.clone(), "spc_wire").await,
        1,
        "a replay writes nothing"
    );
    assert_eq!(
        receipts(pool.clone(), "spc_wire").await,
        1,
        "a replay records no second receipt"
    );

    let repeat = wire(expect_ok(
        MemoryPort::create_space(
            &*session.adapter,
            &principal(),
            typed(json!({ "space_id": "spc_wire", "idempotency_key": "create-wire-2" })),
        )
        .await,
        "a repeat create under a fresh key",
    ));
    assert_caller_absent(&session, "the fresh-key repeat answer", ExpectsReceipt::Yes);
    assert_ne!(repeat["receipt_id"], first_receipt);
    assert_eq!(repeat["result"]["created"], json!(false));
    assert_eq!(
        repeat["result"]["revision"],
        json!(1),
        "the revision did not move"
    );
    // The repeat is a second recorded outcome, not a replay: one more receipt row, nothing else.
    assert_eq!(
        receipts(pool.clone(), "spc_wire").await,
        2,
        "a fresh key records its own receipt"
    );
    assert_eq!(
        grants(pool.clone(), "spc_wire").await,
        1,
        "a repeat writes no second grant row"
    );
    // The repeat's only difference: its receipt. The space row, the registry row, the carrier
    // entry and the creator's grant row are exactly what the create wrote — a widened capability
    // set or a moved expiry on that grant would differ here.
    assert_eq!(
        mind_rows(pool.clone(), "spc_wire").await,
        mind_before,
        "a repeat writes nothing but its receipt"
    );
    let (status, revision): (String, i64) =
        sqlx::query_as("SELECT status, revision FROM memory_spaces WHERE space_id = ?")
            .bind("spc_wire")
            .fetch_one(&pool)
            .await
            .expect("the space row");
    assert_eq!(status, "active");
    assert_eq!(revision, 1, "a repeat changes no space");
    assert_eq!(
        session.adapter.state(),
        RemoteAdapterState::Established,
        "every call stayed on the one signed session"
    );
}

/// A peer without space-wide `admin` on an existing Mind cannot create over it, and the answer is
/// the fixed admission refusal — never the library's `MIND_ALREADY_EXISTS`, which names the id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_create_of_an_existing_mind_without_admin_learns_nothing() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    let (_dir, serve) = fixture.serve();
    let surface = surface(
        &serve,
        faces(
            hello("runtime", &["spoke-memory"]),
            Some(Arc::new(CreateSpaceFace {
                pool: pool.clone(),
                admission: principal_admission(),
            })),
            MindProviders::default(),
        ),
    );
    let session = connect(&surface, &["spoke-memory"]).await;
    session.wire.drain();

    let refusal = expect_reject(
        MemoryPort::create_space(
            &*session.adapter,
            &principal(),
            typed(json!({ "space_id": SPACE, "idempotency_key": "create-wire-no-admin" })),
        )
        .await,
        "an existing-Mind create without admin",
    );
    assert_caller_absent(&session, "the no-admin refusal", ExpectsReceipt::No);
    assert_eq!(refusal.code, SpokeRejectCode::NotAuthorized);
    assert!(
        refusal.details.is_none(),
        "the create refusal carries no details: {refusal:?}"
    );
    for needle in [PRINCIPAL, SPACE] {
        assert!(
            !refusal.message.contains(needle),
            "the create refusal names neither the caller nor the space ({needle}): {refusal:?}"
        );
    }
    assert!(
        !refusal.message.contains("already exists"),
        "the answer is never the taken-id refusal: {refusal:?}"
    );
    let receipts: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM memory_receipts WHERE space_id = ?")
            .bind(SPACE)
            .fetch_one(&pool)
            .await
            .expect("count the receipts");
    assert_eq!(receipts, 0, "a refused create stages no receipt");
}

/// A frozen Mind under its admin answers `SPACE_FROZEN` on the wire, and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_create_of_a_frozen_mind_answers_space_frozen() {
    let fixture = Fixture::open().await;
    fixture.grant("grt_admin", PRINCIPAL, &["admin"]).await;
    let pool = fixture.store.pool();
    sqlx::query("UPDATE memory_spaces SET status = 'frozen' WHERE space_id = ?")
        .bind(SPACE)
        .execute(&pool)
        .await
        .expect("freeze the Mind");
    let (_dir, serve) = fixture.serve();
    let surface = surface(
        &serve,
        faces(
            hello("runtime", &["spoke-memory"]),
            Some(Arc::new(CreateSpaceFace {
                pool: pool.clone(),
                admission: principal_admission(),
            })),
            MindProviders::default(),
        ),
    );
    let session = connect(&surface, &["spoke-memory"]).await;
    session.wire.drain();

    let refusal = expect_reject(
        MemoryPort::create_space(
            &*session.adapter,
            &principal(),
            typed(json!({ "space_id": SPACE, "idempotency_key": "create-wire-frozen" })),
        )
        .await,
        "a create into a frozen Mind",
    );
    assert_caller_absent(&session, "the frozen refusal", ExpectsReceipt::No);
    assert_eq!(
        refusal.code,
        SpokeRejectCode::SpaceFrozen,
        "a frozen Mind answers the frozen refusal: {refusal:?}"
    );
    let receipts: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM memory_receipts WHERE space_id = ?")
            .bind(SPACE)
            .fetch_one(&pool)
            .await
            .expect("count the receipts");
    assert_eq!(receipts, 0, "a frozen Mind stages no receipt");
}

/// The absence walk is not vacuous, and its one permitted occurrence is narrow: a receipt's own
/// `requester_principal_id`, at a receipt's own path, holding the expected scalar, on an answer
/// that expects a receipt. Everything else — another key, another path, a nested value, a
/// different principal, a principal used as an object key, a key that merely spells a receipt
/// path, or any receipt on a refusal — is a leak.
#[test]
fn the_caller_absence_walk_catches_a_leak_under_any_key() {
    let frame =
        |body: &str| -> Value { serde_json::from_str(body).expect("the fixture frame is JSON") };
    let leaks = |body: &str, expects| -> Vec<String> {
        caller_leaks(&frame(body), &[PRINCIPAL], expects, &mut Vec::new())
    };
    // The two receipt paths, and only those, carry the attribution.
    for clean in [
        r#"{"payload":{"receipt_id":"rct_1","requester_principal_id":"prn_loopback"}}"#,
        r#"{"payload":{"receipt":{"receipt_id":"rct_1","requester_principal_id":"prn_loopback"}}}"#,
    ] {
        assert_eq!(
            leaks(clean, ExpectsReceipt::Yes),
            Vec::<String>::new(),
            "the walk accepts the receipt's own attribution: {clean}"
        );
    }

    for leak in [
        r#"{"payload":{"result":{"space":"prn_loopback"}}}"#,
        r#"{"extensions":{"mindcore_caller":{"principal":"p"}}}"#,
        r#"{"payload":{"items":["ok","mindcore_caller"]}}"#,
        r#"{"payload":{"result":{"requester_principal_id":"prn_loopback"}}}"#,
        // The reviewer's two cases: a receipt-shaped object that is not at a receipt path, with a
        // binding nested inside the member, and the principal used as a key.
        r#"{"payload":{"receipt_id":"rct_1","requester_principal_id":{"mindcore_caller":{"principal":"prn_loopback"}}}}"#,
        r#"{"payload":{"prn_loopback":"value"}}"#,
        // The same member at a receipt path but holding something other than the expected scalar.
        r#"{"payload":{"receipt_id":"rct_1","requester_principal_id":"prn_other"}}"#,
        r#"{"payload":{"receipt":{"receipt_id":"rct_1","requester_principal_id":["prn_loopback"]}}}"#,
        // A single member whose literal key spells a receipt path is one key at one level, not a
        // receipt nesting: the wrong value inside it must not be read as the permitted one.
        r#"{"payload.receipt":{"receipt_id":"rct_1","requester_principal_id":"prn_other"}}"#,
        r#"{"payload.receipt":{"receipt_id":"rct_1","requester_principal_id":"prn_loopback"}}"#,
    ] {
        assert_eq!(
            leaks(leak, ExpectsReceipt::Yes).len(),
            1,
            "the walk names the leak in: {leak}"
        );
    }

    // A refusal stages no receipt, so the two bodies a successful create may answer — a committed
    // receipt and a receipt under `payload.receipt` — are both leaks here, including the exact
    // scalar the exception would otherwise permit at that path.
    for refusal in [
        r#"{"error":{"details":{"receipt_id":"rct_1","requester_principal_id":"prn_loopback"}}}"#,
        r#"{"payload":{"receipt_id":"rct_1","requester_principal_id":"prn_loopback"}}"#,
        r#"{"payload":{"receipt":{"receipt_id":"rct_1","requester_principal_id":"prn_loopback"}}}"#,
    ] {
        assert_eq!(
            leaks(refusal, ExpectsReceipt::No).len(),
            2,
            "a refusal carrying receipt attribution is two leaks — the receipt and the caller: \
             {refusal}"
        );
    }
}

// ---------- The composed knowledge face ----------

/// One plain entry a served put creates: no cognition module, so the store validates only its
/// own write bookkeeping.
fn served_entry(entry_id: &str, summary: &str) -> KnowledgeEntry {
    KnowledgeEntry {
        adoption_anchor: None,
        body: KnowledgeEntryBody {
            summary: Some(summary.to_string()),
            ..KnowledgeEntryBody::default()
        },
        canonical_name: KnowledgeEntryCanonicalName::try_from(format!("{entry_id} name"))
            .expect("non-empty canonical name"),
        created_at: None,
        disclosure: None,
        entry_id: entry_id.to_string(),
        entry_type: "note".to_string(),
        extensions: HashMap::new(),
        modules: HashMap::new(),
        owner: None,
        revision: None,
        schema_version: std::num::NonZeroU64::new(1).expect("non-zero wire baseline"),
        source_anchor: None,
        status: "confirmed".to_string(),
        updated_at: None,
    }
}

/// One entry carrying a current-view cognition record whose holder is the entry itself: the
/// shape a served put appends through, so the cognition append is a served write and not removed.
fn served_entry_with_cognition(entry_id: &str, record_id: &str) -> KnowledgeEntry {
    let mut entry = served_entry(entry_id, "a carrier with one belief");
    let mut modules = HashMap::new();
    modules.insert(
        KnowledgeEntryModulesKey::try_from("cognition").expect("valid module key"),
        KnowledgeEntryModulesValue::from(
            json!({
                "records": [{
                    "kind": "belief",
                    "order": 1,
                    "record_id": record_id,
                    "holder": {
                        "kind": "knowledge_entry",
                        "id": entry_id,
                        "namespace": "mindcore",
                    },
                    "target": {
                        "carrier": {
                            "kind": "knowledge_entry",
                            "id": CARRIER,
                            "namespace": "mindcore",
                        },
                        "target_id": "prop_north_gate_closed",
                        "kind": "proposition",
                    },
                    "assessor": {
                        "kind": "knowledge_entry",
                        "id": entry_id,
                        "namespace": "mindcore",
                    },
                    "assessor_role": "holder_self_report",
                    "basis": {
                        "kind": "evidence",
                        "evidence": [{
                            "kind": "memory_record",
                            "id": "rec_north_gate",
                            "namespace": "mindcore",
                        }],
                    },
                    "assessment_context": {"context_id": "ctx_dusk_at_the_gate"},
                    "effective_at": "2026-11-19T18:00:00Z",
                    "assessed_at": "2026-11-19T18:05:00Z",
                    "view": "current",
                    "q": {
                        "value": 0.4,
                        "assertion": "belief_attribution",
                        "method": "single_self_report",
                        "calibration": "uncalibrated",
                    },
                    "extensions": {},
                }],
                "world_facts": [],
                "extensions": {},
            })
            .as_object()
            .expect("cognition module is an object")
            .clone(),
        ),
    );
    entry.modules = modules;
    entry
}

/// The scope a request declares: an admitted Mind, and any extension namespace the caller sends.
fn request_scope(scope_id: &str, extensions: Option<Value>) -> Scope {
    let mut scope = json!({ "scope_id": scope_id });
    if let Some(extensions) = extensions {
        scope["extensions"] = extensions;
    }
    serde_json::from_value(scope).expect("scope JSON matches the schema")
}

/// Every served operation answers the one knowledge-scope refusal: no existence detail, no
/// admission detail, and nothing about the caller.
fn assert_scope_refusal<T: std::fmt::Debug>(result: SpokeResult<T>, context: &str) -> SpokeReject {
    let reject = expect_reject(result, context);
    assert_eq!(
        reject.code,
        SpokeRejectCode::NotAuthorized,
        "{context}: the fixed knowledge-scope refusal: {reject:?}"
    );
    assert_eq!(
        reject.message, "knowledge scope not authorized",
        "{context}: Connect's own admission-deny words"
    );
    assert!(
        reject.details.is_none(),
        "{context}: the refusal carries no details: {reject:?}"
    );
    reject
}

/// A surface whose knowledge face is composed, over the fixture's Mind.
fn knowledge_surface(serve: &MindcoreServe, principals: MemoryAdmission) -> MindcoreSurface {
    let mut faces = faces(
        hello("runtime", &KNOWLEDGE_CAPABILITIES),
        None,
        MindProviders::default(),
    );
    faces.knowledge = true;
    faces.principals = principals;
    surface(serve, faces)
}

/// The composed knowledge face serves the admitted Mind's own collection: a put creates an entry
/// with its cognition record, a get and a list read it back, and no other Mind's entry appears.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_composed_knowledge_face_serves_the_admitted_minds_own_entries() {
    let fixture = Fixture::open().await;
    fixture.seed_second_mind().await;
    let pool = fixture.store.pool();
    let (_dir, serve) = fixture.serve();
    let surface = knowledge_surface(&serve, principal_admission());
    assert!(
        capabilities(&surface).contains(&"spoke-knowledge"),
        "the composed face is advertised: {:?}",
        capabilities(&surface)
    );

    let session = connect(&surface, &KNOWLEDGE_CAPABILITIES).await;
    let dialer = &*session.adapter;

    let written = expect_ok(
        dialer
            .put_knowledge_entry_in_scope(
                served_entry_with_cognition("ke_gate_note", "cog_gate_note"),
                None,
                Some(&request_scope(SPACE, None)),
            )
            .await,
        "a put by the Mind's creator",
    );
    assert_eq!(written.entry_id, "ke_gate_note");
    assert_eq!(written.revision, Some(1), "the store assigned the revision");
    let record_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM cognition_records WHERE collection = ? AND entry_id = ?",
    )
    .bind(COLLECTION)
    .bind("ke_gate_note")
    .fetch_one(&pool)
    .await
    .expect("count the appended records");
    assert_eq!(
        record_rows, 1,
        "the covering grant's put appends its cognition record"
    );

    let read = expect_ok(
        dialer
            .get_knowledge_entry_in_scope("ke_gate_note", Some(&request_scope(SPACE, None)))
            .await,
        "a get in the admitted Mind",
    );
    assert_eq!(
        read.body.summary.as_deref(),
        Some("a carrier with one belief")
    );

    let listed = expect_ok(
        KnowledgeQueryPort::list_knowledge_entries(dialer, &request_scope(SPACE, None)).await,
        "a list in the admitted Mind",
    );
    let ids: Vec<&str> = listed.iter().map(|entry| entry.entry_id.as_str()).collect();
    assert!(
        ids.contains(&"ke_gate_note") && ids.contains(&CARRIER),
        "the admitted Mind's own entries are listed: {ids:?}"
    );
    assert!(
        !ids.contains(&CARRIER_TWO),
        "the second Mind's carrier never reaches this list: {ids:?}"
    );

    // The second Mind's entry is not reachable by name either: the admitted Mind's own collection
    // holds no such row, so the answer is the absence of a live entry, with no details.
    let absent = expect_reject(
        dialer
            .get_knowledge_entry_in_scope(CARRIER_TWO, Some(&request_scope(SPACE, None)))
            .await,
        "a get of the other Mind's entry",
    );
    assert_eq!(
        absent.code,
        SpokeRejectCode::KnowledgeEntryNotFound,
        "another Mind's entry is an absent entry, not a refused one: {absent:?}"
    );
    assert!(
        absent.details.is_none(),
        "the absence names no collection and no caller: {absent:?}"
    );
}

/// Admission precedes existence, and the refusal is one answer: a Mind the caller is not admitted
/// to, a Mind that is no Mind at all, a frozen Mind without a grant, a request that names no Mind
/// at all, and a request whose scope is not the admitted one answer byte for byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_knowledge_face_refuses_every_mind_it_does_not_admit_alike() {
    let fixture = Fixture::open().await;
    fixture.seed_second_mind().await;
    let pool = fixture.store.pool();
    sqlx::query("UPDATE memory_spaces SET status = 'frozen' WHERE space_id = ?")
        .bind(SPACE_TWO)
        .execute(&pool)
        .await
        .expect("freeze the second Mind");
    let (_dir, serve) = fixture.serve();
    let surface = knowledge_surface(&serve, two_principal_admission());

    // The peer that holds no grant in the second Mind, and none in the first either.
    let session = connect_as(&surface, &PEER_TWO_SEED, &KNOWLEDGE_CAPABILITIES).await;
    let dialer = &*session.adapter;

    let unadmitted = assert_scope_refusal(
        KnowledgeQueryPort::list_knowledge_entries(dialer, &request_scope(SPACE, None)).await,
        "a list in a Mind the caller holds no grant in",
    );
    let unknown = assert_scope_refusal(
        KnowledgeQueryPort::list_knowledge_entries(
            dialer,
            &request_scope("spc_no_such_mind", None),
        )
        .await,
        "a list in a Mind that is no Mind",
    );
    let frozen = assert_scope_refusal(
        KnowledgeQueryPort::list_knowledge_entries(dialer, &request_scope(SPACE_TWO, None)).await,
        "a list in a frozen Mind the caller holds no grant in",
    );
    let unscoped_get = assert_scope_refusal(
        KnowledgeEntryPort::get_knowledge_entry(dialer, "ke_absent").await,
        "a get that names no scope at all",
    );
    let unscoped_put = assert_scope_refusal(
        KnowledgeEntryPort::put_knowledge_entry(dialer, served_entry("ke_absent", "x"), None).await,
        "a put that names no scope at all",
    );

    for (label, reject) in [
        ("unknown", unknown),
        ("frozen", frozen),
        ("no scope, get", unscoped_get),
        ("no scope, put", unscoped_put),
    ] {
        assert_eq!(
            reject, unadmitted,
            "{label}: every refusal for want of admission is the same bytes"
        );
    }

    // The same peer, admitted to its own Mind, cannot name the other Mind's scope either: the
    // declared scope must equal the admitted one before any store call.
    sqlx::query(
        "INSERT INTO memory_grants \
         (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
          shared_access, expires_at, revoked_at, revision, created_at) \
         VALUES (?, ?, NULL, ?, ?, '[]', 0, NULL, NULL, 1, ?)",
    )
    .bind("grt_reader_peer_two")
    .bind(SPACE_TWO)
    .bind(PEER_TWO_PRINCIPAL)
    .bind(json!(["knowledge_read"]).to_string())
    .bind(NOW)
    .execute(&pool)
    .await
    .expect("grant the peer its own Mind");
    sqlx::query("UPDATE memory_spaces SET status = 'active' WHERE space_id = ?")
        .bind(SPACE_TWO)
        .execute(&pool)
        .await
        .expect("unfreeze the second Mind");
    let widened = assert_scope_refusal(
        KnowledgeQueryPort::list_knowledge_entries(dialer, &request_scope(SPACE, None)).await,
        "a list naming a Mind the peer is not admitted to",
    );
    assert_eq!(
        widened, unadmitted,
        "a scope that is not the admitted one is the same refusal"
    );

    let served = expect_ok(
        KnowledgeQueryPort::list_knowledge_entries(dialer, &request_scope(SPACE_TWO, None)).await,
        "a list in the peer's own Mind",
    );
    let ids: Vec<&str> = served.iter().map(|entry| entry.entry_id.as_str()).collect();
    assert!(
        ids.contains(&CARRIER_TWO) && !ids.contains(&CARRIER),
        "the admitted Mind's own entries, and only those: {ids:?}"
    );
}

/// A request that declares a viewpoint is refused: the admitted scope carries none, so the
/// viewpoint cannot equal it, and the refusal lands before any store call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_requested_viewpoint_is_not_the_admitted_scope() {
    let fixture = Fixture::open().await;
    let (_dir, serve) = fixture.serve();
    let surface = knowledge_surface(&serve, principal_admission());
    let session = connect(&surface, &KNOWLEDGE_CAPABILITIES).await;
    let dialer = &*session.adapter;

    let viewpoint: Scope = serde_json::from_value(json!({
        "scope_id": SPACE,
        "viewpoint": "kb_someone_else",
    }))
    .expect("scope JSON matches the schema");
    assert_scope_refusal(
        KnowledgeQueryPort::list_knowledge_entries(dialer, &viewpoint).await,
        "a list naming a viewpoint",
    );
    assert_scope_refusal(
        dialer
            .get_knowledge_entry_in_scope("ke_absent", Some(&viewpoint))
            .await,
        "a get naming a viewpoint",
    );
}

/// The gate order on the wire: a scoped `get` or `put` on a session that did not negotiate
/// `spoke-knowledge-scoped` is the dispatch deny — never a scope the peer would silently drop —
/// and a declared viewpoint without `ke-ownership` is the ownership conjunct, on the list op
/// that carries a required scope.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scoped_op_without_its_capability_is_the_dispatch_deny() {
    let fixture = Fixture::open().await;
    let (_dir, serve) = fixture.serve();
    let surface = knowledge_surface(&serve, principal_admission());
    // A dialer that claims `spoke-knowledge` alone: the responder advertises all three, so the
    // negotiated set is exactly this hello's claim.
    let session = connect(&surface, &["spoke-knowledge"]).await;
    let dialer = &*session.adapter;

    // `get`/`put` with a declared scope need the marker. The dialer refuses before sending, with
    // the class the responder would answer, so no scope is ever dropped on the way out.
    session.wire.drain_sent();
    let scoped = expect_reject(
        dialer
            .get_knowledge_entry_in_scope("ke_absent", Some(&request_scope(SPACE, None)))
            .await,
        "a scoped get on a session without the marker",
    );
    assert_eq!(
        scoped.code,
        SpokeRejectCode::CapabilityPortMissing,
        "an unnegotiated marker is the dispatch deny: {scoped:?}"
    );
    assert_eq!(
        scoped
            .details
            .as_ref()
            .and_then(|details| details.get("wire_code"))
            .and_then(Value::as_str),
        Some("op_unsupported"),
        "the pinned raw reject code: {scoped:?}"
    );
    assert!(
        scoped.message.contains("spoke-knowledge-scoped"),
        "the refusal names the marker: {}",
        scoped.message
    );
    assert!(
        session.wire.drain_sent().is_empty(),
        "an unnegotiated marker sends no scope"
    );

    // The list op carries a required scope, so the marker is not its conjunct; a declared
    // viewpoint is, and without `ke-ownership` it is the wire `op_unsupported` deny.
    let viewpoint: Scope = serde_json::from_value(json!({
        "scope_id": SPACE,
        "viewpoint": "kb_someone_else",
    }))
    .expect("scope JSON matches the schema");
    let denied = expect_reject(
        KnowledgeQueryPort::list_knowledge_entries(dialer, &viewpoint).await,
        "a viewpoint list without the ownership flag",
    );
    assert_eq!(
        denied.code,
        SpokeRejectCode::CapabilityPortMissing,
        "the ownership conjunct answers the wire deny: {denied:?}"
    );
    assert!(
        denied.message.contains("ke-ownership"),
        "the refusal names the missing capability: {}",
        denied.message
    );
}

/// Per-operation authorization: `knowledge_read` serves list and get, a put needs one grant
/// covering both capabilities, and a caller holding the read grant only is refused on put without
/// writing anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_knowledge_face_authorizes_each_operation_on_the_callers_own_grant() {
    let fixture = Fixture::open().await;
    fixture
        .grant(
            "grt_reader_peer_two",
            PEER_TWO_PRINCIPAL,
            &["knowledge_read"],
        )
        .await;
    fixture
        .grant(
            "grt_writer_peer_three",
            PEER_THREE_PRINCIPAL,
            &["knowledge_write"],
        )
        .await;
    let pool = fixture.store.pool();
    let (_dir, serve) = fixture.serve();
    let surface = knowledge_surface(&serve, three_principal_admission());

    // The creator's default grant covers both: list, get and put all serve.
    let creator = connect(&surface, &KNOWLEDGE_CAPABILITIES).await;
    expect_ok(
        creator
            .adapter
            .put_knowledge_entry_in_scope(
                served_entry("ke_creator", "the creator's entry"),
                None,
                Some(&request_scope(SPACE, None)),
            )
            .await,
        "the creator's put",
    );
    expect_ok(
        KnowledgeQueryPort::list_knowledge_entries(&*creator.adapter, &request_scope(SPACE, None))
            .await,
        "the creator's list",
    );
    assert_caller_absent_for(
        &creator,
        &[PRINCIPAL, PEER_TWO_PRINCIPAL, PEER_THREE_PRINCIPAL],
        "the creator's served answers",
        ExpectsReceipt::No,
    );

    // A read-only grant lists and gets, and its put is the fixed refusal with no write.
    let reader = connect_as(&surface, &PEER_TWO_SEED, &KNOWLEDGE_CAPABILITIES).await;
    let listed = expect_ok(
        KnowledgeQueryPort::list_knowledge_entries(&*reader.adapter, &request_scope(SPACE, None))
            .await,
        "a read-only grant's list",
    );
    assert!(
        listed.iter().any(|entry| entry.entry_id == "ke_creator"),
        "the read grant sees the Mind's entries"
    );
    expect_ok(
        reader
            .adapter
            .get_knowledge_entry_in_scope("ke_creator", Some(&request_scope(SPACE, None)))
            .await,
        "a read-only grant's get",
    );
    let refused = assert_scope_refusal(
        reader
            .adapter
            .put_knowledge_entry_in_scope(
                served_entry("ke_reader_write", "must not be written"),
                None,
                Some(&request_scope(SPACE, None)),
            )
            .await,
        "a put by a caller without knowledge_write",
    );
    assert_eq!(refused.code, SpokeRejectCode::NotAuthorized);
    assert!(refused.details.is_none());
    assert_caller_absent_for(
        &reader,
        &[PRINCIPAL, PEER_TWO_PRINCIPAL, PEER_THREE_PRINCIPAL],
        "the refused put",
        ExpectsReceipt::No,
    );
    let written: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
    )
    .bind(COLLECTION)
    .bind("ke_reader_write")
    .fetch_one(&pool)
    .await
    .expect("count the refused entry");
    assert_eq!(written, 0, "a refused put writes nothing");

    // `knowledge_write` alone is refused on all three: Connect reads the entry through `get`
    // before it writes, so a put needs one grant covering both.
    let writer = connect_as(&surface, &PEER_THREE_SEED, &KNOWLEDGE_CAPABILITIES).await;
    assert_scope_refusal(
        KnowledgeQueryPort::list_knowledge_entries(&*writer.adapter, &request_scope(SPACE, None))
            .await,
        "a write-only grant's list",
    );
    assert_scope_refusal(
        writer
            .adapter
            .get_knowledge_entry_in_scope("ke_creator", Some(&request_scope(SPACE, None)))
            .await,
        "a write-only grant's get",
    );
    assert_scope_refusal(
        writer
            .adapter
            .put_knowledge_entry_in_scope(
                served_entry("ke_writer_write", "must not be written"),
                None,
                Some(&request_scope(SPACE, None)),
            )
            .await,
        "a write-only grant's put",
    );
    let written: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
    )
    .bind(COLLECTION)
    .bind("ke_writer_write")
    .fetch_one(&pool)
    .await
    .expect("count the refused entry");
    assert_eq!(written, 0, "a write-only grant's put writes nothing");
}

/// A caller whose grant covers neither knowledge capability is refused on all three operations,
/// exactly as one admitted to no Mind is: the same bytes, no details.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_grant_covering_neither_knowledge_capability_serves_nothing() {
    let fixture = Fixture::open().await;
    fixture
        .grant("grt_recall_only", PEER_TWO_PRINCIPAL, &["recall"])
        .await;
    let (_dir, serve) = fixture.serve();
    let surface = knowledge_surface(&serve, two_principal_admission());
    let session = connect_as(&surface, &PEER_TWO_SEED, &KNOWLEDGE_CAPABILITIES).await;
    let dialer = &*session.adapter;

    let listed = assert_scope_refusal(
        KnowledgeQueryPort::list_knowledge_entries(dialer, &request_scope(SPACE, None)).await,
        "a list by a caller with no knowledge capability",
    );
    let read = assert_scope_refusal(
        dialer
            .get_knowledge_entry_in_scope(CARRIER, Some(&request_scope(SPACE, None)))
            .await,
        "a get by a caller with no knowledge capability",
    );
    let written = assert_scope_refusal(
        dialer
            .put_knowledge_entry_in_scope(
                served_entry("ke_ungranted", "must not be written"),
                None,
                Some(&request_scope(SPACE, None)),
            )
            .await,
        "a put by a caller with no knowledge capability",
    );
    for (label, reject) in [("list", listed), ("get", read), ("put", written)] {
        assert_eq!(
            reject.code,
            SpokeRejectCode::NotAuthorized,
            "{label}: one fixed refusal"
        );
    }
}

/// The binding obligation's adversarial half: a peer that supplies the `mindcore_caller`
/// namespace in its own request scope does not become the caller it names.
///
/// The admitted scope is bound on every successful return, so Connect's `effective_scope` merges
/// the request's extensions with the admitted ones and the admitted namespace wins.
///
/// The test walks W's grant through three states so every operation discriminates on its own
/// authorization decision:
///
/// 1. **W write-only, R read-only.** The forgery names W. List and get are served on R's
///    `knowledge_read` and would be refused under W's write-only grant; put is refused for R and
///    would also be refused for W, so put does not discriminate yet.
/// 2. **W covering (both capabilities), R read-only.** The same forged put is refused while W's
///    own covering grant would write it — the side-effect assertions below prove nothing was
///    written, which is the put contrast.
/// 3. **W covering, W's own positive write.** The same entry is then created by W, showing the
///    refusals were R's grant and not the entry.
///
/// The no-scope put is the same test without the forgery. It is not decided by the binding:
/// Connect passes no selector, and admission refuses a request that names no Mind before any
/// scope is built or bound, so nothing unbound can reach the port.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_supplied_caller_namespace_does_not_override_the_admitted_caller() {
    let fixture = Fixture::open().await;
    // The forgery names W (the dialer's principal), who holds the covering default grant. R reads
    // through its own grant and never through W's.
    //
    // The contrast is the forged principal himself: W's grant covers `knowledge_read`, so as long
    // as W holds it neither read discriminates — honouring the forgery and reading on R's grant
    // both succeed. So the test first drops W's read coverage with a write-only grant. Then each
    // operation's answer depends on WHICH principal authorized it: under R's grant (which covers
    // read and not write) list and get are served and put is refused; under W's grant (write and
    // not read) all three are refused. A regression that authorized any one operation with the
    // request's caller flips that operation's answer, not only put's.
    fixture.revoke("grt_knowledge").await;
    fixture
        .grant("grt_writer_forged", PRINCIPAL, &["knowledge_write"])
        .await;
    fixture
        .grant(
            "grt_reader_peer_two",
            PEER_TWO_PRINCIPAL,
            &["knowledge_read"],
        )
        .await;
    let pool = fixture.store.pool();
    let (_dir, serve) = fixture.serve();
    let surface = knowledge_surface(&serve, two_principal_admission());
    let forgery = Some(json!({ "mindcore_caller": { "principal": PRINCIPAL } }));

    let reader = connect_as(&surface, &PEER_TWO_SEED, &KNOWLEDGE_CAPABILITIES).await;
    let forger = &*reader.adapter;

    // Each operation is proven on four sides: the request bytes carry the forgery at the scope
    // path, the answer is decided by R's own grant (and would differ under W's write-only one),
    // and a dialer that stripped `scope.extensions` on any one of the three paths would fail its
    // own assertion here. The wire read is per operation, not once for the suite.
    reader.wire.drain_sent();
    let listed = expect_ok(
        KnowledgeQueryPort::list_knowledge_entries(forger, &request_scope(SPACE, forgery.clone()))
            .await,
        "a list carrying a forged caller namespace",
    );
    assert_sent_forgery(
        &reader.wire.drain_sent(),
        "port.scope.list_knowledge_entries",
        "",
        PRINCIPAL,
    );
    assert!(
        listed.iter().any(|entry| entry.entry_id == CARRIER),
        "the list is served on R's own knowledge_read: the forged write-only grant would refuse it"
    );

    reader.wire.drain_sent();
    expect_ok(
        forger
            .get_knowledge_entry_in_scope(CARRIER, Some(&request_scope(SPACE, forgery.clone())))
            .await,
        "a get carrying a forged caller namespace",
    );
    assert_sent_forgery(
        &reader.wire.drain_sent(),
        "port.knowledge.get",
        CARRIER,
        PRINCIPAL,
    );

    // The put is refused by R's own grant: the forged namespace bought no capability. Under the
    // forged principal's own write-only grant it would be refused as well, for the opposite
    // reason; R's refusal is the one the answer must be, and the row proves nothing was written.
    reader.wire.drain_sent();
    assert_scope_refusal(
        forger
            .put_knowledge_entry_in_scope(
                served_entry("ke_forged", "written as somebody else"),
                None,
                Some(&request_scope(SPACE, forgery.clone())),
            )
            .await,
        "a put carrying a forged caller namespace",
    );
    assert_sent_forgery(
        &reader.wire.drain_sent(),
        "port.knowledge.put",
        "ke_forged",
        PRINCIPAL,
    );
    let forged_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
    )
    .bind(COLLECTION)
    .bind("ke_forged")
    .fetch_one(&pool)
    .await
    .expect("count the forged entry");
    assert_eq!(forged_rows, 0, "the forged put wrote nothing");

    // The same put carrying no scope at all: the admission refuses a request that names no Mind,
    // so the no-scope path is never answered with an unbound scope either.
    assert_scope_refusal(
        forger
            .put_knowledge_entry_in_scope(
                served_entry("ke_forged", "written as somebody else"),
                None,
                None,
            )
            .await,
        "a put carrying no request scope",
    );
    let unscoped_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
    )
    .bind(COLLECTION)
    .bind("ke_forged")
    .fetch_one(&pool)
    .await
    .expect("count the unscoped entry");
    assert_eq!(unscoped_rows, 0, "the unscoped put wrote nothing");

    // The contrast made concrete on W's own session: with the write-only grant the same list and
    // get the forging peer just had served are refused for W. That is the answer a regression
    // honouring the forgery would produce for R, so the three reads above discriminate.
    let write_only = connect(&surface, &KNOWLEDGE_CAPABILITIES).await;
    let w_list = expect_reject(
        KnowledgeQueryPort::list_knowledge_entries(
            &*write_only.adapter,
            &request_scope(SPACE, None),
        )
        .await,
        "the forged principal's own write-only list",
    );
    assert_eq!(w_list.code, SpokeRejectCode::NotAuthorized);
    assert!(w_list.details.is_none());

    // The entry is R's grant, not the entry: W's own peer creates it once a grant covering both
    // capabilities is restored, which also shows the read contrast above is the grant W held.
    sqlx::query(
        "INSERT INTO memory_grants \
         (grant_id, space_id, subject_id, principal_id, capabilities_json, context_ids_json, \
          shared_access, expires_at, revoked_at, revision, created_at) \
         VALUES (?, ?, NULL, ?, ?, '[]', 0, NULL, NULL, 1, ?)",
    )
    .bind("grt_covering_forged")
    .bind(SPACE)
    .bind(PRINCIPAL)
    .bind(json!(["knowledge_read", "knowledge_write"]).to_string())
    .bind(NOW)
    .execute(&pool)
    .await
    .expect("restore the covering grant");

    // The put contrast, now that the forged principal covers both capabilities again: the same
    // request that is refused on R's `knowledge_read` would be WRITTEN if authorization took the
    // caller from the request. The forgery reaches the wire, the answer is the fixed refusal, and
    // neither the entry nor its cognition record exists — the two side effects W's own covering
    // grant produces immediately below.
    reader.wire.drain_sent();
    assert_scope_refusal(
        forger
            .put_knowledge_entry_in_scope(
                served_entry_with_cognition("ke_forged_contrast", "cog_forged_contrast"),
                None,
                Some(&request_scope(SPACE, forgery.clone())),
            )
            .await,
        "a forged put while the forged principal covers both capabilities",
    );
    assert_sent_forgery(
        &reader.wire.drain_sent(),
        "port.knowledge.put",
        "ke_forged_contrast",
        PRINCIPAL,
    );
    let contrast_entries: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
    )
    .bind(COLLECTION)
    .bind("ke_forged_contrast")
    .fetch_one(&pool)
    .await
    .expect("count the contrast entry");
    assert_eq!(
        contrast_entries, 0,
        "the forged put wrote no entry: honouring the forgery would have written one"
    );
    let contrast_records: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM cognition_records WHERE collection = ? AND entry_id = ?",
    )
    .bind(COLLECTION)
    .bind("ke_forged_contrast")
    .fetch_one(&pool)
    .await
    .expect("count the contrast cognition record");
    assert_eq!(
        contrast_records, 0,
        "the forged put wrote no cognition record either"
    );

    let owner = connect(&surface, &KNOWLEDGE_CAPABILITIES).await;
    let written = expect_ok(
        owner
            .adapter
            .put_knowledge_entry_in_scope(
                served_entry("ke_forged", "the owner's own entry"),
                None,
                Some(&request_scope(SPACE, None)),
            )
            .await,
        "the same entry created by the covering grant",
    );
    assert_eq!(written.entry_id, "ke_forged");
    assert_eq!(written.revision, Some(1));

    assert_caller_absent_for(
        &reader,
        &[PRINCIPAL, PEER_TWO_PRINCIPAL],
        "every answer to the forging peer",
        ExpectsReceipt::No,
    );
    assert_caller_absent_for(
        &owner,
        &[PRINCIPAL, PEER_TWO_PRINCIPAL],
        "every answer to the owner",
        ExpectsReceipt::No,
    );
}

/// No wire response carries the caller: every answer to both peers — list, get, put, the
/// refusals, an absent entry, a widening scope and an unadmitted or unknown Mind — is serialized
/// whole and asserted to name neither principal id nor the `mindcore_caller` namespace.
///
/// The walk is layer 1's raw-envelope one, including its response-context rule: a refusal stages
/// no receipt, so neither a receipt nor a receipt's own attribution may appear on one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_wire_response_carries_the_caller() {
    let fixture = Fixture::open().await;
    fixture.seed_second_mind().await;
    fixture
        .grant(
            "grt_reader_peer_two",
            PEER_TWO_PRINCIPAL,
            &["knowledge_read"],
        )
        .await;
    let (_dir, serve) = fixture.serve();
    let surface = knowledge_surface(&serve, two_principal_admission());
    let principals = [PRINCIPAL, PEER_TWO_PRINCIPAL];

    let owner = connect(&surface, &KNOWLEDGE_CAPABILITIES).await;
    let dialer = &*owner.adapter;
    expect_ok(
        dialer
            .put_knowledge_entry_in_scope(
                served_entry("ke_disclosed", "an entry the owner wrote"),
                None,
                Some(&request_scope(SPACE, None)),
            )
            .await,
        "the owner's put",
    );
    assert_caller_absent_for(
        &owner,
        &principals,
        "the owner's put answer",
        ExpectsReceipt::No,
    );

    expect_ok(
        KnowledgeQueryPort::list_knowledge_entries(dialer, &request_scope(SPACE, None)).await,
        "the owner's list",
    );
    assert_caller_absent_for(
        &owner,
        &principals,
        "the owner's list answer",
        ExpectsReceipt::No,
    );

    expect_ok(
        dialer
            .get_knowledge_entry_in_scope("ke_disclosed", Some(&request_scope(SPACE, None)))
            .await,
        "the owner's get",
    );
    assert_caller_absent_for(
        &owner,
        &principals,
        "the owner's get answer",
        ExpectsReceipt::No,
    );

    // An absent entry, an unadmitted Mind and a Mind that is no Mind: all refusals, none naming
    // the caller.
    expect_reject(
        dialer
            .get_knowledge_entry_in_scope("ke_absent", Some(&request_scope(SPACE, None)))
            .await,
        "the owner's get of an absent entry",
    );
    assert_caller_absent_for(
        &owner,
        &principals,
        "the absent-entry answer",
        ExpectsReceipt::No,
    );

    expect_reject(
        KnowledgeQueryPort::list_knowledge_entries(dialer, &request_scope(SPACE_TWO, None)).await,
        "the owner's list in another Mind",
    );
    assert_caller_absent_for(
        &owner,
        &principals,
        "the unadmitted-Mind answer",
        ExpectsReceipt::No,
    );

    expect_reject(
        KnowledgeQueryPort::list_knowledge_entries(
            dialer,
            &request_scope("spc_no_such_mind", None),
        )
        .await,
        "the owner's list in a Mind that is no Mind",
    );
    assert_caller_absent_for(
        &owner,
        &principals,
        "the unknown-Mind answer",
        ExpectsReceipt::No,
    );

    // The reader's answers, including a refused put and a request that widens the admitted scope.
    let reader = connect_as(&surface, &PEER_TWO_SEED, &KNOWLEDGE_CAPABILITIES).await;
    expect_ok(
        KnowledgeQueryPort::list_knowledge_entries(&*reader.adapter, &request_scope(SPACE, None))
            .await,
        "the reader's list",
    );
    assert_caller_absent_for(
        &reader,
        &principals,
        "the reader's list answer",
        ExpectsReceipt::No,
    );

    expect_reject(
        reader
            .adapter
            .put_knowledge_entry_in_scope(
                served_entry("ke_reader_refused", "must not be written"),
                None,
                Some(&request_scope(SPACE, None)),
            )
            .await,
        "the reader's refused put",
    );
    assert_caller_absent_for(
        &reader,
        &principals,
        "the reader's refused-put answer",
        ExpectsReceipt::No,
    );

    expect_reject(
        KnowledgeQueryPort::list_knowledge_entries(
            &*reader.adapter,
            &request_scope(SPACE_TWO, None),
        )
        .await,
        "the reader's list in another Mind",
    );
    assert_caller_absent_for(
        &reader,
        &principals,
        "the reader's unadmitted-Mind answer",
        ExpectsReceipt::No,
    );
}

// ---------- The pinned gate order, on the wire ----------

/// The gate order lives in the responder, not in the typed dialer: `RemoteAdapter` refuses a
/// scoped `get`/`put` locally when the session did not negotiate the marker, so it can never
/// reach the responder's `op_unsupported` branch or its shape-first `INVALID_INPUT`. These helpers
/// drive the wire directly — a signed hello, the session snapshot, then hand-built invoke
/// envelopes — so the ordering is proven where it actually runs.
mod raw_wire {
    use std::collections::HashMap;
    use std::sync::Arc;

    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    use ed25519_dalek::{Signer, SigningKey};
    use serde_json::{json, Value};

    use mindcore_runtime::MindcoreSurface;
    use spoke_connect::core::{derive_peer_id_from_ed25519_pubkey, sign_hello_ed25519};
    use spoke_connect::remote::{
        connect_responder, loopback_transport_pair, ConnectResponderOptions, LoopbackTransport,
        RemoteIdentity, Transport,
    };
    use spoke_schemas::connect_hello::HostCapabilityManifest as HelloManifest;
    use spoke_schemas::{ConnectHello, ConnectSession, HostCapabilityManifest};

    /// The dialer's key: this module's own, so the session it opens is one the fixture's host
    /// admission is told to admit.
    pub(super) const DIALER_SEED: [u8; 32] = [0x11u8; 32];
    const RESPONDER_SEED: [u8; 32] = [0x77u8; 32];

    pub(super) fn dialer_peer_id() -> String {
        derive_peer_id_from_ed25519_pubkey(
            &SigningKey::from_bytes(&DIALER_SEED)
                .verifying_key()
                .to_bytes(),
        )
    }

    /// Start a responder over `surface` WITHOUT dialing, and open a raw session against it: a
    /// signed initiator hello, the responder hello, the signed session snapshot. Returns the
    /// client end and the session id every invoke envelope must name.
    pub(super) async fn open_raw_session(
        surface: &MindcoreSurface,
        dialer_capabilities: &[&str],
    ) -> (Arc<LoopbackTransport>, String) {
        let pair = loopback_transport_pair();
        let dialer = dialer_peer_id();
        let responder_pubkey = SigningKey::from_bytes(&RESPONDER_SEED)
            .verifying_key()
            .to_bytes();
        let responder = connect_responder(ConnectResponderOptions {
            transport: Arc::new(pair.server),
            identity: RemoteIdentity {
                seed: RESPONDER_SEED,
            },
            manifest: surface.manifest.clone(),
            allowlist: vec![dialer.clone()],
            peer_keys: HashMap::from([(
                dialer,
                SigningKey::from_bytes(&DIALER_SEED)
                    .verifying_key()
                    .to_bytes(),
            )]),
            ports: Some(Arc::clone(&surface.ports)),
            invoke_timeout_ms: Some(10_000),
        })
        .await;
        assert!(
            matches!(
                surface.register(&responder),
                spoke_operations::SpokeResult::Ok(())
            ),
            "install the served handlers"
        );

        let manifest: HostCapabilityManifest = serde_json::from_value(json!({
            "schema_version": 1,
            "host_id": "raw-dialer",
            "roles": ["data-store"],
            "capabilities": dialer_capabilities,
            "namespaces": ["toy_world"],
            "extensions": {},
        }))
        .expect("the dialer manifest matches the schema");
        let hello_manifest: HelloManifest =
            serde_json::from_value(serde_json::to_value(&manifest).expect("manifest serializes"))
                .expect("the hello manifest is field-identical");
        let hello = sign_hello_ed25519(&DIALER_SEED, "raw-gate-nonce-0000", &hello_manifest, None)
            .expect("sign the initiator hello");
        pair.client
            .send(&serde_json::to_vec(&hello).expect("hello bytes"))
            .await
            .expect("send the hello");
        let _responder_hello: ConnectHello =
            serde_json::from_slice(&pair.client.recv().await.expect("responder hello"))
                .expect("decode the responder hello");
        let session: ConnectSession =
            serde_json::from_slice(&pair.client.recv().await.expect("session snapshot"))
                .expect("decode the session snapshot");
        assert_eq!(
            session.initiator_peer_id.as_ref() as &str,
            dialer_peer_id().as_str(),
            "the responder bound the session to the dialer"
        );
        let _ = responder_pubkey;
        (Arc::new(pair.client), session.session_id.to_string())
    }

    /// One signed invoke envelope, built the way the contract's 5-field signed object says: the
    /// JCS bytes of `session_id`/`sequence`/`request_id`/`op`/`payload`, never `extensions`.
    pub(super) async fn invoke(
        client: &LoopbackTransport,
        session_id: &str,
        sequence: i64,
        op: &str,
        payload: Value,
    ) -> Value {
        let signed = json!({
            "session_id": session_id,
            "sequence": sequence,
            "request_id": format!("raw-{op}-{sequence}"),
            "op": op,
            "payload": payload,
        });
        let jcs = serde_jcs::to_vec(&signed).expect("the signed object JCS-canonicalizes");
        let signature = SigningKey::from_bytes(&DIALER_SEED).sign(&jcs);
        let mut wire = signed.as_object().expect("signed object").clone();
        wire.insert("extensions".into(), json!({}));
        wire.insert(
            "signature".into(),
            json!(URL_SAFE_NO_PAD.encode(signature.to_bytes())),
        );
        client
            .send(&serde_json::to_vec(&Value::Object(wire)).expect("invoke bytes"))
            .await
            .expect("send the invoke");
        serde_json::from_slice(&client.recv().await.expect("invoke answer")).expect("decode")
    }
}

/// The responder's own gate order, driven on the wire. A scoped `get` and a scoped `put` on a
/// session that never negotiated `spoke-knowledge-scoped` answer the wire `op_unsupported` deny
/// before the face is probed, and a malformed `scope` answers `INVALID_INPUT` before that marker
/// conjunct — the shape first, then the marker, then the face.
///
/// The typed dialer cannot reach any of these: it refuses a declared scope locally, and it cannot
/// serialize a `Scope` that is malformed. Only raw envelopes exercise the responder's branches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_responder_gate_order_puts_the_marker_and_shape_ahead_of_the_face() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    fixture
        .grant("grt_reader_raw", PRINCIPAL, &["knowledge_read"])
        .await;
    let (_dir, serve) = fixture.serve();
    // The face is composed, so a request that passes the gates would be served: the denials below
    // are the gates and not a missing face.
    let surface = knowledge_surface(&serve, raw_wire_admission());
    assert!(
        surface.ports.as_scoped_knowledge().is_some(),
        "the face is composed for this ordering test"
    );
    // A dialer that claims `spoke-knowledge` alone: the responder advertises all three, so the
    // negotiated set is exactly this hello's claim, and the marker is absent from it.
    let (client, session_id) = raw_wire::open_raw_session(&surface, &["spoke-knowledge"]).await;

    // 1. A scoped `get` without the negotiated marker: the marker conjunct, not the face.
    let denied_get = raw_wire::invoke(
        &client,
        &session_id,
        0,
        "port.knowledge.get",
        json!({
            "entry_id": "ke_absent",
            "scope": { "scope_id": SPACE },
        }),
    )
    .await;
    let code = denied_get
        .pointer("/error/code")
        .and_then(Value::as_str)
        .expect("the answer is an error envelope");
    assert_eq!(
        code, "op_unsupported",
        "a declared scope without the marker is the wire deny, before any face call: {denied_get}"
    );
    let message = denied_get
        .pointer("/error/message")
        .and_then(Value::as_str)
        .expect("the deny carries a message");
    assert!(
        message.contains("spoke-knowledge-scoped"),
        "the marker conjunct answered: {message}"
    );

    // 2. A scoped `put` on the same session: the same conjunct, so a peer cannot smuggle a write
    //    scope past a peer that would silently drop it.
    let denied_put = raw_wire::invoke(
        &client,
        &session_id,
        1,
        "port.knowledge.put",
        json!({
            "entry": served_entry("ke_raw_refused", "must not be written"),
            "expected_base_revision": null,
            "scope": { "scope_id": SPACE },
        }),
    )
    .await;
    assert_eq!(
        denied_put.pointer("/error/code").and_then(Value::as_str),
        Some("op_unsupported"),
        "a scoped put without the marker is the wire deny: {denied_put}"
    );

    // 3. A malformed `scope` (a present JSON `null` cognition subtree is shape, not selection)
    //    answers `INVALID_INPUT` — before the marker conjunct would have answered.
    let malformed = raw_wire::invoke(
        &client,
        &session_id,
        2,
        "port.knowledge.get",
        json!({
            "entry_id": "ke_absent",
            "scope": { "scope_id": SPACE, "cognition": Value::Null },
        }),
    )
    .await;
    assert_eq!(
        malformed.pointer("/error/code").and_then(Value::as_str),
        Some("INVALID_INPUT"),
        "the shape gate answers before the marker conjunct: {malformed}"
    );

    // 4. Nothing above reached the store: the forged-entry ids were never written.
    for entry_id in ["ke_raw_refused"] {
        let rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
        )
        .bind(COLLECTION)
        .bind(entry_id)
        .fetch_one(&pool)
        .await
        .expect("count the refused entry");
        assert_eq!(rows, 0, "{entry_id} was never written by a refused op");
    }
}

/// A scoped op WITH the marker negotiated reaches the face, so the denials above are the gates:
/// the same request that answered `op_unsupported` at step 1 is served once the session negotiates
/// `spoke-knowledge-scoped`. This is the negative control that keeps the ordering test honest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_same_scoped_request_is_served_once_the_marker_is_negotiated() {
    let fixture = Fixture::open().await;
    let (_dir, serve) = fixture.serve();
    let surface = knowledge_surface(&serve, raw_wire_admission());
    let (client, session_id) = raw_wire::open_raw_session(&surface, &KNOWLEDGE_CAPABILITIES).await;

    let served = raw_wire::invoke(
        &client,
        &session_id,
        0,
        "port.knowledge.get",
        json!({
            "entry_id": CARRIER,
            "scope": { "scope_id": SPACE },
        }),
    )
    .await;
    assert_eq!(
        served.pointer("/payload/entry_id").and_then(Value::as_str),
        Some(CARRIER),
        "with the marker the same request is served: {served}"
    );
}

// ---------- A storage fault during admission is not a refusal ----------

/// A storage fault while the scoped admission resolves the caller's grants is answered as this
/// request's internal error, not as the fixed `NOT_AUTHORIZED` an unadmitted caller receives.
///
/// Connect maps every admission `Reject` to one `deny_knowledge_scope`, so a fault returned as a
/// `Reject` would be byte-identical to a refusal. The admission therefore raises the fault through
/// the one channel Connect distinguishes — a panic on its blocking pool, which it catches as that
/// request's join error and answers `INTERNAL_ERROR`. This drives both halves through the same
/// signed session, on the same Mind, with only the store's health differing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_storage_fault_during_admission_is_an_internal_error_not_a_refusal() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    let (_dir, serve) = fixture.serve();
    let surface = knowledge_surface(&serve, principal_admission());
    // The marker is advertised, so the scoped op is decodable and reaches the face; the caller
    // holds the Mind's covering grant, so nothing but the store's health decides the answer.
    let session = connect(&surface, &KNOWLEDGE_CAPABILITIES).await;
    let dialer = &*session.adapter;

    // Half one: an intact store serves the admitted caller.
    expect_ok(
        KnowledgeQueryPort::list_knowledge_entries(dialer, &request_scope(SPACE, None)).await,
        "the admitted caller's list over a healthy store",
    );

    // Half two: the same caller, the same request, a store whose grant rows cannot be read. The
    // table is dropped rather than emptied, so the admission's grant read fails rather than
    // returning no rows — the difference between a fault and an admitted-nothing caller.
    sqlx::query("DROP TABLE memory_grants")
        .execute(&pool)
        .await
        .expect("remove the grants table to induce a storage fault");

    let faulty = expect_reject(
        KnowledgeQueryPort::list_knowledge_entries(dialer, &request_scope(SPACE, None)).await,
        "a list while the admission's grant read fails",
    );
    assert_eq!(
        faulty.code,
        SpokeRejectCode::InternalError,
        "a storage fault is an internal error, never the admission refusal: {faulty:?}"
    );
    assert_eq!(
        faulty.message, "knowledge admission failed",
        "Connect's own per-request admission-failure text: {faulty:?}"
    );
    assert_ne!(
        faulty.code,
        SpokeRejectCode::NotAuthorized,
        "the fault is distinguishable from an unadmitted caller: {faulty:?}"
    );
    assert!(
        !faulty.message.contains("memory_grants") && !faulty.message.contains(COLLECTION),
        "the driver text stays host-side: {faulty:?}"
    );
    assert!(
        faulty.details.is_none(),
        "the internal answer carries no details: {faulty:?}"
    );

    assert_caller_absent_for(
        &session,
        &[PRINCIPAL, PEER_TWO_PRINCIPAL],
        "the internal-error answer",
        ExpectsReceipt::No,
    );
}

/// The distinction in one place: with the store healthy, an unadmitted caller and an unknown Mind
/// answer the same fixed refusal; with the store's grant read broken, the fault is an internal
/// error. Together with the test above this pins that the fault path is additive — it changes the
/// answer only for a broken store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_admission_refusal_is_unchanged_for_an_unadmitted_and_an_unknown_mind() {
    let fixture = Fixture::open().await;
    let (_dir, serve) = fixture.serve();
    let surface = knowledge_surface(&serve, two_principal_admission());
    // The second peer holds no grant anywhere: it is the unadmitted case.
    let stranger = connect_as(&surface, &PEER_TWO_SEED, &KNOWLEDGE_CAPABILITIES).await;
    let dialer = &*stranger.adapter;

    for scope_id in [SPACE, "spc_no_such_mind"] {
        let refused = expect_reject(
            KnowledgeQueryPort::list_knowledge_entries(dialer, &request_scope(scope_id, None))
                .await,
            "a list by a caller admitted to nothing",
        );
        assert_eq!(
            refused.code,
            SpokeRejectCode::NotAuthorized,
            "{scope_id}: the fixed admission refusal: {refused:?}"
        );
        assert_eq!(
            refused.message, "knowledge scope not authorized",
            "{scope_id}: Connect's own admission-deny words"
        );
        assert!(
            refused.details.is_none(),
            "{scope_id}: no details, and no hint that the store is healthy: {refused:?}"
        );
    }
}
