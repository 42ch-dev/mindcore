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
use serde_json::{json, Number, Value};
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
    ConnectResponderOptions, MemoryAdmission, RemoteAdapter, RemoteAdapterOptions,
    RemoteAdapterState, RemoteIdentity, RemoteMemoryService, ToolHandler,
};
use spoke_operations::{
    spoke_ok, spoke_reject, validate_judgment, AdoptionPort, HistoryPort, KnowledgeEntryPort,
    MemoryPort, ScopeQueryPort, SpokeReject, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::host_capability_manifest::ToolDescriptor;
use spoke_schemas::{
    HostCapabilityManifest, KnowledgeAdoptRequest, MemoryGrantsListRequest,
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

const DIALER_SEED: [u8; 32] = [0x42u8; 32];
const RESPONDER_SEED: [u8; 32] = [0xA7u8; 32];
/// A second peer the host admits to its own principal, never the carrier route's caller.
const PEER_TWO_SEED: [u8; 32] = [0x5Cu8; 32];
const PEER_TWO_PRINCIPAL: &str = "prn_peer_two";

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
        seed_record_in(&pool, SPACE_TWO, "rec_north_gate_two", "the north gate is closed").await;
        seed_record_in(&pool, SPACE_TWO, "rec_market_two", "the market opens at dawn").await;
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
            payload["records"].as_array().is_some_and(|records| records
                .iter()
                .any(|record| record["record_id"]
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
            payload["records"].as_array().is_some_and(|records| records
                .iter()
                .any(|record| record["record_id"].as_str().is_some_and(|id| id.starts_with("rec_north_gate")))),
            "the authorized memory page reached the reasoner: {}",
            request.user_payload
        );
        let gate_record = payload["records"].as_array()
            .and_then(|records| records.iter()
                .find(|record| record["record_id"].as_str().is_some_and(|id| id.starts_with("rec_north_gate"))))
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
}

impl Drop for Session {
    fn drop(&mut self) {
        self.adapter.close();
        self.responder.close();
    }
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
        transport: Arc::new(pair.client),
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
    Session { adapter, responder }
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
    let mut faces = faces(
        hello("runtime", &[]),
        None,
        MindProviders::default(),
    );
    faces.principals = two_principal_admission();
    let surface = surface(&serve, faces);

    let session = connect_as(&surface, &PEER_TWO_SEED, &[RECALL_CONTEXT]).await;
    let mut arguments = recall_arguments_in(SPACE_TWO);
    arguments["max_tokens"] = json!(4_000);
    let result = expect_ok(
        session
            .adapter
            .invoke_tool(RECALL_CONTEXT, arguments)
            .await,
        "recall by a peer admitted to the second Mind",
    );

    assert_eq!(packed_record_ids(&result), ["rec_market_two", "rec_north_gate_two"]);
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
        faces(
            hello("runtime", &[]),
            None,
            MindProviders::default(),
        ),
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
    let surface = surface(
        &serve,
        faces(hello("runtime", &[]), None, providers),
    );
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
    let surface = surface(
        &serve,
        faces(hello("runtime", &[]), None, providers),
    );

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
    let surface = surface(
        &serve,
        faces(hello("runtime", &[]), None, providers),
    );

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
        second_pack["knowledge"][0]["body"]["summary"], "the knowledge of the second mind, not the first",
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
    with_holder["holder"] = json!({ "kind": "knowledge_entry", "namespace": "mindcore", "id": CARRIER });
    let reject = expect_reject(
        first
            .adapter
            .invoke_tool(DELIBERATE, with_holder)
            .await,
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
        reject.message.contains("the caller is not admitted to the named mind"),
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
    let mut faces = faces(
        hello("runtime", &[]),
        None,
        MindProviders::default(),
    );
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
    let mut faces = faces(
        hello("runtime", &[]),
        None,
        MindProviders::default(),
    );
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
    sqlx::query("UPDATE memory_grants SET expires_at = 'not-a-time' WHERE grant_id = 'grt_knowledge'")
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
        .grant_in_second(
            "grt_shared_peer",
            PRINCIPAL,
            &["recall", "knowledge_read"],
        )
        .await;
    let (_dir, serve) = fixture.serve();
    let mut faces = faces(
        hello("runtime", &[]),
        None,
        MindProviders::default(),
    );
    faces.principals = principal_admission();
    let surface = surface(&serve, faces);

    let session = connect(&surface, &[RECALL_CONTEXT]).await;
    for (space, carrier, name, summary) in [
        (SPACE, CARRIER, "ke_carrier's carrier", "what this holder currently holds"),
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
    assert_eq!(polled["receipt"]["op"], json!("memory.exports.create"));
    assert_eq!(polled["receipt"]["status"], json!("committed"));
    assert_eq!(polled["receipt"]["receipt_id"], json!(receipt_id));
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
        faces(
            hello("runtime", &[]),
            None,
            MindProviders::default(),
        ),
    );
    assert!(!capabilities(&surface).contains(&UNSERVED_TOOL));

    let session = connect(&surface, &[UNSERVED_TOOL]).await;
    assert_wire_deny(
        session.adapter.invoke_tool(UNSERVED_TOOL, json!({})).await,
        UNSERVED_TOOL,
        "op_unsupported",
    );
}

/// The three un-routed families leave composition and advertisement together: the manifest names
/// none of them, no face is composed, and a dialer that claims them is served a missing port.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_un_routed_face_answers_a_missing_port() {
    let fixture = Fixture::open().await;
    let (_dir, serve) = fixture.serve();
    let claimed = [
        "spoke-knowledge",
        "ke-ownership",
        "spoke-history",
        "spoke-knowledge-adoption",
    ];
    let surface = surface(
        &serve,
        faces(
            hello("runtime", &claimed),
            None,
            MindProviders::default(),
        ),
    );
    for capability in claimed {
        assert!(
            !capabilities(&surface).contains(&capability),
            "the un-routed face {capability} is not advertised: {:?}",
            capabilities(&surface)
        );
    }
    assert!(surface.ports.as_knowledge().is_none());
    assert!(surface.ports.as_history().is_none());
    assert!(surface.ports.as_adoption().is_none());

    let session = connect(&surface, &claimed).await;
    let dialer = &*session.adapter;
    assert_wire_deny(
        KnowledgeEntryPort::get_knowledge_entry(dialer, "ke_absent").await,
        "port.knowledge.get",
        "op_unsupported",
    );
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
        AdoptionPort::adopt_knowledge_entry(
            dialer,
            &principal(),
            &scope_of(SCOPE_ID),
            &adopt,
        )
        .await,
        "knowledge.adopt",
        "capability_missing",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_registration_reaches_its_handler_over_the_signed_session() {
    let fixture = Fixture::open().await;
    let (_dir, serve) = fixture.serve();
    let mut faces = faces(
        hello("runtime", &[]),
        None,
        MindProviders::default(),
    );
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

