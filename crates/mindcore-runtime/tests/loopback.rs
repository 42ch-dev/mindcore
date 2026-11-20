//! Signed in-process loopback over the composed surface against a real SQLite store.
//!
//! A remote consumer dials a `ConnectResponder` built from [`MindcoreServe::surface`] on a
//! throwaway Ed25519 pair over the in-process transport: signed hellos, a signed session
//! snapshot, a fail-closed allowlist, then the Mind tools. Handlers are installed before the
//! dialer connects, exactly as a host must.
//!
//! The runtime serves a **protocol-shaped** store. A freshly opened `MindStore` is not yet
//! protocol-shaped: the pre-release `0001_init.sql` legacy memory tables predate the shape the
//! adapter reads (issue `I-000068`), so this fixture drops them and applies the protocol DDL while
//! the migrated `knowledge_entries` table keeps its own store path. The fixture is therefore not
//! the production open path, and the plan's acceptance is runtime reachability on a
//! protocol-shaped host store rather than on a `MindStore`-opened database.
//!
//! Advertisement honesty is the negative side: a face the host did not attach advertises nothing
//! and denies its ops on the wire, and a tool the runtime does not serve answers `op_unsupported`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ed25519_dalek::SigningKey;
use serde_json::{json, Value};
use tempfile::TempDir;

use mindcore_base::knowledge::KnowledgeScope;
use mindcore_runtime::tools::{ABSORB, DELIBERATE, RECALL_CONTEXT};
use mindcore_runtime::{
    MindProviders, MindcoreServe, MindcoreSurface, ServeFaces, ToolRegistration,
};
use mindcore_spi::{
    CognitiveTaskKind, Embedder, EmbeddingIdentity, Reasoner, SpiError, StructuredReasonRequest,
};
use mindcore_sqlite::MindStore;
use spoke_connect::core::derive_peer_id_from_ed25519_pubkey;
use spoke_connect::remote::{
    connect_remote_adapter, connect_responder, loopback_transport_pair, ConnectResponder,
    ConnectResponderOptions, KnowledgeAdmission, MemoryAdmission, RemoteAdapter,
    RemoteAdapterOptions, RemoteAdapterState, RemoteIdentity, RemoteMemoryService, ToolHandler,
};
use spoke_operations::{
    spoke_ok, spoke_reject, KnowledgeEntryPort, MemoryPort, ScopeQueryPort, SpokeReject,
    SpokeRejectCode, SpokeResult,
};
use spoke_schemas::host_capability_manifest::ToolDescriptor;
use spoke_schemas::{HostCapabilityManifest, MemoryRecallRequest, Principal, Scope};

const SCOPE_ID: &str = "spoke-scope-001";
const COLLECTION: &str = "wren";
const CARRIER: &str = "ke_wren";
const SPACE: &str = "spc_loopback";
const SUBJECT: &str = "subj_loopback";
const PRINCIPAL: &str = "prn_loopback";
const NOW: &str = "2026-11-21T00:00:00.000Z";
const UNSERVED_TOOL: &str = "tools.mindcore.unserved";
/// A host-owned tool the runtime serves beside its own three.
const HOST_TOOL: &str = "tools.mindcore.host_tool";

const DIALER_SEED: [u8; 32] = [0x42u8; 32];
const RESPONDER_SEED: [u8; 32] = [0xA7u8; 32];
/// A second peer the host admits to its own principal, never the carrier route's caller.
const PEER_TWO_SEED: [u8; 32] = [0x5Cu8; 32];
const PEER_TWO_PRINCIPAL: &str = "prn_peer_two";

const PROTOCOL_DDL: &str =
    include_str!("../../mindcore-sqlite/tests/fixtures/settlement-prepare.sql");
const LEGACY_MEMORY_TABLES: &str = "
DROP TABLE memory_records;
DROP TABLE review_receipts;
DROP TABLE memory_candidates;";

/// One store the runtime serves: the protocol memory shape with two retained records, the
/// grants the workflows read, and the knowledge collection the face serves.
struct Fixture {
    dir: TempDir,
    store: MindStore,
}

impl Fixture {
    /// Open a throwaway database and make it protocol-shaped; this is not `MindStore`'s production
    /// open path (see the module note and issue `I-000068`).
    async fn open() -> Self {
        let dir = TempDir::new().expect("temporary directory");
        let store = MindStore::open(&dir.path().join("loopback.db"))
            .await
            .expect("open store");
        let pool = store.pool();
        sqlx::raw_sql(LEGACY_MEMORY_TABLES)
            .execute(&pool)
            .await
            .expect("drop the legacy memory tables");
        sqlx::raw_sql(PROTOCOL_DDL)
            .execute(&pool)
            .await
            .expect("apply the protocol DDL");
        sqlx::query(
            "INSERT INTO memory_spaces \
             (space_id, display_name, authority_host_id, authority_generation, status, revision, \
              created_at) \
             VALUES (?, 'loopback space', 'host_1', 1, 'active', 1, ?)",
        )
        .bind(SPACE)
        .bind(NOW)
        .execute(&pool)
        .await
        .expect("seed space");
        sqlx::query(
            "INSERT INTO memory_subjects \
             (space_id, subject_id, kind_ns, kind_id, status, fenced_contexts_json, revision, \
              created_at) \
             VALUES (?, ?, NULL, NULL, 'active', '[]', 1, ?)",
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
        seed_record(&pool, "rec_north_gate", "the north gate is closed").await;
        seed_record(&pool, "rec_market", "the market opens at dawn").await;
        Self { dir, store }
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

    /// The carrier entry the deliberation holder names.
    async fn seed_carrier(&self) {
        expect_ok(
            self.store
                .knowledge(KnowledgeScope::new(COLLECTION))
                .put_knowledge_entry(
                    serde_json::from_value(json!({
                        "schema_version": 1,
                        "entry_id": CARRIER,
                        "entry_type": "note",
                        "canonical_name": "Wren's carrier",
                        "status": "confirmed",
                        "body": { "summary": "what Wren currently holds" },
                        "extensions": {},
                    }))
                    .expect("carrier JSON matches the schema"),
                    None,
                )
                .await,
            "write the carrier entry",
        );
    }

    /// The store and its directory part company: the engine takes the store, the caller keeps the
    /// directory alive for the session's lifetime.
    fn serve(self) -> (TempDir, MindcoreServe) {
        let Self { dir, store } = self;
        (dir, MindcoreServe::new(store, false, |_targets| Ok(())))
    }
}

async fn seed_record(pool: &sqlx::SqlitePool, record_id: &str, text: &str) {
    sqlx::query(
        "INSERT INTO memory_records \
         (record_id, space_id, subject_id, kind, content_json, scope_visibility, \
          scope_context_id, provenance_json, import_ref_json, occurred_at, tombstone_json, \
          revision, created_at, updated_at) \
         VALUES (?, ?, ?, 'fact', ?, 'shared', NULL, ?, NULL, NULL, NULL, 1, ?, ?)",
    )
    .bind(record_id)
    .bind(SPACE)
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

/// The reserved deliberation id's descriptor, as a host could build one.
fn deliberate_descriptor() -> ToolDescriptor {
    serde_json::from_value(json!({
        "schema_version": 1,
        "capability_id": DELIBERATE,
        "op": DELIBERATE,
        "description": "Derive a validated verdict over supplied evidence.",
        "input": { "type": "object" },
        "output": { "type": "object" },
    }))
    .expect("tool descriptor JSON matches the schema")
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
fn knowledge_admission() -> KnowledgeAdmission {
    let peer_id = dialer_peer_id();
    Arc::new(move |peered: &str| match peered == peer_id {
        true => SpokeResult::Ok(scope_of(SCOPE_ID)),
        false => refuse_knowledge(peered),
    })
}

fn refuse_knowledge(peer_id: &str) -> SpokeResult<Scope> {
    spoke_reject(
        SpokeRejectCode::NotAuthorized,
        format!("peer {peer_id} holds no knowledge admission"),
        None,
    )
}

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
                .any(|record| record["record_id"] == "rec_north_gate")),
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

/// The host's side of the surface. `workflow_caller` is the one principal the deliberation route
/// admits; without it the runtime serves and advertises no `deliberate` at all.
fn faces(
    manifest: HostCapabilityManifest,
    memory: Option<Arc<dyn RemoteMemoryService>>,
    providers: MindProviders,
    workflow_caller: Option<&str>,
) -> ServeFaces {
    ServeFaces {
        manifest,
        scope_id: SCOPE_ID.to_string(),
        knowledge: KnowledgeScope::new(COLLECTION),
        admission: knowledge_admission(),
        memory,
        principals: principal_admission(),
        providers,
        workflow_caller: workflow_caller.map(str::to_string),
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
    json!({
        "request": {
            "space_id": SPACE,
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
    json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "holder": { "kind": "knowledge_entry", "namespace": "mindcore", "id": CARRIER },
        "target": {
            "carrier": { "kind": "knowledge_entry", "namespace": "mindcore", "id": CARRIER },
            "target_id": "prop_north_gate",
            "kind": "proposition",
        },
        "evidence": [{ "id": "ev_gate", "text": "the north gate is closed at dusk" }],
        "timeout_ms": 5_000,
    })
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
            None,
        ),
    );

    assert_eq!(
        capabilities(&surface),
        ["spoke-knowledge", "ke-ownership", RECALL_CONTEXT, ABSORB],
        "the manifest advertises exactly the knowledge face and the served tools"
    );
    assert_eq!(
        surface
            .manifest
            .tools
            .iter()
            .map(|tool| tool.capability_id.as_str())
            .collect::<Vec<_>>(),
        [RECALL_CONTEXT, ABSORB],
        "no deliberation route, so no deliberation tool is installed"
    );

    let session = connect(&surface, &["spoke-knowledge", RECALL_CONTEXT]).await;
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

/// A wired deliberation route does not narrow recall: a different admitted peer holding its own
/// knowledge grant receives its pack with the slot filled from the served collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn another_admitted_peer_recalls_its_knowledge_slot_under_a_wired_route() {
    let fixture = Fixture::open().await;
    fixture.seed_carrier().await;
    fixture
        .grant("grt_recall_peer_two", PEER_TWO_PRINCIPAL, &["recall"])
        .await;
    fixture
        .grant(
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
        Some(PRINCIPAL),
    );
    // Both peers are admitted; only the route's caller may deliberate.
    faces.principals = two_principal_admission();
    let surface = surface(&serve, faces);
    assert!(
        capabilities(&surface).contains(&DELIBERATE),
        "the carrier route is wired: {:?}",
        capabilities(&surface)
    );

    let session = connect_as(&surface, &PEER_TWO_SEED, &[RECALL_CONTEXT]).await;
    // One token per serialized byte: two records plus the carrier entry need a wider budget than
    // the default fixture pack.
    let mut arguments = recall_arguments();
    arguments["max_tokens"] = json!(4_000);
    let result = expect_ok(
        session.adapter.invoke_tool(RECALL_CONTEXT, arguments).await,
        "recall by a peer that is not the route's caller",
    );

    assert_eq!(packed_record_ids(&result), ["rec_market", "rec_north_gate"]);
    assert_eq!(
        packed_knowledge_ids(&result),
        [CARRIER],
        "the peer's own knowledge_read grant fills the slot"
    );
}

/// A configured route never narrows recall's authorization: the route's caller keeps the memory
/// grant but loses `knowledge_read`, and still reads its records — the slot is best effort and
/// degrades to empty exactly as it does with no collection bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_route_caller_without_knowledge_read_recalls_with_an_empty_slot() {
    let fixture = Fixture::open().await;
    fixture.seed_carrier().await;
    fixture.revoke("grt_knowledge").await;
    let (_dir, serve) = fixture.serve();
    let surface = surface(
        &serve,
        faces(
            hello("runtime", &[]),
            None,
            MindProviders::default(),
            Some(PRINCIPAL),
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
/// carrier entry through the composed surface and returns the reasoner's validated verdict.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_consumer_deliberates_through_the_composed_surface() {
    let fixture = Fixture::open().await;
    fixture.seed_carrier().await;
    let pool = fixture.store.pool();
    let (_dir, serve) = fixture.serve();
    let providers = MindProviders {
        reasoner: Some(Arc::new(NoChangeReasoner)),
        ..MindProviders::default()
    };
    let surface = surface(
        &serve,
        faces(hello("runtime", &[]), None, providers, Some(PRINCIPAL)),
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
    // `no_change` is the branch that reads the carrier and writes nothing: the carrier is still at
    // the revision the snapshot read. The settled branch's carrier write is covered by the
    // workflow tests in `mindcore-spoke`.
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

/// The carrier write keeps its one-caller authority: a peer holding `recall` and both knowledge
/// grants in the space is still refused the route's carrier, and nothing is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_that_is_not_the_route_caller_cannot_write_the_carrier() {
    let fixture = Fixture::open().await;
    fixture.seed_carrier().await;
    fixture
        .grant(
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
        Some(PRINCIPAL),
    );
    faces.principals = two_principal_admission();
    let surface = surface(&serve, faces);

    let session = connect_as(&surface, &PEER_TWO_SEED, &[DELIBERATE]).await;
    let reject = expect_reject(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        "deliberate by a peer that is not the route's caller",
    );
    assert_eq!(reject.code, SpokeRejectCode::NotAuthorized);
    assert!(
        reject
            .message
            .contains("is not the admitted carrier caller"),
        "the refusal is the carrier's one-caller rule: {reject:?}"
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
            None,
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
            None,
        ),
    );
    assert!(
        capabilities(&attached).contains(&"spoke-memory"),
        "an attached memory face is advertised: {:?}",
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
            None,
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
        faces(hello("runtime", &[]), None, MindProviders::default(), None),
    );
    assert!(!capabilities(&surface).contains(&UNSERVED_TOOL));

    let session = connect(&surface, &[UNSERVED_TOOL]).await;
    assert_wire_deny(
        session.adapter.invoke_tool(UNSERVED_TOOL, json!({})).await,
        UNSERVED_TOOL,
        "op_unsupported",
    );
}

/// Advertisement honesty for the deliberation route: a surface assembled without a workflow caller
/// installs no `deliberate` handler, advertises none, and answers the invoke with
/// `op_unsupported` even when the dialer claims the id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unwired_deliberation_route_is_neither_advertised_nor_served() {
    let fixture = Fixture::open().await;
    let (_dir, serve) = fixture.serve();
    let surface = surface(
        &serve,
        faces(hello("runtime", &[]), None, MindProviders::default(), None),
    );
    assert!(
        !capabilities(&surface).contains(&DELIBERATE),
        "an unwired deliberation route is never advertised: {:?}",
        capabilities(&surface)
    );

    let session = connect(&surface, &[DELIBERATE]).await;
    assert_wire_deny(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        DELIBERATE,
        "op_unsupported",
    );
}

/// A host registration cannot restore the unwired route: the reserved id is dropped from the
/// final registration set, so it is neither advertised nor installed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_registration_cannot_restore_an_unwired_deliberation_route() {
    let fixture = Fixture::open().await;
    let (_dir, serve) = fixture.serve();
    let mut faces = faces(hello("runtime", &[]), None, MindProviders::default(), None);
    faces.tools = vec![ToolRegistration::new(
        deliberate_descriptor(),
        |_responder| {
            let handler: ToolHandler =
                Arc::new(|arguments: Value| Box::pin(async move { spoke_ok(arguments) }));
            handler
        },
    )];
    let surface = surface(&serve, faces);

    assert!(
        !capabilities(&surface).contains(&DELIBERATE),
        "a host registration cannot re-advertise the unwired route: {:?}",
        capabilities(&surface)
    );
    assert!(
        surface
            .manifest
            .tools
            .iter()
            .all(|tool| tool.capability_id.as_str() != DELIBERATE),
        "the reserved descriptor is dropped from the manifest"
    );

    let session = connect(&surface, &[DELIBERATE]).await;
    assert_wire_deny(
        session
            .adapter
            .invoke_tool(DELIBERATE, deliberate_arguments())
            .await,
        DELIBERATE,
        "op_unsupported",
    );
}

/// A host registration is served, not just advertised: the signed session invokes it and the
/// host's own handler answers with its own result.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_registration_reaches_its_handler_over_the_signed_session() {
    let fixture = Fixture::open().await;
    let (_dir, serve) = fixture.serve();
    let mut faces = faces(hello("runtime", &[]), None, MindProviders::default(), None);
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
            None,
        ),
    );
    let session = connect(&surface, &[RECALL_CONTEXT]).await;

    // Identity 1 writes its partition and installs the one query slot.
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

    // Rotation moves the slot to identity 2; identity 1's rows stay until it is retired.
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

    // Identity 2's slot and rows are still warm: a recall needs no provider call at all.
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

    // The retired identity recomputes instead of serving rows it no longer has.
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
            None,
        ),
    );
    let session = connect(&surface, &[RECALL_CONTEXT]).await;

    // Two recalls under identity 1 leave its query slot warm: the second embeds nothing.
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

    // Retiring the identity that owns the cached slot empties the slot with its partition.
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

    // Identity 2 installs a warm slot of its own; retiring identity 1 leaves it warm.
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
