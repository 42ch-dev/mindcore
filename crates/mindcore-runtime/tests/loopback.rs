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
//! and denies its ops on the wire, and a tool the runtime does not serve answers `op_unsupported`.

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ed25519_dalek::SigningKey;
use serde_json::{json, Number, Value};
use tempfile::TempDir;

use mindcore_base::ids::SpaceId;
use mindcore_base::ir::MemorySpaceRow;
use mindcore_base::knowledge::{adoption_anchor_parts, KnowledgeScope};
use mindcore_runtime::tools::{ABSORB, DELIBERATE, RECALL_CONTEXT};
use mindcore_runtime::{
    AdoptionRoute, MindProviders, MindcoreServe, MindcoreSurface, ServeFaces, ToolRegistration,
};
use mindcore_spi::{
    CognitiveTaskKind, Embedder, EmbeddingIdentity, PolicyDecision, PolicyHook, Reasoner,
    SettlementDraft, SpiError, StructuredReasonRequest,
};
use mindcore_spoke::{ServedMemory, TransferService};
use mindcore_sqlite::{MindStore, ScopedKnowledgeStore, SqliteMemoryRepository};
use spoke_connect::core::derive_peer_id_from_ed25519_pubkey;
use spoke_connect::remote::{
    connect_remote_adapter, connect_responder, loopback_transport_pair, ConnectResponder,
    ConnectResponderOptions, KnowledgeAdmission, MemoryAdmission, RemoteAdapter,
    RemoteAdapterOptions, RemoteAdapterState, RemoteIdentity, RemoteMemoryService, ToolHandler,
};
use spoke_operations::{
    revision_snapshots_equal_canonical, spoke_ok, spoke_reject, validate_judgment, AdoptionPort,
    HistoryPort, KnowledgeEntryPort, MemoryPort, ScopeQueryPort, SpokeReject, SpokeRejectCode,
    SpokeResult,
};
use spoke_schemas::host_capability_manifest::ToolDescriptor;
use spoke_schemas::knowledge_entry::{KnowledgeEntryDisclosure, KnowledgeEntryOwner};
use spoke_schemas::{
    HostCapabilityManifest, KnowledgeAdoptRequest, KnowledgeEntry, MemoryRecallRequest, Principal,
    Scope,
};

const SCOPE_ID: &str = "spoke-scope-001";
const COLLECTION: &str = "wren";
/// The carrier entry id every Mind is created with; the wire never names it.
const CARRIER: &str = "ke_carrier";
/// The second Mind's own collection; its carrier entry id is the one every Mind is created with.
const COLLECTION_TWO: &str = "mora";
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
/// The host-configured collection adoption reads from, never one of this fixture's routes.
const SHARED: &str = "shared";
/// The id naming the adoption source (`SHARED`) on the wire.
const SHARED_SCOPE: &str = "spc_shared";
/// The capability the adoption face advertises and `knowledge.adopt` requires.
const ADOPTION: &str = "spoke-knowledge-adoption";
/// A scope id the adoption face never serves as its source or its destination.
const OTHER_SCOPE: &str = "spc_other";
const SHARED_ENTRY: &str = "ke_shared";
/// A source retired into [`TERMINAL_ENTRY`], the live entry an adoption anchors instead.
const RETIRED_ENTRY: &str = "ke_retired";
const TERMINAL_ENTRY: &str = "ke_terminal";
/// An owner-private source: no viewpoint-free read reaches it, adoption included.
const PRIVATE_ENTRY: &str = "ke_private";
/// A source retired into [`HUGE_TERMINAL`], whose stored revision is above the served ceiling.
const HUGE_RETIRED: &str = "ke_huge_retired";
/// The live terminal of [`HUGE_RETIRED`]: revision `2^53 + 2` in its column and its payload.
const HUGE_TERMINAL: &str = "ke_huge_terminal";
/// A revision SQLite accepts and the served adoption success cannot carry.
const HUGE_REVISION: u64 = (1 << 53) + 2;
/// A stored two-node cycle, seeded with direct markers because the store never writes one.
const CYCLE_A: &str = "ke_cycle_a";
const CYCLE_B: &str = "ke_cycle_b";
/// A destination slot holding a row retired into [`SLOT_SUCCESSOR`]: taken, yet hidden from the
/// live-only read an adoption's destination half makes.
const RETIRED_SLOT: &str = "ke_retired_slot";
const SLOT_SUCCESSOR: &str = "ke_slot_successor";

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
        .expect("seed the second mind's carrier entry");
        sqlx::query(
            "INSERT INTO minds (space_id, knowledge_collection, carrier_entry_id) VALUES (?, ?, ?)",
        )
        .bind(SPACE_TWO)
        .bind(COLLECTION_TWO)
        .bind(CARRIER)
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

    /// The database file this fixture opened, for a test that reopens it after a restart.
    fn db_path(&self) -> std::path::PathBuf {
        self.dir.path().join(DB_FILE)
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

/// The same admission with one entry or entry-type restriction on the admitted scope: the id is the
/// one this surface serves, so only the selection can refuse the caller's destination.
fn restricted_knowledge_admission(restriction: Value) -> KnowledgeAdmission {
    let peer_id = dialer_peer_id();
    Arc::new(move |peered: &str| match peered == peer_id {
        true => {
            let mut scope = json!({ "scope_id": SCOPE_ID });
            for (member, value) in restriction.as_object().expect("an object restriction") {
                scope[member.as_str()] = value.clone();
            }
            spoke_ok(
                serde_json::from_value(scope).expect("the admitted scope JSON matches the schema"),
            )
        }
        false => refuse_knowledge(peered),
    })
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

/// The host's side of the surface. `routes` is the retired per-principal route map the binding no
/// longer reads: the registry row decides every collection, so callers pass it and it is ignored.
fn faces(
    manifest: HostCapabilityManifest,
    memory: Option<Arc<dyn ServedMemory>>,
    providers: MindProviders,
    _routes: BTreeMap<String, KnowledgeScope>,
) -> ServeFaces {
    ServeFaces {
        manifest,
        scope_id: SCOPE_ID.to_string(),
        knowledge: KnowledgeScope::new(COLLECTION),
        admission: knowledge_admission(),
        memory,
        history: false,
        principals: principal_admission(),
        providers,
        adoption: None,
        tools: Vec::new(),
    }
}

/// A retired route map shell: the registry row replaced routing, so this only keeps the call
/// sites shaped as they were.
fn routes(principals: &[&str]) -> BTreeMap<String, KnowledgeScope> {
    principals
        .iter()
        .map(|principal| ((*principal).to_string(), KnowledgeScope::new(COLLECTION)))
        .collect()
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
            routes(&[]),
        ),
    );

    assert_eq!(
        capabilities(&surface),
        ["spoke-knowledge", "ke-ownership", "spoke-judgment", RECALL_CONTEXT, ABSORB, DELIBERATE],
        "the manifest advertises the knowledge face and all three workflow tools"
    );
    assert_eq!(
        surface
            .manifest
            .tools
            .iter()
            .map(|tool| tool.capability_id.as_str())
            .collect::<Vec<_>>(),
        [RECALL_CONTEXT, ABSORB, DELIBERATE],
        "the registry row routes every Mind, so the deliberation tool is always installed"
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
        routes(&[PRINCIPAL]),
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
        [CARRIER],
        "the peer's own knowledge_read grant fills the slot from its own route: {result}"
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
            routes(&[PRINCIPAL]),
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
        faces(hello("runtime", &[]), None, providers, routes(&[PRINCIPAL])),
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
            routes(&[PRINCIPAL]),
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
        faces(hello("runtime", &[]), None, providers, routes(&[PRINCIPAL])),
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
        faces(hello("runtime", &[]), None, providers, routes(&[PRINCIPAL])),
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
    let workflow_routes = routes(&[PRINCIPAL]);
    let mut faces = faces(
        hello("runtime", &[]),
        None,
        MindProviders {
            reasoner: Some(Arc::new(SettlingReasoner(SETTLING_RATIONALE))),
            policy_hook: Some(Arc::new(AutoPolicy)),
            ..MindProviders::default()
        },
        workflow_routes,
    );
    faces.principals = two_principal_admission();
    let surface = surface(&serve, faces);

    let first = connect(&surface, &[DELIBERATE, RECALL_CONTEXT]).await;
    let untouched = collection_state(&pool, COLLECTION_TWO, CARRIER).await;
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
        carrier_revision(&pool, COLLECTION_TWO, CARRIER).await,
        1,
        "the second principal's carrier keeps its seeded revision"
    );
    assert_eq!(
        carrier_assessors(&pool, COLLECTION, CARRIER).await,
        [PRINCIPAL]
    );
    assert!(
        carrier_assessors(&pool, COLLECTION_TWO, CARRIER)
            .await
            .is_empty(),
        "the second collection carries no assessment"
    );
    assert_eq!(
        collection_state(&pool, COLLECTION_TWO, CARRIER).await,
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
    assert_eq!(settled["record"]["holder"]["id"], CARRIER);
    assert_eq!(settled["record"]["assessor"]["id"], PEER_TWO_PRINCIPAL);
    assert_eq!(
        settled["mind_state_version"], 2,
        "the second write descends from its own carrier, never the first's"
    );
    assert_eq!(
        carrier_revision(&pool, COLLECTION_TWO, CARRIER).await,
        2
    );
    assert_eq!(
        carrier_revision(&pool, COLLECTION, CARRIER).await,
        2,
        "the first principal's carrier keeps the revision its own call left"
    );
    assert_eq!(
        carrier_assessors(&pool, COLLECTION_TWO, CARRIER).await,
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
        [CARRIER],
        "the second principal's slot lists only its own Mind's collection: {second_pack}"
    );
    assert_eq!(
        second_pack["knowledge"][0]["canonical_name"], "ke_carrier's carrier",
        "the slot carries this collection's entry, not the other Mind's copy: {second_pack}"
    );

    let first_before = collection_state(&pool, COLLECTION, CARRIER).await;
    let second_before = collection_state(&pool, COLLECTION_TWO, CARRIER).await;
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
        collection_state(&pool, COLLECTION_TWO, CARRIER).await,
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
        routes(&[PRINCIPAL]),
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
            routes(&[]),
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
            routes(&[]),
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
            routes(&[]),
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
            routes(&[]),
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
            routes(&[]),
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
            routes(&[]),
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

/// One shared-source entry the adoption copies: a note whose summary names what the source holds.

fn shared_entry(entry_id: &str, summary: &str) -> KnowledgeEntry {
    serde_json::from_value(json!({
        "schema_version": 1,
        "entry_id": entry_id,
        "entry_type": "note",
        "canonical_name": format!("{entry_id}'s source"),
        "status": "confirmed",
        "body": { "summary": summary },
        "extensions": {},
    }))
    .expect("entry JSON matches the schema")
}

/// One adoption request over the configured source and the admitted destination scope.
fn adopt_request(source_entry_id: &str, source_revision: u64, key: &str) -> KnowledgeAdoptRequest {
    serde_json::from_value(json!({
        "source_scope_id": SHARED_SCOPE,
        "source_entry_id": source_entry_id,
        "source_revision": source_revision,
        "destination_scope_id": SCOPE_ID,
        "idempotency_key": key,
    }))
    .expect("request JSON matches the schema")
}

/// The fixture's faces with the one shared adoption source attached.
fn adopting_faces(workflow_routes: BTreeMap<String, KnowledgeScope>) -> ServeFaces {
    ServeFaces {
        adoption: Some(AdoptionRoute {
            scope_id: SHARED_SCOPE.to_string(),
            source: KnowledgeScope::new(SHARED),
        }),
        ..faces(
            hello("runtime", &[]),
            None,
            MindProviders::default(),
            workflow_routes,
        )
    }
}

/// The shared source the adoption face reads: a live note, a note retired into a live terminal,
/// and an owner-private note no viewpoint-free read reaches.
async fn seed_shared_source(pool: &sqlx::SqlitePool) {
    let shared = ScopedKnowledgeStore::new(pool.clone(), KnowledgeScope::new(SHARED));
    for (entry_id, summary) in [
        (SHARED_ENTRY, "the north gate closes at dusk"),
        (RETIRED_ENTRY, "the gatekeeper's first note"),
        (TERMINAL_ENTRY, "the keeper walks the north gate"),
    ] {
        expect_ok(
            shared
                .put_knowledge_entry(shared_entry(entry_id, summary), None)
                .await,
            "seed the shared source",
        );
    }
    expect_ok(
        shared
            .supersede_entry(RETIRED_ENTRY, 1, TERMINAL_ENTRY)
            .await,
        "retire the paired source into its terminal",
    );
    // One terminal whose stored revision sits above the served ceiling. SQLite accepts it, so only
    // the runtime's pre-write domain check keeps it out of a response.
    expect_ok(
        shared
            .put_knowledge_entry(shared_entry(HUGE_TERMINAL, "the keeper's last note"), None)
            .await,
        "seed the out-of-domain terminal",
    );
    expect_ok(
        shared
            .put_knowledge_entry(shared_entry(HUGE_RETIRED, "a note retired upward"), None)
            .await,
        "seed the source retired into it",
    );
    let huge = i64::try_from(HUGE_REVISION).expect("the seeded revision fits SQLite's integer");
    sqlx::query(
        "UPDATE knowledge_entries SET revision = ?, \
         payload = json_set(payload, '$.revision', ?) \
         WHERE collection = ? AND entry_id = ?",
    )
    .bind(huge)
    .bind(huge)
    .bind(SHARED)
    .bind(HUGE_TERMINAL)
    .execute(pool)
    .await
    .expect("stamp the out-of-domain revision");
    expect_ok(
        shared.supersede_entry(HUGE_RETIRED, 1, HUGE_TERMINAL).await,
        "retire the source into the out-of-domain terminal",
    );
    let mut private = shared_entry(PRIVATE_ENTRY, "the keeper keeps a private note");
    private.disclosure =
        Some(KnowledgeEntryDisclosure::from_str("owner-private").expect("non-empty disclosure"));
    private.owner = Some(KnowledgeEntryOwner::try_from("kb_keeper").expect("non-empty owner"));
    expect_ok(
        shared.put_knowledge_entry(private, None).await,
        "seed the owner-private source",
    );
}

/// A two-node cycle in the shared collection's markers: the store refuses to write one, so the
/// seed is two direct updates, the shape a corrupted marker pair would read as.
async fn seed_cycle(pool: &sqlx::SqlitePool) {
    let shared = ScopedKnowledgeStore::new(pool.clone(), KnowledgeScope::new(SHARED));
    for entry_id in [CYCLE_A, CYCLE_B] {
        expect_ok(
            shared
                .put_knowledge_entry(shared_entry(entry_id, "a cycle member"), None)
                .await,
            "seed a cycle member",
        );
    }
    for (entry_id, successor) in [(CYCLE_A, CYCLE_B), (CYCLE_B, CYCLE_A)] {
        sqlx::query(
            "UPDATE knowledge_entries SET superseded_by = ? \
             WHERE collection = ? AND entry_id = ?",
        )
        .bind(successor)
        .bind(SHARED)
        .bind(entry_id)
        .execute(pool)
        .await
        .expect("seed the cycle marker");
    }
}

/// A destination slot holding a row retired into [`SLOT_SUCCESSOR`]: the slot is taken, and the
/// live-only read an adoption's destination half makes answers absence for it.
async fn seed_retired_destination(pool: &sqlx::SqlitePool) {
    let destination = ScopedKnowledgeStore::new(pool.clone(), KnowledgeScope::new(COLLECTION));
    for (entry_id, summary) in [
        (RETIRED_SLOT, "somebody's retired row"),
        (SLOT_SUCCESSOR, "somebody's live row"),
    ] {
        expect_ok(
            destination
                .put_knowledge_entry(shared_entry(entry_id, summary), None)
                .await,
            "seed the destination slot",
        );
    }
    expect_ok(
        destination
            .supersede_entry(RETIRED_SLOT, 1, SLOT_SUCCESSOR)
            .await,
        "retire the destination occupant",
    );
}

/// The slot a refused claim must leave alone: the stored revision and retirement marker of
/// `entry_id` in the destination collection, read without the live-only predicate.
async fn destination_slot(pool: &sqlx::SqlitePool, entry_id: &str) -> (i64, Option<String>) {
    sqlx::query_as(
        "SELECT revision, superseded_by FROM knowledge_entries \
         WHERE collection = ? AND entry_id = ?",
    )
    .bind(COLLECTION)
    .bind(entry_id)
    .fetch_one(pool)
    .await
    .expect("read the destination slot")
}

/// How many rows one collection stores under `entry_id`: the destination slot a copy claims.
async fn destination_rows(pool: &sqlx::SqlitePool, entry_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM knowledge_entries WHERE collection = ? AND entry_id = ?",
    )
    .bind(COLLECTION)
    .bind(entry_id)
    .fetch_one(pool)
    .await
    .expect("count the destination rows")
}

/// Advertisement honesty for the adoption family: a surface with no `AdoptionRoute` advertises no
/// `spoke-knowledge-adoption`, and a dialer that negotiated it is refused at the gate before any
/// handler; the same dialer reaches `knowledge.adopt` once the route is configured.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adoption_is_advertised_and_served_only_with_the_face() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    seed_shared_source(&pool).await;
    let (_dir, serve) = fixture.serve();
    let request = adopt_request(SHARED_ENTRY, 1, "advertise-once");

    let absent = surface(
        &serve,
        faces(
            hello("runtime", &[]),
            None,
            MindProviders::default(),
            routes(&[PRINCIPAL]),
        ),
    );
    assert!(
        !capabilities(&absent).contains(&ADOPTION),
        "an uncomposed adoption face is never advertised: {:?}",
        capabilities(&absent)
    );
    assert!(
        capabilities(&absent).contains(&"spoke-knowledge"),
        "the knowledge face stays advertised either way: {:?}",
        capabilities(&absent)
    );
    assert!(
        absent.ports.as_adoption().is_none(),
        "an uncomposed adoption face installs no adoption port"
    );
    let session = connect(&absent, &[ADOPTION, "spoke-knowledge"]).await;
    assert_wire_deny(
        AdoptionPort::adopt_knowledge_entry(
            &*session.adapter,
            &principal(),
            &scope_of(SCOPE_ID),
            &request,
        )
        .await,
        "knowledge.adopt",
        "capability_missing",
    );
    drop(session);

    let attached = surface(&serve, adopting_faces(routes(&[PRINCIPAL])));
    assert!(
        capabilities(&attached).contains(&ADOPTION),
        "a composed adoption face is advertised: {:?}",
        capabilities(&attached)
    );
    assert!(
        attached.ports.as_adoption().is_some(),
        "the adoption face is composed over the routed store"
    );
    let session = connect(&attached, &[ADOPTION, "spoke-knowledge"]).await;
    let adopted = wire(expect_ok(
        AdoptionPort::adopt_knowledge_entry(
            &*session.adapter,
            &principal(),
            &scope_of(SCOPE_ID),
            &request,
        )
        .await,
        "adopt over the signed session",
    ));
    assert_eq!(adopted["outcome"], json!("adopted"), "{adopted}");
}

/// The served adoption end to end: the copy's anchor names the configured source triple, a repeat
/// under either key serves the stored copy instead of writing a second one, a moved source with the
/// old copy in place is the stale-copy conflict, and a retired source anchors its live terminal
/// under a supplied destination id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knowledge_adopt_anchors_the_terminal_and_repeats_idempotently() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    seed_shared_source(&pool).await;
    let (_dir, serve) = fixture.serve();
    let surface = surface(&serve, adopting_faces(routes(&[PRINCIPAL])));
    let session = connect(&surface, &[ADOPTION, "spoke-knowledge"]).await;
    let dialer = &*session.adapter;
    let destination = scope_of(SCOPE_ID);

    let adopted = wire(expect_ok(
        AdoptionPort::adopt_knowledge_entry(
            dialer,
            &principal(),
            &destination,
            &adopt_request(SHARED_ENTRY, 1, "adopt-once"),
        )
        .await,
        "the first adoption",
    ));
    assert_eq!(adopted["outcome"], json!("adopted"), "{adopted}");
    assert_eq!(adopted["entry"]["entry_id"], json!(SHARED_ENTRY));
    assert_eq!(
        adopted["entry"]["adoption_anchor"],
        json!({
            "schema_version": 1,
            "source_scope_id": SHARED_SCOPE,
            "source_entry_id": SHARED_ENTRY,
            "source_revision": 1,
            "extensions": {},
        }),
        "the anchor is the configured source triple: {adopted}"
    );
    assert_eq!(
        destination_rows(&pool, SHARED_ENTRY).await,
        1,
        "the first adoption writes the one copy"
    );
    let stored = wire(expect_ok(
        KnowledgeEntryPort::get_knowledge_entry(dialer, SHARED_ENTRY).await,
        "read the stored copy back through the port",
    ));
    assert_eq!(
        stored, adopted["entry"],
        "the port serves the entry the first call wrote: {stored}"
    );

    let replay = wire(expect_ok(
        AdoptionPort::adopt_knowledge_entry(
            dialer,
            &principal(),
            &destination,
            &adopt_request(SHARED_ENTRY, 1, "adopt-once"),
        )
        .await,
        "the same key and payload",
    ));
    assert_eq!(
        replay["outcome"],
        json!("already_present"),
        "a replay reports the stored copy: {replay}"
    );
    assert_eq!(
        replay["entry"]["revision"], adopted["entry"]["revision"],
        "the replay serves the copy the first call wrote"
    );
    assert_eq!(
        destination_rows(&pool, SHARED_ENTRY).await,
        1,
        "a replay writes no second copy"
    );

    let fresh = wire(expect_ok(
        AdoptionPort::adopt_knowledge_entry(
            dialer,
            &principal(),
            &destination,
            &adopt_request(SHARED_ENTRY, 1, "adopt-again"),
        )
        .await,
        "a fresh key on the same tuple",
    ));
    assert_eq!(
        fresh["outcome"],
        json!("already_present"),
        "a fresh key re-reads the anchor instead of writing: {fresh}"
    );
    assert_eq!(
        fresh["entry"], stored,
        "a fresh key serves the stored copy, not a different entry: {fresh}"
    );
    assert_eq!(
        destination_rows(&pool, SHARED_ENTRY).await,
        1,
        "a fresh key still leaves one copy"
    );
    let after = wire(expect_ok(
        KnowledgeEntryPort::get_knowledge_entry(dialer, SHARED_ENTRY).await,
        "re-read the copy after the fresh key",
    ));
    assert_eq!(
        after, stored,
        "a fresh key rewrites nothing in the destination slot: {after}"
    );

    // Move the source, then ask for the revision it sits at: the copy in the destination slot was
    // adopted at revision 1, so the slot is a stale copy, not the revision asked for.
    let shared = ScopedKnowledgeStore::new(pool.clone(), KnowledgeScope::new(SHARED));
    expect_ok(
        shared
            .put_knowledge_entry(
                shared_entry(
                    SHARED_ENTRY,
                    "the north gate closes at dusk; a keeper walks it",
                ),
                Some(1),
            )
            .await,
        "move the source to revision 2",
    );
    let stale = expect_reject(
        AdoptionPort::adopt_knowledge_entry(
            dialer,
            &principal(),
            &destination,
            &adopt_request(SHARED_ENTRY, 2, "adopt-moved"),
        )
        .await,
        "the source moved after the copy was adopted",
    );
    assert_eq!(stale.code, SpokeRejectCode::RevisionConflict, "{stale:?}");
    let details = stale.details.clone().unwrap_or_default();
    assert_eq!(
        details.len(),
        1,
        "the refusal carries exactly adopted_revision: {details:?}"
    );
    assert_eq!(details.get("adopted_revision"), Some(&json!(1)));

    let mut retired = adopt_request(RETIRED_ENTRY, 1, "adopt-retired");
    retired.destination_entry_id = Some("ke_copy".parse().expect("non-empty destination id"));
    let terminal = wire(expect_ok(
        AdoptionPort::adopt_knowledge_entry(dialer, &principal(), &destination, &retired).await,
        "adopt a retired source under a supplied destination id",
    ));
    assert_eq!(terminal["outcome"], json!("adopted"), "{terminal}");
    assert_eq!(
        terminal["entry"]["entry_id"],
        json!("ke_copy"),
        "the copy takes the supplied destination id"
    );
    let anchor = &terminal["entry"]["adoption_anchor"];
    assert_eq!(anchor["source_scope_id"], json!(SHARED_SCOPE));
    assert_eq!(
        anchor["source_entry_id"],
        json!(TERMINAL_ENTRY),
        "the anchor names the terminal, never the retired source: {terminal}"
    );
    assert_eq!(anchor["source_revision"], json!(1));
    let copy = expect_ok(
        ScopedKnowledgeStore::new(pool.clone(), KnowledgeScope::new(COLLECTION))
            .get_knowledge_entry("ke_copy")
            .await,
        "read the adopted copy from the destination collection",
    );
    assert_eq!(
        adoption_anchor_parts(&copy),
        Some((SHARED_SCOPE, TERMINAL_ENTRY, 1)),
        "the stored copy carries the terminal's anchor: {copy:?}"
    );
}

/// The refusal matrix over the signed session: one generic absence for an unserved source scope, a
/// missing source and an owner-private source; `NOT_AUTHORIZED` for another destination scope; a
/// stored marker cycle as `SOURCE_RETIRED` with its visited chain; and `IDEMPOTENCY_CONFLICT` when a
/// key replays another payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knowledge_adopt_refuses_with_the_contract_codes() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    seed_shared_source(&pool).await;
    seed_cycle(&pool).await;
    let (_dir, serve) = fixture.serve();
    let surface = surface(&serve, adopting_faces(routes(&[PRINCIPAL])));
    let session = connect(&surface, &[ADOPTION, "spoke-knowledge"]).await;
    let dialer = &*session.adapter;
    let destination = scope_of(SCOPE_ID);

    let mut foreign = adopt_request(SHARED_ENTRY, 1, "refuse-foreign-source");
    foreign.source_scope_id = OTHER_SCOPE.parse().expect("non-empty scope id");
    let foreign = expect_reject(
        AdoptionPort::adopt_knowledge_entry(dialer, &principal(), &destination, &foreign).await,
        "another source scope",
    );
    let missing = expect_reject(
        AdoptionPort::adopt_knowledge_entry(
            dialer,
            &principal(),
            &destination,
            &adopt_request("ke_never_seeded", 1, "refuse-missing"),
        )
        .await,
        "a source the shared collection does not hold",
    );
    let private = expect_reject(
        AdoptionPort::adopt_knowledge_entry(
            dialer,
            &principal(),
            &destination,
            &adopt_request(PRIVATE_ENTRY, 1, "refuse-private"),
        )
        .await,
        "an owner-private source",
    );
    for (context, reject) in [
        ("another source scope", &foreign),
        ("a missing source", &missing),
        ("an owner-private source", &private),
    ] {
        assert_eq!(
            reject.code,
            SpokeRejectCode::NotFound,
            "{context}: {reject:?}"
        );
        assert!(
            reject.details.clone().unwrap_or_default().is_empty(),
            "{context} carries no details: {reject:?}"
        );
    }
    assert_eq!(
        foreign.message, missing.message,
        "an unserved source scope is the same generic absence as a missing source"
    );

    let mut outside = adopt_request(SHARED_ENTRY, 1, "refuse-foreign-destination");
    outside.destination_scope_id = OTHER_SCOPE.parse().expect("non-empty scope id");
    let unauthorized = expect_reject(
        AdoptionPort::adopt_knowledge_entry(dialer, &principal(), &destination, &outside).await,
        "another destination scope",
    );
    assert_eq!(
        unauthorized.code,
        SpokeRejectCode::NotAuthorized,
        "{unauthorized:?}"
    );
    assert!(unauthorized.details.is_none(), "{unauthorized:?}");

    let cycle = expect_reject(
        AdoptionPort::adopt_knowledge_entry(
            dialer,
            &principal(),
            &destination,
            &adopt_request(CYCLE_A, 1, "refuse-cycle"),
        )
        .await,
        "a stored retirement cycle",
    );
    assert_eq!(cycle.code, SpokeRejectCode::SourceRetired, "{cycle:?}");
    let details = cycle.details.clone().unwrap_or_default();
    assert_eq!(
        details.len(),
        1,
        "the refusal carries exactly retirement_chain: {details:?}"
    );
    assert_eq!(
        details.get("retirement_chain"),
        Some(&json!([CYCLE_A, CYCLE_B, CYCLE_A])),
        "the chain lists the visited ids, the repeat last"
    );

    expect_ok(
        AdoptionPort::adopt_knowledge_entry(
            dialer,
            &principal(),
            &destination,
            &adopt_request(TERMINAL_ENTRY, 1, "recorded-key"),
        )
        .await,
        "the call a key records",
    );
    let mut replayed = adopt_request(TERMINAL_ENTRY, 1, "recorded-key");
    replayed.destination_entry_id = Some("ke_recorded".parse().expect("non-empty destination id"));
    let conflict = expect_reject(
        AdoptionPort::adopt_knowledge_entry(dialer, &principal(), &destination, &replayed).await,
        "the same key with another payload",
    );
    assert_eq!(
        conflict.code,
        SpokeRejectCode::IdempotencyConflict,
        "{conflict:?}"
    );

    for (context, reject) in [
        ("another source scope", &foreign),
        ("a missing source", &missing),
        ("an owner-private source", &private),
        ("another destination scope", &unauthorized),
        ("a stored cycle", &cycle),
        ("a replayed key", &conflict),
    ] {
        assert!(
            !reject.message.contains("knowledge_entries"),
            "{context} names no table: {reject:?}"
        );
    }
}

/// An occupied destination slot answers the ordinary occupancy refusal whatever the admitted
/// selection makes of its occupant: hidden and visible occupants are byte-identical, so the answer
/// reports occupancy and nothing about the occupant. A slot the selection cannot claim answers the
/// same refusal and writes nothing, and a genuinely free, usable slot still creates.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_occupied_destination_answers_occupancy_whatever_the_selection_shows() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    seed_shared_source(&pool).await;
    // A destination occupant the caller may or may not be shown, depending on the admitted
    // selection: either way it holds the slot `ke_shared` names.
    expect_ok(
        ScopedKnowledgeStore::new(pool.clone(), KnowledgeScope::new(COLLECTION))
            .put_knowledge_entry(shared_entry(SHARED_ENTRY, "somebody else's row"), None)
            .await,
        "seed the destination occupant",
    );
    let (_dir, serve) = fixture.serve();
    let surface_of = |admission: KnowledgeAdmission| {
        let mut admitted = adopting_faces(routes(&[PRINCIPAL]));
        admitted.admission = admission;
        surface(&serve, admitted)
    };

    let mut refusals = Vec::new();
    for (context, surface) in [
        ("a visible occupant", surface_of(knowledge_admission())),
        (
            "an entry-id selection that hides the occupant",
            surface_of(restricted_knowledge_admission(
                json!({ "entry_ids": ["ke_somebody_else"] }),
            )),
        ),
        (
            "an entry-type selection that hides the occupant",
            surface_of(restricted_knowledge_admission(
                json!({ "entry_types": ["belief"] }),
            )),
        ),
    ] {
        let session = connect(&surface, &[ADOPTION, "spoke-knowledge"]).await;
        let reject = expect_reject(
            AdoptionPort::adopt_knowledge_entry(
                &*session.adapter,
                &principal(),
                &scope_of(SCOPE_ID),
                &adopt_request(SHARED_ENTRY, 1, context),
            )
            .await,
            context,
        );
        assert_eq!(
            reject.code,
            SpokeRejectCode::RevisionConflict,
            "{context} is an occupancy, not an authorization answer: {reject:?}"
        );
        assert!(
            reject.details.is_none(),
            "{context} carries no details: {reject:?}"
        );
        drop(session);
        refusals.push((context, reject));
    }
    let (_, visible) = &refusals[0];
    for (context, hidden) in &refusals[1..] {
        assert_eq!(
            hidden, visible,
            "{context} must not be distinguishable from a visible occupant"
        );
    }

    // A slot the selection cannot claim is not adoptable either: it answers the same occupancy and
    // leaves the collection without a copy.
    let restricted = surface_of(restricted_knowledge_admission(
        json!({ "entry_types": ["belief"] }),
    ));
    let session = connect(&restricted, &[ADOPTION, "spoke-knowledge"]).await;
    let reject = expect_reject(
        AdoptionPort::adopt_knowledge_entry(
            &*session.adapter,
            &principal(),
            &scope_of(SCOPE_ID),
            &adopt_request(TERMINAL_ENTRY, 1, "unusable-free-slot"),
        )
        .await,
        "a free slot outside the admitted selection",
    );
    assert_eq!(
        reject.code,
        SpokeRejectCode::RevisionConflict,
        "a free slot this call may not claim answers the occupancy: {reject:?}"
    );
    assert!(
        reject.details.is_none(),
        "the unusable-slot refusal carries no details: {reject:?}"
    );
    assert_eq!(
        destination_rows(&pool, TERMINAL_ENTRY).await,
        0,
        "the unusable free slot writes nothing"
    );
    drop(session);

    // The same free slot under an admitting selection still creates.
    let admitting = surface_of(knowledge_admission());
    let session = connect(&admitting, &[ADOPTION, "spoke-knowledge"]).await;
    let adopted = wire(expect_ok(
        AdoptionPort::adopt_knowledge_entry(
            &*session.adapter,
            &principal(),
            &scope_of(SCOPE_ID),
            &adopt_request(TERMINAL_ENTRY, 1, "unrestricted-selection"),
        )
        .await,
        "an unrestricted admission still adopts into a free slot",
    ));
    assert_eq!(adopted["outcome"], json!("adopted"), "{adopted}");
    assert_eq!(
        destination_rows(&pool, TERMINAL_ENTRY).await,
        1,
        "the unrestricted admission writes the one copy"
    );
    let stored = wire(expect_ok(
        KnowledgeEntryPort::get_knowledge_entry(&*session.adapter, TERMINAL_ENTRY).await,
        "read the stored copy back through the knowledge face",
    ));
    drop(session);

    // The same slot re-read under a selection that cannot see it: the repeat is the ordinary
    // occupancy again — never the stored copy, never a distinct answer — and the slot is left
    // byte-identical.
    let surface = surface_of(restricted_knowledge_admission(
        json!({ "entry_ids": ["ke_somebody_else"] }),
    ));
    let session = connect(&surface, &[ADOPTION, "spoke-knowledge"]).await;
    let reject = expect_reject(
        AdoptionPort::adopt_knowledge_entry(
            &*session.adapter,
            &principal(),
            &scope_of(SCOPE_ID),
            &adopt_request(TERMINAL_ENTRY, 1, "restricted-reread"),
        )
        .await,
        "a repeat under a selection that hides the stored copy",
    );
    assert_eq!(
        reject.code,
        SpokeRejectCode::RevisionConflict,
        "the hidden repeat answers occupancy instead of serving the copy: {reject:?}"
    );
    assert!(
        reject.details.is_none(),
        "the hidden repeat carries no details: {reject:?}"
    );
    drop(session);
    assert_eq!(
        destination_rows(&pool, TERMINAL_ENTRY).await,
        1,
        "a refused repeat writes no second copy"
    );
    let session = connect(&admitting, &[ADOPTION, "spoke-knowledge"]).await;
    let after = wire(expect_ok(
        KnowledgeEntryPort::get_knowledge_entry(&*session.adapter, TERMINAL_ENTRY).await,
        "re-read the copy under an admitting scope",
    ));
    assert_eq!(
        after, stored,
        "a refused repeat leaves the destination slot unchanged: {after}"
    );
}

/// A destination slot whose occupant is retired reads as absent to the live-only destination read,
/// so the adoption must answer the ordinary occupancy instead: `REVISION_CONFLICT` with no details
/// over a signed session, and the slot's revision and retirement marker stay as they were.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retired_destination_occupant_answers_occupancy_over_the_signed_session() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    seed_shared_source(&pool).await;
    seed_retired_destination(&pool).await;
    let before = destination_slot(&pool, RETIRED_SLOT).await;
    let (_dir, serve) = fixture.serve();
    let surface = surface(&serve, adopting_faces(routes(&[PRINCIPAL])));
    let session = connect(&surface, &[ADOPTION, "spoke-knowledge"]).await;

    let mut claim = adopt_request(SHARED_ENTRY, 1, "retired-destination");
    claim.destination_entry_id = Some(RETIRED_SLOT.parse().expect("non-empty destination id"));
    let reject = expect_reject(
        AdoptionPort::adopt_knowledge_entry(
            &*session.adapter,
            &principal(),
            &scope_of(SCOPE_ID),
            &claim,
        )
        .await,
        "a claim on a retired destination slot",
    );
    assert_eq!(
        reject.code,
        SpokeRejectCode::RevisionConflict,
        "a retired occupant is occupancy, not an absence: {reject:?}"
    );
    assert!(
        reject.details.is_none(),
        "the retired-slot refusal carries no details: {reject:?}"
    );
    drop(session);

    assert_eq!(
        destination_slot(&pool, RETIRED_SLOT).await,
        before,
        "a refused claim leaves the retired slot's revision and marker unchanged"
    );
    assert_eq!(
        destination_rows(&pool, RETIRED_SLOT).await,
        1,
        "the refused claim writes no second row into the slot"
    );
}

/// A stored source can sit at a revision the served wire cannot carry. The call refuses before the
/// copy is written, so a committed copy is never reported as a transport failure and the
/// destination stays empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adoption_refuses_a_terminal_revision_outside_the_served_range() {
    let fixture = Fixture::open().await;
    let pool = fixture.store.pool();
    seed_shared_source(&pool).await;
    let (_dir, serve) = fixture.serve();
    let surface = surface(&serve, adopting_faces(routes(&[PRINCIPAL])));
    let session = connect(&surface, &[ADOPTION, "spoke-knowledge"]).await;
    let dialer = &*session.adapter;

    // A retired source whose live terminal sits at 2^53 + 2: the request's own revision is in range
    // and is never compared against a retired source, so only the resolved terminal can refuse.
    let reject = expect_reject(
        AdoptionPort::adopt_knowledge_entry(
            dialer,
            &principal(),
            &scope_of(SCOPE_ID),
            &adopt_request(HUGE_RETIRED, 1, "adopt-out-of-domain"),
        )
        .await,
        "a terminal revision above the served ceiling",
    );
    assert_eq!(reject.code, SpokeRejectCode::InternalError, "{reject:?}");
    assert_eq!(
        reject.message, "knowledge adoption could not complete",
        "the refusal carries the fixed fault message: {reject:?}"
    );
    assert_eq!(
        reject
            .details
            .as_ref()
            .and_then(|details| details.get("kind"))
            .and_then(Value::as_str),
        Some("store"),
        "the refusal names the failing stage: {reject:?}"
    );
    assert_eq!(
        destination_rows(&pool, HUGE_TERMINAL).await,
        0,
        "an unservable terminal writes no copy"
    );
    assert_eq!(
        destination_rows(&pool, HUGE_RETIRED).await,
        0,
        "and no copy under the requested id either"
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
        routes(&[]),
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
            routes(&[]),
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
            routes(&[]),
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

/// One entry the history face replays: a note whose summary names what that revision held.
fn history_entry(entry_id: &str, summary: &str) -> KnowledgeEntry {
    serde_json::from_value(json!({
        "schema_version": 1,
        "entry_id": entry_id,
        "entry_type": "note",
        "canonical_name": format!("{entry_id}'s history"),
        "status": "confirmed",
        "body": { "summary": summary },
        "extensions": {},
    }))
    .expect("entry JSON matches the schema")
}

/// Advertisement honesty for the history family: the dialer claims `spoke-history` in both
/// sessions, yet both ops are denied while the host composes no history service and served once
/// it does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_is_advertised_and_served_only_with_the_face() {
    let fixture = Fixture::open().await;
    expect_ok(
        fixture
            .store
            .knowledge(KnowledgeScope::new(COLLECTION))
            .put_knowledge_entry(history_entry("ke_history", "the gate opens at dawn"), None)
            .await,
        "seed the replayed entry",
    );
    let (_dir, serve) = fixture.serve();

    let absent = surface(
        &serve,
        faces(
            hello("runtime", &["spoke-history"]),
            None,
            MindProviders::default(),
            routes(&[]),
        ),
    );
    assert!(
        !capabilities(&absent).contains(&"spoke-history"),
        "an uncomposed history face is never advertised: {:?}",
        capabilities(&absent)
    );
    let session = connect(&absent, &["spoke-history"]).await;
    assert_wire_deny(
        HistoryPort::get_history_snapshot(
            &*session.adapter,
            &principal(),
            &scope_of(SCOPE_ID),
            "knowledge_entry",
            "ke_history",
            1,
        )
        .await,
        "history.revisions.get",
        "capability_missing",
    );
    assert_wire_deny(
        session
            .adapter
            .prune_history(
                &scope_of(SCOPE_ID),
                "knowledge_entry",
                "ke_history",
                1,
                None,
            )
            .await,
        "history.revisions.prune",
        "capability_missing",
    );
    drop(session);

    let mut attached = faces(
        hello("runtime", &["spoke-history"]),
        None,
        MindProviders::default(),
        routes(&[]),
    );
    attached.history = true;
    let attached = surface(&serve, attached);
    assert!(
        capabilities(&attached).contains(&"spoke-history"),
        "a composed history face is advertised: {:?}",
        capabilities(&attached)
    );
    let session = connect(&attached, &["spoke-history"]).await;
    let snapshot = expect_ok(
        HistoryPort::get_history_snapshot(
            &*session.adapter,
            &principal(),
            &scope_of(SCOPE_ID),
            "knowledge_entry",
            "ke_history",
            1,
        )
        .await,
        "replay revision 1 over the signed session",
    );
    assert_eq!(snapshot.revision, 1);
    assert_eq!(snapshot.entity_id.as_str(), "ke_history");
    let pruned = expect_ok(
        session
            .adapter
            .prune_history(
                &scope_of(SCOPE_ID),
                "knowledge_entry",
                "ke_history",
                1,
                None,
            )
            .await,
        "prune the live row's empty checkpoint history",
    );
    assert_eq!(pruned.deleted, 0, "the live row is not a checkpoint");
    assert_eq!(pruned.floor_revision, None, "a no-op reports no floor");
}

/// The served history face end to end: three revisions over the signed session, both historic
/// states replayed byte-identically, one explicit prune, then the refusal matrix's history rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_replays_prunes_and_refuses_over_a_signed_session() {
    let fixture = Fixture::open().await;
    let (_dir, serve) = fixture.serve();
    let mut attached = faces(
        hello("runtime", &["spoke-history", "spoke-knowledge"]),
        None,
        MindProviders::default(),
        routes(&[]),
    );
    attached.history = true;
    let surface = surface(&serve, attached);
    let session = connect(&surface, &["spoke-history", "spoke-knowledge"]).await;
    let dialer = &*session.adapter;
    let scope = scope_of(SCOPE_ID);

    let created = expect_ok(
        KnowledgeEntryPort::put_knowledge_entry(
            dialer,
            history_entry("ke_history", "the gate opens at dawn"),
            None,
        )
        .await,
        "create the entry over the signed session",
    );
    let updated_once = expect_ok(
        KnowledgeEntryPort::put_knowledge_entry(
            dialer,
            history_entry("ke_history", "the gate closes at dusk"),
            created.revision,
        )
        .await,
        "update the entry to revision 2",
    );
    let updated_twice = expect_ok(
        KnowledgeEntryPort::put_knowledge_entry(
            dialer,
            history_entry("ke_history", "the gate opens again at dawn"),
            updated_once.revision,
        )
        .await,
        "update the entry to revision 3",
    );
    assert_eq!(updated_twice.revision, Some(3));

    for (revision, historic) in [(1u64, &created), (2u64, &updated_once)] {
        let replay = expect_ok(
            HistoryPort::get_history_snapshot(
                dialer,
                &principal(),
                &scope,
                "knowledge_entry",
                "ke_history",
                revision,
            )
            .await,
            "replay a historic revision",
        );
        assert_eq!(replay.entity_kind.as_str(), "knowledge_entry");
        assert_eq!(replay.entity_id.as_str(), "ke_history");
        assert_eq!(replay.revision, revision);
        assert_eq!(
            serde_json::to_value(&replay.snapshot).expect("the snapshot serializes"),
            serde_json::to_value(historic).expect("the historic entry serializes"),
            "revision {revision} replays the entry as it stood"
        );
        let again = expect_ok(
            HistoryPort::get_history_snapshot(
                dialer,
                &principal(),
                &scope,
                "knowledge_entry",
                "ke_history",
                revision,
            )
            .await,
            "replay the same revision again",
        );
        assert!(
            expect_ok(
                revision_snapshots_equal_canonical(&replay, &again),
                "compare two replays of one revision"
            ),
            "two replays of revision {revision} are canonically equal"
        );
    }

    let pruned = expect_ok(
        session
            .adapter
            .prune_history(
                &scope,
                "knowledge_entry",
                "ke_history",
                1,
                Some("prune-one"),
            )
            .await,
        "prune the entry to its newest checkpoint",
    );
    assert_eq!(pruned.deleted, 1, "the older checkpoint was deleted");
    assert_eq!(
        pruned.floor_revision,
        Some(2),
        "the floor is the newest deleted revision plus one"
    );

    let reject = expect_reject(
        HistoryPort::get_history_snapshot(
            dialer,
            &principal(),
            &scope,
            "knowledge_entry",
            "ke_history",
            1,
        )
        .await,
        "replay a revision below the pruned floor",
    );
    assert_eq!(reject.code, SpokeRejectCode::HistoryTruncated);
    let details = reject.details.clone().unwrap_or_default();
    assert_eq!(
        details.len(),
        1,
        "the refusal carries exactly floor_revision: {details:?}"
    );
    assert_eq!(details.get("floor_revision"), Some(&json!(2)));
    expect_ok(
        HistoryPort::get_history_snapshot(
            dialer,
            &principal(),
            &scope,
            "knowledge_entry",
            "ke_history",
            2,
        )
        .await,
        "a revision equal to the floor still replays",
    );

    let reject = expect_reject(
        HistoryPort::get_history_snapshot(
            dialer,
            &principal(),
            &scope,
            "knowledge_entry",
            "ke_never_seeded",
            1,
        )
        .await,
        "replay an unknown entity id",
    );
    assert_eq!(reject.code, SpokeRejectCode::NotFound);
    assert!(
        reject.details.clone().unwrap_or_default().is_empty(),
        "an absence carries no details"
    );

    let reject = expect_reject(
        HistoryPort::get_history_snapshot(
            dialer,
            &principal(),
            &scope,
            "relation",
            "rel_history",
            1,
        )
        .await,
        "replay an entity kind this store never keeps",
    );
    assert_eq!(reject.code, SpokeRejectCode::HistoryEntityUnsupported);
    let details = reject.details.clone().unwrap_or_default();
    assert_eq!(
        details.len(),
        1,
        "the refusal carries exactly entity_kind: {details:?}"
    );
    assert_eq!(details.get("entity_kind"), Some(&json!("relation")));
}

/// Two history surfaces on one database route to different collections. They share a wire scope,
/// a principal and an idempotency key, and the second surface's prune must run rather than replay
/// the first surface's recorded outcome for a collection it does not serve.
///
/// This fails before the namespace: both surfaces derive the same dedup key, so the second answers
/// the first's `{deleted: 1}` for an entry its own collection does not hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prune_does_not_replay_across_collections_on_one_database() {
    let fixture = Fixture::open().await;
    let store = fixture.store.knowledge(KnowledgeScope::new(COLLECTION));
    let mut base = None;
    for summary in [
        "the gate opens at dawn",
        "the gate closes at dusk",
        "the gate opens again at dawn",
    ] {
        base = expect_ok(
            store
                .put_knowledge_entry(history_entry("ke_history", summary), base)
                .await,
            "seed the first collection's history",
        )
        .revision;
    }
    let scope = scope_of(SCOPE_ID);
    let (dir, serve) = fixture.serve();

    let mut first = faces(
        hello("runtime", &["spoke-history"]),
        None,
        MindProviders::default(),
        routes(&[]),
    );
    first.history = true;
    let first_surface = surface(&serve, first);
    let first_session = connect(&first_surface, &["spoke-history"]).await;
    let pruned = expect_ok(
        first_session
            .adapter
            .prune_history(&scope, "knowledge_entry", "ke_history", 1, Some("key-one"))
            .await,
        "the first collection's prune",
    );
    assert_eq!(pruned.deleted, 1);
    assert_eq!(pruned.floor_revision, Some(2));

    // Same wire scope, same principal, same idempotency key — another collection.
    let mut second = faces(
        hello("runtime", &["spoke-history"]),
        None,
        MindProviders::default(),
        routes(&[]),
    );
    second.history = true;
    second.knowledge = KnowledgeScope::new(COLLECTION_TWO);
    let second_surface = surface(&serve, second);
    let second_session = connect(&second_surface, &["spoke-history"]).await;

    let reject = expect_reject(
        second_session
            .adapter
            .prune_history(&scope, "knowledge_entry", "ke_history", 1, Some("key-one"))
            .await,
        "the second collection holds no such entry, so its own prune runs and refuses",
    );
    assert_eq!(
        reject.code,
        SpokeRejectCode::NotFound,
        "the second surface executed its own prune instead of replaying the first's outcome"
    );

    // The refusal is now the second surface's own recorded outcome for that key.
    let replay = expect_reject(
        second_session
            .adapter
            .prune_history(&scope, "knowledge_entry", "ke_history", 1, Some("key-one"))
            .await,
        "the second surface's same-key replay",
    );
    assert_eq!(replay.code, SpokeRejectCode::NotFound);
    assert_eq!(replay, reject, "the recorded refusal replays verbatim");

    // And the first surface still replays its own recorded success.
    let replay = expect_ok(
        first_session
            .adapter
            .prune_history(&scope, "knowledge_entry", "ke_history", 1, Some("key-one"))
            .await,
        "the first surface's same-key replay",
    );
    assert_eq!(
        serde_json::to_vec(&replay).expect("the result serializes"),
        serde_json::to_vec(&pruned).expect("the result serializes"),
        "the recorded outcome replays byte-equal within its own collection"
    );

    drop(dir);
}

/// The prune's idempotency contract over the wire: one key replays its recorded outcome, the same
/// key with another payload conflicts, and a fresh key runs the prune again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prune_replays_its_recorded_outcome_for_the_same_key() {
    let fixture = Fixture::open().await;
    let store = fixture.store.knowledge(KnowledgeScope::new(COLLECTION));
    let mut base = None;
    for summary in [
        "the gate opens at dawn",
        "the gate closes at dusk",
        "the gate opens again at dawn",
    ] {
        base = expect_ok(
            store
                .put_knowledge_entry(history_entry("ke_history", summary), base)
                .await,
            "seed the entry's history",
        )
        .revision;
    }
    let (_dir, serve) = fixture.serve();
    let mut attached = faces(
        hello("runtime", &["spoke-history"]),
        None,
        MindProviders::default(),
        routes(&[]),
    );
    attached.history = true;
    let surface = surface(&serve, attached);
    let session = connect(&surface, &["spoke-history"]).await;
    let dialer = &*session.adapter;
    let scope = scope_of(SCOPE_ID);

    let first = expect_ok(
        dialer
            .prune_history(&scope, "knowledge_entry", "ke_history", 1, Some("key-one"))
            .await,
        "the first prune",
    );
    assert_eq!(first.deleted, 1);
    assert_eq!(first.floor_revision, Some(2));

    // One more revision after the first prune leaves two checkpoints (2 and 3), so a replay that
    // re-ran the prune instead of replaying its record would delete checkpoint 2 and report a
    // higher floor. The record is the only thing keeping that checkpoint alive.
    expect_ok(
        store
            .put_knowledge_entry(
                history_entry("ke_history", "the gate is bolted at midnight"),
                base,
            )
            .await,
        "write another revision after the first prune",
    );

    let replay = expect_ok(
        session
            .adapter
            .prune_history(&scope, "knowledge_entry", "ke_history", 1, Some("key-one"))
            .await,
        "the same key and payload",
    );
    assert_eq!(
        serde_json::to_vec(&replay).expect("the result serializes"),
        serde_json::to_vec(&first).expect("the result serializes"),
        "the recorded outcome replays byte-equal"
    );
    for revision in [2u64, 3] {
        expect_ok(
            HistoryPort::get_history_snapshot(
                dialer,
                &principal(),
                &scope,
                "knowledge_entry",
                "ke_history",
                revision,
            )
            .await,
            "the replay deleted nothing, so both checkpoints above the floor still exist",
        );
    }

    let reject = expect_reject(
        session
            .adapter
            .prune_history(&scope, "knowledge_entry", "ke_history", 2, Some("key-one"))
            .await,
        "the same key with another keep",
    );
    assert_eq!(reject.code, SpokeRejectCode::IdempotencyConflict);

    let fresh = expect_ok(
        session
            .adapter
            .prune_history(&scope, "knowledge_entry", "ke_history", 1, Some("key-two"))
            .await,
        "a new key runs the prune again",
    );
    assert_eq!(
        fresh.deleted, 1,
        "the checkpoint written after the first prune is now the older one"
    );
    assert_eq!(
        fresh.floor_revision,
        Some(3),
        "the fresh key really pruned: the floor moved past checkpoint 2"
    );
    let reject = expect_reject(
        HistoryPort::get_history_snapshot(
            dialer,
            &principal(),
            &scope,
            "knowledge_entry",
            "ke_history",
            2,
        )
        .await,
        "replay the checkpoint the fresh key pruned",
    );
    assert_eq!(reject.code, SpokeRejectCode::HistoryTruncated);
    assert_eq!(
        reject
            .details
            .clone()
            .unwrap_or_default()
            .get("floor_revision"),
        Some(&json!(3))
    );
}

/// The record lives in the database file, not in the process: after the session, the surface and
/// the serve are gone, a reopened file replays the prune's outcome instead of executing again
/// against checkpoints the original call never saw.
///
/// This fails against the in-memory default — a fresh surface starts with an empty store, the
/// retry answers `Unseen`, and the re-run deletes checkpoint 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prune_replays_its_recorded_outcome_after_the_store_is_reopened() {
    let fixture = Fixture::open().await;
    let path = fixture.db_path();
    let store = fixture.store.knowledge(KnowledgeScope::new(COLLECTION));
    let mut base = None;
    for summary in [
        "the gate opens at dawn",
        "the gate closes at dusk",
        "the gate opens again at dawn",
    ] {
        base = expect_ok(
            store
                .put_knowledge_entry(history_entry("ke_history", summary), base)
                .await,
            "seed the entry's history",
        )
        .revision;
    }
    let scope = scope_of(SCOPE_ID);

    let (dir, serve) = fixture.serve();
    let first = {
        let mut attached = faces(
            hello("runtime", &["spoke-history"]),
            None,
            MindProviders::default(),
            routes(&[]),
        );
        attached.history = true;
        let surface = surface(&serve, attached);
        let session = connect(&surface, &["spoke-history"]).await;
        let first = expect_ok(
            session
                .adapter
                .prune_history(&scope, "knowledge_entry", "ke_history", 1, Some("key-one"))
                .await,
            "the first prune",
        );
        assert_eq!(first.deleted, 1);
        assert_eq!(first.floor_revision, Some(2));
        first
    };

    // One more revision leaves checkpoints 2 and 3 above the floor, so a retry that re-ran the
    // prune instead of replaying its record would delete checkpoint 2 and report a higher floor.
    expect_ok(
        store
            .put_knowledge_entry(
                history_entry("ke_history", "the gate is bolted at midnight"),
                base,
            )
            .await,
        "write another revision after the first prune",
    );

    // The whole serving stack goes away: the record has to be on disk to survive this.
    drop(store);
    drop(serve);

    let reopened = MindStore::open(&path)
        .await
        .expect("reopen the same database file");
    let serve = MindcoreServe::new(reopened, false, |_targets| Ok(()));
    let mut attached = faces(
        hello("runtime", &["spoke-history"]),
        None,
        MindProviders::default(),
        routes(&[]),
    );
    attached.history = true;
    let surface = surface(&serve, attached);
    let session = connect(&surface, &["spoke-history"]).await;
    let dialer = &*session.adapter;

    let replay = expect_ok(
        dialer
            .prune_history(&scope, "knowledge_entry", "ke_history", 1, Some("key-one"))
            .await,
        "the same key and payload after a reopen",
    );
    assert_eq!(
        serde_json::to_vec(&replay).expect("the result serializes"),
        serde_json::to_vec(&first).expect("the result serializes"),
        "the recorded outcome replays byte-equal across a reopen"
    );
    for revision in [2u64, 3] {
        expect_ok(
            HistoryPort::get_history_snapshot(
                dialer,
                &principal(),
                &scope,
                "knowledge_entry",
                "ke_history",
                revision,
            )
            .await,
            "the replayed prune deleted nothing, so both checkpoints above the floor still exist",
        );
    }

    let reject = expect_reject(
        dialer
            .prune_history(&scope, "knowledge_entry", "ke_history", 2, Some("key-one"))
            .await,
        "the same key with another keep after a reopen",
    );
    assert_eq!(reject.code, SpokeRejectCode::IdempotencyConflict);

    drop(dir);
}
