//! Memory-only Connect composition proof against SPOKE `spoke-connect =0.17.0`
//! (`remote-adapter` feature).
//!
//! Both peers advertise exactly `["spoke-memory"]` in their signed hellos over
//! an in-process `loopback_transport_pair`. The responder attaches a memory
//! face only: 0.17 has no public empty or memory-only composite constructor, so
//! the test supplies a local [`RemoteServePorts`] whose single override is
//! `as_memory`. That face drives a real `mindcore_spoke::MemoryAdapter`
//! `memory.recall` on a caller-owned SQLite transaction, with the principal
//! admitted from the authenticated peer (MEM-R003), never from the payload.
//!
//! The baseline six stay absent: a real baseline op
//! (`port.scope.list_timeline_events`) is denied on the wire with
//! `op_unsupported` instead of running a stub.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use ed25519_dalek::SigningKey;
use serde_json::{json, Value};
use sqlx::{Sqlite, SqlitePool, Transaction};
use tempfile::TempDir;

use mindcore_spoke::{MemoryAdapter, MemoryStage};
use mindcore_sqlite::MindStore;
use spoke_connect::core::derive_peer_id_from_ed25519_pubkey;
use spoke_connect::remote::{
    connect_remote_adapter, connect_responder, loopback_transport_pair, ConnectResponder,
    ConnectResponderOptions, MemoryAdmission, RemoteAdapter, RemoteAdapterOptions,
    RemoteAdapterState, RemoteIdentity, RemoteMemoryService, RemoteServePorts,
};
use spoke_operations::{
    spoke_ok, spoke_reject, MemoryPort, ScopeQueryPort, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::{HostCapabilityManifest, MemoryRecallRequest, Principal, Scope};

const SPACE: &str = "sp_connect";
const SUBJECT: &str = "s_connect";
const PRINCIPAL: &str = "connect-fixture-principal";
const NOW: &str = "2026-11-20T09:20:01.123Z";

/// The Nexus-shaped memory schema with the space, subject, recall grant and
/// two retained records the responder's admitted recall reads.
struct Fixture {
    _dir: TempDir,
    pool: SqlitePool,
}

impl Fixture {
    async fn open() -> Self {
        let dir = TempDir::new().expect("temporary directory");
        let pool = MindStore::open(&dir.path().join("mem_connect.db"))
            .await
            .expect("open store")
            .pool();

        sqlx::query(
            "INSERT INTO memory_spaces \
             (space_id, display_name, authority_host_id, authority_generation, status, revision, \
              created_at) \
             VALUES (?, 'connect space', 'host_1', 1, 'active', 1, ?)",
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
        sqlx::query(
            "INSERT INTO memory_grants \
             (grant_id, space_id, subject_id, principal_id, capabilities_json, \
              context_ids_json, shared_access, expires_at, revoked_at, revision, created_at) \
             VALUES ('grt_connect', ?, NULL, ?, '[\"recall\"]', '[]', 0, NULL, NULL, 1, ?)",
        )
        .bind(SPACE)
        .bind(PRINCIPAL)
        .bind(NOW)
        .execute(&pool)
        .await
        .expect("seed grant");
        seed_record(&pool, "rec_a", "alpha memory", "2026-11-20T09:00:02.000Z").await;
        seed_record(&pool, "rec_b", "beta memory", "2026-11-20T09:00:01.000Z").await;

        Self { _dir: dir, pool }
    }
}

async fn seed_record(pool: &SqlitePool, record_id: &str, text: &str, created_at: &str) {
    let provenance = json!({
        "source_context_ids": [],
        "anchors": [],
        "captured_by": "adapter",
        "captured_at": NOW,
    });
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
    .bind(provenance.to_string())
    .bind(created_at)
    .bind(created_at)
    .execute(pool)
    .await
    .expect("seed record");
}

fn memory_only_manifest(host_id: &str) -> HostCapabilityManifest {
    serde_json::from_value(json!({
        "schema_version": 1,
        "host_id": host_id,
        "roles": ["memory-provider"],
        "capabilities": ["spoke-memory"],
        "namespaces": ["toy_world"],
        "extensions": {},
    }))
    .expect("manifest JSON matches the schema")
}

fn fixture_principal() -> Principal {
    serde_json::from_value(json!({
        "principal_id": PRINCIPAL,
        "extensions": {},
    }))
    .expect("principal JSON matches the schema")
}

/// Admission context: only the fixture peer maps to the fixture principal.
fn fixture_admission(adapter_peer_id: &str) -> MemoryAdmission {
    let adapter_peer_id = adapter_peer_id.to_string();
    Arc::new(move |peer_id: &str| {
        if peer_id == adapter_peer_id {
            spoke_ok(fixture_principal())
        } else {
            spoke_reject(
                SpokeRejectCode::NotAuthorized,
                format!("peer {peer_id} holds no connect fixture principal"),
                None,
            )
        }
    })
}

/// The memory face over the real Mindcore adapter. Each invoke opens the
/// caller-owned transaction the adapter requires; recall is read-only, so the
/// service commits only a ready body and leaves a rejection for rollback.
struct FixtureMemoryService {
    pool: SqlitePool,
    adapter: MemoryAdapter,
    admission: MemoryAdmission,
}

#[async_trait]
impl RemoteMemoryService for FixtureMemoryService {
    async fn invoke(&self, peer_id: &str, op: &str, payload: Value) -> SpokeResult<Value> {
        let principal = match (self.admission)(peer_id) {
            SpokeResult::Ok(principal) => principal,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        if op != "memory.recall" {
            // why: the fixture serves one meaningful Memory op; anything else is
            // outside this face's provided surface.
            return spoke_reject(
                SpokeRejectCode::CapabilityPortMissing,
                format!("memory-only connect fixture does not serve {op}"),
                None,
            );
        }
        let mut tx: Transaction<'_, Sqlite> = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(error) => {
                return spoke_reject(
                    SpokeRejectCode::InternalError,
                    format!("memory.recall begin failed: {error}"),
                    None,
                )
            }
        };
        match self.adapter.recall(&mut tx, &principal, payload).await {
            SpokeResult::Ok(MemoryStage::Ready(body)) => match tx.commit().await {
                Ok(()) => SpokeResult::Ok(body),
                Err(error) => spoke_reject(
                    SpokeRejectCode::InternalError,
                    format!("memory.recall commit failed: {error}"),
                    None,
                ),
            },
            SpokeResult::Ok(stage) => spoke_reject(
                SpokeRejectCode::InternalError,
                format!("memory.recall answered an unexpected stage: {stage:?}"),
                None,
            ),
            SpokeResult::Reject(reject) => {
                let _ = tx.rollback().await;
                SpokeResult::Reject(reject)
            }
        }
    }
}

/// 0.17 has no public empty or memory-only composite constructor; the test
/// supplies the one probe it needs and leaves every other family absent.
struct MemoryOnlyPorts {
    memory: Arc<dyn RemoteMemoryService>,
}

impl RemoteServePorts for MemoryOnlyPorts {
    fn as_memory(&self) -> Option<Arc<dyn RemoteMemoryService>> {
        Some(Arc::clone(&self.memory))
    }
}

struct MemoryOnlySession {
    adapter: Arc<RemoteAdapter>,
    responder: Arc<ConnectResponder>,
    adapter_peer_id: String,
    responder_peer_id: String,
}

/// Establish a real signed loopback session: Ed25519 throwaway identities,
/// signed hellos bound to the peer nonce, signed session snapshot, and a
/// fail-closed allowlist on both ends. Both manifests advertise
/// `["spoke-memory"]` only, and the responder attaches a memory face only.
async fn connect_memory_only_session(fixture: &Fixture) -> MemoryOnlySession {
    let pair = loopback_transport_pair();

    let adapter_seed = [0x42u8; 32];
    let responder_seed = [0xA7u8; 32];
    let adapter_pubkey = SigningKey::from_bytes(&adapter_seed)
        .verifying_key()
        .to_bytes();
    let responder_pubkey = SigningKey::from_bytes(&responder_seed)
        .verifying_key()
        .to_bytes();
    let adapter_peer_id = derive_peer_id_from_ed25519_pubkey(&adapter_pubkey);
    let responder_peer_id = derive_peer_id_from_ed25519_pubkey(&responder_pubkey);

    let service = Arc::new(FixtureMemoryService {
        pool: fixture.pool.clone(),
        adapter: MemoryAdapter::new(true),
        admission: fixture_admission(&adapter_peer_id),
    });
    let ports: Arc<dyn RemoteServePorts + Send + Sync> =
        Arc::new(MemoryOnlyPorts { memory: service });

    let responder = connect_responder(ConnectResponderOptions {
        transport: Arc::new(pair.server),
        identity: RemoteIdentity {
            seed: responder_seed,
        },
        manifest: memory_only_manifest("connect-memory-only-responder"),
        allowlist: vec![adapter_peer_id.clone()],
        peer_keys: HashMap::from([(adapter_peer_id.clone(), adapter_pubkey)]),
        ports: Some(ports),
        invoke_timeout_ms: Some(10_000),
    })
    .await;

    let adapter = connect_remote_adapter(RemoteAdapterOptions {
        transport: Arc::new(pair.client),
        local_identity: RemoteIdentity { seed: adapter_seed },
        local_manifest: memory_only_manifest("connect-memory-only-adapter"),
        remote_pubkey: responder_pubkey,
        allowlist: vec![responder_peer_id.clone()],
        invoke_timeout_ms: Some(10_000),
        capability_token: None,
    })
    .await
    .expect("signed Memory-only loopback handshake must succeed");

    MemoryOnlySession {
        adapter,
        responder,
        adapter_peer_id,
        responder_peer_id,
    }
}

impl Drop for MemoryOnlySession {
    fn drop(&mut self) {
        self.adapter.close();
        self.responder.close();
    }
}

/// Signed Memory-only negotiation: the session establishes over signed
/// hellos and both sides observe the peer manifest advertising exactly
/// `["spoke-memory"]` — no baseline, no Knowledge capability.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_only_signed_negotiation() {
    let fixture = Fixture::open().await;
    let session = connect_memory_only_session(&fixture).await;

    assert_eq!(session.adapter.state(), RemoteAdapterState::Established);
    assert!(session.adapter.session_id().is_some());

    // why: the adapter verified the responder's signature and observed its
    // authenticated peer id and signed-hello manifest.
    assert_eq!(
        session.adapter.remote_peer_id().as_deref(),
        Some(session.responder_peer_id.as_str())
    );
    let remote_manifest = session
        .adapter
        .remote_manifest()
        .expect("responder manifest present after handshake");
    assert_eq!(remote_manifest.capabilities, vec!["spoke-memory"]);
    assert!(!remote_manifest
        .capabilities
        .iter()
        .any(|capability| capability == "spoke-baseline"));

    // why: the responder's view of the dialer is symmetric: authenticated peer id
    // and `["spoke-memory"]` only.
    assert_eq!(
        session.responder.remote_peer_id().as_deref(),
        Some(session.adapter_peer_id.as_str())
    );
    let dialer_manifest = session
        .responder
        .remote_manifest()
        .expect("dialer manifest present after handshake");
    assert_eq!(dialer_manifest.capabilities, vec!["spoke-memory"]);
}

/// The negotiated memory face serves a meaningful op: `memory.recall` reaches
/// the real Mindcore adapter through the authenticated peer's fixture
/// principal and a caller-owned transaction, and returns the seeded records.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_recall_served_over_memory_only_session() {
    let fixture = Fixture::open().await;
    let session = connect_memory_only_session(&fixture).await;

    let request: MemoryRecallRequest = serde_json::from_value(json!({
        "space_id": SPACE,
        "subject_id": SUBJECT,
        "selection": { "mode": "subject_all" },
        "budget": { "max_items": 10 },
    }))
    .expect("recall request JSON matches the schema");

    let response = match MemoryPort::recall(&*session.adapter, &fixture_principal(), request).await
    {
        SpokeResult::Ok(response) => response,
        SpokeResult::Reject(reject) => {
            panic!("memory.recall must be served over the memory-only face: {reject:?}")
        }
    };
    let body = serde_json::to_value(&response).expect("recall response serializes");

    assert_eq!(
        body["total_authorized"], 2,
        "recall admits exactly the seeded records for the fixture principal"
    );
    assert_eq!(body["truncated"], false);
    assert_eq!(
        body["records"]
            .as_array()
            .expect("records array")
            .iter()
            .map(|record| record["record_id"].as_str().expect("record id"))
            .collect::<Vec<_>>(),
        vec!["rec_a", "rec_b"],
        "the memory face served the seeded fixture records in rank order"
    );
    assert_eq!(body["records"][0]["content"]["text"], "alpha memory");
}

/// The baseline face is genuinely absent: a real baseline op without
/// negotiated `spoke-baseline` is denied by the dispatch gate with raw
/// `op_unsupported`, which the client maps to typed `CAPABILITY_PORT_MISSING`
/// preserving `details.wire_code`. No baseline handler can run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn baseline_only_op_denied_without_spoke_baseline() {
    let fixture = Fixture::open().await;
    let session = connect_memory_only_session(&fixture).await;
    let scope: Scope =
        serde_json::from_value(json!({ "scope_id": "connect-fixture-scope" })).expect("scope");

    let result = ScopeQueryPort::list_timeline_events(&*session.adapter, &scope).await;
    let SpokeResult::Reject(reject) = result else {
        panic!("port.scope.list_timeline_events must be refused without spoke-baseline");
    };
    assert_eq!(reject.code, SpokeRejectCode::CapabilityPortMissing);
    let details = reject.details.expect("dispatch deny carries details");
    assert_eq!(
        details.get("wire_code").and_then(Value::as_str),
        Some("op_unsupported"),
        "pinned raw reject code for the unnegotiated baseline port op"
    );
    assert!(
        reject.message.contains("port.scope.list_timeline_events"),
        "reject message names the refused op: {}",
        reject.message
    );
}
