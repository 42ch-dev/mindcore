//! Task 1 of `v02-connect-bound`: Memory-only Connect composition bound.
//!
//! Pinned `spoke-connect =0.16.1` (`remote-adapter` feature) over an in-process
//! `loopback_transport_pair`. Both peers advertise exactly `["spoke-memory"]`
//! in their signed hellos; the responder runs with `ports: None` — the only
//! public configuration, because `RemoteServePorts: BaselinePorts` and
//! `RemoteServePortsComposite::new` requires `Arc<dyn BaselinePorts>`, so no
//! public API supplies `RemoteMemoryService` without a baseline provider.
//!
//! The result is the bounded negative branch: signed negotiation proves
//! `spoke-memory` is the negotiated set, `port.knowledge.get` answers the
//! pinned baseline refusal (`op_unsupported` → `CAPABILITY_PORT_MISSING`),
//! and `memory.describe` reaches the service probe and answers
//! `op_unsupported` terminal because no memory service can be attached.

use std::collections::HashMap;
use std::sync::Arc;

use ed25519_dalek::SigningKey;
use serde_json::{json, Value};
use spoke_connect::core::derive_peer_id_from_ed25519_pubkey;
use spoke_connect::remote::{
    connect_remote_adapter, connect_responder, loopback_transport_pair, ConnectResponder,
    ConnectResponderOptions, RemoteAdapter, RemoteAdapterOptions, RemoteAdapterState,
    RemoteIdentity,
};
use spoke_operations::{KnowledgeEntryPort, MemoryPort, SpokeRejectCode, SpokeResult};
use spoke_schemas::{HostCapabilityManifest, MemoryDescribeRequest, Principal};

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
        "principal_id": "connect-fixture-principal",
        "extensions": {},
    }))
    .expect("principal JSON matches the schema")
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
/// `["spoke-memory"]` only.
async fn connect_memory_only_session() -> MemoryOnlySession {
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

    let responder = connect_responder(ConnectResponderOptions {
        transport: Arc::new(pair.server),
        identity: RemoteIdentity {
            seed: responder_seed,
        },
        manifest: memory_only_manifest("connect-memory-only-responder"),
        allowlist: vec![adapter_peer_id.clone()],
        peer_keys: HashMap::from([(adapter_peer_id.clone(), adapter_pubkey)]),
        // Blocker reproduction in one line: `ports` is the only attachment
        // point and accepts `Arc<dyn RemoteServePorts>`, which has a
        // `BaselinePorts` supertrait — `None` negotiates but cannot serve.
        ports: None,
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
    let session = connect_memory_only_session().await;

    assert_eq!(session.adapter.state(), RemoteAdapterState::Established);
    assert!(session.adapter.session_id().is_some());

    // The adapter verified the responder's signature and observed its
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

    // The responder's view of the dialer is symmetric: authenticated peer id
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

/// Pinned baseline refusal: `port.knowledge.get` without negotiated
/// `spoke-baseline` is denied by the serve gate with raw `op_unsupported`;
/// the client maps the dispatch-deny to typed `CAPABILITY_PORT_MISSING`
/// preserving `details.wire_code`. No baseline handler can run (ports is
/// `None`), so `spoke-baseline` is not implied by the session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn baseline_refusal_without_spoke_baseline() {
    let session = connect_memory_only_session().await;

    let result = KnowledgeEntryPort::get_knowledge_entry(&*session.adapter, "entry-1").await;
    let SpokeResult::Reject(reject) = result else {
        panic!("port.knowledge.get must be refused without spoke-baseline");
    };
    assert_eq!(reject.code, SpokeRejectCode::CapabilityPortMissing);
    let details = reject.details.expect("dispatch deny carries details");
    assert_eq!(
        details.get("wire_code").and_then(Value::as_str),
        Some("op_unsupported"),
        "pinned raw reject code for the unnegotiated baseline port op"
    );
    assert!(
        reject.message.contains("port.knowledge.get"),
        "reject message names the refused op: {}",
        reject.message
    );
}

/// Composition bound: `spoke-memory` negotiated, `memory.describe` passes
/// the capability gate (otherwise the wire code would be
/// `capability_missing`), then the service probe denies with
/// `op_unsupported` + `retry_class = terminal` because no
/// `RemoteMemoryService` can be attached without `BaselinePorts`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_describe_denied_without_servable_memory() {
    let session = connect_memory_only_session().await;

    let result = MemoryPort::describe(
        &*session.adapter,
        &fixture_principal(),
        MemoryDescribeRequest::default(),
    )
    .await;
    let SpokeResult::Reject(reject) = result else {
        panic!("memory.describe must not succeed without a memory service");
    };
    assert_eq!(reject.code, SpokeRejectCode::CapabilityPortMissing);
    let details = reject.details.expect("dispatch deny carries details");
    // `op_unsupported` (not `capability_missing`) proves the negotiated set
    // contained spoke-memory: the gate admitted the op and the service
    // probe denied it.
    assert_eq!(
        details.get("wire_code").and_then(Value::as_str),
        Some("op_unsupported"),
        "negotiated spoke-memory admits the op; the missing service denies it"
    );
    assert_eq!(
        details.get("retry_class").and_then(Value::as_str),
        Some("terminal")
    );
    assert!(
        reject.message.contains("no memory service"),
        "deny message names the missing service: {}",
        reject.message
    );
}
