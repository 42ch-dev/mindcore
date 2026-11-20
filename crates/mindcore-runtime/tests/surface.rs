//! Fixture wiring for the serving surface: the manifest follows the composed faces and the tool
//! registrations, the knowledge admission is validated where the face is assembled, and the ports
//! compose no baseline provider.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use tempfile::TempDir;

use mindcore_base::knowledge::KnowledgeScope;
use mindcore_runtime::{MindcoreServe, ServeFaces, ToolRegistration};
use mindcore_sqlite::MindStore;
use spoke_connect::remote::{
    connect_responder, loopback_transport_pair, ConnectResponderOptions, KnowledgeAdmission,
    RemoteIdentity, RemoteMemoryService, ToolHandler,
};
use spoke_operations::{
    spoke_ok, spoke_reject, validate_manifest_tools, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::host_capability_manifest::ToolDescriptor;
use spoke_schemas::{HostCapabilityManifest, Scope};

const SCOPE_ID: &str = "spoke-scope-001";
const COLLECTION: &str = "wren";
const RECALL_CONTEXT: &str = "tools.mindcore.recall_context";

struct Fixture {
    _dir: TempDir,
    store: MindStore,
}

impl Fixture {
    async fn open() -> Self {
        let dir = TempDir::new().expect("temporary directory");
        let store = MindStore::open(&dir.path().join("runtime_serve.db"))
            .await
            .expect("open store");
        Self { _dir: dir, store }
    }

    fn serve(self) -> MindcoreServe {
        let published = Arc::new(Mutex::new(Vec::new()));
        MindcoreServe::new(self.store, false, move |targets| {
            published
                .lock()
                .expect("publish lock")
                .push(targets.to_vec());
            Ok(())
        })
    }
}

/// Host hello metadata. The capability claim and the missing tool namespace are deliberate: the
/// runtime must replace them with what it actually serves.
fn metadata(host_id: &str) -> HostCapabilityManifest {
    serde_json::from_value(json!({
        "schema_version": 1,
        "host_id": host_id,
        "roles": ["data-store"],
        "capabilities": ["spoke-baseline"],
        "namespaces": ["toy_world"],
        "extensions": {},
    }))
    .expect("manifest JSON matches the schema")
}

fn scope_of(scope_id: &str) -> Scope {
    serde_json::from_value(json!({ "scope_id": scope_id })).expect("scope JSON matches the schema")
}

/// One selection the knowledge face does not serve.
fn unsupported_scope() -> Scope {
    serde_json::from_value(json!({
        "scope_id": SCOPE_ID,
        "cognition": { "view": "recorded_at", "filter": {} },
    }))
    .expect("scope JSON matches the schema")
}

/// The host's admission: one admitted peer, one peer whose admitted scope carries a selection the
/// face does not serve, and a stranger.
fn admission() -> KnowledgeAdmission {
    Arc::new(|peer_id: &str| match peer_id {
        "peer-owner" => SpokeResult::Ok(scope_of(SCOPE_ID)),
        "peer-cognition" => SpokeResult::Ok(unsupported_scope()),
        _ => spoke_reject(
            SpokeRejectCode::NotAuthorized,
            format!("peer {peer_id} holds no knowledge admission"),
            None,
        ),
    })
}

/// The memory service is host-supplied; this fixture only proves the face is attached.
struct RecordingMemory;

#[async_trait]
impl RemoteMemoryService for RecordingMemory {
    async fn invoke(&self, _peer_id: &str, _op: &str, _payload: Value) -> SpokeResult<Value> {
        spoke_reject(
            SpokeRejectCode::InternalError,
            "the wiring fixture serves no memory op",
            None,
        )
    }
}

fn faces(manifest: HostCapabilityManifest) -> ServeFaces {
    ServeFaces {
        manifest,
        scope_id: SCOPE_ID.to_string(),
        knowledge: KnowledgeScope::new(COLLECTION),
        admission: admission(),
        memory: None,
        tools: Vec::new(),
    }
}

fn descriptor(capability_id: &str) -> ToolDescriptor {
    serde_json::from_value(json!({
        "schema_version": 1,
        "capability_id": capability_id,
        "op": capability_id,
        "description": "Recall a context pack",
        "input": { "type": "object" },
        "output": { "type": "object" },
    }))
    .expect("tool descriptor JSON matches the schema")
}

fn capabilities(surface: &mindcore_runtime::MindcoreSurface) -> Vec<&str> {
    surface
        .manifest
        .capabilities
        .iter()
        .map(|capability| capability.as_str())
        .collect()
}

fn expect_ok<T>(result: SpokeResult<T>, context: &str) -> T {
    match result {
        SpokeResult::Ok(value) => value,
        SpokeResult::Reject(reject) => panic!("{context}: unexpectedly rejected with {reject:?}"),
    }
}

/// The knowledge face is the only composed face, so it is the only family advertised.
#[tokio::test]
async fn manifest_follows_the_composed_faces() {
    let serve = Fixture::open().await.serve();
    let surface = expect_ok(
        serve.surface(faces(metadata("runtime"))),
        "assemble surface",
    );

    assert_eq!(capabilities(&surface), ["spoke-knowledge", "ke-ownership"]);
    assert!(
        surface.manifest.tools.is_empty(),
        "no tool is registered, so none is advertised"
    );
    assert!(
        surface
            .manifest
            .namespaces
            .iter()
            .all(|namespace| namespace.as_str() != "mindcore"),
        "the tool namespace is advertised only when a tool is served"
    );
    expect_ok(
        validate_manifest_tools(&surface.manifest),
        "derived manifest is self-consistent",
    );

    assert!(
        surface.ports.as_baseline().is_none(),
        "no baseline provider is composed"
    );
    assert!(
        surface.ports.as_knowledge().is_some(),
        "the knowledge face is composed"
    );
    assert!(
        surface.ports.as_memory().is_none(),
        "no memory service was attached"
    );
}

/// Attaching the host's memory service adds its capability and its face, still without a baseline.
#[tokio::test]
async fn attached_memory_service_is_advertised_and_served() {
    let serve = Fixture::open().await.serve();
    let surface = expect_ok(
        serve.surface(ServeFaces {
            memory: Some(Arc::new(RecordingMemory)),
            ..faces(metadata("runtime"))
        }),
        "assemble mixed surface",
    );

    assert_eq!(
        capabilities(&surface),
        ["spoke-knowledge", "ke-ownership", "spoke-memory"]
    );
    assert!(
        surface.ports.as_memory().is_some(),
        "the host's memory service is served"
    );
    assert!(surface.ports.as_knowledge().is_some());
    assert!(surface.ports.as_baseline().is_none());
}

/// A registration advertises its descriptor and installs its handler on the responder.
#[tokio::test]
async fn tool_registrations_drive_advertisement_and_installation() {
    let serve = Fixture::open().await.serve();
    let installed = Arc::new(AtomicUsize::new(0));
    let registration = {
        let installed = Arc::clone(&installed);
        ToolRegistration::new(descriptor(RECALL_CONTEXT), move |_responder| {
            installed.fetch_add(1, Ordering::SeqCst);
            let handler: ToolHandler =
                Arc::new(|arguments: Value| Box::pin(async move { spoke_ok(arguments) }));
            handler
        })
    };
    let surface = expect_ok(
        serve.surface(ServeFaces {
            tools: vec![registration],
            ..faces(metadata("runtime"))
        }),
        "assemble tool-serving surface",
    );

    assert_eq!(
        capabilities(&surface),
        ["spoke-knowledge", "ke-ownership", RECALL_CONTEXT]
    );
    assert_eq!(surface.manifest.tools.len(), 1);
    assert_eq!(
        surface.manifest.tools[0].capability_id.as_str(),
        RECALL_CONTEXT
    );
    assert!(
        surface
            .manifest
            .namespaces
            .iter()
            .any(|namespace| namespace.as_str() == "mindcore"),
        "the runtime owns the tool namespace it advertises"
    );
    expect_ok(
        validate_manifest_tools(&surface.manifest),
        "derived manifest is self-consistent",
    );

    let pair = loopback_transport_pair();
    let responder = connect_responder(ConnectResponderOptions {
        transport: Arc::new(pair.server),
        identity: RemoteIdentity { seed: [0xA7u8; 32] },
        manifest: surface.manifest.clone(),
        allowlist: Vec::new(),
        peer_keys: HashMap::new(),
        ports: Some(Arc::clone(&surface.ports)),
        invoke_timeout_ms: Some(10_000),
    })
    .await;
    expect_ok(surface.register(&responder), "install handlers");
    assert_eq!(
        installed.load(Ordering::SeqCst),
        1,
        "one handler is built and installed per registration"
    );
    responder.close();
}

/// The runtime validates the scope the host admits, before Connect merges any request selection.
///
/// An unsupported host admission is not the peer's input, so it leaves the face as the same
/// generic `NotAuthorized` a rejecting resolver returns; a caller-declared unsupported selection
/// stays `InvalidInput` at the routed store.
#[tokio::test]
async fn admission_is_validated_where_the_face_is_assembled() {
    let serve = Fixture::open().await.serve();
    let surface = expect_ok(
        serve.surface(faces(metadata("runtime"))),
        "assemble surface",
    );
    let face = surface
        .ports
        .as_knowledge()
        .expect("knowledge face composed");

    let admitted = expect_ok((face.admission)("peer-owner"), "admitted peer");
    assert_eq!(admitted.scope_id, SCOPE_ID);

    let refused = match (face.admission)("peer-stranger") {
        SpokeResult::Reject(reject) => reject,
        SpokeResult::Ok(scope) => panic!("a stranger was admitted with {scope:?}"),
    };
    assert_eq!(refused.code, SpokeRejectCode::NotAuthorized);

    let unsupported = match (face.admission)("peer-cognition") {
        SpokeResult::Reject(reject) => reject,
        SpokeResult::Ok(scope) => panic!("an unsupported selection reached Connect with {scope:?}"),
    };
    assert_eq!(
        unsupported.code,
        SpokeRejectCode::NotAuthorized,
        "a host-admitted unsupported selection is a deny, not the caller's input error"
    );
    assert!(
        unsupported.details.is_none(),
        "host admission discloses no target details: {unsupported:?}"
    );

    let declared = match face
        .store
        .list_scoped_knowledge_entries(&unsupported_scope())
        .await
    {
        SpokeResult::Reject(reject) => reject,
        SpokeResult::Ok(entries) => panic!("a caller-declared selection listed {entries:?}"),
    };
    assert_eq!(
        declared.code,
        SpokeRejectCode::InvalidInput,
        "a caller-declared unsupported selection stays the caller's input error"
    );
}
