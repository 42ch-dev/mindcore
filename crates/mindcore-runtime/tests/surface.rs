//! Fixture wiring for the serving surface: the manifest follows the composed faces and the tool
//! registrations, the un-routed faces are absent rather than stubbed, and the ports compose no
//! baseline provider.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use tempfile::TempDir;

use mindcore_runtime::tools::{ABSORB, DELIBERATE, READ_COGNITION, RECALL_CONTEXT};
use mindcore_runtime::{MindProviders, MindcoreServe, ServeFaces, ToolRegistration};
use mindcore_spoke::{ServedMemory, TRANSFER_OPERATIONS};
use mindcore_sqlite::MindStore;
use spoke_connect::core::CAPABILITY_SPOKE_JUDGMENT;
use spoke_connect::remote::{
    connect_responder, loopback_transport_pair, ConnectResponderOptions, MemoryAdmission,
    RemoteIdentity, RemoteMemoryService, ToolHandler,
};
use spoke_operations::{
    spoke_ok, spoke_reject, validate_manifest_tools, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::host_capability_manifest::ToolDescriptor;
use spoke_schemas::HostCapabilityManifest;

const HOST_TOOL: &str = "tools.mindcore.host_tool";
/// The retired product adoption id the runtime drops from every host registration.
const RETIRED_ADOPT: &str = "tools.mindcore.adopt";

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

/// The host's peer-to-principal admission: the owner is admitted, a stranger is not.
fn principals() -> MemoryAdmission {
    Arc::new(|peer_id: &str| match peer_id {
        "peer-owner" => SpokeResult::Ok(
            serde_json::from_value(json!({ "principal_id": "prn_owner", "extensions": {} }))
                .expect("principal JSON matches the schema"),
        ),
        _ => spoke_reject(
            SpokeRejectCode::NotAuthorized,
            format!("peer {peer_id} holds no workflow admission"),
            None,
        ),
    })
}

/// The memory service is host-supplied; this fixture only proves the face is attached. It takes
/// the trait's default and reports no operation.
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

impl ServedMemory for RecordingMemory {}

/// Four of the five transfer operations: a partial family must advertise nothing.
struct FourOfFive;

static FOUR_OF_FIVE: [&str; 4] = [
    TRANSFER_OPERATIONS[0],
    TRANSFER_OPERATIONS[1],
    TRANSFER_OPERATIONS[2],
    TRANSFER_OPERATIONS[3],
];

#[async_trait]
impl RemoteMemoryService for FourOfFive {
    async fn invoke(&self, peer_id: &str, op: &str, payload: Value) -> SpokeResult<Value> {
        RecordingMemory.invoke(peer_id, op, payload).await
    }
}

impl ServedMemory for FourOfFive {
    fn operations(&self) -> &'static [&'static str] {
        &FOUR_OF_FIVE
    }
}

fn faces(manifest: HostCapabilityManifest) -> ServeFaces {
    ServeFaces {
        manifest,
        memory: None,
        knowledge: false,
        history: false,
        principals: principals(),
        providers: MindProviders::default(),
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

/// The three Mind tools and their judgment family are the only served capabilities: no knowledge,
/// history or adoption face is composed, so none is advertised and none is served.
#[tokio::test]
async fn manifest_follows_the_composed_faces() {
    let serve = Fixture::open().await.serve();
    let surface = expect_ok(
        serve.surface(faces(metadata("runtime"))),
        "assemble surface",
    );

    assert_eq!(
        capabilities(&surface),
        [
            CAPABILITY_SPOKE_JUDGMENT,
            RECALL_CONTEXT,
            ABSORB,
            DELIBERATE,
            READ_COGNITION
        ]
    );
    let tools: Vec<&str> = surface
        .manifest
        .tools
        .iter()
        .map(|tool| tool.capability_id.as_str())
        .collect();
    assert_eq!(tools, [RECALL_CONTEXT, ABSORB, DELIBERATE, READ_COGNITION]);
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

    assert!(
        surface.ports.as_baseline().is_none(),
        "no baseline provider is composed"
    );
    assert!(
        surface.ports.as_knowledge().is_none(),
        "no peer-only knowledge face is composed"
    );
    assert!(
        surface.ports.as_scoped_knowledge().is_none(),
        "no scoped knowledge face is composed: the responder answers its ops as a missing port"
    );
    assert!(
        surface.ports.as_history().is_none(),
        "no history face is composed"
    );
    assert!(
        surface.ports.as_adoption().is_none(),
        "no adoption face is composed"
    );
    assert!(
        surface.ports.as_memory().is_none(),
        "no memory service was attached"
    );
}

/// Every reserved id is the runtime's: a host registration borrowing one of the four served
/// tool ids, or the retired adoption id, is dropped, so it appears in neither `capabilities`
/// nor `tools` and the runtime's own registrations stand.
#[tokio::test]
async fn a_host_registration_cannot_shadow_a_runtime_tool() {
    let serve = Fixture::open().await.serve();
    let surface = expect_ok(
        serve.surface(ServeFaces {
            tools: vec![
                shadowing(RECALL_CONTEXT),
                shadowing(ABSORB),
                shadowing(DELIBERATE),
                shadowing(READ_COGNITION),
                shadowing(RETIRED_ADOPT),
            ],
            ..faces(metadata("runtime"))
        }),
        "assemble a surface a host tries to shadow",
    );

    assert_eq!(
        capabilities(&surface),
        [
            CAPABILITY_SPOKE_JUDGMENT,
            RECALL_CONTEXT,
            ABSORB,
            DELIBERATE,
            READ_COGNITION
        ],
        "a host registration supplied a reserved id: {:?}",
        capabilities(&surface)
    );
    let tools: Vec<&str> = surface
        .manifest
        .tools
        .iter()
        .map(|tool| tool.capability_id.as_str())
        .collect();
    assert_eq!(
        tools,
        [RECALL_CONTEXT, ABSORB, DELIBERATE, READ_COGNITION],
        "each runtime tool is advertised once, from the runtime's own registration"
    );
}

/// A host registration borrowing a reserved tool id: its descriptor would be advertised and its
/// handler installed if the runtime did not drop it.
fn shadowing(capability_id: &str) -> ToolRegistration {
    ToolRegistration::new(descriptor(capability_id), |_responder| {
        let handler: ToolHandler = Arc::new(|arguments: Value| {
            Box::pin(
                async move { spoke_ok(json!({ "adopted_by": "host", "arguments": arguments })) },
            )
        });
        handler
    })
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
        [
            "spoke-memory",
            CAPABILITY_SPOKE_JUDGMENT,
            RECALL_CONTEXT,
            ABSORB,
            DELIBERATE,
            READ_COGNITION
        ]
    );
    assert!(
        surface.ports.as_memory().is_some(),
        "the host's memory service is served"
    );
    assert!(surface.ports.as_knowledge().is_none());
    assert!(surface.ports.as_baseline().is_none());
}

/// A bare `RemoteMemoryService` takes the trait's default and reports no operation, so it earns
/// `spoke-memory` and never `spoke-memory-transfer`.
#[tokio::test]
async fn a_bare_memory_service_advertises_only_the_memory_family() {
    let serve = Fixture::open().await.serve();
    let surface = expect_ok(
        serve.surface(ServeFaces {
            memory: Some(Arc::new(RecordingMemory)),
            ..faces(metadata("runtime"))
        }),
        "assemble a memory-serving surface",
    );
    assert!(capabilities(&surface).contains(&"spoke-memory"));
    assert!(
        !capabilities(&surface).contains(&"spoke-memory-transfer"),
        "a service that reports no operation cannot prove the family: {:?}",
        capabilities(&surface)
    );
}

/// Four of the five operations is not the family: the manifest claims neither transfer nor a
/// partial family.
#[tokio::test]
async fn a_service_reporting_four_of_five_advertises_no_transfer_family() {
    let serve = Fixture::open().await.serve();
    let surface = expect_ok(
        serve.surface(ServeFaces {
            memory: Some(Arc::new(FourOfFive)),
            ..faces(metadata("runtime"))
        }),
        "assemble a partially transfer-serving surface",
    );
    assert!(capabilities(&surface).contains(&"spoke-memory"));
    assert!(
        !capabilities(&surface).contains(&"spoke-memory-transfer"),
        "a partial family is not the family: {:?}",
        capabilities(&surface)
    );
    assert!(
        !capabilities(&surface)
            .iter()
            .any(|capability| capability.starts_with("spoke-memory-transfer")),
        "no partial family capability exists to advertise: {:?}",
        capabilities(&surface)
    );
}

/// A registration advertises its descriptor and is installed on the responder. Whether the
/// installed handler answers is proven over a signed session by the loopback task.
#[tokio::test]
async fn tool_registrations_drive_advertisement_and_installation() {
    let serve = Fixture::open().await.serve();
    let registration = ToolRegistration::new(descriptor(HOST_TOOL), |_responder| {
        let handler: ToolHandler =
            Arc::new(|arguments: Value| Box::pin(async move { spoke_ok(arguments) }));
        handler
    });
    let surface = expect_ok(
        serve.surface(ServeFaces {
            tools: vec![registration],
            ..faces(metadata("runtime"))
        }),
        "assemble tool-serving surface",
    );

    assert_eq!(
        capabilities(&surface),
        [
            CAPABILITY_SPOKE_JUDGMENT,
            RECALL_CONTEXT,
            ABSORB,
            DELIBERATE,
            READ_COGNITION,
            HOST_TOOL
        ]
    );
    assert_eq!(surface.manifest.tools.len(), 5);
    assert_eq!(surface.manifest.tools[4].capability_id.as_str(), HOST_TOOL);
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
    responder.close();
}

/// The knowledge face is one composition decision: `ServeFaces.knowledge` true advertises
/// `spoke-knowledge` and `spoke-knowledge-scoped` together and answers `as_scoped_knowledge()`
/// with the face; false advertises neither and answers nothing.
///
/// `ke-ownership` is advertised in neither case. Every admitted scope carries `viewpoint: None`,
/// so the face cannot honour the flag; with it never offered, no session can negotiate it, and
/// Connect's ownership conjunct refuses a viewpoint request with wire `op_unsupported` before
/// admission. Claiming the flag would advertise a capability the face cannot honour.
#[tokio::test]
async fn the_knowledge_face_is_advertised_exactly_when_it_is_composed() {
    let serve = Fixture::open().await.serve();
    let composed = expect_ok(
        serve.surface(ServeFaces {
            knowledge: true,
            ..faces(metadata("runtime"))
        }),
        "assemble a knowledge-serving surface",
    );
    for capability in ["spoke-knowledge", "spoke-knowledge-scoped"] {
        assert!(
            capabilities(&composed).contains(&capability),
            "the composed face advertises {capability}: {:?}",
            capabilities(&composed)
        );
    }
    assert!(
        !capabilities(&composed).contains(&"ke-ownership"),
        "the composed face does not advertise a flag it cannot honour: {:?}",
        capabilities(&composed)
    );
    assert!(
        composed.ports.as_scoped_knowledge().is_some(),
        "the composed face answers the scoped probe"
    );
    assert!(
        composed.ports.as_knowledge().is_none(),
        "the request-scoped face is the one composed, never the peer-only fallback beside it"
    );
    expect_ok(
        validate_manifest_tools(&composed.manifest),
        "the derived manifest is self-consistent",
    );

    let absent = expect_ok(
        serve.surface(faces(metadata("runtime"))),
        "assemble a surface without the knowledge face",
    );
    for capability in ["spoke-knowledge", "spoke-knowledge-scoped", "ke-ownership"] {
        assert!(
            !capabilities(&absent).contains(&capability),
            "the absent face advertises nothing: {capability} in {:?}",
            capabilities(&absent)
        );
    }
    assert!(
        absent.ports.as_scoped_knowledge().is_none(),
        "the absent face answers no scoped probe"
    );
}

/// The history face rides on the knowledge face's admission: `ServeFaces.history` true advertises
/// `spoke-history` and answers `as_history()` with the face, false advertises neither, and
/// `history` without `knowledge` is refused at assembly, before any port is built.
#[tokio::test]
async fn the_history_face_is_advertised_exactly_when_it_is_composed() {
    let serve = Fixture::open().await.serve();
    let composed = expect_ok(
        serve.surface(ServeFaces {
            knowledge: true,
            history: true,
            ..faces(metadata("runtime"))
        }),
        "assemble a history-serving surface",
    );
    assert!(
        capabilities(&composed).contains(&"spoke-history"),
        "the composed face advertises the family: {:?}",
        capabilities(&composed)
    );
    assert!(
        composed.ports.as_history().is_some(),
        "the composed face answers the history probe"
    );
    expect_ok(
        validate_manifest_tools(&composed.manifest),
        "the derived manifest is self-consistent",
    );

    let absent = expect_ok(
        serve.surface(ServeFaces {
            knowledge: true,
            history: false,
            ..faces(metadata("runtime"))
        }),
        "assemble a knowledge-only surface",
    );
    assert!(
        !capabilities(&absent).contains(&"spoke-history"),
        "the absent face advertises nothing: {:?}",
        capabilities(&absent)
    );
    assert!(
        absent.ports.as_history().is_none(),
        "the absent face answers no history probe"
    );

    let refused = serve.surface(ServeFaces {
        history: true,
        ..faces(metadata("runtime"))
    });
    match refused {
        SpokeResult::Reject(reject) => assert_eq!(
            reject.code,
            SpokeRejectCode::InvalidInput,
            "history without knowledge is an assembly refusal"
        ),
        SpokeResult::Ok(_) => panic!("history without the knowledge face must not assemble"),
    }
}
