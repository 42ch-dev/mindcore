//! Fixture wiring for the serving surface: the manifest follows the composed faces and the tool
//! registrations, the knowledge admission is validated where the face is assembled, and the ports
//! compose no baseline provider.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use tempfile::TempDir;

use mindcore_base::knowledge::KnowledgeScope;
use mindcore_runtime::tools::{ABSORB, DELIBERATE, RECALL_CONTEXT};
use mindcore_runtime::{AdoptionRoute, MindProviders, MindcoreServe, ServeFaces, ToolRegistration};
use mindcore_spoke::{ServedMemory, TRANSFER_OPERATIONS};
use mindcore_sqlite::{MindStore, ScopedKnowledgeStore};
use spoke_connect::core::{
    CAPABILITY_SPOKE_HISTORY, CAPABILITY_SPOKE_JUDGMENT, CAPABILITY_SPOKE_KNOWLEDGE_ADOPTION,
};
use spoke_connect::remote::{
    connect_responder, loopback_transport_pair, ConnectResponderOptions, KnowledgeAdmission,
    MemoryAdmission, RemoteIdentity, RemoteMemoryService, ToolHandler, KNOWLEDGE_OP_ADOPT,
};
use spoke_operations::{
    spoke_ok, spoke_reject, validate_manifest_tools, KnowledgeEntryPort, SpokeReject,
    SpokeRejectCode, SpokeResult,
};
use spoke_schemas::host_capability_manifest::ToolDescriptor;
use spoke_schemas::{HostCapabilityManifest, Scope};

const SCOPE_ID: &str = "spoke-scope-001";
const COLLECTION: &str = "wren";
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
/// face does not serve, one peer the host admits under a scope id this surface does not serve, and
/// a stranger.
fn admission() -> KnowledgeAdmission {
    Arc::new(|peer_id: &str| match peer_id {
        "peer-owner" => SpokeResult::Ok(scope_of(SCOPE_ID)),
        "peer-cognition" => SpokeResult::Ok(unsupported_scope()),
        "peer-elsewhere" => SpokeResult::Ok(scope_of("spoke-scope-other")),
        _ => spoke_reject(
            SpokeRejectCode::NotAuthorized,
            format!("peer {peer_id} holds no knowledge admission"),
            None,
        ),
    })
}

/// The host's peer-to-principal admission: the driver is admitted, and the mismatch peer is
/// admitted as the same principal so only the admitted scope id can refuse it. A stranger is not.
fn principals() -> MemoryAdmission {
    Arc::new(|peer_id: &str| match peer_id {
        "peer-owner" | "peer-elsewhere" => SpokeResult::Ok(
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

/// The one shared source collection an adopting surface reads from.
fn adoption_source() -> AdoptionRoute {
    AdoptionRoute {
        scope_id: "spc_shared".to_string(),
        source: KnowledgeScope::new("shared"),
    }
}

/// The host's route map: its one admitted principal's workflow collection.
fn routes() -> BTreeMap<String, KnowledgeScope> {
    BTreeMap::from([("prn_owner".to_string(), KnowledgeScope::new(COLLECTION))])
}

fn faces(manifest: HostCapabilityManifest) -> ServeFaces {
    ServeFaces {
        manifest,
        scope_id: SCOPE_ID.to_string(),
        knowledge: KnowledgeScope::new(COLLECTION),
        admission: admission(),
        memory: None,
        history: false,
        principals: principals(),
        providers: MindProviders::default(),
        adoption: None,
        workflow_routes: routes(),
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

fn expect_reject<T: std::fmt::Debug>(result: SpokeResult<T>, context: &str) -> SpokeReject {
    match result {
        SpokeResult::Reject(reject) => reject,
        SpokeResult::Ok(value) => panic!("{context}: unexpectedly accepted with {value:?}"),
    }
}

/// The knowledge family and the three Mind tools are the only served capabilities.
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
            "spoke-knowledge",
            "ke-ownership",
            CAPABILITY_SPOKE_JUDGMENT,
            RECALL_CONTEXT,
            ABSORB,
            DELIBERATE
        ]
    );
    assert!(
        capabilities(&surface).contains(&CAPABILITY_SPOKE_JUDGMENT),
        "the wire map registers `deliberate`, so its judgment family is advertised: {:?}",
        capabilities(&surface)
    );
    let tools: Vec<&str> = surface
        .manifest
        .tools
        .iter()
        .map(|tool| tool.capability_id.as_str())
        .collect();
    assert_eq!(tools, [RECALL_CONTEXT, ABSORB, DELIBERATE]);
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
        surface.ports.as_knowledge().is_some(),
        "the knowledge face is composed"
    );
    assert!(
        surface.ports.as_memory().is_none(),
        "no memory service was attached"
    );
    assert!(
        surface.ports.as_history().is_none(),
        "the history face is off, so no history service is composed"
    );

    let served = expect_ok(
        serve.surface(ServeFaces {
            history: true,
            ..faces(metadata("runtime"))
        }),
        "assemble a history-serving surface",
    );
    assert_eq!(
        capabilities(&served),
        [
            "spoke-knowledge",
            "ke-ownership",
            CAPABILITY_SPOKE_HISTORY,
            CAPABILITY_SPOKE_JUDGMENT,
            RECALL_CONTEXT,
            ABSORB,
            DELIBERATE
        ],
        "the history face is advertised exactly when it is served"
    );
    assert!(
        served.ports.as_history().is_some(),
        "the history face is composed over the routed store"
    );
    expect_ok(
        validate_manifest_tools(&served.manifest),
        "the history-serving manifest is self-consistent",
    );

    assert!(
        !capabilities(&served).contains(&CAPABILITY_SPOKE_KNOWLEDGE_ADOPTION),
        "no adoption source is configured, so the adoption face is not advertised"
    );
    let adopting = expect_ok(
        serve.surface(ServeFaces {
            adoption: Some(adoption_source()),
            ..faces(metadata("runtime"))
        }),
        "assemble an adopting surface",
    );
    assert_eq!(
        capabilities(&adopting),
        [
            "spoke-knowledge",
            "ke-ownership",
            CAPABILITY_SPOKE_KNOWLEDGE_ADOPTION,
            CAPABILITY_SPOKE_JUDGMENT,
            RECALL_CONTEXT,
            ABSORB,
            DELIBERATE
        ],
        "the adoption face is advertised exactly when it is served"
    );
    assert!(
        adopting.ports.as_adoption().is_some(),
        "the adoption face is composed over the routed store"
    );
    expect_ok(
        validate_manifest_tools(&adopting.manifest),
        "the adopting manifest is self-consistent",
    );
}

/// The host's route map is validated once, where the surface is assembled: an empty principal key,
/// an empty destination collection, or one destination collection claimed by two routes is the
/// host's input error, refused before any port or registration exists.
#[tokio::test]
async fn an_unroutable_destination_is_refused_at_assembly() {
    let serve = Fixture::open().await.serve();
    for (routes, message) in [
        (
            BTreeMap::from([(String::new(), KnowledgeScope::new(COLLECTION))]),
            "a knowledge route needs a non-empty principal id",
        ),
        (
            BTreeMap::from([("prn_owner".to_string(), KnowledgeScope::new(""))]),
            "the knowledge route for principal prn_owner names an empty collection",
        ),
        (
            BTreeMap::from([
                ("prn_owner".to_string(), KnowledgeScope::new(COLLECTION)),
                ("prn_other".to_string(), KnowledgeScope::new(COLLECTION)),
            ]),
            "a destination collection is routed to more than one principal",
        ),
    ] {
        let reject = match serve.surface(ServeFaces {
            workflow_routes: routes,
            ..faces(metadata("runtime"))
        }) {
            SpokeResult::Reject(reject) => reject,
            SpokeResult::Ok(surface) => {
                panic!(
                    "an unroutable destination assembled {:?}",
                    capabilities(&surface)
                )
            }
        };
        assert_eq!(reject.code, SpokeRejectCode::InvalidInput);
        assert_eq!(reject.message, message);
        assert!(
            reject.details.is_none(),
            "the assembly refusal carries no route details: {reject:?}"
        );
    }
}

/// The adoption source is one collection for the whole surface. Beside the per-principal routes it
/// assembles, and the source need not hold any row.
#[tokio::test]
async fn an_adoption_source_beside_the_routes_assembles() {
    let serve = Fixture::open().await.serve();
    let surface = expect_ok(
        serve.surface(ServeFaces {
            adoption: Some(AdoptionRoute {
                scope_id: "spc_shared".to_string(),
                source: KnowledgeScope::new("shared"),
            }),
            ..faces(metadata("runtime"))
        }),
        "assemble an adopting surface",
    );

    assert!(surface.ports.as_knowledge().is_some());
}

/// The adoption source must differ from every route destination, connected or not: a source that
/// is also a principal's destination would let one principal's write become another's source.
#[tokio::test]
async fn an_adoption_source_matching_a_route_destination_is_refused_at_assembly() {
    let serve = Fixture::open().await.serve();
    // An unconnected principal's route is a refusal too: the copy it would write is one no
    // knowledge-face read of the sharing principal reaches.
    let reject = match serve.surface(ServeFaces {
        workflow_routes: BTreeMap::from([
            ("prn_owner".to_string(), KnowledgeScope::new(COLLECTION)),
            ("prn_unconnected".to_string(), KnowledgeScope::new("shared")),
        ]),
        adoption: Some(AdoptionRoute {
            scope_id: "spc_shared".to_string(),
            source: KnowledgeScope::new("shared"),
        }),
        ..faces(metadata("runtime"))
    }) {
        SpokeResult::Reject(reject) => reject,
        SpokeResult::Ok(surface) => panic!(
            "a route destination became the adoption source {:?}",
            capabilities(&surface)
        ),
    };
    assert_eq!(reject.code, SpokeRejectCode::InvalidInput);
    assert!(
        reject.details.is_none(),
        "the assembly refusal carries no route details: {reject:?}"
    );
}

/// A source naming the collection the knowledge face serves would copy the destination into
/// itself, so assembly refuses it even with no route map at all.
#[tokio::test]
async fn an_adoption_source_in_the_knowledge_faces_collection_is_refused() {
    let serve = Fixture::open().await.serve();
    let reject = match serve.surface(ServeFaces {
        workflow_routes: BTreeMap::new(),
        adoption: Some(AdoptionRoute {
            scope_id: "spc_shared".to_string(),
            source: KnowledgeScope::new(COLLECTION),
        }),
        ..faces(metadata("runtime"))
    }) {
        SpokeResult::Reject(reject) => reject,
        SpokeResult::Ok(surface) => panic!(
            "the served collection became the adoption source {:?}",
            capabilities(&surface)
        ),
    };
    assert_eq!(reject.code, SpokeRejectCode::InvalidInput);
    assert_eq!(
        reject.message,
        "the adoption source collection is the collection the knowledge face serves"
    );
    assert!(reject.details.is_none(), "{reject:?}");
}

/// A source scope equal to the destination scope would let a principal adopt from its own
/// destination, so assembly refuses it.
#[tokio::test]
async fn an_adoption_source_equal_to_the_destination_scope_is_refused() {
    let serve = Fixture::open().await.serve();
    let reject = match serve.surface(ServeFaces {
        adoption: Some(AdoptionRoute {
            scope_id: SCOPE_ID.to_string(),
            source: KnowledgeScope::new("shared"),
        }),
        ..faces(metadata("runtime"))
    }) {
        SpokeResult::Reject(reject) => reject,
        SpokeResult::Ok(surface) => {
            panic!(
                "a source scope equal to the destination assembled {:?}",
                capabilities(&surface)
            )
        }
    };
    assert_eq!(reject.code, SpokeRejectCode::InvalidInput);
    assert_eq!(
        reject.message,
        "the adoption source scope is the destination scope"
    );
    assert!(reject.details.is_none(), "{reject:?}");
}

/// An empty source collection or an empty source scope id names no source, so assembly refuses it.
#[tokio::test]
async fn an_empty_adoption_source_is_refused_at_assembly() {
    let serve = Fixture::open().await.serve();
    for route in [
        AdoptionRoute {
            scope_id: "spc_shared".to_string(),
            source: KnowledgeScope::new(""),
        },
        AdoptionRoute {
            scope_id: String::new(),
            source: KnowledgeScope::new("shared"),
        },
    ] {
        let reject = match serve.surface(ServeFaces {
            adoption: Some(route),
            ..faces(metadata("runtime"))
        }) {
            SpokeResult::Reject(reject) => reject,
            SpokeResult::Ok(surface) => {
                panic!(
                    "an empty adoption source assembled {:?}",
                    capabilities(&surface)
                )
            }
        };
        assert_eq!(reject.code, SpokeRejectCode::InvalidInput);
        assert!(reject.details.is_none(), "{reject:?}");
    }
}

/// The retired adoption id is the runtime's: a host registration of `tools.mindcore.adopt` is
/// dropped, so it appears in neither `capabilities` nor `tools` — with or without a configured
/// adoption source, which serves nothing until the protocol face arrives.
#[tokio::test]
async fn a_host_registration_cannot_shadow_the_adoption_tool() {
    let serve = Fixture::open().await.serve();

    let adopted = || {
        Some(AdoptionRoute {
            scope_id: "spc_shared".to_string(),
            source: KnowledgeScope::new("shared"),
        })
    };
    for (context, adoption) in [
        ("without a configured adoption source", None),
        ("with a configured adoption source", adopted()),
    ] {
        let surface = expect_ok(
            serve.surface(ServeFaces {
                adoption,
                tools: vec![shadowing_adoption()],
                ..faces(metadata("runtime"))
            }),
            "assemble a surface a host tries to shadow",
        );

        assert!(
            !capabilities(&surface).contains(&RETIRED_ADOPT),
            "a host registration supplied the retired id {context}: {:?}",
            capabilities(&surface)
        );
        assert!(
            surface
                .manifest
                .tools
                .iter()
                .all(|tool| tool.capability_id.as_str() != RETIRED_ADOPT),
            "the host's retired adoption descriptor is not advertised {context}"
        );
    }
}

/// A host registration borrowing the retired adoption id: its descriptor would be advertised and
/// its handler installed if the runtime did not drop it.
fn shadowing_adoption() -> ToolRegistration {
    let descriptor: ToolDescriptor = serde_json::from_value(json!({
        "schema_version": 1,
        "capability_id": RETIRED_ADOPT,
        "op": RETIRED_ADOPT,
        "description": "A host-supplied adoption tool.",
        "input": { "type": "object" },
        "output": { "type": "object" },
    }))
    .expect("tool descriptor JSON matches the schema");
    ToolRegistration::new(descriptor, |_responder| {
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
            "spoke-knowledge",
            "ke-ownership",
            "spoke-memory",
            CAPABILITY_SPOKE_JUDGMENT,
            RECALL_CONTEXT,
            ABSORB,
            DELIBERATE
        ]
    );
    assert!(
        surface.ports.as_memory().is_some(),
        "the host's memory service is served"
    );
    assert!(surface.ports.as_knowledge().is_some());
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
            "spoke-knowledge",
            "ke-ownership",
            CAPABILITY_SPOKE_JUDGMENT,
            RECALL_CONTEXT,
            ABSORB,
            DELIBERATE,
            HOST_TOOL
        ]
    );
    assert_eq!(surface.manifest.tools.len(), 4);
    assert_eq!(surface.manifest.tools[3].capability_id.as_str(), HOST_TOOL);
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

/// The admitted scope id must be the one this surface serves, not merely a well-formed selection:
/// a resolver that admits another id is refused on both faces. Without the id check, an adoption
/// whose destination names that foreign id matches the admitted context and writes into the
/// knowledge face's collection, while the knowledge face denies the same peer.
#[tokio::test]
async fn an_admitted_scope_id_outside_the_served_one_is_refused() {
    let serve = Fixture::open().await.serve();
    let surface = expect_ok(
        serve.surface(ServeFaces {
            adoption: Some(adoption_source()),
            ..faces(metadata("runtime"))
        }),
        "assemble an adopting surface",
    );
    let face = surface
        .ports
        .as_knowledge()
        .expect("knowledge face composed");

    let refused = match (face.admission)("peer-elsewhere") {
        SpokeResult::Reject(reject) => reject,
        SpokeResult::Ok(scope) => panic!("a foreign admitted id reached Connect with {scope:?}"),
    };
    assert_eq!(refused.code, SpokeRejectCode::NotAuthorized);
    assert_eq!(refused.message, "knowledge scope not authorized");
    assert!(refused.details.is_none(), "{refused:?}");

    let adoption = surface.ports.as_adoption().expect("adoption face composed");
    let denied = expect_reject(
        adoption
            .invoke(
                "peer-elsewhere",
                KNOWLEDGE_OP_ADOPT,
                json!({
                    "source_scope_id": "spc_shared",
                    "source_entry_id": "ke_source",
                    "source_revision": 1,
                    "destination_scope_id": "spoke-scope-other",
                    "idempotency_key": "mismatch",
                }),
            )
            .await,
        "adoption under a foreign admitted id",
    );
    assert_eq!(
        denied.code,
        SpokeRejectCode::NotAuthorized,
        "the adoption face refuses what the knowledge face denies"
    );
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

/// A second admitted principal and the shared source's seed rows: an entry the adoption copies,
/// and the knowledge face's own collection.
fn two_principal_faces(manifest: HostCapabilityManifest) -> ServeFaces {
    ServeFaces {
        adoption: Some(adoption_source()),
        admission: Arc::new(|peer_id: &str| match peer_id {
            "peer-owner" => SpokeResult::Ok(scope_of(SCOPE_ID)),
            // Admitted with knowledge access but routed to no workflow collection at all.
            "peer-second" => SpokeResult::Ok(scope_of(SCOPE_ID)),
            _ => spoke_reject(
                SpokeRejectCode::NotAuthorized,
                format!("peer {peer_id} holds no knowledge admission"),
                None,
            ),
        }),
        principals: Arc::new(|peer_id: &str| match peer_id {
            "peer-owner" | "peer-second" => SpokeResult::Ok(
                serde_json::from_value(json!({ "principal_id": peer_id, "extensions": {} }))
                    .expect("principal JSON matches the schema"),
            ),
            _ => spoke_reject(
                SpokeRejectCode::NotAuthorized,
                format!("peer {peer_id} holds no workflow admission"),
                None,
            ),
        }),
        // One routed principal and one admitted principal without any workflow route.
        workflow_routes: BTreeMap::from([(
            "peer-owner".to_string(),
            KnowledgeScope::new(COLLECTION),
        )]),
        ..faces(manifest)
    }
}

fn adopt_payload(entry_id: &str, key: &str) -> Value {
    json!({
        "source_scope_id": "spc_shared",
        "source_entry_id": entry_id,
        "source_revision": 1,
        "destination_scope_id": SCOPE_ID,
        "idempotency_key": key,
    })
}

/// An adopted copy lands in the collection `knowledge.get` serves for the same admitted peer: two
/// principals adopt into that one collection, both copies read back through the knowledge face, and
/// a principal with knowledge admission but no workflow route adopts too. Without this, a copy could
/// be written into a collection no knowledge-face read ever reaches.
#[tokio::test]
async fn an_adopted_copy_is_readable_through_the_knowledge_face_by_its_own_principal() {
    let fixture = Fixture::open().await;
    let store = ScopedKnowledgeStore::new(fixture.store.pool(), KnowledgeScope::new("shared"));
    for entry_id in ["ke_owner", "ke_second"] {
        expect_ok(
            store
                .put_knowledge_entry(
                    serde_json::from_value(json!({
                        "schema_version": 1,
                        "entry_id": entry_id,
                        "entry_type": "note",
                        "canonical_name": format!("{entry_id} source"),
                        "status": "confirmed",
                        "body": { "summary": format!("{entry_id} summary") },
                        "extensions": {},
                    }))
                    .expect("entry JSON matches the schema"),
                    None,
                )
                .await,
            "seed the shared source",
        );
    }
    let surface = expect_ok(
        fixture
            .serve()
            .surface(two_principal_faces(metadata("runtime"))),
        "assemble a two-principal adopting surface",
    );
    let adoption = surface.ports.as_adoption().expect("adoption face composed");
    let face = surface
        .ports
        .as_knowledge()
        .expect("knowledge face composed");
    let admitted = scope_of(SCOPE_ID);

    for (peer, entry_id) in [("peer-owner", "ke_owner"), ("peer-second", "ke_second")] {
        let adopted = expect_ok(
            adoption
                .invoke(peer, KNOWLEDGE_OP_ADOPT, adopt_payload(entry_id, peer))
                .await,
            "adopt",
        );
        assert_eq!(adopted["outcome"], "adopted", "{adopted}");

        let read = expect_ok(
            face.store
                .get_scoped_knowledge_entry(&admitted, entry_id)
                .await,
            "read the copy back through the knowledge face",
        );
        assert_eq!(read.entry_id, entry_id, "{read:?}");
        assert_eq!(
            read.adoption_anchor
                .as_ref()
                .map(|anchor| anchor.source_scope_id.as_str()),
            Some("spc_shared"),
            "the knowledge face serves the adopted copy: {read:?}"
        );
    }
}
