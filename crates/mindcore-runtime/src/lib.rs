//! The Mindcore serving surface: the workflow engine, the SQLite knowledge face and the host's
//! memory face assembled into the pieces a Connect responder is built from.
//!
//! The crate opens nothing: no listener, accept loop, identity or transport. The host supplies
//! those plus its admissions, publisher and memory service, then builds
//! `ConnectResponderOptions` from [`MindcoreSurface`]. Advertisements follow what is served:
//! [`MindcoreServe::surface`] derives `capabilities` and `tools` from the composed faces and the
//! registrations, never from a host-supplied capability string.

pub mod tools;

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::Value;

use mindcore_base::knowledge::KnowledgeScope;
use mindcore_spi::{EmbeddingIdentity, MindWorkflows, SpiError, SpiMindWorkflows};
use mindcore_spoke::MemoryWorkflowBinding;
use mindcore_sqlite::{validate_admission_scope, vectors, MindStore};
use spoke_connect::core::{
    CAPABILITY_KE_OWNERSHIP, CAPABILITY_SPOKE_KNOWLEDGE, CAPABILITY_SPOKE_MEMORY,
    CAPABILITY_SPOKE_MEMORY_TRANSFER,
};
use spoke_connect::remote::{
    ConnectResponder, KnowledgeAdmission, MemoryAdmission, RemoteKnowledgeFace,
    RemoteMemoryService, RemoteServePorts, RemoteServePortsComposite, ToolHandler,
};
use spoke_operations::{
    spoke_reject, validate_manifest_tools, SpokeReject, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::host_capability_manifest::ToolDescriptor;
use spoke_schemas::{HostCapabilityManifest, Scope};
use sqlx::SqlitePool;

pub use tools::MindProviders;
use tools::WorkflowTools;

/// The `tools.*` namespace this runtime owns.
const TOOLS_NAMESPACE: &str = "mindcore";

/// The Mindcore serving side over one host store.
///
/// The workflow engine runs on the same pool the knowledge face serves, so a tool call and a
/// knowledge op see one snapshot boundary. The engine never opens a transaction across provider
/// work; each workflow keeps its own `SpiError` classification.
pub struct MindcoreServe {
    store: MindStore,
    reflections: bool,
    publish: Arc<JournalPublisher>,
}

/// The host's journal publisher: a settlement leg without one is refused by the binding, and the
/// runtime never invents a publisher.
type JournalPublisher = dyn Fn(&[Value]) -> Result<(), SpiError> + Send + Sync;

impl MindcoreServe {
    /// Build over the host's store.
    ///
    /// `reflections_enabled` matches the memory adapter instance the host serves. `publish` is the
    /// host's journal publisher: a settlement leg without one is refused by the binding, and the
    /// runtime never invents a publisher.
    pub fn new(
        store: MindStore,
        reflections_enabled: bool,
        publish: impl Fn(&[Value]) -> Result<(), SpiError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            store,
            reflections: reflections_enabled,
            publish: Arc::new(publish),
        }
    }

    /// The engine one surface serves: each workflow call resolves the collection it reaches from
    /// the principal its admission returns, through the host's route map.
    fn workflows(
        &self,
        routes: BTreeMap<String, KnowledgeScope>,
    ) -> Result<Arc<SpiMindWorkflows>, SpokeReject> {
        let publish = Arc::clone(&self.publish);
        let binding = MemoryWorkflowBinding::new(self.store.pool(), self.reflections)
            .with_publisher(move |targets| publish(targets))
            .with_knowledge_routes(routes)?;
        Ok(Arc::new(SpiMindWorkflows::new(binding)))
    }

    /// Assemble the ports, manifest and tool registrations for `ConnectResponderOptions`.
    ///
    /// Nothing is served until the host creates its responder and calls
    /// [`MindcoreSurface::register`]: `connect_responder` starts its own receive task and never
    /// installs handlers from the manifest.
    pub fn surface(&self, faces: ServeFaces) -> SpokeResult<MindcoreSurface> {
        // why: the route map's emptiness decides the deliberation advertisement; read it before the
        // map moves into the binding.
        let routed = !faces.workflow_routes.is_empty();
        let workflows = match self.workflows(faces.workflow_routes) {
            Ok(workflows) => workflows,
            // why: the host's own configuration is refused before any port or registration exists.
            Err(reject) => return SpokeResult::Reject(reject),
        };
        let engine: Arc<dyn MindWorkflows> = workflows.clone();
        let mut tools =
            WorkflowTools::new(engine, faces.principals, faces.providers).registrations();
        tools.extend(faces.tools);
        // invariant: deliberation settles onto a routed carrier collection; a surface without a route serves
        // no deliberation. The invariant holds over the final set, so a host registration cannot
        // restore the tool or its handler.
        if !routed {
            tools.retain(|tool| tool.descriptor.capability_id.as_str() != tools::DELIBERATE);
        }
        let manifest = derive_manifest(faces.manifest, faces.memory.as_ref(), &tools);
        let manifest = match manifest {
            SpokeResult::Ok(manifest) => manifest,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        let face = RemoteKnowledgeFace {
            store: Arc::new(self.store.served_knowledge(faces.scope_id, faces.knowledge)),
            admission: validated(faces.admission),
        };
        let mut ports = RemoteServePortsComposite::knowledge_only(Arc::new(face));
        if let Some(memory) = faces.memory {
            ports = ports.with_memory(Arc::clone(&memory.service));
        }
        SpokeResult::Ok(MindcoreSurface {
            ports: Arc::new(ports),
            manifest,
            tools,
            workflows,
            pool: self.store.pool(),
        })
    }
}

/// The five §7.29–§7.33 transfer operations. A memory face that registers every one of them
/// dispatches the whole family; anything less is a base-only face and must not claim it.
pub const MEMORY_TRANSFER_OPERATIONS: [&str; 5] = [
    "memory.exports.create",
    "memory.exports.get",
    "memory.imports.stage",
    "memory.imports.activate",
    "memory.imports.discard",
];

/// One installed memory face: the responder's service and the `memory.*` operations that service
/// dispatches.
///
/// Advertisement is derived from this registration, never from a host-supplied capability string.
/// The face earns `spoke-memory` by being installed; it earns `spoke-memory-transfer` only when
/// every one of [`MEMORY_TRANSFER_OPERATIONS`] is registered, so an opaque base-only service —
/// one that answers the transfer ops with `CAPABILITY_PORT_MISSING` — cannot claim the family.
pub struct MemoryFace {
    service: Arc<dyn RemoteMemoryService>,
    operations: Vec<&'static str>,
}

impl MemoryFace {
    /// A face over `service`, which dispatches exactly `operations`. Register
    /// [`MEMORY_TRANSFER_OPERATIONS`] when it serves the transfer family.
    pub fn new(
        service: Arc<dyn RemoteMemoryService>,
        operations: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        Self {
            service,
            operations: operations.into_iter().collect(),
        }
    }

    /// Whether this registration covers the whole transfer family.
    fn serves_transfer(&self) -> bool {
        MEMORY_TRANSFER_OPERATIONS
            .iter()
            .all(|op| self.operations.contains(op))
    }
}

/// The host's side of the surface: non-capability manifest metadata and the face wiring the
/// runtime cannot invent.
pub struct ServeFaces {
    /// The responder's hello metadata. `capabilities` and `tools` are replaced with what the
    /// runtime actually serves; every other member stays the host's.
    pub manifest: HostCapabilityManifest,
    /// The one configured route: an admitted `scope_id` reads and writes `knowledge`'s collection.
    pub scope_id: String,
    /// The collection that route serves.
    pub knowledge: KnowledgeScope,
    /// Maps the authenticated peer to the one scope it may read and write; never read from the wire.
    pub admission: KnowledgeAdmission,
    /// The host's memory family service, with the operations it registers. `None` composes no
    /// memory face and advertises none.
    pub memory: Option<MemoryFace>,
    /// Maps the authenticated peer to the `Principal` the workflows admit. The same host mapping
    /// the memory face uses; never read from the wire.
    pub principals: MemoryAdmission,
    /// Providers and the domain lens injected into each workflow call.
    pub providers: MindProviders,
    /// The host's per-principal workflow routes: each exact principal id — not a peer id — to the
    /// one workflow collection its calls may reach. The map is the workflow collection allowlist, so
    /// a route need not name a collection that already holds rows; each destination collection
    /// belongs to exactly one principal route, and assembly refuses a collection named by two
    /// routes. A route names the principal `principals` admits; a principal without one reads no
    /// knowledge and never writes a carrier. Empty serves no deliberation and advertises none.
    /// Routes are never read from the wire.
    pub workflow_routes: BTreeMap<String, KnowledgeScope>,
    /// Additional host tools: every descriptor is advertised and its handler installed, except the
    /// reserved `tools.mindcore.deliberate` id on a surface with an empty route map.
    pub tools: Vec<ToolRegistration>,
}

/// Builds one tool's handler from the responder it serves.
type ToolInstall = dyn Fn(&Arc<ConnectResponder>) -> ToolHandler + Send + Sync;

/// One served tool: the descriptor the manifest advertises and the handler built for the responder
/// that serves it.
///
/// `ToolHandler` receives only JSON arguments, so a handler that needs the authenticated peer holds
/// the responder it serves — weakly: a handler must never extend its session's lifetime, and a
/// handler bound to one session must not be installed on another.
pub struct ToolRegistration {
    descriptor: ToolDescriptor,
    install: Box<ToolInstall>,
}

impl ToolRegistration {
    pub fn new(
        descriptor: ToolDescriptor,
        install: impl Fn(&Arc<ConnectResponder>) -> ToolHandler + Send + Sync + 'static,
    ) -> Self {
        Self {
            descriptor,
            install: Box::new(install),
        }
    }
}

/// The assembled surface: what `ConnectResponderOptions` takes, and the handler installation that
/// must run before inbound hello traffic reaches the responder.
pub struct MindcoreSurface {
    /// Serve these ports: the knowledge face over the shared store, plus the memory face when the
    /// host attached a service. No baseline provider is composed.
    pub ports: Arc<dyn RemoteServePorts + Send + Sync>,
    /// Advertise this manifest: it lists exactly the capabilities `ports` and the registrations
    /// serve.
    pub manifest: HostCapabilityManifest,
    tools: Vec<ToolRegistration>,
    /// The engine behind the tools: its query slot is the in-memory half of identity retirement.
    workflows: Arc<SpiMindWorkflows>,
    /// The pool the vectors live on: the disk half of identity retirement.
    pool: SqlitePool,
}

impl MindcoreSurface {
    /// Retire one serving identity: clear its in-memory query slot, then delete its stored
    /// partition.
    ///
    /// This is the one supported host entry, so rotating identity never means remembering both
    /// halves. Returns how many stored vectors were deleted — zero for an identity with no rows —
    /// and another identity keeps its rows and its query slot. An identity with no dimension is
    /// refused as `InvalidInput`.
    ///
    /// Call it between recalls: the surface does not drain a recall already in flight for
    /// `identity`, and a fill that commits afterwards re-creates rows this call deleted.
    pub async fn retire_embedding_identity(
        &self,
        identity: &EmbeddingIdentity,
    ) -> SpokeResult<u64> {
        self.workflows.retire_identity(identity);
        let hash = match vectors::identity_hash(
            &identity.provider_id,
            &identity.model_id,
            &identity.model_version,
            identity.dim,
        ) {
            SpokeResult::Ok(hash) => hash,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        vectors::delete_identity_vectors(&self.pool, &hash).await
    }

    /// Install every served tool handler on `responder`.
    ///
    /// Run this before the host lets inbound hello traffic reach the responder: a dialer may invoke
    /// an advertised tool as soon as the session is established, and the responder does not read
    /// handlers from its manifest.
    pub fn register(&self, responder: &Arc<ConnectResponder>) -> SpokeResult<()> {
        for tool in &self.tools {
            let capability_id = tool.descriptor.capability_id.to_string();
            match responder.register_tool_handler(&capability_id, (tool.install)(responder)) {
                SpokeResult::Ok(()) => {}
                SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
            }
        }
        SpokeResult::Ok(())
    }
}

/// The advertised capabilities follow the composed faces: the knowledge face always, the memory
/// face only when its service is attached, and each memory family flag from the operations that
/// face registered.
///
/// The knowledge face applies SPOKE's owner-private disclosure, so a viewpoint-scoped list is
/// served and the supplemental ownership capability is truthful; it never stands in for peer or
/// entry admission.
fn face_capabilities(memory: Option<&MemoryFace>) -> Vec<String> {
    let mut capabilities = vec![
        CAPABILITY_SPOKE_KNOWLEDGE.to_string(),
        CAPABILITY_KE_OWNERSHIP.to_string(),
    ];
    if let Some(face) = memory {
        capabilities.push(CAPABILITY_SPOKE_MEMORY.to_string());
        if face.serves_transfer() {
            capabilities.push(CAPABILITY_SPOKE_MEMORY_TRANSFER.to_string());
        }
    }
    capabilities
}

/// Replace the host's capability claims with the served faces and registrations, then check the
/// manifest the responder will sign.
fn derive_manifest(
    mut manifest: HostCapabilityManifest,
    memory: Option<&MemoryFace>,
    tools: &[ToolRegistration],
) -> SpokeResult<HostCapabilityManifest> {
    manifest.capabilities = face_capabilities(memory);
    for tool in tools {
        manifest
            .capabilities
            .push(tool.descriptor.capability_id.to_string());
    }
    manifest.tools = tools.iter().map(|tool| tool.descriptor.clone()).collect();
    if !tools.is_empty()
        && !manifest
            .namespaces
            .iter()
            .any(|namespace| namespace.as_str() == TOOLS_NAMESPACE)
    {
        manifest.namespaces.push(
            TOOLS_NAMESPACE
                .parse()
                .expect("the tool namespace matches the manifest namespace pattern"),
        );
    }
    match validate_manifest_tools(&manifest) {
        SpokeResult::Ok(()) => SpokeResult::Ok(manifest),
        SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
    }
}

/// The admitted scope is validated where the face is assembled: Connect's list merge keeps only
/// the request's cognition selection, so a selection the face does not serve must be refused
/// before the merge can drop it.
///
/// Host admission is not peer input, so an unsupported admitted selection is refused as the same
/// generic `NotAuthorized` a rejecting resolver returns, never as the caller-declared
/// `InvalidInput` the routed store keeps for request scopes: the peer learns nothing about what
/// the face does not serve. The resolver's own refusal passes through unchanged.
fn validated(admission: KnowledgeAdmission) -> KnowledgeAdmission {
    Arc::new(move |peer_id: &str| -> SpokeResult<Scope> {
        let scope = match admission(peer_id) {
            SpokeResult::Ok(scope) => scope,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        match validate_admission_scope(&scope) {
            SpokeResult::Ok(()) => SpokeResult::Ok(scope),
            SpokeResult::Reject(_) => spoke_reject(
                SpokeRejectCode::NotAuthorized,
                "knowledge scope not authorized",
                None,
            ),
        }
    })
}
