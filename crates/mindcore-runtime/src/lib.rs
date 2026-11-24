//! The Mindcore serving surface: the workflow engine, the SQLite knowledge face and the host's
//! memory face assembled into the pieces a Connect responder is built from.
//!
//! The crate opens nothing: no listener, accept loop, identity or transport. The host supplies
//! those plus its admissions, publisher and memory service, then builds
//! `ConnectResponderOptions` from [`MindcoreSurface`]. Advertisements follow what is served:
//! [`MindcoreServe::surface`] derives `capabilities` and `tools` from the composed faces and the
//! registrations, never from a host-supplied capability string.

mod adoption;
mod history_dedup;
pub mod tools;

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::Value;

use mindcore_base::knowledge::KnowledgeScope;
use mindcore_spi::{EmbeddingIdentity, MindWorkflows, SpiError, SpiMindWorkflows};
use mindcore_spoke::{MemoryWorkflowBinding, ServedMemory, TRANSFER_OPERATIONS};
use mindcore_sqlite::{validate_admission_scope, vectors, MindStore, RoutedKnowledgeStore};
use spoke_connect::core::{
    CAPABILITY_KE_OWNERSHIP, CAPABILITY_SPOKE_HISTORY, CAPABILITY_SPOKE_JUDGMENT,
    CAPABILITY_SPOKE_KNOWLEDGE, CAPABILITY_SPOKE_KNOWLEDGE_ADOPTION, CAPABILITY_SPOKE_MEMORY,
    CAPABILITY_SPOKE_MEMORY_TRANSFER,
};
use spoke_connect::remote::{
    AdoptionAdmission, AdoptionPortService, ConnectResponder, HistoryAdmission, HistoryPortService,
    KnowledgeAdmission, MemoryAdmission, RemoteAdoptionService, RemoteHistoryService,
    RemoteKnowledgeFace, RemoteMemoryService, RemoteServePorts, RemoteServePortsComposite,
    ToolHandler,
};
use spoke_operations::{
    spoke_reject, validate_manifest_tools, AdoptionPort, HistoryPort, ScopedKnowledgePort,
    SpokeReject, SpokeRejectCode, SpokeResult,
};
use spoke_schemas::host_capability_manifest::ToolDescriptor;
use spoke_schemas::{HostCapabilityManifest, Scope};
use sqlx::SqlitePool;

use adoption::AdoptionService;
pub use tools::MindProviders;
use tools::WorkflowTools;

/// The `tools.*` namespace this runtime owns.
const TOOLS_NAMESPACE: &str = "mindcore";

/// The id of the retired product adoption tool.
///
/// `knowledge.adopt` replaces it: the id is dropped from every host registration, so no host can
/// advertise or install anything under it.
const RETIRED_ADOPT_TOOL: &str = "tools.mindcore.adopt";

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
    /// `reflections` matches the memory adapter instance the host serves. `publish` is the
    /// host's journal publisher: a settlement leg without one is refused by the binding, and the
    /// runtime never invents a publisher.
    pub fn new(
        store: MindStore,
        reflections: bool,
        publish: impl Fn(&[Value]) -> Result<(), SpiError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            store,
            reflections,
            publish: Arc::new(publish),
        }
    }

    /// The engine one surface serves: each workflow call resolves the collection it reaches from
    /// the principal its admission returns, through the host's route map.
    fn workflows(
        &self,
        routes: Arc<BTreeMap<String, KnowledgeScope>>,
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
        let adoption = faces.adoption;
        if let Some(adoption) = &adoption {
            let source = &adoption.source.collection;
            if adoption.scope_id.is_empty() {
                return spoke_reject(
                    SpokeRejectCode::InvalidInput,
                    "the adoption source names an empty scope id",
                    None,
                );
            }
            if source.is_empty() {
                return spoke_reject(
                    SpokeRejectCode::InvalidInput,
                    "the adoption source names an empty collection",
                    None,
                );
            }
            if adoption.scope_id == faces.scope_id {
                return spoke_reject(
                    SpokeRejectCode::InvalidInput,
                    "the adoption source scope is the destination scope",
                    None,
                );
            }
            if source == &faces.knowledge.collection {
                return spoke_reject(
                    SpokeRejectCode::InvalidInput,
                    "the adoption source collection is the collection the knowledge face serves",
                    None,
                );
            }
            if let Some((principal, _)) = faces
                .workflow_routes
                .iter()
                .find(|(_, route)| route.collection == *source)
            {
                return spoke_reject(
                    SpokeRejectCode::InvalidInput,
                    format!(
                        "the adoption source collection {source} is the workflow destination of principal {principal}"
                    ),
                    None,
                );
            }
        }
        let routes = Arc::new(faces.workflow_routes);
        let routed = !routes.is_empty();
        let workflows = match self.workflows(Arc::clone(&routes)) {
            Ok(workflows) => workflows,
            Err(reject) => return SpokeResult::Reject(reject),
        };
        let engine: Arc<dyn MindWorkflows> = workflows.clone();
        let principals = faces.principals;
        let mut tools =
            WorkflowTools::new(engine, Arc::clone(&principals), faces.providers).registrations();
        tools.extend(
            faces
                .tools
                .into_iter()
                .filter(|tool| tool.descriptor.capability_id.as_str() != RETIRED_ADOPT_TOOL),
        );
        if !routed {
            tools.retain(|tool| tool.descriptor.capability_id.as_str() != tools::DELIBERATE);
        }
        let manifest = derive_manifest(
            faces.manifest,
            faces.memory.as_deref(),
            faces.history,
            adoption.is_some(),
            &tools,
        );
        let manifest = match manifest {
            SpokeResult::Ok(manifest) => manifest,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        let admission = validated(faces.admission, &faces.scope_id);
        let served: KnowledgeScope = faces.knowledge;
        let store: Arc<RoutedKnowledgeStore> =
            Arc::new(self.store.served_knowledge(faces.scope_id, served.clone()));
        let knowledge: Arc<dyn ScopedKnowledgePort + Send + Sync> = store.clone();
        let face = RemoteKnowledgeFace {
            store: knowledge,
            admission: Arc::clone(&admission),
        };
        let mut ports = RemoteServePortsComposite::knowledge_only(Arc::new(face));
        if let Some(adoption) = adoption {
            // Adoption reuses the knowledge face's admission and its one served collection, so a
            // peer that does not serve knowledge cannot adopt, and every adopted copy is readable
            // through the knowledge face that shares this admission.
            let adoption_admission: AdoptionAdmission = {
                let principals = Arc::clone(&principals);
                let admission = Arc::clone(&admission);
                Arc::new(move |peer_id: &str| {
                    let principal = match principals(peer_id) {
                        SpokeResult::Ok(principal) => principal,
                        SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
                    };
                    match admission(peer_id) {
                        SpokeResult::Ok(scope) => SpokeResult::Ok((principal, scope)),
                        SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
                    }
                })
            };
            let port: Arc<dyn AdoptionPort + Send + Sync> =
                Arc::new(AdoptionService::new(self.store.pool(), served, adoption));
            let service: Arc<dyn RemoteAdoptionService> =
                Arc::new(AdoptionPortService::new(port, adoption_admission));
            ports = ports.with_adoption(service);
        }
        if faces.history {
            // History serves the knowledge face's own route and admission: one store, two port
            // roles, so a peer replays exactly the scope it reads and writes.
            let history_admission: HistoryAdmission = Arc::new(move |peer_id: &str| {
                let principal = match principals(peer_id) {
                    SpokeResult::Ok(principal) => principal,
                    SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
                };
                match admission(peer_id) {
                    SpokeResult::Ok(scope) => SpokeResult::Ok((principal, scope)),
                    SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
                }
            });
            let port: Arc<dyn HistoryPort + Send + Sync> = store;
            let service: Arc<dyn RemoteHistoryService> =
                Arc::new(HistoryPortService::new(port, history_admission));
            ports = ports.with_history(service);
        }
        if let Some(memory) = faces.memory {
            let service: Arc<dyn RemoteMemoryService> = memory;
            ports = ports.with_memory(service);
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

/// The shared collection adoption reads from: one source for the whole surface. The destination is
/// the one collection the knowledge face serves, shared by every peer admitted to that scope: it
/// holds one slot per `destination_entry_id`, the first adoption of a given id claims it, a repeat is
/// served that stored copy as `already_present` only at the anchor revision the copy records, and
/// every peer the host admits to the scope reads it through the knowledge face when its own admitted
/// selection admits it. Assembly refuses a source that names a route destination or the
/// knowledge face's own collection, and a source scope equal to the destination scope, so a
/// principal's write can never become the source another reads.
pub struct AdoptionRoute {
    /// The host-owned id naming this source on the wire, recorded in every adopted copy's anchor
    /// triple. Callers cannot supply it: this baseline forwards only the configured value.
    pub scope_id: String,
    pub source: KnowledgeScope,
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
    /// The host's memory family service. `None` composes no memory face and advertises none.
    ///
    /// Installing the service proves the `spoke-memory` family. It earns
    /// `spoke-memory-transfer` only by reporting all five §7.29–§7.33 operations through
    /// [`ServedMemory::operations`]: a signed manifest must not claim a family the bound service
    /// cannot show it dispatches. A bare `RemoteMemoryService` takes the trait's default and
    /// reports none.
    pub memory: Option<Arc<dyn ServedMemory>>,
    /// Serve the `spoke-history` face over the same routed store and knowledge admission as the
    /// knowledge face; `false` composes no history service and advertises none. A peer replays and
    /// prunes exactly the scope it reads and writes through `port.knowledge.*`.
    pub history: bool,
    /// Maps the authenticated peer to the `Principal` the workflows admit. The same host mapping
    /// the memory face uses; never read from the wire.
    pub principals: MemoryAdmission,
    /// Providers and the domain lens injected into each workflow call.
    pub providers: MindProviders,
    /// The one host-configured adoption source, or `None` to serve no adoption. Its collection must
    /// be non-empty and must not be [`ServeFaces::knowledge`]'s collection or any
    /// [`ServeFaces::workflow_routes`] destination, including a route of a principal that is not
    /// connected, and its scope id must differ from [`ServeFaces::scope_id`]; assembly refuses
    /// otherwise. The source need not hold rows. When it is configured, the
    /// `spoke-knowledge-adoption` face serves `knowledge.adopt` over the knowledge face's admission,
    /// and writes into [`ServeFaces::knowledge`]'s collection — one shared collection with one slot
    /// per `destination_entry_id`: the first adoption of a given id claims it, a repeat is answered
    /// `already_present` with the stored copy only at the anchor revision the copy records (another
    /// revision is a conflict), and a peer whose own admitted selection hides the copy is answered
    /// the occupancy refusal instead of the copy.
    pub adoption: Option<AdoptionRoute>,
    /// The host's per-principal workflow routes: each exact principal id — not a peer id — to the
    /// one workflow collection its calls may reach. The map is the workflow collection allowlist, so
    /// a route need not name a collection that already holds rows; each destination collection
    /// belongs to exactly one principal route, and assembly refuses a collection named by two
    /// routes. A route names the principal `principals` admits; a principal without one reads no
    /// knowledge and never writes a carrier. Empty serves no deliberation and advertises none.
    /// Routes are never read from the wire.
    pub workflow_routes: BTreeMap<String, KnowledgeScope>,
    /// Additional host tools: every descriptor is advertised and its handler installed, except the
    /// ids this runtime owns — `tools.mindcore.deliberate` on a surface with an empty route map, and
    /// the retired `tools.mindcore.adopt` on every surface.
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
    /// Serve these ports: the knowledge face over the shared store, plus the history, adoption and
    /// memory faces the host attached. No baseline provider is composed.
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
/// family flag when a memory service is attached, the transfer family flag when that service
/// reports every one of [`TRANSFER_OPERATIONS`], and the history and adoption flags only when
/// their faces are served.
///
/// The knowledge face applies SPOKE's owner-private disclosure, so a viewpoint-scoped list is
/// served and the supplemental ownership capability is truthful; it never stands in for peer or
/// entry admission.
fn face_capabilities(
    memory: Option<&dyn ServedMemory>,
    history: bool,
    adoption: bool,
) -> Vec<String> {
    let mut capabilities = vec![
        CAPABILITY_SPOKE_KNOWLEDGE.to_string(),
        CAPABILITY_KE_OWNERSHIP.to_string(),
    ];
    if history {
        capabilities.push(CAPABILITY_SPOKE_HISTORY.to_string());
    }
    if adoption {
        // The co-capability `spoke-knowledge` is unconditional above, so the compound gate
        // Connect applies to `knowledge.adopt` always holds.
        capabilities.push(CAPABILITY_SPOKE_KNOWLEDGE_ADOPTION.to_string());
    }
    if let Some(memory) = memory {
        capabilities.push(CAPABILITY_SPOKE_MEMORY.to_string());
        let reported = memory.operations();
        if TRANSFER_OPERATIONS
            .iter()
            .all(|operation| reported.contains(operation))
        {
            capabilities.push(CAPABILITY_SPOKE_MEMORY_TRANSFER.to_string());
        }
    }
    capabilities
}

/// Replace the host's capability claims with the served faces and registrations, then check the
/// manifest the responder will sign.
fn derive_manifest(
    mut manifest: HostCapabilityManifest,
    memory: Option<&dyn ServedMemory>,
    history: bool,
    adoption: bool,
    tools: &[ToolRegistration],
) -> SpokeResult<HostCapabilityManifest> {
    manifest.capabilities = face_capabilities(memory, history, adoption);
    if tools
        .iter()
        .any(|tool| tool.descriptor.capability_id.as_str() == tools::DELIBERATE)
    {
        // `spoke-judgment` gates no op: it is advertised exactly when the tool that carries a
        // `Judgment` is registered, and never stands in for one.
        manifest
            .capabilities
            .push(CAPABILITY_SPOKE_JUDGMENT.to_string());
    }
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
/// The admitted id must also be the one this surface serves: the routed store refuses any other
/// id, so a resolver returning a different one would otherwise be admitted here and denied there.
/// Both faces share this admission, so one check covers them.
///
/// Host admission is not peer input, so an unsupported admitted selection is refused as the same
/// generic `NotAuthorized` a rejecting resolver returns, never as the caller-declared
/// `InvalidInput` the routed store keeps for request scopes: the peer learns nothing about what
/// the face does not serve. The resolver's own refusal passes through unchanged.
fn validated(admission: KnowledgeAdmission, scope_id: &str) -> KnowledgeAdmission {
    let scope_id = scope_id.to_string();
    Arc::new(move |peer_id: &str| -> SpokeResult<Scope> {
        let scope = match admission(peer_id) {
            SpokeResult::Ok(scope) => scope,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        if scope.scope_id != scope_id {
            return spoke_reject(
                SpokeRejectCode::NotAuthorized,
                "knowledge scope not authorized",
                None,
            );
        }
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
