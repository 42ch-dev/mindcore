//! The Mindcore serving surface: the workflow engine, the host's memory face and the per-Mind
//! knowledge face assembled into the pieces a Connect responder is built from.
//!
//! The crate opens nothing: no listener, accept loop, identity or transport. The host supplies
//! those plus its admission, publisher and memory service, then builds
//! `ConnectResponderOptions` from [`MindcoreSurface`]. Advertisements follow what is served:
//! [`MindcoreServe::surface`] derives `capabilities` and `tools` from the composed faces and the
//! registrations, never from a host-supplied capability string.
//!
//! The knowledge face is one composition decision ([`ServeFaces::knowledge`]): the request's scope
//! selector names a Mind, admission resolves it exactly as the memory path does, and only then is
//! the caller bound into the returned scope. The face therefore serves several Minds from one
//! responder, and the admission callback blocks — see [`ServeFaces::knowledge`] for the runtime
//! requirement that follows.
//!
//! The crate is also the library's entry crate: [`minds`] holds the in-process entry points
//! `create`, `list` and `get` a host calls on its `MindStore`, and [`mind`] holds the `Mind`
//! handle a caller opens over one Mind.

pub mod journal;
pub mod mind;
pub mod minds;
pub mod tools;

use std::sync::Arc;

use serde_json::Value;

use mindcore_spi::{EmbeddingIdentity, MindWorkflows, SpiError, SpiMindWorkflows};
use mindcore_spoke::{
    admitted_mind, MemoryWorkflowBinding, MindKnowledgePort, ServedMemory, TRANSFER_OPERATIONS,
};
use mindcore_sqlite::{bind_caller, validate_admission_scope, vectors, MindStore};
use spoke_connect::core::{
    CAPABILITY_KE_OWNERSHIP, CAPABILITY_SPOKE_JUDGMENT, CAPABILITY_SPOKE_KNOWLEDGE,
    CAPABILITY_SPOKE_KNOWLEDGE_SCOPED, CAPABILITY_SPOKE_MEMORY, CAPABILITY_SPOKE_MEMORY_TRANSFER,
};
use spoke_connect::remote::{
    ConnectResponder, MemoryAdmission, RemoteMemoryService, RemoteScopedKnowledgeFace,
    RemoteServePorts, ScopedKnowledgeAdmission, ToolHandler,
};
use spoke_operations::{validate_manifest_tools, ScopedKnowledgePort, SpokeResult};
use spoke_schemas::host_capability_manifest::ToolDescriptor;
use spoke_schemas::{HostCapabilityManifest, Scope};
use sqlx::SqlitePool;
use tokio::runtime::Handle;

pub use journal::journal_dir;
pub use mind::{Capture, Deliberation, Mind, Recall};
pub use mindcore_spoke::Cognition;
pub use tools::MindProviders;
use tools::WorkflowTools;

/// The `tools.*` namespace this runtime owns.
const TOOLS_NAMESPACE: &str = "mindcore";

/// The id of the retired product adoption tool.
///
/// The id is dropped from every host registration, so no host can advertise or install anything
/// under it.
const RETIRED_ADOPT_TOOL: &str = "tools.mindcore.adopt";

/// The tool ids the runtime reserves: those it advertises, plus the retired adoption tool. A
/// host registration carrying one is dropped, so no host can advertise or install anything the
/// runtime serves, or anything under the id a retired tool once answered. Host tools keep every
/// other id, inside the namespace and out.
///
/// The set is derived from [`WorkflowTools::advertised`] — the one list advertisement and
/// installation both read — so a tool added there is reserved without touching this filter.
fn reserved_tools() -> Vec<&'static str> {
    let mut reserved: Vec<&str> = tools::WorkflowTools::advertised_ids();
    reserved.push(RETIRED_ADOPT_TOOL);
    reserved
}

/// The Mindcore serving side over one host store.
///
/// The workflow engine runs on the same pool the host's store serves, so a tool call and a
/// registry read see one snapshot boundary. The engine never opens a transaction across provider
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

    /// The engine one surface serves: each workflow call resolves the Mind it reaches from the
    /// `space_id` the request carries, through the registry row.
    fn workflows(&self) -> Arc<SpiMindWorkflows> {
        let publish = Arc::clone(&self.publish);
        let binding = MemoryWorkflowBinding::new(self.store.pool(), self.reflections)
            .with_publisher(move |targets| publish(targets));
        Arc::new(SpiMindWorkflows::new(binding))
    }

    /// Assemble the ports, manifest and tool registrations for `ConnectResponderOptions`.
    ///
    /// Nothing is served until the host creates its responder and calls
    /// [`MindcoreSurface::register`]: `connect_responder` starts its own receive task and never
    /// installs handlers from the manifest.
    pub fn surface(&self, faces: ServeFaces) -> SpokeResult<MindcoreSurface> {
        let principals = faces.principals;
        let workflows = self.workflows();
        let engine: Arc<dyn MindWorkflows> = workflows.clone();
        let mut tools = WorkflowTools::new(
            engine,
            Arc::clone(&principals),
            faces.providers,
            self.store.pool(),
        )
        .registrations();
        let reserved = reserved_tools();
        tools.extend(
            faces
                .tools
                .into_iter()
                .filter(|tool| !reserved.contains(&tool.descriptor.capability_id.as_str())),
        );
        let knowledge = faces
            .knowledge
            .then(|| self.scoped_knowledge(Arc::clone(&principals)));
        let manifest = match derive_manifest(
            faces.manifest,
            faces.memory.as_deref(),
            knowledge.is_some(),
            &tools,
        ) {
            SpokeResult::Ok(manifest) => manifest,
            SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
        };
        SpokeResult::Ok(MindcoreSurface {
            ports: Arc::new(MindPorts {
                memory: faces.memory,
                knowledge,
            }),
            manifest,
            tools,
            workflows,
            pool: self.store.pool(),
        })
    }

    /// The per-Mind scoped knowledge face: one store and one admission for every Mind.
    ///
    /// The store is layer 1's served port over the per-call routed store, so every call resolves
    /// the collection from the admitted `scope_id`. The admission is the callback Connect runs on
    /// its blocking pool: the host's own peer-to-principal mapping, then the Mind the request's
    /// selector names through [`admitted_mind`], then the caller binding
    /// ([`mindcore_sqlite::bind_caller`]) — the last step runs on every successful return, so no
    /// scope leaves this face unbound.
    fn scoped_knowledge(&self, principals: MemoryAdmission) -> Arc<RemoteScopedKnowledgeFace> {
        let pool = self.store.pool();
        let store: Arc<dyn ScopedKnowledgePort + Send + Sync> = Arc::new(MindKnowledgePort::new(
            pool.clone(),
            Arc::new(self.store.served_knowledge()),
        ));
        let admission: ScopedKnowledgeAdmission = Arc::new(move |peer_id, selector| {
            let principal = match principals(peer_id) {
                SpokeResult::Ok(principal) => principal,
                SpokeResult::Reject(reject) => return SpokeResult::Reject(reject),
            };
            // A selector is what the request asked for, never a grant. No selector names no Mind,
            // and the empty id no Mind carries makes admission refuse it through the one
            // `admitted_mind` rule rather than a second spelling of it.
            let scope_id = selector.unwrap_or_default();
            // Synchronous because Connect's admission is; the responder runs it on the blocking
            // pool, where `block_on` is legal on a multi-thread runtime.
            let admitted =
                Handle::current().block_on(admitted_mind(&pool, &principal.principal_id, scope_id));
            if let Err(reject) = admitted {
                return SpokeResult::Reject(reject);
            }
            let mut scope = Scope {
                cognition: None,
                entry_ids: None,
                entry_types: None,
                extensions: None,
                fork_id: None,
                scope_id: scope_id.to_string(),
                source_id: None,
                timeline_event_ids: None,
                timeline_scale: None,
                viewpoint: None,
            };
            bind_caller(&mut scope, &principal.principal_id);
            match validate_admission_scope(&scope) {
                SpokeResult::Ok(()) => SpokeResult::Ok(scope),
                SpokeResult::Reject(reject) => SpokeResult::Reject(reject),
            }
        });
        Arc::new(RemoteScopedKnowledgeFace { store, admission })
    }
}

/// The runtime's own ports: the host's memory service and, when `ServeFaces.knowledge` asks for
/// it, the per-Mind scoped knowledge face.
///
/// History and adoption stay absent — a call to each is refused on the responder's dispatch-deny
/// branch, wire `capability_missing`, which the Rust dialer surfaces as `CapabilityPortMissing` —
/// because neither can name a Mind yet; each is composed per Mind once SPOKE carries that
/// contract.
struct MindPorts {
    memory: Option<Arc<dyn ServedMemory>>,
    knowledge: Option<Arc<RemoteScopedKnowledgeFace>>,
}

/// The responder takes the family's bare service; `ServedMemory` only adds the operation report.
fn remote_memory(memory: Arc<dyn ServedMemory>) -> Arc<dyn RemoteMemoryService> {
    memory
}

impl RemoteServePorts for MindPorts {
    fn as_memory(&self) -> Option<Arc<dyn RemoteMemoryService>> {
        self.memory.clone().map(remote_memory)
    }

    fn as_scoped_knowledge(&self) -> Option<Arc<RemoteScopedKnowledgeFace>> {
        self.knowledge.clone()
    }
}

/// The host's side of the surface: non-capability manifest metadata and the face wiring the
/// runtime cannot invent.
pub struct ServeFaces {
    /// The responder's hello metadata. `capabilities` and `tools` are replaced with what the
    /// runtime actually serves; every other member stays the host's.
    pub manifest: HostCapabilityManifest,
    /// The host's memory family service. `None` composes no memory face and advertises none.
    ///
    /// Installing the service proves the `spoke-memory` family. It earns
    /// `spoke-memory-transfer` only by reporting all five §7.29–§7.33 operations through
    /// [`ServedMemory::operations`]: a signed manifest must not claim a family the bound service
    /// cannot show it dispatches. A bare `RemoteMemoryService` takes the trait's default and
    /// reports none.
    pub memory: Option<Arc<dyn ServedMemory>>,
    /// Whether the per-Mind knowledge face is composed.
    ///
    /// `true` composes the scoped knowledge face — [`MindKnowledgePort`] over the store's
    /// per-call routed store, with one admission for every Mind — and advertises
    /// `spoke-knowledge`, `ke-ownership` and `spoke-knowledge-scoped` together. `false` composes
    /// and advertises none, and the knowledge ops keep the dispatch-deny answer.
    ///
    /// The admission callback is synchronous and Connect runs it on its blocking pool, so it
    /// drives the admission through [`Handle::block_on`]: the host must run the responder on a
    /// **multi-thread** runtime. A single-thread runtime cannot run the callback's store read,
    /// and the callback would panic (contained by Connect as `INTERNAL_ERROR`).
    pub knowledge: bool,
    /// Maps the authenticated peer to the `Principal` the workflows admit. The same host mapping
    /// the memory face uses; never read from the wire.
    pub principals: MemoryAdmission,
    /// Providers and the domain lens injected into each workflow call.
    pub providers: MindProviders,
    /// Additional host tools: every descriptor is advertised and its handler installed, except
    /// registrations carrying one of the runtime's reserved tool ids — the four tools the
    /// runtime registers itself and the retired `tools.mindcore.adopt` — which are dropped on
    /// every surface.
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
    /// Serve these ports: the host's memory face when it attached one, and genuine absence for
    /// every other family. No baseline provider is composed.
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

/// The advertised capabilities follow the composed faces: the memory family flag when a memory
/// service is attached and the transfer family flag when that service reports every one of
/// [`TRANSFER_OPERATIONS`]; the three knowledge flags together when the scoped face is composed.
/// No other family is composed, so none is advertised.
fn face_capabilities(memory: Option<&dyn ServedMemory>, knowledge: bool) -> Vec<String> {
    let mut capabilities = Vec::new();
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
    if knowledge {
        // Without the marker a per-Mind face could not serve a scoped `get` or `put` at all, and
        // without `ke-ownership` Connect refuses any request that names a viewpoint.
        capabilities.push(CAPABILITY_SPOKE_KNOWLEDGE.to_string());
        capabilities.push(CAPABILITY_KE_OWNERSHIP.to_string());
        capabilities.push(CAPABILITY_SPOKE_KNOWLEDGE_SCOPED.to_string());
    }
    capabilities
}

/// Replace the host's capability claims with the served faces and registrations, then check the
/// manifest the responder will sign.
fn derive_manifest(
    mut manifest: HostCapabilityManifest,
    memory: Option<&dyn ServedMemory>,
    knowledge: bool,
    tools: &[ToolRegistration],
) -> SpokeResult<HostCapabilityManifest> {
    manifest.capabilities = face_capabilities(memory, knowledge);
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
