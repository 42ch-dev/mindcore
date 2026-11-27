//! The serving phase: one surface, one listener, one accept loop and the drain.
//!
//! `main` hands over after the lock, and this phase runs the rest of the lifecycle order: the
//! store, the journal root, the assembled surface, the listener, the accept loop and — on
//! `SIGTERM` or `SIGINT` — the drain.
//!
//! The surface is assembled once and shared by every connection. Each accepted stream gets its
//! own transport and responder in a task of its own; the live responders sit in one collection
//! that `drain` walks before the socket name is removed.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};

use mindcore_base::knowledge::KnowledgeScope;
use mindcore_runtime::{journal_dir, AdoptionRoute, MindcoreServe, MindcoreSurface, ServeFaces};
use mindcore_sqlite::MindStore;
use serde_json::json;
use spoke_connect::remote::{
    connect_responder, ConnectResponder, ConnectResponderOptions, MemoryAdmission, RemoteIdentity,
    Transport,
};
use spoke_operations::{spoke_ok, spoke_reject, SpokeRejectCode, SpokeResult};
use spoke_schemas::{HostCapabilityManifest, Principal};
use tokio::net::{UnixListener, UnixStream};
use tokio::signal::unix::{signal, SignalKind};

use crate::config::{hex32, DaemonConfig};
use crate::lock::StoreLock;
use crate::transport::UnixSocketTransport;
use crate::DaemonError;

/// What every connection shares: the one assembled surface and the responder's own wiring.
///
/// Held behind one `Arc` so a connection task clones a handle rather than the peer table.
struct Host {
    surface: Arc<MindcoreSurface>,
    seed: [u8; 32],
    allowlist: Vec<String>,
    peer_keys: HashMap<String, [u8; 32]>,
    invoke_timeout_ms: Option<u64>,
    /// The live responders `drain` walks; a connection removes its own entry when it ends.
    live: Mutex<Vec<(usize, Arc<ConnectResponder>)>>,
}

/// Runs the serving phase of the lifecycle order: store, journal, surface, listen, accept, drain.
///
/// The lock arrives held and is dropped last, after every live session has been told to close.
pub async fn run(config: DaemonConfig, lock: StoreLock) -> Result<(), DaemonError> {
    let store = MindStore::open(&config.store_path)
        .await
        .map_err(|error| DaemonError::Store(format!("{}: {error}", config.store_path.display())))?;
    journal_root(&config.journal.dir)?;
    // `MindcoreServe` owns the store, so the pool handle is kept here: it is the reachable route
    // to the awaited close the drain order names, and it closes the same pool.
    let pool = store.pool();
    let surface = match MindcoreServe::new(
        store,
        // No memory service is served, so no reflection is composed either.
        false,
        journal_dir(config.journal.dir.clone()),
    )
    .surface(faces(&config))
    .await
    {
        SpokeResult::Ok(surface) => Arc::new(surface),
        SpokeResult::Reject(reject) => {
            return Err(DaemonError::Surface(format!(
                "{}: {}",
                reject.code, reject.message
            )))
        }
    };
    // Both signals are installed before the socket exists: a process that cannot hear them would
    // serve until it was killed some other way and leave the name for no reason.
    let mut terminate = shutdown_signal(SignalKind::terminate())?;
    let mut interrupt = shutdown_signal(SignalKind::interrupt())?;
    let host = Arc::new(Host {
        surface,
        seed: crate::config::read_seed(&config.identity.seed_file)?,
        allowlist: config.peers.keys().cloned().collect(),
        peer_keys: peer_keys(&config)?,
        invoke_timeout_ms: config.invoke.timeout_ms,
        live: Mutex::new(Vec::new()),
    });
    let listener = listen(&config.socket_path)?;

    let serving = tokio::select! {
        outcome = accept(&listener, &host) => outcome,
        _ = terminate.recv() => Ok(()),
        _ = interrupt.recv() => Ok(()),
    };

    // Every exit path drains: a failed accept keeps the sessions it already accepted, and the
    // socket name must not outlive the process that answers on it. The listener is dropped first
    // so the accept loop ends before any session is closed.
    drop(listener);
    let live = std::mem::take(&mut *host.live.lock().expect("live lock"));
    drain(&config.socket_path, &live);
    // Step 4 of the drain order: the store's connections finish rather than being cut when the
    // runtime drops. Inline so the pool's type stays the surface's business, not this module's.
    pool.close().await;
    drop(lock);
    serving
}

/// Creates the journal root and syncs its parent, so the new directory is durable before the first
/// publication writes into it.
fn journal_root(dir: &Path) -> Result<(), DaemonError> {
    std::fs::create_dir_all(dir)
        .map_err(|error| DaemonError::Journal(format!("{}: {error}", dir.display())))?;
    let parent = match dir.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    File::open(parent)
        .and_then(|parent| parent.sync_all())
        .map_err(|error| DaemonError::Journal(format!("{}: {error}", parent.display())))
}

/// The accept loop; it ends when its listener is dropped, which `run` does before draining.
async fn accept(listener: &UnixListener, host: &Arc<Host>) -> Result<(), DaemonError> {
    let mut next_id = 0usize;
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(error) => return Err(DaemonError::Socket(format!("accept: {error}"))),
        };
        let host = Arc::clone(host);
        let id = next_id;
        next_id += 1;
        tokio::spawn(async move { serve_connection(stream, id, host).await });
    }
}

/// One accepted connection: its transport, its responder, the tool registration that must run
/// before this task yields, and the two held together until the session ends.
async fn serve_connection(stream: UnixStream, id: usize, host: Arc<Host>) {
    let transport = Arc::new(UnixSocketTransport::from_stream(stream));
    let options = ConnectResponderOptions {
        transport: Arc::clone(&transport) as Arc<dyn Transport>,
        identity: RemoteIdentity { seed: host.seed },
        manifest: host.surface.manifest.clone(),
        allowlist: host.allowlist.clone(),
        peer_keys: host.peer_keys.clone(),
        ports: Some(Arc::clone(&host.surface.ports)),
        invoke_timeout_ms: host.invoke_timeout_ms,
    };
    let responder = connect_responder(options).await;
    // Before this task's own body can yield: a dialer may invoke an advertised tool as soon as
    // its session is established, and the responder never installs handlers from the manifest.
    // `register` is synchronous, so no lock of this module's is held across an `await`.
    if let SpokeResult::Reject(reject) = host.surface.register(&responder) {
        eprintln!(
            "mindcore-daemon: surface: {}: {}",
            reject.code, reject.message
        );
        responder.close();
        return;
    }
    host.live
        .lock()
        .expect("live lock")
        .push((id, Arc::clone(&responder)));
    transport.closed().await;
    responder.close();
    host.live
        .lock()
        .expect("live lock")
        .retain(|(live_id, _)| *live_id != id);
}

/// One `SIGTERM` or `SIGINT` stream.
///
/// A signal the process cannot listen for is a start failure: without both, no signal could ever
/// reach the drain and the socket name would outlive a process killed some other way. It is
/// reported through the phase that owns the listener, so it keeps exit code 1.
fn shutdown_signal(kind: SignalKind) -> Result<tokio::signal::unix::Signal, DaemonError> {
    signal(kind).map_err(|error| {
        DaemonError::Surface(format!("cannot listen for a shutdown signal: {error}"))
    })
}

/// Steps one to three of the drain: close every live session, then remove the socket name.
///
/// The name is the signal that the daemon has stopped serving, so it is given only after every
/// responder has been told to close. A removal that fails is not reported: the name is reused by
/// the next start's stale-name recovery.
fn drain(socket_path: &Path, live: &[(usize, Arc<ConnectResponder>)]) {
    for (_, responder) in live {
        responder.close();
    }
    let _ = std::fs::remove_file(socket_path);
}

/// The host's peer-to-principal mapping: a peer with an entry answers its principal, any other
/// answers `NotAuthorized`. The copy is the whole rule — nothing is read from the wire.
fn principals(peers: BTreeMap<String, String>) -> MemoryAdmission {
    Arc::new(move |peer_id: &str| match peers.get(peer_id) {
        Some(principal) => spoke_ok(principal_of(principal)),
        None => spoke_reject(
            SpokeRejectCode::NotAuthorized,
            format!("peer {peer_id} is not mapped to a principal"),
            None,
        ),
    })
}

/// The `Principal` a configured principal id answers as.
fn principal_of(principal_id: &str) -> Principal {
    serde_json::from_value(json!({
        "principal_id": principal_id,
        "extensions": {},
    }))
    .expect("the configured principal matches the schema")
}

/// The faces the daemon composes: the knowledge face always, history and adoption when the config
/// asks for them, no memory face, no host tools and no providers.
fn faces(config: &DaemonConfig) -> ServeFaces {
    let manifest: HostCapabilityManifest = serde_json::from_value(json!({
        "schema_version": 1,
        "host_id": config.host_id,
        "roles": ["data-store"],
        "capabilities": [],
        "namespaces": [],
        "extensions": {},
    }))
    .expect("the daemon's manifest metadata matches the schema");
    let peers = config
        .peers
        .iter()
        .map(|(peer_id, peer)| (peer_id.clone(), peer.principal.clone()))
        .collect();
    ServeFaces {
        manifest,
        memory: None,
        knowledge: true,
        history: config.history,
        adoption: config.adoption.as_ref().map(|adoption| AdoptionRoute {
            scope_id: adoption.scope_id.clone(),
            source: KnowledgeScope::new(&adoption.source.collection),
        }),
        principals: principals(peers),
        providers: Default::default(),
        tools: Vec::new(),
    }
}

/// The responder's allowlist keys, decoded: one entry per `[peers]` row, so the allowlist and the
/// keys cannot disagree.
fn peer_keys(config: &DaemonConfig) -> Result<HashMap<String, [u8; 32]>, DaemonError> {
    config
        .peers
        .iter()
        .map(|(peer_id, peer)| {
            hex32(&peer.public_key)
                .map(|key| (peer_id.clone(), key))
                .map_err(|fault| {
                    DaemonError::Config(format!("peers.{peer_id}.public_key: {fault}"))
                })
        })
        .collect()
}

/// Binds `socket_path`, removing a leftover name a crashed daemon left behind.
///
/// A path that exists is dialled once: a connection means a live listener owns it, which is a
/// start failure; a refusal means the name is stale, and it is removed before the second bind.
fn listen(socket_path: &Path) -> Result<UnixListener, DaemonError> {
    let first = match UnixListener::bind(socket_path) {
        Ok(listener) => return Ok(listener),
        Err(error) => error,
    };
    if !socket_path.exists() {
        return Err(DaemonError::Socket(format!(
            "{}: {first}",
            socket_path.display()
        )));
    }
    // A synchronous probe: this runs before the accept loop, so it must not need the runtime to
    // decide whether the name is live.
    if std::os::unix::net::UnixStream::connect(socket_path).is_ok() {
        return Err(DaemonError::Socket(format!(
            "{}: a live listener already owns the name",
            socket_path.display()
        )));
    }
    std::fs::remove_file(socket_path).map_err(|error| {
        DaemonError::Socket(format!(
            "{}: a stale socket name could not be removed: {error}",
            socket_path.display()
        ))
    })?;
    UnixListener::bind(socket_path)
        .map_err(|error| DaemonError::Socket(format!("{}: {error}", socket_path.display())))
}

#[cfg(test)]
mod tests {
    use mindcore_base::boundary::CallerId;
    use mindcore_runtime::minds::{self, CreateMind};
    use tempfile::TempDir;

    use super::*;
    use crate::config::{
        AdoptionConfig, AdoptionSource, IdentityConfig, InvokeConfig, JournalConfig, PeerConfig,
    };

    const SEED: &str = "0101010101010101010101010101010101010101010101010101010101010101";
    const PEER_KEY: &str = "0202020202020202020202020202020202020202020202020202020202020202";

    /// A config whose every path lives under `dir`, as the phase sequence would have loaded one.
    fn config(dir: &Path) -> DaemonConfig {
        let seed = dir.join("seed");
        std::fs::write(&seed, SEED).expect("write seed");
        DaemonConfig {
            store_path: dir.join("store.db"),
            socket_path: dir.join("daemon.sock"),
            host_id: "reference-host".to_string(),
            history: false,
            invoke: InvokeConfig::default(),
            identity: IdentityConfig { seed_file: seed },
            peers: BTreeMap::from([(
                "peer-a".to_string(),
                PeerConfig {
                    public_key: PEER_KEY.to_string(),
                    principal: "prn_a".to_string(),
                },
            )]),
            journal: JournalConfig {
                dir: dir.join("journal"),
            },
            adoption: None,
        }
    }

    struct Fixture {
        _dir: TempDir,
        config: DaemonConfig,
        store: MindStore,
    }

    impl Fixture {
        async fn open() -> Self {
            let dir = TempDir::new().expect("temporary directory");
            let config = config(dir.path());
            let store = MindStore::open(&config.store_path)
                .await
                .expect("open store");
            Self {
                _dir: dir,
                config,
                store,
            }
        }

        /// The serving side over this fixture's store; `surface` is called on it as often as a
        /// test needs, since assembly reads the store without owning it.
        fn serve(self) -> MindcoreServe {
            MindcoreServe::new(self.store, false, |_| Ok(()))
        }
    }

    fn expect_ok<T>(result: SpokeResult<T>, what: &str) -> T {
        match result {
            SpokeResult::Ok(value) => value,
            SpokeResult::Reject(reject) => panic!("{what}: {}: {}", reject.code, reject.message),
        }
    }

    /// The advertised names, sorted, so an exact set is one assertion.
    fn capabilities(manifest: &HostCapabilityManifest) -> Vec<String> {
        let mut capabilities = manifest.capabilities.clone();
        capabilities.sort();
        capabilities
    }

    /// The always-composed families: the two knowledge flags, `spoke-judgment` (the `deliberate`
    /// tool carries it) and the four tools the runtime registers itself.
    fn default_capabilities() -> Vec<String> {
        let mut expected = vec![
            "spoke-judgment".to_string(),
            "spoke-knowledge".to_string(),
            "spoke-knowledge-scoped".to_string(),
            "tools.mindcore.absorb".to_string(),
            "tools.mindcore.deliberate".to_string(),
            "tools.mindcore.read_cognition".to_string(),
            "tools.mindcore.recall_context".to_string(),
        ];
        expected.sort();
        expected
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_default_surface_advertises_the_knowledge_faces_and_the_tools() {
        let fixture = Fixture::open().await;
        let config = fixture.config.clone();
        let surface = expect_ok(
            fixture.serve().surface(faces(&config)).await,
            "assemble the default surface",
        );
        assert_eq!(capabilities(&surface.manifest), default_capabilities());
        assert!(
            surface.ports.as_memory().is_none(),
            "the daemon composes no memory face"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn history_adds_its_own_capability_and_nothing_else() {
        let fixture = Fixture::open().await;
        let enabled = DaemonConfig {
            history: true,
            ..fixture.config.clone()
        };
        let surface = fixture.serve();
        let composed = expect_ok(
            surface.surface(faces(&enabled)).await,
            "assemble with history",
        );
        let mut expected = default_capabilities();
        expected.push("spoke-history".to_string());
        expected.sort();
        assert_eq!(capabilities(&composed.manifest), expected);
        assert!(composed.ports.as_history().is_some(), "the face is served");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn adoption_adds_its_own_capability_and_nothing_else() {
        let fixture = Fixture::open().await;
        let enabled = DaemonConfig {
            adoption: Some(AdoptionConfig {
                scope_id: "spoke-source".to_string(),
                source: AdoptionSource {
                    collection: "shared-source".to_string(),
                },
            }),
            ..fixture.config.clone()
        };
        let surface = fixture.serve();
        let composed = expect_ok(
            surface.surface(faces(&enabled)).await,
            "assemble with adoption",
        );
        let mut expected = default_capabilities();
        expected.push("spoke-knowledge-adoption".to_string());
        expected.sort();
        assert_eq!(capabilities(&composed.manifest), expected);
        assert!(composed.ports.as_adoption().is_some(), "the face is served");
    }

    /// No config composes a memory face or an ownership conjunct, so neither family may ever be
    /// advertised.
    #[tokio::test(flavor = "multi_thread")]
    async fn no_config_advertises_a_face_the_daemon_does_not_serve() {
        let fixture = Fixture::open().await;
        let cases = [
            fixture.config.clone(),
            DaemonConfig {
                history: true,
                ..fixture.config.clone()
            },
            DaemonConfig {
                history: true,
                adoption: Some(AdoptionConfig {
                    scope_id: "spoke-source".to_string(),
                    source: AdoptionSource {
                        collection: "shared-source".to_string(),
                    },
                }),
                ..fixture.config.clone()
            },
        ];
        let surface = fixture.serve();
        for config in cases {
            let composed = expect_ok(
                surface.surface(faces(&config)).await,
                "assemble every configured surface",
            );
            for absent in ["spoke-memory", "spoke-memory-transfer", "ke-ownership"] {
                assert!(
                    !composed
                        .manifest
                        .capabilities
                        .iter()
                        .any(|name| name == absent),
                    "{absent} must never be advertised: {:?}",
                    composed.manifest.capabilities
                );
            }
        }
    }

    /// Every registered Mind is a possible adoption destination, so a source collection one of
    /// them already holds is an assembly refusal.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_adoption_collection_a_registered_mind_holds_is_refused() {
        let fixture = Fixture::open().await;
        let mind = expect_ok(
            minds::create(
                &fixture.store,
                &CreateMind {
                    mind_id: None,
                    display_name: None,
                    owner: CallerId::new("prn_a"),
                },
            )
            .await,
            "create the Mind that holds the collection",
        );
        let config = DaemonConfig {
            adoption: Some(AdoptionConfig {
                scope_id: "spoke-source".to_string(),
                source: AdoptionSource {
                    collection: mind.knowledge.collection.clone(),
                },
            }),
            ..fixture.config.clone()
        };
        match fixture.serve().surface(faces(&config)).await {
            SpokeResult::Reject(reject) => assert_eq!(reject.code, SpokeRejectCode::InvalidInput),
            SpokeResult::Ok(_) => panic!("a taken adoption collection must not assemble"),
        }
    }

    /// The mapping is the whole admission rule: a configured peer answers its principal, and a
    /// peer the config never named is not authorized.
    #[test]
    fn the_principal_mapping_answers_the_configured_peer_only() {
        let dir = TempDir::new().expect("temporary directory");
        let config = config(dir.path());
        let admission = principals(
            config
                .peers
                .iter()
                .map(|(peer_id, peer)| (peer_id.clone(), peer.principal.clone()))
                .collect(),
        );
        match admission("peer-a") {
            SpokeResult::Ok(principal) => assert_eq!(principal.principal_id.as_str(), "prn_a"),
            SpokeResult::Reject(reject) => panic!("peer-a is configured: {reject:?}"),
        }
        match admission("peer-unknown") {
            SpokeResult::Ok(_) => panic!("an unmapped peer holds no admission"),
            SpokeResult::Reject(reject) => assert_eq!(reject.code, SpokeRejectCode::NotAuthorized),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_leftover_file_at_the_socket_path_is_replaced() {
        let dir = TempDir::new().expect("temporary directory");
        let path = dir.path().join("daemon.sock");
        std::fs::write(&path, b"not a socket").expect("write the leftover");
        let listener = listen(&path).expect("replace a leftover name");
        assert!(path.exists(), "the replacement name is bound");
        drop(listener);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_live_listener_on_the_socket_path_is_refused() {
        let dir = TempDir::new().expect("temporary directory");
        let path = dir.path().join("daemon.sock");
        let live = UnixListener::bind(&path).expect("bind the live listener");
        match listen(&path) {
            Err(DaemonError::Socket(message)) => {
                assert!(message.contains("live listener"), "{message}")
            }
            Err(other) => panic!("a live listener is a socket refusal, not {other:?}"),
            Ok(_) => panic!("a live listener owns the name"),
        }
        drop(live);
    }

    /// A daemon that exits non-zero has bound no socket: a bind under a missing directory fails
    /// and names the path.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_bind_failure_names_the_path() {
        let dir = TempDir::new().expect("temporary directory");
        let path = dir.path().join("absent").join("daemon.sock");
        match listen(&path) {
            Err(DaemonError::Socket(message)) => assert!(message.contains("absent"), "{message}"),
            Err(other) => panic!("a bind failure is a socket refusal, not {other:?}"),
            Ok(_) => panic!("no directory holds the socket"),
        }
    }

    /// The journal root is the host's to create and make durable before the first publication.
    #[test]
    fn the_journal_root_is_created_with_its_parent_synced() {
        let dir = TempDir::new().expect("temporary directory");
        let root = dir.path().join("journal");
        journal_root(&root).expect("create the journal root");
        assert!(root.is_dir());
    }
}
