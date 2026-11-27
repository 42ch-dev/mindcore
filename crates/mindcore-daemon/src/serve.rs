//! The serving phase: one surface, one listener, one accept loop and the drain.
//!
//! `main` hands over after the lock, and this phase runs the rest of the lifecycle order: the
//! store, the journal root, the assembled surface, the listener, the accept loop and — on
//! `SIGTERM` or `SIGINT` — the drain.
//!
//! The surface is assembled once and shared by every connection. Each accepted stream gets its
//! own transport and responder in a task of its own, every task is owned by one `JoinSet`, and
//! the live responders sit in one collection the drain walks. Nothing the daemon owns is ever
//! unlinked: `socket_path` may not name the store or the seed file, checked before the start and
//! again immediately before any stale name is removed.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::ErrorKind;
use std::os::unix::fs::FileTypeExt;
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
use tokio::task::JoinSet;

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
    /// The live responders the drain walks; a connection removes its own entry when it ends.
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
    let host = Arc::new(host(surface, &config)?);
    let listener = listen(&config)?;

    // Every accepted connection is owned by `connections`, so the drain can end each one rather
    // than hope it finished setting up. Dropping the accept future drops the listener with it.
    let mut connections: JoinSet<()> = JoinSet::new();
    let serving = {
        let accepting = accept(listener, &host, &mut connections);
        tokio::pin!(accepting);
        tokio::select! {
            outcome = &mut accepting => outcome,
            _ = terminate.recv() => Ok(()),
            _ = interrupt.recv() => Ok(()),
        }
    };

    // The drain, in the stated order. Closing first lets a session end its in-flight work; the
    // shutdown then cancels whatever was still setting up — a task suspended in
    // `connect_responder` or between its responder and its registration — so no responder can
    // outlive this snapshot, and the second walk catches one registered inside that window.
    let live = std::mem::take(&mut *host.live.lock().expect("live lock"));
    drain_sessions(&live);
    connections.shutdown().await;
    drain_sessions(&std::mem::take(&mut *host.live.lock().expect("live lock")));
    let removed = remove_socket(&config.socket_path);
    // Step 4: the store's connections finish rather than being cut when the runtime drops. Inline
    // so the pool's type stays the surface's business, not this module's.
    pool.close().await;
    drop(lock);
    serving.and(removed)
}

/// The daemon's own wiring for one assembled surface.
fn host(surface: Arc<MindcoreSurface>, config: &DaemonConfig) -> Result<Host, DaemonError> {
    Ok(Host {
        surface,
        seed: crate::config::read_seed(&config.identity.seed_file)?,
        allowlist: config.peers.keys().cloned().collect(),
        peer_keys: peer_keys(config)?,
        invoke_timeout_ms: config.invoke.timeout_ms,
        live: Mutex::new(Vec::new()),
    })
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

/// The accept loop; it ends when its listener is dropped, which happens when `run` drops this
/// future, and reaps each connection task as it finishes.
async fn accept(
    listener: UnixListener,
    host: &Arc<Host>,
    connections: &mut JoinSet<()>,
) -> Result<(), DaemonError> {
    let mut next_id = 0usize;
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                // An accept fault ends the loop; `run` drains what was already accepted and
                // reports it, because a daemon that cannot accept is not serving.
                let stream = match accepted {
                    Ok((stream, _)) => stream,
                    Err(error) => {
                        return Err(DaemonError::Socket(format!("accept: {error}")))
                    }
                };
                let host = Arc::clone(host);
                let id = next_id;
                next_id += 1;
                // The set owns the task: dropping it aborts the task, and `run`'s drain joins.
                connections.spawn(async move { serve_connection(stream, id, host).await });
            }
            // Finished sessions are reaped here, so a long-lived daemon does not accumulate them.
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
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
    // Tracked before this task's next await, so the drain owns the session from the moment it
    // exists: `connect_responder` spawned the responder's own task with an `Arc` of its own, and
    // an aborted connection task would otherwise leave that session serving unclosed.
    host.live
        .lock()
        .expect("live lock")
        .push((id, Arc::clone(&responder)));
    park_in_setup().await;
    // A dialer may invoke an advertised tool as soon as its session is established, and the
    // responder never installs handlers from the manifest. `register` is synchronous, so no lock
    // of this module's is held across an `await`.
    if let SpokeResult::Reject(reject) = host.surface.register(&responder) {
        eprintln!(
            "mindcore-daemon: surface: {}: {}",
            reject.code, reject.message
        );
        responder.close();
        host.live
            .lock()
            .expect("live lock")
            .retain(|(live_id, _)| *live_id != id);
        return;
    }
    transport.closed().await;
    responder.close();
    host.live
        .lock()
        .expect("live lock")
        .retain(|(live_id, _)| *live_id != id);
}

/// A test seam: when installed, a connection task parks here — after its responder is tracked and
/// before it registers — so a drain can be driven while a session is still setting up.
#[cfg(test)]
static SETUP_PARK: Mutex<Option<SetupPark>> = Mutex::new(None);

#[cfg(test)]
struct SetupPark {
    parked: tokio::sync::mpsc::UnboundedSender<()>,
    resume: tokio::sync::mpsc::UnboundedReceiver<()>,
}

#[cfg(test)]
async fn park_in_setup() {
    let seam = SETUP_PARK.lock().expect("setup seam lock").take();
    let Some(mut seam) = seam else { return };
    let _ = seam.parked.send(());
    let _ = seam.resume.recv().await;
}

#[cfg(not(test))]
async fn park_in_setup() {}

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

/// Tells every live session to close. `ConnectResponder::close` is synchronous and idempotent.
fn drain_sessions(live: &[(usize, Arc<ConnectResponder>)]) {
    for (_, responder) in live {
        responder.close();
    }
}

/// Removes the socket name — the signal that the daemon has stopped serving.
///
/// It is given only after every session has been told to close, and its failure is reported: a
/// daemon that leaves the name behind while exiting 0 claims a clean stop it did not reach. An
/// absent name is the intended end state, not a failure.
fn remove_socket(socket_path: &Path) -> Result<(), DaemonError> {
    match std::fs::remove_file(socket_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(DaemonError::Socket(format!(
            "{}: the socket name could not be removed: {error}",
            socket_path.display()
        ))),
    }
}

/// Binds `socket_path`, removing the name only when it is a socket established to be dead.
///
/// Nothing that is not a socket is ever unlinked, so a name the daemon does not own — the store,
/// the seed file, any other file, a directory, a symlink or a device — is refused rather than
/// destroyed. That removes the check-then-unlink race instead of narrowing it: the only unlink on
/// a start is a socket whose own probe was refused, so an alias can no longer be reached at all.
fn listen(config: &DaemonConfig) -> Result<UnixListener, DaemonError> {
    let socket_path = &config.socket_path;
    let fail = |fault: String| DaemonError::Socket(format!("{}: {fault}", socket_path.display()));
    let bind = || UnixListener::bind(socket_path).map_err(|error| fail(error.to_string()));

    let file_type = match std::fs::symlink_metadata(socket_path) {
        // A free name is the ordinary start.
        Err(error) if error.kind() == ErrorKind::NotFound => return bind(),
        Err(error) => return Err(fail(error.to_string())),
        Ok(metadata) => metadata.file_type(),
    };
    if !file_type.is_socket() {
        return Err(fail(format!(
            "the name is occupied by {}, which this daemon does not remove; move it aside first",
            file_type_name(&file_type)
        )));
    }
    // A socket is the one kind this daemon may remove, and only when its own probe says no
    // listener answers.
    match std::os::unix::net::UnixStream::connect(socket_path) {
        Ok(_) => return Err(fail("a live listener already owns the name".to_string())),
        Err(error) if error.kind() == ErrorKind::ConnectionRefused => {}
        Err(error) => {
            return Err(fail(format!(
                "the name could not be probed, so its liveness is unknown: {error}"
            )))
        }
    }
    std::fs::remove_file(socket_path)
        .map_err(|error| fail(format!("a stale socket could not be removed: {error}")))?;
    bind()
}

/// The file type's name in a refusal, so the operator knows what occupies the path.
fn file_type_name(file_type: &std::fs::FileType) -> &'static str {
    if file_type.is_file() {
        "a regular file"
    } else if file_type.is_dir() {
        "a directory"
    } else if file_type.is_symlink() {
        "a symbolic link"
    } else if file_type.is_fifo() {
        "a FIFO"
    } else if file_type.is_char_device() {
        "a character device"
    } else if file_type.is_block_device() {
        "a block device"
    } else {
        "an unsupported file type"
    }
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

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

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

        /// The serving side over this fixture's store. `surface` reads the store, so one fixture
        /// answers as many assemblies as a test needs.
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

    /// A regular file at the name is refused and survives: the daemon unlinks nothing it does not
    /// own, so the withdrawn "replace a leftover file" rule can no longer destroy one.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_regular_file_at_the_socket_path_is_refused_and_survives() {
        let dir = TempDir::new().expect("temporary directory");
        let config = config(dir.path());
        let bytes = b"somebody else's file";
        std::fs::write(&config.socket_path, bytes).expect("write the file");
        match listen(&config) {
            Err(DaemonError::Socket(message)) => {
                assert!(message.contains("regular file"), "{message}");
                assert!(message.contains("does not remove"), "{message}");
            }
            Err(other) => panic!("a regular file is a socket refusal, not {other:?}"),
            Ok(_) => panic!("a regular file must not be replaced"),
        }
        assert_eq!(
            std::fs::read(&config.socket_path).expect("read the file"),
            bytes,
            "the file survives the refusal"
        );
    }

    /// A stale socket — its listener is gone — is the one name the daemon replaces.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_socket_is_replaced() {
        let dir = TempDir::new().expect("temporary directory");
        let config = config(dir.path());
        // A bound-then-dropped listener leaves its name behind, exactly like a crashed daemon.
        drop(UnixListener::bind(&config.socket_path).expect("leave a stale socket"));
        assert!(config.socket_path.exists(), "the stale name is there");
        let listener = listen(&config).expect("replace a stale socket");
        assert!(config.socket_path.exists(), "the replacement name is bound");
        drop(listener);
    }

    /// A symlink to a live listener is refused before any probe, and the listener keeps serving.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_symlink_to_a_live_listener_is_refused_and_the_listener_serves() {
        let dir = TempDir::new().expect("temporary directory");
        let config = config(dir.path());
        let real = dir.path().join("real.sock");
        let live = UnixListener::bind(&real).expect("bind the live listener");
        let alias = dir.path().join("alias.sock");
        std::os::unix::fs::symlink(&real, &alias).expect("link to the listener");
        let aliased = DaemonConfig {
            socket_path: alias.clone(),
            ..config.clone()
        };
        match listen(&aliased) {
            Err(DaemonError::Socket(message)) => {
                assert!(message.contains("symbolic link"), "{message}")
            }
            Err(other) => panic!("a symlink is a socket refusal, not {other:?}"),
            Ok(_) => panic!("a symlink is never bound or removed"),
        }
        assert!(alias.exists(), "the symlink survives");
        assert!(real.exists(), "the listener's name survives");
        // The listener still answers, so nothing replaced its name.
        drop(
            UnixStream::connect(&real)
                .await
                .expect("the listener serves"),
        );
        drop(live);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_live_listener_on_the_socket_path_is_refused() {
        let dir = TempDir::new().expect("temporary directory");
        let config = config(dir.path());
        let live = UnixListener::bind(&config.socket_path).expect("bind the live listener");
        match listen(&config) {
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
    #[test]
    fn a_bind_failure_names_the_path() {
        let dir = TempDir::new().expect("temporary directory");
        let config = DaemonConfig {
            socket_path: dir.path().join("absent").join("daemon.sock"),
            ..config(dir.path())
        };
        match listen(&config) {
            Err(DaemonError::Socket(message)) => {
                assert!(message.contains("absent"), "{message}")
            }
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

    /// F-001 end to end: a socket path naming the store is refused, because the store is a regular
    /// file and the daemon unlinks nothing that is not a dead socket. The bytes survive.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_socket_path_naming_the_store_is_refused_and_the_store_survives() {
        let dir = TempDir::new().expect("temporary directory");
        let config = config(dir.path());
        // A real database, opened and closed, so the refusal protects genuine bytes.
        MindStore::open(&config.store_path)
            .await
            .expect("create the store")
            .close()
            .await;
        let bytes = std::fs::read(&config.store_path).expect("read the store");
        assert!(!bytes.is_empty(), "the store has content to protect");
        let collided = DaemonConfig {
            socket_path: config.store_path.clone(),
            ..config.clone()
        };
        let lock = StoreLock::acquire(&collided.store_path).expect("take the lock");
        match run(collided, lock).await {
            Err(DaemonError::Socket(message)) => {
                assert!(message.contains("regular file"), "{message}")
            }
            Err(other) => panic!("a colliding socket path is a socket refusal, not {other:?}"),
            Ok(()) => panic!("a socket path naming the store must not serve"),
        }
        assert_eq!(
            std::fs::read(&config.store_path).expect("read the store again"),
            bytes,
            "the store survives the refusal"
        );
    }

    /// The alias form of the same case: a symlink to the store is refused as a symlink, and the
    /// store keeps its bytes.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_symlink_to_the_store_is_refused_and_the_store_survives() {
        let dir = TempDir::new().expect("temporary directory");
        let config = config(dir.path());
        let bytes = b"the store's bytes";
        std::fs::write(&config.store_path, bytes).expect("write the store");
        let alias = dir.path().join("alias.sock");
        std::os::unix::fs::symlink(&config.store_path, &alias).expect("link to the store");
        let collided = DaemonConfig {
            socket_path: alias.clone(),
            ..config.clone()
        };
        match listen(&collided) {
            Err(DaemonError::Socket(message)) => {
                assert!(message.contains("symbolic link"), "{message}")
            }
            other => panic!("a symlink is a socket refusal, not {other:?}"),
        }
        assert!(alias.exists(), "the symlink survives");
        assert_eq!(
            std::fs::read(&config.store_path).expect("read the store"),
            bytes,
            "the store survives"
        );
    }

    /// F-003: a probe that cannot establish the listener is dead removes nothing. Only a refused
    /// connection means a leftover name.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unprobeable_socket_name_is_refused_without_unlinking() {
        // A search-only denial does not constrain uid 0, so the case cannot be built as root.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let dir = TempDir::new().expect("temporary directory");
        let config = config(dir.path());
        let live = UnixListener::bind(&config.socket_path).expect("bind the live listener");
        let parent = config
            .socket_path
            .parent()
            .expect("the socket names a parent")
            .to_path_buf();
        let original = std::fs::metadata(&parent)
            .expect("stat the parent")
            .permissions();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o000))
            .expect("close the parent");
        let outcome = listen(&config);
        std::fs::set_permissions(&parent, original).expect("reopen the parent");
        match outcome {
            Err(DaemonError::Socket(_)) => {}
            other => panic!("an unprobeable name is a socket refusal, not {other:?}"),
        }
        assert!(
            config.socket_path.exists(),
            "a name not established to be dead is never unlinked"
        );
        drop(live);
    }

    /// F-004: a name that cannot be removed is a start-phase failure, not a clean exit. A
    /// directory stands in for a name the filesystem refuses to unlink.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_socket_name_that_cannot_be_removed_is_reported() {
        let dir = TempDir::new().expect("temporary directory");
        let path = dir.path().join("daemon.sock");
        std::fs::create_dir(&path).expect("create a name that is not a socket");
        match remove_socket(&path) {
            Err(DaemonError::Socket(message)) => {
                assert!(message.contains("could not be removed"), "{message}")
            }
            other => panic!("an unremovable name is a socket failure, not {other:?}"),
        }
        // An absent name is the intended end state.
        std::fs::remove_dir(&path).expect("clear the directory");
        remove_socket(&path).expect("an absent name is already removed");
    }

    /// F-002: a connection accepted and parked before its registration is owned by the drain. Its
    /// responder is tracked from the moment it exists, so the drain closes it; the task itself is
    /// then cancelled, so nothing inserts after the snapshot and no session survives the name.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_drain_ends_a_connection_parked_before_its_registration() {
        // The temporary directory is kept alive: the seed file the host reads lives in it.
        let Fixture {
            _dir,
            config,
            store,
        } = Fixture::open().await;
        let surface = expect_ok(
            MindcoreServe::new(store, false, |_| Ok(()))
                .surface(faces(&config))
                .await,
            "assemble the surface the connection serves",
        );
        let host = Arc::new(host(Arc::new(surface), &config).expect("build the host"));
        let listener = UnixListener::bind(&config.socket_path).expect("bind the listener");

        let (parked, mut park_signal) = tokio::sync::mpsc::unbounded_channel();
        let (resume, parked_resume) = tokio::sync::mpsc::unbounded_channel();
        *SETUP_PARK.lock().expect("setup seam lock") = Some(SetupPark {
            parked,
            resume: parked_resume,
        });

        let mut connections: JoinSet<()> = JoinSet::new();
        {
            let accepting = accept(listener, &host, &mut connections);
            tokio::pin!(accepting);
            // The dial completes against the listen backlog; the accept loop then owns the task.
            let _dial = UnixStream::connect(&config.socket_path)
                .await
                .expect("dial the daemon");
            tokio::select! {
                outcome = &mut accepting => panic!("the accept loop ended with {outcome:?}"),
                _ = park_signal.recv() => {}
            }
        }
        // The task is suspended between its tracked responder and its registration: it has not
        // registered a handler, and the drain still owns the session.
        let snapshot = std::mem::take(&mut *host.live.lock().expect("live lock"));
        assert_eq!(
            snapshot.len(),
            1,
            "a task still setting up has its responder tracked"
        );
        drain_sessions(&snapshot);
        connections.shutdown().await;
        assert_eq!(connections.len(), 0, "no accepted task outlives the drain");
        assert!(
            snapshot.iter().all(|(_, responder)| responder.state()
                == spoke_connect::remote::ConnectResponderState::Closed
                || responder.state() == spoke_connect::remote::ConnectResponderState::Disconnected),
            "every responder was told to close"
        );
        assert!(
            host.live.lock().expect("live lock").is_empty(),
            "no responder was registered after the snapshot"
        );
        drop(resume);
    }
}
