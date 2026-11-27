//! The serving phase: one surface, one listener, one accept loop and the drain.
//!
//! `main` hands over after the lock, and this phase runs the rest of the lifecycle order: the
//! store, the journal root, the assembled surface, the listener, the accept loop and — on
//! `SIGTERM` or `SIGINT` — the drain.
//!
//! The surface is assembled once and shared by every connection. Each accepted stream gets its
//! own transport and responder in a task of its own, every task is owned by one `JoinSet`, and
//! the live responders sit in one collection the drain walks.
//!
//! Only a socket established to be dead is ever replaced. A name occupied by anything else — a
//! regular file, a directory, a symlink, a device — is refused with its type, because nothing that
//! is not a socket is the daemon's to unlink; and a `BoundSocket` owns the name it bound until
//! `into_parts` hands the listener over, so a start that fails before that point takes the name
//! with it.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::ErrorKind;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
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
use crate::transport::{GatedTransport, UnixSocketTransport};
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
    /// The socket this daemon bound, so a removal can prove it is removing its own name.
    socket: SocketIdentity,
    /// The live responders the drain walks; a connection removes its own entry when it ends.
    live: Mutex<Vec<(usize, Arc<ConnectResponder>)>>,
}

/// The mode every socket this daemon binds carries: owner read/write, nobody else.
const SOCKET_MODE: u32 = 0o600;

/// The device and inode of the socket name this daemon created.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SocketIdentity {
    dev: u64,
    ino: u64,
}

impl SocketIdentity {
    /// The identity of whatever occupies `path`, following no symlink.
    fn of(metadata: &std::fs::Metadata) -> Self {
        Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
        }
    }
}

/// A bound socket name, owned until the accept loop takes the listener.
///
/// A start can fail after `bind` and before the accept loop exists — reading the seed, decoding a
/// peer key. Each of those is a `?` in `prepare`, and this guard makes the failure path unlink the
/// name it bound without a branch at every one of them. `into_parts` disarms it at the one point the
/// listener is handed over; the shutdown signals are installed before the bind, so they are not
/// part of this window.
#[derive(Debug)]
struct BoundSocket {
    /// `Some` until `into_parts`; `Option` so the listener can be moved out of a `Drop` type.
    listener: Option<UnixListener>,
    identity: SocketIdentity,
    path: PathBuf,
    armed: bool,
}

impl BoundSocket {
    /// Binds `path` and arms the guard with the name's identity, so a start that fails before
    /// handing the listener over leaves no socket behind.
    fn bind(path: &Path) -> Result<Self, DaemonError> {
        let fail = |fault: String| DaemonError::Socket(format!("{}: {fault}", path.display()));
        let listener = UnixListener::bind(path).map_err(|error| fail(error.to_string()))?;
        // Owner-only, not the umask's choice: who may hand this daemon a connection is part of the
        // process contract, not a property of the shell that started it.
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(SOCKET_MODE)).map_err(
            |error| {
                let _ = std::fs::remove_file(path);
                fail(format!("the socket mode could not be set: {error}"))
            },
        )?;
        // Taken after the bind, so it names the socket this process created.
        let identity = match bound_identity(path) {
            Ok(identity) => identity,
            Err(error) => {
                // The identity read failed, so this is the name this call just bound. The guard
                // does not exist yet, and `remove_if_own` could not check without the same read.
                //
                // shortcut: a read-only socket directory cannot be cleaned up by anyone, so when
                // the removal fails too the refusal names the leftover path instead of hiding it.
                return Err(match std::fs::remove_file(path) {
                    Ok(()) => fail(format!("the bound socket could not be inspected: {error}")),
                    Err(removal) => fail(format!(
                        "the bound socket could not be inspected ({error}) and the name could not \
                         be removed ({removal}): remove {} before the next start",
                        path.display()
                    )),
                });
            }
        };
        Ok(Self {
            listener: Some(listener),
            identity,
            path: path.to_path_buf(),
            armed: true,
        })
    }

    /// The identity of the bound name, for the drain's later conditional removal.
    fn identity(&self) -> SocketIdentity {
        self.identity
    }

    /// Unlinks the name this guard bound, reporting whether it could.
    ///
    /// The explicit cleanup a failed start calls: `Drop` cannot report, so it is only the last
    /// resort. Clears `armed` first, so a later `Drop` neither repeats the attempt nor reports a
    /// second, contradictory answer.
    fn cleanup(&mut self) -> Result<(), DaemonError> {
        if !self.armed {
            return Ok(());
        }
        self.armed = false;
        remove_if_own(&self.path, self.identity)
            .map_err(|fault| DaemonError::Socket(format!("{}: {fault}", self.path.display())))
    }

    /// Disarms the guard and hands the listener over: from here the name belongs to the caller.
    fn into_parts(mut self) -> (UnixListener, SocketIdentity) {
        self.armed = false;
        let listener = self.listener.take().expect("the guard holds its listener");
        (listener, self.identity)
    }
}

impl Drop for BoundSocket {
    /// Last resort only: a start that returned without calling `cleanup` or `into_parts` still
    /// leaves no name behind. `cleanup` disarms first, so the two never both unlink. Best-effort
    /// because `Drop` cannot report; a name that changed identity is left alone by `remove_if_own`
    /// exactly as the drain would leave it.
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

/// Runs the serving phase of the lifecycle order: store, journal, surface, listen, accept, drain.
///
/// `store_path` is the caller's one canonical resolution, the same path the lock was taken on; it is
/// opened here unchanged. The lock arrives held and is dropped last, after every live session has
/// been told to close.
pub async fn run(
    config: DaemonConfig,
    store_path: PathBuf,
    lock: StoreLock,
) -> Result<(), DaemonError> {
    let store = MindStore::open(&store_path)
        .await
        .map_err(|error| DaemonError::Store(format!("{}: {error}", store_path.display())))?;
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
    // Both signals are installed before the socket exists. From the moment the name is bound an
    // ordinary supervisor stop must reach the drain; a signal listener that cannot be installed is
    // its own start failure, reported as such.
    let (mut terminate, mut interrupt) = signals()?;
    // The guard owns the bound name from here, and the steps below are prepared without letting
    // `?` drop it: a failure is cleaned up explicitly, so the refusal can report what the removal
    // did instead of leaving `Drop` to swallow it.
    let mut bound = match listen(&config) {
        Ok(bound) => bound,
        Err(error) => {
            // No listener exists, but the store's pool is open: end its connections before the
            // start fails rather than cutting them when the runtime drops.
            pool.close().await;
            return Err(error);
        }
    };
    let host = match prepare(surface, &config, &mut bound).await {
        Ok(prepared) => prepared,
        Err(error) => {
            pool.close().await;
            return Err(cleanup_refusal(&mut bound, error));
        }
    };
    // The accept loop owns the listener from here; the guard has nothing left to undo.
    let (listener, _) = bound.into_parts();

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
    // Only the socket this daemon bound is removed; a name since replaced is refused and stays.
    let removed = remove_own_socket(&config.socket_path, host.socket);
    // Step 4: the store's connections finish rather than being cut when the runtime drops. Inline
    // so the pool's type stays the surface's business, not this module's.
    pool.close().await;
    drop(lock);
    serving.and(removed)
}

/// Both shutdown signal streams, installed before the socket exists.
///
/// A signal the process cannot listen for is its own start failure: without it no signal could reach
/// the drain, and the socket name would outlive a process killed some other way.
fn signals() -> Result<(tokio::signal::unix::Signal, tokio::signal::unix::Signal), DaemonError> {
    Ok((
        signal(SignalKind::terminate())
            .map_err(|error| DaemonError::Signal(format!("cannot listen for SIGTERM: {error}")))?,
        signal(SignalKind::interrupt())
            .map_err(|error| DaemonError::Signal(format!("cannot listen for SIGINT: {error}")))?,
    ))
}

/// The remaining steps between a bound name and a listener the accept loop can own: the seed read,
/// the allowlist and peer keys, and the host wiring.
///
/// It takes the guard by `&mut` and never removes the name itself: on failure the caller cleans up
/// explicitly, so the refusal can carry the removal's own outcome.
async fn prepare(
    surface: Arc<MindcoreSurface>,
    config: &DaemonConfig,
    bound: &mut BoundSocket,
) -> Result<Arc<Host>, DaemonError> {
    let seed = crate::config::read_seed(&config.identity.seed_file)?;
    let allowlist: Vec<String> = config.peers.keys().cloned().collect();
    let peer_keys = peer_keys(config)?;
    let host = Arc::new(Host {
        socket: bound.identity(),
        surface,
        seed,
        allowlist,
        peer_keys,
        invoke_timeout_ms: config.invoke.timeout_ms,
        live: Mutex::new(Vec::new()),
    });
    Ok(host)
}

/// Cleans up after a failed `prepare` and folds both faults into one refusal.
///
/// `Drop` cannot report, so the removal is explicit here: when it fails too the message names the
/// removal error and the leftover path, the same wording `bind` uses for the window inside itself.
fn cleanup_refusal(bound: &mut BoundSocket, error: DaemonError) -> DaemonError {
    match bound.cleanup() {
        Ok(()) => error,
        Err(cleanup) => DaemonError::Socket(format!(
            "{error} and the socket name could not be removed ({cleanup}): remove {} before the \
             next start",
            bound.path.display()
        )),
    }
}

/// Creates the journal root and makes every entry in its chain durable before the first
/// publication writes into it.
///
/// The walk is idempotent, not first-creation-only: every component from the nearest existing
/// ancestor down to the root has its parent synced unconditionally, including the components this
/// start found already there. A start that failed after creating a level but before syncing its
/// entry left that entry unproven, and nothing marks which one it was — so the next start redoes the
/// whole chain, and redoing an already-durable sync costs one `fsync`.
fn journal_root(dir: &Path) -> Result<(), DaemonError> {
    // Bottom-up: every level from the root up to the first one that exists, which is included.
    let mut chain: Vec<&Path> = Vec::new();
    let mut existing = None;
    let mut cursor = Some(dir);
    while let Some(level) = cursor {
        if level.exists() {
            // An existing component that is not a directory cannot be a journal root: accepting it
            // would publish into whatever the path really names.
            let metadata = std::fs::symlink_metadata(level)
                .map_err(|error| DaemonError::Journal(format!("{}: {error}", level.display())))?;
            if !metadata.is_dir() {
                return Err(DaemonError::Journal(format!(
                    "{}: exists and is not a directory",
                    level.display()
                )));
            }
            chain.push(level);
            existing = Some(chain.len() - 1);
            break;
        }
        chain.push(level);
        cursor = level
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty());
    }
    // Top-down: create what is missing, then sync every level's parent whether it was created now
    // or was already there.
    for (index, level) in chain.iter().enumerate().rev() {
        if Some(index) != existing {
            std::fs::create_dir(level)
                .map_err(|error| DaemonError::Journal(format!("{}: {error}", level.display())))?;
        }
        sync_parent_of(level)?;
    }
    Ok(())
}

/// Syncs the directory holding `level`'s entry, so that entry is durable.
///
/// A level with no parent is the filesystem root: no directory holds its entry, so there is nothing
/// to sync.
fn sync_parent_of(level: &Path) -> Result<(), DaemonError> {
    match level.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => sync_parent(parent),
        _ => Ok(()),
    }
}

/// Syncs `parent`, so the directory entry just created beneath it is durable.
///
/// A failed sync is a journal fault naming the directory: reporting a clean start on a chain that is
/// not yet on disk would promise durability the daemon does not have.
fn sync_parent(parent: &Path) -> Result<(), DaemonError> {
    #[cfg(test)]
    record_injected_sync(parent);
    #[cfg(test)]
    if take_injected_sync_failure(parent) {
        return Err(DaemonError::Journal(format!(
            "{}: injected sync fault",
            parent.display()
        )));
    }
    File::open(parent)
        .and_then(|parent| parent.sync_all())
        .map_err(|error| DaemonError::Journal(format!("{}: {error}", parent.display())))
}

/// A test seam: the next sync of this exact directory fails, and every sync attempt is counted. An
/// `fsync` fault cannot be built from a test's side, so this is the only way to pin that the failure
/// is propagated rather than ignored, and that a later start retries a sync it owes.
#[cfg(test)]
static INJECT_SYNC_FAILURE: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// How many times each directory has been asked to sync, so a test can see a retry.
#[cfg(test)]
static SYNC_ATTEMPTS: Mutex<Vec<(PathBuf, usize)>> = Mutex::new(Vec::new());

#[cfg(test)]
fn record_injected_sync(path: &Path) {
    let mut attempts = SYNC_ATTEMPTS.lock().expect("sync attempt lock");
    match attempts.iter_mut().find(|(seen, _)| seen == path) {
        Some((_, count)) => *count += 1,
        None => attempts.push((path.to_path_buf(), 1)),
    }
}

/// How many times `path` has been synced since the last [`clear_sync_attempts`].
#[cfg(test)]
fn sync_attempts(path: &Path) -> usize {
    SYNC_ATTEMPTS
        .lock()
        .expect("sync attempt lock")
        .iter()
        .find(|(seen, _)| seen == path)
        .map_or(0, |(_, count)| *count)
}

#[cfg(test)]
fn clear_sync_attempts() {
    SYNC_ATTEMPTS.lock().expect("sync attempt lock").clear();
}

#[cfg(test)]
fn push_injected_sync_failure(path: &Path) {
    INJECT_SYNC_FAILURE
        .lock()
        .expect("sync seam lock")
        .push(path.to_path_buf());
}

#[cfg(test)]
fn take_injected_sync_failure(path: &Path) -> bool {
    let mut armed = INJECT_SYNC_FAILURE.lock().expect("sync seam lock");
    match armed.iter().position(|armed_path| armed_path == path) {
        Some(index) => {
            armed.remove(index);
            true
        }
        None => false,
    }
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
    // The responder may read the dialer's hello before this task gets to register a single
    // handler. The gate withholds that first frame until registration is done, so a session
    // cannot be established into an empty handler registry.
    let gated = Arc::new(GatedTransport::new(
        Arc::clone(&transport) as Arc<dyn Transport>
    ));
    let options = ConnectResponderOptions {
        transport: Arc::clone(&gated) as Arc<dyn Transport>,
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
    // Registered: the dialer's hello may now be read and its tool invokes served.
    gated.open();
    transport.closed().await;
    responder.close();
    host.live
        .lock()
        .expect("live lock")
        .retain(|(live_id, _)| *live_id != id);
}

/// The identity of the socket `bind` just created at `path`, following no symlink.
///
/// Split out so the seam below can fail it: a real `lstat` failure cannot be interposed on, because
/// the bind and this read are adjacent and a parent made unsearchable fails the bind first.
fn bound_identity(path: &Path) -> std::io::Result<SocketIdentity> {
    #[cfg(test)]
    if let Some(failure) = take_injected_failure(path) {
        // The seam is the only moment between the bind and the read, so a test needs it to take
        // the directory's write permission before the fallback tries to unlink.
        if let Some(directory) = failure.close_directory {
            let _ = std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o500));
        }
        return Err(std::io::Error::other("injected identity read failure"));
    }
    std::fs::symlink_metadata(path).map(|metadata| SocketIdentity::of(&metadata))
}

/// A test seam armed by the regressions below: the next identity read of **this exact path** fails,
/// and it may first make `close_directory` read-only so the fallback's removal fails too.
///
/// Keyed to the path and consumed only on a match, so a bind in another test never takes it.
#[cfg(test)]
#[derive(Default)]
struct IdentityFailure {
    close_directory: Option<PathBuf>,
}

#[cfg(test)]
static INJECT_IDENTITY_FAILURE: Mutex<Vec<(PathBuf, IdentityFailure)>> = Mutex::new(Vec::new());

/// Arms the next identity read of `path` to fail.
#[cfg(test)]
fn push_injected_failure(path: &Path, failure: IdentityFailure) {
    INJECT_IDENTITY_FAILURE
        .lock()
        .expect("identity seam lock")
        .push((path.to_path_buf(), failure));
}

/// Removes the first armed fault whose path is `path`, leaving every other entry in place.
///
/// A queue rather than one slot, so two tests arming different paths run in parallel without
/// stealing each other's fault and without a serializing lock.
#[cfg(test)]
fn take_injected_failure(path: &Path) -> Option<IdentityFailure> {
    let mut armed = INJECT_IDENTITY_FAILURE.lock().expect("identity seam lock");
    let index = armed
        .iter()
        .position(|(armed_path, _)| armed_path == path)?;
    Some(armed.remove(index).1)
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

/// Tells every live session to close. `ConnectResponder::close` is synchronous and idempotent.
fn drain_sessions(live: &[(usize, Arc<ConnectResponder>)]) {
    for (_, responder) in live {
        responder.close();
    }
}

/// Removes the socket name — the signal that the daemon has stopped serving — but only while the
/// name is still the socket this daemon bound.
///
/// The drain gives the name only after every session has been told to close, and reports its
/// failure: a daemon that leaves the name behind while exiting 0 claims a clean stop it did not
/// reach. An absent name is the intended end state. A name replaced since the bind belongs to
/// whoever replaced it, and is refused rather than removed.
///
// shortcut: POSIX cannot unlink by inode, so a process running as the same user can replace the
// name between the identity check and `remove_file`. The daemon is not designed to resist a local
// same-user attacker — upgrade if that changes.
fn remove_own_socket(socket_path: &Path, owned: SocketIdentity) -> Result<(), DaemonError> {
    remove_if_own(socket_path, owned)
        .map_err(|fault| DaemonError::Socket(format!("{}: {fault}", socket_path.display())))
}

/// The conditional unlink both callers share: `NotFound` is the intended end state, and a name
/// that is no longer the socket `owned` names — a different file type, or a different device and
/// inode — is refused with nothing removed.
fn remove_if_own(socket_path: &Path, owned: SocketIdentity) -> Result<(), String> {
    let metadata = match std::fs::symlink_metadata(socket_path) {
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("the socket name could not be inspected: {error}")),
        Ok(metadata) => metadata,
    };
    if !metadata.file_type().is_socket() {
        return Err(format!(
            "the name is no longer this daemon's socket: it became {}",
            file_type_name(&metadata.file_type())
        ));
    }
    if SocketIdentity::of(&metadata) != owned {
        return Err(
            "the name is no longer this daemon's socket: it was replaced by another socket"
                .to_string(),
        );
    }
    std::fs::remove_file(socket_path)
        .map_err(|error| format!("the socket name could not be removed: {error}"))
}

/// Binds `socket_path`, removing the name only when it is a socket established to be dead, and
/// answers a guard that unlinks what it bound unless the caller disarms it.
///
/// Nothing that is not a socket is ever unlinked, so a name the daemon does not own — the store,
/// the seed file, any other file, a directory, a symlink or a device — is refused rather than
/// destroyed. The one removal on a start re-checks the identity it just took, so a socket swapped
/// in between the probe and the unlink survives.
fn listen(config: &DaemonConfig) -> Result<BoundSocket, DaemonError> {
    let socket_path = &config.socket_path;
    let fail = |fault: String| DaemonError::Socket(format!("{}: {fault}", socket_path.display()));

    let metadata = match std::fs::symlink_metadata(socket_path) {
        // A free name is the ordinary start.
        Err(error) if error.kind() == ErrorKind::NotFound => return BoundSocket::bind(socket_path),
        Err(error) => return Err(fail(error.to_string())),
        Ok(metadata) => metadata,
    };
    let file_type = metadata.file_type();
    if !file_type.is_socket() {
        return Err(fail(format!(
            "the name is occupied by {}, which this daemon does not remove; move it aside first",
            file_type_name(&file_type)
        )));
    }
    // A socket is the one kind this daemon may remove, and only when its own probe says no
    // listener answers.
    let stale = SocketIdentity::of(&metadata);
    match std::os::unix::net::UnixStream::connect(socket_path) {
        Ok(_) => return Err(fail("a live listener already owns the name".to_string())),
        Err(error) if error.kind() == ErrorKind::ConnectionRefused => {}
        Err(error) => {
            return Err(fail(format!(
                "the name could not be probed, so its liveness is unknown: {error}"
            )))
        }
    }
    // Bound to the identity taken before the probe: a socket swapped in since is refused and
    // stays, because it is not the one this start established to be dead.
    remove_if_own(socket_path, stale).map_err(fail)?;
    BoundSocket::bind(socket_path)
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
        let bound = listen(&config).expect("replace a stale socket");
        let (listener, identity) = bound.into_parts();
        assert_eq!(
            identity,
            SocketIdentity::of(&std::fs::symlink_metadata(&config.socket_path).expect("lstat")),
            "the boundary identity names the socket just bound"
        );
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

    /// A regular file where the journal root belongs is a journal fault naming it, not a success:
    /// publishing into whatever the path really names would be worse than refusing to start.
    #[test]
    fn a_journal_component_that_is_not_a_directory_is_refused() {
        let dir = TempDir::new().expect("temporary directory");
        let file = dir.path().join("journal");
        std::fs::write(&file, b"not a directory").expect("write the file");
        match journal_root(&file) {
            Err(DaemonError::Journal(message)) => {
                assert!(message.contains("not a directory"), "{message}");
                assert!(
                    message.contains(&file.display().to_string()),
                    "the refusal names it: {message}"
                );
            }
            other => panic!("a file at the journal root is a journal fault, not {other:?}"),
        }
        assert_eq!(
            std::fs::read(&file).expect("read the file"),
            b"not a directory",
            "the file is untouched"
        );
    }

    /// An ancestor that is a file is refused too, not just the leaf.
    #[test]
    fn a_journal_ancestor_that_is_not_a_directory_is_refused() {
        let dir = TempDir::new().expect("temporary directory");
        let file = dir.path().join("new-parent");
        std::fs::write(&file, b"file").expect("write the file");
        match journal_root(&file.join("journal")) {
            Err(DaemonError::Journal(message)) => {
                assert!(message.contains("not a directory"), "{message}");
            }
            other => panic!("a file above the journal root is a journal fault, not {other:?}"),
        }
    }

    /// The chain sync is idempotent across restarts, not first-creation-only. A start that created a
    /// level and then failed before syncing its entry leaves nothing marking which entry is unproven,
    /// so the next start must re-sync every component — including the ones it found already there.
    #[test]
    fn a_restart_retries_the_sync_of_an_already_existing_level() {
        let dir = TempDir::new().expect("temporary directory");
        let root = dir.path().join("journal");
        let parent = dir.path().to_path_buf();

        // First start: the leaf is created, then its parent's sync fails.
        clear_sync_attempts();
        push_injected_sync_failure(&parent);
        match journal_root(&root) {
            Err(DaemonError::Journal(message)) => {
                assert!(message.contains("injected sync fault"), "{message}")
            }
            other => panic!("the armed first start must fail, not {other:?}"),
        }
        assert!(
            root.is_dir(),
            "the level was created before its sync failed"
        );
        assert_eq!(
            sync_attempts(&parent),
            1,
            "the first start attempted it once"
        );

        // Second start, seam cleared: the level now exists, and its parent is synced again.
        clear_sync_attempts();
        journal_root(&root).expect("the retry accepts the directory");
        assert_eq!(
            sync_attempts(&parent),
            1,
            "the restart re-synced the already-existing level's parent"
        );
    }

    /// A sync that fails is a journal fault, not a success: the daemon must not report a clean start
    /// on a chain that is not on disk yet.
    #[test]
    fn a_failed_parent_sync_is_a_journal_fault() {
        let dir = TempDir::new().expect("temporary directory");
        let root = dir.path().join("new-parent").join("journal");
        let armed_parent = dir.path().join("new-parent");
        push_injected_sync_failure(&armed_parent);
        match journal_root(&root) {
            Err(DaemonError::Journal(message)) => {
                assert!(message.contains("injected sync fault"), "{message}");
                assert!(
                    message.contains(&armed_parent.display().to_string()),
                    "the refusal names the directory that failed to sync: {message}"
                );
            }
            other => panic!("a failed sync is a journal fault, not {other:?}"),
        }
    }

    /// Two missing levels: each newly created entry must be linked in its own parent, not just the
    /// leaf inside a directory that is itself not yet durable.
    #[test]
    fn every_journal_level_is_created_and_synced() {
        let dir = TempDir::new().expect("temporary directory");
        let root = dir.path().join("new-parent").join("journal");
        journal_root(&root).expect("create two levels");
        assert!(
            dir.path().join("new-parent").is_dir(),
            "the ancestor exists"
        );
        assert!(root.is_dir(), "the leaf exists");
        // A second call finds every level and does nothing, which must not be an error.
        journal_root(&root).expect("an existing root is accepted");
    }

    /// The socket is owner-only whatever the process umask is: who may connect is part of the
    /// contract, not a property of the shell that started the daemon.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_bound_socket_is_owner_only() {
        let dir = TempDir::new().expect("temporary directory");
        let path = dir.path().join("daemon.sock");
        let bound = BoundSocket::bind(&path).expect("bind the socket");
        let mode = std::fs::metadata(&path)
            .expect("stat the socket")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, SOCKET_MODE, "the socket is {mode:o}, not owner-only");
        drop(bound);
    }

    /// Both signal streams exist before the socket does: a supervisor stop arriving the moment the
    /// name appears must reach the drain, not the default action.
    #[tokio::test(flavor = "multi_thread")]
    async fn both_signals_are_installed_before_the_socket_exists() {
        let dir = TempDir::new().expect("temporary directory");
        let path = dir.path().join("daemon.sock");
        let (terminate, interrupt) = signals().expect("install both signals");
        // The ordering the start relies on: signals first, then the name.
        assert!(!path.exists(), "no socket exists before the bind");
        let bound = BoundSocket::bind(&path).expect("bind after the signals");
        assert!(path.exists(), "the name exists only after both handlers do");
        drop(terminate);
        drop(interrupt);
        drop(bound);
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
        let store_path = crate::lock::canonical_store_path(&collided.store_path)
            .expect("resolve the store path");
        let lock = StoreLock::acquire(&store_path).expect("take the lock");
        match run(collided, store_path, lock).await {
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

    /// A name that cannot be removed is a start-phase failure, not a clean exit. A directory
    /// stands in for a name the filesystem refuses to unlink.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_socket_name_that_cannot_be_removed_is_reported() {
        let dir = TempDir::new().expect("temporary directory");
        let path = dir.path().join("daemon.sock");
        std::fs::create_dir(&path).expect("create a name that is not a socket");
        // No identity is recorded for a name this daemon never bound, so a directory there is a
        // type refusal before any unlink is attempted.
        let foreign = SocketIdentity { dev: 0, ino: 0 };
        match remove_own_socket(&path, foreign) {
            Err(DaemonError::Socket(message)) => {
                assert!(
                    message.contains("no longer this daemon's socket"),
                    "{message}"
                )
            }
            other => panic!("an unremovable name is a socket failure, not {other:?}"),
        }
        assert!(path.is_dir(), "the directory survives");
        // An absent name is the intended end state.
        std::fs::remove_dir(&path).expect("clear the directory");
        remove_own_socket(&path, foreign).expect("an absent name is already removed");
    }

    /// The guard cannot cover the window inside `bind` itself: if the identity read fails after a
    /// successful bind, `bind` unlinks the name before answering the error, so the start leaves no
    /// socket behind either way.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_identity_read_failure_inside_bind_leaves_no_socket() {
        let dir = TempDir::new().expect("temporary directory");
        let path = dir.path().join("daemon.sock");
        push_injected_failure(&path, IdentityFailure::default());
        match BoundSocket::bind(&path) {
            Err(DaemonError::Socket(message)) => {
                assert!(message.contains("could not be inspected"), "{message}")
            }
            other => panic!("an unreadable identity is a socket refusal, not {other:?}"),
        }
        assert!(
            !path.exists(),
            "bind removed the name it could not identify"
        );
    }

    /// An unwritable socket directory is an environment property, not a defect: nobody can remove
    /// the name. The refusal must then say both faults and name the leftover, rather than report a
    /// removal that did not happen.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unremovable_leftover_is_named_in_the_both_fail_refusal() {
        // The mode bits do not constrain uid 0, so the environment cannot be built as root.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let dir = TempDir::new().expect("temporary directory");
        let parent = dir.path().join("socket-dir");
        std::fs::create_dir(&parent).expect("create the socket directory");
        let path = parent.join("daemon.sock");
        // The bind needs a writable directory; the seam below closes it before the fallback's
        // unlink, which is exactly the environment the refusal describes.
        let seam = IdentityFailure {
            close_directory: Some(parent.clone()),
        };
        push_injected_failure(&path, seam);
        let outcome = BoundSocket::bind(&path);
        // Restore before asserting, so a failure does not leave an unwritable directory behind.
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700))
            .expect("restore the directory");
        match outcome {
            Err(DaemonError::Socket(message)) => {
                assert!(message.contains("could not be inspected"), "{message}");
                assert!(message.contains("could not be removed"), "{message}");
                assert!(
                    message.contains(&path.display().to_string()),
                    "the refusal names the leftover: {message}"
                );
                assert!(
                    message.contains("before the next start"),
                    "the refusal says what to do: {message}"
                );
            }
            other => panic!("an unremovable leftover is a socket refusal, not {other:?}"),
        }
        assert!(
            path.exists(),
            "the leftover is still there, as the refusal says"
        );
    }

    /// A post-bind step that fails in a read-only socket directory: nobody can remove the name, so
    /// the refusal must name both causes and the path, and the name must still be there.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_prepare_in_a_read_only_directory_names_both_causes() {
        // The mode bits do not constrain uid 0, so the environment cannot be built as root.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let dir = TempDir::new().expect("temporary directory");
        let parent = dir.path().join("socket-dir");
        std::fs::create_dir(&parent).expect("create the socket directory");
        let config = DaemonConfig {
            socket_path: parent.join("daemon.sock"),
            // Not 64 hex digits: `prepare`'s peer-key decode is the step that fails.
            peers: BTreeMap::from([(
                "peer-a".to_string(),
                PeerConfig {
                    public_key: "nonsense".to_string(),
                    principal: "prn_a".to_string(),
                },
            )]),
            ..config(dir.path())
        };
        let store = MindStore::open(&config.store_path)
            .await
            .expect("open the store");
        let surface = expect_ok(
            MindcoreServe::new(store, false, |_| Ok(()))
                .surface(faces(&config))
                .await,
            "assemble the surface the start serves",
        );

        let mut bound = listen(&config).expect("bind the socket");
        // Closed after the bind: the bind needs a writable directory, the cleanup does not get one.
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o500))
            .expect("make the directory read-only");
        let failure = match prepare(Arc::new(surface), &config, &mut bound).await {
            Err(failure) => failure,
            Ok(_) => panic!("an undecodable peer key fails the prepare"),
        };
        let refusal = cleanup_refusal(&mut bound, failure);
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700))
            .expect("restore the directory");

        let message = refusal.to_string();
        assert!(message.contains("public_key"), "{message}");
        assert!(message.contains("could not be removed"), "{message}");
        assert!(
            message.contains(&config.socket_path.display().to_string()),
            "the refusal names the leftover: {message}"
        );
        assert!(message.contains("before the next start"), "{message}");
        assert!(
            config.socket_path.exists(),
            "the leftover is still there, as the refusal says"
        );
    }

    /// A failure between `bind` and the hand-over leaves no socket behind: the guard unlinks the
    /// name it bound. `config::load` validates peer keys, so a directly constructed config is the
    /// only way to fail there — exactly the injected failure this covers.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_start_that_fails_after_the_bind_leaves_no_socket() {
        let dir = TempDir::new().expect("temporary directory");
        let config = DaemonConfig {
            peers: BTreeMap::from([(
                "peer-a".to_string(),
                PeerConfig {
                    // Not 64 hex digits: `peer_keys` refuses it after the socket is bound.
                    public_key: "nonsense".to_string(),
                    principal: "prn_a".to_string(),
                },
            )]),
            ..config(dir.path())
        };
        let store_path =
            crate::lock::canonical_store_path(&config.store_path).expect("resolve the store path");
        let lock = StoreLock::acquire(&store_path).expect("take the lock");
        match run(config.clone(), store_path, lock).await {
            Err(DaemonError::Config(message)) => {
                assert!(message.contains("public_key"), "{message}")
            }
            Err(other) => panic!("a bad peer key is a config refusal, not {other:?}"),
            Ok(()) => panic!("the start must not serve with an undecodable peer key"),
        }
        assert!(
            !config.socket_path.exists(),
            "the guard removed the name bound before the failure"
        );
    }

    /// The registration window is upstream's, not this crate's: `connect_responder` spawns its own
    /// task before returning and offers no paused start, so a dialer can reach a handler lookup
    /// first. This pins the answer is the fail-closed deny and not a hang — the bounded wait is the
    /// assertion, because a request that never resolves fails the test on the budget.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_tool_invoke_before_registration_answers_the_dispatch_deny() {
        use spoke_connect::remote::{
            connect_responder, loopback_transport_pair, ConnectResponderOptions, RemoteIdentity,
        };

        let pair = loopback_transport_pair();
        let (server, client) = (pair.server, pair.client);
        let responder = connect_responder(ConnectResponderOptions {
            transport: Arc::new(server),
            identity: RemoteIdentity { seed: [0x33u8; 32] },
            manifest: serde_json::from_value(serde_json::json!({
                "schema_version": 1,
                "host_id": "registration-window",
                "roles": ["data-store"],
                "capabilities": ["spoke-knowledge"],
                "namespaces": [],
                "extensions": {},
            }))
            .expect("the manifest matches the schema"),
            allowlist: Vec::new(),
            peer_keys: HashMap::new(),
            ports: None,
            invoke_timeout_ms: Some(2_000),
        })
        .await;
        // Deliberately no `register` call: this is the window the daemon closes immediately.
        let answered = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            responder.invoke_tool("tools.mindcore.recall_context", serde_json::json!({})),
        )
        .await
        .expect("a pre-registration invoke must answer, not hang");
        match answered {
            SpokeResult::Reject(reject) => {
                // The dialer's own session is not established here, so either the dispatch deny or
                // a session refusal is a fail-closed answer; what must not happen is silence.
                assert!(
                    matches!(
                        reject.code,
                        SpokeRejectCode::CapabilityPortMissing
                            | SpokeRejectCode::NotAuthorized
                            | SpokeRejectCode::InternalError
                    ),
                    "a pre-registration invoke answers a refuse: {reject:?}"
                );
            }
            SpokeResult::Ok(_) => panic!("nothing is registered, so nothing can be served"),
        }
        drop(client);
        responder.close();
    }

    /// A name replaced by a regular file after the bind belongs to whoever replaced it. The drain
    /// refuses to remove it and reports the refusal, so a clean-exit claim is never made.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_drain_refuses_a_socket_name_replaced_by_a_regular_file() {
        let dir = TempDir::new().expect("temporary directory");
        let config = config(dir.path());
        let (listener, bound) = listen(&config).expect("bind the socket").into_parts();
        // The listener still owns the socket; swap the name anyway, as a same-user process could.
        std::fs::remove_file(&config.socket_path).expect("unlink the socket");
        let bytes = b"the replacement's own bytes";
        std::fs::write(&config.socket_path, bytes).expect("write the replacement");
        match remove_own_socket(&config.socket_path, bound) {
            Err(DaemonError::Socket(message)) => {
                assert!(message.contains("became a regular file"), "{message}")
            }
            other => panic!("a replaced name is refused, not {other:?}"),
        }
        assert_eq!(
            std::fs::read(&config.socket_path).expect("read the replacement"),
            bytes,
            "the replacement survives the drain"
        );
        drop(listener);
    }

    /// Stale recovery binds its removal to the identity it probed: a socket swapped in between the
    /// probe and the unlink is a different socket, so it is refused and survives.
    #[tokio::test(flavor = "multi_thread")]
    async fn stale_recovery_refuses_a_socket_swapped_before_removal() {
        let dir = TempDir::new().expect("temporary directory");
        let config = config(dir.path());
        let stale = UnixListener::bind(&config.socket_path).expect("leave a stale socket");
        let probed = SocketIdentity::of(
            &std::fs::symlink_metadata(&config.socket_path).expect("lstat the stale socket"),
        );
        drop(stale);
        // A dropped listener leaves its name; the swap is exactly that name taken over.
        std::fs::remove_file(&config.socket_path).expect("clear the stale name");
        let replacement = UnixListener::bind(&config.socket_path).expect("swap in another socket");
        let swapped = SocketIdentity::of(
            &std::fs::symlink_metadata(&config.socket_path).expect("lstat the replacement"),
        );
        assert_ne!(probed, swapped, "the replacement is a different inode");
        match remove_if_own(&config.socket_path, probed) {
            Err(message) => assert!(message.contains("replaced by another socket"), "{message}"),
            Ok(()) => panic!("a swapped socket must not be removed"),
        }
        assert!(
            config.socket_path.exists(),
            "the swapped-in socket survives the refused removal"
        );
        drop(replacement);
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
        let listener = UnixListener::bind(&config.socket_path).expect("bind the listener");
        let socket =
            SocketIdentity::of(&std::fs::symlink_metadata(&config.socket_path).expect("lstat"));
        // The same wiring `prepare` builds, without the signals this test never awaits.
        let host = Arc::new(Host {
            socket,
            surface: Arc::new(surface),
            seed: crate::config::read_seed(&config.identity.seed_file).expect("read the seed"),
            allowlist: config.peers.keys().cloned().collect(),
            peer_keys: peer_keys(&config).expect("decode the peer keys"),
            invoke_timeout_ms: config.invoke.timeout_ms,
            live: Mutex::new(Vec::new()),
        });

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
