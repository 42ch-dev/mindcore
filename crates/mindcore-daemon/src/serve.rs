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
    /// Test seam: parks this host's first connection before its registration, so a drain can be
    /// driven while a session is still setting up. Per host, not global: the connection the
    /// register-rejection test drives belongs to a different host and must not be the one parked.
    #[cfg(test)]
    park_setup: Option<tokio::sync::mpsc::UnboundedSender<()>>,
    /// Test seam: makes `Surface::register` answer `Reject` for this host only.
    ///
    /// A refused registration is not constructible through the public API — `derive_manifest`
    /// validates every descriptor before assembly — so the rejection branch is reachable only
    /// through this field, and keying it to one `Host` keeps it out of every other test.
    #[cfg(test)]
    reject_registration: bool,
}

/// A test seam: makes the next probe of this exact path pending, so the timeout branch is reachable.
///
/// Keyed to the path and consumed only on a match. A bare flag was wrong: the probe tests run in
/// parallel, and a global one-shot let one test's arming be taken by another's dial — which made
/// both this test and the live-listener refusal fail intermittently.
#[cfg(test)]
static PENDING_PROBE: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);

#[cfg(test)]
fn take_pending_probe(path: &Path) -> bool {
    let mut armed = PENDING_PROBE.lock().expect("pending probe lock");
    match armed.as_ref() {
        Some(armed_path) if armed_path == path => {
            *armed = None;
            true
        }
        _ => false,
    }
}

/// What a liveness probe established about the name.
#[derive(Debug)]
enum Probe {
    /// A listener answered the dial: something is serving.
    Live,
    /// The dial was refused: nothing is listening, so the name is a leftover.
    Dead,
    /// The dial neither answered nor was refused: liveness stays unknown.
    Inconclusive(String),
}

/// Dials `path` once, bounded, and reports what that established.
///
/// The bound matters: a listener that accepts into a full backlog and never dequeues would
/// otherwise hold a start indefinitely, and a dial that has not answered says nothing either way.
async fn probe_liveness(path: &Path, budget: std::time::Duration) -> Probe {
    #[cfg(test)]
    if take_pending_probe(path) {
        return match tokio::time::timeout(budget, std::future::pending::<()>()).await {
            Ok(()) => unreachable!("a pending future never resolves"),
            Err(_) => Probe::Inconclusive(format!(
                "the probe did not answer within {}s, so its liveness is unknown",
                budget.as_secs()
            )),
        };
    }
    match tokio::time::timeout(budget, UnixStream::connect(path)).await {
        Ok(Ok(_)) => Probe::Live,
        Ok(Err(error))
            if matches!(
                error.kind(),
                ErrorKind::ConnectionRefused | ErrorKind::NotFound
            ) =>
        {
            Probe::Dead
        }
        Ok(Err(error)) => Probe::Inconclusive(format!(
            "the name could not be probed, so its liveness is unknown: {error}"
        )),
        Err(_) => Probe::Inconclusive(format!(
            "the probe did not answer within {}s, so its liveness is unknown",
            budget.as_secs()
        )),
    }
}

/// How long the liveness probe may take before its answer stops meaning anything.
const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// The mode every socket this daemon binds carries: owner read/write, nobody else.
const SOCKET_MODE: u32 = 0o600;

/// The mode every journal directory this daemon creates carries: owner only.
const JOURNAL_DIR_MODE: u32 = 0o700;

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
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(SOCKET_MODE)).map_err(
            |error| {
                let _ = std::fs::remove_file(path);
                fail(format!("the socket mode could not be set: {error}"))
            },
        )?;
        let identity = match bound_identity(path) {
            Ok(identity) => identity,
            Err(error) => {
                // The identity read failed, so this is the name this call just bound. The guard
                // does not exist yet, and `remove_if_own` could not check without the same read.
                //
                // shortcut: a read-only socket directory cannot be cleaned up by anyone, so when
                // the removal fails too the refusal names the leftover path instead of hiding it.
                // upgrade: when the bind path can guarantee a writable directory.
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
    async fn cleanup(&mut self) -> Result<(), DaemonError> {
        if !self.armed {
            return Ok(());
        }
        self.armed = false;
        remove_if_own(&self.path, self.identity)
            .await
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
        if !self.armed {
            return;
        }
        self.armed = false;
        let _ = remove_if_own_blocking(&self.path, self.identity);
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
    // `MindcoreServe` owns the store, so the pool handle is taken first: it is the reachable route to
    // the awaited close the drain order names, and every early failure below closes it rather than
    // cutting the connections when the runtime drops.
    let pool = store.pool();
    if let Err(error) = journal_root(&config.journal.dir).await {
        pool.close().await;
        return Err(error);
    }
    let surface = match MindcoreServe::new(
        store,
        false,
        journal_dir(config.journal.dir.clone()),
    )
    .surface(faces(&config))
    .await
    {
        SpokeResult::Ok(surface) => Arc::new(surface),
        SpokeResult::Reject(reject) => {
            pool.close().await;
            return Err(DaemonError::Surface(format!(
                "{}: {}",
                reject.code, reject.message
            )));
        }
    };
    let (mut terminate, mut interrupt) = match signals() {
        Ok(signals) => signals,
        Err(error) => {
            pool.close().await;
            return Err(error);
        }
    };
    let mut bound = match listen(&config).await {
        Ok(bound) => bound,
        Err(error) => {
            pool.close().await;
            return Err(error);
        }
    };
    let host = match prepare(surface, &config, &mut bound).await {
        Ok(prepared) => prepared,
        Err(error) => {
            pool.close().await;
            return Err(cleanup_refusal(&mut bound, error).await);
        }
    };
    let (listener, _) = bound.into_parts();

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
    let removed = remove_own_socket(&config.socket_path, host.socket).await;
    pool.close().await;
    drop(lock);
    settle(serving, removed, &config.socket_path)
}

/// The drain's last word: both faults travel.
///
/// `Result::and` would report only the accept fault and drop the removal's, leaving an operator who
/// sees a non-zero exit with no idea the socket name is still there.
fn settle(
    serving: Result<(), DaemonError>,
    removed: Result<(), DaemonError>,
    socket_path: &Path,
) -> Result<(), DaemonError> {
    match (serving, removed) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(serving), Err(removed)) => Err(DaemonError::Socket(format!(
            "{serving} and the socket name could not be removed ({removed}): remove {} before the \
             next start",
            socket_path.display()
        ))),
    }
}

/// Installs the surface's handlers, or answers why not.
///
/// One call so the start's ordering has a single site: the connection task passes this result to the
/// rejection branch, which must run before the responder is closed.
fn registration(host: &Host, responder: &Arc<ConnectResponder>) -> SpokeResult<()> {
    #[cfg(test)]
    if host.reject_registration {
        return spoke_reject(
            SpokeRejectCode::InternalError,
            "injected registration failure",
            None,
        );
    }
    host.surface.register(responder)
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
        #[cfg(test)]
        park_setup: None,
        #[cfg(test)]
        reject_registration: false,
    });
    Ok(host)
}

/// Cleans up after a failed `prepare` and folds both faults into one refusal.
///
/// `Drop` cannot report, so the removal is explicit here: when it fails too the message names the
/// removal error and the leftover path, the same wording `bind` uses for the window inside itself.
async fn cleanup_refusal(bound: &mut BoundSocket, error: DaemonError) -> DaemonError {
    match bound.cleanup().await {
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
async fn journal_root(dir: &Path) -> Result<(), DaemonError> {
    let mut chain: Vec<&Path> = Vec::new();
    let mut existing = None;
    let mut cursor = Some(dir);
    while let Some(level) = cursor {
        if level.exists() {
            // The resolved type decides: a symlink to a directory is a directory, and a journal root
            // reached through one is the operator's arrangement, not a fault. `metadata` follows the
            // link, so a link to a file or a FIFO still fails the gate below.
            // A level that cannot be inspected is a journal fault, wherever in the chain it sits: the
            // walk cannot tell an ancestor from the level beneath the root, and stopping early would
            // skip the flush this start owes — which is the defect this rule exists to prevent.
            let metadata = std::fs::metadata(level)
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
    let mut synced: Vec<PathBuf> = Vec::new();
    for (index, level) in chain.iter().enumerate().rev() {
        let created = created_at(index, existing);
        if created {
            let path = level.to_path_buf();
            tokio::task::spawn_blocking(move || {
                let mut builder = std::fs::DirBuilder::new();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::DirBuilderExt;
                    builder.mode(JOURNAL_DIR_MODE);
                }
                builder
                    .create(&path)
                    .map_err(|error| DaemonError::Journal(format!("{}: {error}", path.display())))
            })
            .await
            .map_err(|error| DaemonError::Journal(format!("{}: {error}", level.display())))??;
        }

        let Some(parent) = holding_directory(level) else {
            continue;
        };
        if created {
            forget_synced(parent, &mut synced);
        }
        sync_once(parent, &mut synced).await?;
    }
    // A symlink in the chain means the journal really lives somewhere else. The chain walk flushed
    // the entry that names the link; the directory that really holds the root has its own entry, in
    // its own parent, which nothing in that walk names — so that one entry is flushed too.
    //
    // Exactly one extra entry, never the ancestors above it: a root at `/srv/link/journal` resolving to
    // `/data/journal` costs the chain's entries plus `/data` here, and stops. A plain path resolves to
    // itself, so its holding directory is already in `synced` and this adds nothing — the walk stays
    // one flush per level, and never touches a directory above the chain.
    let resolved = std::fs::canonicalize(dir).map_err(|error| {
        DaemonError::Journal(format!(
            "{}: the journal root cannot be resolved after it was created: {error}",
            dir.display()
        ))
    })?;
    // The root's own entry, so a failure here is the root's: fatal, unlike an ancestor's.
    if resolved != dir {
        sync_level_entry(&resolved, &mut synced).await?;
    }
    Ok(())
}

/// Flushes the directory holding `level`'s entry, once per resolved directory.
///
/// A level's own name lives in its parent, so creating or discovering `level` dirties that parent.
/// The filesystem root has no parent and no directory holds its entry, so `holding_directory` answers
/// `None` there and this does nothing.
async fn sync_level_entry(level: &Path, synced: &mut Vec<PathBuf>) -> Result<(), DaemonError> {
    let Some(parent) = holding_directory(level) else {
        return Ok(());
    };
    sync_once(parent, synced).await
}

/// Drops `parent`'s key from the flush ledger, so the next [`sync_once`] flushes it again.
///
/// Used after creating a level: the holding directory was possibly flushed already, and creating the
/// level dirtied it a second time. Keyed the same way `sync_once` keys, so a relative and a resolved
/// spelling of one directory both match.
fn forget_synced(parent: &Path, synced: &mut Vec<PathBuf>) {
    let key = std::fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
    synced.retain(|seen| *seen != key);
}

/// [`sync_parent`], skipped for a directory this call has already flushed.
///
/// The key is the directory's resolved path, not the spelling it arrived as: a relative root's
/// configured walk names the current directory `.` while the resolved walk names the same directory
/// absolutely, and both must count as one flush. A directory that cannot be resolved is keyed by its
/// spelling, so it still gets exactly one.
async fn sync_once(parent: &Path, synced: &mut Vec<PathBuf>) -> Result<(), DaemonError> {
    let key = std::fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
    if synced.contains(&key) {
        return Ok(());
    }
    synced.push(key);
    sync_parent(parent).await
}

/// Whether `chain[index]` was created by this start: everything below the first pre-existing level
/// is, and that level and everything above it is not. Nothing pre-existing means every level is ours.
fn created_at(index: usize, existing: Option<usize>) -> bool {
    match existing {
        None => true,
        Some(first_existing) => index < first_existing,
    }
}

/// The directory holding `level`'s entry; `None` at the filesystem root, which no directory holds.
fn holding_directory(level: &Path) -> Option<&Path> {
    match level.parent() {
        None => None,
        Some(parent) if parent.as_os_str().is_empty() => Some(Path::new(".")),
        Some(parent) => Some(parent),
    }
}

/// Syncs the directory holding `level`'s entry, so that entry is durable.
///
/// Only `None` means the filesystem root, where no directory holds the entry and there is nothing to
/// sync. `Some("")` is not that: it is what `Path::new("journal").parent()` answers, and it means the
/// **current directory** — `Path::new("").parent()` is the `None` case — so a single-segment relative
/// journal root syncs `"."` rather than skipping its own entry.
/// Flushes one directory: `fsync` on `parent`, so every entry it holds is durable.
///
/// This is the syscall itself; which directories deserve one is [`sync_level_entry`]'s and
/// [`sync_once`]'s decision. A failure is a journal fault naming the directory — the caller has
/// already established that the directory holds an entry this start is responsible for, so a
/// clean-start report without it would promise durability the daemon does not have.
async fn sync_parent(parent: &Path) -> Result<(), DaemonError> {
    #[cfg(test)]
    record_injected_sync(parent);
    #[cfg(test)]
    if take_injected_sync_failure(parent) {
        return Err(DaemonError::Journal(format!(
            "{}: injected sync fault",
            parent.display()
        )));
    }
    let path = parent.to_path_buf();
    let fault = tokio::task::spawn_blocking(move || {
        File::open(&path)
            .and_then(|file| file.sync_all())
            .map_err(|error| DaemonError::Journal(format!("{}: {error}", path.display())))
    })
    .await
    .map_err(|error| DaemonError::Journal(format!("{}: {error}", parent.display())))?;
    fault
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
    let key = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let mut attempts = SYNC_ATTEMPTS.lock().expect("sync attempt lock");
    match attempts
        .iter_mut()
        .find(|(seen, _)| std::fs::canonicalize(seen).unwrap_or_else(|_| seen.clone()) == key)
    {
        Some((_, count)) => *count += 1,
        None => attempts.push((key, 1)),
    }
}

/// How many times `path` has been synced since the last [`clear_sync_attempts`] for it.
///
/// Counted by resolved spelling: the code syncs whichever spelling reached it, and on macOS `/var`
/// canonicalizes to `/private/var`, so comparing spellings would mis-count a directory the walk
/// really flushed.
#[cfg(test)]
fn sync_attempts(path: &Path) -> usize {
    let key = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    SYNC_ATTEMPTS
        .lock()
        .expect("sync attempt lock")
        .iter()
        .filter(|(seen, _)| std::fs::canonicalize(seen).unwrap_or_else(|_| seen.clone()) == key)
        .map(|(_, count)| *count)
        .sum()
}

/// Forgets the attempts recorded for `path` only: the seam is one global, so a global wipe would
/// race with another test's count.
#[cfg(test)]
/// Resets the count for `path` to zero, keeping the row.
///
/// The contract the tests rely on: after `clear`, one flush reads as `1`, not as an absent row. That
/// is why this zeroes rather than removes — a removed row and a never-counted row would be the same
/// reading, and the retry tests could not tell one flush from none.
fn clear_sync_attempts(path: &Path) {
    let key = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let mut attempts = SYNC_ATTEMPTS.lock().expect("sync attempt lock");
    match attempts
        .iter_mut()
        .find(|(seen, _)| std::fs::canonicalize(seen).unwrap_or_else(|_| seen.clone()) == key)
    {
        Some((_, count)) => *count = 0,
        None => attempts.push((key, 0)),
    }
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
    let key = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let mut armed = INJECT_SYNC_FAILURE.lock().expect("sync seam lock");
    match armed.iter().position(|armed_path| {
        std::fs::canonicalize(armed_path).unwrap_or_else(|_| armed_path.clone()) == key
    }) {
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
                let stream = match accepted {
                    Ok((stream, _)) => stream,
                    Err(error) => {
                        return Err(DaemonError::Socket(format!("accept: {error}")))
                    }
                };
                let host = Arc::clone(host);
                let id = next_id;
                next_id += 1;
                connections.spawn(async move { serve_connection(stream, id, host).await });
            }
            Some(joined) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = joined {
                    eprintln!("mindcore-daemon: connection task failed: {error}");
                }
            }
        }
    }
}

/// One accepted connection: its transport, its responder, the tool registration that must run
/// before this task yields, and the two held together until the session ends.
async fn serve_connection(stream: UnixStream, id: usize, host: Arc<Host>) {
    let transport = Arc::new(UnixSocketTransport::from_stream(stream));
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
    park_in_setup(&host).await;
    if let SpokeResult::Reject(reject) = registration(&host, &responder) {
        eprintln!(
            "mindcore-daemon: surface: {}: {}",
            reject.code, reject.message
        );
        gated.reject();
        responder.close();
        host.live
            .lock()
            .expect("live lock")
            .retain(|(live_id, _)| *live_id != id);
        return;
    }
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
    resume: tokio::sync::mpsc::UnboundedReceiver<()>,
}

#[cfg(test)]
async fn park_in_setup(host: &Host) {
    #[cfg(test)]
    if let Some(parked) = &host.park_setup {
        let _ = parked.send(());
        let seam = SETUP_PARK.lock().expect("setup seam lock").take();
        let Some(mut seam) = seam else { return };
        let _ = seam.resume.recv().await;
    }
    #[cfg(not(test))]
    let _ = host;
}

#[cfg(not(test))]
async fn park_in_setup(_host: &Host) {}

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
// same-user attacker — upgrade: when the local attacker becomes part of the threat model.
async fn remove_own_socket(socket_path: &Path, owned: SocketIdentity) -> Result<(), DaemonError> {
    remove_if_own(socket_path, owned)
        .await
        .map_err(|fault| DaemonError::Socket(format!("{}: {fault}", socket_path.display())))
}

/// The conditional unlink both callers share: `NotFound` is the intended end state, and a name
/// that is no longer the socket `owned` names — a different file type, or a different device and
/// inode — is refused with nothing removed.
async fn remove_if_own(socket_path: &Path, owned: SocketIdentity) -> Result<(), String> {
    if matches!(check_is_own(socket_path, owned)?, Occupant::Absent) {
        return Ok(());
    }
    let path = socket_path.to_path_buf();
    tokio::task::spawn_blocking(move || std::fs::remove_file(&path))
        .await
        .map_err(|error| format!("the blocking task failed: {error}"))?
        .map_err(|error| format!("the socket name could not be removed: {error}"))
}

/// The synchronous form, for `Drop`: it cannot await, and a start that reaches it has already
/// failed, so one inline syscall is not a serving delay.
fn remove_if_own_blocking(socket_path: &Path, owned: SocketIdentity) -> Result<(), String> {
    if matches!(check_is_own(socket_path, owned)?, Occupant::Absent) {
        return Ok(());
    }
    std::fs::remove_file(socket_path)
        .map_err(|error| format!("the socket name could not be removed: {error}"))
}

/// What a removal should do with the name it found.
enum Occupant {
    /// Nothing is there: already the intended end state, and nothing to unlink.
    Absent,
    /// The socket this daemon bound: unlink it.
    Own,
}

/// The rules both removals share: absent is the intended end state, a different occupant is refused
/// with nothing touched, and only a socket of the recorded identity may be unlinked.
fn check_is_own(socket_path: &Path, owned: SocketIdentity) -> Result<Occupant, String> {
    let metadata = match std::fs::symlink_metadata(socket_path) {
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Occupant::Absent),
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
    Ok(Occupant::Own)
}

/// Binds `socket_path`, removing the name only when it is a socket established to be dead, and
/// answers a guard that unlinks what it bound unless the caller disarms it.
///
/// Nothing that is not a socket is ever unlinked, so a name the daemon does not own — the store,
/// the seed file, any other file, a directory, a symlink or a device — is refused rather than
/// destroyed. The one removal on a start re-checks the identity it just took, so a socket swapped
/// in between the probe and the unlink survives.
async fn listen(config: &DaemonConfig) -> Result<BoundSocket, DaemonError> {
    let socket_path = &config.socket_path;
    let fail = |fault: String| DaemonError::Socket(format!("{}: {fault}", socket_path.display()));

    let metadata = match std::fs::symlink_metadata(socket_path) {
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
    let stale = SocketIdentity::of(&metadata);
    match probe_liveness(socket_path, PROBE_TIMEOUT).await {
        Probe::Live => return Err(fail("a live listener already owns the name".to_string())),
        Probe::Dead => {}
        Probe::Inconclusive(fault) => return Err(fail(fault)),
    }
    remove_if_own(socket_path, stale).await.map_err(fail)?;
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

/// Serializes the two tests that depend on the process's working directory: the `./journal` case
/// changes it, and the `.`-relative case reads it. Nothing else in this crate resolves a relative
/// path, so the lock's reach is exactly these two.
///
/// A tokio mutex, not a `std` one: both tests hold it across `.await` points, and a `std` guard
/// across an await is exactly the deadlock hazard clippy rejects.
#[cfg(test)]
static CWD_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
        std::fs::set_permissions(&seed, std::fs::Permissions::from_mode(0o600))
            .expect("make the seed owner-only");
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
        match listen(&config).await {
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
        drop(UnixListener::bind(&config.socket_path).expect("leave a stale socket"));
        assert!(config.socket_path.exists(), "the stale name is there");
        let bound = listen(&config).await.expect("replace a stale socket");
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
        match listen(&aliased).await {
            Err(DaemonError::Socket(message)) => {
                assert!(message.contains("symbolic link"), "{message}")
            }
            Err(other) => panic!("a symlink is a socket refusal, not {other:?}"),
            Ok(_) => panic!("a symlink is never bound or removed"),
        }
        assert!(alias.exists(), "the symlink survives");
        assert!(real.exists(), "the listener's name survives");
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
        match listen(&config).await {
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
        let config = DaemonConfig {
            socket_path: dir.path().join("absent").join("daemon.sock"),
            ..config(dir.path())
        };
        match listen(&config).await {
            Err(DaemonError::Socket(message)) => {
                assert!(message.contains("absent"), "{message}")
            }
            Err(other) => panic!("a bind failure is a socket refusal, not {other:?}"),
            Ok(_) => panic!("no directory holds the socket"),
        }
    }

    /// The journal root is the host's to create and make durable before the first publication.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_journal_root_is_created_with_its_parent_synced() {
        let dir = TempDir::new().expect("temporary directory");
        let root = dir.path().join("journal");
        journal_root(&root).await.expect("create the journal root");
        assert!(root.is_dir());
    }

    /// A regular file where the journal root belongs is a journal fault naming it, not a success:
    /// publishing into whatever the path really names would be worse than refusing to start.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_journal_component_that_is_not_a_directory_is_refused() {
        let dir = TempDir::new().expect("temporary directory");
        let file = dir.path().join("journal");
        std::fs::write(&file, b"not a directory").expect("write the file");
        match journal_root(&file).await {
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
    #[tokio::test(flavor = "multi_thread")]
    async fn a_journal_ancestor_that_is_not_a_directory_is_refused() {
        let dir = TempDir::new().expect("temporary directory");
        let file = dir.path().join("new-parent");
        std::fs::write(&file, b"file").expect("write the file");
        match journal_root(&file.join("journal")).await {
            Err(DaemonError::Journal(message)) => {
                assert!(message.contains("not a directory"), "{message}");
            }
            other => panic!("a file above the journal root is a journal fault, not {other:?}"),
        }
    }

    /// A failure after the store opened closes its pool before returning, so the connections end in
    /// order rather than being cut when the runtime drops.
    ///
    /// This test pins the failure path's shape — the journal fault is reported and the store file is
    /// left usable — but it does NOT prove the pool was closed: `SqlitePool::close` has no observable
    /// effect from outside, the daemon crate has no `sqlx` dependency to reach `is_closed`, and a
    /// second `MindStore::open` succeeds either way. Verified by rerunning with the `pool.close()`
    /// removed: this test still passes. The close is required by the drain contract and is visible in
    /// the source, not here.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_journal_failure_reports_the_fault_and_leaves_the_store_usable() {
        let dir = TempDir::new().expect("temporary directory");
        let store_path = dir.path().join("mind.db");
        let blocker = dir.path().join("journal");
        std::fs::write(&blocker, b"not a directory").expect("write the blocker");
        let config = DaemonConfig {
            store_path: store_path.clone(),
            journal: crate::config::JournalConfig {
                dir: blocker.clone(),
            },
            ..config(dir.path())
        };
        let lock = StoreLock::acquire(&store_path).expect("take the lock");

        match run(config, store_path.clone(), lock).await {
            Err(DaemonError::Journal(message)) => {
                assert!(message.contains("not a directory"), "{message}")
            }
            other => panic!("a file at the journal root is a journal fault, not {other:?}"),
        }

        let reopened = MindStore::open(&store_path)
            .await
            .expect("the store reopens after the failed start");
        reopened.close().await;
    }

    /// The probe's answers, all constructible portably: a listener is `Live`, an absent name is
    /// `Dead`, and a name removed between the stat and the dial is `Dead` too — that last one is why
    /// `NotFound` counts as dead, since a vanished name is not a live listener's.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_probe_answers_live_or_dead() {
        let dir = TempDir::new().expect("temporary directory");

        let live_path = dir.path().join("live.sock");
        let live = UnixListener::bind(&live_path).expect("bind the socket");
        assert!(
            matches!(probe_liveness(&live_path, PROBE_TIMEOUT).await, Probe::Live),
            "a listener answers the dial"
        );
        drop(live);

        let absent = dir.path().join("absent.sock");
        assert!(
            matches!(probe_liveness(&absent, PROBE_TIMEOUT).await, Probe::Dead),
            "a name with nothing on it is dead"
        );

        let gone = dir.path().join("gone.sock");
        drop(UnixListener::bind(&gone).expect("bind then drop"));
        assert!(
            matches!(probe_liveness(&gone, PROBE_TIMEOUT).await, Probe::Dead),
            "a listener that went away is dead"
        );
    }

    /// The timeout branch: a dial that neither answers nor is refused becomes `Inconclusive` naming
    /// the budget. Reached through the seam, because a full, undrained backlog cannot be held across
    /// a test call.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_probe_that_outlives_its_budget_is_inconclusive() {
        let dir = TempDir::new().expect("temporary directory");
        let silent = dir.path().join("silent.sock");
        *PENDING_PROBE.lock().expect("pending probe lock") = Some(silent.clone());

        let started = std::time::Instant::now();
        let outcome = probe_liveness(&silent, PROBE_TIMEOUT).await;
        assert!(
            started.elapsed() >= PROBE_TIMEOUT,
            "the budget was actually waited out, not skipped"
        );
        match outcome {
            Probe::Inconclusive(message) => {
                assert!(message.contains("did not answer within"), "{message}");
                assert!(
                    message.contains(&format!("{}s", PROBE_TIMEOUT.as_secs())),
                    "the refusal names the budget: {message}"
                );
                assert!(message.contains("liveness is unknown"), "{message}");
            }
            other => panic!("a dial past its budget is inconclusive, not {other:?}"),
        }
    }

    /// A journal root reached through a symlink is the operator's arrangement: the resolved type
    /// decides, so a link to a directory starts and a link to anything else is still refused.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_symlinked_journal_root_is_accepted() {
        let dir = TempDir::new().expect("temporary directory");
        let real = dir.path().join("real");
        std::fs::create_dir(&real).expect("create the real directory");
        let leaf = dir.path().join("leaf-link");
        std::os::unix::fs::symlink(&real, &leaf).expect("link to a directory");

        journal_root(&leaf)
            .await
            .expect("a link to a directory is a root");

        let file = dir.path().join("plain-file");
        std::fs::write(&file, b"not a directory").expect("write the file");
        let bad = dir.path().join("bad-link");
        std::os::unix::fs::symlink(&file, &bad).expect("link to a file");
        match journal_root(&bad).await {
            Err(DaemonError::Journal(message)) => {
                assert!(message.contains("not a directory"), "{message}");
                assert!(
                    message.contains(&bad.display().to_string()),
                    "the refusal names the configured path: {message}"
                );
            }
            other => panic!("a link to a file is a journal fault, not {other:?}"),
        }
    }

    /// An ancestor reached through a symlink is accepted too: the walk resolves each level it finds.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_symlinked_journal_ancestor_is_accepted() {
        let dir = TempDir::new().expect("temporary directory");
        let real = dir.path().join("real-ancestor");
        std::fs::create_dir(&real).expect("create the real ancestor");
        let link = dir.path().join("ancestor-link");
        std::os::unix::fs::symlink(&real, &link).expect("link to the ancestor");

        let root = link.join("journal");
        journal_root(&root)
            .await
            .expect("create beneath a linked ancestor");
        assert!(
            real.join("journal").is_dir(),
            "the level was created inside the resolved directory"
        );
    }

    /// Every level this start creates is owner-only, not the umask's choice: the journal holds what
    /// the Mind settled on, and a directory another user can write into is a publication target they
    /// control. A level that already existed keeps the mode it had.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_created_journal_level_is_owner_only_and_an_existing_one_keeps_its_mode() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().expect("temporary directory");
        let existing = dir.path().join("existing");
        std::fs::create_dir(&existing).expect("create the ancestor");
        std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o750))
            .expect("give the ancestor its own mode");

        let root = existing.join("a").join("b");
        journal_root(&root).await.expect("create two levels");
        for level in [existing.join("a"), root] {
            let mode = std::fs::metadata(&level)
                .expect("stat the created level")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(
                mode,
                JOURNAL_DIR_MODE,
                "{} is {mode:04o}, not owner-only",
                level.display()
            );
        }

        let kept = std::fs::metadata(&existing)
            .expect("stat the existing level")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(kept, 0o750, "a pre-existing level keeps {kept:04o}");
    }

    /// The probe runs on a single-worker runtime against a socket whose backlog is full, and the
    /// start still finishes. This is a smoke test, not a discriminating one: a `connect` on a local
    /// Unix socket returns immediately whether or not the backlog has room and whether the dial is
    /// async or blocking, so both implementations pass here. What it does guard is that the async
    /// rewrite did not introduce a wait that never resolves — a real regression class when a probe
    /// is moved onto the runtime.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn a_full_backlog_does_not_park_the_probe() {
        let dir = TempDir::new().expect("temporary directory");
        let config = config(dir.path());
        let live = UnixListener::bind(&config.socket_path).expect("bind the socket");

        let mut queued = Vec::new();
        for _ in 0..32 {
            match UnixStream::connect(&config.socket_path).await {
                Ok(stream) => queued.push(stream),
                Err(_) => break,
            }
        }
        assert!(!queued.is_empty(), "the backlog took at least one dial");

        let probing = tokio::spawn({
            let config = config.clone();
            async move { listen(&config).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        probing.abort();
        drop(live);
        drop(queued);
    }

    /// A drain where the accept loop failed and the socket name also could not be removed reports
    /// both: an operator who sees a non-zero exit must learn the name is still there, which `and`
    /// would have thrown away.
    #[test]
    fn a_drain_that_failed_twice_names_both_faults_and_the_leftover() {
        let path = Path::new("/run/mindcore/daemon.sock");
        let error = settle(
            Err(DaemonError::Socket(
                "accept: too many open files".to_string(),
            )),
            Err(DaemonError::Socket(format!(
                "{}: the name is no longer this daemon's socket",
                path.display()
            ))),
            path,
        )
        .expect_err("both faults are an error");

        let message = error.to_string();
        assert!(message.contains("too many open files"), "{message}");
        assert!(message.contains("could not be removed"), "{message}");
        assert!(
            message.contains(&path.display().to_string()),
            "the refusal names the leftover: {message}"
        );
        assert!(message.contains("before the next start"), "{message}");

        assert!(
            settle(Err(DaemonError::Socket("accept".to_string())), Ok(()), path)
                .expect_err("one fault")
                .to_string()
                .contains("accept")
        );
        settle(Ok(()), Ok(()), path).expect("nothing failed");
    }

    /// A single-segment relative journal root still syncs its own directory entry.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_single_relative_segment_syncs_the_current_directory() {
        let _cwd = CWD_LOCK.lock().await;
        let here = Path::new(".");
        clear_sync_attempts(here);
        let mut synced = Vec::new();
        sync_level_entry(Path::new("journal"), &mut synced)
            .await
            .expect("sync the current directory");
        assert_eq!(
            sync_attempts(here),
            1,
            "a one-segment relative level syncs the current directory"
        );

        clear_sync_attempts(here);
        let mut synced = Vec::new();
        sync_level_entry(Path::new("journal"), &mut synced)
            .await
            .expect("sync it again");
        assert_eq!(
            sync_attempts(here),
            1,
            "every start re-syncs it: one flush in this start's own count"
        );

        let dir = TempDir::new().expect("temporary directory");
        let parent = dir.path().join("parent");
        std::fs::create_dir(&parent).expect("create the ancestor");
        clear_sync_attempts(&parent);
        clear_sync_attempts(dir.path());
        let mut synced = Vec::new();
        sync_level_entry(&parent.join("journal"), &mut synced)
            .await
            .expect("sync the leaf's entry");
        sync_level_entry(&parent, &mut synced)
            .await
            .expect("sync the ancestor's entry");
        assert_eq!(sync_attempts(&parent), 1, "the leaf's entry is in parent");
        assert_eq!(
            sync_attempts(dir.path()),
            1,
            "the ancestor's own entry is in its parent"
        );

        assert!(holding_directory(Path::new("/")).is_none());
    }

    /// A symlink in the chain means the journal really lives elsewhere. The daemon creates `journal`
    /// inside the target directory, which dirties that directory — so the target is flushed, and the
    /// walk stops there rather than climbing the target's ancestors.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_symlinked_journal_root_syncs_the_target_directory() {
        let dir = TempDir::new().expect("temporary directory");
        let elsewhere = TempDir::new().expect("temporary directory");
        let real = elsewhere.path().join("real");
        std::fs::create_dir(&real).expect("create the real directory");
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real, &link).expect("link to the real directory");

        clear_sync_attempts(&real);
        clear_sync_attempts(elsewhere.path());
        journal_root(&link.join("journal"))
            .await
            .expect("create through the link");
        assert_eq!(
            sync_attempts(&real),
            1,
            "the target directory holds the new level, so it is flushed"
        );
        assert_eq!(
            sync_attempts(elsewhere.path()),
            0,
            "the walk stops at the target, not at its ancestors"
        );

        let plain_parent = dir.path().join("plain");
        std::fs::create_dir(&plain_parent).expect("create the plain parent");
        clear_sync_attempts(&plain_parent);
        clear_sync_attempts(dir.path());
        journal_root(&plain_parent.join("journal"))
            .await
            .expect("create the plain root");
        assert_eq!(
            sync_attempts(&plain_parent),
            1,
            "the plain case still flushes each level once"
        );
        assert_eq!(sync_attempts(dir.path()), 1, "the ancestor once");
    }

    /// A created level's holding directory owes a flush *after* the creation, even when the same
    /// directory was already flushed earlier in the walk.
    ///
    /// `dir = "./journal"` is the case that exposes it: the chain is `["./journal", "."]`, and `.` is
    /// both the level's holder and its own holder, so the walk flushes `.` before the level exists and
    /// would dedupe away the flush it owes once the level does.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_created_level_flushes_its_holder_after_the_creation() {
        let _cwd = CWD_LOCK.lock().await;
        let dir = TempDir::new().expect("temporary directory");
        let original = std::env::current_dir().expect("a working directory");
        std::env::set_current_dir(dir.path()).expect("enter the scratch directory");

        let here = Path::new(".");
        clear_sync_attempts(here);
        let outcome = journal_root(Path::new("./journal")).await;
        // Read the count while the scratch directory is still current: `sync_attempts` resolves the
        // spelling it is given, so `"."` means a different directory after the chdir back.
        let flushed = sync_attempts(here);
        std::env::set_current_dir(&original).expect("leave the scratch directory");

        outcome.expect("a relative root starts");
        assert!(
            dir.path().join("journal").is_dir(),
            "the level was created in the scratch directory"
        );
        assert_eq!(
            flushed, 2,
            "`.` is flushed before the level exists and again once its entry is new"
        );

        let holder = dir.path().join("holder");
        std::fs::create_dir(&holder).expect("create the holder");
        clear_sync_attempts(&holder);
        journal_root(&holder.join("journal"))
            .await
            .expect("create the absolute root");
        assert_eq!(
            sync_attempts(&holder),
            1,
            "a plain path flushes each level once"
        );
    }

    /// A level whose holder flush failed on one start must not become "pre-existing, therefore the
    /// operator's" on the next: the directory it created is still there, and its entry was never made
    /// durable. So the chain stays fatal in full, and a restart re-attempts the same flush.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_holder_flush_is_retried_fatally_on_the_next_start() {
        let dir = TempDir::new().expect("temporary directory");
        let holder = dir.path().join("holder");
        std::fs::create_dir(&holder).expect("create the holder");
        let root = holder.join("journal");

        clear_sync_attempts(&holder);
        push_injected_sync_failure(&holder);
        match journal_root(&root).await {
            Err(DaemonError::Journal(message)) => {
                assert!(message.contains("injected sync fault"), "{message}")
            }
            other => panic!("the armed first start must fail, not {other:?}"),
        }
        assert!(
            root.is_dir(),
            "the level was created before the flush failed"
        );

        clear_sync_attempts(&holder);
        journal_root(&root)
            .await
            .expect("the retry accepts the directory");
        assert_eq!(
            sync_attempts(&holder),
            1,
            "the restart re-attempts the same level's holder"
        );

        push_injected_sync_failure(&holder);
        match journal_root(&root).await {
            Err(DaemonError::Journal(message)) => {
                assert!(message.contains("injected sync fault"), "{message}")
            }
            other => panic!("a pre-existing level's failed flush must still refuse, not {other:?}"),
        }
    }

    /// The walk is bounded by the chain: a root under an existing directory flushes that directory and
    /// stops, so the cost is one flush per level.
    ///
    /// Counted on the chain's own directories, never on a shared one: every test's tempdir sits under
    /// the same `/var/folders/.../T`, so asserting that a shared parent was never flushed races with
    /// the other tests running in parallel — the first version of this test did, failing one run in
    /// five. Boundedness is asserted where it is a property of this call alone: the flush count on
    /// the chain's own two directories, which a walk that climbed to `/` would exceed.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_walk_stops_at_the_root_s_holding_directory() {
        let outer = TempDir::new().expect("temporary directory");
        let holder = outer.path().join("holder");
        std::fs::create_dir(&holder).expect("create the holder");

        clear_sync_attempts(&holder);
        clear_sync_attempts(outer.path());
        journal_root(&holder.join("journal"))
            .await
            .expect("create the root");

        assert_eq!(sync_attempts(&holder), 1, "the holding directory, once");
        assert_eq!(
            sync_attempts(outer.path()),
            1,
            "the holding directory's own entry, once"
        );
    }

    /// Every directory the walk flushes is inside the chain and fatal, and nothing above the chain is
    /// named at all. Driven through `journal_root` itself so the split is the code's, not a
    /// reimplementation of it: with the fault armed on the chain's highest holder the start refuses, and
    /// with it armed on the directory above the chain the start succeeds.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_fault_inside_the_chain_refuses_and_one_above_it_does_not() {
        let outer = TempDir::new().expect("temporary directory");
        let holder = outer.path().join("holder");
        std::fs::create_dir(&holder).expect("create the holder");
        let root = holder.join("journal");

        push_injected_sync_failure(&holder);
        match journal_root(&root).await {
            Err(DaemonError::Journal(message)) => {
                assert!(message.contains("injected sync fault"), "{message}")
            }
            other => panic!("a fault on the chain's own holder must refuse, not {other:?}"),
        }
        assert!(root.is_dir(), "the levels were created before the refusal");

        clear_sync_attempts(outer.path());
        push_injected_sync_failure(outer.path());
        journal_root(&root)
            .await
            .expect("a fault above the chain is never reached, so it cannot refuse");
        assert_eq!(
            sync_attempts(outer.path()),
            0,
            "the walk never flushed a directory above the chain"
        );
        assert!(
            take_injected_sync_failure(outer.path()),
            "the fault above the chain was never taken"
        );
    }

    /// The chain sync is idempotent across restarts, not first-creation-only.    /// The chain sync is idempotent across restarts, not first-creation-only. A start that created a
    /// level and then failed before syncing its entry leaves nothing marking which entry is unproven,
    /// so the next start must re-sync every component — including the ones it found already there.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_restart_retries_the_sync_of_an_already_existing_level() {
        let dir = TempDir::new().expect("temporary directory");
        let root = dir.path().join("journal");
        let parent = dir.path().to_path_buf();

        clear_sync_attempts(&parent);
        push_injected_sync_failure(&parent);
        match journal_root(&root).await {
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

        clear_sync_attempts(&parent);
        journal_root(&root)
            .await
            .expect("the retry accepts the directory");
        assert_eq!(
            sync_attempts(&parent),
            1,
            "the restart re-flushed the already-existing root's own entry"
        );
    }

    /// A sync that fails is a journal fault, not a success: the daemon must not report a clean start
    /// on a chain that is not on disk yet.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_parent_sync_is_a_journal_fault() {
        let dir = TempDir::new().expect("temporary directory");
        let root = dir.path().join("new-parent").join("journal");
        let armed_parent = dir.path().join("new-parent");
        push_injected_sync_failure(&armed_parent);
        match journal_root(&root).await {
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
    #[tokio::test(flavor = "multi_thread")]
    async fn every_journal_level_is_created_and_synced() {
        let dir = TempDir::new().expect("temporary directory");
        let root = dir.path().join("new-parent").join("journal");
        journal_root(&root).await.expect("create two levels");
        assert!(
            dir.path().join("new-parent").is_dir(),
            "the ancestor exists"
        );
        assert!(root.is_dir(), "the leaf exists");
        journal_root(&root)
            .await
            .expect("an existing root is accepted");
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
        match listen(&collided).await {
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
        let outcome = listen(&config).await;
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
        let foreign = SocketIdentity { dev: 0, ino: 0 };
        match remove_own_socket(&path, foreign).await {
            Err(DaemonError::Socket(message)) => {
                assert!(
                    message.contains("no longer this daemon's socket"),
                    "{message}"
                )
            }
            other => panic!("an unremovable name is a socket failure, not {other:?}"),
        }
        assert!(path.is_dir(), "the directory survives");
        std::fs::remove_dir(&path).expect("clear the directory");
        remove_own_socket(&path, foreign)
            .await
            .expect("an absent name is already removed");
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

        let mut bound = listen(&config).await.expect("bind the socket");
        // Closed after the bind: the bind needs a writable directory, the cleanup does not get one.
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o500))
            .expect("make the directory read-only");
        let failure = match prepare(Arc::new(surface), &config, &mut bound).await {
            Err(failure) => failure,
            Ok(_) => panic!("an undecodable peer key fails the prepare"),
        };
        let refusal = cleanup_refusal(&mut bound, failure).await;
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
        let (listener, bound) = listen(&config).await.expect("bind the socket").into_parts();
        // The listener still owns the socket; swap the name anyway, as a same-user process could.
        std::fs::remove_file(&config.socket_path).expect("unlink the socket");
        let bytes = b"the replacement's own bytes";
        std::fs::write(&config.socket_path, bytes).expect("write the replacement");
        match remove_own_socket(&config.socket_path, bound).await {
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

    /// Stale recovery binds its removal to the identity it probed, so a name now occupied by a
    /// different socket is refused and the occupant survives.
    ///
    /// The claimed identity is supplied directly rather than produced by swapping the file. An
    /// inode-based "different file" fixture is not portable: a filesystem may hand a freed inode
    /// number straight to the next occupant — Linux does — so two successive binds at one name can
    /// report the same `(dev, ino)`, and a test asserting they differ would fail there while the
    /// code behaved correctly. The rule under test compares values, not how a second file arose.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_socket_of_another_identity_is_refused_and_survives() {
        let dir = TempDir::new().expect("temporary directory");
        let config = config(dir.path());
        let live = UnixListener::bind(&config.socket_path).expect("bind the socket");
        let real = SocketIdentity::of(
            &std::fs::symlink_metadata(&config.socket_path).expect("lstat the socket"),
        );

        // `u64::MAX` for both members names nothing a device or inode can be, so this cannot
        // collide with `real` on any filesystem.
        let claimed = SocketIdentity {
            dev: u64::MAX,
            ino: u64::MAX,
        };
        assert_ne!(claimed, real, "the stated identity is not the socket's");
        match remove_if_own(&config.socket_path, claimed).await {
            Err(message) => assert!(message.contains("replaced by another socket"), "{message}"),
            Ok(()) => panic!("a socket of another identity must not be removed"),
        }
        assert!(
            config.socket_path.exists(),
            "the socket survives the refused removal"
        );

        remove_if_own(&config.socket_path, real)
            .await
            .expect("the matching identity removes the name");
        assert!(
            !config.socket_path.exists(),
            "the matching identity removed the name"
        );
        drop(live);
    }

    /// A registration the responder refuses must end the connection task, not park it forever. The
    /// responder's read task waits on the gate, which `close` cannot reach; without the rejection
    /// state the task would sit there until the process died.
    ///
    /// The production path runs: `serve_connection` itself, over a real socket pair, with a seam that
    /// makes its `register` answer `Reject`. A refused registration is not otherwise constructible —
    /// `derive_manifest` validates every descriptor before assembly, and `register_tool_handler`
    /// refuses a non-`tools.` id — so the seam is the only way to reach this branch.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_register_rejection_ends_the_connection_task() {
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
        let (client, server) = UnixStream::pair().expect("a socket pair");
        let host = Arc::new(Host {
            socket: SocketIdentity { dev: 0, ino: 0 },
            surface: Arc::new(surface),
            seed: crate::config::read_seed(&config.identity.seed_file).expect("read the seed"),
            allowlist: Vec::new(),
            peer_keys: HashMap::new(),
            invoke_timeout_ms: Some(1_000),
            live: Mutex::new(Vec::new()),
            park_setup: None,
            reject_registration: true,
        });

        let task = tokio::spawn({
            let host = Arc::clone(&host);
            async move { serve_connection(server, 0, host).await }
        });
        let ended = tokio::time::timeout(std::time::Duration::from_secs(5), task).await;
        assert!(
            ended.is_ok(),
            "the connection task must end on a register rejection, not park on the gate"
        );
        ended
            .expect("the task finished")
            .expect("the connection task did not panic");
        assert!(
            host.live.lock().expect("live lock").is_empty(),
            "the rejected session is untracked"
        );
        drop(client);
    }

    /// F-002: a connection accepted and parked before its registration is owned by the drain. Its
    /// responder is tracked from the moment it exists, so the drain closes it; the task itself is
    /// then cancelled, so nothing inserts after the snapshot and no session survives the name.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_drain_ends_a_connection_parked_before_its_registration() {
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
        let (park_setup, mut park_signal) = tokio::sync::mpsc::unbounded_channel();
        // The same wiring `prepare` builds, without the signals this test never awaits.
        let host = Arc::new(Host {
            socket,
            surface: Arc::new(surface),
            seed: crate::config::read_seed(&config.identity.seed_file).expect("read the seed"),
            allowlist: config.peers.keys().cloned().collect(),
            peer_keys: peer_keys(&config).expect("decode the peer keys"),
            invoke_timeout_ms: config.invoke.timeout_ms,
            live: Mutex::new(Vec::new()),
            park_setup: Some(park_setup),
            reject_registration: false,
        });

        let (resume, parked_resume) = tokio::sync::mpsc::unbounded_channel();
        *SETUP_PARK.lock().expect("setup seam lock") = Some(SetupPark {
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
