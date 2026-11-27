//! The daemon end to end over a real socket: one stored entry read by a signed dialer, one
//! `SIGTERM` that exits 0 and takes the socket with it, and one second instance refused while the
//! first holds the store's lock.
//!
//! The binary is the one Cargo builds for this package, started with `--config` exactly as an
//! operator starts it. The fixture is the library's own write path: `MindStore::open`, a Mind
//! created for the peer's principal, one entry put through the `Mind` handle, then the store
//! closed so the daemon opens the same file.
//!
//! Both keys are throwaway seeds written here, never committed material: the daemon's seed file
//! holds the responder's, and the config's `[peers]` row holds the dialer's public key.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ed25519_dalek::SigningKey;
use serde_json::json;
use tempfile::TempDir;
use tokio::net::UnixStream;

use mindcore_base::boundary::CallerId;
use mindcore_base::ids::MindId;
use mindcore_daemon::transport::UnixSocketTransport;
use mindcore_runtime::minds::{self, CreateMind};
use mindcore_runtime::Mind;
use mindcore_sqlite::MindStore;
use spoke_connect::core::derive_peer_id_from_ed25519_pubkey;
use spoke_connect::remote::{
    connect_remote_adapter, RemoteAdapter, RemoteAdapterOptions, RemoteIdentity,
};
use spoke_operations::SpokeResult;
use spoke_schemas::knowledge_entry::{KnowledgeEntryBody, KnowledgeEntryCanonicalName};
use spoke_schemas::{HostCapabilityManifest, KnowledgeEntry, Scope};

const STORE_FILE: &str = "mind.db";
const SOCKET_FILE: &str = "mind.sock";
const SECOND_SOCKET_FILE: &str = "second.sock";
const SEED_FILE: &str = "daemon.seed";
const JOURNAL_DIR: &str = "journal";

const PRINCIPAL: &str = "prn_serve";
const ENTRY_ID: &str = "ke_north_gate";
const SUMMARY: &str = "the north gate is shut after dusk";

const DIALER_SEED: [u8; 32] = [0x42u8; 32];
const RESPONDER_SEED: [u8; 32] = [0xA7u8; 32];

/// The knowledge face's two flags: a scoped get is negotiated, so the dialer claims both.
const KNOWLEDGE_CAPABILITIES: [&str; 2] = ["spoke-knowledge", "spoke-knowledge-scoped"];

/// How long a start may take before the test calls it a failure, and how often it re-probes.
const CONNECT_BUDGET: Duration = Duration::from_secs(10);
const CONNECT_INTERVAL: Duration = Duration::from_millis(25);
/// How long an exit may take: a drain that never returns fails the test rather than hanging it.
const EXIT_BUDGET: Duration = Duration::from_secs(10);

/// A started daemon, killed on drop so a failed assertion cannot leave one serving.
struct Daemon {
    child: Child,
}

impl Daemon {
    fn spawn(config: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_mindcore-daemon"))
            .arg("--config")
            .arg(config)
            .spawn()
            .expect("start the daemon");
        Self { child }
    }

    fn pid(&self) -> i32 {
        i32::try_from(self.child.id()).expect("a child pid fits in i32")
    }

    /// Waits for the daemon's exit, bounded: a regression that leaves the process running fails the
    /// test on the budget instead of blocking a worker thread forever. `try_wait` is polled because
    /// a blocking `wait` in an async test is exactly the hang this bound exists to prevent.
    async fn wait(&mut self) -> ExitStatus {
        let started = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().expect("poll the daemon") {
                return status;
            }
            assert!(
                started.elapsed() < EXIT_BUDGET,
                "the daemon did not exit within {EXIT_BUDGET:?}"
            );
            tokio::time::sleep(CONNECT_INTERVAL).await;
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn ok<T>(result: SpokeResult<T>, step: &str) -> T {
    match result {
        SpokeResult::Ok(value) => value,
        SpokeResult::Reject(reject) => {
            panic!("{step} refused: {}: {}", reject.code, reject.message)
        }
    }
}

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn public_key(seed: &[u8; 32]) -> [u8; 32] {
    SigningKey::from_bytes(seed).verifying_key().to_bytes()
}

/// The entry the fixture stores: one plain knowledge entry the served get reads back by id.
fn entry() -> KnowledgeEntry {
    KnowledgeEntry {
        adoption_anchor: None,
        body: KnowledgeEntryBody {
            summary: Some(SUMMARY.to_string()),
            ..KnowledgeEntryBody::default()
        },
        canonical_name: KnowledgeEntryCanonicalName::try_from("north gate".to_string())
            .expect("a non-empty canonical name"),
        created_at: None,
        disclosure: None,
        entry_id: ENTRY_ID.to_string(),
        entry_type: "note".to_string(),
        extensions: std::collections::HashMap::new(),
        modules: std::collections::HashMap::new(),
        owner: None,
        revision: Some(1),
        schema_version: std::num::NonZeroU64::new(1).expect("the wire baseline is not zero"),
        source_anchor: None,
        status: "confirmed".to_string(),
        updated_at: None,
    }
}

/// The store the daemon serves: one Mind owned by the peer's principal with one entry put through
/// the `Mind` handle, closed before the daemon opens the same file.
async fn seed(dir: &Path) -> MindId {
    let store = MindStore::open(&dir.join(STORE_FILE))
        .await
        .expect("open the store");
    let owner = CallerId::new(PRINCIPAL);
    let mind = ok(
        minds::create(
            &store,
            &CreateMind {
                mind_id: None,
                display_name: Some("Serve".to_string()),
                owner: owner.clone(),
            },
        )
        .await,
        "create the Mind",
    );
    let handle = ok(Mind::open(&store, &owner, &mind.id).await, "open the Mind");
    ok(handle.knowledge_put(entry(), None).await, "store the entry");
    store.close().await;
    mind.id
}

/// The config the daemon reads, written where a host would put it: one store, one socket, the
/// responder's seed file, the journal root and the one admitted peer.
fn config_text(dir: &Path, socket: &Path) -> String {
    let dialer = derive_peer_id_from_ed25519_pubkey(&public_key(&DIALER_SEED));
    format!(
        r#"
store_path = "{}"
socket_path = "{}"
host_id = "host-serve"

[identity]
seed_file = "{}"

[journal]
dir = "{}"

[peers."{dialer}"]
public_key = "{}"
principal = "{PRINCIPAL}"
"#,
        dir.join(STORE_FILE).display(),
        socket.display(),
        dir.join(SEED_FILE).display(),
        dir.join(JOURNAL_DIR).display(),
        hex(&public_key(&DIALER_SEED)),
    )
}

fn write(dir: &Path, name: &str, text: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, text).expect("write the file");
    path
}

/// Waits for the daemon to bind, and answers the stream that reached it: the successful probe is
/// the session's own stream, so no second connection is opened only to be abandoned.
async fn wait_for_socket(path: &Path) -> UnixStream {
    let started = Instant::now();
    loop {
        let fault = match UnixStream::connect(path).await {
            Ok(stream) => return stream,
            Err(fault) => fault,
        };
        assert!(
            started.elapsed() < CONNECT_BUDGET,
            "the daemon did not bind {} within {CONNECT_BUDGET:?}: {fault}",
            path.display()
        );
        tokio::time::sleep(CONNECT_INTERVAL).await;
    }
}

fn hello(capabilities: &[&str]) -> HostCapabilityManifest {
    serde_json::from_value(json!({
        "schema_version": 1,
        "host_id": "serve-dialer",
        "roles": ["data-store"],
        "capabilities": capabilities,
        "namespaces": [],
        "extensions": {},
    }))
    .expect("the dialer's manifest matches the schema")
}

/// One signed session over `stream`, the dialer claiming the knowledge face's two flags.
async fn dial(stream: UnixStream) -> Arc<RemoteAdapter> {
    let responder_pubkey = public_key(&RESPONDER_SEED);
    connect_remote_adapter(RemoteAdapterOptions {
        transport: Arc::new(UnixSocketTransport::from_stream(stream)),
        local_identity: RemoteIdentity { seed: DIALER_SEED },
        local_manifest: hello(&KNOWLEDGE_CAPABILITIES),
        remote_pubkey: responder_pubkey,
        allowlist: vec![derive_peer_id_from_ed25519_pubkey(&responder_pubkey)],
        invoke_timeout_ms: Some(10_000),
        capability_token: None,
    })
    .await
    .expect("the signed session must establish")
}

fn scope_of(scope_id: &str) -> Scope {
    serde_json::from_value(json!({ "scope_id": scope_id })).expect("scope JSON matches the schema")
}

/// The lock file beside the store's canonical name: the daemon leaves it in place, and nothing else
/// in this directory is named `<store>.lock`.
fn lock_file(dir: &Path) -> PathBuf {
    let mut locks: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("read the directory")
        .map(|entry| entry.expect("a directory entry").path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".lock"))
        })
        .collect();
    assert_eq!(locks.len(), 1, "one lock file beside the store: {locks:?}");
    locks.remove(0)
}

/// The daemon's socket is owner-only whatever the umask of the shell that started it was: who may
/// connect is part of the process contract, and an integration test is the only place the real
/// binary's mode is observable.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_daemon_binds_an_owner_only_socket() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new().expect("a scratch directory");
    seed(dir.path()).await;
    std::fs::write(dir.path().join(SEED_FILE), hex(&RESPONDER_SEED)).expect("write the seed");
    let socket = dir.path().join(SOCKET_FILE);
    let config = write(dir.path(), "daemon.toml", &config_text(dir.path(), &socket));

    let daemon = Daemon::spawn(&config);
    wait_for_socket(&socket).await;

    let mode = std::fs::metadata(&socket)
        .expect("stat the socket")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "the socket is {mode:o}, not owner-only");
    drop(daemon);
}

/// A signed dialer reads a stored entry out of the daemon's own store, over the socket the daemon
/// bound: `get` answers the entry the fixture wrote, in the Mind the caller's grant admits it to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_dialer_reads_a_stored_entry_over_the_socket() {
    let dir = TempDir::new().expect("a scratch directory");
    let mind = seed(dir.path()).await;
    std::fs::write(dir.path().join(SEED_FILE), hex(&RESPONDER_SEED)).expect("write the seed");
    let socket = dir.path().join(SOCKET_FILE);
    let config = write(dir.path(), "daemon.toml", &config_text(dir.path(), &socket));

    let _daemon = Daemon::spawn(&config);
    let adapter = dial(wait_for_socket(&socket).await).await;

    let read = ok(
        adapter
            .get_knowledge_entry_in_scope(ENTRY_ID, Some(&scope_of(mind.as_str())))
            .await,
        "a get in the admitted Mind",
    );
    assert_eq!(read.entry_id, ENTRY_ID);
    assert_eq!(
        read.body.summary.as_deref(),
        Some(SUMMARY),
        "the daemon answered the entry the store holds"
    );
}

/// The process contract: a second daemon on the same store is refused with exit code 2 and binds
/// nothing, and the first takes `SIGTERM` as an exit 0 that removes its socket and keeps its lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_instance_is_refused_and_sigterm_stops_the_first() {
    let dir = TempDir::new().expect("a scratch directory");
    let _mind = seed(dir.path()).await;
    std::fs::write(dir.path().join(SEED_FILE), hex(&RESPONDER_SEED)).expect("write the seed");
    let socket = dir.path().join(SOCKET_FILE);
    let config = write(dir.path(), "daemon.toml", &config_text(dir.path(), &socket));

    let mut daemon = Daemon::spawn(&config);
    wait_for_socket(&socket).await;
    let lock = lock_file(dir.path());

    // The same store, another socket: the lock, not the name, is what a second instance meets.
    let second_socket = dir.path().join(SECOND_SOCKET_FILE);
    let second_config = write(
        dir.path(),
        "second.toml",
        &config_text(dir.path(), &second_socket),
    );
    let mut second = Daemon::spawn(&second_config);
    let refused = second.wait().await;
    assert_eq!(refused.code(), Some(2), "a held lock exits 2: {refused:?}");
    assert!(
        !second_socket.exists(),
        "the refused instance binds no socket"
    );

    let signalled = unsafe { libc::kill(daemon.pid(), libc::SIGTERM) };
    assert_eq!(signalled, 0, "SIGTERM reaches the daemon");
    let stopped = daemon.wait().await;
    assert!(stopped.success(), "the drain exits 0: {stopped:?}");
    assert!(
        !socket.exists(),
        "the drained daemon removes the socket it bound"
    );
    assert!(
        lock.exists(),
        "the lock file outlives the process: {}",
        lock.display()
    );
}
