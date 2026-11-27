//! The sidecar daemon: one process, one store file, one Unix socket.
//!
//! This crate carries the process contract — the config surface, the single-instance lock and the
//! phase sequence the binary runs. What is served over the socket is `mindcore-runtime`'s: the
//! runtime opens no listener, and the daemon owns the process.

pub mod config;
pub mod lock;
pub mod serve;
pub mod transport;

/// A start failure, which `main` maps to the process exit code.
#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    /// The config file is unusable; the message names the file or the key at fault.
    #[error("config: {0}")]
    Config(String),
    /// The store — or the lock file beside it — could not be opened.
    #[error("store: {0}")]
    Store(String),
    /// The journal root could not be created or made durable.
    #[error("journal: {0}")]
    Journal(String),
    /// The assembled surface refused to serve.
    #[error("surface: {0}")]
    Surface(String),
    /// The socket could not be bound.
    #[error("socket: {0}")]
    Socket(String),
    /// Another daemon holds this store's lock.
    #[error("another daemon holds the lock on {0}")]
    LockHeld(std::path::PathBuf),
}

impl DaemonError {
    /// The process exit code: 2 for a held lock, 1 for every other start failure.
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::LockHeld(_) => 2,
            _ => 1,
        }
    }
}
