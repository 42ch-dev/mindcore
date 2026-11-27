//! The `mindcore-daemon` binary: `--config`, the start phase sequence and the exit-code mapping.
//!
//! Every phase begins only after the previous one succeeded, so a failed start leaves nothing
//! behind and a daemon that exits non-zero has bound no socket.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use mindcore_daemon::config;
use mindcore_daemon::lock::StoreLock;
use mindcore_daemon::DaemonError;
use mindcore_sqlite::MindStore;

const USAGE: &str = "\
usage: mindcore-daemon --config <path>

  --config <path>  the TOML config to read; it is read once, at start
  --help           print this message
";

/// Multi-thread on purpose: the runtime's scoped-knowledge and history admissions are synchronous
/// callbacks Connect runs on its blocking pool through `Handle::block_on`.
#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    let config_path = match config_path(std::env::args().skip(1)) {
        Ok(Some(path)) => path,
        Ok(None) => {
            print!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            eprint!("mindcore-daemon: {message}\n\n{USAGE}");
            return ExitCode::from(1);
        }
    };

    match start(&config_path).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mindcore-daemon: {error}");
            ExitCode::from(error.exit_code())
        }
    }
}

/// The start sequence: config, lock, store, journal root, then the serving phase.
async fn start(config_path: &Path) -> Result<(), DaemonError> {
    let config = config::load(config_path)?;
    // The lock precedes the store: `open` creates the file and runs the embedded migration, so a
    // second instance must be refused before it. The guard lives to the end of the process.
    let _lock = StoreLock::acquire(&config.store_path)?;
    let store = MindStore::open(&config.store_path)
        .await
        .map_err(|error| DaemonError::Store(format!("{}: {error}", config.store_path.display())))?;
    journal_root(&config.journal_dir)?;

    // The serving phase takes the assembled surface, the listener, the accept loop and the drain;
    // until it exists the process must not report a clean start it cannot serve.
    store.close().await;
    Err(DaemonError::Surface(
        "the serving phase is not built into this binary".into(),
    ))
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

/// `--config <path>`, or `--help` answering `None`.
fn config_path(args: impl Iterator<Item = String>) -> Result<Option<PathBuf>, String> {
    let mut config = None;
    let mut args = args;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--config" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--config needs a path".to_string())?;
                if config.replace(PathBuf::from(value)).is_some() {
                    return Err("--config was given twice".to_string());
                }
            }
            "--help" | "-h" => return Ok(None),
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    match config {
        Some(path) => Ok(Some(path)),
        None => Err("--config is required".to_string()),
    }
}
