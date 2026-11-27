//! The `mindcore-daemon` binary: `--config`, the start phase sequence and the exit-code mapping.
//!
//! Every phase begins only after the previous one succeeded, so a failed start leaves nothing
//! behind and a daemon that exits non-zero has bound no socket.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use mindcore_daemon::config;
use mindcore_daemon::lock::StoreLock;
use mindcore_daemon::serve;
use mindcore_daemon::DaemonError;

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

/// The start sequence: config, lock, then the serving phase, which owns the store, the journal
/// root, the surface, the listener and the drain.
async fn start(config_path: &Path) -> Result<(), DaemonError> {
    let config = config::load(config_path)?;
    // The lock precedes the store: `open` creates the file and runs the embedded migration, so a
    // second instance must be refused before it. The lock is a file of its own beside the store,
    // named for the store's inode, so every alias of the store contends on it while the store
    // itself stays unlocked for SQLite.
    let lock = StoreLock::acquire(&config.store_path)?;
    serve::run(config, lock).await
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
