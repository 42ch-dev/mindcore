//! The `mindcore-daemon` binary: `--config`, the start phase sequence and the exit-code mapping.
//!
//! Every phase begins only after the previous one succeeded, so a daemon that exits non-zero has
//! bound no socket. What a failed start does leave behind is the lock file, a store the lock
//! created, the journal root, and — when the socket directory is unwritable — the socket name,
//! which the refusal names.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use mindcore_daemon::config;
use mindcore_daemon::lock::{self, StoreLock};
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
    // Resolved once, and the same `PathBuf` goes to the lock and to `MindStore::open`: resolving
    // twice would let a symlink re-pointed between the calls open a file the lock never named.
    let store_path = lock::canonical_store_path(&config.store_path)?;
    // The lock precedes the store: `open` creates the file and runs the embedded migration, so a
    // second instance must be refused before it. The lock is a file of its own beside the store's
    // one canonical name, so every spelling and every symlink contends on it while the store itself
    // stays unlocked for SQLite.
    let lock = StoreLock::acquire(&store_path)?;
    serve::run(config, store_path, lock).await
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

#[cfg(test)]
mod tests {
    use super::config_path;

    /// `config_path` over `arguments`, as `main` calls it.
    fn parse(arguments: &[&str]) -> Result<Option<std::path::PathBuf>, String> {
        config_path(arguments.iter().map(|argument| (*argument).to_string()))
    }

    /// The argument contract the exit-code table names: `--help` answers `None` (exit 0), and
    /// `--config <path>` answers the path.
    #[test]
    fn the_documented_arguments_answer_their_values() {
        assert_eq!(parse(&["--help"]).expect("--help"), None, "--help exits 0");
        assert_eq!(parse(&["-h"]).expect("-h"), None, "-h exits 0");
        assert_eq!(
            parse(&["--config", "/tmp/daemon.toml"]).expect("--config"),
            Some(std::path::PathBuf::from("/tmp/daemon.toml")),
        );
    }

    /// Every unusable argument list is an error, which `main` maps to exit 1.
    #[test]
    fn every_unusable_argument_list_is_refused() {
        let cases: [(&str, &[&str]); 5] = [
            ("no arguments", &[]),
            ("a value-less --config", &["--config"]),
            (
                "a repeated --config",
                &["--config", "a.toml", "--config", "b.toml"],
            ),
            ("an unknown argument", &["--socket", "a.sock"]),
            ("--help after a path", &["--config", "a.toml", "--help"]),
        ];
        for (case, arguments) in cases {
            // `--help` anywhere answers usage and exit 0, which is the one documented exception.
            if case == "--help after a path" {
                assert!(parse(arguments).is_ok(), "{case}");
                continue;
            }
            assert!(parse(arguments).is_err(), "{case} is refused");
        }
        assert!(parse(&["--config", "a.toml"]).is_ok(), "the valid form");
    }
}
