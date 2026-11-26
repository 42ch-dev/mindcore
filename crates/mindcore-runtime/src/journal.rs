//! The one journal publisher the crate offers: `journal_dir(root)` writes every journaled
//! file target under `root` at the target's own path.

use std::path::{Path, PathBuf};

use mindcore_spi::SpiError;
use serde_json::Value;

/// A file publisher over `root`: each target's `content` bytes land at `root` joined with the
/// target's `path`, parent directories are created, and `Ok(())` is returned only when every
/// target's exact bytes are durable — the file contents are synced to disk and the directory
/// entries that name it are synced too, so a power loss cannot quietly lose a target the
/// settlement was told had landed.
///
/// Target paths are the adapter's own (`records/<id>.md`, `candidates/<id>.md`,
/// `profiles/<digest>.md`): they carry no separators that could escape `root`, and they are
/// published as given rather than re-derived here.
pub fn journal_dir(
    root: impl Into<PathBuf>,
) -> impl Fn(&[Value]) -> Result<(), SpiError> + Send + Sync + 'static {
    let root = root.into();
    move |targets| {
        for target in targets {
            let path = target.get("path").and_then(Value::as_str).ok_or_else(|| {
                SpiError::InvalidInput("a journal target carries no path".to_string())
            })?;
            let content = target
                .get("content")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    SpiError::InvalidInput(format!("journal target {path} carries no content"))
                })?;
            let file = root.join(path);
            if let Some(parent) = file.parent() {
                std::fs::create_dir_all(parent).map_err(|error| {
                    SpiError::Settlement(spoke_reject(format!(
                        "the directory for {path} could not be created: {error}"
                    )))
                })?;
            }
            std::fs::write(&file, content).map_err(|error| {
                SpiError::Settlement(spoke_reject(format!(
                    "journal target {path} could not be written: {error}"
                )))
            })?;
            // Durability, not visibility: the write must reach the medium before the caller
            // may settle the intent. Sync the file's contents, then the directory entries of
            // every directory `create_dir_all` may have created up to the publisher root.
            std::fs::File::open(&file)
                .and_then(|file| file.sync_all())
                .map_err(|error| {
                    SpiError::Settlement(spoke_reject(format!(
                        "journal target {path} could not be synced: {error}"
                    )))
                })?;
            let mut directory = file.parent().map(Path::to_path_buf);
            while let Some(dir) = directory {
                std::fs::File::open(&dir)
                    .and_then(|dir_file| dir_file.sync_all())
                    .map_err(|error| {
                        SpiError::Settlement(spoke_reject(format!(
                            "the directory for {path} could not be synced: {error}"
                        )))
                    })?;
                directory = (dir != root)
                    .then(|| dir.parent().map(Path::to_path_buf))
                    .flatten();
            }
            let landed = std::fs::read_to_string(&file).map_err(|error| {
                SpiError::Settlement(spoke_reject(format!(
                    "journal target {path} could not be confirmed: {error}"
                )))
            })?;
            if landed != content {
                return Err(SpiError::Settlement(spoke_reject(format!(
                    "journal target {path} did not land with its exact bytes"
                ))));
            }
        }
        Ok(())
    }
}

/// The publisher's refusal: a retryable `INTERNAL_ERROR` — nothing the settlement leg staged
/// is durable yet, so the same review can be repeated once the medium is writable.
fn spoke_reject(message: String) -> spoke_operations::SpokeReject {
    let mut details = serde_json::Map::new();
    details.insert(
        "retry_class".to_string(),
        Value::String("retryable".to_string()),
    );
    spoke_operations::SpokeReject {
        code: spoke_operations::SpokeRejectCode::InternalError,
        message,
        details: Some(details),
    }
}
