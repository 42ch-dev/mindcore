//! The one journal publisher the crate offers: `journal_dir(root)` writes every journaled
//! file target under `root` at the target's own path.

use std::path::{Component, Path, PathBuf};

use mindcore_spi::SpiError;
use serde_json::Value;

/// A file publisher over `root`: each target's `content` bytes land at `root` joined with the
/// target's `path`, parent directories are created, and `Ok(())` is returned only when every
/// target's exact bytes are durable.
///
/// Confinement: a target path that is absolute or carries any `..` — a `ParentDir`, `RootDir`
/// or `Prefix` component — is refused before its first byte, so no target can ever write
/// outside `root` whatever a journaled row holds.
///
/// The sync anchor is `root` itself. Publication creates no directory at or above it: the host
/// establishes `root` (or the path it will be created at) as already durable, so every entry
/// publication depends on lies within the publisher's own tree. Each target syncs its file,
/// then every directory of that tree its entry depends on, deepest first, ending at `root`.
/// The chain is a function of the target's path alone — never of which directories happen to
/// exist — so a retry after a publication that failed between creating directories and
/// syncing them syncs the same chain as the attempt that failed.
///
/// Permission requirement: syncing a directory opens it for reading, which POSIX refuses for
/// a search-only directory, so the host must grant read on `root` and every directory below
/// it. A write-only journal root cannot be published: the write itself would succeed, but the
/// durability sync that must precede settlement would be refused for a reason the write never
/// hit.
///
/// Target paths are the adapter's own (`records/<id>.md`, `candidates/<id>.md`,
/// `profiles/<digest>.md`): they are confined to `root` as given rather than re-derived here.
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
            // Confinement is refused before the first byte of this target: an absolute path
            // or one carrying `..` would escape `root` under `join`, whatever it names.
            let confined = !Path::new(path).is_absolute()
                && Path::new(path).components().all(|component| {
                    matches!(
                        component,
                        Component::Normal(_) | Component::CurDir
                    )
                });
            if !confined {
                return Err(SpiError::InvalidInput(format!(
                    "journal target {path} is not confined to the journal root"
                )));
            }
            let file = root.join(path);
            let parent = match file.parent() {
                Some(parent) if !parent.as_os_str().is_empty() => parent,
                // A target at the publisher root has the root itself as its parent: the
                // confinement check above has already erased any `.` components, so the
                // empty parent is reached only when the target names one plain file.
                _ => root.as_path(),
            };
            std::fs::create_dir_all(parent).map_err(|error| {
                SpiError::Settlement(spoke_reject(format!(
                    "the directory for {path} could not be created: {error}"
                )))
            })?;
            std::fs::write(&file, content).map_err(|error| {
                SpiError::Settlement(spoke_reject(format!(
                    "journal target {path} could not be written: {error}"
                )))
            })?;
            // Durability, not visibility: the write must reach the medium before the caller
            // may settle the intent. Sync the file's contents, then every directory of the
            // publisher's own tree its entry depends on, deepest first, ending at `root` —
            // the anchor the host established as durable.
            std::fs::File::open(&file)
                .and_then(|file| file.sync_all())
                .map_err(|error| {
                    SpiError::Settlement(spoke_reject(format!(
                        "journal target {path} could not be synced: {error}"
                    )))
                })?;
            let mut cursor = Some(parent.to_path_buf());
            while let Some(directory) = cursor {
                std::fs::File::open(&directory)
                    .and_then(|directory| directory.sync_all())
                    .map_err(|error| {
                        SpiError::Settlement(spoke_reject(format!(
                            "the directory {} could not be synced: {error}",
                            directory.display()
                        )))
                    })?;
                if directory == root {
                    break;
                }
                cursor = directory
                    .components()
                    .as_path()
                    .parent()
                    .map(Path::to_path_buf);
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
