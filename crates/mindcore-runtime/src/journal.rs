//! The one journal publisher the crate offers: `journal_dir(root)` writes every journaled
//! file target under `root` at the target's own path.

use std::path::{Component, Path, PathBuf};

use mindcore_spi::SpiError;
use serde_json::Value;

/// A file publisher over `root`: each target's `content` bytes land at `root` joined with the
/// target's `path`, parent directories are created, and `Ok(())` is returned only when every
/// target's exact bytes are durable.
///
/// Confinement is filesystem resolution, not spelling. A target path that is absolute or
/// carries any `..` — a `ParentDir`, `RootDir` or `Prefix` component — is refused before its
/// first byte. What remains is resolved: the root and every target parent are canonicalized,
/// and a parent whose resolution leaves the canonical root — a symlink below the root
/// pointing outside it, at the destination or anywhere in between — is refused before
/// anything is created or written. The root the publisher serves is the canonical root: a
/// host that passes a symlinked `root` publishes inside what it resolves to.
///
/// Durability: each target syncs its file, then every directory entry the target depends on,
/// deepest first, ending at the deepest pre-existing ancestor of its parent — inclusive. That
/// directory already exists, so its own creation entry is the host's; everything below it was
/// created by this publication (on this attempt or an earlier failed one) and is synced on
/// every attempt. When `root` itself is absent, `create_dir_all` creates it, and the chain
/// ends at its existing parent: the entry that names the freshly created root is persisted
/// before `Ok(())`, so a settled record can never outlive the projection tree that proves it.
///
/// Permission requirement: syncing a directory opens it for reading, which POSIX refuses for
/// a search-only directory, so the host must grant read on the deepest pre-existing ancestor
/// and everything the publication creates below it. Ancestors above that anchor need only
/// traversal: the publisher never opens them.
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
            // The spelling check is refused before any filesystem effect: an absolute path
            // or one carrying `..` would escape `root` under `join`, whatever it names.
            let confined = !Path::new(path).is_absolute()
                && Path::new(path)
                    .components()
                    .all(|component| matches!(component, Component::Normal(_) | Component::CurDir));
            if !confined {
                return Err(SpiError::InvalidInput(format!(
                    "journal target {path} is not confined to the journal root"
                )));
            }
            let file = root.join(path);
            let parent = match file.parent() {
                Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
                // A target at the publisher root has the root itself as its parent: the
                // spelling check above has already erased any `.` components, so the empty
                // parent is reached only when the target names one plain file.
                _ => root.clone(),
            };
            // The deepest pre-existing ancestor of the parent — the sync anchor. Existence
            // is `symlink_metadata`: a dangling symlink is an existing entry, so it is never
            // silently replaced by a created directory.
            let anchor = deepest_existing_ancestor(&parent);
            // Symlink escape, refused before anything is created — but only where a symlink
            // can exist: inside the root. An anchor strictly within the root means the root
            // itself exists, so both canonicalize; an anchor at or above the root means the
            // root itself is missing, and a root that does not exist yet holds no symlinks to
            // escape through — its containment is the post-creation check's job.
            if anchor != root && anchor.starts_with(&root) {
                let canonical_root = root.canonicalize().map_err(|error| {
                    SpiError::Settlement(spoke_reject(format!(
                        "the journal root {} could not be resolved: {error}",
                        root.display()
                    )))
                })?;
                let resolved = anchor.canonicalize().map_err(|error| {
                    SpiError::Settlement(spoke_reject(format!(
                        "the directory {} could not be resolved: {error}",
                        anchor.display()
                    )))
                })?;
                if !resolved.starts_with(&canonical_root) {
                    return Err(SpiError::InvalidInput(format!(
                        "journal target {path} resolves outside the journal root"
                    )));
                }
            }
            std::fs::create_dir_all(&parent).map_err(|error| {
                SpiError::Settlement(spoke_reject(format!(
                    "the directory for {path} could not be created: {error}"
                )))
            })?;
            // Resolution after creation: every component of the parent now exists, so its
            // canonical form must sit inside the canonical root — a symlink introduced at the
            // destination or resolved differently by the creating platform is refused before
            // the first byte is written.
            let canonical_root = root.canonicalize().map_err(|error| {
                SpiError::Settlement(spoke_reject(format!(
                    "the journal root {} could not be resolved: {error}",
                    root.display()
                )))
            })?;
            let resolved = parent.canonicalize().map_err(|error| {
                SpiError::Settlement(spoke_reject(format!(
                    "the directory {} could not be resolved: {error}",
                    parent.display()
                )))
            })?;
            if !resolved.starts_with(&canonical_root) {
                return Err(SpiError::InvalidInput(format!(
                    "journal target {path} resolves outside the journal root"
                )));
            }
            std::fs::write(&file, content).map_err(|error| {
                SpiError::Settlement(spoke_reject(format!(
                    "journal target {path} could not be written: {error}"
                )))
            })?;
            // Durability, not visibility: the write must reach the medium before the caller
            // may settle the intent. Sync the file's contents, then every directory entry the
            // target depends on, deepest first, ending at the deepest pre-existing ancestor —
            // inclusive, because the entry that names what this publication created below it
            // (the fresh journal root included) lives in that directory.
            std::fs::File::open(&file)
                .and_then(|file| file.sync_all())
                .map_err(|error| {
                    SpiError::Settlement(spoke_reject(format!(
                        "journal target {path} could not be synced: {error}"
                    )))
                })?;
            let mut cursor = Some(parent.clone());
            while let Some(directory) = cursor {
                std::fs::File::open(&directory)
                    .and_then(|directory| directory.sync_all())
                    .map_err(|error| {
                        SpiError::Settlement(spoke_reject(format!(
                            "the directory {} could not be synced: {error}",
                            directory.display()
                        )))
                    })?;
                if directory == anchor {
                    break;
                }
                cursor = directory.parent().map(Path::to_path_buf);
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

/// The deepest ancestor of `path` that exists on the filesystem — symlink entries included —
/// or the path itself when it already exists.
///
/// This is the durability anchor of one publication: it existed before the attempt, so the
/// entry that names it is the host's, while every entry below it was created by this
/// publication (on this attempt or an earlier failed one) and must be synced on every
/// attempt — including the entry in this directory that names a freshly created root.
fn deepest_existing_ancestor(path: &Path) -> PathBuf {
    let mut cursor = path.to_path_buf();
    loop {
        if std::fs::symlink_metadata(&cursor).is_ok() {
            return cursor;
        }
        match cursor.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => cursor = parent.to_path_buf(),
            // The current directory for a relative path, the filesystem root for an absolute
            // one: both always exist.
            _ => return cursor,
        }
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
