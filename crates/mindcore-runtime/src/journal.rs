//! The one journal publisher the crate offers: `journal_dir(root)` writes every journaled
//! file target under `root` at the target's own path.

use std::path::{Component, Path, PathBuf};

use mindcore_spi::SpiError;
use serde_json::Value;

/// A file publisher over `root`: each target's `content` bytes land at `root` joined with the
/// target's `path`, parent directories are created, and `Ok(())` is returned only when every
/// target's exact bytes are durable.
///
/// The root is the host's, not the publisher's: it must exist — durable — before the first
/// publication, because the entry that names it lives in its parent and only the host can
/// establish that durability. A missing root is refused with the requirement named, and the
/// publisher never creates it. Everything the publisher creates is beneath the root.
///
/// Confinement is filesystem resolution, not spelling. A target path that is absolute or
/// carries any `..` — a `ParentDir`, `RootDir` or `Prefix` component — is refused before its
/// first byte. What remains is resolved: the root and every target parent are canonicalized,
/// and a parent whose resolution leaves the canonical root — a symlink below the root
/// pointing outside it, at the destination or anywhere in between — is refused before
/// anything is created or written. The root the publisher serves is the canonical root: a
/// host that passes a symlinked `root` publishes inside what it resolves to. At the target
/// file itself no symlink can redirect the write: the bytes are written to a temporary file
/// created exclusively in the same directory and renamed over the target atomically — rename
/// replaces the directory entry, so a symlink at the target is replaced rather than followed.
/// Refusal applies instead to a symlink at the temporary name and, as above, to an escaping
/// parent chain.
///
/// Durability: each target syncs its file, then every directory the publication created or
/// wrote into beneath the root, deepest first, ending at the root itself — inclusive. The
/// chain is a pure function of the target's path, never of which directories happen to exist,
/// so a retry after a publication that failed between creating directories and syncing them
/// syncs the same chain as the attempt that failed. The root's own creation entry is the
/// host's and never the publisher's to sync: the host established it durable before the first
/// publication, which is exactly why the publisher requires the root to exist.
///
/// Permission requirement: syncing a directory opens it for reading, which POSIX refuses for
/// a search-only directory, so the host must grant read on `root` and everything beneath it.
/// Ancestors above the root need only traversal: the publisher never opens them.
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
            // The root is the host's: it must already exist — durable — before the first
            // publication. The publisher never creates it, because the entry that names it
            // lives in its parent and only the host can establish that durability.
            if std::fs::symlink_metadata(&root).is_err() {
                return Err(SpiError::InvalidInput(format!(
                    "journal root {} does not exist: the host must create it — durable — before publication",
                    root.display()
                )));
            }
            // Symlink escape, refused before anything is created — but only where a symlink
            // can exist: inside the root. The deepest pre-existing component strictly within
            // the root is canonicalized and must resolve inside the canonical root; a target
            // whose parent sits in a freshly creatable subtree has no symlink to escape
            // through yet, and its containment is the post-creation check's job.
            let anchor = deepest_existing_ancestor(&parent);
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
            // The write itself can never follow a symlink at the target: the bytes land in a
            // temporary file inside the target's own directory, are synced there, and the
            // `rename` then replaces the target entry atomically — rename swaps the directory
            // entry, it never follows what the entry points to, so a symlink swapped in at any
            // moment before it is replaced itself rather than written through. An existing
            // regular file is the retry of this target's earlier write and is replaced with
            // the identical bytes; any other pre-existing non-file entry is refused first, so
            // a directory or device node can never be clobbered by the rename.
            if let Ok(metadata) = std::fs::symlink_metadata(&file) {
                if !metadata.file_type().is_file() && !metadata.file_type().is_symlink() {
                    return Err(SpiError::InvalidInput(format!(
                        "journal target {path} already exists as a non-file entry"
                    )));
                }
            }
            let temporary = parent.join(format!(
                ".journal-{}-{pathhash:016x}.tmp",
                std::process::id(),
                pathhash = {
                    // A stable hash of the target path: retries of the same target reuse one
                    // temporary name, so a crashed earlier attempt's leftover is overwritten
                    // rather than accumulated.
                    use std::hash::{Hash, Hasher};
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    file.hash(&mut hasher);
                    hasher.finish()
                }
            ));
            // The temporary itself is created exclusively: `create_new` fails on any existing
            // entry — a pre-planted symlink included — and otherwise creates a fresh regular
            // file, so the write can never follow a symlink at the temporary name either. A
            // leftover regular file is this target's own crashed attempt and is removed before
            // the retry; a symlink or any other entry is refused.
            let mut attempts = 0u32;
            let mut temporary_file = loop {
                match std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&temporary)
                {
                    Ok(temporary_file) => break temporary_file,
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        match std::fs::symlink_metadata(&temporary) {
                            Ok(metadata) if metadata.file_type().is_file() => {
                                std::fs::remove_file(&temporary).map_err(|error| {
                                    SpiError::Settlement(spoke_reject(format!(
                                        "the leftover temporary for {path} could not be removed: {error}"
                                    )))
                                })?;
                            }
                            _ => {
                                return Err(SpiError::InvalidInput(format!(
                                    "the temporary file for journal target {path} already exists as a non-file entry"
                                )));
                            }
                        }
                        attempts += 1;
                        if attempts >= 8 {
                            return Err(SpiError::Settlement(spoke_reject(format!(
                                "the temporary file for journal target {path} could not be created exclusively"
                            ))));
                        }
                    }
                    Err(error) => {
                        return Err(SpiError::Settlement(spoke_reject(format!(
                            "journal target {path} could not be written: {error}"
                        ))));
                    }
                }
            };
            use std::io::Write as _;
            temporary_file
                .write_all(content.as_bytes())
                .map_err(|error| {
                    SpiError::Settlement(spoke_reject(format!(
                        "journal target {path} could not be written: {error}"
                    )))
                })?;
            temporary_file.sync_all().map_err(|error| {
                SpiError::Settlement(spoke_reject(format!(
                    "journal target {path} could not be synced: {error}"
                )))
            })?;
            drop(temporary_file);
            std::fs::rename(&temporary, &file).map_err(|error| {
                SpiError::Settlement(spoke_reject(format!(
                    "journal target {path} could not be renamed into place: {error}"
                )))
            })?;
            // Durability, not visibility: the write must reach the medium before the caller
            // may settle the intent. The temporary's contents were synced before the rename;
            // what remains is the directory entries: the rename itself, and every directory the
            // publication created or wrote into beneath the root, deepest first, ending at the
            // root.
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
                if directory == root {
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
