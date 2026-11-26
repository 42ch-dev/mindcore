//! The one journal publisher the crate offers: `journal_dir(root)` writes every journaled
//! file target under `root` at the target's own path.

use std::path::{Path, PathBuf};

use mindcore_spi::SpiError;
use serde_json::Value;

/// A file publisher over `root`: each target's `content` bytes land at `root` joined with the
/// target's `path`, parent directories are created, and `Ok(())` is returned only when every
/// target's exact bytes are durable — the file contents are synced to disk, and so is every
/// directory entry the publication created, up to and including the nearest ancestor that
/// already existed when publication began. `root` itself may be created here: its entry in
/// its parent is synced as part of that chain, so a crash cannot leave a settled intent
/// naming a tree the medium no longer holds.
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
            let Some(parent) = file.parent() else {
                return Err(SpiError::InvalidInput(format!(
                    "journal target {path} has no parent directory"
                )));
            };
            // Decide the durable chain before creating anything: every directory that does
            // not exist yet, deepest first, and the nearest ancestor that does — its entry is
            // the one a newly created topmost directory lands in.
            let chain = durable_chain(parent, Path::exists);
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
            // may settle the intent. Sync the file's contents, then each directory entry the
            // publication created, deepest first.
            std::fs::File::open(&file)
                .and_then(|file| file.sync_all())
                .map_err(|error| {
                    SpiError::Settlement(spoke_reject(format!(
                        "journal target {path} could not be synced: {error}"
                    )))
                })?;
            for directory in &chain {
                std::fs::File::open(directory)
                    .and_then(|dir| dir.sync_all())
                    .map_err(|error| {
                        SpiError::Settlement(spoke_reject(format!(
                            "the directory {} could not be synced: {error}",
                            directory.display()
                        )))
                    })?;
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

/// The directories whose entries must be durable for one target to count as published,
/// deepest first: every directory from `parent` upward that `exists` reports as absent —
/// these are the ones the publication creates — and the nearest ancestor that already exists,
/// whose entry names the topmost created directory. Stops at an existing directory, or at the
/// filesystem root when nothing on the way exists yet.
///
/// `exists` is a parameter so the ordering is provable without touching a filesystem.
fn durable_chain(parent: &Path, mut exists: impl FnMut(&Path) -> bool) -> Vec<PathBuf> {
    let mut chain = Vec::new();
    let mut cursor = Some(parent);
    while let Some(directory) = cursor {
        chain.push(directory.to_path_buf());
        if exists(directory) {
            break;
        }
        cursor = directory.parent();
    }
    chain
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

#[cfg(test)]
mod tests {
    use super::durable_chain;
    use std::path::{Path, PathBuf};

    fn paths(chain: &[PathBuf]) -> Vec<&str> {
        chain
            .iter()
            .map(|path| path.to_str().expect("utf-8"))
            .collect()
    }

    /// A root that does not exist yet is itself a created entry: the chain reaches its
    /// pre-existing parent, so syncing the chain persists the root's own directory entry
    /// as well as the entries beneath it.
    #[test]
    fn a_created_root_chain_reaches_its_existing_parent() {
        let parent = Path::new("/scratch/one/two/records");
        let chain = durable_chain(parent, |directory| directory == Path::new("/scratch"));
        assert_eq!(
            paths(&chain),
            vec![
                "/scratch/one/two/records",
                "/scratch/one/two",
                "/scratch/one",
                "/scratch"
            ],
            "deepest first, ending at the nearest existing ancestor"
        );
    }

    /// Nothing created: only the target's own parent is synced, and no ancestor is touched.
    #[test]
    fn an_existing_parent_chain_stops_at_it() {
        let parent = Path::new("/scratch/one/two/records");
        let chain = durable_chain(parent, |_| true);
        assert_eq!(paths(&chain), vec!["/scratch/one/two/records"]);
    }

    /// The chain walks to the filesystem root when nothing on the way exists: the root is the
    /// last existing ancestor and is synced too.
    #[test]
    fn a_chain_with_no_existing_ancestor_ends_at_the_root() {
        let parent = Path::new("/one/two");
        let chain = durable_chain(parent, |directory| directory == Path::new("/"));
        assert_eq!(paths(&chain), vec!["/one/two", "/one", "/"]);
    }
}
