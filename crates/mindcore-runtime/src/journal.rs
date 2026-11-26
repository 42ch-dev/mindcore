//! The one journal publisher the crate offers: `journal_dir(root)` writes every journaled
//! file target under `root` at the target's own path.

use std::path::{Path, PathBuf};

use mindcore_spi::SpiError;
use serde_json::Value;

/// A file publisher over `root`: each target's `content` bytes land at `root` joined with the
/// target's `path`, parent directories are created, and `Ok(())` is returned only when every
/// target's exact bytes are durable.
///
/// Durability covers the whole required ancestry of each target, deepest first, through a
/// bounded anchor: the filesystem root for an absolute path, the current directory for a
/// relative one. The ancestry is a function of the target's path alone — never of which
/// directories happen to exist — so a retry after a publication that failed between creating
/// directories and syncing them is never weaker than the attempt that failed: it syncs the
/// same chain, including the entries the failed attempt left behind.
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
            // A target at the publisher root has the empty path as its parent; the current
            // directory is what that names, and it is this publication's anchor.
            let parent = match file.parent() {
                Some(parent) if !parent.as_os_str().is_empty() => parent,
                _ => Path::new("."),
            };
            let chain = durable_chain(parent);
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
            // may settle the intent. Sync the file's contents, then the whole ancestry the
            // target's entry depends on, deepest first.
            std::fs::File::open(&file)
                .and_then(|file| file.sync_all())
                .map_err(|error| {
                    SpiError::Settlement(spoke_reject(format!(
                        "journal target {path} could not be synced: {error}"
                    )))
                })?;
            for directory in &chain {
                std::fs::File::open(directory)
                    .and_then(|directory| directory.sync_all())
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

/// The directories whose entries one target depends on, deepest first, through the bounded
/// anchor of its path: every ancestor named by the path, and then `/` for an absolute path or
/// `.` for a relative one.
///
/// Existence is deliberately not consulted: the chain is the same for a first attempt and for
/// a retry after a failed attempt left some of those directories behind, so a retry can never
/// skip a directory entry the failed attempt created.
fn durable_chain(parent: &Path) -> Vec<PathBuf> {
    let mut chain = Vec::new();
    let mut cursor = parent.to_path_buf();
    loop {
        if cursor.as_os_str().is_empty() {
            chain.push(PathBuf::from("."));
            break;
        }
        let at_anchor = cursor == Path::new(".");
        chain.push(cursor.clone());
        if at_anchor {
            break;
        }
        match cursor.parent() {
            Some(ancestor) => cursor = ancestor.to_path_buf(),
            // An absolute path's anchor `/` has no parent and was already pushed.
            None => break,
        }
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
    use super::{durable_chain, journal_dir};
    use serde_json::json;
    use std::path::{Path, PathBuf};

    fn paths(chain: &[PathBuf]) -> Vec<&str> {
        chain
            .iter()
            .map(|path| path.to_str().expect("utf-8"))
            .collect()
    }

    /// An absolute ancestry runs deepest first through `/`, even when nothing exists yet.
    #[test]
    fn an_absolute_chain_ends_at_the_filesystem_root() {
        let chain = durable_chain(Path::new("/scratch/one/two/records"));
        assert_eq!(
            paths(&chain),
            vec![
                "/scratch/one/two/records",
                "/scratch/one/two",
                "/scratch/one",
                "/scratch",
                "/"
            ],
            "every named ancestor, then the anchor"
        );
    }

    /// A relative ancestry anchors at the current directory: the empty parent path never
    /// reaches `File::open`, and no path in the chain is empty.
    #[test]
    fn a_relative_chain_anchors_at_the_current_directory() {
        let chain = durable_chain(Path::new("journal/records"));
        assert_eq!(paths(&chain), vec!["journal/records", "journal", "."]);
        assert!(
            chain.iter().all(|path| !path.as_os_str().is_empty()),
            "an empty path can never be opened"
        );
    }

    /// `journal_dir("journal")` builds `journal/records/…` style parents, so the relative
    /// anchor is exercised exactly as the first publication of such a root would.
    #[test]
    fn a_relative_parent_under_the_current_directory_stops_at_it() {
        assert_eq!(
            paths(&durable_chain(Path::new("./records"))),
            vec!["./records", "."]
        );
        assert_eq!(paths(&durable_chain(Path::new("."))), vec!["."]);
    }

    /// The chain depends on the path, never on which directories already exist: this is what
    /// keeps a retry from being weaker than a failed first attempt.
    #[test]
    fn the_chain_is_identical_before_and_after_the_directories_exist() {
        let directory = tempfile::TempDir::new().expect("temporary directory");
        let parent = directory.path().join("one/two/records");
        let before = durable_chain(&parent);
        std::fs::create_dir_all(&parent).expect("create the tree");
        let after = durable_chain(&parent);
        assert_eq!(
            paths(&before),
            paths(&after),
            "a retry sees the same chain as the attempt that created the tree"
        );
    }

    /// The failure/retry boundary: a refusal after an earlier target already created a tree
    /// leaves directories behind; the retry syncs the same chain and lands both targets.
    #[test]
    fn a_retry_after_a_refused_publication_syncs_the_same_chain() {
        let directory = tempfile::TempDir::new().expect("temporary directory");
        let root = directory.path().to_path_buf();
        let parent = root.join("nested").join("records");
        let before = durable_chain(&parent);

        let publish = journal_dir(&root);
        let refused = publish(&[
            json!({ "path": "nested/records/one.md", "content": "one" }),
            // The second target has no content: the first has already landed and created the
            // tree when this one is refused.
            json!({ "path": "nested/records/two.md" }),
        ]);
        assert!(
            refused.is_err(),
            "the malformed target refuses the publication"
        );
        assert!(parent.is_dir(), "the failed attempt created the tree");

        let after = durable_chain(&parent);
        assert_eq!(
            paths(&before),
            paths(&after),
            "the retry syncs the same ancestry, including what the failed attempt created"
        );

        publish(&[
            json!({ "path": "nested/records/one.md", "content": "one" }),
            json!({ "path": "nested/records/two.md", "content": "two" }),
        ])
        .expect("the retry publishes");
        assert_eq!(
            std::fs::read_to_string(parent.join("two.md")).expect("the retried target"),
            "two"
        );
    }
}
