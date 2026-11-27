//! The single-instance lock: one daemon per store file per host.
//!
//! The lock is an exclusive advisory lock on a lock file of its own, in a directory outside the
//! store's tree, named for the store's inode identity. Because the name is derived from the inode
//! rather than the path, every name that reaches one database contends on one lock — a symlink, a
//! dangling symlink and a hard link in any directory alike.
//!
//! The lock file is independent of SQLite's own lock space, so holding it leaves the store fully
//! usable; locking the store file itself does not work, because SQLite's `fcntl` record locks and a
//! `flock` on that file share one space on macOS, so a held lock starves the store of every read.

use std::fs::{DirBuilder, File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::DaemonError;

/// The lock holds a name outside the store's tree, so a store directory that is read-only or
/// removed does not take the lock with it, and no lock file can be mistaken for a store file.
const LOCK_DIR: &str = "mindcore-locks";

/// How many times a create-then-verify cycle is retried after a temp cleaner removed the lock file
/// between the open and the lock.
const ATTEMPTS: u8 = 3;

/// The held lock. Dropping it releases the lock and leaves the file in place.
#[derive(Debug)]
pub struct StoreLock {
    /// Held only for the lock it carries; the guard keeps it open for its whole life.
    _file: File,
}

/// The guard is `Send`, so `main` may move it into the serving phase.
const _: () = {
    const fn assert_send<T: Send>() {}
    assert_send::<StoreLock>();
};

impl StoreLock {
    /// Takes the lock for `store_path`'s database, or answers [`DaemonError::LockHeld`].
    ///
    /// A store this run is about to create is created here first, empty, so it has an inode to name
    /// the lock after; SQLite initializes an empty file on open. This runs before
    /// `MindStore::open`, so a losing instance never opens or migrates the store.
    pub fn acquire(store_path: &Path) -> Result<Self, DaemonError> {
        let store = resolve(store_path)?;
        let path = lock_path(&store)?;
        for _ in 0..ATTEMPTS {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .map_err(|error| DaemonError::Store(format!("{}: {error}", path.display())))?;
            match file.try_lock() {
                Ok(()) => {
                    // A temp cleaner may have unlinked the file between the open and the lock, in
                    // which case this handle guards an inode nobody can name. Recreate and retry.
                    if same_inode(&file, &path) {
                        return Ok(Self { _file: file });
                    }
                    drop(file);
                }
                Err(std::fs::TryLockError::WouldBlock) => {
                    return Err(DaemonError::LockHeld(store_path.to_path_buf()))
                }
                Err(std::fs::TryLockError::Error(error)) => {
                    return Err(DaemonError::Store(format!("{}: {error}", path.display())))
                }
            }
        }
        Err(DaemonError::LockHeld(store_path.to_path_buf()))
    }
}

/// The lock file for an existing store, creating the store first when it is absent.
fn lock_path(store: &Path) -> Result<PathBuf, DaemonError> {
    if !store.exists() {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(store)
            .map_err(|error| DaemonError::Store(format!("{}: {error}", store.display())))?;
    }
    Ok(lock_dir()?.join(lock_name(store)?))
}

/// The lock directory, created `0o700` so only this user can read, write or enter it.
fn lock_dir() -> Result<PathBuf, DaemonError> {
    let dir = std::env::temp_dir().join(LOCK_DIR);
    let mut builder = DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(&dir)
        .map_err(|error| DaemonError::Store(format!("{}: {error}", dir.display())))?;
    Ok(dir)
}

/// `"<dev>-<ino>.lock"` for the store's inode: the identity every alias of one file shares.
#[cfg(unix)]
fn lock_name(store: &Path) -> Result<String, DaemonError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = std::fs::metadata(store)
        .map_err(|error| DaemonError::Store(format!("{}: {error}", store.display())))?;
    Ok(format!("{}-{}.lock", metadata.dev(), metadata.ino()))
}

// shortcut: without an inode identity, the name is a hash of the resolved path, so a hard link in
// another directory is a different path and gets a different lock; upgrade when Windows becomes a
// target and needs the guarantee.
#[cfg(not(unix))]
fn lock_name(store: &Path) -> Result<String, DaemonError> {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    store.hash(&mut hasher);
    Ok(format!("{:016x}.lock", hasher.finish()))
}

/// Whether `path` still names the inode `file` holds.
#[cfg(unix)]
fn same_inode(file: &File, path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;

    match (file.metadata(), std::fs::metadata(path)) {
        (Ok(held), Ok(named)) => held.dev() == named.dev() && held.ino() == named.ino(),
        _ => false,
    }
}

#[cfg(not(unix))]
fn same_inode(_file: &File, _path: &Path) -> bool {
    true
}

/// How many alias hops a store path may take. A chain longer than this is refused rather than
/// walked, so a symlink loop cannot spin here.
const MAX_ALIAS_HOPS: usize = 16;

/// The database's real path.
///
/// `canonicalize` answers it whenever the store exists. Otherwise the final component may itself
/// be a symlink to a database that does not exist yet, so the link is followed by hand: the literal
/// name is precisely what an alias defeats, and a dangling alias of a first-created store is a case
/// the daemon must still lock once.
fn resolve(store_path: &Path) -> Result<PathBuf, DaemonError> {
    let unresolvable = |cause: std::io::Error| {
        DaemonError::Config(format!("store_path {}: {cause}", store_path.display()))
    };
    let mut path = store_path.to_path_buf();
    for _ in 0..MAX_ALIAS_HOPS {
        if let Ok(real) = std::fs::canonicalize(&path) {
            return Ok(real);
        }
        let name = path
            .file_name()
            .ok_or_else(|| {
                DaemonError::Config(format!(
                    "store_path {}: names no file to lock",
                    store_path.display()
                ))
            })?
            .to_owned();
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let resolved = std::fs::canonicalize(parent).map_err(unresolvable)?;
        let candidate = resolved.join(&name);
        // Nothing is there: the candidate is the store this run will create.
        let Ok(metadata) = std::fs::symlink_metadata(&candidate) else {
            return Ok(candidate);
        };
        if !metadata.is_symlink() {
            return Ok(candidate);
        }
        let target = std::fs::read_link(&candidate).map_err(unresolvable)?;
        path = if target.is_absolute() {
            target
        } else {
            resolved.join(target)
        };
    }
    Err(DaemonError::Config(format!(
        "store_path {}: more than {MAX_ALIAS_HOPS} symlink hops",
        store_path.display()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A lock path of this test's own, so a test never contends with another test's store.
    fn lock_file_for(store: &Path) -> PathBuf {
        lock_path(&resolve(store).expect("resolve")).expect("lock path")
    }

    fn assert_held(error: &DaemonError, locked: &Path) {
        assert!(
            matches!(error, DaemonError::LockHeld(path) if path == locked),
            "a held lock is LockHeld naming {locked:?}: {error:?}"
        );
        assert_eq!(error.exit_code(), 2);
    }

    /// The store bytes a losing attempt must not disturb.
    const BYTES: &[u8] = b"store bytes";

    #[test]
    fn a_second_acquire_is_refused_until_the_first_guard_drops() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, BYTES).expect("create the store file");

        let first = StoreLock::acquire(&store).expect("first acquire");
        let error = StoreLock::acquire(&store).expect_err("second acquire");
        assert_held(&error, &store);
        assert_eq!(std::fs::read(&store).expect("read"), BYTES);

        drop(first);
        let second = StoreLock::acquire(&store).expect("acquire after the guard dropped");
        drop(second);
        assert_eq!(std::fs::read(&store).expect("read"), BYTES);
    }

    /// The lock file lives under the lock directory, not beside the store, and the store is a
    /// separate file whose bytes nothing touched.
    #[test]
    fn the_lock_lives_under_the_lock_dir_and_not_beside_the_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, BYTES).expect("create the store file");

        let lock = lock_file_for(&store);
        assert_eq!(
            lock.parent(),
            Some(std::env::temp_dir().join(LOCK_DIR).as_path()),
            "the lock lives under the lock directory: {}",
            lock.display()
        );
        assert_ne!(lock.parent(), Some(dir.path()), "and not beside the store");
        assert_ne!(lock, store, "the lock is not the store file");

        let first = StoreLock::acquire(&store).expect("acquire");
        assert!(lock.exists(), "the lock file exists while held");
        assert_eq!(std::fs::read(&store).expect("read"), BYTES);
        assert_eq!(
            std::fs::metadata(&store).expect("metadata").len(),
            BYTES.len() as u64,
            "the store file is unchanged by the lock"
        );
        drop(first);
    }

    /// A fresh store: the lock creates it empty, and SQLite must still be able to initialize it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_fresh_store_is_created_empty_and_still_opens() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");

        let first = StoreLock::acquire(&store).expect("acquire a store that does not exist");
        assert!(store.exists(), "the lock created the store file");
        assert_eq!(
            std::fs::metadata(&store).expect("metadata").len(),
            0,
            "the created store is empty, ready for SQLite to initialize"
        );

        // The whole point of a lock outside SQLite's lock space: the store opens while held.
        let opened = mindcore_sqlite::MindStore::open(&store).await;
        assert!(
            opened.is_ok(),
            "the held lock does not starve the store: {:?}",
            opened.err()
        );
        opened.expect("opened").close().await;

        let error = StoreLock::acquire(&store).expect_err("second acquire");
        assert_held(&error, &store);
        drop(first);
        StoreLock::acquire(&store).expect("acquire after the guard dropped");
    }

    /// A store reached through a symlink is the same inode, so it is the same lock.
    #[cfg(unix)]
    #[test]
    fn an_alias_reaches_the_same_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, BYTES).expect("create the store file");
        let alias = dir.path().join("alias.db");
        std::os::unix::fs::symlink(&store, &alias).expect("symlink");

        assert_eq!(lock_file_for(&store), lock_file_for(&alias));

        let first = StoreLock::acquire(&store).expect("acquire through the real name");
        let error = StoreLock::acquire(&alias).expect_err("acquire through the alias");
        assert_held(&error, &alias);

        drop(first);
        StoreLock::acquire(&alias).expect("the alias acquires once the guard drops");
    }

    /// A dangling alias of a store that does not exist yet resolves to its target, so both names
    /// reach one inode and one lock.
    #[cfg(unix)]
    #[test]
    fn a_dangling_alias_reaches_the_same_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        let alias = dir.path().join("alias.db");
        std::os::unix::fs::symlink(&store, &alias).expect("dangling symlink");

        assert_eq!(lock_file_for(&store), lock_file_for(&alias));

        let first = StoreLock::acquire(&store).expect("acquire through the target");
        assert!(store.exists(), "the store was created at the target name");
        let error = StoreLock::acquire(&alias).expect_err("acquire through the alias");
        assert_held(&error, &alias);

        drop(first);
        StoreLock::acquire(&alias).expect("the alias acquires once the guard drops");
    }

    /// A hard link is a second directory entry for one inode, so it is the same lock.
    #[cfg(unix)]
    #[test]
    fn a_hard_link_reaches_the_same_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, BYTES).expect("create the store file");
        let link = dir.path().join("hard.db");
        std::fs::hard_link(&store, &link).expect("hard link");

        assert_eq!(lock_file_for(&store), lock_file_for(&link));

        let first = StoreLock::acquire(&store).expect("acquire through the first name");
        let error = StoreLock::acquire(&link).expect_err("acquire through the hard link");
        assert_held(&error, &link);
        assert_eq!(std::fs::read(&link).expect("read"), BYTES);

        drop(first);
        StoreLock::acquire(&link).expect("the hard link acquires once the guard drops");
    }

    /// The case no lock file beside the store could cover: a hard link in another directory is
    /// still one inode, so it is still one lock, and one lock directory serves both.
    #[cfg(unix)]
    #[test]
    fn a_hard_link_in_another_directory_reaches_the_same_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let elsewhere = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, BYTES).expect("create the store file");
        let link = elsewhere.path().join("mind.db");
        std::fs::hard_link(&store, &link).expect("hard link across directories");

        assert_eq!(lock_file_for(&store), lock_file_for(&link));

        let first = StoreLock::acquire(&store).expect("acquire through the first name");
        let error = StoreLock::acquire(&link).expect_err("acquire through the other directory");
        assert_held(&error, &link);
        assert_eq!(std::fs::read(&link).expect("read"), BYTES);

        drop(first);
        StoreLock::acquire(&link).expect("the other name acquires once the guard drops");
    }

    /// An unusable store parent is a config fault naming the path, not a lock failure.
    #[test]
    fn an_unresolvable_parent_is_a_config_fault() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("absent").join("mind.db");

        let error = StoreLock::acquire(&store).expect_err("unresolvable parent");
        assert!(
            matches!(&error, DaemonError::Config(message) if message.contains("store_path")),
            "a missing parent is a config fault naming the path: {error:?}"
        );
        assert_eq!(error.exit_code(), 1);
        assert!(!dir.path().join("absent").exists(), "nothing was created");
    }
}
