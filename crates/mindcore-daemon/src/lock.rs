//! The single-instance lock: one daemon per store directory entry per host.
//!
//! The lock is an exclusive advisory lock on a lock file beside the store, named for the store's
//! inode identity. It is taken before the store is opened, so a second instance is refused without
//! running a migration, and because it is a file of its own it is independent of SQLite's lock
//! space — the store stays fully usable while the lock is held.
//!
//! Every name that resolves to one directory entry contends on one lock: the path itself, any
//! symlink to it (including a dangling one, whose creation lands on the target) and a hard link in
//! the same directory, which shares the inode the name is derived from. A hard link in another
//! directory is the documented limit below.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};

use crate::DaemonError;

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
        let path = lock_path(store_path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| DaemonError::Store(format!("{}: {error}", path.display())))?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(DaemonError::LockHeld(store_path.to_path_buf())),
            Err(TryLockError::Error(error)) => {
                Err(DaemonError::Store(format!("{}: {error}", path.display())))
            }
        }
    }
}

/// `<store directory>/.mindcore-store-<dev>-<ino>.lock`, creating the store when it is absent.
fn lock_path(store_path: &Path) -> Result<PathBuf, DaemonError> {
    let real = resolve(store_path)?;
    if !real.exists() {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&real)
            .map_err(|error| DaemonError::Store(format!("{}: {error}", real.display())))?;
    }
    let dir = real
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    Ok(dir.join(lock_name(&real)?))
}

/// `.mindcore-store-<dev>-<ino>.lock` — the store's inode identity, in its own directory.
///
/// Two names in one directory that reach one inode therefore answer one lock file.
#[cfg(unix)]
fn lock_name(store: &Path) -> Result<String, DaemonError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = std::fs::metadata(store)
        .map_err(|error| DaemonError::Store(format!("{}: {error}", store.display())))?;
    Ok(format!(
        ".mindcore-store-{}-{}.lock",
        metadata.dev(),
        metadata.ino()
    ))
}

// shortcut: without `dev`/`ino` the name falls back to the store's file name, so a hard link under
// another name — same directory or not — gets its own lock and could start a second daemon;
// upgrade when Windows becomes a target and needs the guarantee.
#[cfg(not(unix))]
fn lock_name(store: &Path) -> Result<String, DaemonError> {
    let name = store.file_name().ok_or_else(|| {
        DaemonError::Config(format!(
            "store_path {}: names no file to lock",
            store.display()
        ))
    })?;
    let mut lock = name.to_os_string();
    lock.push(".lock");
    Ok(lock.to_string_lossy().into_owned())
}

/// How many alias hops a store path may take. A chain longer than this is refused rather than
/// walked, so a symlink loop cannot spin here.
const MAX_ALIAS_HOPS: usize = 16;

/// The database's real path.
///
/// `canonicalize` answers it whenever the store exists. Otherwise the final component may itself
/// be a symlink to a database that does not exist yet, so the link is followed by hand: the literal
/// name is precisely what an alias defeats, and a dangling alias of a first-created store must land
/// on the target's name and therefore the target's lock.
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

    /// The temp dir with its own spelling resolved, so comparisons are about lock names and not
    /// about `/var` versus `/private/var`.
    fn resolved(path: &Path) -> PathBuf {
        std::fs::canonicalize(path).expect("resolved path")
    }

    /// The lock path for a store that already exists, without creating anything.
    fn lock_file_for(store: &Path) -> PathBuf {
        lock_path(store).expect("lock path")
    }

    fn assert_held(error: &DaemonError, locked: &Path) {
        assert!(
            matches!(error, DaemonError::LockHeld(path) if path == locked),
            "a held lock is LockHeld naming {locked:?}: {error:?}"
        );
        assert_eq!(error.exit_code(), 2);
    }

    /// A store's bytes that no attempt may disturb.
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

    /// The lock is a separate file beside the store, named for the store's inode, and the store's
    /// own bytes and length are untouched by taking it.
    #[cfg(unix)]
    #[test]
    fn the_lock_is_a_separate_file_beside_the_store() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, BYTES).expect("create the store file");
        let metadata = std::fs::metadata(&store).expect("store metadata");

        let lock = lock_file_for(&store);
        assert_eq!(
            lock.parent(),
            Some(resolved(dir.path()).as_path()),
            "the lock lives in the store's directory: {}",
            lock.display()
        );
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

    /// The point of a lock file beside the store rather than on it: the store opens while held.
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

    /// A store reached through a symlink is the same directory entry, so it is the same lock.
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

    /// A dangling alias of a store that does not exist yet: acquiring through the alias must land
    /// on the target's name, so the target answers the same lock.
    #[cfg(unix)]
    #[test]
    fn a_dangling_alias_locks_the_target() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let resolved_dir = resolved(dir.path());
        let store = dir.path().join("mind.db");
        let alias = dir.path().join("alias.db");
        std::os::unix::fs::symlink(&store, &alias).expect("dangling symlink");
        assert!(!store.exists(), "the target does not exist yet");

        // Nothing has created the target: acquiring the alias is what creates it.
        let first = StoreLock::acquire(&alias).expect("acquire the dangling alias");
        assert!(store.exists(), "the alias created the target");

        let metadata = std::fs::metadata(&store).expect("target metadata");
        let expected = resolved_dir.join(format!(
            ".mindcore-store-{}-{}.lock",
            metadata.dev(),
            metadata.ino()
        ));
        assert!(
            expected.exists(),
            "the alias locked the target's name: {}",
            expected.display()
        );
        assert!(
            !resolved_dir.join("alias.db.lock").exists(),
            "no lock was named for the alias itself"
        );

        let error = StoreLock::acquire(&store).expect_err("acquire the target");
        assert_held(&error, &store);

        drop(first);
        StoreLock::acquire(&store).expect("acquire after the guard dropped");
    }

    /// A hard link in the same directory shares the inode, so it shares the lock.
    #[cfg(unix)]
    #[test]
    fn a_hard_link_in_the_same_directory_reaches_the_same_lock() {
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

    /// The documented limit, pinned so it stays explicit: a hard link in another directory names a
    /// lock in that directory, so the two names do not contend.
    ///
    /// `shortcut:` one daemon is guaranteed per directory entry, its symlink targets and its
    /// same-directory hard links; a cross-directory hard link escapes that and is tracked as an
    /// issue for the iteration that needs it.
    #[cfg(unix)]
    #[test]
    fn a_cross_directory_hard_link_names_its_own_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let elsewhere = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, BYTES).expect("create the store file");
        let link = elsewhere.path().join("mind.db");
        std::fs::hard_link(&store, &link).expect("hard link across directories");

        assert_eq!(lock_file_for(&store), lock_file_for(&link));

        // Two directory entries, two inodes' worth of naming, two locks — the pinned limit.
        let first = StoreLock::acquire(&store).expect("acquire through the first name");
        let second = StoreLock::acquire(&link).expect("acquire through the other directory");
        assert_eq!(std::fs::read(&link).expect("read"), BYTES);
        drop(first);
        drop(second);
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
