//! The single-instance lock: one daemon per store file per host.
//!
//! The lock is an exclusive advisory lock on the store file itself, taken before the store is
//! opened so a second instance is refused without running a migration. Because the kernel locks the
//! inode, every name that reaches one database contends on one lock — a symlink, a dangling symlink
//! (whose creation lands on the target), a hard link in the same directory and one in another
//! directory alike. No second file is created and no path is resolved.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::Path;

use crate::DaemonError;

/// The held lock. Dropping it releases the lock; the file it lives on is the store itself.
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
    /// Takes the lock on the store file, or answers [`DaemonError::LockHeld`].
    ///
    /// The file is opened for read and write and created when absent, so a first start can lock a
    /// store it is about to create. It is never truncated: a losing attempt must leave the store's
    /// bytes as it found them.
    pub fn acquire(store_path: &Path) -> Result<Self, DaemonError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(store_path)
            .map_err(|error| DaemonError::Store(format!("{}: {error}", store_path.display())))?;
        // shortcut: this holds a POSIX `flock` on the store inode while SQLite takes its own
        // `fcntl` record locks (the unix VFS default), so the two lock spaces stay apart; a build
        // that enables SQLite's `flock` locking-style fallback would share this space — upgrade
        // then.
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(DaemonError::LockHeld(store_path.to_path_buf())),
            Err(TryLockError::Error(error)) => Err(DaemonError::Store(format!(
                "{}: {error}",
                store_path.display()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Asserts a losing attempt answers `LockHeld` naming the store it tried to lock.
    fn assert_held(error: &DaemonError, locked: &Path) {
        assert!(
            matches!(error, DaemonError::LockHeld(path) if path == locked),
            "a held lock is LockHeld on {locked:?}: {error:?}"
        );
        assert_eq!(error.exit_code(), 2);
    }

    #[test]
    fn a_second_acquire_is_refused_until_the_first_guard_drops() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, b"store bytes").expect("create the store file");

        let first = StoreLock::acquire(&store).expect("first acquire");
        let error = StoreLock::acquire(&store).expect_err("second acquire");
        assert_held(&error, &store);
        assert_eq!(
            std::fs::read(&store).expect("read the store"),
            b"store bytes",
            "a losing attempt leaves the store's bytes untouched"
        );

        drop(first);
        let second = StoreLock::acquire(&store).expect("acquire after the guard dropped");
        drop(second);
        assert_eq!(
            std::fs::read(&store).expect("read the store"),
            b"store bytes",
            "the lock file is the store, and its bytes survived both guards"
        );
    }

    /// A store reached through a symlink is the same inode, so it is the same lock.
    #[cfg(unix)]
    #[test]
    fn an_alias_reaches_the_same_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, b"store bytes").expect("create the store file");
        let alias = dir.path().join("alias.db");
        std::os::unix::fs::symlink(&store, &alias).expect("symlink");

        let first = StoreLock::acquire(&store).expect("acquire through the real name");
        let error = StoreLock::acquire(&alias).expect_err("acquire through the alias");
        assert_held(&error, &alias);

        drop(first);
        StoreLock::acquire(&alias).expect("the alias acquires once the guard drops");
    }

    /// A dangling alias of a store that does not exist yet: creating through the link lands on the
    /// target name, so the lock is taken on the target's inode.
    #[cfg(unix)]
    #[test]
    fn a_dangling_alias_reaches_the_same_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        let alias = dir.path().join("alias.db");
        std::os::unix::fs::symlink(&store, &alias).expect("dangling symlink");

        let first = StoreLock::acquire(&store).expect("acquire through the target");
        assert!(
            store.exists(),
            "the lock created the store at the target name"
        );
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
        std::fs::write(&store, b"store bytes").expect("create the store file");
        let link = dir.path().join("hard.db");
        std::fs::hard_link(&store, &link).expect("hard link");

        let first = StoreLock::acquire(&store).expect("acquire through the first name");
        let error = StoreLock::acquire(&link).expect_err("acquire through the hard link");
        assert_held(&error, &link);

        drop(first);
        StoreLock::acquire(&link).expect("the hard link acquires once the guard drops");
    }

    /// The case a name-derived lock could never cover: a hard link in another directory is still
    /// one inode, so it is still one lock.
    #[cfg(unix)]
    #[test]
    fn a_hard_link_in_another_directory_reaches_the_same_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let elsewhere = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, b"store bytes").expect("create the store file");
        let link = elsewhere.path().join("mind.db");
        std::fs::hard_link(&store, &link).expect("hard link across directories");

        let first = StoreLock::acquire(&store).expect("acquire through the first name");
        let error = StoreLock::acquire(&link).expect_err("acquire through the other directory");
        assert_held(&error, &link);
        assert_eq!(
            std::fs::read(&link).expect("read through the link"),
            b"store bytes",
            "a losing attempt leaves the store's bytes untouched"
        );

        drop(first);
        StoreLock::acquire(&link).expect("the other name acquires once the guard drops");
    }

    /// A lock that created the store leaves a real file behind, so the store can be opened after it.
    #[test]
    fn a_first_acquire_leaves_the_store_file_in_place() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");

        let first = StoreLock::acquire(&store).expect("first acquire");
        assert!(store.exists(), "the lock created the store file it holds");
        drop(first);

        assert!(store.exists(), "the store file outlives the guard");
        assert_eq!(
            std::fs::metadata(&store).expect("metadata").len(),
            0,
            "the created file is empty, ready for the store to initialize"
        );
    }
}
