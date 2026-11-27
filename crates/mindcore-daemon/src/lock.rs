//! The single-instance lock: one daemon per store file per host.
//!
//! The lock is an exclusive advisory lock on `<store_path>.lock`, taken before the store is
//! opened so a second instance is refused without running a migration. The file is never removed:
//! unlinking it would let a third daemon lock the replacement inode while the first still holds
//! the old one.

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
    /// Takes the lock beside `store_path`, or answers [`DaemonError::LockHeld`].
    pub fn acquire(store_path: &Path) -> Result<Self, DaemonError> {
        let path = lock_path(store_path);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| DaemonError::Store(format!("{}: {error}", path.display())))?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(DaemonError::LockHeld(path)),
            Err(TryLockError::Error(error)) => {
                Err(DaemonError::Store(format!("{}: {error}", path.display())))
            }
        }
    }
}

/// `<store_path>.lock` — the store's sibling, named after the file it guards.
fn lock_path(store_path: &Path) -> PathBuf {
    let mut name = store_path.as_os_str().to_owned();
    name.push(".lock");
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_acquire_is_refused_until_the_first_guard_drops() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");

        let first = StoreLock::acquire(&store).expect("first acquire");
        let error = StoreLock::acquire(&store).expect_err("second acquire");
        assert!(
            matches!(&error, DaemonError::LockHeld(path) if path == &lock_path(&store)),
            "a held lock is LockHeld on its own path: {error:?}"
        );
        assert_eq!(error.exit_code(), 2);

        drop(first);
        let second = StoreLock::acquire(&store).expect("acquire after the guard dropped");
        drop(second);

        assert!(
            lock_path(&store).exists(),
            "the lock file survives the guard"
        );
    }
}
