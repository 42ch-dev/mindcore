//! The single-instance lock: one daemon per store file per host.
//!
//! The lock is an exclusive advisory lock on `<real store path>.lock`, taken before the store is
//! opened so a second instance is refused without running a migration. The name is derived from
//! the resolved database path, not the configured spelling, so a symlink or any other alias to the
//! same file lands on the same lock. The file is never removed: unlinking it would let a third
//! daemon lock the replacement inode while the first still holds the old one.

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
    pub fn acquire(store_path: &Path) -> Result<Self, DaemonError> {
        let path = lock_path(store_path)?;
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

/// `<real store path>.lock` — the store's sibling, named after the file it guards.
///
/// The configured path is resolved first, so every alias of one database answers one lock name.
/// A store that does not exist yet is resolved through its parent directory, which is what makes
/// first creation work; a parent that cannot be resolved is a config fault rather than a fallback
/// to the literal spelling, because that spelling is exactly what an alias defeats.
fn lock_path(store_path: &Path) -> Result<PathBuf, DaemonError> {
    let real = resolve(store_path)?;
    let mut name = real.into_os_string();
    name.push(".lock");
    Ok(PathBuf::from(name))
}

/// How many alias hops a store path may take. A chain longer than this is refused rather than
/// walked, so a symlink loop cannot spin here.
const MAX_ALIAS_HOPS: usize = 16;

/// The database's real path.
///
/// `canonicalize` answers it whenever the store exists. Otherwise the final component may itself
/// be a symlink to a database that does not exist yet, so the link is followed by hand: the
/// literal name is precisely what an alias defeats, and a dangling alias of a first-created store
/// is a case the daemon must still lock once.
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

    #[test]
    fn a_second_acquire_is_refused_until_the_first_guard_drops() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        let expected = lock_path(&store).expect("lock path");

        let first = StoreLock::acquire(&store).expect("first acquire");
        let error = StoreLock::acquire(&store).expect_err("second acquire");
        assert!(
            matches!(&error, DaemonError::LockHeld(path) if path == &expected),
            "a held lock is LockHeld on its own path: {error:?}"
        );
        assert_eq!(error.exit_code(), 2);

        drop(first);
        let second = StoreLock::acquire(&store).expect("acquire after the guard dropped");
        drop(second);

        assert!(expected.exists(), "the lock file survives the guard");
    }

    /// A store reached through a symlink is the same database, so it must answer one lock.
    #[cfg(unix)]
    #[test]
    fn an_alias_reaches_the_same_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, b"").expect("create the store file");
        let alias = dir.path().join("alias.db");
        std::os::unix::fs::symlink(&store, &alias).expect("symlink");

        let first = StoreLock::acquire(&store).expect("acquire through the real name");
        let error = StoreLock::acquire(&alias).expect_err("acquire through the alias");
        assert!(
            matches!(&error, DaemonError::LockHeld(_)),
            "an alias answers LockHeld, not a second lock: {error:?}"
        );
        assert_eq!(error.exit_code(), 2);

        drop(first);
        StoreLock::acquire(&alias).expect("the alias acquires once the guard drops");
    }

    /// The same holds before the store exists: a dangling alias and its target are one store, and
    /// a first run through either must name the same lock.
    #[cfg(unix)]
    #[test]
    fn a_dangling_alias_reaches_the_same_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        let alias = dir.path().join("alias.db");
        std::os::unix::fs::symlink(&store, &alias).expect("dangling symlink");

        assert_eq!(
            lock_path(&store).expect("target lock"),
            lock_path(&alias).expect("alias lock"),
            "a dangling alias resolves to its target's lock"
        );

        let first = StoreLock::acquire(&store).expect("acquire through the target");
        let error = StoreLock::acquire(&alias).expect_err("acquire through the alias");
        assert!(
            matches!(&error, DaemonError::LockHeld(_)),
            "an alias answers LockHeld: {error:?}"
        );
        drop(first);
    }

    /// A parent that does not exist cannot be resolved, and must not fall back to the literal name.
    #[test]
    fn an_unresolvable_parent_is_a_config_fault() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("absent").join("mind.db");

        let error = StoreLock::acquire(&store).expect_err("unresolvable parent");
        assert!(
            matches!(&error, DaemonError::Config(message) if message.contains("store_path")),
            "a missing parent is a config fault naming the path: {error:?}"
        );
        assert!(!dir.path().join("absent").exists(), "nothing was created");
    }
}
