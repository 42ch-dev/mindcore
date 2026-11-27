//! The single-instance lock: one daemon per store file per host.
//!
//! The lock is an exclusive advisory lock taken before the store is opened, so a second instance is
//! refused without running a migration. Every name that identifies the database is locked — see
//! [`lock_paths`] — so a symlink, a dangling symlink and a hard link all contend on the same lock,
//! and a store that does not exist yet still refuses a second first-run. The lock files are never
//! removed: unlinking one would let a third daemon lock the replacement inode while the first still
//! holds the old one.

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};

use crate::DaemonError;

/// The held lock. Dropping it releases every lock it carries and leaves the files in place.
#[derive(Debug)]
pub struct StoreLock {
    /// Held only for the locks they carry; the guard keeps them open for its whole life.
    _files: Vec<File>,
    /// The names `_files` were taken under, so [`StoreLock::cover`] never repeats one.
    held: BTreeSet<PathBuf>,
}

/// The guard is `Send`, so `main` may move it into the serving phase.
const _: () = {
    const fn assert_send<T: Send>() {}
    assert_send::<StoreLock>();
};

impl StoreLock {
    /// Takes the lock for `store_path`'s database, or answers [`DaemonError::LockHeld`].
    pub fn acquire(store_path: &Path) -> Result<Self, DaemonError> {
        let mut guard = Self {
            _files: Vec::new(),
            held: BTreeSet::new(),
        };
        guard.cover(store_path)?;
        Ok(guard)
    }

    /// Takes every lock name the store's current existence introduces, keeping those already held.
    ///
    /// A store created between [`Self::acquire`] and `MindStore::open` gains an inode name that the
    /// first run could not take, and a hard link to the new file is exactly what another daemon
    /// would use to reach it. The caller runs this once the store exists; a name already held by
    /// another daemon is [`DaemonError::LockHeld`], which is the arbitration point — this daemon
    /// must then abort rather than serve a store it does not exclusively hold.
    pub fn cover(&mut self, store_path: &Path) -> Result<(), DaemonError> {
        for path in lock_paths(store_path)? {
            if self.held.contains(&path) {
                continue;
            }
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&path)
                .map_err(|error| DaemonError::Store(format!("{}: {error}", path.display())))?;
            match file.try_lock() {
                Ok(()) => {
                    self._files.push(file);
                    self.held.insert(path);
                }
                Err(TryLockError::WouldBlock) => return Err(DaemonError::LockHeld(path)),
                Err(TryLockError::Error(error)) => {
                    return Err(DaemonError::Store(format!("{}: {error}", path.display())))
                }
            }
        }
        Ok(())
    }
}

/// Every name that identifies `store_path`'s database, in one fixed order.
///
/// Two names, because neither alone covers the store's whole life:
///
/// - `<resolved>.lock` always, so a store this run is about to create is locked before it exists
///   and two first-runs still contend;
/// - `.mindcore-store-<dev>-<ino>.lock` once the store exists, so a hard link — a second directory
///   entry for one inode, which no path resolution collapses — reaches the same lock, and so do a
///   symlink and a dangling symlink.
///
/// Holding both is what closes the transition between them: a daemon that locked the path name
/// before the store existed keeps holding it after creating the store, so a later start that sees
/// the store and takes the identity name is still refused on the path name it takes as well.
///
/// A parent that cannot be resolved is a config fault rather than a fallback to the literal
/// spelling, because that spelling is what an alias defeats.
fn lock_paths(store_path: &Path) -> Result<Vec<PathBuf>, DaemonError> {
    let real = resolve(store_path)?;
    let dir = real
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut name = real.as_os_str().to_owned();
    name.push(".lock");
    let mut paths = vec![PathBuf::from(name)];
    if let Some(identity) = identity_name(&real) {
        paths.push(dir.join(identity));
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

/// `.mindcore-store-<dev>-<ino>.lock` for an existing store, `None` when it does not exist yet.
#[cfg(unix)]
fn identity_name(real: &Path) -> Option<String> {
    use std::os::unix::fs::MetadataExt;

    let metadata = std::fs::metadata(real).ok()?;
    Some(format!(
        ".mindcore-store-{}-{}.lock",
        metadata.dev(),
        metadata.ino()
    ))
}

// shortcut: a hard-link alias of an existing store is only detectable by inode, so Windows keeps
// the path-derived name and two hard links there would still get two locks; upgrade when Windows
// becomes a target and needs the guarantee.
#[cfg(not(unix))]
fn identity_name(_real: &Path) -> Option<String> {
    None
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

    /// The lock set for `store`, with the temp dir's own spelling resolved, so a comparison is
    /// about the lock names and not about `/var` versus `/private/var`.
    fn paths(store: &Path) -> Vec<PathBuf> {
        lock_paths(store).expect("lock paths")
    }

    /// Asserts `error` is `LockHeld` naming one of `expected`.
    fn assert_held(error: &DaemonError, expected: &[PathBuf]) {
        assert!(
            matches!(error, DaemonError::LockHeld(path) if expected.contains(path)),
            "a held lock is LockHeld on one of {expected:?}: {error:?}"
        );
        assert_eq!(error.exit_code(), 2);
    }

    #[test]
    fn a_second_acquire_is_refused_until_the_first_guard_drops() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, b"").expect("create the store file");
        let expected = paths(&store);

        let first = StoreLock::acquire(&store).expect("first acquire");
        let error = StoreLock::acquire(&store).expect_err("second acquire");
        assert_held(&error, &expected);

        drop(first);
        let second = StoreLock::acquire(&store).expect("acquire after the guard dropped");
        drop(second);

        for path in &expected {
            assert!(path.exists(), "the lock file survives the guard: {path:?}");
        }
    }

    /// A brand-new store is locked under its path name alone: a hard link can only reference an
    /// existing inode, so there is no identity to name yet.
    #[test]
    fn a_store_that_does_not_exist_is_locked_by_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        let dir = std::fs::canonicalize(dir.path()).expect("resolved dir");

        assert_eq!(
            paths(&store),
            vec![dir.join("mind.db.lock")],
            "a not-yet-created store is named by its path"
        );

        let first = StoreLock::acquire(&store).expect("first acquire");
        let error = StoreLock::acquire(&store).expect_err("second acquire");
        assert_held(&error, &[dir.join("mind.db.lock")]);
        drop(first);
    }

    /// The locks sit beside the store, never on it, and one of them carries the store's inode.
    #[cfg(unix)]
    #[test]
    fn the_locks_live_beside_the_store_and_are_not_the_store() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let resolved = std::fs::canonicalize(dir.path()).expect("resolved dir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, b"store bytes").expect("create the store file");
        let metadata = std::fs::metadata(&store).expect("store metadata");

        let expected = paths(&store);
        assert_eq!(
            expected,
            vec![
                resolved.join(format!(
                    ".mindcore-store-{}-{}.lock",
                    metadata.dev(),
                    metadata.ino()
                )),
                resolved.join("mind.db.lock"),
            ],
            "the lock set is the inode identity and the path name"
        );
        for path in &expected {
            assert_eq!(
                path.parent(),
                Some(resolved.as_path()),
                "the lock lives in the store's directory: {}",
                path.display()
            );
            assert_ne!(path, &store, "the lock is not the store file itself");
        }

        let first = StoreLock::acquire(&store).expect("acquire");
        assert_eq!(
            std::fs::read(&store).expect("read the store"),
            b"store bytes",
            "taking the lock leaves the store's own contents alone"
        );
        drop(first);
    }

    /// A hard link is a second directory entry for one inode, which no path resolution collapses:
    /// the identity lock is what makes both names contend.
    #[cfg(unix)]
    #[test]
    fn a_hard_link_reaches_the_same_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, b"").expect("create the store file");
        let link = dir.path().join("hard.db");
        std::fs::hard_link(&store, &link).expect("hard link");

        let identity = paths(&store)[0].clone();
        assert!(
            identity
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(".mindcore-store-")),
            "the shared name is the identity lock: {identity:?}"
        );
        assert!(
            paths(&link).contains(&identity),
            "two hard links to one inode share the identity lock"
        );

        let first = StoreLock::acquire(&store).expect("acquire through the first name");
        let error = StoreLock::acquire(&link).expect_err("acquire through the hard link");
        assert_held(&error, &paths(&link));

        drop(first);
        StoreLock::acquire(&link).expect("the hard link acquires once the guard drops");
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
        assert_held(&error, &paths(&alias));

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
            paths(&store),
            paths(&alias),
            "a dangling alias resolves to its target's lock"
        );

        let first = StoreLock::acquire(&store).expect("acquire through the target");
        let error = StoreLock::acquire(&alias).expect_err("acquire through the alias");
        assert_held(&error, &paths(&alias));
        drop(first);
    }

    /// The transition between the two names: a daemon that locked a store before creating it takes
    /// the inode name as soon as the file exists, so the store cannot be opened twice across the
    /// change — not even through a hard link made to the fresh file.
    #[cfg(unix)]
    #[test]
    fn a_store_created_under_the_path_lock_covers_its_identity() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");

        // The first daemon: the store does not exist, so only the path name is taken.
        let mut first = StoreLock::acquire(&store).expect("first acquire");
        assert_eq!(paths(&store).len(), 1, "one name before the store exists");
        std::fs::write(&store, b"").expect("the first daemon creates the store");

        // A later start sees the store and takes both names; the path name is still held.
        let error = StoreLock::acquire(&store).expect_err("acquire after creation");
        assert_held(&error, &paths(&store));

        // The creating daemon takes the name the file's creation introduced.
        first.cover(&store).expect("cover the identity name");
        assert_eq!(paths(&store).len(), 2, "both names once the store exists");
        let link = dir.path().join("hard.db");
        std::fs::hard_link(&store, &link).expect("hard link to the fresh store");
        let error = StoreLock::acquire(&link).expect_err("acquire through a hard link");
        assert_held(&error, &paths(&link));

        drop(first);
        StoreLock::acquire(&link).expect("acquires once the first daemon drops the guard");
    }

    /// The identity name is the arbitration point: a daemon that created the store aborts when
    /// another one already holds the inode, so the two never both serve it.
    #[cfg(unix)]
    #[test]
    fn the_creator_aborts_when_another_daemon_holds_the_identity() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");

        // This daemon takes the path name while the store is absent.
        let mut creator = StoreLock::acquire(&store).expect("creator acquire");
        std::fs::write(&store, b"").expect("create the store");

        // A rival reaches the fresh inode through a hard link and wins the identity name.
        let link = dir.path().join("hard.db");
        std::fs::hard_link(&store, &link).expect("hard link");
        let rival = StoreLock::acquire(&link).expect("rival acquire");

        // The creator cannot complete: it must abort rather than serve a store it does not hold.
        let error = creator.cover(&store).expect_err("creator cover");
        assert_held(&error, &paths(&store));

        drop(rival);
        creator.cover(&store).expect("covers once the rival drops");
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
        assert_eq!(error.exit_code(), 1);
        assert!(!dir.path().join("absent").exists(), "nothing was created");
    }
}
