//! The single-instance lock: one daemon per store file per host.
//!
//! The lock is an exclusive advisory lock on `<canonical store>.lock`, taken before the store is
//! opened, so a second instance is refused without running a migration. Because it is a file of its
//! own it is independent of SQLite's lock space — the store stays fully usable while the lock is
//! held — and because it names the store's one canonical path, every way of reaching that store
//! answers the same lock.
//!
//! A store with more than one name is refused rather than locked. SQLite's write-ahead log is named
//! for the file it belongs to, not for its inode, so a second hard link would open the same bytes
//! under a different `-wal` and miss committed but uncheckpointed pages.

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
    /// Takes the lock for the store at `store_path`, which MUST already be
    /// [`canonical_store_path`]'s answer, or answers [`DaemonError::LockHeld`].
    ///
    /// The caller resolves once and hands the same path to `MindStore::open`, so the lock, the
    /// database and its sidecars all name one file: resolving again here would reopen the door a
    /// re-pointed symlink could walk through between the two calls. A store this run is about to
    /// create is created here first, empty. This runs before `MindStore::open`, so a losing instance
    /// never opens or migrates the store. A store reachable under more than one name is refused.
    pub fn acquire(store_path: &Path) -> Result<Self, DaemonError> {
        create_if_absent(store_path)?;
        refuse_second_name(store_path)?;
        let path = lock_path(store_path);
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

/// Creates the store when it is absent, so it has a name to lock and a link count to read.
fn create_if_absent(store: &Path) -> Result<(), DaemonError> {
    if store.exists() {
        return Ok(());
    }
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(store)
        .map_err(|error| DaemonError::Store(format!("{}: {error}", store.display())))?;
    Ok(())
}

/// Refuses a store reachable under more than one name.
///
/// A hard link is another name for the same bytes, and SQLite's `-wal` and `-shm` files are named
/// for the path rather than the inode: a second link would write its own log beside itself and a
/// restart through either name could miss the other's committed pages. One name is the contract, so
/// the second is a start failure, not a narrower kind of support.
fn refuse_second_name(store: &Path) -> Result<(), DaemonError> {
    use std::os::unix::fs::MetadataExt;

    let links = std::fs::metadata(store)
        .map_err(|error| DaemonError::Store(format!("{}: {error}", store.display())))?
        .nlink();
    if links > 1 {
        return Err(DaemonError::Config(format!(
            "store_path {}: {links} hard links; a store must have exactly one name",
            store.display()
        )));
    }
    Ok(())
}

/// `<canonical store>.lock` — the one lock file a store's single name answers.
fn lock_path(store: &Path) -> PathBuf {
    let mut lock = store.as_os_str().to_os_string();
    lock.push(".lock");
    PathBuf::from(lock)
}

/// `store_path`'s canonical spelling — the one path SQLite and its sidecars are named after.
///
/// The lock is taken on this path, and the store must be opened on it too: a store reachable under
/// two names would otherwise get two write-ahead logs.
pub fn canonical_store_path(store_path: &Path) -> Result<PathBuf, DaemonError> {
    resolve(store_path)
}

/// How many alias hops a store path may take. A chain longer than this is refused rather than
/// walked, so a symlink loop cannot spin here.
const MAX_ALIAS_HOPS: usize = 16;

/// The database's canonical path: the one spelling SQLite and its sidecars are named after.
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

    /// The lock path a store's canonical name answers, without creating anything.
    fn lock_file_for(store: &Path) -> PathBuf {
        lock_path(&canonical_store_path(store).expect("the store's canonical path"))
    }

    /// The lock for `store`, resolved once exactly as `main` does it.
    fn acquire(store: &Path) -> Result<StoreLock, DaemonError> {
        let store_path = canonical_store_path(store)?;
        StoreLock::acquire(&store_path)
    }

    /// A held lock answers `LockHeld` naming the store's canonical path.
    fn assert_held(error: &DaemonError, locked: &Path) {
        let canonical = resolved(locked);
        assert!(
            matches!(error, DaemonError::LockHeld(path) if path == &canonical),
            "a held lock is LockHeld naming {canonical:?}: {error:?}"
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

        let first = acquire(&store).expect("first acquire");
        let error = acquire(&store).expect_err("second acquire");
        assert_held(&error, &store);
        assert_eq!(std::fs::read(&store).expect("read"), BYTES);

        drop(first);
        let second = acquire(&store).expect("acquire after the guard dropped");
        drop(second);
        assert_eq!(std::fs::read(&store).expect("read"), BYTES);
    }

    /// The lock is a separate file beside the store's canonical name, and the store's own bytes and
    /// length are untouched by taking it.
    #[test]
    fn the_lock_is_a_separate_file_beside_the_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, BYTES).expect("create the store file");

        let lock = lock_file_for(&store);
        assert_ne!(
            lock,
            resolved(&store),
            "the lock is not the store file: {}",
            lock.display()
        );
        assert_eq!(
            lock.file_name().and_then(|name| name.to_str()),
            Some("mind.db.lock"),
            "the lock is named for the store's one name: {}",
            lock.display()
        );
        assert_eq!(
            lock.parent(),
            Some(resolved(dir.path()).as_path()),
            "the lock sits beside the canonical store: {}",
            lock.display()
        );

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

        let first = acquire(&store).expect("acquire a store that does not exist");
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

        let error = acquire(&store).expect_err("second acquire");
        assert_held(&error, &store);
        drop(first);
        acquire(&store).expect("acquire after the guard dropped");
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

        let first = acquire(&store).expect("acquire through the real name");
        let error = acquire(&alias).expect_err("acquire through the alias");
        assert_held(&error, &store);

        drop(first);
        acquire(&alias).expect("the alias acquires once the guard drops");
    }

    /// A dangling alias of a store that does not exist yet: acquiring through the alias must land
    /// on the target's name, so the target answers the same lock.
    #[cfg(unix)]
    #[test]
    fn a_dangling_alias_locks_the_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let resolved_dir = resolved(dir.path());
        let store = dir.path().join("mind.db");
        let alias = dir.path().join("alias.db");
        std::os::unix::fs::symlink(&store, &alias).expect("dangling symlink");
        assert!(!store.exists(), "the target does not exist yet");

        // Nothing has created the target: acquiring the alias is what creates it.
        let first = acquire(&alias).expect("acquire the dangling alias");
        assert!(store.exists(), "the alias created the target");

        let expected = resolved_dir.join("mind.db.lock");
        assert!(
            expected.exists(),
            "the alias locked the target's canonical name: {}",
            expected.display()
        );
        assert!(
            !resolved_dir.join("alias.db.lock").exists(),
            "no lock was named for the alias itself"
        );

        let error = acquire(&store).expect_err("acquire the target");
        assert_held(&error, &store);

        drop(first);
        acquire(&store).expect("acquire after the guard dropped");
    }

    /// Every hard link is refused, so the same-directory case never reaches the lock: the store's
    /// link count is what the rule reads, and it is the inode's, not the directory entry's.
    #[cfg(unix)]
    #[test]
    fn a_hard_link_in_the_same_directory_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, BYTES).expect("create the store file");
        let link = dir.path().join("hard.db");
        std::fs::hard_link(&store, &link).expect("hard link");

        // Two names, one database: SQLite's sidecars are named for the path, so one of the two
        // would write a log the other never reads. Refused, not served under a narrower rule.
        for name in [&store, &link] {
            match StoreLock::acquire(name) {
                Err(DaemonError::Config(message)) => {
                    assert!(message.contains("hard links"), "{message}");
                    assert!(message.contains("exactly one name"), "{message}");
                }
                Err(other) => panic!("a hard-linked store is a config refusal, not {other:?}"),
                Ok(_) => panic!("a store with two names must not start"),
            }
        }
        assert_eq!(std::fs::read(&store).expect("read"), BYTES);
        assert_eq!(std::fs::read(&link).expect("read"), BYTES);
    }

    /// The cross-directory case is impossible by construction rather than a documented limit: the
    /// link count belongs to the inode, so a second name in any directory trips `nlink() > 1` and
    /// the second name is refused, not served under a narrower guarantee.
    #[cfg(unix)]
    #[test]
    fn a_cross_directory_hard_link_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let elsewhere = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("mind.db");
        std::fs::write(&store, BYTES).expect("create the store file");
        let link = elsewhere.path().join("mind.db");
        std::fs::hard_link(&store, &link).expect("hard link across directories");

        // The link count is a property of the inode, so the other directory does not matter: one
        // database with two names is refused from either side.
        for name in [&store, &link] {
            match StoreLock::acquire(name) {
                Err(DaemonError::Config(message)) => {
                    assert!(message.contains("hard links"), "{message}");
                }
                Err(other) => panic!("a hard-linked store is a config refusal, not {other:?}"),
                Ok(_) => panic!("a store linked across directories must not start"),
            }
        }
    }

    /// Resolution happens once, before the lock is taken. A symlink re-pointed after that point
    /// cannot move the store: the caller holds a `PathBuf`, and both the lock and the database are
    /// named from it, not from the link it was read through.
    #[tokio::test]
    async fn a_symlink_re_pointed_after_resolution_cannot_move_the_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let elsewhere = tempfile::tempdir().expect("tempdir");
        let first = dir.path().join("first.db");
        let second = elsewhere.path().join("second.db");
        // A real database, so opening the resolved path is the observable "the right file opened".
        mindcore_sqlite::MindStore::open(&first)
            .await
            .expect("create the first store")
            .close()
            .await;
        let first_bytes = std::fs::read(&first).expect("read the first store");
        std::fs::write(&second, b"other bytes").expect("create the second store");
        let link = dir.path().join("store.db");
        std::os::unix::fs::symlink(&first, &link).expect("point the link at the first store");

        // The one resolution the start performs.
        let resolved = canonical_store_path(&link).expect("resolve once");
        assert_eq!(
            resolved,
            resolved_of(&first),
            "the link resolves to the first store"
        );

        // Re-point the link to a different database after resolution.
        std::fs::remove_file(&link).expect("unlink the link");
        std::os::unix::fs::symlink(&second, &link).expect("re-point at the second store");

        // The lock and the open both use the resolved path, so neither follows the new target.
        let guard = StoreLock::acquire(&resolved).expect("lock the resolved path");
        let lock = lock_path(&resolved);
        assert_eq!(
            lock.parent(),
            Some(resolved_of(dir.path()).as_path()),
            "the lock sits under the resolved parent: {}",
            lock.display()
        );
        assert_eq!(
            lock.file_name().and_then(|name| name.to_str()),
            Some("first.db.lock"),
            "the lock is the resolved name's: {}",
            lock.display()
        );
        let opened = mindcore_sqlite::MindStore::open(&resolved)
            .await
            .expect("open the resolved path");
        opened.close().await;
        assert_eq!(
            std::fs::read(&first).expect("read the first store"),
            first_bytes,
            "the resolved store is the one opened"
        );
        assert_eq!(
            std::fs::read(&second).expect("read the second store"),
            b"other bytes",
            "the re-pointed target was never touched"
        );
        drop(guard);
    }

    /// `path` resolved, for comparisons that are about identity rather than spelling.
    fn resolved_of(path: &Path) -> PathBuf {
        std::fs::canonicalize(path).expect("canonical path")
    }

    /// An unusable store parent is a config fault naming the path, not a lock failure.
    #[test]
    fn an_unresolvable_parent_is_a_config_fault() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = dir.path().join("absent").join("mind.db");

        let error = canonical_store_path(&store).expect_err("unresolvable parent");
        assert!(
            matches!(&error, DaemonError::Config(message) if message.contains("store_path")),
            "a missing parent is a config fault naming the path: {error:?}"
        );
        assert_eq!(error.exit_code(), 1);
        assert!(!dir.path().join("absent").exists(), "nothing was created");
    }
}
