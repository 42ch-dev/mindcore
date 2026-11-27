# A process lock on a database file shares SQLite's lock space

**Problem.** A sidecar process that must refuse a second instance over one database looks for a
lock primitive on the database file. `flock` looks right: it is advisory, it lives on the inode, and
it needs no companion file. On macOS it deadlocks the database instead.

**What happens.** Holding `flock(LOCK_EX)` on a store file makes `sqlite3` (and `sqlx`'s open, which
runs `PRAGMA journal_mode=WAL`) answer `(code: 5) database is locked` — on a read as well as on an
open, and on a zero-length file as well as on a populated one. A control confirms the two are one
lock space on that platform: a `flock` blocks while SQLite holds a SHARED read lock, and a
`shutdown`-style experiment on a parked write behaves the same way. Taking the `flock` **after**
SQLite opens does not help: the lock is granted, and every later statement answers `locked`.

**The mistake to avoid.** Reading POSIX and concluding "`flock` and `fcntl` are independent, so this
is fine on Linux" and shipping the platform split. The behaviour is measured, not asserted: verify on
each target, and do not let a comment claim neutral behaviour for an unmeasured platform.

**What works.** A lock of your own, outside both SQLite's lock space and any path the process does
not own:

- Name the lock file from the **resolved** store identity and keep it in a directory the daemon
  controls (`<store dir>/<resolved name>.lock`), never a temp directory — `TMPDIR` differs per
  process, and a cleaner can unlink a held lock file, both of which split the lock.
- Resolve the configured path **once** and use that same resolved `PathBuf` for the lock file and for
  `MindStore::open`. A second resolution lets a symlink be re-pointed between the two calls, so the
  lock guards one inode and the database opens another.
- Refuse a store whose link count is above one (`nlink() > 1`). Hard links defeat any name-derived
  lock, and SQLite's WAL belongs to the file **name**: opening one inode through two names can miss
  committed-but-uncheckpointed pages. Refusing them is a mechanism, not a documented limitation.
- Remove a bound socket name only when its `dev`/`ino` still matches what you bound. POSIX has no
  unlink-by-inode, so a same-user process can still swap the path inside that window; say so and
  record the attacker model instead of claiming atomicity.
- Never unlink something that is not a socket. A regular file, directory or symlink at the socket
  path is refused with its type; only a name that `is_socket()` and answers `ConnectionRefused` is
  stale.

**Evidence.** `crates/mindcore-daemon/src/lock.rs` and `src/serve.rs`; the behaviour is pinned by
`a_hard_link_in_the_same_directory_is_refused`, `a_cross_directory_hard_link_is_refused`,
`a_symlink_re_pointed_after_resolution_cannot_move_the_store` and
`the_drain_refuses_a_socket_name_replaced_by_a_regular_file`. The macOS measurements that motivated
the design are not reproducible from this repository — re-measure them on the target you ship to
instead of trusting this note.
