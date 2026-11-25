# Storage compatibility

`MindStore::open` owns one SQLite file and one embedded migration set
(`crates/mindcore-sqlite/migrations/`, compiled in by `sqlx::migrate!`). This page is the contract
those files are held to from the first published release onward, and the matrix of databases `open`
accepts.

## Published set

The published set is `tooling/migrations/migrations.lock.json`: an ordered array of
`{ "file", "sha256" }` entries naming every migration file that has shipped in a release. A file in
the lock is published, and published files are immutable.

## What `open` accepts

`open` runs the embedded migrator, which compares the embedded set with the rows in the database's
`_sqlx_migrations` table. There is no other compatibility path.

| Applied state, relative to the embedded set | Result |
| --- | --- |
| Applied set exactly the embedded one: every embedded version applied, every checksum equal | Accepted; the database reopens with its records. |
| Prefix: an earlier release's set, later files pending | Accepted; the pending files are applied in order. |
| Superset: an applied version the embedded set does not contain | Refused with SQLx `VersionMissing(<version>)`. |
| Subset with a gap: the schema is present but a recorded version is not | Refused with SQLx `ExecuteMigration` — the pending file reruns against tables it already created. |
| Divergent: an applied version is embedded but its SQL changed | Refused with SQLx `VersionMismatch(<version>)` (checksum mismatch). |

For `0.1.0` the embedded set is the single `0001_init.sql`, so `open` runs it and creates a
database that already has that schema and records `{1}` in `_sqlx_migrations`. It accepts a
database recorded as exactly `{1}`; any other recorded row set is refused, and with no later
file there is nothing to append either.

Later releases only append, so a database recorded at the set of release *N* opens under release
*N* and under every later release — the appended files are pending there and the migrator applies
them in order. This is the only forward movement. It does not open under an earlier release (its row
set is a superset there), and it does not open under any release that rewrote or removed a file it
applied. `open` never repairs, downgrades or interprets a database: the only outcomes are "reopen",
"append the pending files", and "refused with one of the errors above".

A `VersionMissing` or `VersionMismatch` refusal is not recoverable in place. Neither is a busy,
locked, unreadable or permission-denied database — those are retried after the cause is fixed, and
are not migration mismatches.

## Rules for the migration set

1. A file that is in the lock is immutable. Its bytes may not change: SQLx records a checksum of
   each applied file, so any edit makes every database that applied it unopenable.
2. A schema change appends a new file named `NNNN_<description>.sql`, where `NNNN` is greater than
   every locked prefix. It never edits, renames, splits or deletes a locked file, and never
   backfills a lower number.
3. Appending is the only way a database reaches a new schema: the appended file runs on an existing
   database and must be written to apply on top of the published schema, not to recreate it.
4. A new file becomes published when its entry is added to the lock. The lock entry is added by the
   release pull request before its tag; an appended file may merge before that, unlocked, and a lock
   entry is never rewritten afterwards.

## Enforcement

`node tooling/migrations/check.mjs` (run with no arguments, from any directory) reads the lock and
the migrations directory and fails when:

- a locked file is missing;
- a locked file's `sha256` differs from the lock;
- a `NNNN_*.sql` file is present but unlocked and its prefix does not come after every locked
  prefix;
- a `*.sql` file is present that is not a `NNNN_*.sql` name — SQLx would run shorter prefixes,
  but this contract requires exactly four digits, so the file is reported, not skipped.

The comparison runs against two locks: the working tree's and the lock shipped at the baseline
stable `v*` tag. The baseline comparison is what closes the remaining hole — editing a published
migration and bumping its lock hash in the same change still fails, because the lock that shipped
under the tag records the shipped bytes.

An unlocked file with a greater prefix is an append and passes; committing it publishes it once its
lock entry lands. The script prints the locked set, the present set and the appended-but-unlocked
files, reports every violation it found, and exits non-zero if there was any.

The release PR runs `node tooling/migrations/check.mjs --write-lock`: it performs every check above
first, and rewrites the lock to every present migration only if all of them pass. So each migration
shipped in that release is locked before the tag is cut. A failed check never rewrites the lock —
the run exits non-zero and `migrations.lock.json` keeps its previous contents.

Appending to the lock by hand is deliberate for ordinary changes: it makes publishing a migration a
reviewed change rather than a side effect of adding a file. `--write-lock` is only the release-time
fold of migrations already merged and reviewed, and a second run on an unchanged tree is a no-op.

## Version 0.1.0

`0.1.0` is the first release, so its published set is exactly the current lock — the single
`0001_init.sql` — and this page's matrix starts there.

- There is no data-movement path. Nothing reads a database produced by another product, by a
  fork's schema, or by a pre-release checkout of this repository and converts it. A database from a
  pre-release checkout is not an accepted applied set: if its `0001_init.sql` differs from the
  locked one, `open` refuses it with `VersionMismatch(1)` and the file must be recreated (move it
  aside first if the records matter to you).
- There is no upgrade tool, no downgrade, no rewriting of the schema and no compatibility shim. Forward
  movement happens only by the migrator applying appended files, and only from a published set.
- A store that refuses to open is not converted; the supported recovery is to recreate the
  database, which loses its records.
