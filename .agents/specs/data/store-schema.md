# Data: store schema and migration policy

## Scope

The SQLite schema a host gets from `MindStore::open`, and the policy for changing it before the first release. It does not define protocol semantics (those live with the adapter and repository), persistence for remote faces, or any upgrade path.

## Current state

`crates/mindcore-sqlite/migrations/` holds exactly one file, `0001_init.sql`. It creates:

- the ten protocol memory tables — `memory_spaces`, `memory_subjects`, `memory_grants`, `memory_candidates`, `memory_records`, `memory_receipts`, `memory_journal`, `memory_profiles`, `memory_reflections`, `memory_provider_meta` — with the column names, types, `CHECK` clauses, `WITHOUT ROWID` declarations and indexes that `SqliteMemoryRepository` decodes;
- `knowledge_entries` then `cognition_records`, two tables that reference each other: the entry's composite foreign key from `cognition_head_seq` points at the record primary key, and each record points back at its entry. Neither orders the other, and `knowledge_entries` is created first, so its cross-reference is a forward reference SQLite accepts without resolving at creation time. `knowledge_entries.superseded_by TEXT` is the nullable liveness marker: its composite foreign key `(collection, superseded_by)` references the entry primary key, so a marker can only name an entry of the same collection, and no cascade removes a successor. Its `NULL` means current; an ordinary payload write never sets or clears it. `cognition_records.append_prior_shape` is `NULL` for a record a full-entry put committed and, for a dedicated append, classifies what that append replaced: `0` the module kept `records`, `1` the module was absent, `2` the module existed without `records`. A partial index, `idx_knowledge_entries_live` on `(collection, entry_id) WHERE superseded_by IS NULL`, serves the live listing: the collection's current rows are read in `entry_id` order from the index alone, so retired rows are never scanned for that page;
- `knowledge_entry_revisions`, the preimage of every revision a full-entry update displaces: one row per `(collection, entry_id, revision)` holding the replaced payload, its cognition head and the instant that state was written (`recorded_at` copies the replaced row's `updated_at`, not the instant it was displaced). A `BEFORE UPDATE OF revision` trigger on `knowledge_entries` — `knowledge_entries_capture_revision` — inserts it, and skips only a dedicated cognition append — the head advanced by exactly one and the record now indexed by it carries a non-null `append_prior_shape`. A full put, even one whose head advances by one, saves its preimage. There is no conflict suppression: a replayed revision assignment surfaces as a constraint failure instead of overwriting history. The parent key is non-cascading, so an entry holding history is never deleted out from under it;
- `memory_embedding_vectors`, keyed `(record_id, identity_hash)` with the record revision, dimension and little-endian `f32` bytes, plus the identity index.

`MindStore::open` applies the embedded set with `PRAGMA foreign_keys` on, so every foreign key is enforced from the first write. The set's one forward reference, `knowledge_entries` naming `cognition_records`, resolves when that table is created, since no row can be written before the migration finishes. The protocol memory tables carry no fixture-only foreign keys: the Nexus writer guards, outbox/immutability triggers, `nexus_writer_*` UDFs and the three `core_*` tables remain caller-owned and are not installed here.

There is one schema source. Tests open a temporary database through `MindStore::open` and seed rows; no test file declares `CREATE TABLE` for a table `open` creates. SQLx preparation reads the same migration into an empty file.

## Migration policy

The workspace is unreleased: every crate is `publish = false` at `0.1.0`, so no migration history is owed. The set is therefore **edited in place** and a database is recreated rather than upgraded.

`MindStore::open` refuses a database whose applied set no longer matches, with two distinct shapes:

- `VersionMissing(n)` — the database applied a version the embedded set no longer contains.
- A migration checksum mismatch — the applied version is still embedded but its SQL changed.

Both mean recreate. A busy or locked database, an I/O error or a permissions problem is not a migration mismatch: fix the cause and retry; never delete a valid file.

## Invariants

- One schema source: the migration. Tests seed data, never tables.
- The production open path is the only way a test obtains the schema, so a regression in the migration fails the end-to-end test instead of passing through a fixture that supplies the missing table.
- A history preimage is written by the update that displaces it, in that update's transaction: a refused, stale or rolled-back write leaves none.
- The liveness marker is storage-owned and only a marking update writes it: a payload write can neither forge nor clear it, and every marking update advances the source revision, so the revision trigger saves its preimage like any other displaced state.
- `revision` and `cognition_head_seq` are signed SQLite integers: a writer refuses a next revision it cannot represent before writing any row, so every committed revision stays addressable by the preimage table and replay.
- Saved preimages and committed cognition records are immutable; replay reads them and never rewrites them.
- A migration change and the code that reads it land together; an intermediate state where the schema moved and the readers did not is not a deliverable.
- Column names, types and constraints are shared with the borrowed-transaction repository; changing one without the other breaks decoding at runtime, not at compile time.

## Where it lives

- The whole embedded set: `crates/mindcore-sqlite/migrations/0001_init.sql`, applied by `MindStore::open` in `crates/mindcore-sqlite/src/store.rs`.
- Readers of the tables it creates: `crates/mindcore-sqlite/src/repository.rs` (`SqliteMemoryRepository`, the `memory_*` tables), `crates/mindcore-sqlite/src/knowledge.rs` and `crates/mindcore-sqlite/src/cognition.rs` (`knowledge_entries`, `cognition_records`), `crates/mindcore-sqlite/src/vectors.rs` (`memory_embedding_vectors`).
- Open-path tests that seed rows through `MindStore::open`: `crates/mindcore-sqlite/tests/store_bootstrap.rs` and the seed helpers beside them.
- Decision record: `.agents/adr/implemented/architecture/2026-11-21-single-migration-store-bootstrap.md`.

## Change obligations

A migration edit lands together with this page, the reader that decodes the changed table, and the tests that open the database through `MindStore::open` — a schema without its reader, or a reader without its schema, is not a deliverable. Because a database is recreated rather than upgraded, no back-fill or compatibility shim is owed; changing that policy, or the two refusal shapes `open` reports, changes this page in the same commit.

## Adoption anchor contract

Adoption writes a copy of a shared entry into a personal collection. The copy is addressed by the **same `entry_id`** as its source — the collection is what distinguishes it — and it carries one Mindcore adoption anchor naming the source entry and the source revision it was taken at (`shared:{entry_id}` plus an explicit revision member). `mindcore_base::knowledge::adoption_anchor_parts` is the single parser for that anchor: the producer in `mindcore-base` and every consumer, including the SQLite resolver, read back through it. A repeated adoption returns the existing copy instead of writing a second one, and it never rewrites the source row.
