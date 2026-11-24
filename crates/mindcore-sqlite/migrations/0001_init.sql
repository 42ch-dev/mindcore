-- Pre-release schema: the migration set is a single initial file, so a database created before a
-- change is refused — a missing version when an applied file is gone, a checksum mismatch when an
-- applied file changed — and must be recreated, not upgraded.
--
-- The fourteen `memory_*` tables are the protocol schema `MindStore::open` must produce: the served
-- adapter and the borrowed-transaction `SqliteMemoryRepository` both read exactly these columns.
-- Nexus host-only objects stay absent: the `guard_*`/`outbox_*`/`immutable_*` triggers and the
-- `nexus_writer_*` UDFs they call, plus `core_workspace_gate`/`core_writer_registration`/
-- `core_changes`, which serve only those triggers. They enforce the caller's writer registration
-- and outbox policy, no statement here reads or writes them, and importing them would tie `open`
-- to a writer runtime it does not own.

-- ── Control plane ──
CREATE TABLE memory_spaces (
    space_id TEXT PRIMARY KEY,
    display_name TEXT,
    authority_host_id TEXT NOT NULL,
    authority_generation INTEGER NOT NULL CHECK (authority_generation >= 1),
    status TEXT NOT NULL CHECK (status IN ('active', 'frozen')),
    revision INTEGER NOT NULL CHECK (revision >= 1),
    created_at TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE memory_subjects (
    space_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    kind_ns TEXT,
    kind_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('active', 'fenced', 'archived')),
    fenced_contexts_json TEXT NOT NULL DEFAULT '[]',
    revision INTEGER NOT NULL CHECK (revision >= 1),
    created_at TEXT NOT NULL,
    PRIMARY KEY (space_id, subject_id),
    CHECK ((kind_ns IS NULL) = (kind_id IS NULL))
) WITHOUT ROWID;

CREATE TABLE memory_grants (
    grant_id TEXT PRIMARY KEY,
    space_id TEXT NOT NULL,
    subject_id TEXT,
    principal_id TEXT NOT NULL,
    capabilities_json TEXT NOT NULL,
    context_ids_json TEXT NOT NULL DEFAULT '[]',
    shared_access INTEGER NOT NULL DEFAULT 0 CHECK (shared_access IN (0, 1)),
    expires_at TEXT,
    revoked_at TEXT,
    revision INTEGER NOT NULL CHECK (revision >= 1),
    created_at TEXT NOT NULL
) WITHOUT ROWID;

CREATE INDEX idx_memory_grants_space_principal
    ON memory_grants (space_id, principal_id);

CREATE INDEX idx_memory_grants_space_subject
    ON memory_grants (space_id, subject_id);

-- The serving path lists one principal's grants, so this index drives from the principal and the
-- cost follows that principal's grants rather than the total number of spaces. The trailing
-- `grant_id` also carries the `(space_id, grant_id)` order, so the scan needs no sort step and no
-- planner choice can prefer a space-driven plan to avoid one.
CREATE INDEX idx_memory_grants_principal_space
    ON memory_grants (principal_id, space_id, grant_id);

-- ── Content plane ──
CREATE TABLE memory_candidates (
    candidate_id TEXT PRIMARY KEY,
    space_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    capture_visibility TEXT NOT NULL CHECK (capture_visibility IN ('shared', 'context')),
    capture_context_id TEXT,
    kind TEXT NOT NULL,
    content_json TEXT,
    provenance_json TEXT NOT NULL,
    classification_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'consumed')),
    consumed_by_json TEXT,
    content_digest TEXT,
    occurred_at TEXT,
    expires_at TEXT,
    revision INTEGER NOT NULL CHECK (revision >= 1),
    created_at TEXT NOT NULL,
    CHECK ((capture_visibility = 'context') = (capture_context_id IS NOT NULL)),
    CHECK ((status = 'consumed') = (consumed_by_json IS NOT NULL)),
    CHECK ((status = 'consumed') = (content_json IS NULL)),
    CHECK ((status = 'consumed') = (content_digest IS NOT NULL))
) WITHOUT ROWID;

CREATE INDEX idx_memory_candidates_subject_status
    ON memory_candidates (space_id, subject_id, status);

CREATE INDEX idx_memory_candidates_pending_expires
    ON memory_candidates (expires_at)
    WHERE status = 'pending';

CREATE TABLE memory_records (
    record_id TEXT PRIMARY KEY,
    space_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    content_json TEXT NOT NULL,
    scope_visibility TEXT NOT NULL CHECK (scope_visibility IN ('shared', 'context')),
    scope_context_id TEXT,
    provenance_json TEXT NOT NULL,
    import_ref_json TEXT,
    occurred_at TEXT,
    tombstone_json TEXT,
    revision INTEGER NOT NULL CHECK (revision >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK ((scope_visibility = 'context') = (scope_context_id IS NOT NULL))
) WITHOUT ROWID;

CREATE INDEX idx_memory_records_subject_created
    ON memory_records (space_id, subject_id, created_at DESC, record_id);

CREATE TABLE memory_profiles (
    space_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    profile_kind TEXT NOT NULL,
    document_json TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (space_id, subject_id, profile_kind)
) WITHOUT ROWID;

CREATE TABLE memory_receipts (
    receipt_id TEXT PRIMARY KEY,
    space_id TEXT NOT NULL,
    requester_principal_id TEXT NOT NULL,
    op TEXT NOT NULL,
    -- NULL iff the op is the sole §7.0.5 exemption (`memory.subjects.ensure`).
    idempotency_key TEXT,
    payload_digest TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('committed', 'pending', 'failed')),
    result_json TEXT,
    error_json TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    -- Generation lease (§7.0.3); NULL on terminal receipts.
    owner_id TEXT,
    lease_expires_at TEXT,
    -- §6.10 payload contract: pending carries no payload/stamp; a settled row
    -- carries one payload (served body) or none (content-free tombstone).
    CHECK (
        (status = 'pending' AND result_json IS NULL AND error_json IS NULL AND completed_at IS NULL)
        OR (
            status IN ('committed', 'failed')
            AND completed_at IS NOT NULL
            AND NOT (result_json IS NOT NULL AND error_json IS NOT NULL)
        )
    ),
    CHECK (
        (idempotency_key IS NULL) = (op = 'memory.subjects.ensure')
    )
) WITHOUT ROWID;

CREATE UNIQUE INDEX idx_memory_receipts_dedup_namespace
    ON memory_receipts (space_id, requester_principal_id, op, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_memory_receipts_space_status
    ON memory_receipts (space_id, status);

CREATE INDEX idx_memory_receipts_created_at
    ON memory_receipts (created_at);

-- ── Transfer plane ──
-- One committed export: the header metadata plus the original authorization scope a later `get`
-- must recheck before it reveals an ordinal or an expiry. The complete wire chunks live only in
-- `memory_export_chunks`; no whole-bundle blob is duplicated here. Expiry is fixed at creation and
-- never reset by a replay or a page read.
CREATE TABLE memory_exports (
    export_id TEXT PRIMARY KEY CHECK (length(export_id) > 0),
    space_id TEXT NOT NULL CHECK (length(space_id) > 0),
    requester_principal_id TEXT NOT NULL CHECK (length(requester_principal_id) > 0),
    -- NULL for a whole-space export, set for a subject export.
    subject_id TEXT CHECK (subject_id IS NULL OR length(subject_id) > 0),
    -- Canonical `ReadSelection` JSON; NULL means the subject scope's default and is invalid for a
    -- whole-space export, which selects no placement.
    selection_json TEXT,
    -- Admin-only tombstone inclusion: `get` rechecks admin coverage when set.
    include_tombstoned INTEGER NOT NULL CHECK (include_tombstoned IN (0, 1)),
    bundle_digest TEXT NOT NULL CHECK (length(bundle_digest) = 64),
    chunk_count INTEGER NOT NULL CHECK (typeof(chunk_count) = 'integer' AND chunk_count >= 1),
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    -- Persisted expiry decision: a transfer write that reclaims the payload sets `expired`, and a
    -- later page read answers `EXPORT_EXPIRED` from this value instead of the wall clock, which a
    -- clock rollback would move back inside the retention window.
    state TEXT NOT NULL DEFAULT 'live' CHECK (state IN ('live', 'expired')),
    CHECK (selection_json IS NULL OR subject_id IS NOT NULL)
) WITHOUT ROWID;

-- The expiry decision reads exports by this key: an expired export is reclaimed
-- (`expired`, payload released) on the next successful transfer write of its space.
CREATE INDEX idx_memory_exports_expires
    ON memory_exports (expires_at);

-- Each complete immutable wire chunk of one export, keyed by its ordinal: ordinal zero carries the
-- header and every later chunk carries whole elements. Paging reads this key in order.
CREATE TABLE memory_export_chunks (
    export_id TEXT NOT NULL CHECK (length(export_id) > 0),
    ordinal INTEGER NOT NULL CHECK (typeof(ordinal) = 'integer' AND ordinal >= 0),
    -- The chunk object as published; it already carries its own digest and `bytes`.
    payload TEXT NOT NULL CHECK (length(payload) > 0),
    PRIMARY KEY (export_id, ordinal)
) WITHOUT ROWID;

-- One staged import, fixed by ordinal zero and advanced per accepted chunk. The wire tail carries
-- no import id, so a later chunk resolves its import by the
-- (space_id, requester_principal_id, source_export_id) key that the unique partial index below
-- makes single-valued; the digest and count are fixed with it.
CREATE TABLE memory_imports (
    import_id TEXT PRIMARY KEY CHECK (length(import_id) > 0),
    space_id TEXT NOT NULL CHECK (length(space_id) > 0),
    requester_principal_id TEXT NOT NULL CHECK (length(requester_principal_id) > 0),
    source_export_id TEXT NOT NULL CHECK (length(source_export_id) > 0),
    bundle_digest TEXT NOT NULL CHECK (length(bundle_digest) = 64),
    -- Header `chunk_count`; a header-only bundle still stages one chunk.
    expected_count INTEGER NOT NULL CHECK (typeof(expected_count) = 'integer' AND expected_count >= 1),
    -- Next ordinal the caller must present; durable per accepted chunk.
    next_ordinal INTEGER NOT NULL
        CHECK (typeof(next_ordinal) = 'integer' AND next_ordinal BETWEEN 0 AND expected_count),
    conflict_policy TEXT NOT NULL CHECK (conflict_policy IN ('fail', 'rename_subject')),
    -- Ordinal-zero `adopt_grants`: applied on activation only, never auto-activated.
    adopt_grants_json TEXT NOT NULL DEFAULT '[]',
    -- Ordinal-zero `reference_translations`: applied to transferred Content on activation only.
    reference_translations_json TEXT NOT NULL DEFAULT '[]',
    state TEXT NOT NULL CHECK (state IN ('staging', 'staged', 'activated', 'discarded', 'expired')),
    -- Written once by final verification, before the inbound chunks are dropped: the translated
    -- rows to apply and the report that fixes the ID mappings.
    staged_payload_json TEXT,
    staged_report_json TEXT,
    -- Content-free source receipt summaries retained as import provenance after activation,
    -- deduplicated by (source authority host, source space, receipt identity).
    provenance_json TEXT,
    created_at TEXT NOT NULL,
    -- Fixed at ordinal zero; never reset by a replay or a later chunk.
    expires_at TEXT NOT NULL,
    activated_at TEXT,
    CHECK ((state = 'activated') = (activated_at IS NOT NULL)),
    -- `staged` means every ordinal landed, so the inbound sequence is complete.
    CHECK (state NOT IN ('staged', 'activated') OR next_ordinal = expected_count)
) WITHOUT ROWID;

-- The tail chunk carries no import id, so each subsequent `stage` resolves one live import through
-- this key; the unique index also holds the one-live-import-per-source-export invariant, across
-- both non-terminal states, so a second idempotency key cannot open a second import for the same
-- source export while the first is `staging` or `staged`.
CREATE UNIQUE INDEX idx_memory_imports_staging
    ON memory_imports (space_id, requester_principal_id, source_export_id)
    WHERE state IN ('staging', 'staged');

-- The expiry decision reads live imports by this key: an expired import is reclaimed
-- (`expired`, payload released) on the next successful transfer write of its space.
CREATE INDEX idx_memory_imports_expires
    ON memory_imports (expires_at);

-- Inbound chunks of one import, keyed by ordinal: each accepted chunk is durable without rewriting
-- the earlier ones. Final verification consumes and removes them.
CREATE TABLE memory_import_chunks (
    import_id TEXT NOT NULL CHECK (length(import_id) > 0),
    ordinal INTEGER NOT NULL CHECK (typeof(ordinal) = 'integer' AND ordinal >= 0),
    -- The staged chunk as received; ordinal zero carries the header.
    payload TEXT NOT NULL CHECK (length(payload) > 0),
    PRIMARY KEY (import_id, ordinal)
) WITHOUT ROWID;

-- ── Durable intent journal ──
CREATE TABLE memory_journal (
    seq INTEGER PRIMARY KEY,
    receipt_id TEXT NOT NULL UNIQUE,
    space_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    requester_principal_id TEXT NOT NULL,
    op TEXT NOT NULL,
    idempotency_key TEXT,
    state TEXT NOT NULL CHECK (state IN ('pending', 'applied', 'quarantined', 'deferred')),
    targets_json TEXT NOT NULL,
    result_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) WITHOUT ROWID;

-- The literal `state` predicates below match the partial-index predicates exactly; the planner
-- only uses a partial index when the query's predicate is provably implied by the index's.
CREATE INDEX idx_memory_journal_unsettled
    ON memory_journal (space_id, subject_id, created_at)
    WHERE state <> 'applied';

CREATE INDEX idx_memory_journal_recovery
    ON memory_journal (space_id, seq)
    WHERE state <> 'applied';

CREATE INDEX idx_memory_journal_applied_created
    ON memory_journal (created_at)
    WHERE state = 'applied';

-- ── Provider-owned scalar metadata ──
CREATE TABLE memory_provider_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
) WITHOUT ROWID;

-- ── Append-only reflection envelopes ──
CREATE TABLE memory_reflections (
    seq INTEGER NOT NULL UNIQUE,
    reflection_id TEXT PRIMARY KEY,
    space_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    requester_principal_id TEXT NOT NULL,
    selection_json TEXT NOT NULL,
    filter_json TEXT,
    cognition_filter_json TEXT,
    basis_json TEXT NOT NULL,
    engine_policy_version TEXT NOT NULL,
    model TEXT,
    schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
    revision INTEGER NOT NULL CHECK (revision >= 1),
    extensions_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    body_text TEXT NOT NULL,
    body_format TEXT NOT NULL CHECK (body_format IN ('markdown', 'json')),
    cost_unit_kind TEXT,
    cost_units REAL,
    stale_cache INTEGER NOT NULL DEFAULT 0,
    CHECK ((cost_unit_kind IS NULL) = (cost_units IS NULL))
) WITHOUT ROWID;

CREATE INDEX idx_memory_reflections_subject_seq
    ON memory_reflections (space_id, subject_id, seq);

CREATE INDEX idx_memory_reflections_subject_reflection
    ON memory_reflections (space_id, subject_id, reflection_id);

-- ── Knowledge and cognition ──
CREATE TABLE knowledge_entries (
  collection   TEXT NOT NULL,
  entry_id     TEXT NOT NULL,
  -- `INTEGER` affinity stores a fractional value as REAL, so the cast comparison also requires
  -- integral storage.
  revision     INTEGER NOT NULL CHECK (revision >= 1 AND revision = CAST(revision AS INTEGER)),
  -- First revision above the newest deleted preimage; explicit pruning advances it. Every new row
  -- starts at 1, so "nothing pruned" needs no back-fill.
  history_floor_revision INTEGER NOT NULL DEFAULT 1
    CHECK (
      typeof(history_floor_revision) = 'integer' AND history_floor_revision >= 1
    ),
  payload      TEXT NOT NULL,
  -- Composite pointer to the holder's newest record; NULL while it has none.
  cognition_head_seq INTEGER,
  -- Liveness marker: the same-collection entry that replaced this one. NULL while the entry is
  -- current; the row, its payload and its history stay, and no cascade ever removes a successor.
  superseded_by TEXT,
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL,
  PRIMARY KEY (collection, entry_id),
  FOREIGN KEY (collection, entry_id, cognition_head_seq)
    REFERENCES cognition_records (collection, entry_id, append_seq),
  FOREIGN KEY (collection, superseded_by)
    REFERENCES knowledge_entries (collection, entry_id)
);

-- The live listing reads the current rows ordered by `entry_id`; this partial index serves that
-- page from the live rows alone, so retired rows are never scanned for it.
CREATE INDEX idx_knowledge_entries_live
  ON knowledge_entries (collection, entry_id)
  WHERE superseded_by IS NULL;

-- One row per appended cognition record: `append_seq` is the storage order the carrier reads,
-- and rows are immutable once written. `append_prior_shape` is NULL for a record a full-entry
-- put committed and classifies what a dedicated append is about to replace: `0` the module kept
-- its `records` member, `1` the module was absent, `2` the module existed without `records`.
CREATE TABLE cognition_records (
  collection TEXT NOT NULL,
  entry_id   TEXT NOT NULL,
  append_seq INTEGER NOT NULL CHECK (append_seq > 0),
  record_id  TEXT NOT NULL,
  payload    TEXT NOT NULL,
  append_prior_shape INTEGER CHECK (append_prior_shape IN (0, 1, 2)),
  PRIMARY KEY (collection, entry_id, append_seq),
  UNIQUE (collection, entry_id, record_id),
  FOREIGN KEY (collection, entry_id) REFERENCES knowledge_entries (collection, entry_id)
);

-- The preimage of every revision a full-entry update displaces: one row per replaced revision,
-- keyed by it, so a range read needs no second index. `recorded_at` is the instant the replaced
-- state was written, not the instant it was displaced. A dedicated cognition append advances the
-- revision without a row here; its checkpoint is the next saved revision or the live row, and
-- the appended record's `append_prior_shape` reverses the first append.
CREATE TABLE knowledge_entry_revisions (
  collection         TEXT NOT NULL,
  entry_id           TEXT NOT NULL,
  revision           INTEGER NOT NULL
                     CHECK (revision >= 1 AND revision = CAST(revision AS INTEGER)),
  payload            TEXT NOT NULL,
  cognition_head_seq INTEGER,
  recorded_at        TEXT NOT NULL,
  PRIMARY KEY (collection, entry_id, revision),
  FOREIGN KEY (collection, entry_id) REFERENCES knowledge_entries (collection, entry_id)
);

-- Save what a revision-changing update replaces, unless it is a dedicated append: the head
-- advances by exactly one and the record now indexed by that head was written by such an append
-- (a full put stamps NULL). Insertion is unconditional otherwise — no conflict suppression, so a
-- replayed assignment surfaces as a constraint failure instead of overwriting history.
CREATE TRIGGER knowledge_entries_capture_revision
BEFORE UPDATE OF revision ON knowledge_entries
WHEN NEW.revision <> OLD.revision
 AND NOT (
   NEW.cognition_head_seq IS COALESCE(OLD.cognition_head_seq, 0) + 1
   AND (
     SELECT append_prior_shape FROM cognition_records
     WHERE collection = OLD.collection
       AND entry_id = OLD.entry_id
       AND append_seq = NEW.cognition_head_seq
   ) IS NOT NULL
 )
BEGIN
  INSERT INTO knowledge_entry_revisions
    (collection, entry_id, revision, payload, cognition_head_seq, recorded_at)
  VALUES
    (OLD.collection, OLD.entry_id, OLD.revision, OLD.payload, OLD.cognition_head_seq, OLD.updated_at);
END;

-- Prune dedup: one row per retry identity — the Connect key itself — prefixed with the collection
-- its history surface routes to and a NUL, so two surfaces on one database that share a wire scope
-- and principal still never replay each other's prunes. Nothing orders or expires rows, so there
-- is no timestamp column; each namespace is capped at 4096 rows, and a namespace at the bound
-- answers a fresh key `RECEIPT_EXPIRED` instead of inserting past it. `lookup` claims a key by
-- inserting the both-NULL row, and `record` fills it: a claim whose outcome never arrived keeps
-- the retry out of the port, so a prune that committed before a crash is never re-executed.
CREATE TABLE history_dedup (
  -- The collection whose history surface wrote the row, and Connect's opaque retry key. Two
  -- columns, not one framed string: a collection name may contain any character, so no separator
  -- can be unambiguous, and the capacity count needs an exact namespace equality.
  namespace      TEXT NOT NULL,
  key            TEXT NOT NULL,
  -- SHA-256 hex over the RFC 8785 canonical JSON of the whole request payload.
  payload_digest TEXT,
  -- The settled outcome as `{"ok": …}` or `{"reject": {"code", "message", "details"}}`.
  outcome_json   TEXT,
  PRIMARY KEY (namespace, key),
  CHECK ((payload_digest IS NULL) = (outcome_json IS NULL))
) WITHOUT ROWID;

-- ── Retained Memory record vectors ──
-- Optional cache of retained Memory record vectors: one row per record and embedding identity.
-- `identity_hash` partitions rows by serving model, so retiring an identity is one indexed bulk
-- delete; `record_revision` is a freshness marker, never part of the key.
CREATE TABLE memory_embedding_vectors (
  record_id       TEXT NOT NULL,
  identity_hash   TEXT NOT NULL,
  -- `INTEGER` affinity keeps a fractional value as REAL, so `typeof` enforces integral storage.
  record_revision INTEGER NOT NULL
                  CHECK (typeof(record_revision) = 'integer' AND record_revision > 0),
  dim             INTEGER NOT NULL CHECK (typeof(dim) = 'integer' AND dim > 0),
  -- Little-endian IEEE 754 binary32 components: no header, no quantization. `typeof` guards
  -- against a text value whose `length()` would count characters instead of bytes.
  vector          BLOB NOT NULL CHECK (typeof(vector) = 'blob' AND length(vector) = 4 * dim),
  PRIMARY KEY (record_id, identity_hash)
);

CREATE INDEX memory_embedding_vectors_identity ON memory_embedding_vectors (identity_hash);
