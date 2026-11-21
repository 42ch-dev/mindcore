-- Pre-release schema: the migration set is a single initial file, so a database created before a
-- change is refused — a missing version when an applied file is gone, a checksum mismatch when an
-- applied file changed — and must be recreated, not upgraded.
--
-- The ten `memory_*` tables are the protocol schema `MindStore::open` must produce: the served
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
-- cost follows that principal's grants rather than the total number of spaces.
CREATE INDEX idx_memory_grants_principal_space
    ON memory_grants (principal_id, space_id);

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
  revision     INTEGER NOT NULL,
  payload      TEXT NOT NULL,
  -- Composite pointer to the holder's newest record; NULL while it has none.
  cognition_head_seq INTEGER,
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL,
  PRIMARY KEY (collection, entry_id),
  FOREIGN KEY (collection, entry_id, cognition_head_seq)
    REFERENCES cognition_records (collection, entry_id, append_seq)
);

-- One row per appended cognition record: `append_seq` is the storage order the carrier reads,
-- and rows are immutable once written.
CREATE TABLE cognition_records (
  collection TEXT NOT NULL,
  entry_id   TEXT NOT NULL,
  append_seq INTEGER NOT NULL CHECK (append_seq > 0),
  record_id  TEXT NOT NULL,
  payload    TEXT NOT NULL,
  PRIMARY KEY (collection, entry_id, append_seq),
  UNIQUE (collection, entry_id, record_id),
  FOREIGN KEY (collection, entry_id) REFERENCES knowledge_entries (collection, entry_id)
);

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
