-- settlement-prepare.sql — local DDL-only compile fixture for the borrowed-tx
-- `MemoryRepository` (settle-repo Task 1). Names, columns, types, nullability
-- and constraints mirror the caller-migrated Nexus memory schema at pinned
-- 2d3cd8dd5 (nexus-local-db/migrations/), at post-migration state.
--
-- Omitted on purpose (never opened at runtime; guarded parity applies the real
-- pinned migrations and executes the real triggers):
--   * guard_*/outbox_*/immutable_* triggers and the nexus_writer_* UDFs they call
--   * core_workspace_gate / core_writer_registration / core_changes — they exist
--     only to serve those triggers, no borrowed-path statement reads them
--
-- Recorded deviations from pinned DDL:
--   * owner_id / lease_expires_at on memory_receipts are INLINED into the table
--     rather than ALTERed (source: 20261110000001_memory_reflections.sql:63-64)
--   * no other column deviation: the pinned schema stores no per-document
--     `extensions` on candidates/records/receipts — the borrowed path does not
--     persist them either (task-1-schema-map.md §3 D-1)

-- ── Control plane (v1.227 NXM-1 round A) ──
-- Source: 20261101000001_memory_control_metadata.sql:35-43
CREATE TABLE IF NOT EXISTS memory_spaces (
    space_id TEXT PRIMARY KEY,
    display_name TEXT,
    authority_host_id TEXT NOT NULL,
    authority_generation INTEGER NOT NULL CHECK (authority_generation >= 1),
    status TEXT NOT NULL CHECK (status IN ('active', 'frozen')),
    revision INTEGER NOT NULL CHECK (revision >= 1),
    created_at TEXT NOT NULL
) WITHOUT ROWID;

-- Source: 20261101000001_memory_control_metadata.sql:45-57
CREATE TABLE IF NOT EXISTS memory_subjects (
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

-- Source: 20261101000001_memory_control_metadata.sql:59-77
CREATE TABLE IF NOT EXISTS memory_grants (
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

CREATE INDEX IF NOT EXISTS idx_memory_grants_space_principal
    ON memory_grants (space_id, principal_id);

CREATE INDEX IF NOT EXISTS idx_memory_grants_space_subject
    ON memory_grants (space_id, subject_id);

-- ── Content plane (v1.227 NXM-1 round B) ──
-- Source: 20261101000002_memory_content_metadata.sql:37-67
CREATE TABLE IF NOT EXISTS memory_candidates (
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

-- Source: 20261101000002_memory_content_metadata.sql:69-78
CREATE INDEX IF NOT EXISTS idx_memory_candidates_subject_status
    ON memory_candidates (space_id, subject_id, status);

CREATE INDEX IF NOT EXISTS idx_memory_candidates_pending_expires
    ON memory_candidates (expires_at)
    WHERE status = 'pending';

-- Source: 20261101000002_memory_content_metadata.sql:80-97
CREATE TABLE IF NOT EXISTS memory_records (
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

-- Source: 20261101000002_memory_content_metadata.sql:99-100
CREATE INDEX IF NOT EXISTS idx_memory_records_subject_created
    ON memory_records (space_id, subject_id, created_at DESC, record_id);

-- Source: 20261101000002_memory_content_metadata.sql:102-112
CREATE TABLE IF NOT EXISTS memory_profiles (
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

-- Source: 20261101000003_memory_receipt_tombstones.sql:15-54 (post-rebuild shape)
-- + owner_id/lease_expires_at inlined from 20261110000001_memory_reflections.sql:63-64
CREATE TABLE IF NOT EXISTS memory_receipts (
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

-- Source: 20261101000003_memory_receipt_tombstones.sql:58-68
CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_receipts_dedup_namespace
    ON memory_receipts (space_id, requester_principal_id, op, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_memory_receipts_space_status
    ON memory_receipts (space_id, status);

CREATE INDEX IF NOT EXISTS idx_memory_receipts_created_at
    ON memory_receipts (created_at);

-- ── Durable intent journal (v1.227 NXM-3) ──
-- Source: 20261101000005_memory_journal.sql:40-53
CREATE TABLE IF NOT EXISTS memory_journal (
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

-- Source: 20261101000005_memory_journal.sql:67-84 — literal `state` predicates
-- match the partial-index predicates exactly (planner requirement, SQL:63-66).
CREATE INDEX IF NOT EXISTS idx_memory_journal_unsettled
    ON memory_journal (space_id, subject_id, created_at)
    WHERE state <> 'applied';

CREATE INDEX IF NOT EXISTS idx_memory_journal_recovery
    ON memory_journal (space_id, seq)
    WHERE state <> 'applied';

CREATE INDEX IF NOT EXISTS idx_memory_journal_applied_created
    ON memory_journal (created_at)
    WHERE state = 'applied';

-- ── Provider-owned scalar metadata (v1.227 NXM-2) ──
-- Source: 20261101000004_memory_provider_meta.sql:16-20
CREATE TABLE IF NOT EXISTS memory_provider_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
) WITHOUT ROWID;

-- ── Append-only reflection envelopes (v1.230 N4-R) ──
-- Source: 20261110000001_memory_reflections.sql:30-54
CREATE TABLE IF NOT EXISTS memory_reflections (
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

-- Source: 20261110000001_memory_reflections.sql:68-73
CREATE INDEX IF NOT EXISTS idx_memory_reflections_subject_seq
    ON memory_reflections (space_id, subject_id, seq);

CREATE INDEX IF NOT EXISTS idx_memory_reflections_subject_reflection
    ON memory_reflections (space_id, subject_id, reflection_id);
