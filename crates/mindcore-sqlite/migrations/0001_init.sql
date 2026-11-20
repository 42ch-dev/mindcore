CREATE TABLE memory_candidates (
  candidate_id TEXT PRIMARY KEY,
  space_id     TEXT NOT NULL,
  subject_id   TEXT NOT NULL,
  context_id   TEXT NOT NULL,
  content      TEXT NOT NULL,
  observed_at  TEXT NOT NULL
);

CREATE TABLE memory_records (
  record_id    TEXT PRIMARY KEY,
  candidate_id TEXT NOT NULL UNIQUE REFERENCES memory_candidates(candidate_id),
  space_id     TEXT NOT NULL,
  subject_id   TEXT NOT NULL,
  context_id   TEXT NOT NULL,
  content      TEXT NOT NULL,
  recorded_at  TEXT NOT NULL
);

CREATE TABLE review_receipts (
  request_id   TEXT PRIMARY KEY,
  candidate_id TEXT NOT NULL UNIQUE REFERENCES memory_candidates(candidate_id),
  record_id    TEXT NOT NULL REFERENCES memory_records(record_id)
);

CREATE INDEX memory_records_recall ON memory_records (space_id, subject_id, context_id, recorded_at, record_id);

-- Pre-release schema: the migration set is mutable until the first release, so a database
-- created before a change is refused by its migration checksum and must be recreated, not
-- upgraded.
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
