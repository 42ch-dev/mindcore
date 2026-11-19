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

CREATE TABLE knowledge_entries (
  collection   TEXT NOT NULL,
  entry_id     TEXT NOT NULL,
  revision     INTEGER NOT NULL,
  payload      TEXT NOT NULL,
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL,
  PRIMARY KEY (collection, entry_id)
);
