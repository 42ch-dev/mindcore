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
  candidate_id TEXT NOT NULL UNIQUE,
  record_id    TEXT NOT NULL
);

CREATE INDEX memory_records_recall ON memory_records (space_id, subject_id, context_id, recorded_at, record_id);
