-- Optional cache of retained Memory record vectors: one row per record and embedding identity.
-- `identity_hash` partitions rows by serving model, so retiring an identity is one indexed bulk
-- delete; `record_revision` is a freshness marker, never part of the key.
CREATE TABLE memory_embedding_vectors (
  record_id       TEXT NOT NULL,
  identity_hash   TEXT NOT NULL,
  record_revision INTEGER NOT NULL CHECK (record_revision > 0),
  dim             INTEGER NOT NULL CHECK (dim > 0),
  -- Little-endian IEEE 754 binary32 components: no header, no quantization. `typeof` guards
  -- against a text value whose `length()` would count characters instead of bytes.
  vector          BLOB NOT NULL CHECK (typeof(vector) = 'blob' AND length(vector) = 4 * dim),
  PRIMARY KEY (record_id, identity_hash)
);

CREATE INDEX memory_embedding_vectors_identity ON memory_embedding_vectors (identity_hash);
