CREATE TABLE s3_objects (
  id BIGSERIAL PRIMARY KEY,
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())
);
CREATE UNIQUE INDEX idx_s3_objects_bucket_key ON s3_objects(bucket, key);

CREATE TABLE metadatas (
  id BIGSERIAL PRIMARY KEY,
  object_id BIGINT NOT NULL REFERENCES s3_objects(id),
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())
);
CREATE INDEX idx_metadatas_key_value ON metadatas(key, value);
