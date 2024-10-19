DROP INDEX IF EXISTS idx_metadatas_uniq_key;

CREATE UNIQUE INDEX idx_metadatas_uniq_key ON
  metadatas(
    COALESCE(object_id,'00000000-0000-0000-0000-000000000000'),
    COALESCE(document_id,'00000000-0000-0000-0000-000000000000'),
    key
  );

CREATE INDEX idx_metadatas_on_key_value ON
  metadatas(
    key,
    value,
    object_id,
    document_id
  );
