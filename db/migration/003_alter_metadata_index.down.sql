DROP INDEX IF EXISTS idx_metadatas_on_key_value;
DROP INDEX IF EXISTS idx_metadatas_uniq_key;

CREATE UNIQUE INDEX idx_metadatas_uniq_key ON metadatas(object_id, document_id, key, value);
