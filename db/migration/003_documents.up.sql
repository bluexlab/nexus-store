CREATE TABLE documents (
  id BIGSERIAL PRIMARY KEY,
  content JSONB NOT NULL,
  created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())
);

ALTER TABLE metadatas ALTER COLUMN object_id DROP NOT NULL;
ALTER TABLE metadatas ADD COLUMN document_id BIGINT REFERENCES documents(id);
ALTER TABLE metadatas ADD CONSTRAINT check_metadatas_object_id_document_id CHECK ((object_id IS NULL) <> (document_id IS NULL));
