-- For unstructure data
CREATE TABLE s3_objects (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY, -- id is also the key of the object in the S3 bucket
  created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())
);

-- For semi-structure data that is of JSON format
CREATE TABLE documents (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  content JSONB NOT NULL,
  created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())
);

CREATE TABLE metadatas (
  object_id UUID REFERENCES s3_objects(id),
  document_id UUID REFERENCES documents(id),
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())
);
CREATE UNIQUE INDEX idx_metadatas_uniq_key ON metadatas(object_id, document_id, key, value);
ALTER TABLE metadatas ADD CONSTRAINT check_metadatas_object_id_document_id CHECK ((object_id IS NULL) <> (document_id IS NULL));
