ALTER TABLE metadatas DROP CONSTRAINT check_metadatas_object_id_document_id;
ALTER TABLE metadatas DROP COLUMN document_id;
ALTER TABLE metadatas ALTER COLUMN object_id SET NOT NULL;
DROP TABLE documents;
