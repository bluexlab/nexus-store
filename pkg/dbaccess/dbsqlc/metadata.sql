-- name: MetadataInsert :one
INSERT INTO metadatas (
    object_id,
    document_id,
    key,
    value
) VALUES (
    @object_id,
    @document_id,
    @key,
    @value
)
ON CONFLICT (
    COALESCE(object_id,'00000000-0000-0000-0000-000000000000'),
    COALESCE(document_id,'00000000-0000-0000-0000-000000000000'),
    key
) DO UPDATE SET value = @value
RETURNING *;

-- name: MetadataInsertBatch :one
WITH document_or_object_id AS (
  SELECT documents.id as document_id, NULL as object_id
  FROM documents
  WHERE documents.id = @id::UUID
  UNION ALL
  SELECT NULL as document_id, s3_objects.id as object_id
  FROM s3_objects
  WHERE s3_objects.id = @id::UUID
), insert_metadata AS (
  INSERT INTO metadatas (object_id, document_id, key, value)
  SELECT doid.object_id, doid.document_id, unnest(@keys::text[]), unnest(@values::text[])
  FROM document_or_object_id as doid
  ON CONFLICT (
    COALESCE(object_id,'00000000-0000-0000-0000-000000000000'),
    COALESCE(document_id,'00000000-0000-0000-0000-000000000000'),
    key
  ) DO UPDATE SET value = EXCLUDED.value
)
SELECT * FROM document_or_object_id;

-- name: MetadataUpdateBatch :one
WITH document_or_object_id AS (
  SELECT documents.id as document_id, NULL as object_id
  FROM documents
  WHERE documents.id = @id::UUID
  UNION ALL
  SELECT NULL as document_id, s3_objects.id as object_id
  FROM s3_objects
  WHERE s3_objects.id = @id::UUID
), remove_unused_metadata AS (
  DELETE FROM metadatas
  WHERE (object_id = @id::UUID OR document_id = @id::UUID)
    AND key NOT IN (SELECT unnest(@keys::text[]))
), key_value_pairs AS (
  SELECT unnest(@keys::text[]) AS key, unnest(@values::text[]) AS value
), insert_metadata AS (
  INSERT INTO metadatas (object_id, document_id, key, value)
  SELECT doid.object_id, doid.document_id, unnest(@keys::text[]), unnest(@values::text[])
  FROM document_or_object_id as doid
  ON CONFLICT (
    COALESCE(object_id,'00000000-0000-0000-0000-000000000000'),
    COALESCE(document_id,'00000000-0000-0000-0000-000000000000'),
    key
  ) DO UPDATE SET value = EXCLUDED.value
)
SELECT * FROM document_or_object_id;

-- name: MetadataFindByDocumentId :many
SELECT key, value FROM metadatas WHERE document_id = @document_id;

-- name: MetadataFindByDocumentIds :many
SELECT
    document_id,
    jsonb_object_agg(key, value) AS metadata
FROM metadatas
WHERE document_id = ANY(@document_ids::uuid[])
GROUP BY document_id;

-- name: MetadataFindByObjectIds :many
SELECT
    object_id,
    jsonb_object_agg(key, value) AS metadata
FROM metadatas
WHERE object_id = ANY(@object_ids::uuid[])
GROUP BY object_id;
