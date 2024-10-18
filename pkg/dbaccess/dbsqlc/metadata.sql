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
ON CONFLICT (object_id, document_id, key, value) DO NOTHING
RETURNING *;

-- name: MetadataInsertBatch :exec
INSERT INTO metadatas (object_id, document_id, key, value)
SELECT unnest(@object_ids::uuid[]), unnest(@document_ids::uuid[]), unnest(@keys::text[]), unnest(@values::text[])
ON CONFLICT (object_id, document_id, key, value) DO NOTHING;

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
