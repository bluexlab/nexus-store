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