-- name: DocumentInsert :one
INSERT INTO documents (
    content
) VALUES (
    @content
) RETURNING *;

-- name: DocumentUpdate :one
UPDATE documents
SET content = @content, created_at = EXTRACT(epoch FROM now())
WHERE id = @id
RETURNING id;

-- name: DocumentFindById :one
SELECT *
FROM documents
WHERE id = @id;

-- name: DocumentFindByIds :many
SELECT *
FROM documents
WHERE id = ANY(@ids::UUID[]);

-- name: DocumentOrObjectById :many
SELECT documents.id as document_id, NULL as object_id
FROM documents
WHERE documents.id = @id
UNION ALL
SELECT NULL as document_id, s3_objects.id as object_id
FROM s3_objects
WHERE s3_objects.id = @id;
