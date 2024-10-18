-- name: DocumentInsert :one
INSERT INTO documents (
    content
) VALUES (
    @content
) RETURNING *;

-- name: DocumentFindById :one
SELECT *
FROM documents
WHERE id = @id;

-- name: DocumentFindByIds :many
SELECT *
FROM documents
WHERE id = ANY(@ids::UUID[]);
