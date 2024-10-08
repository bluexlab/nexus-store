-- name: S3ObjectInsert :one
INSERT INTO s3_objects (
    bucket,
    key
) VALUES (
    @bucket,
    @key
) RETURNING *;

-- name: S3ObjectFindById :one
SELECT *
FROM s3_objects
WHERE id = @id;
