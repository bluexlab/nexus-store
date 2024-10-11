-- name: S3ObjectInsert :one
INSERT INTO s3_objects DEFAULT VALUES RETURNING *;

-- name: S3ObjectFindById :one
SELECT *
FROM s3_objects
WHERE id = @id;
