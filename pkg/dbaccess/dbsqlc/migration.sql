-- name: MigrationDeleteByVersionMany :many
DELETE FROM _migration
WHERE version = any(@version::bigint[])
RETURNING *;

-- name: MigrationGetAll :many
SELECT *
FROM _migration
ORDER BY version;

-- name: MigrationInsert :one
INSERT INTO _migration (
    version
) VALUES (
    @version
) RETURNING *;

-- name: MigrationInsertMany :many
INSERT INTO _migration (
    version
)
SELECT
    unnest(@version::bigint[])
RETURNING *;

-- name: TableExists :one
SELECT CASE WHEN to_regclass(@table_name) IS NULL THEN false
            ELSE true END;
