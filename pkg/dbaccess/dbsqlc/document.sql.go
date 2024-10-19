// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: document.sql

package dbsqlc

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

const documentFindById = `-- name: DocumentFindById :one
SELECT id, content, created_at
FROM documents
WHERE id = $1
`

func (q *Queries) DocumentFindById(ctx context.Context, db DBTX, id pgtype.UUID) (*Document, error) {
	row := db.QueryRow(ctx, documentFindById, id)
	var i Document
	err := row.Scan(&i.ID, &i.Content, &i.CreatedAt)
	return &i, err
}

const documentFindByIds = `-- name: DocumentFindByIds :many
SELECT id, content, created_at
FROM documents
WHERE id = ANY($1::UUID[])
`

func (q *Queries) DocumentFindByIds(ctx context.Context, db DBTX, ids []pgtype.UUID) ([]*Document, error) {
	rows, err := db.Query(ctx, documentFindByIds, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*Document
	for rows.Next() {
		var i Document
		if err := rows.Scan(&i.ID, &i.Content, &i.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const documentInsert = `-- name: DocumentInsert :one
INSERT INTO documents (
    content
) VALUES (
    $1
) RETURNING id, content, created_at
`

func (q *Queries) DocumentInsert(ctx context.Context, db DBTX, content []byte) (*Document, error) {
	row := db.QueryRow(ctx, documentInsert, content)
	var i Document
	err := row.Scan(&i.ID, &i.Content, &i.CreatedAt)
	return &i, err
}

const documentOrObjectById = `-- name: DocumentOrObjectById :many
SELECT documents.id as document_id, NULL as object_id
FROM documents
WHERE documents.id = $1
UNION ALL
SELECT NULL as document_id, s3_objects.id as object_id
FROM s3_objects
WHERE s3_objects.id = $1
`

type DocumentOrObjectByIdRow struct {
	DocumentID pgtype.UUID
	ObjectID   interface{}
}

func (q *Queries) DocumentOrObjectById(ctx context.Context, db DBTX, id pgtype.UUID) ([]*DocumentOrObjectByIdRow, error) {
	rows, err := db.Query(ctx, documentOrObjectById, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []*DocumentOrObjectByIdRow
	for rows.Next() {
		var i DocumentOrObjectByIdRow
		if err := rows.Scan(&i.DocumentID, &i.ObjectID); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const documentUpdate = `-- name: DocumentUpdate :one
UPDATE documents
SET content = $1, created_at = EXTRACT(epoch FROM now())
WHERE id = $2
RETURNING id
`

type DocumentUpdateParams struct {
	Content []byte
	ID      pgtype.UUID
}

func (q *Queries) DocumentUpdate(ctx context.Context, db DBTX, arg *DocumentUpdateParams) (pgtype.UUID, error) {
	row := db.QueryRow(ctx, documentUpdate, arg.Content, arg.ID)
	var id pgtype.UUID
	err := row.Scan(&id)
	return id, err
}
