package storage

import (
	"context"
	"errors"
)

var (
	ErrNoSuchKey = errors.New("no such key")
)

// Storage is the interface provide document storage service for bookkeeper.
type Storage interface {
	// AddDocument adds a document into the storage.
	AddDocument(ctx context.Context, fileKey string, data []byte, autoExpire bool, metadata map[string]*string) (string, error)

	// SetDocumentAutoExpire sets the tag of the document to make to be auto expired or not.
	SetDocumentAutoExpire(ctx context.Context, fileKey string, autoExpire bool) error

	// GetDownloadURL returns an URL with expiration duration to download the specified file.
	GetDownloadURL(ctx context.Context, fileKey string, expire int64) (string, error)

	// DeleteDocument deletes a document.
	DeleteDocument(ctx context.Context, fileKey string) error
}
