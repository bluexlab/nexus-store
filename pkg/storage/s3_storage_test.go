package storage_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/navyx/nexus/nexus-store/pkg/storage"
)

func TestS3Storage(t *testing.T) {
	// Skip the test because it requires AWS credentials
	// To run the test, please set the AWS credentials in the environment variables:
	// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
	t.SkipNow()

	ctx := context.Background()
	s3Storage, err := storage.NewS3Storage(ctx, os.Getenv("FILE_S3_BUCKET"))
	assert.NoError(t, err)

	filename := "test.txt"
	content := []byte("This is a test file.")
	metadata := map[string]string{"filename": "test.txt"}
	fileKey, err := s3Storage.AddDocument(ctx, filename, content, true, metadata)
	assert.NoError(t, err)
	assert.Equal(t, filename, fileKey)

	storedContent, err := s3Storage.GetDocument(ctx, filename)
	assert.NoError(t, err)
	assert.Equal(t, content, storedContent)

	storedMetadata, err := s3Storage.GetMetadata(ctx, filename)
	assert.NoError(t, err)
	assert.Equal(t, metadata, storedMetadata)

	err = s3Storage.DeleteDocument(ctx, filename)
	assert.NoError(t, err)

	_, err = s3Storage.GetDocument(ctx, filename)
	assert.Error(t, err)
}

func TestAddDocument(t *testing.T) {
	ctx := context.Background()
	s3Storage, err := storage.NewS3InMemoryStorage(ctx, "test-bucket")
	assert.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		filename := "test.txt"
		content := []byte("This is a test file.")
		fileKey, err := s3Storage.AddDocument(ctx, filename, content, true, nil)

		assert.NoError(t, err)
		assert.Equal(t, filename, fileKey)

		// Verify document is stored
		storedContent, err := s3Storage.GetDocument(ctx, filename)
		assert.NoError(t, err)
		assert.Equal(t, content, storedContent)

		// Verify auto-expire tag is set
		mockClient := s3Storage.Client().(*storage.MockS3Client)
		tags := mockClient.Tags[filename]
		assert.Equal(t, map[string]string{storage.TagAutoExpire: "1"}, tags)
	})

	t.Run("WithMetadata", func(t *testing.T) {
		filename := "metadata.txt"
		content := []byte("File with metadata")
		metadata := map[string]string{"key": "value"}
		fileKey, err := s3Storage.AddDocument(ctx, filename, content, false, metadata)

		assert.NoError(t, err)
		assert.Equal(t, filename, fileKey)

		// Verify document is stored
		storedContent, err := s3Storage.GetDocument(ctx, filename)
		assert.NoError(t, err)
		assert.Equal(t, content, storedContent)

		// Verify metadata is stored
		storedMetadata, err := s3Storage.GetMetadata(ctx, filename)
		assert.NoError(t, err)
		assert.Equal(t, metadata, storedMetadata)

		// Verify auto-expire tag is not set
		_, exists := storedMetadata[storage.TagAutoExpire]
		assert.False(t, exists)
	})
}

func TestGetDownloadURL(t *testing.T) {
	ctx := context.Background()
	s3Storage, err := storage.NewS3InMemoryStorage(ctx, "test-bucket")
	assert.NoError(t, err)

	t.Run("ExistingFile", func(t *testing.T) {
		filename := "existing.txt"
		content := []byte("Existing file")
		_, err := s3Storage.AddDocument(ctx, filename, content, false, nil)
		assert.NoError(t, err)

		url, err := s3Storage.GetDownloadURL(ctx, filename, 600)

		assert.NoError(t, err)
		assert.Contains(t, url, filename)
	})

	t.Run("NonExistentFile", func(t *testing.T) {
		url, err := s3Storage.GetDownloadURL(ctx, "nonexistent.txt", 600)

		assert.Error(t, err)
		assert.Empty(t, url)
	})
}

func TestDeleteDocument(t *testing.T) {
	ctx := context.Background()
	s3Storage, err := storage.NewS3InMemoryStorage(ctx, "test-bucket")
	assert.NoError(t, err)

	t.Run("ExistingFile", func(t *testing.T) {
		filename := "to_delete.txt"
		content := []byte("File to delete")
		_, err := s3Storage.AddDocument(ctx, filename, content, false, nil)
		assert.NoError(t, err)

		// Verify file exists before deletion
		_, err = s3Storage.GetDocument(ctx, filename)
		assert.NoError(t, err)

		err = s3Storage.DeleteDocument(ctx, filename)
		assert.NoError(t, err)

		// Verify file is deleted
		_, err = s3Storage.GetDocument(ctx, filename)
		assert.Error(t, err)
	})

	t.Run("NonExistentFile", func(t *testing.T) {
		err := s3Storage.DeleteDocument(ctx, "nonexistent.txt")
		assert.NoError(t, err)

		// Verify attempting to get non-existent file returns an error
		_, err = s3Storage.GetDocument(ctx, "nonexistent.txt")
		assert.Error(t, err)
	})
}

func TestEnableExpiration(t *testing.T) {
	ctx := context.Background()
	s3Storage, err := storage.NewS3InMemoryStorage(ctx, "test-bucket")
	assert.NoError(t, err)

	t.Run("ValidDays", func(t *testing.T) {
		err := s3Storage.EnableExpiration(ctx, 30)
		assert.NoError(t, err)

		mockClient := s3Storage.Client().(*storage.MockS3Client)
		assert.NotNil(t, mockClient.LifecycleConfiguration)
		assert.Len(t, mockClient.LifecycleConfiguration.Rules, 1)
		assert.Equal(t, int32(30), *mockClient.LifecycleConfiguration.Rules[0].Expiration.Days)
	})

	t.Run("ZeroDays", func(t *testing.T) {
		err := s3Storage.EnableExpiration(ctx, 0)
		assert.Error(t, err)
	})

	t.Run("NegativeDays", func(t *testing.T) {
		err := s3Storage.EnableExpiration(ctx, -1)
		assert.Error(t, err)
	})

	t.Run("DisableExpiration", func(t *testing.T) {
		err := s3Storage.DeleteBucketLifecycle(ctx)
		assert.NoError(t, err)

		mockClient := s3Storage.Client().(*storage.MockS3Client)
		assert.Nil(t, mockClient.LifecycleConfiguration)
	})
}

func TestSetCORS(t *testing.T) {
	ctx := context.Background()
	s3Storage, err := storage.NewS3InMemoryStorage(ctx, "test-bucket")
	assert.NoError(t, err)

	t.Run("ValidInput", func(t *testing.T) {
		err := s3Storage.SetCORS(ctx, []string{"*"}, []string{"GET", "HEAD"}, 3000, []string{"Authorization"})
		assert.NoError(t, err)

		// Validate CORS is set
		mockClient := s3Storage.Client().(*storage.MockS3Client)
		assert.NotNil(t, mockClient.Cors)
		assert.Equal(t, 1, len(mockClient.Cors.CORSRules))
		assert.Equal(t, []string{"*"}, mockClient.Cors.CORSRules[0].AllowedOrigins)
		assert.Equal(t, []string{"GET", "HEAD"}, mockClient.Cors.CORSRules[0].AllowedMethods)
		assert.Equal(t, int32(3000), *mockClient.Cors.CORSRules[0].MaxAgeSeconds)
		assert.Equal(t, []string{"Authorization"}, mockClient.Cors.CORSRules[0].AllowedHeaders)
	})

	t.Run("EmptyOrigins", func(t *testing.T) {
		err := s3Storage.SetCORS(ctx, []string{}, []string{"GET"}, 3000, nil)
		assert.NoError(t, err)

		// Validate CORS is set
		mockClient := s3Storage.Client().(*storage.MockS3Client)
		assert.NotNil(t, mockClient.Cors)
		assert.Equal(t, 1, len(mockClient.Cors.CORSRules))
		assert.Empty(t, mockClient.Cors.CORSRules[0].AllowedOrigins)
		assert.Equal(t, []string{"GET"}, mockClient.Cors.CORSRules[0].AllowedMethods)
		assert.Equal(t, int32(3000), *mockClient.Cors.CORSRules[0].MaxAgeSeconds)
		assert.Nil(t, mockClient.Cors.CORSRules[0].AllowedHeaders)
	})

	t.Run("NegativeMaxAge", func(t *testing.T) {
		err := s3Storage.SetCORS(ctx, []string{"*"}, []string{"GET"}, -1, nil)
		assert.NoError(t, err)

		// Validate CORS is set
		mockClient := s3Storage.Client().(*storage.MockS3Client)
		assert.NotNil(t, mockClient.Cors)
		assert.Equal(t, 1, len(mockClient.Cors.CORSRules))
		assert.Equal(t, []string{"*"}, mockClient.Cors.CORSRules[0].AllowedOrigins)
		assert.Equal(t, []string{"GET"}, mockClient.Cors.CORSRules[0].AllowedMethods)
		assert.Equal(t, int32(-1), *mockClient.Cors.CORSRules[0].MaxAgeSeconds)
		assert.Nil(t, mockClient.Cors.CORSRules[0].AllowedHeaders)
	})
}

func TestSetDocumentAutoExpire(t *testing.T) {
	ctx := context.Background()
	s3Storage, err := storage.NewS3InMemoryStorage(ctx, "test-bucket")
	assert.NoError(t, err)

	t.Run("ExistingDocument", func(t *testing.T) {
		filename := "auto_expire.txt"
		content := []byte("Auto expire test")
		_, err := s3Storage.AddDocument(ctx, filename, content, false, nil)
		assert.NoError(t, err)

		err = s3Storage.SetDocumentAutoExpire(ctx, filename, true)
		assert.NoError(t, err)

		// Validate tag is set
		mockClient := s3Storage.Client().(*storage.MockS3Client)
		assert.Equal(t, map[string]string{storage.TagAutoExpire: "1"}, mockClient.Tags[filename])

		err = s3Storage.SetDocumentAutoExpire(ctx, filename, false)
		assert.NoError(t, err)

		// Validate tag is removed
		assert.Empty(t, mockClient.Tags[filename])
	})

	t.Run("NonExistentDocument", func(t *testing.T) {
		err := s3Storage.SetDocumentAutoExpire(ctx, "nonexistent.txt", true)
		assert.Error(t, err)

		// Validate tag is not set
		mockClient := s3Storage.Client().(*storage.MockS3Client)
		assert.Empty(t, mockClient.Tags["nonexistent.txt"])
	})
}
