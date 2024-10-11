package storage_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"gitlab.com/navyx/nexus/nexus-store/pkg/storage"
)

func TestSubmittedFile(t *testing.T) {
	t.SkipNow()
	filename := "TestS3Storage.txt"
	content := []byte("This is a adfasdfasdftest file.")
	ctx := context.Background()

	s3Storage, err := storage.NewS3Storage("ennio-test")
	if err != nil {
		t.Error(err)
		return
	}

	// Enable Auto Expiration life cycle management.
	err = s3Storage.EnableExpiration(1)
	if err != nil {
		t.Error(err)
		return
	}

	// Upload submitted file
	fileKey, err := s3Storage.AddDocument(ctx, filename, content, true, nil)
	if err != nil {
		t.Error(err)
	}

	// Then test GetDownloadURL.
	presignURL, err := s3Storage.GetDownloadURL(ctx, fileKey, 600)
	if err != nil {
		t.Error(err)
	}
	assert.NotEqual(t, 0, len(presignURL))

	// Try to download the file
	{
		resp, err := http.Get(presignURL)
		if err != nil {
			t.Error(err)
			return
		}
		body := resp.Body
		defer body.Close()
		downloadedData, err := io.ReadAll(body)
		if err != nil {
			t.Error(err)
			return
		}
		assert.Equal(t, content, downloadedData)
	}

	// Test DeleteFile
	err = s3Storage.DeleteDocument(ctx, fileKey)
	if err != nil {
		t.Error(err)
		return
	}

	// CORS setting
	{
		err := s3Storage.SetCORS([]string{"*"}, []string{"GET", "HEAD"}, 3000, []string{"Authorization"})
		if err != nil {
			t.Error(err)
		}
	}
}

func TestSetDocumentAutoExpire(t *testing.T) {
	t.SkipNow()
	s3Storage, err := storage.NewS3Storage("ennio-test")
	if err != nil {
		t.Error(err)
		return
	}

	ctx := context.Background()
	err = s3Storage.SetDocumentAutoExpire(ctx, "sdfjlkdsjflsdjfd", true)
	var awsError awserr.Error
	if errors.As(err, &awsError) {
		if awsError.Code() == s3.ErrCodeNoSuchKey {
			fmt.Println("No such key")
		}
	}
}
