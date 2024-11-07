package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type S3ClientInterface interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	PutObjectTagging(ctx context.Context, params *s3.PutObjectTaggingInput, optFns ...func(*s3.Options)) (*s3.PutObjectTaggingOutput, error)
	DeleteObjectTagging(ctx context.Context, params *s3.DeleteObjectTaggingInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectTaggingOutput, error)
	PutBucketLifecycleConfiguration(ctx context.Context, params *s3.PutBucketLifecycleConfigurationInput, optFns ...func(*s3.Options)) (*s3.PutBucketLifecycleConfigurationOutput, error)
	DeleteBucketLifecycle(ctx context.Context, params *s3.DeleteBucketLifecycleInput, optFns ...func(*s3.Options)) (*s3.DeleteBucketLifecycleOutput, error)
	PutBucketCors(ctx context.Context, params *s3.PutBucketCorsInput, optFns ...func(*s3.Options)) (*s3.PutBucketCorsOutput, error)
}

type S3PresignClientInterface interface {
	PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}

// S3Storage is the implementation of storage.Storage based on AWS S3.
type S3Storage struct {
	bucket        string
	client        S3ClientInterface
	presignClient S3PresignClientInterface
}

const (
	// TagAutoExpire is the tag name for S3 bucket to auto expire objects
	TagAutoExpire = "auto_expire"
)

// NewS3Storage create S3 implementation for Storage interface
// The credential is stored in environment variable "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY"
func NewS3Storage(ctx context.Context, bucket string) (s *S3Storage, err error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		slog.Error("Fail to load AWS config", "err", err)
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	presignClient := s3.NewPresignClient(client)

	s = &S3Storage{
		bucket:        bucket,
		client:        client,
		presignClient: presignClient,
	}
	return s, nil
}

// NewS3InMemoryStorage creates a new S3Storage implementation that does not interact with AWS S3,
// used only for testing.
func NewS3InMemoryStorage(ctx context.Context, bucket string) (s *S3Storage, err error) {
	mockClient := &MockS3Client{
		Objects:  make(map[string][]byte),
		Tags:     make(map[string]map[string]string),
		Metadata: make(map[string]map[string]string),
	}
	mockPresignClient := &MockS3PresignClient{
		PresignGetObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
			if mockClient.Objects[*params.Key] == nil {
				return nil, &smithy.GenericAPIError{
					Code:    "NotFound",
					Message: "Object not found",
				}
			}
			return &v4.PresignedHTTPRequest{
				URL: "https://mock-presigned-url.com/" + *params.Key,
			}, nil
		},
	}

	s = &S3Storage{
		bucket:        bucket,
		client:        mockClient,
		presignClient: mockPresignClient,
	}
	return s, nil
}

// GetFileName get the file name from a file key.
func GetFileName(fileKey string) string {
	parts := strings.Split(fileKey, "/")
	return parts[len(parts)-1]
}

func (s *S3Storage) GetMetadata(ctx context.Context, fileKey string) (map[string]string, error) {
	getObjInput := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fileKey),
	}
	result, err := s.client.HeadObject(ctx, getObjInput)
	if err != nil {
		return nil, TranslateError(err)
	}
	return result.Metadata, nil
}

// AddDocument adds a document into the storage.
func (s *S3Storage) AddDocument(ctx context.Context, fileKey string, data []byte, autoExpire bool, metadata map[string]string) (string, error) {
	var tagging *string
	if autoExpire {
		str := fmt.Sprintf("%s=1", TagAutoExpire)
		tagging = &str
	}

	putObjInput := &s3.PutObjectInput{
		Body:     bytes.NewReader(data),
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(fileKey),
		Tagging:  tagging,
		Metadata: metadata,
	}
	_, err := s.client.PutObject(ctx, putObjInput)
	if err != nil {
		slog.Error("Fail to PutObject", "err", err, "bucket", s.bucket, "key", fileKey)
		return "", err
	}

	return fileKey, nil
}

func (s *S3Storage) GetDocument(ctx context.Context, fileKey string) ([]byte, error) {
	getObjInput := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fileKey),
	}
	result, err := s.client.GetObject(ctx, getObjInput)
	if err != nil {
		return nil, TranslateError(err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, TranslateError(err)
	}

	return data, nil
}

// SetDocumentAutoExpire sets the tag of the document to make to be auto expired or not.
func (s *S3Storage) SetDocumentAutoExpire(ctx context.Context, fileKey string, autoExpire bool) error {
	if autoExpire {
		input := &s3.PutObjectTaggingInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(fileKey),
			Tagging: &types.Tagging{
				TagSet: []types.Tag{
					{
						Key:   aws.String(TagAutoExpire),
						Value: aws.String("1"),
					},
				},
			},
		}

		_, err := s.client.PutObjectTagging(ctx, input)
		if err != nil {
			return TranslateError(err)
		}
		return nil
	}

	input := &s3.DeleteObjectTaggingInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fileKey),
	}
	_, err := s.client.DeleteObjectTagging(ctx, input)
	if err != nil {
		return TranslateError(err)
	}
	return nil
}

// GetDownloadURL returns an URL with expiration duration to download the specified file.
func (s *S3Storage) GetDownloadURL(ctx context.Context, fileKey string, expire int64) (string, error) {
	request, err := s.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fileKey),
	}, s3.WithPresignExpires(time.Duration(expire)*time.Second))

	if err != nil {
		slog.Error("Fail to generate presigned URL", "err", err, "bucket", s.bucket, "key", fileKey)
		return "", TranslateError(err)
	}
	return request.URL, nil
}

// DeleteDocument deletes a document.
func (s *S3Storage) DeleteDocument(ctx context.Context, fileKey string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fileKey),
	}
	_, err := s.client.DeleteObject(ctx, input)
	if err != nil {
		slog.Error("Fail to delete object", "err", err, "bucket", s.bucket, "key", fileKey)
		return TranslateError(err)
	}
	return nil
}

// EnableExpiration sets the expiration day of auto expire documents.
func (s *S3Storage) EnableExpiration(ctx context.Context, days int32) error {
	input := &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(s.bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:     aws.String(TagAutoExpire),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{
						Tag: &types.Tag{
							Key:   aws.String(TagAutoExpire),
							Value: aws.String("1"),
						},
					},
					Expiration: &types.LifecycleExpiration{
						Days: aws.Int32(days),
					},
				},
			},
		},
	}

	_, err := s.client.PutBucketLifecycleConfiguration(ctx, input)
	return err
}

// DeleteBucketLifecycle removes all the lifecycle configuration rules in the lifecycle subresource associated with the bucket.
func (s *S3Storage) DeleteBucketLifecycle(ctx context.Context) error {
	input := &s3.DeleteBucketLifecycleInput{
		Bucket: aws.String(s.bucket),
	}

	_, err := s.client.DeleteBucketLifecycle(ctx, input)
	if err != nil {
		return err
	}
	slog.Info("S3Storage::DeleteBucketLifecycle() completed")
	return nil
}

// SetCORS updates CORS setting of the S3 bucket.
func (s *S3Storage) SetCORS(ctx context.Context, origins []string, methods []string, maxAgeSeconds int64, headers []string) error {
	input := &s3.PutBucketCorsInput{
		Bucket: aws.String(s.bucket),
		CORSConfiguration: &types.CORSConfiguration{
			CORSRules: []types.CORSRule{
				{
					AllowedOrigins: origins,
					AllowedMethods: methods,
					MaxAgeSeconds:  aws.Int32(int32(maxAgeSeconds)),
					AllowedHeaders: headers,
				},
			},
		},
	}

	_, err := s.client.PutBucketCors(ctx, input)
	if err != nil {
		slog.Error("Fail to PutBucketCors", "err", err, "bucket", s.bucket)
		return err
	}

	return nil
}

func (s *S3Storage) GetBucket() string {
	return s.bucket
}

// Client returns the S3 client. for testing only.
func (s *S3Storage) Client() S3ClientInterface {
	return s.client
}

func TranslateError(err error) error {
	return err
}
