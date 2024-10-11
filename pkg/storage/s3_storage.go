package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
)

// S3Storage is the implementation of storage.Storage based on AWS S3.
type S3Storage struct {
	bucket string
	s3     *s3.S3
}

const (
	// TagAutoExpire is the tag name for S3 bucket to auto expire objects
	TagAutoExpire = "auto_expire"
)

// NewS3Storage create S3 implementation for Storage interface
// The credential is stored in environment variable "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY"
func NewS3Storage(bucket string) (s *S3Storage, err error) {
	sess, err := session.NewSession()
	if err != nil {
		slog.Error("Fail to create AWS session", "err", err)
		return
	}

	forcePathStyle := true
	// disableEndpointHostPrefix := true
	s3 := s3.New(sess, &aws.Config{
		S3ForcePathStyle: &forcePathStyle,
		// DisableEndpointHostPrefix: &disableEndpointHostPrefix,
	})
	s = &S3Storage{
		bucket: bucket,
		s3:     s3,
	}
	return
}

// GenFileKey generates a file key.
func GenFileKey(fileName string) string {
	uuid := uuid.NewString()
	return fmt.Sprintf("%s/%s", uuid, fileName)
}

// GetFileName get the file name from a file key.
func GetFileName(fileKey string) string {
	parts := strings.Split(fileKey, "/")
	return parts[len(parts)-1]
}

// AddDocument adds a document into the storage.
func (s *S3Storage) AddDocument(ctx context.Context, fileKey string, data []byte, autoExpire bool, metadata map[string]*string) (string, error) {
	var tag *string
	if autoExpire {
		str := fmt.Sprintf("%s=1", TagAutoExpire)
		tag = &str
	}

	body := bytes.NewReader(data)
	putObjInput := &s3.PutObjectInput{
		Body:     body,
		Bucket:   &s.bucket,
		Key:      &fileKey,
		Tagging:  tag,
		Metadata: metadata,
	}
	_, err := s.s3.PutObjectWithContext(ctx, putObjInput)
	if err != nil {
		slog.Error("Fail to PutObject", "err", err, "bucket", s.bucket, "key", fileKey)
		return "", err
	}

	return fileKey, nil
}

// SetDocumentAutoExpire sets the tag of the document to make to be auto expired or not.
func (s *S3Storage) SetDocumentAutoExpire(ctx context.Context, fileKey string, autoExpire bool) error {
	if autoExpire {
		tag := &s3.Tag{}
		tag.SetKey(TagAutoExpire)
		tag.SetValue("1")

		input := &s3.PutObjectTaggingInput{
			Bucket: &s.bucket,
			Key:    &fileKey,
			Tagging: &s3.Tagging{
				TagSet: []*s3.Tag{tag},
			},
		}

		_, err := s.s3.PutObjectTaggingWithContext(ctx, input)
		if err != nil {
			return TranslateError(err)
		}
		return nil
	}

	input := &s3.DeleteObjectTaggingInput{
		Bucket: &s.bucket,
		Key:    &fileKey,
	}
	_, err := s.s3.DeleteObjectTaggingWithContext(ctx, input)
	if err != nil {
		return TranslateError(err)
	}
	return nil
}

// GetDownloadURL returns an URL with expiration duration to download the specified file.
func (s *S3Storage) GetDownloadURL(ctx context.Context, fileKey string, expire int64) (string, error) {
	input := &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &fileKey,
	}
	request, _ := s.s3.GetObjectRequest(input)
	request.SetContext(ctx)
	expireDuration := time.Second * time.Duration(expire)
	url, err := request.Presign(expireDuration)
	if err != nil {
		slog.Error("Fail to generate presigned URL", "err", err, "bucket", s.bucket, "key", fileKey)
		return "", TranslateError(err)
	}
	return url, nil
}

// DeleteDocument deletes a document.
func (s *S3Storage) DeleteDocument(ctx context.Context, fileKey string) error {
	input := &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &fileKey,
	}
	_, err := s.s3.DeleteObject(input)
	if err != nil {
		slog.Error("Fail to delete object", "err", err, "bucket", s.bucket, "key", fileKey)
		return TranslateError(err)
	}
	return nil
}

// EnableExpiration sets the expiration day of auto expire documents.
func (s *S3Storage) EnableExpiration(days int64) (err error) {
	tag := &s3.Tag{}
	tag.SetKey(TagAutoExpire)
	tag.SetValue("1")
	lifecycleRuleFilter := &s3.LifecycleRuleFilter{}
	lifecycleRuleFilter.SetTag(tag)
	expiration := &s3.LifecycleExpiration{}
	expiration.SetDays(days)
	lifecycleRule := &s3.LifecycleRule{}
	lifecycleRule.SetID(TagAutoExpire)
	lifecycleRule.SetExpiration(expiration)
	lifecycleRule.SetFilter(lifecycleRuleFilter)
	lifecycleRule.SetStatus("Enabled")

	input := &s3.PutBucketLifecycleConfigurationInput{
		Bucket: &s.bucket,
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				lifecycleRule,
			},
		},
	}

	_, err = s.s3.PutBucketLifecycleConfiguration(input)
	if err != nil {
		return
	}
	return
}

// DeleteBucketLifecycle removes all the lifecycle configuration rules in the lifecycle subresource associated with the bucket.
func (s *S3Storage) DeleteBucketLifecycle() error {
	input := &s3.DeleteBucketLifecycleInput{
		Bucket: &s.bucket,
	}

	result, err := s.s3.DeleteBucketLifecycle(input)
	if err != nil {
		return err
	}
	slog.Info("S3Storage::DeleteBucketLifecycle() result", "result", result)
	return nil
}

// SetCORS updates CORS setting of the S3 bucket.
func (s *S3Storage) SetCORS(origins []string, methods []string, maxAgeSeconds int64, headers []string) (err error) {
	input := &s3.PutBucketCorsInput{
		Bucket: &s.bucket,
		CORSConfiguration: &s3.CORSConfiguration{
			CORSRules: []*s3.CORSRule{
				&s3.CORSRule{},
			},
		},
	}

	transformStringSlice := func(strs []string) []*string {
		r := make([]*string, 0)
		for _, str := range strs {
			// DON'T REMOVE tmpStr:=str.
			// Because "str" is the same instance in all iterations of this for loop.
			tmpStr := str
			r = append(r, &tmpStr)
		}
		return r
	}

	corsRule := input.CORSConfiguration.CORSRules[0]
	if len(origins) > 0 {
		corsRule.AllowedOrigins = transformStringSlice(origins)
	}
	if len(methods) > 0 {
		corsRule.AllowedMethods = transformStringSlice(methods)
	}
	if maxAgeSeconds > 0 {
		corsRule.MaxAgeSeconds = &maxAgeSeconds
	}
	if len(headers) > 0 {
		corsRule.AllowedHeaders = transformStringSlice(headers)
	}

	_, err = s.s3.PutBucketCors(input)
	if err != nil {
		slog.Error("Fail to PutBucketCors", "err", err, "bucket", s.bucket)
		return
	}

	return
}

func (s *S3Storage) GetBucket() string {
	return s.bucket
}

func TranslateError(err error) error {
	var awsError awserr.Error
	if errors.As(err, &awsError) && awsError.Code() == s3.ErrCodeNoSuchKey {
		return ErrNoSuchKey
	}
	return err
}
