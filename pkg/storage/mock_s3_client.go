package storage

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"sync"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// MockS3Client is a mock implementation of the S3ClientInterface for testing
type MockS3Client struct {
	Objects                map[string][]byte
	Tags                   map[string]map[string]string
	Metadata               map[string]map[string]string
	Cors                   *types.CORSConfiguration
	LifecycleConfiguration *types.BucketLifecycleConfiguration
	mutex                  sync.RWMutex
}

type MockS3PresignClient struct {
	PresignGetObjectFunc func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}

func (m *MockS3PresignClient) PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	return m.PresignGetObjectFunc(ctx, params, optFns...)
}

func (m *MockS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	key := *params.Key
	metadata, ok := m.Metadata[key]
	if !ok {
		return nil, &smithy.GenericAPIError{
			Code:    "NotFound",
			Message: "Object not found",
		}
	}

	return &s3.HeadObjectOutput{
		Metadata: metadata,
	}, nil
}

func (m *MockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	key := *params.Key
	data, ok := m.Objects[key]
	if !ok {
		return nil, &smithy.GenericAPIError{
			Code:    "NotFound",
			Message: "Object not found",
		}
	}

	return &s3.GetObjectOutput{
		Body:     io.NopCloser(bytes.NewReader(data)),
		Metadata: m.Metadata[key],
	}, nil
}

func (m *MockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	key := *params.Key
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	m.Objects[key] = data
	if params.Tagging != nil {
		if m.Tags[key] == nil {
			m.Tags[key] = make(map[string]string)
		}
		tagValues, err := url.ParseQuery(*params.Tagging)
		if err != nil {
			return nil, &smithy.GenericAPIError{
				Code:    "InvalidTagging",
				Message: "Invalid tagging format",
			}
		}
		for k, v := range tagValues {
			if len(v) > 0 {
				m.Tags[key][k] = v[0]
			}
		}
	}

	if params.Metadata != nil {
		if m.Metadata[key] == nil {
			m.Metadata[key] = make(map[string]string)
		}
		for k, v := range params.Metadata {
			m.Metadata[key][k] = v
		}
	}

	if params.Tagging != nil {
		m.Tags[key] = map[string]string{TagAutoExpire: "1"}
	}

	return &s3.PutObjectOutput{}, nil
}

func (m *MockS3Client) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	key := *params.Key
	delete(m.Objects, key)
	delete(m.Tags, key)

	return &s3.DeleteObjectOutput{}, nil
}

func (m *MockS3Client) PutObjectTagging(ctx context.Context, params *s3.PutObjectTaggingInput, optFns ...func(*s3.Options)) (*s3.PutObjectTaggingOutput, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	key := *params.Key
	if _, ok := m.Objects[key]; !ok {
		return nil, &smithy.GenericAPIError{
			Code:    "NotFound",
			Message: "Object not found",
		}
	}

	m.Tags[key] = make(map[string]string)
	for _, tag := range params.Tagging.TagSet {
		m.Tags[key][*tag.Key] = *tag.Value
	}

	return &s3.PutObjectTaggingOutput{}, nil
}

func (m *MockS3Client) DeleteObjectTagging(ctx context.Context, params *s3.DeleteObjectTaggingInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectTaggingOutput, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	key := *params.Key
	delete(m.Tags, key)

	return &s3.DeleteObjectTaggingOutput{}, nil
}

func (m *MockS3Client) PutBucketLifecycleConfiguration(ctx context.Context, params *s3.PutBucketLifecycleConfigurationInput, optFns ...func(*s3.Options)) (*s3.PutBucketLifecycleConfigurationOutput, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if params.LifecycleConfiguration == nil {
		return nil, &smithy.GenericAPIError{
			Code:    "NotFound",
			Message: "Lifecycle configuration not found",
		}
	}
	if len(params.LifecycleConfiguration.Rules) == 0 {
		return nil, &smithy.GenericAPIError{
			Code:    "NotFound",
			Message: "Lifecycle configuration not found",
		}
	}
	if *params.LifecycleConfiguration.Rules[0].ID != TagAutoExpire {
		return nil, &smithy.GenericAPIError{
			Code:    "NotFound",
			Message: "Lifecycle configuration not found",
		}
	}
	if params.LifecycleConfiguration.Rules[0].Expiration == nil || params.LifecycleConfiguration.Rules[0].Expiration.Days == nil {
		return nil, &smithy.GenericAPIError{
			Code:    "NotFound",
			Message: "Lifecycle configuration not found",
		}
	}
	if *params.LifecycleConfiguration.Rules[0].Expiration.Days <= 0 {
		return nil, &smithy.GenericAPIError{
			Code:    "NotFound",
			Message: "Lifecycle configuration not found",
		}
	}

	m.LifecycleConfiguration = params.LifecycleConfiguration
	return &s3.PutBucketLifecycleConfigurationOutput{}, nil
}

func (m *MockS3Client) DeleteBucketLifecycle(ctx context.Context, params *s3.DeleteBucketLifecycleInput, optFns ...func(*s3.Options)) (*s3.DeleteBucketLifecycleOutput, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.LifecycleConfiguration = nil
	return &s3.DeleteBucketLifecycleOutput{}, nil
}

func (m *MockS3Client) PutBucketCors(ctx context.Context, params *s3.PutBucketCorsInput, optFns ...func(*s3.Options)) (*s3.PutBucketCorsOutput, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if params.CORSConfiguration == nil {
		return nil, &smithy.GenericAPIError{
			Code:    "NotFound",
			Message: "CORS configuration not found",
		}
	}

	m.Cors = params.CORSConfiguration
	return &s3.PutBucketCorsOutput{}, nil
}
