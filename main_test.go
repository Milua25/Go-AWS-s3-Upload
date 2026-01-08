package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"testing"
)

type MockS3Client struct {
	ListBucketOutput   *s3.ListBucketsOutput
	CreateBucketOutput *s3.CreateBucketOutput
}

func (m MockS3Client) ListBuckets(ctx context.Context, params *s3.ListBucketsInput, optFns ...func(*s3.Options)) (*s3.ListBucketsOutput, error) {
	return m.ListBucketOutput, nil
}
func (m MockS3Client) CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	return m.CreateBucketOutput, nil
}

type MockS3Uploader struct {
	UploadOutput *manager.UploadOutput
}

func (m MockS3Uploader) Upload(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
	return m.UploadOutput, nil
}

func TestCreateS3Bucket(t *testing.T) {

	ctx := context.Background()

	err = createS3Bucket(ctx, MockS3Client{
		ListBucketOutput: &s3.ListBucketsOutput{
			Buckets: []types.Bucket{
				{
					Name: aws.String("test-bucket"),
				},
				{
					Name: aws.String("test-bucket-2"),
				},
			},
		},
		CreateBucketOutput: &s3.CreateBucketOutput{
			Location: aws.String("us-east-1"),
		},
	}, "test-bucket-1")

	if err != nil {
		t.Fatalf("create s3 bucket error: %s", err)
	}
}

func TestUploadToS3Bucket(t *testing.T) {
	mockUploader := MockS3Uploader{
		UploadOutput: &manager.UploadOutput{},
	}
	err := uploadToS3Bucket(context.Background(), mockUploader, "testdata/test.txt")
	if err != nil {
		t.Fatalf("UploadToS3Bucket error: %s", err)
	}
}
