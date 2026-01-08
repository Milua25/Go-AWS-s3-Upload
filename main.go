package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
)

var (
	s3Client *s3.Client
	err      error
)

const (
	bucketName  = "testing-aws-go-yjhu"
	letterBytes = "abcdefghijklmnopqrstuvwxyz"
)

type S3client interface {
	ListBuckets(ctx context.Context, params *s3.ListBucketsInput, optFns ...func(*s3.Options)) (*s3.ListBucketsOutput, error)
	CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
}

type S3Uploader interface {
	Upload(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error)
}

type s3Downloader interface {
	Download(ctx context.Context, w io.WriterAt, input *s3.GetObjectInput, options ...func(*manager.Downloader)) (n int64, err error)
}

// RandStringBytes Function
func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// initS3Client Function
func initS3Client(ctx context.Context, region string) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config %s", err)
	}
	return s3.NewFromConfig(cfg), nil
}

// main Function
func main() {
	ctx := context.Background()

	if s3Client, err = initS3Client(ctx, "us-east-1"); err != nil {
		log.Printf("init S3 Client error: %s", err)
		os.Exit(1)
	}

	if err = createS3Bucket(ctx, s3Client, bucketName); err != nil {
		log.Print(err)
		os.Exit(1)
	}

	if err = uploadToS3Bucket(ctx, manager.NewUploader(s3Client), bucketName); err != nil {
		log.Print(err)
		os.Exit(1)
	}
	fmt.Println("Upload Completed")

	if _, err = downloadFromS3(ctx, manager.NewDownloader(s3Client), bucketName); err != nil {
		log.Print(err)
		os.Exit(1)
	}
	fmt.Println("Download Completed")

}

// createS3Bucket Function
func createS3Bucket(ctx context.Context, s3Client S3client, bucketName string) error {
	var allBuckets *s3.ListBucketsOutput

	var discoveredBucket = false

	if allBuckets, err = s3Client.ListBuckets(ctx, &s3.ListBucketsInput{}); err != nil {
		log.Printf("could not list the s3 buckets %s", err)
	}

	for k, bucketsName := range allBuckets.Buckets {
		fmt.Printf("%v, %v\n", k, *bucketsName.Name)
		if *bucketsName.Name == bucketName {
			discoveredBucket = true
			fmt.Printf("Found s3 Bucket %s\n", bucketName)
		}
	}

	if !discoveredBucket {
		bucketOutput, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			return fmt.Errorf("unable to create s3 bucket %s", err)
		}
		fmt.Printf("s3 Bucket location is %s\n", *bucketOutput.Location)
	}

	return nil
}

// uploadToS3Bucket Function
func uploadToS3Bucket(ctx context.Context, uploader S3Uploader, bucketName string) error {
	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("test/test.text"),
		Body:   strings.NewReader("Hello Go!!!"),
	})
	if err != nil {
		return fmt.Errorf("unable to upload to s3 bucket %s", err)
	}
	return nil
}

// downloadFromS3 Function
func downloadFromS3(ctx context.Context, downloader s3Downloader, bucketName string) ([]byte, error) {

	buffer := manager.NewWriteAtBuffer([]byte{})

	numBytes, err := downloader.Download(ctx, buffer, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("test.txt"),
	})
	if err != nil {
		return nil, fmt.Errorf("download error: %s", err)
	}
	if numBytesReceived := len(buffer.Bytes()); numBytes != int64(numBytesReceived) {
		return nil, fmt.Errorf("number of bytes recevied does not match: %d vs %d", numBytes, numBytesReceived)
	}
	return buffer.Bytes(), nil
}
