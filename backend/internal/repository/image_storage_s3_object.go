package repository

// YeaGrow execution plane (T2.4): object writes with customMetadata + SHA-256 checksum, and readback.
// Origin: finefu/sub2api branch yeagrow/execution-plane (base Wei-Shaw/sub2api ee58c0013).

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/Wei-Shaw/sub2api/internal/pkg/servertiming"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

var _ service.ImageObjectStorage = (*S3ImageStorage)(nil)

// imageObjectReadLimit bounds a readback (objects are <= 10 MiB).
const imageObjectReadLimit = service.ImageTaskAssetMaxBytes + 1

// PutImageObject writes key with the given content type, user metadata
// (x-amz-meta-*) and an x-amz-checksum-sha256 the store verifies and keeps.
func (s *S3ImageStorage) PutImageObject(ctx context.Context, key, contentType string, data []byte, metadata map[string]string, sha256Digest [32]byte) error {
	checksum := base64.StdEncoding.EncodeToString(sha256Digest[:])
	finish := servertiming.ObserveDependency(ctx, "s3")
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:         &s.bucket,
		Key:            &key,
		Body:           bytes.NewReader(data),
		ContentType:    &contentType,
		Metadata:       metadata,
		ChecksumSHA256: &checksum,
	})
	finish()
	if err != nil {
		return fmt.Errorf("S3 PutObject: %w", err)
	}
	return nil
}

// GetImageObject reads the object back with its checksum (x-amz-checksum-mode: ENABLED).
func (s *S3ImageStorage) GetImageObject(ctx context.Context, key string) (*service.StoredImageObject, error) {
	finish := servertiming.ObserveDependency(ctx, "s3")
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       &s.bucket,
		Key:          &key,
		ChecksumMode: types.ChecksumModeEnabled,
	})
	finish()
	if err != nil {
		return nil, fmt.Errorf("S3 GetObject: %w", err)
	}
	defer func() { _ = out.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(out.Body, imageObjectReadLimit))
	if err != nil {
		return nil, fmt.Errorf("S3 GetObject body: %w", err)
	}
	stored := &service.StoredImageObject{Data: data, Metadata: map[string]string{}}
	if out.ContentType != nil {
		stored.ContentType = *out.ContentType
	}
	for k, v := range out.Metadata {
		stored.Metadata[strings.ToLower(k)] = v
	}
	if out.ChecksumSHA256 != nil {
		raw, err := base64.StdEncoding.DecodeString(*out.ChecksumSHA256)
		if err == nil {
			stored.ChecksumSHA256 = raw
		}
	}
	return stored, nil
}
