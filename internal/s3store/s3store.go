// Package s3store is the only place that talks to the object storage.
// Two calls: list by prefix, delete a batch of keys. Nothing else.
package s3store

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"limewire-purge/internal/spconfig"
)

// Object is one listed key.
type Object struct {
	Key  string
	Size int64
}

// KeyError is a per-key failure reported inside a DeleteObjects response.
type KeyError struct {
	Key, Code, Message string
}

func (e KeyError) String() string { return fmt.Sprintf("%s: %s %s", e.Key, e.Code, e.Message) }

type Store struct {
	c      *s3.Client
	Bucket string
}

// New builds a client from the SP's BucketURL / IAMType.
//   - IAMType=SA : nothing to do, the SDK picks up AWS_ROLE_ARN + AWS_WEB_IDENTITY_TOKEN_FILE (IRSA).
//   - IAMType=AKSK: the SP uses AWS_ACCESS_KEY / AWS_SECRET_KEY / AWS_SESSION_TOKEN; map them.
func New(ctx context.Context, ps spconfig.ObjectStorage) (*Store, error) {
	t, err := spconfig.ParseBucketURL(ps.BucketURL)
	if err != nil {
		return nil, fmt.Errorf("BucketURL: %w", err)
	}
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(t.Region)}
	if ps.IAMType == "AKSK" || ps.IAMType == "" {
		ak, sk, tok := os.Getenv("AWS_ACCESS_KEY"), os.Getenv("AWS_SECRET_KEY"), os.Getenv("AWS_SESSION_TOKEN")
		if ak != "" && sk != "" {
			opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, tok)))
		}
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	c := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if t.Endpoint != "" {
			o.BaseEndpoint = aws.String(t.Endpoint)
		}
		o.UsePathStyle = t.PathStyle
	})
	return &Store{c: c, Bucket: t.Bucket}, nil
}

// List returns up to max keys under prefix, always from the start of the prefix.
// The purge loop relies on this: after a successful delete the same call shows what is left.
func (s *Store) List(ctx context.Context, prefix string, max int32) ([]Object, error) {
	out, err := s.c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.Bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(max),
	})
	if err != nil {
		return nil, err
	}
	return toObjects(out.Contents), nil
}

// ListAll pages through the whole prefix with continuation tokens. Read-only paths
// (dry-run, verify) use this; the delete loop must not.
func (s *Store) ListAll(ctx context.Context, prefix string, fn func([]Object) error) error {
	var token *string
	for {
		out, err := s.c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(s.Bucket), Prefix: aws.String(prefix),
			MaxKeys: aws.Int32(1000), ContinuationToken: token,
		})
		if err != nil {
			return err
		}
		if err := fn(toObjects(out.Contents)); err != nil {
			return err
		}
		if out.IsTruncated == nil || !*out.IsTruncated || out.NextContinuationToken == nil {
			return nil
		}
		token = out.NextContinuationToken
	}
}

// Empty reports whether nothing is left under prefix.
func (s *Store) Empty(ctx context.Context, prefix string) (bool, error) {
	objs, err := s.List(ctx, prefix, 1)
	return len(objs) == 0, err
}

// EnsureVersioningDisabled refuses to run against a versioned bucket. This tool
// only ever deletes by key, which on a versioned bucket does not do what we need:
// with versioning Enabled a plain delete just writes a delete marker and the data
// survives as a noncurrent version, still billed, while every listing here reports
// the prefix as empty — the tool would clear metadata and call the object done.
// With versioning Suspended the current ("null") version IS removed, so space is
// freed, but any version created while it was Enabled is left behind, and each
// delete leaves a zero-byte marker this tool cannot see.
//
// allowVersioned is the operator's explicit escape hatch (--allow-versioned-bucket)
// for environments where that is acceptable — e.g. a throwaway test bucket, or a
// bucket already confirmed to hold no historical versions. It is never safe by
// default: an unknown versioning status is still fatal, because then we cannot even
// tell which of the above applies.
func (s *Store) EnsureVersioningDisabled(ctx context.Context, allowVersioned bool) error {
	out, err := s.c.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(s.Bucket)})
	if err != nil {
		return fmt.Errorf("cannot confirm versioning status of bucket %s: %w", s.Bucket, err)
	}
	if out.Status == "" {
		return nil
	}
	if !allowVersioned {
		return fmt.Errorf("bucket %s has versioning %q; this tool deletes by key only and would leave versioned data behind. "+
			"Pass --allow-versioned-bucket to proceed anyway (only for buckets where that is acceptable)", s.Bucket, out.Status)
	}
	log.Printf("WARNING: bucket %s has versioning %q and --allow-versioned-bucket was given. "+
		"Deleting by key only: historical versions (if any) are NOT removed, each delete leaves a "+
		"zero-byte delete marker, and verify cannot see either — a clean verify does not prove the "+
		"prefix is truly empty. Do not use this against production without checking for historical versions.",
		s.Bucket, out.Status)
	return nil
}

// Delete removes one batch (<=1000 keys). It returns what S3 confirmed deleted and
// what it refused; a non-nil error means the request itself failed.
func (s *Store) Delete(ctx context.Context, keys []string) (deleted []string, failed []KeyError, err error) {
	if len(keys) == 0 {
		return nil, nil, nil
	}
	if len(keys) > 1000 {
		return nil, nil, errors.New("batch larger than 1000 keys")
	}
	ids := make([]types.ObjectIdentifier, 0, len(keys))
	for _, k := range keys {
		ids = append(ids, types.ObjectIdentifier{Key: aws.String(k)})
	}
	out, err := s.c.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(s.Bucket),
		Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(false)},
	})
	if err != nil {
		return nil, nil, err
	}
	for _, d := range out.Deleted {
		deleted = append(deleted, aws.ToString(d.Key))
	}
	for _, e := range out.Errors {
		failed = append(failed, KeyError{Key: aws.ToString(e.Key), Code: aws.ToString(e.Code), Message: aws.ToString(e.Message)})
	}
	return deleted, failed, nil
}

func toObjects(cs []types.Object) []Object {
	objs := make([]Object, 0, len(cs))
	for _, c := range cs {
		objs = append(objs, Object{Key: aws.ToString(c.Key), Size: aws.ToInt64(c.Size)})
	}
	return objs
}
