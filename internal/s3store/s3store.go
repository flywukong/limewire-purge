// Package s3store is the only place that talks to the object storage.
// Two calls: list by prefix, delete a batch of keys. Nothing else.
package s3store

import (
	"context"
	"errors"
	"fmt"
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

// Version is one entry from a versioned listing: either an object version or a
// delete marker. VersionID is "null" for data written while versioning was
// suspended (or before it was ever enabled) — that is a real, deletable id.
type Version struct {
	Key            string
	VersionID      string
	Size           int64
	IsDeleteMarker bool
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

// VersioningStatus returns "" (never enabled), "Enabled" or "Suspended".
// An error here is fatal to the caller: with an unknown status we cannot pick a
// safe deletion mode, since a plain key delete on a versioned bucket only writes
// a delete marker and leaves the data (and the billed space) behind while every
// unversioned listing reports the prefix as empty.
func (s *Store) VersioningStatus(ctx context.Context) (string, error) {
	out, err := s.c.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(s.Bucket)})
	if err != nil {
		return "", fmt.Errorf("cannot confirm versioning status of bucket %s: %w", s.Bucket, err)
	}
	return string(out.Status), nil
}

// ListVersions returns up to max entries under prefix, always from the start of
// the prefix, including delete markers. Used instead of List on a versioned
// bucket: the delete loop relies on a successful version delete making the entry
// disappear from the next listing.
func (s *Store) ListVersions(ctx context.Context, prefix string, max int32) ([]Version, error) {
	out, err := s.c.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(s.Bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(max),
	})
	if err != nil {
		return nil, err
	}
	return toVersions(out.Versions, out.DeleteMarkers), nil
}

// ListAllVersions pages through every version and delete marker under prefix.
// Read-only paths (dry-run, verify) use this; the delete loop must not.
func (s *Store) ListAllVersions(ctx context.Context, prefix string, fn func([]Version) error) error {
	var keyMarker, versionMarker *string
	for {
		out, err := s.c.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket: aws.String(s.Bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(1000),
			KeyMarker: keyMarker, VersionIdMarker: versionMarker,
		})
		if err != nil {
			return err
		}
		if err := fn(toVersions(out.Versions, out.DeleteMarkers)); err != nil {
			return err
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			return nil
		}
		keyMarker, versionMarker = out.NextKeyMarker, out.NextVersionIdMarker
	}
}

// EmptyVersions reports whether nothing at all is left under prefix — no object
// version and no delete marker. A prefix holding only delete markers counts as
// non-empty: the marker is a leftover entry this tool is expected to remove.
func (s *Store) EmptyVersions(ctx context.Context, prefix string) (bool, error) {
	vs, err := s.ListVersions(ctx, prefix, 1)
	return len(vs) == 0, err
}

// DeleteVersions permanently removes one batch (<=1000 entries) by Key+VersionId.
// Unlike a plain key delete this frees the bytes on a versioned bucket and adds
// no new delete marker; passing a delete marker's own id removes the marker.
func (s *Store) DeleteVersions(ctx context.Context, vs []Version) (deleted []Version, failed []KeyError, err error) {
	if len(vs) == 0 {
		return nil, nil, nil
	}
	if len(vs) > 1000 {
		return nil, nil, errors.New("batch larger than 1000 entries")
	}
	byKeyVer := make(map[string]Version, len(vs))
	ids := make([]types.ObjectIdentifier, 0, len(vs))
	for _, v := range vs {
		ids = append(ids, types.ObjectIdentifier{Key: aws.String(v.Key), VersionId: aws.String(v.VersionID)})
		byKeyVer[v.Key+"\x00"+v.VersionID] = v
	}
	out, err := s.c.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(s.Bucket),
		Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(false)},
	})
	if err != nil {
		return nil, nil, err
	}
	for _, d := range out.Deleted {
		k, ver := aws.ToString(d.Key), aws.ToString(d.VersionId)
		if v, ok := byKeyVer[k+"\x00"+ver]; ok {
			deleted = append(deleted, v)
		} else {
			deleted = append(deleted, Version{Key: k, VersionID: ver})
		}
	}
	for _, e := range out.Errors {
		failed = append(failed, KeyError{
			Key:     aws.ToString(e.Key) + "@" + aws.ToString(e.VersionId),
			Code:    aws.ToString(e.Code),
			Message: aws.ToString(e.Message),
		})
	}
	return deleted, failed, nil
}

func toVersions(vs []types.ObjectVersion, ms []types.DeleteMarkerEntry) []Version {
	out := make([]Version, 0, len(vs)+len(ms))
	for _, v := range vs {
		out = append(out, Version{
			Key: aws.ToString(v.Key), VersionID: aws.ToString(v.VersionId), Size: aws.ToInt64(v.Size),
		})
	}
	for _, m := range ms {
		out = append(out, Version{
			Key: aws.ToString(m.Key), VersionID: aws.ToString(m.VersionId), IsDeleteMarker: true,
		})
	}
	return out
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
