// Package chain does the two read-only REST lookups the tool needs.
package chain

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	base string
	http *http.Client
}

func New(baseURL string) *Client {
	return &Client{base: strings.TrimRight(baseURL, "/"), http: &http.Client{Timeout: 30 * time.Second}}
}

type ObjectHead struct {
	ID         uint64
	BucketName string
	Status     string
	Version    int64
}

// HeadObjectByID is the pre-delete check: the object must belong to the target bucket.
func (c *Client) HeadObjectByID(ctx context.Context, oid uint64) (*ObjectHead, error) {
	var resp struct {
		ObjectInfo struct {
			ID         string `json:"id"`
			BucketName string `json:"bucket_name"`
			Status     string `json:"object_status"`
			Version    string `json:"version"`
		} `json:"object_info"`
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := c.get(ctx, fmt.Sprintf("/greenfield/storage/head_object_by_id/%d", oid), &resp); err != nil {
		return nil, err
	}
	if resp.ObjectInfo.ID == "" {
		return nil, fmt.Errorf("head_object_by_id %d: %s", oid, firstNonEmpty(resp.Message, "not found"))
	}
	id, _ := strconv.ParseUint(resp.ObjectInfo.ID, 10, 64)
	ver, _ := strconv.ParseInt(resp.ObjectInfo.Version, 10, 64)
	return &ObjectHead{ID: id, BucketName: resp.ObjectInfo.BucketName, Status: resp.ObjectInfo.Status, Version: ver}, nil
}

// HeadBucketID resolves a bucket name to its numeric id (checked once at start-up).
func (c *Client) HeadBucketID(ctx context.Context, name string) (uint64, error) {
	var resp struct {
		BucketInfo struct {
			ID string `json:"id"`
		} `json:"bucket_info"`
		Message string `json:"message"`
	}
	if err := c.get(ctx, "/greenfield/storage/head_bucket/"+name, &resp); err != nil {
		return 0, err
	}
	if resp.BucketInfo.ID == "" {
		return 0, fmt.Errorf("head_bucket %s: %s", name, firstNonEmpty(resp.Message, "not found"))
	}
	return strconv.ParseUint(resp.BucketInfo.ID, 10, 64)
}

func (c *Client) get(ctx context.Context, path string, v any) error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
		if err != nil {
			return err
		}
		res, err := c.http.Do(req)
		if err == nil {
			err = json.NewDecoder(res.Body).Decode(v)
			res.Body.Close()
			if err == nil && res.StatusCode < 500 {
				return nil
			}
			if err == nil {
				err = fmt.Errorf("http %d", res.StatusCode)
			}
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(1<<attempt) * 500 * time.Millisecond):
		}
	}
	return fmt.Errorf("GET %s: %w", path, lastErr)
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
