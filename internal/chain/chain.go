// Package chain makes the tool's three read-only chain lookups through the
// official Greenfield chain client (greenfield/sdk/client), which speaks the
// proper query protocol and returns typed ObjectInfo / BucketInfo.
//
// The query path drops the caller's context: cosmos-sdk's client.Context runs
// every ABCI query with context.Background(). A context deadline therefore cannot
// stop a stalled request, so the timeout lives in the RPC client's http.Client,
// which aborts the request itself. Package deadline adds an outer bound and a
// limited retries for timeouts and transient network/HTTP errors.
package chain

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	gnfdsdk "github.com/bnb-chain/greenfield/sdk/client"
	storagetypes "github.com/bnb-chain/greenfield/x/storage/types"
	rpcclient "github.com/cometbft/cometbft/rpc/jsonrpc/client"

	"limewire-purge/internal/deadline"
)

// attempts is the maximum total tries for a transient chain query failure.
const attempts = 3

type Client struct {
	c       *gnfdsdk.GreenfieldClient
	timeout time.Duration
}

// New builds a client for the chain's CometBFT RPC endpoint. rpcEndpoint must be
// a full RPC URL with scheme, e.g. "https://greenfield-chain.bnbchain.org:443"
// (mainnet) or "http://localhost:26657" (local) — NOT a bare host:port (that
// parses to an empty host) and NOT the gRPC port. chainID is e.g.
// "greenfield_1017-1" (mainnet) or "greenfield_9000-121" (local). No account is
// needed for read-only queries, and building the client makes no network call.
// timeout bounds every HTTP request the client sends.
func New(chainID, rpcEndpoint string, timeout time.Duration) (*Client, error) {
	if !strings.Contains(rpcEndpoint, "://") {
		rpcEndpoint = "https://" + rpcEndpoint // avoid the silent empty-host misparse
	}
	dialer := func(addr string) (*http.Client, error) {
		hc, err := rpcclient.DefaultHTTPClient(addr)
		if err != nil {
			return nil, err
		}
		hc.Timeout = timeout
		hc.Transport = transientStatusTransport{base: hc.Transport}
		return hc, nil
	}
	c, err := gnfdsdk.NewCustomGreenfieldClient(rpcEndpoint, chainID, dialer)
	if err != nil {
		return nil, fmt.Errorf("chain client %s @ %s: %w", chainID, rpcEndpoint, err)
	}
	return &Client{c: c, timeout: timeout}, nil
}

// call retries transient network/HTTP failures. The outer deadline is a little
// longer than the HTTP timeout so the request is normally aborted by the HTTP
// client, not merely abandoned.
func call[T any](ctx context.Context, c *Client, what string, fn func(context.Context) (T, error)) (T, error) {
	return deadline.RetryIf(ctx, c.timeout+5*time.Second, attempts, what, isTransient, func(ctx context.Context) (T, error) {
		v, err := fn(ctx)
		if err != nil && isTimeout(err) {
			return v, fmt.Errorf("%w: %v", deadline.ErrTimeout, err)
		}
		return v, err
	})
}

// Preserve retryable HTTP status even when the SDK flattens transport errors
// into gRPC status text (or tries to decode an HTML error page as JSON).
type transientStatusTransport struct{ base http.RoundTripper }

func (t transientStatusTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	if resp.StatusCode == 429 || (resp.StatusCode >= 500 && resp.StatusCode <= 599) {
		resp.Body.Close()
		return nil, fmt.Errorf("temporary RPC HTTP status %d", resp.StatusCode)
	}
	return resp, nil
}

func isTransient(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, deadline.ErrTimeout) || isTimeout(err) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && (ne.Timeout() || ne.Temporary()) {
		return true
	}
	// ABCI -> gRPC wrapping can discard the original error type.
	s := strings.ToLower(err.Error())
	for _, marker := range []string{"temporary rpc http status ", "connection refused", "connection reset by peer", "broken pipe", "unexpected eof", "i/o timeout", "temporary failure in name resolution"} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return strings.HasSuffix(s, ": eof")
}

// isTimeout recognises the http.Client timeout once the query stack has wrapped
// it (the gRPC-over-ABCI layer flattens it into a status error string).
func isTimeout(err error) bool {
	s := err.Error()
	return strings.Contains(s, "Client.Timeout exceeded") || strings.Contains(s, "context deadline exceeded")
}

type ObjectHead struct {
	ID          uint64
	BucketName  string
	Status      string // e.g. SEALED / CREATED (OBJECT_STATUS_ prefix stripped)
	Version     int64
	PayloadSize uint64
}

// HeadObjectByID is the pre-delete check: the object must belong to the target bucket.
func (c *Client) HeadObjectByID(ctx context.Context, oid uint64) (*ObjectHead, error) {
	return call(ctx, c, fmt.Sprintf("chain head_object_by_id %d", oid), func(ctx context.Context) (*ObjectHead, error) {
		resp, err := c.c.StorageQueryClient.HeadObjectById(ctx, &storagetypes.QueryHeadObjectByIdRequest{ObjectId: fmt.Sprintf("%d", oid)})
		if err != nil {
			return nil, err
		}
		oi := resp.ObjectInfo
		if oi == nil {
			return nil, fmt.Errorf("head_object_by_id %d: empty object info", oid)
		}
		return &ObjectHead{
			ID:          oi.Id.Uint64(),
			BucketName:  oi.BucketName,
			Status:      strings.TrimPrefix(oi.ObjectStatus.String(), "OBJECT_STATUS_"),
			Version:     oi.Version,
			PayloadSize: oi.PayloadSize,
		}, nil
	})
}

type redundancy struct {
	dataChunks  uint32
	segmentSize uint64
}

// RedundancyParams returns the chain's current EC data-chunk count and max segment size.
func (c *Client) RedundancyParams(ctx context.Context) (dataChunks uint32, segmentSize uint64, err error) {
	r, err := call(ctx, c, "chain storage params", func(ctx context.Context) (redundancy, error) {
		resp, err := c.c.StorageQueryClient.Params(ctx, &storagetypes.QueryParamsRequest{})
		if err != nil {
			return redundancy{}, err
		}
		vp := resp.Params.VersionedParams
		return redundancy{vp.GetRedundantDataChunkNum(), vp.GetMaxSegmentSize()}, nil
	})
	return r.dataChunks, r.segmentSize, err
}

// HeadBucketID resolves a bucket name to its numeric id (checked once at start-up).
func (c *Client) HeadBucketID(ctx context.Context, name string) (uint64, error) {
	return call(ctx, c, "chain head_bucket "+name, func(ctx context.Context) (uint64, error) {
		resp, err := c.c.StorageQueryClient.HeadBucket(ctx, &storagetypes.QueryHeadBucketRequest{BucketName: name})
		if err != nil {
			return 0, err
		}
		if resp.BucketInfo == nil {
			return 0, fmt.Errorf("head_bucket %s: empty bucket info", name)
		}
		return resp.BucketInfo.Id.Uint64(), nil
	})
}
