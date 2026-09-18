// Package chain wraps the official greenfield-go-sdk for the two read-only
// lookups the tool needs, instead of hand-rolling REST calls: the SDK talks the
// proper chain query protocol and returns typed ObjectInfo / BucketInfo.
package chain

import (
	"context"
	"fmt"
	"strings"

	gnfdclient "github.com/bnb-chain/greenfield-go-sdk/client"
)

type Client struct {
	c gnfdclient.IClient
}

// New connects to the chain's CometBFT RPC endpoint. The go-sdk builds a
// tendermint/CometBFT HTTP RPC client under the hood, so rpcEndpoint must be a
// full RPC URL with scheme, e.g. "https://greenfield-chain.bnbchain.org:443"
// (mainnet) or "http://localhost:26657" (local) — NOT a bare host:port (that
// parses to an empty host) and NOT the gRPC port. chainID is e.g.
// "greenfield_1017-1" (mainnet) or "greenfield_9000-121" (local). No account is
// needed for read-only queries.
func New(chainID, rpcEndpoint string) (*Client, error) {
	if !strings.Contains(rpcEndpoint, "://") {
		rpcEndpoint = "https://" + rpcEndpoint // avoid the silent empty-host misparse
	}
	c, err := gnfdclient.New(chainID, rpcEndpoint, gnfdclient.Option{})
	if err != nil {
		return nil, fmt.Errorf("dial chain %s @ %s: %w", chainID, rpcEndpoint, err)
	}
	return &Client{c: c}, nil
}

type ObjectHead struct {
	ID         uint64
	BucketName string
	Status     string // e.g. SEALED / CREATED (OBJECT_STATUS_ prefix stripped)
	Version    int64
}

// HeadObjectByID is the pre-delete check: the object must belong to the target bucket.
func (c *Client) HeadObjectByID(ctx context.Context, oid uint64) (*ObjectHead, error) {
	d, err := c.c.HeadObjectByID(ctx, fmt.Sprintf("%d", oid))
	if err != nil {
		return nil, err
	}
	oi := d.ObjectInfo
	if oi == nil {
		return nil, fmt.Errorf("head_object_by_id %d: empty object info", oid)
	}
	return &ObjectHead{
		ID:         oi.Id.Uint64(),
		BucketName: oi.BucketName,
		Status:     strings.TrimPrefix(oi.ObjectStatus.String(), "OBJECT_STATUS_"),
		Version:    oi.Version,
	}, nil
}

// HeadBucketID resolves a bucket name to its numeric id (checked once at start-up).
func (c *Client) HeadBucketID(ctx context.Context, name string) (uint64, error) {
	b, err := c.c.HeadBucket(ctx, name)
	if err != nil {
		return 0, err
	}
	return b.Id.Uint64(), nil
}
