// Package spconfig reads the few fields the tool needs from a Greenfield SP's TOML:
// [PieceStore.Store] (bucket URL, IAM type), [SpDB] and [BsDB] connection info.
// It mirrors the SP's own parsing rules so the tool talks to exactly the same bucket.
package spconfig

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
)

type SQLDB struct {
	User     string
	Passwd   string
	Address  string
	Database string
}

type ObjectStorage struct {
	Storage   string
	BucketURL string
	IAMType   string
}

type PieceStore struct {
	Shards int
	Store  ObjectStorage
}

type Config struct {
	SpDB       SQLDB
	BsDB       SQLDB
	PieceStore PieceStore
}

// Load parses the SP TOML and applies the same env overrides the SP daemon honours.
func Load(path string) (*Config, error) {
	var c Config
	if _, err := toml.DecodeFile(path, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	applyEnv(&c.SpDB, "SP_DB_USER", "SP_DB_PASSWORD", "SP_DB_ADDRESS", "SP_DB_DATABASE")
	applyEnv(&c.BsDB, "BS_DB_USER", "BS_DB_PASSWORD", "BS_DB_ADDRESS", "BS_DB_DATABASE")
	if c.PieceStore.Shards > 1 {
		return nil, fmt.Errorf("PieceStore.Shards=%d: sharded piece stores are not supported by this tool", c.PieceStore.Shards)
	}
	switch c.PieceStore.Store.Storage {
	case "", "s3", "minio": // minio is the SP's name for "S3 API at a custom endpoint"; same wire protocol
	default:
		return nil, fmt.Errorf("PieceStore.Store.Storage=%q: only s3 / minio are supported", c.PieceStore.Store.Storage)
	}
	return &c, nil
}

func applyEnv(d *SQLDB, user, pass, addr, db string) {
	if v, ok := os.LookupEnv(user); ok {
		d.User = v
	}
	if v, ok := os.LookupEnv(pass); ok {
		d.Passwd = v
	}
	if v, ok := os.LookupEnv(addr); ok {
		d.Address = v
	}
	if v, ok := os.LookupEnv(db); ok {
		d.Database = v
	}
}

// DSN builds a go-sql-driver/mysql DSN.
func (d SQLDB) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&charset=utf8mb4&interpolateParams=true",
		d.User, d.Passwd, d.Address, d.Database)
}

// S3Target is what the SP's parseS3Endpoint derives from BucketURL.
type S3Target struct {
	Bucket    string
	Region    string
	Endpoint  string // empty = AWS default endpoint for Region
	PathStyle bool
	UseSSL    bool
}

// ParseBucketURL mirrors store/piecestore/storage/s3.go parseS3Endpoint + parseS3Region.
// Note: like the SP, any path segments after the bucket name are ignored.
func ParseBucketURL(raw string) (S3Target, error) {
	raw = strings.Trim(raw, "/")
	u, err := url.ParseRequestURI(raw)
	if err != nil {
		return S3Target{}, err
	}
	t := S3Target{UseSSL: strings.EqualFold(u.Scheme, "https")}
	isAWS := strings.Contains(u.Host, ".amazonaws.com")
	if u.Path != "" { // path style: https://s3.<region>.amazonaws.com/<bucket>
		parts := strings.Split(u.Path, "/")
		if len(parts) < 2 || parts[1] == "" {
			return S3Target{}, errors.New("bucket name missing in BucketURL path")
		}
		t.Bucket = parts[1]
		t.PathStyle = true
		if isAWS {
			t.Region = parseRegion(u.Host)
		} else {
			t.Endpoint = u.Scheme + "://" + u.Host
		}
	} else { // virtual-hosted: https://<bucket>.s3.<region>.amazonaws.com
		if !isAWS {
			return S3Target{}, fmt.Errorf("cannot derive bucket from %q", raw)
		}
		hp := strings.SplitN(u.Host, ".s3", 2)
		if len(hp) != 2 {
			return S3Target{}, fmt.Errorf("unexpected virtual-hosted URL %q", raw)
		}
		t.Bucket = hp[0]
		t.Region = parseRegion("s3" + hp[1])
	}
	if t.Region == "" && isAWS {
		t.Region = "us-east-1"
	}
	if t.Region == "" {
		t.Region = "us-east-1" // non-AWS endpoints need some region string for the SDK signer
	}
	return t, nil
}

func parseRegion(endpoint string) string {
	if strings.HasPrefix(endpoint, "s3-") || strings.HasPrefix(endpoint, "s3.") {
		endpoint = endpoint[3:]
	}
	endpoint = strings.TrimPrefix(endpoint, "dualstack.")
	if endpoint == "amazonaws.com" {
		return "us-east-1"
	}
	region := strings.Split(endpoint, ".")[0]
	if region == "external-1" {
		region = "us-east-1"
	}
	return region
}
