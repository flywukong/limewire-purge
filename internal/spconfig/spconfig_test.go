package spconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseBucketURL(t *testing.T) {
	cases := []struct {
		in                 string
		bucket, region, ep string
		pathStyle          bool
	}{
		// SP 1 mainnet, path style on AWS
		{"https://s3.us-east-1.amazonaws.com/tf-nodereal-prod-greenfield-mainnet-sp-bnbchain", "tf-nodereal-prod-greenfield-mainnet-sp-bnbchain", "us-east-1", "", true},
		// trailing slash and an extra path segment are ignored, like the SP does
		{"https://s3.us-east-1.amazonaws.com/some-bucket/ignored/", "some-bucket", "us-east-1", "", true},
		// virtual hosted
		{"https://my-bucket.s3.ap-northeast-1.amazonaws.com", "my-bucket", "ap-northeast-1", "", false},
		// MinIO / any S3-compatible endpoint
		{"http://127.0.0.1:9000/fake-sp-bucket", "fake-sp-bucket", "us-east-1", "http://127.0.0.1:9000", true},
	}
	for _, c := range cases {
		got, err := ParseBucketURL(c.in)
		if err != nil {
			t.Fatalf("%s: %v", c.in, err)
		}
		if got.Bucket != c.bucket || got.Region != c.region || got.Endpoint != c.ep || got.PathStyle != c.pathStyle {
			t.Errorf("%s: got %+v", c.in, got)
		}
	}
	if _, err := ParseBucketURL("https://s3.us-east-1.amazonaws.com"); err == nil {
		t.Error("expected error for URL without bucket")
	}
}

func TestLoadAndEnvOverride(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "sp.toml")
	os.WriteFile(p, []byte(`
[SpDB]
User = "sp"
Passwd = "x"
Address = "127.0.0.1:3306"
Database = "spdb"
[BsDB]
User = "bs"
Passwd = "y"
Address = "127.0.0.1:3306"
Database = "bsdb"
[PieceStore]
Shards = 0
[PieceStore.Store]
Storage = "s3"
BucketURL = "https://s3.us-east-1.amazonaws.com/b"
IAMType = "SA"
`), 0o600)
	t.Setenv("BS_DB_PASSWORD", "from-env")
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.BsDB.Passwd != "from-env" || c.SpDB.Passwd != "x" {
		t.Errorf("env override wrong: %+v", c)
	}
	if c.PieceStore.Store.IAMType != "SA" {
		t.Errorf("IAMType: %+v", c.PieceStore.Store)
	}
	if c.SpDB.DSN() != "sp:x@tcp(127.0.0.1:3306)/spdb?parseTime=true&charset=utf8mb4&interpolateParams=true" {
		t.Errorf("dsn: %s", c.SpDB.DSN())
	}
}

func TestLoadAcceptsMinio(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "sp.toml")
	os.WriteFile(p, []byte("[PieceStore.Store]\nStorage = \"minio\"\nBucketURL = \"http://127.0.0.1:9000/sp0\"\nIAMType = \"AKSK\"\n"), 0o600)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	tgt, err := ParseBucketURL(c.PieceStore.Store.BucketURL)
	if err != nil || tgt.Bucket != "sp0" || tgt.Endpoint != "http://127.0.0.1:9000" || !tgt.PathStyle {
		t.Fatalf("minio target: %+v %v", tgt, err)
	}
}

func TestLoadRejectsSharded(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "sp.toml")
	os.WriteFile(p, []byte("[PieceStore]\nShards = 3\n"), 0o600)
	if _, err := Load(p); err == nil {
		t.Fatal("expected error for Shards > 1")
	}
}
