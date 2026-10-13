package s3store

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type versioningHTTPClient func(*http.Request) (*http.Response, error)

func (f versioningHTTPClient) Do(r *http.Request) (*http.Response, error) { return f(r) }

func TestVersioningOptIn(t *testing.T) {
	for _, status := range []string{"", "Enabled", "Suspended"} {
		for _, allow := range []bool{false, true} {
			name := status + "/default"
			if allow {
				name = status + "/opt-in"
			}
			t.Run(name, func(t *testing.T) {
				body := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`
				if status != "" {
					body += "<Status>" + status + "</Status>"
				}
				body += "</VersioningConfiguration>"
				client := s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, HTTPClient: versioningHTTPClient(func(r *http.Request) (*http.Response, error) {
					if r.Method != http.MethodGet || !r.URL.Query().Has("versioning") {
						t.Fatalf("unexpected request: %s %s", r.Method, r.URL)
					}
					return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/xml"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
				})})
				err := (&Store{c: client, Bucket: "test-bucket"}).EnsureVersioningDisabled(context.Background(), allow)
				wantError := status != "" && !allow
				if (err != nil) != wantError {
					t.Fatalf("status=%q allow=%v: %v", status, allow, err)
				}
			})
		}
	}
}

func TestVersioningOptInDoesNotBypassQueryFailure(t *testing.T) {
	client := s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, HTTPClient: versioningHTTPClient(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 403, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`<Error><Code>AccessDenied</Code></Error>`))}, nil
	})})
	for _, allow := range []bool{false, true} {
		err := (&Store{c: client, Bucket: "test-bucket"}).EnsureVersioningDisabled(context.Background(), allow)
		if err == nil || !strings.Contains(err.Error(), "cannot confirm versioning status") {
			t.Fatalf("allow=%v: %v", allow, err)
		}
	}
}

func TestCheckKeys(t *testing.T) {
	if err := CheckKeys(5, []string{"s5_s0", "s5_s1_v2", "e5_s0_p3", "e5_s0_p3_v2"}); err != nil {
		t.Fatalf("valid batch rejected: %v", err)
	}
	for _, bad := range [][]string{
		{"s5_s0", "s5_backup"}, // unknown layout under the right prefix
		{"s5_s0", "s55_s0"},    // another object
		{"s5_s0_v0"},           // not something the SP writes
	} {
		if err := CheckKeys(5, bad); !errors.Is(err, ErrUnsafeKeys) {
			t.Errorf("%v: want ErrUnsafeKeys, got %v", bad, err)
		}
	}
}

// Delete refuses an unsafe batch before touching the client (nil here: any
// request would panic).
func TestDeleteRefusesUnsafeBatchWithoutRequest(t *testing.T) {
	s := &Store{Bucket: "b"}
	_, _, err := s.Delete(context.Background(), 5, []string{"s5_s0", "s5_backup"})
	if !errors.Is(err, ErrUnsafeKeys) {
		t.Fatalf("want ErrUnsafeKeys, got %v", err)
	}
}
