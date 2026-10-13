package spdb

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func TestMetadataRetryClassification(t *testing.T) {
	for _, code := range []uint16{1040, 1053, 1205, 1213, 2006, 2013} {
		if !transient(fmt.Errorf("wrapped: %w", &mysql.MySQLError{Number: code})) {
			t.Errorf("should retry %d", code)
		}
	}
	for _, err := range []error{&mysql.MySQLError{Number: 1045}, &mysql.MySQLError{Number: 1142}, &mysql.MySQLError{Number: 1146}, &mysql.MySQLError{Number: 1064}, context.Canceled} {
		if transient(err) {
			t.Errorf("must not retry %v", err)
		}
	}
}

func TestMetadataRetryRecoversAndIsBounded(t *testing.T) {
	for _, recover := range []bool{false, true} {
		t.Run(fmt.Sprint(recover), func(t *testing.T) {
			calls := 0
			err := Retry(context.Background(), "oid=7 metadata delete", func(ctx context.Context) error {
				calls++
				if _, ok := ctx.Deadline(); !ok {
					t.Error("missing attempt deadline")
				}
				if recover && calls == 3 {
					return nil
				}
				return &mysql.MySQLError{Number: 1213, Message: "Deadlock"}
			})
			if calls != 3 || (err == nil) != recover {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestMetadataPermanentErrorAndCancellationDoNotRetry(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), "test", func(context.Context) error { calls++; return &mysql.MySQLError{Number: 1142} })
	if err == nil || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls = 0
	err = Retry(ctx, "test", func(context.Context) error { calls++; cancel(); return &mysql.MySQLError{Number: 2013} })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}
