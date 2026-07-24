//go:build integration

package flinksqlgateway_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/heartblast/flink-sql-go/flinksqlgateway"
)

func TestFlink1204Gateway(t *testing.T) {
	baseURL := os.Getenv("FLINK_SQL_GATEWAY_URL")
	if baseURL == "" {
		t.Skip("FLINK_SQL_GATEWAY_URL is not set")
	}
	version := os.Getenv("FLINK_SQL_GATEWAY_API_VERSION")
	client, err := flinksqlgateway.NewClient(flinksqlgateway.Config{
		BaseURL:          baseURL,
		APIVersion:       version,
		RequestTimeout:   10 * time.Second,
		ExecutionTimeout: 30 * time.Second,
		MaxResultRows:    100,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	info, err := client.GetInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Version != "1.20.4" {
		t.Fatalf("gateway version = %q, want 1.20.4", info.Version)
	}
	session, err := client.OpenSession(ctx, flinksqlgateway.OpenSessionRequest{SessionName: "go-integration-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseSession(context.Background(), session.Handle)
	result, err := client.ExecuteAndWait(ctx, session.Handle, "SELECT 1", flinksqlgateway.ExecuteOptions{MaxRows: 10})
	if err != nil {
		t.Fatal(err)
	}
	if result.RowsReceived == 0 {
		t.Fatal("SELECT 1 returned no rows")
	}
}
