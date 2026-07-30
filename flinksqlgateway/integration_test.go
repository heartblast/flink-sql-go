//go:build integration

package flinksqlgateway_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/heartblast/flink-sql-go/flinksqlgateway"
)

func TestGatewayCompatibility(t *testing.T) {
	baseURL := strings.TrimSpace(os.Getenv("FLINK_SQL_GATEWAY_URL"))
	if baseURL == "" {
		t.Skip("FLINK_SQL_GATEWAY_URL is not set")
	}
	expectedVersion := requireIntegrationEnv(t, "FLINK_TEST_VERSION")
	expectedReleaseLine := requireIntegrationEnv(t, "FLINK_TEST_RELEASE_LINE")
	expectedAPIVersion := strings.TrimSpace(os.Getenv("FLINK_TEST_API_VERSION"))
	if expectedAPIVersion == "" {
		expectedAPIVersion = strings.TrimSpace(os.Getenv("FLINK_SQL_GATEWAY_API_VERSION"))
		if expectedAPIVersion != "" {
			t.Log("FLINK_SQL_GATEWAY_API_VERSION is deprecated; use FLINK_TEST_API_VERSION")
		}
	}
	if expectedAPIVersion == "" {
		t.Fatal("FLINK_TEST_API_VERSION must be set when FLINK_SQL_GATEWAY_URL is set")
	}
	expectedAPIVersion = normalizeIntegrationAPIVersion(expectedAPIVersion)

	client, err := flinksqlgateway.NewClient(flinksqlgateway.Config{
		BaseURL:           baseURL,
		CompatibilityMode: flinksqlgateway.CompatibilityAuto,
		APIVersionPolicy:  flinksqlgateway.APIVersionExplicit,
		APIVersion:        expectedAPIVersion,
		RequestTimeout:    10 * time.Second,
		ExecutionTimeout:  30 * time.Second,
		MaxResultRows:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("client Close() error = %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	info, err := client.GetInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Version != expectedVersion {
		t.Fatalf("gateway version = %q, want %q", info.Version, expectedVersion)
	}
	compatibility, err := client.GetCompatibilityInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if compatibility.FlinkVersion != expectedVersion {
		t.Fatalf("detected Flink version = %q, want %q", compatibility.FlinkVersion, expectedVersion)
	}
	if compatibility.ReleaseLine != flinksqlgateway.ReleaseLine(expectedReleaseLine) {
		t.Fatalf("detected release line = %q, want %q", compatibility.ReleaseLine, expectedReleaseLine)
	}
	if compatibility.APIVersion != expectedAPIVersion {
		t.Fatalf("selected API version = %q, want %q", compatibility.APIVersion, expectedAPIVersion)
	}

	session, err := client.OpenSession(ctx, flinksqlgateway.OpenSessionRequest{SessionName: "go-integration-test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := client.CloseSession(cleanupCtx, session.Handle); err != nil {
			t.Errorf("CloseSession() cleanup error = %v", err)
		}
	})

	if compatibility.Capabilities.ConfigureSession {
		setup, err := client.ApplySessionSetup(ctx, session.Handle, flinksqlgateway.SessionSetupPlan{
			Tables: []flinksqlgateway.TableSetup{{
				Target: flinksqlgateway.Identifier{
					Catalog:  "default_catalog",
					Database: "default_database",
					Object:   "flink_sql_go_setup_test",
				},
				Statement:   "(id BIGINT) WITH ('connector' = 'datagen', 'number-of-rows' = '1')",
				IfNotExists: true,
				Verify:      true,
			}},
			CurrentCatalog:  "default_catalog",
			CurrentDatabase: "default_database",
		}, flinksqlgateway.SessionSetupOptions{VerifyMetadata: true, VerifyTableSchema: true})
		if err != nil {
			t.Fatal(err)
		}
		if !setup.Complete || len(setup.Steps) == 0 || !setup.Steps[0].Verified {
			t.Fatalf("session setup = %+v", setup)
		}
	}

	result, err := client.ExecuteAndWait(ctx, session.Handle, "SELECT 1", flinksqlgateway.ExecuteOptions{MaxRows: 10})
	if err != nil {
		t.Fatal(err)
	}
	if result.RowsReceived == 0 {
		t.Fatal("SELECT 1 returned no rows")
	}
}

func requireIntegrationEnv(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s must be set when FLINK_SQL_GATEWAY_URL is set", name)
	}
	return value
}

func normalizeIntegrationAPIVersion(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if strings.HasPrefix(value, "v") {
		return value
	}
	return "v" + value
}
