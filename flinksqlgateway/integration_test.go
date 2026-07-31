//go:build integration

package flinksqlgateway_test

import (
	"context"
	"encoding/json"
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
	advertisedVersions, err := client.GetAPIVersions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if expectedReleaseLine == string(flinksqlgateway.Flink23) {
		for _, expected := range []string{"v1", "v2", "v3", "v4"} {
			if !containsIntegrationValue(advertisedVersions, expected) {
				t.Fatalf("Flink 2.3 advertised API versions = %v; missing %s", advertisedVersions, expected)
			}
		}
		assertIntegrationPolicy(t, baseURL, flinksqlgateway.APIVersionStable, "v3")
		assertIntegrationPolicy(t, baseURL, flinksqlgateway.APIVersionHighest, "v4")
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

	operation, err := client.ExecuteStatement(ctx, session.Handle, flinksqlgateway.ExecuteStatementRequest{
		Statement:        "SELECT 2",
		ExecutionTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	status, err := client.GetOperationStatus(ctx, session.Handle, operation.Handle)
	if err != nil || status == "" {
		t.Fatalf("GetOperationStatus() = %q, %v", status, err)
	}
	page := waitForIntegrationResult(t, ctx, client, session.Handle, operation.Handle, flinksqlgateway.RowFormatPlainText)
	if page.Results == nil || page.Results.RowFormat != flinksqlgateway.RowFormatPlainText {
		t.Fatalf("PLAIN_TEXT result = %+v", page)
	}
	if err := client.CloseOperation(ctx, session.Handle, operation.Handle); err != nil {
		t.Fatal(err)
	}

	if expectedAPIVersion == "v3" {
		identifier := strings.TrimSpace(os.Getenv("FLINK_TEST_MATERIALIZED_TABLE_IDENTIFIER"))
		if identifier == "" {
			t.Log("FLINK_TEST_MATERIALIZED_TABLE_IDENTIFIER is not set; skipping live Materialized Table refresh")
		} else {
			refreshOperation, err := client.RefreshMaterializedTable(ctx, session.Handle, identifier, flinksqlgateway.RefreshMaterializedTableRequest{
				Periodic:         strings.EqualFold(strings.TrimSpace(os.Getenv("FLINK_TEST_MATERIALIZED_TABLE_PERIODIC")), "true"),
				ScheduleTime:     strings.TrimSpace(os.Getenv("FLINK_TEST_MATERIALIZED_TABLE_SCHEDULE_TIME")),
				DynamicOptions:   optionalIntegrationMap(t, "FLINK_TEST_MATERIALIZED_TABLE_DYNAMIC_OPTIONS"),
				StaticPartitions: optionalIntegrationMap(t, "FLINK_TEST_MATERIALIZED_TABLE_STATIC_PARTITIONS"),
				ExecutionConfig:  optionalIntegrationMap(t, "FLINK_TEST_MATERIALIZED_TABLE_EXECUTION_CONFIG"),
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.GetOperationStatus(ctx, session.Handle, refreshOperation.Handle); err != nil {
				t.Fatal(err)
			}
			if err := client.CloseOperation(ctx, session.Handle, refreshOperation.Handle); err != nil {
				t.Fatal(err)
			}
		}
	}

	if expectedAPIVersion == "v4" {
		script := os.Getenv("FLINK_TEST_DEPLOY_SCRIPT")
		scriptURI := strings.TrimSpace(os.Getenv("FLINK_TEST_DEPLOY_SCRIPT_URI"))
		if script == "" && scriptURI == "" {
			t.Log("FLINK_TEST_DEPLOY_SCRIPT and FLINK_TEST_DEPLOY_SCRIPT_URI are not set; skipping live Script deployment")
		} else {
			deployment, err := client.DeployScript(ctx, session.Handle, flinksqlgateway.DeployScriptRequest{
				Script:          script,
				ScriptURI:       scriptURI,
				ExecutionConfig: optionalIntegrationMap(t, "FLINK_TEST_DEPLOY_SCRIPT_EXECUTION_CONFIG"),
			})
			if err != nil {
				t.Fatal(err)
			}
			if strings.TrimSpace(deployment.ClusterID) == "" {
				t.Fatal("DeployScript returned an empty clusterID")
			}
		}
	}
}

func assertIntegrationPolicy(t *testing.T, baseURL string, policy flinksqlgateway.APIVersionPolicy, expected string) {
	t.Helper()
	client, err := flinksqlgateway.NewClient(flinksqlgateway.Config{
		BaseURL:           baseURL,
		CompatibilityMode: flinksqlgateway.CompatibilityAuto,
		APIVersionPolicy:  policy,
		RequestTimeout:    10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Errorf("policy client Close() error = %v", err)
		}
	}()
	info, err := client.GetCompatibilityInfo(context.Background())
	if err != nil || info.ReleaseLine != flinksqlgateway.Flink23 || info.APIVersion != expected {
		t.Fatalf("policy %s compatibility = %+v, %v", policy, info, err)
	}
}

func waitForIntegrationResult(
	t *testing.T,
	ctx context.Context,
	client *flinksqlgateway.GatewayClient,
	sessionHandle string,
	operationHandle string,
	rowFormat flinksqlgateway.RowFormat,
) *flinksqlgateway.ResultPage {
	t.Helper()
	for poll := 0; poll < 100; poll++ {
		page, err := client.FetchResults(ctx, sessionHandle, operationHandle, 0, rowFormat)
		if err != nil {
			t.Fatal(err)
		}
		if page.ResultType != flinksqlgateway.ResultNotReady {
			return page
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatal("result did not become ready within 100 polls")
	return nil
}

func optionalIntegrationMap(t *testing.T, name string) map[string]string {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return nil
	}
	var result map[string]string
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		t.Fatalf("%s must be a JSON object of string values: %v", name, err)
	}
	return result
}

func containsIntegrationValue(values []string, expected string) bool {
	for _, value := range values {
		if strings.EqualFold(value, expected) {
			return true
		}
	}
	return false
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
