package flinksqlgateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFlink23CapabilityMatrixAndSelection(t *testing.T) {
	profile, err := profileForReleaseLine(Flink23)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		version      string
		configure    bool
		complete     bool
		rowFormat    bool
		materialized bool
		deploy       bool
		wireTimeout  bool
	}{
		{version: "v1", wireTimeout: true},
		{version: "v2", configure: true, complete: true, rowFormat: true, wireTimeout: true},
		{version: "v3", configure: true, complete: true, rowFormat: true, materialized: true, wireTimeout: true},
		{version: "v4", configure: true, complete: true, rowFormat: true, deploy: true, wireTimeout: true},
		{version: "v9"},
	}
	for _, test := range tests {
		t.Run(test.version, func(t *testing.T) {
			actual := capabilitiesForProfile(profile, test.version)
			if actual.ConfigureSession != test.configure || actual.CompleteStatement != test.complete ||
				actual.RowFormat != test.rowFormat || actual.MaterializedTable != test.materialized ||
				actual.DeployScript != test.deploy || actual.WireExecutionTimeout != test.wireTimeout {
				t.Fatalf("capabilitiesForProfile(%q) = %+v", test.version, actual)
			}
		})
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/info":
			writeTestJSON(t, w, map[string]string{"productName": "Apache Flink", "version": "2.3.0"})
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V1", "V2", "V3", "V4"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	for _, test := range []struct {
		name   string
		policy APIVersionPolicy
		want   string
	}{
		{name: "stable", policy: APIVersionStable, want: "v3"},
		{name: "highest", policy: APIVersionHighest, want: "v4"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newTestClient(t, Config{BaseURL: server.URL, CompatibilityMode: CompatibilityAuto, APIVersionPolicy: test.policy})
			info, err := client.GetCompatibilityInfo(context.Background())
			if err != nil || info.ReleaseLine != Flink23 || info.APIVersion != test.want {
				t.Fatalf("GetCompatibilityInfo() = %+v, %v", info, err)
			}
		})
	}
}

func TestFlink23V1SendsExecutionTimeout(t *testing.T) {
	var timeout any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V1"}})
		case "/v1/sessions/s/statements":
			var body map[string]any
			decodeTestJSON(t, r, &body)
			timeout = body["executionTimeout"]
			writeTestJSON(t, w, map[string]string{"operationHandle": "op"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL, CompatibilityMode: CompatibilityFlink23, APIVersionPolicy: APIVersionExplicit, APIVersion: "v1"})
	_, err := client.ExecuteStatement(context.Background(), "s", ExecuteStatementRequest{Statement: "SELECT 1", ExecutionTimeout: 1500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if timeout != float64(1500) {
		t.Fatalf("executionTimeout = %#v", timeout)
	}
}

func TestUnsupportedFlink23HelpersDoNotUseNetwork(t *testing.T) {
	var calls atomic.Int32
	httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("network must not be used")
	})}
	v4 := newTestClient(t, Config{
		BaseURL:           "https://flink.example",
		HTTPClient:        httpClient,
		CompatibilityMode: CompatibilityFlink23,
		APIVersionPolicy:  APIVersionExplicit,
		APIVersion:        "v4",
	})
	_, err := v4.RefreshMaterializedTable(context.Background(), "s", "catalog.db.table", RefreshMaterializedTableRequest{})
	if !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("RefreshMaterializedTable() error = %v", err)
	}

	v3 := newTestClient(t, Config{
		BaseURL:           "https://flink.example",
		HTTPClient:        httpClient,
		CompatibilityMode: CompatibilityFlink23,
		APIVersionPolicy:  APIVersionExplicit,
		APIVersion:        "v3",
	})
	_, err = v3.DeployScript(context.Background(), "s", DeployScriptRequest{Script: "SELECT 1;"})
	if !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("DeployScript() error = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("network calls = %d, want 0", calls.Load())
	}
}

func TestRefreshMaterializedTableWireAndOperationLifecycle(t *testing.T) {
	var paths []string
	var bodies []map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/refresh"):
			paths = append(paths, r.URL.EscapedPath())
			var body map[string]json.RawMessage
			decodeTestJSON(t, r, &body)
			bodies = append(bodies, body)
			writeTestJSON(t, w, map[string]string{"operationHandle": "refresh-op"})
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/operations/refresh-op/status"):
			writeTestJSON(t, w, map[string]string{"status": "RUNNING"})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/operations/refresh-op/cancel"):
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/operations/refresh-op/close"):
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newFlink23ExplicitClient(t, server.URL, "v3", nil)
	session := "session/한 글"
	identifier := "`catalog`.`db`.`table/name`"
	operation, err := client.RefreshMaterializedTable(context.Background(), session, identifier, RefreshMaterializedTableRequest{Periodic: true})
	if err != nil {
		t.Fatal(err)
	}
	if operation.Handle != "refresh-op" || operation.SessionHandle != session {
		t.Fatalf("operation = %+v", operation)
	}
	status, err := client.GetOperationStatus(context.Background(), operation.SessionHandle, operation.Handle)
	if err != nil || status != OperationRunning {
		t.Fatalf("GetOperationStatus() = %q, %v", status, err)
	}
	if err := client.CancelOperation(context.Background(), operation.SessionHandle, operation.Handle); err != nil {
		t.Fatal(err)
	}
	if err := client.CloseOperation(context.Background(), operation.SessionHandle, operation.Handle); err != nil {
		t.Fatal(err)
	}

	empty := map[string]string{}
	_, err = client.RefreshMaterializedTable(context.Background(), session, identifier, RefreshMaterializedTableRequest{
		ScheduleTime:     "2026-07-31T12:00:00Z",
		DynamicOptions:   empty,
		StaticPartitions: empty,
		ExecutionConfig:  empty,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 {
		t.Fatalf("refresh paths = %v", paths)
	}
	wantPath := "/v3/sessions/" + url.PathEscape(session) + "/materialized-tables/" + url.PathEscape(identifier) + "/refresh"
	if paths[0] != wantPath || strings.Contains(paths[0], "%252F") {
		t.Fatalf("refresh escaped path = %q, want %q", paths[0], wantPath)
	}
	if string(bodies[0]["isPeriodic"]) != "true" {
		t.Fatalf("first refresh body = %s", mustMarshalJSON(t, bodies[0]))
	}
	for _, field := range []string{"periodic", "scheduleTime", "dynamicOptions", "staticPartitions", "executionConfig"} {
		if _, exists := bodies[0][field]; exists {
			t.Errorf("nil request unexpectedly encoded %q: %s", field, mustMarshalJSON(t, bodies[0]))
		}
	}
	for _, field := range []string{"dynamicOptions", "staticPartitions", "executionConfig"} {
		if string(bodies[1][field]) != "{}" {
			t.Errorf("empty map field %q = %s", field, bodies[1][field])
		}
	}
	if _, exists := bodies[1]["periodic"]; exists {
		t.Fatalf("non-canonical periodic field was encoded: %s", mustMarshalJSON(t, bodies[1]))
	}
}

func TestRefreshMaterializedTableValidationAndInvalidHandle(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/api_versions" {
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
			return
		}
		writeTestJSON(t, w, map[string]string{"operationHandle": ""})
	}))
	defer server.Close()
	client := newFlink23ExplicitClient(t, server.URL, "v3", nil)
	for _, test := range []struct {
		session    string
		identifier string
	}{
		{session: "", identifier: "catalog.db.table"},
		{session: "s", identifier: ""},
		{session: "s", identifier: "bad\nidentifier"},
	} {
		if _, err := client.RefreshMaterializedTable(context.Background(), test.session, test.identifier, RefreshMaterializedTableRequest{}); err == nil {
			t.Fatalf("invalid refresh input succeeded: %+v", test)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("validation used network: calls=%d", calls.Load())
	}
	_, err := client.RefreshMaterializedTable(context.Background(), "s", "catalog.db.table", RefreshMaterializedTableRequest{})
	var unknown *MaterializedTableRefreshOutcomeUnknownError
	if !errors.Is(err, ErrMaterializedTableRefreshOutcomeUnknown) || !errors.As(err, &unknown) {
		t.Fatalf("invalid operationHandle error = %v", err)
	}
}

func TestRefreshMaterializedTableHTTPClassificationAndRedaction(t *testing.T) {
	for _, test := range []struct {
		status      int
		wantUnknown bool
	}{
		{status: http.StatusBadRequest},
		{status: http.StatusRequestTimeout, wantUnknown: true},
		{status: http.StatusTooManyRequests, wantUnknown: true},
		{status: http.StatusInternalServerError, wantUnknown: true},
	} {
		t.Run(fmt.Sprint(test.status), func(t *testing.T) {
			const secret = "refresh-secret-value"
			var refreshCalls atomic.Int32
			observer := &compatibilityObservationRecorder{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api_versions" {
					writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
					return
				}
				refreshCalls.Add(1)
				writeTestJSONStatus(t, w, test.status, map[string]string{"message": "rejected " + secret})
			}))
			defer server.Close()
			client := newFlink23ExplicitClient(t, server.URL, "v3", observer)
			_, err := client.RefreshMaterializedTable(context.Background(), "s", "catalog.db.table", RefreshMaterializedTableRequest{
				DynamicOptions:  map[string]string{"password": secret},
				ExecutionConfig: map[string]string{"token": secret},
			})
			if errors.Is(err, ErrMaterializedTableRefreshOutcomeUnknown) != test.wantUnknown {
				t.Fatalf("RefreshMaterializedTable() error = %v", err)
			}
			if refreshCalls.Load() != 1 {
				t.Fatalf("refresh calls = %d, want 1", refreshCalls.Load())
			}
			if strings.Contains(fmt.Sprint(err), secret) || strings.Contains(observer.String(), secret) {
				t.Fatalf("secret exposed: error=%v observer=%s", err, observer.String())
			}
		})
	}
}

func TestRefreshMaterializedTableTransportFailureIsOutcomeUnknown(t *testing.T) {
	var refreshCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api_versions" {
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
			return
		}
		refreshCalls.Add(1)
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("response writer does not support hijacking")
			return
		}
		connection, _, err := hijacker.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		_ = connection.Close()
	}))
	defer server.Close()
	client := newFlink23ExplicitClient(t, server.URL, "v3", nil)
	_, err := client.RefreshMaterializedTable(context.Background(), "s", "catalog.db.table", RefreshMaterializedTableRequest{})
	if !errors.Is(err, ErrMaterializedTableRefreshOutcomeUnknown) || refreshCalls.Load() != 1 {
		t.Fatalf("transport error = %v, calls=%d", err, refreshCalls.Load())
	}
}

func TestDeployScriptWireForms(t *testing.T) {
	var bodies []map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api_versions" {
			writeTestJSON(t, w, map[string]any{"versions": []string{"V4"}})
			return
		}
		if r.Method != http.MethodPost || r.URL.EscapedPath() != "/v4/sessions/session%2Fopaque/scripts" {
			http.NotFound(w, r)
			return
		}
		var body map[string]json.RawMessage
		decodeTestJSON(t, r, &body)
		bodies = append(bodies, body)
		writeTestJSON(t, w, map[string]string{"clusterID": fmt.Sprintf("opaque-cluster-%d", len(bodies))})
	}))
	defer server.Close()
	client := newFlink23ExplicitClient(t, server.URL, "v4", nil)
	inline := "SET 'pipeline.name' = 'inline';\nSELECT 1;"
	first, err := client.DeployScript(context.Background(), "session/opaque", DeployScriptRequest{Script: inline})
	if err != nil || first.ClusterID != "opaque-cluster-1" {
		t.Fatalf("DeployScript(inline) = %+v, %v", first, err)
	}
	uri := "s3://user:password@bucket/script.sql?token=secret"
	second, err := client.DeployScript(context.Background(), "session/opaque", DeployScriptRequest{
		ScriptURI:       uri,
		ExecutionConfig: map[string]string{"kubernetes.cluster-id": "application-cluster"},
	})
	if err != nil || second.ClusterID != "opaque-cluster-2" {
		t.Fatalf("DeployScript(uri) = %+v, %v", second, err)
	}
	if len(bodies) != 2 {
		t.Fatalf("deploy bodies = %d", len(bodies))
	}
	assertJSONString(t, bodies[0]["script"], inline)
	if string(bodies[0]["scriptUri"]) != "null" || string(bodies[0]["executionConfig"]) != "{}" {
		t.Fatalf("inline wire body = %s", mustMarshalJSON(t, bodies[0]))
	}
	assertJSONString(t, bodies[1]["scriptUri"], uri)
	if string(bodies[1]["script"]) != "null" {
		t.Fatalf("URI wire body = %s", mustMarshalJSON(t, bodies[1]))
	}
	var config map[string]string
	if err := json.Unmarshal(bodies[1]["executionConfig"], &config); err != nil || config["kubernetes.cluster-id"] != "application-cluster" {
		t.Fatalf("executionConfig = %v, %v", config, err)
	}
}

func TestDeployScriptValidationEmptyClusterAndRedaction(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/api_versions" {
			writeTestJSON(t, w, map[string]any{"versions": []string{"V4"}})
			return
		}
		writeTestJSON(t, w, map[string]string{"clusterID": ""})
	}))
	defer server.Close()
	client := newFlink23ExplicitClient(t, server.URL, "v4", nil)
	invalid := []DeployScriptRequest{
		{},
		{Script: "SELECT 1", ScriptURI: "file:///script.sql"},
		{Script: " \n\t "},
		{ScriptURI: "file:///bad\nuri"},
	}
	for _, req := range invalid {
		if _, err := client.DeployScript(context.Background(), "s", req); err == nil {
			t.Fatalf("invalid DeployScript request succeeded: %+v", req)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("validation used network: calls=%d", calls.Load())
	}
	_, err := client.DeployScript(context.Background(), "s", DeployScriptRequest{Script: "SELECT 1;"})
	var unknown *ScriptDeploymentOutcomeUnknownError
	if !errors.Is(err, ErrScriptDeploymentOutcomeUnknown) || !errors.As(err, &unknown) {
		t.Fatalf("empty clusterID error = %v", err)
	}

	const scriptSecret = "SELECT 'inline-secret-value';"
	const configSecret = "deployment-config-secret"
	const uriSecret = "s3://user:password@bucket/script.sql?token=uri-secret"
	observer := &compatibilityObservationRecorder{}
	var deployCalls atomic.Int32
	failureServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api_versions" {
			writeTestJSON(t, w, map[string]any{"versions": []string{"V4"}})
			return
		}
		deployCalls.Add(1)
		writeTestJSONStatus(t, w, http.StatusInternalServerError, map[string]string{"message": scriptSecret + " " + configSecret + " " + uriSecret})
	}))
	defer failureServer.Close()
	failureClient := newFlink23ExplicitClient(t, failureServer.URL, "v4", observer)
	_, err = failureClient.DeployScript(context.Background(), "s", DeployScriptRequest{
		ScriptURI:       uriSecret,
		ExecutionConfig: map[string]string{"password": configSecret},
	})
	if !errors.Is(err, ErrScriptDeploymentOutcomeUnknown) || deployCalls.Load() != 1 {
		t.Fatalf("deployment error = %v, calls=%d", err, deployCalls.Load())
	}
	combined := fmt.Sprint(err) + observer.String()
	for _, secret := range []string{scriptSecret, configSecret, uriSecret, "password@bucket", "token=uri-secret"} {
		if strings.Contains(combined, secret) {
			t.Fatalf("deployment secret exposed: %q in %q", secret, combined)
		}
	}
}

func TestFlink23SQLPassThrough(t *testing.T) {
	statements := []string{
		"SELECT * FROM TABLE(FROM_CHANGELOG(TABLE source_table));",
		"SELECT * FROM TABLE(TO_CHANGELOG(TABLE source_table));",
		"CREATE MATERIALIZED TABLE mt (id BIGINT PRIMARY KEY NOT ENFORCED, ts TIMESTAMP(3), WATERMARK FOR ts AS ts) FRESHNESS = INTERVAL '1' MINUTE AS SELECT id, ts FROM source_table;",
		"ALTER MATERIALIZED TABLE mt ADD (computed AS id + 1);",
		"ALTER MATERIALIZED TABLE mt AS SELECT * FROM source_table START_MODE = FROM_NOW;",
		"INSERT INTO target_table SELECT * FROM source_table ON CONFLICT DO NOTHING;",
		"INSERT INTO target_table SELECT * FROM source_table ON CONFLICT DO ERROR;",
		"INSERT INTO target_table\nSELECT * FROM source_table\nON CONFLICT DO DEDUPLICATE;",
		"CREATE FUNCTION my_func\nAS 'com.example.MyFunction'\nUSING ARTIFACT 's3://bucket/my-function.jar';",
		"SELECT * FROM TABLE(process_rows(TABLE source_table ORDER BY event_time));",
	}
	var received []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api_versions" {
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
			return
		}
		var body struct {
			Statement string `json:"statement"`
		}
		decodeTestJSON(t, r, &body)
		received = append(received, body.Statement)
		writeTestJSON(t, w, map[string]string{"operationHandle": fmt.Sprintf("op-%d", len(received))})
	}))
	defer server.Close()
	client := newFlink23ExplicitClient(t, server.URL, "v3", nil)
	for _, statement := range statements {
		if _, err := client.ExecuteStatement(context.Background(), "s", ExecuteStatementRequest{Statement: statement}); err != nil {
			t.Fatalf("ExecuteStatement() error = %v", err)
		}
	}
	if !reflect.DeepEqual(received, statements) {
		t.Fatalf("received SQL differs:\n got: %#v\nwant: %#v", received, statements)
	}
}

func newFlink23ExplicitClient(t *testing.T, baseURL, version string, observer Observer) *GatewayClient {
	t.Helper()
	return newTestClient(t, Config{
		BaseURL:           baseURL,
		CompatibilityMode: CompatibilityFlink23,
		APIVersionPolicy:  APIVersionExplicit,
		APIVersion:        version,
		Observer:          observer,
		PollInterval:      time.Millisecond,
	})
}

func mustMarshalJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func assertJSONString(t *testing.T, raw json.RawMessage, want string) {
	t.Helper()
	var got string
	if err := json.Unmarshal(raw, &got); err != nil || got != want {
		t.Fatalf("JSON string = %q, %v; want %q", got, err, want)
	}
}
