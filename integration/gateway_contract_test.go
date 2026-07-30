package integration

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/heartblast/flink-sql-go/flinksqlgateway"
)

// fixtureFS는 working directory와 무관하게 버전별 wire contract를 읽는다.
//
//go:embed fixtures/*/*.json
var fixtureFS embed.FS

type fixtureManifest struct {
	SchemaVersion       int    `json:"schemaVersion"`
	ReleaseLine         string `json:"releaseLine"`
	FlinkVersion        string `json:"flinkVersion"`
	APIVersion          string `json:"apiVersion"`
	Synthetic           bool   `json:"synthetic"`
	CountsAsTestedPatch bool   `json:"countsAsTestedPatch"`
	Provenance          string `json:"provenance"`
}

type contractCase struct {
	directory            string
	releaseLine          flinksqlgateway.ReleaseLine
	apiVersion           string
	wireExecutionTimeout bool
}

func TestGatewayFixtureContracts(t *testing.T) {
	t.Parallel()
	tests := []contractCase{
		{directory: "flink-1.20", releaseLine: flinksqlgateway.Flink120, apiVersion: "v3"},
		{directory: "flink-2.0", releaseLine: flinksqlgateway.Flink20, apiVersion: "v4", wireExecutionTimeout: true},
		{directory: "flink-2.1", releaseLine: flinksqlgateway.Flink21, apiVersion: "v4", wireExecutionTimeout: true},
	}

	for _, test := range tests {
		test := test
		t.Run(test.directory, func(t *testing.T) {
			t.Parallel()
			runGatewayFixtureContract(t, test)
		})
	}
}

func runGatewayFixtureContract(t *testing.T, test contractCase) {
	t.Helper()
	manifest := loadFixtureManifest(t, test.directory)
	if manifest.SchemaVersion != 1 || manifest.ReleaseLine != string(test.releaseLine) || manifest.APIVersion != test.apiVersion {
		t.Fatalf("fixture manifest = %+v", manifest)
	}
	if test.releaseLine != flinksqlgateway.Flink120 {
		if !manifest.Synthetic || manifest.CountsAsTestedPatch || !strings.Contains(manifest.Provenance, "Apache Flink OpenAPI schema") {
			t.Fatalf("2.x fixture provenance must remain synthetic and untested: %+v", manifest)
		}
	}

	harness := newContractHarness(t, test.directory, test.apiVersion, test.wireExecutionTimeout)
	server := httptest.NewServer(http.HandlerFunc(harness.serveHTTP))
	t.Cleanup(server.Close)

	client, err := flinksqlgateway.NewClient(flinksqlgateway.Config{
		BaseURL:           server.URL,
		CompatibilityMode: flinksqlgateway.CompatibilityAuto,
		APIVersionPolicy:  flinksqlgateway.APIVersionExplicit,
		APIVersion:        test.apiVersion,
		RequestTimeout:    2 * time.Second,
		ExecutionTimeout:  2 * time.Second,
		MaxResultRows:     32,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("client Close() error = %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	compatibility, err := client.GetCompatibilityInfo(ctx)
	if err != nil {
		t.Fatalf("GetCompatibilityInfo() error = %v", err)
	}
	if compatibility.FlinkVersion != manifest.FlinkVersion || compatibility.ReleaseLine != test.releaseLine || compatibility.APIVersion != test.apiVersion {
		t.Fatalf("compatibility = %+v", compatibility)
	}
	if compatibility.DetectionSource != flinksqlgateway.DetectionSourceAuto || compatibility.Capabilities.WireExecutionTimeout != test.wireExecutionTimeout {
		t.Fatalf("compatibility capabilities = %+v", compatibility)
	}

	session, err := client.OpenSession(ctx, flinksqlgateway.OpenSessionRequest{
		SessionName: "fixture-contract",
		Properties:  map[string]string{"execution.runtime-mode": "streaming"},
	})
	if err != nil || session.Handle != contractSessionHandle {
		t.Fatalf("OpenSession() = %+v, %v", session, err)
	}
	properties, err := client.GetSessionConfig(ctx, session.Handle)
	if err != nil || properties["pipeline.name"] != "fixture-contract" {
		t.Fatalf("GetSessionConfig() = %v, %v", properties, err)
	}
	if err := client.ConfigureSession(ctx, session.Handle, "USE CATALOG fixture_catalog", 1250*time.Millisecond); err != nil {
		t.Fatalf("ConfigureSession() error = %v", err)
	}

	operation, err := client.ExecuteStatement(ctx, session.Handle, flinksqlgateway.ExecuteStatementRequest{
		Statement:        "SELECT 42, 'flink', NULL",
		ExecutionTimeout: 1750 * time.Millisecond,
		ExecutionConfig:  map[string]string{"table.local-time-zone": "UTC"},
	})
	if err != nil || operation.Handle != contractOperationHandle {
		t.Fatalf("ExecuteStatement() = %+v, %v", operation, err)
	}
	status, err := client.GetOperationStatus(ctx, session.Handle, operation.Handle)
	if err != nil || status != flinksqlgateway.OperationRunning {
		t.Fatalf("GetOperationStatus() = %q, %v", status, err)
	}

	notReady, err := client.FetchResults(ctx, session.Handle, operation.Handle, 0, flinksqlgateway.RowFormatJSON)
	if err != nil || notReady.ResultType != flinksqlgateway.ResultNotReady {
		t.Fatalf("FetchResults(NOT_READY) = %+v, %v", notReady, err)
	}
	payload, err := client.FetchResults(ctx, session.Handle, operation.Handle, 0, flinksqlgateway.RowFormatJSON)
	if err != nil || payload.ResultType != flinksqlgateway.ResultPayload || payload.Results == nil || len(payload.Results.Data) != 2 {
		t.Fatalf("FetchResults(JSON PAYLOAD) = %+v, %v", payload, err)
	}
	if !strings.HasPrefix(payload.NextResultURI, "/"+test.apiVersion+"/") || !payload.QueryResult || payload.JobID == "" {
		t.Fatalf("JSON payload metadata = %+v", payload)
	}
	accessor := payload.Results.Data[0].WithColumns(payload.Results.Columns, nil)
	value, null, err := accessor.Int64("id")
	if err != nil || null || value != 42 {
		t.Fatalf("JSON fixture id = %d, null=%v, error=%v", value, null, err)
	}

	plainText, err := client.FetchResults(ctx, session.Handle, operation.Handle, 2, flinksqlgateway.RowFormatPlainText)
	if err != nil || plainText.ResultType != flinksqlgateway.ResultPayload || plainText.Results == nil || plainText.Results.RowFormat != flinksqlgateway.RowFormatPlainText {
		t.Fatalf("FetchResults(PLAIN_TEXT) = %+v, %v", plainText, err)
	}
	plainAccessor := plainText.Results.Data[0].WithColumns(plainText.Results.Columns, nil)
	plainValue, null, err := plainAccessor.Int64("id")
	if err != nil || null || plainValue != 42 {
		t.Fatalf("PLAIN_TEXT fixture id = %d, null=%v, error=%v", plainValue, null, err)
	}

	eos, err := client.FetchResults(ctx, session.Handle, operation.Handle, 1, flinksqlgateway.RowFormatJSON)
	if err != nil || eos.ResultType != flinksqlgateway.ResultEOS {
		t.Fatalf("FetchResults(EOS) = %+v, %v", eos, err)
	}
	if err := client.CancelOperation(ctx, session.Handle, operation.Handle); err != nil {
		t.Fatalf("CancelOperation() error = %v", err)
	}
	if err := client.CloseOperation(ctx, session.Handle, operation.Handle); err != nil {
		t.Fatalf("CloseOperation() error = %v", err)
	}
	if err := client.CloseSession(ctx, session.Handle); err != nil {
		t.Fatalf("CloseSession() error = %v", err)
	}

	harness.assertComplete(t)
}

const (
	contractSessionHandle   = "contract-session"
	contractOperationHandle = "contract-operation"
)

var requiredResponseFixtures = []string{
	"info.json",
	"api_versions.json",
	"open_session.json",
	"session_config.json",
	"configure_session.json",
	"execute_statement.json",
	"operation_status.json",
	"result_not_ready.json",
	"result_payload_json.json",
	"result_payload_plain_text.json",
	"result_eos.json",
	"cancel_operation.json",
	"close_operation.json",
	"close_session.json",
}

type contractHarness struct {
	directory            string
	apiVersion           string
	wireExecutionTimeout bool

	mu              sync.Mutex
	jsonResultCalls int
	served          map[string]int
	errors          []string
}

func newContractHarness(t *testing.T, directory, apiVersion string, wireExecutionTimeout bool) *contractHarness {
	t.Helper()
	for _, name := range append([]string{"manifest.json"}, requiredResponseFixtures...) {
		if _, err := fixtureFS.ReadFile("fixtures/" + directory + "/" + name); err != nil {
			t.Fatalf("read fixture %s/%s: %v", directory, name, err)
		}
	}
	return &contractHarness{
		directory:            directory,
		apiVersion:           apiVersion,
		wireExecutionTimeout: wireExecutionTimeout,
		served:               make(map[string]int),
	}
}

func (h *contractHarness) serveHTTP(w http.ResponseWriter, r *http.Request) {
	prefix := "/" + h.apiVersion
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/info":
		h.writeFixture(w, "info.json")
	case r.Method == http.MethodGet && r.URL.Path == "/api_versions":
		h.writeFixture(w, "api_versions.json")
	case r.Method == http.MethodPost && r.URL.Path == prefix+"/sessions":
		h.validateOpenSession(r)
		h.writeFixture(w, "open_session.json")
	case r.Method == http.MethodGet && r.URL.Path == prefix+"/sessions/"+contractSessionHandle:
		h.writeFixture(w, "session_config.json")
	case r.Method == http.MethodPost && r.URL.Path == prefix+"/sessions/"+contractSessionHandle+"/configure-session":
		h.validateTimedStatement(r, "USE CATALOG fixture_catalog", 1250)
		h.writeFixture(w, "configure_session.json")
	case r.Method == http.MethodPost && r.URL.Path == prefix+"/sessions/"+contractSessionHandle+"/statements":
		h.validateExecuteStatement(r)
		h.writeFixture(w, "execute_statement.json")
	case r.Method == http.MethodGet && r.URL.Path == prefix+"/sessions/"+contractSessionHandle+"/operations/"+contractOperationHandle+"/status":
		h.writeFixture(w, "operation_status.json")
	case r.Method == http.MethodGet && r.URL.Path == prefix+"/sessions/"+contractSessionHandle+"/operations/"+contractOperationHandle+"/result/0":
		h.validateRowFormat(r, "JSON")
		h.mu.Lock()
		h.jsonResultCalls++
		call := h.jsonResultCalls
		h.mu.Unlock()
		if call == 1 {
			h.writeFixture(w, "result_not_ready.json")
		} else {
			h.writeFixture(w, "result_payload_json.json")
		}
	case r.Method == http.MethodGet && r.URL.Path == prefix+"/sessions/"+contractSessionHandle+"/operations/"+contractOperationHandle+"/result/1":
		h.validateRowFormat(r, "JSON")
		h.writeFixture(w, "result_eos.json")
	case r.Method == http.MethodGet && r.URL.Path == prefix+"/sessions/"+contractSessionHandle+"/operations/"+contractOperationHandle+"/result/2":
		h.validateRowFormat(r, "PLAIN_TEXT")
		h.writeFixture(w, "result_payload_plain_text.json")
	case r.Method == http.MethodPost && r.URL.Path == prefix+"/sessions/"+contractSessionHandle+"/operations/"+contractOperationHandle+"/cancel":
		h.writeFixture(w, "cancel_operation.json")
	case r.Method == http.MethodDelete && r.URL.Path == prefix+"/sessions/"+contractSessionHandle+"/operations/"+contractOperationHandle+"/close":
		h.writeFixture(w, "close_operation.json")
	case r.Method == http.MethodDelete && r.URL.Path == prefix+"/sessions/"+contractSessionHandle:
		h.writeFixture(w, "close_session.json")
	default:
		h.recordError("unexpected request: %s %s", r.Method, r.URL.String())
		http.Error(w, "unexpected fixture request", http.StatusNotFound)
	}
}

func (h *contractHarness) validateOpenSession(r *http.Request) {
	var body struct {
		SessionName string            `json:"sessionName"`
		Properties  map[string]string `json:"properties"`
	}
	if err := decodeRequestJSON(r, &body); err != nil {
		h.recordError("decode open-session request: %v", err)
		return
	}
	if body.SessionName != "fixture-contract" || body.Properties["execution.runtime-mode"] != "streaming" {
		h.recordError("open-session body = %+v", body)
	}
}

func (h *contractHarness) validateTimedStatement(r *http.Request, statement string, milliseconds int64) {
	var body struct {
		Statement        string `json:"statement"`
		ExecutionTimeout *int64 `json:"executionTimeout"`
	}
	if err := decodeRequestJSON(r, &body); err != nil {
		h.recordError("decode timed statement request: %v", err)
		return
	}
	if body.Statement != statement {
		h.recordError("statement = %q, want %q", body.Statement, statement)
	}
	h.validateExecutionTimeout(body.ExecutionTimeout, milliseconds)
}

func (h *contractHarness) validateExecuteStatement(r *http.Request) {
	var body struct {
		Statement        string            `json:"statement"`
		ExecutionConfig  map[string]string `json:"executionConfig"`
		ExecutionTimeout *int64            `json:"executionTimeout"`
	}
	if err := decodeRequestJSON(r, &body); err != nil {
		h.recordError("decode execute-statement request: %v", err)
		return
	}
	if body.Statement != "SELECT 42, 'flink', NULL" || body.ExecutionConfig["table.local-time-zone"] != "UTC" {
		h.recordError("execute-statement body = %+v", body)
	}
	h.validateExecutionTimeout(body.ExecutionTimeout, 1750)
}

func (h *contractHarness) validateExecutionTimeout(actual *int64, expected int64) {
	if !h.wireExecutionTimeout {
		if actual != nil {
			h.recordError("executionTimeout = %d, want omitted", *actual)
		}
		return
	}
	if actual == nil || *actual != expected {
		h.recordError("executionTimeout = %v, want %d", actual, expected)
	}
}

func (h *contractHarness) validateRowFormat(r *http.Request, expected string) {
	if actual := r.URL.Query().Get("rowFormat"); actual != expected {
		h.recordError("rowFormat = %q, want %q", actual, expected)
	}
}

func (h *contractHarness) writeFixture(w http.ResponseWriter, name string) {
	data, err := fixtureFS.ReadFile("fixtures/" + h.directory + "/" + name)
	if err != nil {
		h.recordError("read response fixture %s: %v", name, err)
		http.Error(w, "missing fixture", http.StatusInternalServerError)
		return
	}
	h.mu.Lock()
	h.served[name]++
	h.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}

func (h *contractHarness) recordError(format string, arguments ...any) {
	h.mu.Lock()
	h.errors = append(h.errors, fmt.Sprintf(format, arguments...))
	h.mu.Unlock()
}

func (h *contractHarness) assertComplete(t *testing.T) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.errors) > 0 {
		t.Fatalf("fixture server errors: %s", strings.Join(h.errors, "; "))
	}
	for _, name := range requiredResponseFixtures {
		if h.served[name] == 0 {
			t.Errorf("fixture %s was not exercised", name)
		}
	}
}

func loadFixtureManifest(t *testing.T, directory string) fixtureManifest {
	t.Helper()
	data, err := fixtureFS.ReadFile("fixtures/" + directory + "/manifest.json")
	if err != nil {
		t.Fatalf("read fixture manifest: %v", err)
	}
	var manifest fixtureManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("decode fixture manifest: %v", err)
	}
	return manifest
}

func decodeRequestJSON(r *http.Request, destination any) error {
	defer r.Body.Close()
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(destination); err != nil {
		return err
	}
	return nil
}
