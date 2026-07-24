package flinksqlgateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientLifecycleAndActualFlinkResultShape(t *testing.T) {
	t.Parallel()
	var sessionCloseCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Test-Auth"); got != "secret" {
			t.Errorf("custom header = %q", got)
		}
		if got := r.Header.Get("User-Agent"); got != "test-agent" {
			t.Errorf("user agent = %q", got)
		}
		switch {
		case r.URL.Path == "/info" && r.Method == http.MethodGet:
			writeTestJSON(t, w, map[string]any{"productName": "Apache Flink", "version": "1.20.4"})
		case r.URL.Path == "/api_versions" && r.Method == http.MethodGet:
			writeTestJSON(t, w, map[string]any{"versions": []string{"V1", "V2", "V3"}})
		case r.URL.Path == "/v3/sessions" && r.Method == http.MethodPost:
			var body map[string]any
			decodeTestJSON(t, r, &body)
			if body["sessionName"] != "workspace-1" {
				t.Errorf("sessionName = %#v", body["sessionName"])
			}
			writeTestJSON(t, w, map[string]any{"sessionHandle": "session-1"})
		case r.URL.Path == "/v3/sessions/session-1" && r.Method == http.MethodGet:
			writeTestJSON(t, w, map[string]any{"properties": map[string]string{"execution.runtime-mode": "streaming"}})
		case r.URL.Path == "/v3/sessions/session-1/configure-session" && r.Method == http.MethodPost:
			var body struct {
				Statement        string `json:"statement"`
				ExecutionTimeout int64  `json:"executionTimeout"`
			}
			decodeTestJSON(t, r, &body)
			if body.Statement != "USE CATALOG cat" || body.ExecutionTimeout != 2_000 {
				t.Errorf("configure body = %+v", body)
			}
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v3/sessions/session-1/complete-statement" && r.Method == http.MethodGet:
			if r.URL.Query().Get("statement") != "SEL" || r.URL.Query().Get("position") != "3" {
				t.Errorf("completion query = %q", r.URL.RawQuery)
			}
			writeTestJSON(t, w, map[string]any{"candidates": []string{"SELECT"}})
		case r.URL.Path == "/v3/sessions/session-1/heartbeat" && r.Method == http.MethodPost:
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v3/sessions/session-1/statements" && r.Method == http.MethodPost:
			var body struct {
				Statement        string `json:"statement"`
				ExecutionTimeout int64  `json:"executionTimeout"`
			}
			decodeTestJSON(t, r, &body)
			if body.Statement != "SHOW TABLES" || body.ExecutionTimeout != 30_000 {
				t.Errorf("execute body = %+v", body)
			}
			writeTestJSON(t, w, map[string]any{"operationHandle": "operation-1"})
		case r.URL.Path == "/v3/sessions/session-1/operations/operation-1/status" && r.Method == http.MethodGet:
			writeTestJSON(t, w, map[string]any{"status": "RUNNING"})
		case r.URL.Path == "/v3/sessions/session-1/operations/operation-1/result/0" && r.Method == http.MethodGet:
			if r.URL.Query().Get("rowFormat") != "JSON" {
				t.Errorf("rowFormat = %q", r.URL.Query().Get("rowFormat"))
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{
                    "resultType":"EOS",
                    "isQueryResult":true,
                    "jobID":"0123456789abcdef0123456789abcdef",
                    "resultKind":"SUCCESS_WITH_CONTENT",
                    "results":{
                      "columns":[{"name":"amount","logicalType":{"type":"FUTURE_DECIMAL","nullable":true,"future":42},"comment":"value"}],
                      "rowFormat":"JSON",
                      "data":[
                        {"kind":"INSERT","fields":[1.25]},
                        {"kind":"UPDATE_BEFORE","fields":[1.25]},
                        {"kind":"UPDATE_AFTER","fields":[2.50]},
                        {"kind":"DELETE","fields":[2.50]}
                      ]
                    }
                  }`)
		case r.URL.Path == "/v3/sessions/session-1/operations/operation-1/cancel" && r.Method == http.MethodPost:
			writeTestJSON(t, w, map[string]any{"status": "CANCELED"})
		case r.URL.Path == "/v3/sessions/session-1/operations/operation-1/close" && r.Method == http.MethodDelete:
			writeTestJSON(t, w, map[string]any{"status": "CLOSED"})
		case r.URL.Path == "/v3/sessions/session-1" && r.Method == http.MethodDelete:
			sessionCloseCalls.Add(1)
			writeTestJSON(t, w, map[string]any{"status": "CLOSED"})
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := newTestClient(t, Config{
		BaseURL:    server.URL + "/",
		APIVersion: "V3",
		UserAgent:  "test-agent",
		Headers:    map[string]string{"X-Test-Auth": "secret"},
	})
	ctx := context.Background()

	info, err := client.GetInfo(ctx)
	if err != nil || info.Version != "1.20.4" {
		t.Fatalf("GetInfo() = %+v, %v", info, err)
	}
	versions, err := client.GetAPIVersions(ctx)
	if err != nil || strings.Join(versions, ",") != "v1,v2,v3" {
		t.Fatalf("GetAPIVersions() = %v, %v", versions, err)
	}
	session, err := client.OpenSession(ctx, OpenSessionRequest{SessionName: "workspace-1", Properties: map[string]string{"execution.runtime-mode": "streaming"}})
	if err != nil || session.Handle != "session-1" {
		t.Fatalf("OpenSession() = %+v, %v", session, err)
	}
	config, err := client.GetSessionConfig(ctx, session.Handle)
	if err != nil || config["execution.runtime-mode"] != "streaming" {
		t.Fatalf("GetSessionConfig() = %v, %v", config, err)
	}
	if err := client.ConfigureSession(ctx, session.Handle, "USE CATALOG cat", 2*time.Second); err != nil {
		t.Fatalf("ConfigureSession() error = %v", err)
	}
	candidates, err := client.CompleteStatement(ctx, session.Handle, "SEL", 3)
	if err != nil || len(candidates) != 1 || candidates[0] != "SELECT" {
		t.Fatalf("CompleteStatement() = %v, %v", candidates, err)
	}
	if err := client.Heartbeat(ctx, session.Handle); err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}
	operation, err := client.ExecuteStatement(ctx, session.Handle, ExecuteStatementRequest{Statement: "SHOW TABLES"})
	if err != nil || operation.Handle != "operation-1" {
		t.Fatalf("ExecuteStatement() = %+v, %v", operation, err)
	}
	status, err := client.GetOperationStatus(ctx, session.Handle, operation.Handle)
	if err != nil || status != OperationRunning || status.Terminal() {
		t.Fatalf("GetOperationStatus() = %q, %v", status, err)
	}
	page, err := client.FetchResults(ctx, session.Handle, operation.Handle, 0, RowFormatJSON)
	if err != nil {
		t.Fatalf("FetchResults() error = %v", err)
	}
	if page.ResultType != ResultEOS || !page.QueryResult || page.JobID != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("page metadata = %+v", page)
	}
	if got := page.Results.Data; len(got) != 4 || got[0].Kind != RowInsert || got[1].Kind != RowUpdateBefore || got[2].Kind != RowUpdateAfter || got[3].Kind != RowDelete {
		t.Fatalf("row kinds = %+v", got)
	}
	if !strings.Contains(string(page.Results.Columns[0].LogicalType.Raw), `"future":42`) {
		t.Fatalf("logical type raw JSON = %s", page.Results.Columns[0].LogicalType.Raw)
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
	if err := client.CloseSession(ctx, session.Handle); err != nil {
		t.Fatalf("second CloseSession() error = %v", err)
	}
	if got := sessionCloseCalls.Load(); got != 1 {
		t.Fatalf("session close calls = %d", got)
	}
}

func TestAPIVersionAndV1FeatureGuards(t *testing.T) {
	t.Parallel()
	var statements atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V1"}})
		case "/v4/sessions":
			statements.Add(1)
			writeTestJSON(t, w, map[string]any{"sessionHandle": "should-not-open"})
		case "/v1/sessions/s/operations/o/result/0":
			statements.Add(1)
			writeTestJSON(t, w, map[string]any{"resultType": "EOS"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	unsupported := newTestClient(t, Config{BaseURL: server.URL, APIVersion: "v4"})
	_, err := unsupported.OpenSession(context.Background(), OpenSessionRequest{})
	if !errors.Is(err, ErrUnsupportedAPI) || statements.Load() != 0 {
		t.Fatalf("unsupported OpenSession error = %v, requests = %d", err, statements.Load())
	}

	v1 := newTestClient(t, Config{BaseURL: server.URL, APIVersion: "1"})
	if _, err := v1.CompleteStatement(context.Background(), "s", "SEL", 3); !errors.Is(err, ErrUnsupportedAPI) {
		t.Fatalf("v1 CompleteStatement error = %v", err)
	}
	if err := v1.ConfigureSession(context.Background(), "s", "USE CATALOG c", time.Second); !errors.Is(err, ErrUnsupportedAPI) {
		t.Fatalf("v1 ConfigureSession error = %v", err)
	}
	if _, err := v1.FetchResults(context.Background(), "s", "o", 0, RowFormatPlainText); !errors.Is(err, ErrUnsupportedAPI) {
		t.Fatalf("v1 PLAIN_TEXT error = %v", err)
	}
	if statements.Load() != 0 {
		t.Fatalf("guarded requests reached server: %d", statements.Load())
	}
}

func TestRetryPolicyAndPOSTNoRetry(t *testing.T) {
	t.Parallel()
	var infoCalls atomic.Int32
	var statementCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case "/info":
			if infoCalls.Add(1) == 1 {
				http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
				return
			}
			writeTestJSON(t, w, map[string]any{"productName": "Flink", "version": "1.20.4"})
		case "/v3/sessions/s/statements":
			statementCalls.Add(1)
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newTestClient(t, Config{BaseURL: server.URL, PollInterval: time.Millisecond, MaxPollInterval: 2 * time.Millisecond})
	if _, err := client.GetInfo(context.Background()); err != nil || infoCalls.Load() != 2 {
		t.Fatalf("retryable GetInfo error = %v, calls = %d", err, infoCalls.Load())
	}
	_, err := client.ExecuteStatement(context.Background(), "s", ExecuteStatementRequest{Statement: "INSERT INTO sink SELECT * FROM source"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || !apiErr.Retryable || statementCalls.Load() != 1 {
		t.Fatalf("ExecuteStatement error = %v, calls = %d", err, statementCalls.Load())
	}
}

func TestKnownMissingSessionIsExpiredAndUnknownIsNotFound(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions" && r.Method == http.MethodPost:
			writeTestJSON(t, w, map[string]string{"sessionHandle": "known"})
		case strings.HasPrefix(r.URL.Path, "/v3/sessions/"):
			http.Error(w, "missing", http.StatusNotFound)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})
	if _, err := client.OpenSession(context.Background(), OpenSessionRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetSessionConfig(context.Background(), "known"); !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("known session error = %v", err)
	}
	if _, err := client.GetSessionConfig(context.Background(), "unknown"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("unknown session error = %v", err)
	}
}

func TestStatementValidatorReceivesSessionContextAndBlocksPOST(t *testing.T) {
	t.Parallel()
	var statementCalls atomic.Int32
	var validated SessionContext
	policyErr := errors.New("statement denied")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case "/v3/sessions":
			writeTestJSON(t, w, map[string]string{"sessionHandle": "s"})
		case "/v3/sessions/s/statements":
			statementCalls.Add(1)
			writeTestJSON(t, w, map[string]string{"operationHandle": "o"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{
		BaseURL: server.URL,
		Validator: statementValidatorFunc(func(_ context.Context, session SessionContext, _ string) error {
			validated = session
			return policyErr
		}),
	})
	_, err := client.OpenSession(context.Background(), OpenSessionRequest{SessionName: "owner", Properties: map[string]string{"k": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ExecuteStatement(context.Background(), "s", ExecuteStatementRequest{Statement: "DROP TABLE protected"})
	if !errors.Is(err, policyErr) || statementCalls.Load() != 0 {
		t.Fatalf("ExecuteStatement error = %v, calls=%d", err, statementCalls.Load())
	}
	if validated.Handle != "s" || validated.Name != "owner" || validated.Properties["k"] != "v" {
		t.Fatalf("validator context = %+v", validated)
	}
}

func TestTransportErrorsLimitsAndTimeout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		handler http.HandlerFunc
		config  Config
		match   error
	}{
		{
			name: "invalid json",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, "not-json")
			},
			match: nil,
		},
		{
			name: "response too large",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, `{"productName":"`+strings.Repeat("x", 100)+`"}`)
			},
			config: Config{MaxResponseBytes: 32},
			match:  ErrResponseTooLarge,
		},
		{
			name: "request timeout",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				time.Sleep(40 * time.Millisecond)
				writeTestJSON(t, w, map[string]string{"version": "late"})
			},
			config: Config{RequestTimeout: 5 * time.Millisecond, PollInterval: time.Millisecond, MaxPollInterval: 2 * time.Millisecond},
			match:  context.DeadlineExceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()
			cfg := tt.config
			cfg.BaseURL = server.URL
			client := newTestClient(t, cfg)
			_, err := client.GetInfo(context.Background())
			if err == nil {
				t.Fatal("GetInfo() error = nil")
			}
			if tt.match != nil && !errors.Is(err, tt.match) {
				t.Fatalf("GetInfo() error = %v, want errors.Is(%v)", err, tt.match)
			}
			var apiErr *APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("GetInfo() error type = %T", err)
			}
		})
	}
}

func TestNonJSONErrorIsSanitizedAndTyped(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "public summary\nstack trace secret")
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})
	_, err := client.GetInfo(context.Background())
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Message != "public summary" || strings.Contains(err.Error(), "secret") {
		t.Fatalf("sanitized error = %#v / %v", apiErr, err)
	}
}

func TestNewClientValidationAndSecurity(t *testing.T) {
	t.Parallel()
	if _, err := NewClient(Config{}); err == nil {
		t.Fatal("NewClient without BaseURL succeeded")
	}
	if _, err := NewClient(Config{BaseURL: "ftp://gateway"}); err == nil {
		t.Fatal("NewClient with ftp BaseURL succeeded")
	}
	client := newTestClient(t, Config{BaseURL: "https://gateway.example/root/"})
	if client.cfg.APIVersion != "v3" {
		t.Fatalf("default API version = %q", client.cfg.APIVersion)
	}
	if _, err := client.validateNextResultURI("/v3/sessions/s/operations/o/result/1"); err != nil {
		t.Fatalf("relative next URI error = %v", err)
	}
	for _, value := range []string{"http://127.0.0.1/v3/result/1", "https://evil.example/v3/result/1", "file:///tmp/result"} {
		if _, err := client.validateNextResultURI(value); !errors.Is(err, ErrUnsafeNextResultURI) {
			t.Errorf("validateNextResultURI(%q) error = %v", value, err)
		}
	}
	if got := MaskHandle("1234567890abcdef"); got != "1234...cdef" {
		t.Fatalf("MaskHandle() = %q", got)
	}
	sanitized := sanitizeEndpointPath("/v3/sessions/session-123456/operations/operation-123456/status")
	if strings.Contains(sanitized, "session-123456") || strings.Contains(sanitized, "operation-123456") {
		t.Fatalf("sanitizeEndpointPath() = %q", sanitized)
	}
}

func newTestClient(t *testing.T, cfg Config) *GatewayClient {
	t.Helper()
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	return client
}

func writeTestJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Errorf("encode response: %v", err)
	}
}

func decodeTestJSON(t *testing.T, r *http.Request, destination any) {
	t.Helper()
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(destination); err != nil {
		t.Errorf("decode request: %v", err)
	}
}

type statementValidatorFunc func(context.Context, SessionContext, string) error

func (f statementValidatorFunc) Validate(ctx context.Context, session SessionContext, statement string) error {
	return f(ctx, session, statement)
}
