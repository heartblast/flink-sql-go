package flinksqlgateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecuteStatementOutcomeUnknownWhenResponseHeaderIsMissing(t *testing.T) {
	var observations lifecycleRecorder
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case "/v3/sessions/s/statements":
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Fatal("response writer does not support hijacking")
			}
			connection, _, err := hijacker.Hijack()
			if err != nil {
				t.Fatalf("Hijack() error = %v", err)
			}
			_ = connection.Close()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newTestClient(t, Config{BaseURL: server.URL, LifecycleObserver: &observations})
	_, err := client.ExecuteStatement(context.Background(), "s", ExecuteStatementRequest{Statement: "INSERT INTO secret_table SELECT 'do-not-log'"})
	if !errors.Is(err, ErrExecutionOutcomeUnknown) {
		t.Fatalf("ExecuteStatement() error = %v", err)
	}
	var unknown *ExecutionOutcomeUnknownError
	if !errors.As(err, &unknown) {
		t.Fatalf("error type = %T", err)
	}
	if unknown.RequestPhase != ResponseHeaderMissing && unknown.RequestPhase != RequestPossiblySent {
		t.Fatalf("request phase = %s", unknown.RequestPhase)
	}
	if strings.Contains(err.Error(), "secret_table") || strings.Contains(err.Error(), "do-not-log") {
		t.Fatalf("error leaks SQL: %v", err)
	}
	if !observations.contains(ObservationStatementOutcomeUnknown) {
		t.Fatalf("lifecycle events = %v", observations.events())
	}
}

func TestExecuteStatementPreSendFailureIsNotOutcomeUnknown(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("dial refused")
	})}
	client := newTestClient(t, Config{BaseURL: "http://gateway.invalid", HTTPClient: httpClient})
	client.versionChecked = true

	_, err := client.ExecuteStatement(context.Background(), "s", ExecuteStatementRequest{Statement: "SELECT 1"})
	if err == nil || errors.Is(err, ErrExecutionOutcomeUnknown) {
		t.Fatalf("ExecuteStatement() error = %v", err)
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.RequestPhase != RequestNotSent {
		t.Fatalf("API error = %#v", apiErr)
	}
}

func TestExecuteStatementOmitsUnsupportedFlink120ExecutionTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case "/v3/sessions/s/statements":
			var body map[string]json.RawMessage
			decodeTestJSON(t, r, &body)
			if _, exists := body["executionTimeout"]; exists {
				http.Error(w, "SqlGatewayService doesn't support timeout mechanism now.", http.StatusInternalServerError)
				return
			}
			writeTestJSON(t, w, map[string]string{"operationHandle": "o"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newTestClient(t, Config{BaseURL: server.URL, ExecutionTimeout: 30 * time.Second})
	operation, err := client.ExecuteStatement(context.Background(), "s", ExecuteStatementRequest{
		Statement:        "SELECT 1",
		ExecutionTimeout: time.Minute,
	})
	if err != nil || operation == nil || operation.Handle != "o" {
		t.Fatalf("ExecuteStatement() = %+v, %v", operation, err)
	}
}

func TestExecuteStatementHTTPStatusClassification(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		wantUnknown bool
	}{
		{name: "bad request", status: http.StatusBadRequest},
		{name: "request timeout", status: http.StatusRequestTimeout, wantUnknown: true},
		{name: "too many requests", status: http.StatusTooManyRequests, wantUnknown: true},
		{name: "internal error", status: http.StatusInternalServerError, wantUnknown: true},
		{name: "unavailable", status: http.StatusServiceUnavailable, wantUnknown: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api_versions":
					writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
				case "/v3/sessions/s/statements":
					http.Error(w, "submission failed", test.status)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client := newTestClient(t, Config{BaseURL: server.URL})
			_, err := client.ExecuteStatement(context.Background(), "s", ExecuteStatementRequest{Statement: "INSERT INTO sink SELECT * FROM source"})
			if errors.Is(err, ErrExecutionOutcomeUnknown) != test.wantUnknown {
				t.Fatalf("ExecuteStatement() error = %v", err)
			}
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.Retryable {
				t.Fatalf("API error = %#v", apiErr)
			}
		})
	}
}

func TestConfigureSessionOutcomeUnknownWhenResponseHeaderIsMissing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case "/v3/sessions/s/configure-session":
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Fatal("response writer does not support hijacking")
			}
			connection, _, err := hijacker.Hijack()
			if err != nil {
				t.Fatalf("Hijack() error = %v", err)
			}
			_ = connection.Close()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})

	err := client.ConfigureSession(context.Background(), "s", "CREATE TABLE secret_table (value STRING)", time.Second)
	if !errors.Is(err, ErrConfigurationOutcomeUnknown) {
		t.Fatalf("ConfigureSession() error = %v", err)
	}
	var unknown *ConfigurationOutcomeUnknownError
	if !errors.As(err, &unknown) || unknown.StepIndex != -1 {
		t.Fatalf("unknown error = %+v", unknown)
	}
	if strings.Contains(err.Error(), "secret_table") {
		t.Fatalf("error leaks SQL: %v", err)
	}
}

func TestConfigureSessionPreSendFailureIsNotOutcomeUnknown(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("dial refused")
	})}
	client := newTestClient(t, Config{BaseURL: "http://gateway.invalid", HTTPClient: httpClient})
	client.versionChecked = true

	err := client.ConfigureSession(context.Background(), "s", "USE CATALOG c", time.Second)
	if err == nil || errors.Is(err, ErrConfigurationOutcomeUnknown) {
		t.Fatalf("ConfigureSession() error = %v", err)
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.RequestPhase != RequestNotSent {
		t.Fatalf("API error = %+v", apiErr)
	}
}

func TestConfigureSessionHTTPStatusOutcomeClassificationAndNoRetry(t *testing.T) {
	tests := []struct {
		status      int
		wantUnknown bool
	}{
		{status: http.StatusBadRequest},
		{status: http.StatusRequestTimeout, wantUnknown: true},
		{status: http.StatusTooManyRequests, wantUnknown: true},
		{status: http.StatusInternalServerError, wantUnknown: true},
	}
	for _, test := range tests {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api_versions":
					writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
				case "/v3/sessions/s/configure-session":
					calls.Add(1)
					writeTestJSONStatus(t, w, test.status, map[string]string{"message": "configuration failed"})
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client := newTestClient(t, Config{BaseURL: server.URL, PollInterval: time.Millisecond})
			err := client.ConfigureSession(context.Background(), "s", "USE CATALOG c", time.Second)
			if errors.Is(err, ErrConfigurationOutcomeUnknown) != test.wantUnknown {
				t.Fatalf("ConfigureSession() error = %v", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("configure calls = %d", calls.Load())
			}
		})
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type lifecycleRecorder struct {
	mu           sync.Mutex
	observations []Observation
}

func (r *lifecycleRecorder) ObserveLifecycle(_ context.Context, observation Observation) {
	r.mu.Lock()
	r.observations = append(r.observations, observation)
	r.mu.Unlock()
}

func (r *lifecycleRecorder) contains(event ObservationEvent) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, observation := range r.observations {
		if observation.Event == event {
			return true
		}
	}
	return false
}

func (r *lifecycleRecorder) events() []ObservationEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]ObservationEvent, len(r.observations))
	for index, observation := range r.observations {
		result[index] = observation.Event
	}
	return result
}
