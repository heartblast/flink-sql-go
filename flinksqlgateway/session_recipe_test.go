package flinksqlgateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenSessionFromRecipeReplaysStatementsInOrder(t *testing.T) {
	var mu sync.Mutex
	var statements []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions" && r.Method == http.MethodPost:
			var request OpenSessionRequest
			decodeTestJSON(t, r, &request)
			if request.SessionName != "workspace" || request.Properties["execution.runtime-mode"] != "streaming" {
				t.Errorf("open session request = %+v", request)
			}
			writeTestJSON(t, w, map[string]string{"sessionHandle": "recipe-session"})
		case r.URL.Path == "/v3/sessions/recipe-session/statements":
			var request struct {
				Statement string `json:"statement"`
			}
			decodeTestJSON(t, r, &request)
			mu.Lock()
			statements = append(statements, request.Statement)
			index := len(statements)
			mu.Unlock()
			writeTestJSON(t, w, map[string]string{"operationHandle": "recipe-op-" + string(rune('0'+index))})
		case strings.Contains(r.URL.Path, "/result/0"):
			writeTestJSON(t, w, resultPageFixture("EOS", "", "", nil))
		case strings.HasSuffix(r.URL.Path, "/close"):
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})
	recipe := SessionRecipe{
		Name:       "workspace",
		Properties: map[string]string{"execution.runtime-mode": "streaming"},
		SetupStatements: []string{
			"USE CATALOG kafka_catalog",
			"USE kafka_database",
		},
	}
	result, err := client.OpenSessionFromRecipe(context.Background(), recipe)
	if err != nil {
		t.Fatalf("OpenSessionFromRecipe() error = %v", err)
	}
	if !result.Complete || result.FailedIndex != -1 || len(result.Applied) != 2 {
		t.Fatalf("result = %+v", result)
	}
	mu.Lock()
	got := strings.Join(statements, ",")
	mu.Unlock()
	if got != "USE CATALOG kafka_catalog,USE kafka_database" {
		t.Fatalf("statement order = %q", got)
	}
}

func TestOpenSessionFromRecipeStopsAndClosesAfterFailure(t *testing.T) {
	var statementCalls atomic.Int32
	var sessionCloseCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions" && r.Method == http.MethodPost:
			writeTestJSON(t, w, map[string]string{"sessionHandle": "recipe-session"})
		case r.URL.Path == "/v3/sessions/recipe-session/statements":
			if statementCalls.Add(1) == 2 {
				writeTestJSONStatus(t, w, http.StatusBadRequest, map[string]string{"message": "setup failed"})
				return
			}
			writeTestJSON(t, w, map[string]string{"operationHandle": "recipe-op"})
		case r.URL.Path == "/v3/sessions/recipe-session/operations/recipe-op/result/0":
			writeTestJSON(t, w, resultPageFixture("EOS", "", "", nil))
		case r.URL.Path == "/v3/sessions/recipe-session/operations/recipe-op/close":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v3/sessions/recipe-session" && r.Method == http.MethodDelete:
			sessionCloseCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})
	result, err := client.OpenSessionFromRecipe(context.Background(), SessionRecipe{SetupStatements: []string{"first", "second", "third"}})
	var replayErr *RecipeReplayError
	if err == nil || !errors.As(err, &replayErr) {
		t.Fatalf("OpenSessionFromRecipe() error = %v", err)
	}
	if result.FailedIndex != 1 || result.Complete || len(result.Applied) != 1 || statementCalls.Load() != 2 || sessionCloseCalls.Load() != 1 {
		t.Fatalf("result=%+v statements=%d closes=%d", result, statementCalls.Load(), sessionCloseCalls.Load())
	}
	if strings.Contains(err.Error(), "second") {
		t.Fatalf("recipe error leaks statement text: %v", err)
	}
}

func TestOpenSessionFromRecipeContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var sessionCloseCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions":
			writeTestJSON(t, w, map[string]string{"sessionHandle": "recipe-session"})
		case r.URL.Path == "/v3/sessions/recipe-session/statements":
			writeTestJSON(t, w, map[string]string{"operationHandle": "recipe-op"})
		case r.URL.Path == "/v3/sessions/recipe-session/operations/recipe-op/result/0":
			cancel()
			writeTestJSON(t, w, map[string]any{"resultType": "NOT_READY"})
		case r.URL.Path == "/v3/sessions/recipe-session/operations/recipe-op/cancel":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v3/sessions/recipe-session/operations/recipe-op/close":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v3/sessions/recipe-session" && r.Method == http.MethodDelete:
			sessionCloseCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL, CancelOnContextDone: true, PollInterval: time.Millisecond})
	result, err := client.OpenSessionFromRecipe(ctx, SessionRecipe{SetupStatements: []string{"first", "second"}})
	if !errors.Is(err, context.Canceled) || result.FailedIndex != 0 || sessionCloseCalls.Load() != 1 {
		t.Fatalf("result=%+v error=%v closes=%d", result, err, sessionCloseCalls.Load())
	}
}

func TestOpenSessionFromRecipeCanKeepFailedSession(t *testing.T) {
	var sessionCloseCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions" && r.Method == http.MethodPost:
			writeTestJSON(t, w, map[string]string{"sessionHandle": "kept-session"})
		case r.URL.Path == "/v3/sessions/kept-session/statements":
			writeTestJSONStatus(t, w, http.StatusBadRequest, map[string]string{"message": "setup failed"})
		case r.URL.Path == "/v3/sessions/kept-session" && r.Method == http.MethodDelete:
			sessionCloseCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})

	result, err := client.OpenSessionFromRecipeWithOptions(
		context.Background(),
		SessionRecipe{SetupStatements: []string{"sensitive setup"}},
		SessionRecipeOptions{KeepSessionOnFailure: true},
	)
	if err == nil || result.SessionHandle != "kept-session" || result.FailedIndex != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if sessionCloseCalls.Load() != 0 {
		t.Fatalf("session close calls = %d", sessionCloseCalls.Load())
	}
	if strings.Contains(err.Error(), "sensitive setup") {
		t.Fatalf("recipe error leaks statement text: %v", err)
	}
}

func writeTestJSONStatus(t *testing.T, w http.ResponseWriter, status int, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	writeTestJSON(t, w, value)
}
