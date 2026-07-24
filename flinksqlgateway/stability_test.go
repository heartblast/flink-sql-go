package flinksqlgateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockingObserver struct {
	release <-chan struct{}
}

func (o blockingObserver) ObserveRequest(context.Context, RequestObservation) {
	<-o.release
}

func TestManagedHeartbeatRequestTimeoutRecovers(t *testing.T) {
	var timeoutMode atomic.Bool
	var heartbeatCalls atomic.Int32
	timeoutMode.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions" && r.Method == http.MethodPost:
			writeTestJSON(t, w, map[string]string{"sessionHandle": "timeout-session"})
		case r.URL.Path == "/v3/sessions/timeout-session/heartbeat":
			heartbeatCalls.Add(1)
			if timeoutMode.Load() {
				select {
				case <-r.Context().Done():
				case <-time.After(25 * time.Millisecond):
				}
				return
			}
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v3/sessions/timeout-session" && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newTestClient(t, Config{
		BaseURL:           server.URL,
		RequestTimeout:    5 * time.Millisecond,
		PollInterval:      time.Millisecond,
		MaxPollInterval:   2 * time.Millisecond,
		HeartbeatInterval: 2 * time.Millisecond,
	})
	managed, err := client.OpenManagedSession(context.Background(), OpenSessionRequest{}, ManagedSessionOptions{
		HeartbeatInterval: 2 * time.Millisecond,
		FailureThreshold:  2,
		CleanupTimeout:    time.Second,
	})
	if err != nil {
		t.Fatalf("OpenManagedSession() error = %v", err)
	}
	waitFor(t, time.Second, func() bool { return managed.Health() == SessionDegraded })
	callsAfterTimeout := heartbeatCalls.Load()
	timeoutMode.Store(false)
	waitFor(t, time.Second, func() bool {
		return heartbeatCalls.Load() > callsAfterTimeout && managed.Health() == SessionHealthy
	})
	if err := managed.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestOpenSessionReturnsIsolatedSnapshot(t *testing.T) {
	var validated SessionContext
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions":
			writeTestJSON(t, w, map[string]string{"sessionHandle": "internal-handle"})
		case r.URL.Path == "/v3/sessions/internal-handle/statements":
			writeTestJSON(t, w, map[string]string{"operationHandle": "operation"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{
		BaseURL: server.URL,
		Validator: statementValidatorFunc(func(_ context.Context, session SessionContext, _ string) error {
			validated = session
			return nil
		}),
	})
	session, err := client.OpenSession(context.Background(), OpenSessionRequest{
		SessionName: "original-name",
		Properties:  map[string]string{"owner": "original-owner"},
	})
	if err != nil {
		t.Fatalf("OpenSession() error = %v", err)
	}
	session.Handle = "changed-handle"
	session.Name = "changed-name"
	session.Properties["owner"] = "changed-owner"
	if _, err := client.ExecuteStatement(context.Background(), "internal-handle", ExecuteStatementRequest{Statement: "SELECT 1"}); err != nil {
		t.Fatalf("ExecuteStatement() error = %v", err)
	}
	if validated.Handle != "internal-handle" || validated.Name != "original-name" || validated.Properties["owner"] != "original-owner" {
		t.Fatalf("validator session = %#v", validated)
	}
}

func TestSerializedCloseWaitsForOperationCleanup(t *testing.T) {
	fetchStarted := make(chan struct{})
	var fetchOnce sync.Once
	var orderMu sync.Mutex
	var order []string
	appendOrder := func(value string) {
		orderMu.Lock()
		order = append(order, value)
		orderMu.Unlock()
	}
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v3/sessions/s/operations/o/result/0":
			fetchOnce.Do(func() { close(fetchStarted) })
			<-r.Context().Done()
			return true
		case "/v3/sessions/s/operations/o/cancel":
			appendOrder("cancel")
			w.WriteHeader(http.StatusOK)
			return true
		case "/v3/sessions/s/operations/o/close":
			appendOrder("close-operation")
			w.WriteHeader(http.StatusOK)
			return true
		case "/v3/sessions/s":
			if r.Method == http.MethodDelete {
				appendOrder("close-session")
				w.WriteHeader(http.StatusOK)
				return true
			}
		}
		return false
	})
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL, CancelOnContextDone: true, RequestTimeout: time.Second})
	serialized := NewSerializedSession(client, "s")
	executeDone := make(chan error, 1)
	go func() {
		_, err := serialized.Execute(context.Background(), "SELECT * FROM source", ExecuteOptions{})
		executeDone <- err
	}()
	select {
	case <-fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("result fetch did not start")
	}
	if err := serialized.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := <-executeDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v", err)
	}
	orderMu.Lock()
	got := append([]string(nil), order...)
	orderMu.Unlock()
	want := []string{"cancel", "close-operation", "close-session"}
	if len(got) != len(want) {
		t.Fatalf("cleanup order = %v", got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("cleanup order = %v", got)
		}
	}
}

func TestSerializedCloseCanRetryServerCleanup(t *testing.T) {
	var closeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions/s" && r.Method == http.MethodDelete:
			if closeCalls.Add(1) == 1 {
				select {
				case <-r.Context().Done():
				case <-time.After(25 * time.Millisecond):
				}
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL, RequestTimeout: time.Second})
	serialized := NewSerializedSession(client, "s")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := serialized.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := serialized.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if closeCalls.Load() != 2 {
		t.Fatalf("close calls = %d", closeCalls.Load())
	}
}

func TestSerializedClosePropagatesCallerDeadlineToStreamCleanup(t *testing.T) {
	var sessionCloseCalls atomic.Int32
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v3/sessions/s/operations/o/cancel":
			select {
			case <-r.Context().Done():
			case <-time.After(40 * time.Millisecond):
			}
			return true
		case "/v3/sessions/s/operations/o/close":
			w.WriteHeader(http.StatusOK)
			return true
		case "/v3/sessions/s":
			sessionCloseCalls.Add(1)
			w.WriteHeader(http.StatusOK)
			return true
		}
		return false
	})
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL, RequestTimeout: time.Second})
	serialized := NewSerializedSession(client, "s")
	if _, err := serialized.Stream(context.Background(), "SELECT 1", StreamOptions{}); err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := serialized.Close(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("Close() exceeded caller deadline: %s", elapsed)
	}
	if err := serialized.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if sessionCloseCalls.Load() < 1 || sessionCloseCalls.Load() > 2 {
		t.Fatalf("session close calls = %d", sessionCloseCalls.Load())
	}
}

func TestCleanupClosesOperationAfterCancelTimeout(t *testing.T) {
	var closeCalls atomic.Int32
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v3/sessions/s/operations/o/result/0":
			writeTestJSON(t, w, resultPageFixture("EOS", "", "", []map[string]any{
				{"kind": "INSERT", "fields": []any{1}},
				{"kind": "INSERT", "fields": []any{2}},
			}))
			return true
		case "/v3/sessions/s/operations/o/cancel":
			select {
			case <-r.Context().Done():
			case <-time.After(40 * time.Millisecond):
			}
			return true
		case "/v3/sessions/s/operations/o/close":
			closeCalls.Add(1)
			w.WriteHeader(http.StatusOK)
			return true
		}
		return false
	})
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL, RequestTimeout: 50 * time.Millisecond})
	_, err := client.ExecuteAndWait(context.Background(), "s", "SELECT * FROM source", ExecuteOptions{MaxRows: 1})
	if !errors.Is(err, ErrResultLimit) {
		t.Fatalf("ExecuteAndWait() error = %v", err)
	}
	if closeCalls.Load() != 1 {
		t.Fatalf("close calls = %d", closeCalls.Load())
	}
}

func TestHandleValidationRejectsBeforeNetwork(t *testing.T) {
	var requests atomic.Int32
	httpClient := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return nil, fmt.Errorf("unexpected network request")
	})}
	client := newTestClient(t, Config{BaseURL: "http://gateway.invalid", HTTPClient: httpClient})
	client.versionChecked = true
	tests := []struct {
		name string
		call func() error
	}{
		{"get session", func() error { _, err := client.GetSessionConfig(context.Background(), " "); return err }},
		{"configure", func() error { return client.ConfigureSession(context.Background(), "", "SET 'k'='v'", time.Second) }},
		{"complete", func() error { _, err := client.CompleteStatement(context.Background(), "\n", "SEL", 3); return err }},
		{"heartbeat", func() error { return client.Heartbeat(context.Background(), "\x00") }},
		{"close session", func() error { return client.CloseSession(context.Background(), "") }},
		{"execute", func() error {
			_, err := client.ExecuteStatement(context.Background(), "", ExecuteStatementRequest{Statement: "SELECT 1"})
			return err
		}},
		{"status", func() error { _, err := client.GetOperationStatus(context.Background(), "s", ""); return err }},
		{"fetch", func() error {
			_, err := client.FetchResults(context.Background(), "s", " ", 0, RowFormatJSON)
			return err
		}},
		{"cancel", func() error { return client.CancelOperation(context.Background(), "s", "\t") }},
		{"close operation", func() error { return client.CloseOperation(context.Background(), "", "o") }},
		{"heartbeat runner", func() error { _, err := client.StartHeartbeat(context.Background(), ""); return err }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil {
				t.Fatal("invalid handle was accepted")
			}
		})
	}
	if requests.Load() != 0 {
		t.Fatalf("network requests = %d", requests.Load())
	}
	if err := validateSessionHandle("세션/opaque"); err != nil {
		t.Fatalf("valid opaque handle error = %v", err)
	}
}

func TestClosedSessionCacheIsBounded(t *testing.T) {
	client := newTestClient(t, Config{BaseURL: "http://gateway.invalid"})
	client.stateMu.Lock()
	for index := 0; index < maxRememberedClosedSessions*3; index++ {
		client.rememberClosedSessionLocked(fmt.Sprintf("session-%d", index))
	}
	closedCount := len(client.closed)
	orderCount := len(client.closedOrder)
	client.stateMu.Unlock()
	if closedCount != maxRememberedClosedSessions || orderCount != maxRememberedClosedSessions {
		t.Fatalf("closed cache sizes = %d, %d", closedCount, orderCount)
	}
}

func TestReissuedSessionHandleKeepsNewestCloseCacheEntry(t *testing.T) {
	client := newTestClient(t, Config{BaseURL: "http://gateway.invalid"})
	client.stateMu.Lock()
	client.rememberClosedSessionLocked("reissued")
	client.forgetClosedSessionLocked("reissued")
	client.rememberClosedSessionLocked("reissued")
	for index := 0; index < maxRememberedClosedSessions-1; index++ {
		client.rememberClosedSessionLocked(fmt.Sprintf("session-%d", index))
	}
	_, retained := client.closed["reissued"]
	closedCount := len(client.closed)
	orderCount := len(client.closedOrder)
	client.stateMu.Unlock()
	if !retained || closedCount != maxRememberedClosedSessions || orderCount != maxRememberedClosedSessions {
		t.Fatalf("reissued retained=%v cache sizes=%d,%d", retained, closedCount, orderCount)
	}
}

func TestBlockingObserverIsBounded(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(t, w, map[string]any{"productName": "Flink", "version": "1.20.4"})
	}))
	defer server.Close()
	client := newTestClient(t, Config{
		BaseURL:             server.URL,
		Observer:            blockingObserver{release: release},
		ObserverTimeout:     10 * time.Millisecond,
		ObserverMaxInFlight: 1,
	})
	started := time.Now()
	if _, err := client.GetInfo(context.Background()); err != nil {
		t.Fatalf("GetInfo() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("GetInfo() blocked for %s", elapsed)
	}
	started = time.Now()
	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("Close() blocked for %s", elapsed)
	}
	close(release)
}

func TestClientCloseStopsUnreadStreamResults(t *testing.T) {
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/v3/sessions/s/operations/o/result/0" {
			writeTestJSON(t, w, resultPageFixture("EOS", "", "", []map[string]any{
				{"kind": "INSERT", "fields": []any{1}},
				{"kind": "INSERT", "fields": []any{2}},
			}))
			return true
		}
		return false
	})
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL, StreamBuffer: 1, RequestTimeout: time.Second})
	client.StreamResults(context.Background(), "s", "SELECT * FROM source", StreamOptions{})
	done := make(chan error, 1)
	go func() { done <- client.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Client.Close() blocked on unread stream")
	}
}
