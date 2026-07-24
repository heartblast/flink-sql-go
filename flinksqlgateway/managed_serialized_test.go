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

func TestManagedSessionHealthAndConcurrentClose(t *testing.T) {
	var heartbeatMode atomic.Int32
	var heartbeatCalls atomic.Int32
	var closeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions" && r.Method == http.MethodPost:
			writeTestJSON(t, w, map[string]string{"sessionHandle": "managed-session"})
		case r.URL.Path == "/v3/sessions/managed-session/heartbeat":
			heartbeatCalls.Add(1)
			switch heartbeatMode.Load() {
			case 1:
				http.Error(w, "temporary", http.StatusServiceUnavailable)
			case 2:
				http.Error(w, "session expired", http.StatusGone)
			default:
				w.WriteHeader(http.StatusOK)
			}
		case r.URL.Path == "/v3/sessions/managed-session" && r.Method == http.MethodDelete:
			closeCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newTestClient(t, Config{BaseURL: server.URL, PollInterval: time.Millisecond, MaxPollInterval: 2 * time.Millisecond})
	managed, err := client.OpenManagedSession(context.Background(), OpenSessionRequest{SessionName: "workspace"}, ManagedSessionOptions{
		HeartbeatInterval: 2 * time.Millisecond,
		FailureThreshold:  2,
		CleanupTimeout:    time.Second,
	})
	if err != nil {
		t.Fatalf("OpenManagedSession() error = %v", err)
	}
	waitFor(t, time.Second, func() bool { return heartbeatCalls.Load() > 0 && managed.Health() == SessionHealthy })

	heartbeatMode.Store(1)
	waitFor(t, time.Second, func() bool { return managed.Health() == SessionDegraded })
	heartbeatMode.Store(0)
	waitFor(t, time.Second, func() bool { return managed.Health() == SessionHealthy })
	heartbeatMode.Store(2)
	waitFor(t, time.Second, func() bool { return managed.Health() == SessionExpired })
	if _, err := managed.Execute(context.Background(), "SELECT 1", ExecuteOptions{}); !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("Execute() after expiration error = %v", err)
	}

	var waitGroup sync.WaitGroup
	errorsChannel := make(chan error, 8)
	for range 8 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			errorsChannel <- managed.Close(context.Background())
		}()
	}
	waitGroup.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}
	if closeCalls.Load() != 1 || managed.Health() != SessionClosed {
		t.Fatalf("close calls=%d health=%s", closeCalls.Load(), managed.Health())
	}
}

func TestSerializedSessionQueueCancellationAndOrdering(t *testing.T) {
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var statementCalls atomic.Int32
	var mu sync.Mutex
	var statements []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions/s/statements":
			var body struct {
				Statement string `json:"statement"`
			}
			decodeTestJSON(t, r, &body)
			statementCalls.Add(1)
			mu.Lock()
			statements = append(statements, body.Statement)
			mu.Unlock()
			if body.Statement == "first" {
				close(firstEntered)
				<-releaseFirst
			}
			writeTestJSON(t, w, map[string]string{"operationHandle": "op-" + body.Statement})
		case strings.Contains(r.URL.Path, "/result/0"):
			writeTestJSON(t, w, resultPageFixture("EOS", "", "", nil))
		case strings.HasSuffix(r.URL.Path, "/close"):
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v3/sessions/s" && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})
	serialized := NewSerializedSession(client, "s")

	firstDone := make(chan error, 1)
	go func() {
		_, err := serialized.Execute(context.Background(), "first", ExecuteOptions{})
		firstDone <- err
	}()
	<-firstEntered

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := serialized.Execute(waitCtx, "canceled", ExecuteOptions{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued Execute() error = %v", err)
	}
	if statementCalls.Load() != 1 {
		t.Fatalf("queued canceled statement reached server; calls=%d", statementCalls.Load())
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	if _, err := serialized.Execute(context.Background(), "second", ExecuteOptions{}); err != nil {
		t.Fatalf("second Execute() error = %v", err)
	}
	mu.Lock()
	got := append([]string(nil), statements...)
	mu.Unlock()
	if strings.Join(got, ",") != "first,second" {
		t.Fatalf("statement order = %v", got)
	}
	if err := serialized.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := serialized.Execute(context.Background(), "third", ExecuteOptions{}); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("Execute() after Close error = %v", err)
	}
}

func TestSerializedSessionsDoNotBlockEachOther(t *testing.T) {
	entered := make(chan string, 2)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case strings.HasSuffix(r.URL.Path, "/statements"):
			parts := strings.Split(r.URL.Path, "/")
			session := parts[3]
			entered <- session
			<-release
			writeTestJSON(t, w, map[string]string{"operationHandle": "op"})
		case strings.HasSuffix(r.URL.Path, "/result/0"):
			writeTestJSON(t, w, resultPageFixture("EOS", "", "", nil))
		case strings.HasSuffix(r.URL.Path, "/close"):
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})
	one := NewSerializedSession(client, "s1")
	two := NewSerializedSession(client, "s2")

	done := make(chan error, 2)
	go func() { _, err := one.Execute(context.Background(), "one", ExecuteOptions{}); done <- err }()
	go func() { _, err := two.Execute(context.Background(), "two", ExecuteOptions{}); done <- err }()
	seen := map[string]bool{}
	for range 2 {
		select {
		case session := <-entered:
			seen[session] = true
		case <-time.After(time.Second):
			t.Fatal("different sessions did not execute concurrently")
		}
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
	}
	if !seen["s1"] || !seen["s2"] {
		t.Fatalf("sessions = %v", seen)
	}
}

func TestClientCloseIsIdempotentAndRejectsRequests(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(t, w, map[string]string{"productName": "Apache Flink", "version": "1.20.4"})
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})
	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := client.GetInfo(context.Background()); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("GetInfo() after Close error = %v", err)
	}
}

func TestManagedSessionCloseCancelsHeartbeatInFlight(t *testing.T) {
	heartbeatStarted := make(chan struct{})
	heartbeatRelease := make(chan struct{})
	var startOnce sync.Once
	var closeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions" && r.Method == http.MethodPost:
			writeTestJSON(t, w, map[string]string{"sessionHandle": "race-session"})
		case r.URL.Path == "/v3/sessions/race-session/heartbeat":
			startOnce.Do(func() { close(heartbeatStarted) })
			select {
			case <-r.Context().Done():
			case <-heartbeatRelease:
			}
		case r.URL.Path == "/v3/sessions/race-session" && r.Method == http.MethodDelete:
			closeCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL, RequestTimeout: time.Second})
	managed, err := client.OpenManagedSession(context.Background(), OpenSessionRequest{}, ManagedSessionOptions{HeartbeatInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("OpenManagedSession() error = %v", err)
	}
	select {
	case <-heartbeatStarted:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not start")
	}
	if err := managed.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	close(heartbeatRelease)
	if closeCalls.Load() != 1 || managed.Health() != SessionClosed {
		t.Fatalf("close calls=%d health=%s", closeCalls.Load(), managed.Health())
	}
}

func TestClientCloseStopsActiveResultStream(t *testing.T) {
	fetchStarted := make(chan struct{})
	fetchRelease := make(chan struct{})
	var fetchOnce sync.Once
	var cancelCalls atomic.Int32
	var closeCalls atomic.Int32
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v3/sessions/s/operations/o/result/0":
			fetchOnce.Do(func() { close(fetchStarted) })
			select {
			case <-r.Context().Done():
			case <-fetchRelease:
			}
			return true
		case "/v3/sessions/s/operations/o/cancel":
			cancelCalls.Add(1)
			w.WriteHeader(http.StatusOK)
			return true
		case "/v3/sessions/s/operations/o/close":
			closeCalls.Add(1)
			w.WriteHeader(http.StatusOK)
			return true
		}
		return false
	})
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL, CancelOnContextDone: true, RequestTimeout: time.Second})
	stream, err := client.ExecuteStream(context.Background(), "s", "SELECT * FROM source", StreamOptions{})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	nextDone := make(chan bool, 1)
	go func() { nextDone <- stream.Next() }()
	select {
	case <-fetchStarted:
	case <-time.After(time.Second):
		t.Fatal("result fetch did not start")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Client.Close() error = %v", err)
	}
	close(fetchRelease)
	select {
	case next := <-nextDone:
		if next {
			t.Fatal("Next() succeeded after Client.Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Next() did not stop after Client.Close")
	}
	if cancelCalls.Load() != 1 || closeCalls.Load() != 1 {
		t.Fatalf("cleanup calls: cancel=%d close=%d", cancelCalls.Load(), closeCalls.Load())
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not satisfied before timeout")
}
