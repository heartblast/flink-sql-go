package flinksqlgateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecuteAndWaitFollowsPagingAndCloses(t *testing.T) {
	t.Parallel()
	var statementCalls atomic.Int32
	var fetchCalls atomic.Int32
	var closeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions/s/statements":
			statementCalls.Add(1)
			writeTestJSON(t, w, map[string]string{"operationHandle": "o"})
		case r.URL.Path == "/v3/sessions/s/operations/o/result/0":
			if fetchCalls.Add(1) == 1 {
				writeTestJSON(t, w, map[string]any{"resultType": "NOT_READY", "nextResultUri": "/v3/sessions/s/operations/o/result/0?rowFormat=JSON"})
				return
			}
			writeTestJSON(t, w, resultPageFixture("PAYLOAD", "/v3/sessions/s/operations/o/result/1?rowFormat=JSON", "0123456789abcdef0123456789abcdef", []map[string]any{
				{"kind": "INSERT", "fields": []any{1}},
				{"kind": "UPDATE_AFTER", "fields": []any{2}},
			}))
		case r.URL.Path == "/v3/sessions/s/operations/o/result/1":
			fetchCalls.Add(1)
			writeTestJSON(t, w, resultPageFixture("EOS", "", "0123456789abcdef0123456789abcdef", []map[string]any{
				{"kind": "DELETE", "fields": []any{2}},
			}))
		case r.URL.Path == "/v3/sessions/s/operations/o/close":
			closeCalls.Add(1)
			writeTestJSON(t, w, map[string]string{"status": "CLOSED"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newTestClient(t, Config{BaseURL: server.URL, PollInterval: time.Millisecond, MaxPollInterval: 2 * time.Millisecond})
	result, err := client.ExecuteAndWait(context.Background(), "s", "SHOW TABLES", ExecuteOptions{MaxRows: 10})
	if err != nil {
		t.Fatalf("ExecuteAndWait() error = %v", err)
	}
	if statementCalls.Load() != 1 || fetchCalls.Load() != 3 || closeCalls.Load() != 1 {
		t.Fatalf("calls: statement=%d fetch=%d close=%d", statementCalls.Load(), fetchCalls.Load(), closeCalls.Load())
	}
	if result.Status != OperationFinished || result.Polls != 3 || result.Pages != 2 || result.RowsReceived != 3 || len(result.Rows) != 3 {
		t.Fatalf("result = %+v", result)
	}
	if result.Rows[0].Kind != RowInsert || result.Rows[1].Kind != RowUpdateAfter || result.Rows[2].Kind != RowDelete {
		t.Fatalf("row kinds = %+v", result.Rows)
	}
	if result.JobID != "0123456789abcdef0123456789abcdef" || !result.QueryResult {
		t.Fatalf("result metadata = %+v", result)
	}
}

func TestResultLimitCancelsAndCloses(t *testing.T) {
	t.Parallel()
	var cancelCalls atomic.Int32
	var closeCalls atomic.Int32
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case r.URL.Path == "/v3/sessions/s/operations/o/result/0":
			writeTestJSON(t, w, resultPageFixture("EOS", "", "", []map[string]any{
				{"kind": "INSERT", "fields": []any{1}},
				{"kind": "INSERT", "fields": []any{2}},
			}))
			return true
		case r.URL.Path == "/v3/sessions/s/operations/o/cancel":
			cancelCalls.Add(1)
			writeTestJSON(t, w, map[string]string{"status": "CANCELED"})
			return true
		case r.URL.Path == "/v3/sessions/s/operations/o/close":
			closeCalls.Add(1)
			writeTestJSON(t, w, map[string]string{"status": "CLOSED"})
			return true
		}
		return false
	})
	defer server.Close()

	client := newTestClient(t, Config{BaseURL: server.URL, PollInterval: time.Millisecond, MaxPollInterval: 2 * time.Millisecond})
	result, err := client.ExecuteAndWait(context.Background(), "s", "SELECT * FROM unbounded", ExecuteOptions{MaxRows: 1})
	if !errors.Is(err, ErrResultLimit) || result.RowsReceived != 1 || len(result.Rows) != 1 {
		t.Fatalf("ExecuteAndWait() = %+v, %v", result, err)
	}
	if cancelCalls.Load() != 1 || closeCalls.Load() != 1 {
		t.Fatalf("cleanup calls: cancel=%d close=%d", cancelCalls.Load(), closeCalls.Load())
	}
}

func TestContextCancellationPreservedAndOperationCanceled(t *testing.T) {
	t.Parallel()
	var cancelCalls atomic.Int32
	var closeCalls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case r.URL.Path == "/v3/sessions/s/operations/o/result/0":
			writeTestJSON(t, w, map[string]any{"resultType": "NOT_READY", "nextResultUri": "/v3/sessions/s/operations/o/result/0?rowFormat=JSON"})
			cancel()
			return true
		case r.URL.Path == "/v3/sessions/s/operations/o/cancel":
			cancelCalls.Add(1)
			writeTestJSON(t, w, map[string]string{"status": "CANCELED"})
			return true
		case r.URL.Path == "/v3/sessions/s/operations/o/close":
			closeCalls.Add(1)
			writeTestJSON(t, w, map[string]string{"status": "CLOSED"})
			return true
		}
		return false
	})
	defer server.Close()

	client := newTestClient(t, Config{
		BaseURL:             server.URL,
		PollInterval:        time.Millisecond,
		MaxPollInterval:     2 * time.Millisecond,
		CancelOnContextDone: true,
	})
	_, err := client.ExecuteAndWait(ctx, "s", "SELECT * FROM unbounded", ExecuteOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ExecuteAndWait() error = %v", err)
	}
	if cancelCalls.Load() != 1 || closeCalls.Load() != 1 {
		t.Fatalf("cleanup calls: cancel=%d close=%d", cancelCalls.Load(), closeCalls.Load())
	}
}

func TestUnsafeNextResultURIIsBlocked(t *testing.T) {
	t.Parallel()
	var closeCalls atomic.Int32
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case r.URL.Path == "/v3/sessions/s/operations/o/result/0":
			writeTestJSON(t, w, resultPageFixture("PAYLOAD", "http://127.0.0.1/private", "", nil))
			return true
		case r.URL.Path == "/v3/sessions/s/operations/o/close":
			closeCalls.Add(1)
			writeTestJSON(t, w, map[string]string{"status": "CLOSED"})
			return true
		}
		return false
	})
	defer server.Close()

	client := newTestClient(t, Config{BaseURL: server.URL})
	_, err := client.ExecuteAndWait(context.Background(), "s", "SHOW TABLES", ExecuteOptions{})
	if !errors.Is(err, ErrUnsafeNextResultURI) || closeCalls.Load() != 1 {
		t.Fatalf("ExecuteAndWait() error = %v, close=%d", err, closeCalls.Load())
	}
}

func TestStreamResultsEventOrder(t *testing.T) {
	t.Parallel()
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v3/sessions/s/operations/o/result/0":
			writeTestJSON(t, w, resultPageFixture("EOS", "", "", []map[string]any{{"kind": "UPDATE_BEFORE", "fields": []any{"old"}}}))
			return true
		case "/v3/sessions/s/operations/o/close":
			writeTestJSON(t, w, map[string]string{"status": "CLOSED"})
			return true
		}
		return false
	})
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})

	events, errs := client.StreamResults(context.Background(), "s", "SELECT * FROM changelog", StreamOptions{})
	var kinds []ResultEventType
	for event := range events {
		kinds = append(kinds, event.Type)
		if event.Type == ResultEventRow && event.Row.Kind != RowUpdateBefore {
			t.Errorf("stream row = %+v", event.Row)
		}
		if event.Type == ResultEventPage && event.Page.Results != nil && len(event.Page.Results.Data) != 0 {
			t.Errorf("page event exposed unbounded rows: %+v", event.Page.Results.Data)
		}
	}
	for err := range errs {
		t.Fatalf("StreamResults() error = %v", err)
	}
	want := []ResultEventType{ResultEventOperation, ResultEventPage, ResultEventRow, ResultEventEOS}
	if len(kinds) != len(want) {
		t.Fatalf("event types = %v", kinds)
	}
	for index := range want {
		if kinds[index] != want[index] {
			t.Fatalf("event types = %v", kinds)
		}
	}
}

func TestHeartbeatRunnerIsUniqueAndStopsWithSession(t *testing.T) {
	t.Parallel()
	var heartbeatCalls atomic.Int32
	var closeCalls atomic.Int32
	heartbeatSeen := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions/s/heartbeat":
			heartbeatCalls.Add(1)
			select {
			case heartbeatSeen <- struct{}{}:
			default:
			}
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v3/sessions/s" && r.Method == http.MethodDelete:
			closeCalls.Add(1)
			writeTestJSON(t, w, map[string]string{"status": "CLOSED"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newTestClient(t, Config{BaseURL: server.URL, HeartbeatInterval: 2 * time.Millisecond})
	runner1, err := client.StartHeartbeat(context.Background(), "s")
	if err != nil {
		t.Fatalf("StartHeartbeat() error = %v", err)
	}
	runner2, err := client.StartHeartbeat(context.Background(), "s")
	if err != nil || runner1 != runner2 {
		t.Fatalf("duplicate StartHeartbeat() = %p, %v", runner2, err)
	}
	select {
	case <-heartbeatSeen:
	case <-time.After(time.Second):
		t.Fatal("heartbeat was not sent")
	}
	if err := client.CloseSession(context.Background(), "s"); err != nil {
		t.Fatalf("CloseSession() error = %v", err)
	}
	count := heartbeatCalls.Load()
	time.Sleep(8 * time.Millisecond)
	if heartbeatCalls.Load() != count || closeCalls.Load() != 1 {
		t.Fatalf("heartbeat continued: before=%d after=%d close=%d", count, heartbeatCalls.Load(), closeCalls.Load())
	}
	if _, ok := <-runner1.Errors(); ok {
		t.Fatal("heartbeat errors channel is still open")
	}
}

func executionServer(t *testing.T, extension func(http.ResponseWriter, *http.Request) bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
			return
		case "/v3/sessions/s/statements":
			writeTestJSON(t, w, map[string]string{"operationHandle": "o"})
			return
		}
		if !extension(w, r) {
			http.NotFound(w, r)
		}
	}))
}

func resultPageFixture(resultType, nextURI, jobID string, rows []map[string]any) map[string]any {
	result := map[string]any{
		"resultType":    resultType,
		"isQueryResult": true,
		"resultKind":    "SUCCESS_WITH_CONTENT",
		"results": map[string]any{
			"columns":   []map[string]any{{"name": "value", "logicalType": map[string]any{"type": "INTEGER", "nullable": true}, "comment": nil}},
			"rowFormat": "JSON",
			"data":      rows,
		},
	}
	if nextURI != "" {
		result["nextResultUri"] = nextURI
	}
	if jobID != "" {
		result["jobID"] = jobID
	}
	return result
}
