package flinksqlgateway

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestResultStreamIteratesPagesAndClosesAtEOS(t *testing.T) {
	var fetchCalls atomic.Int32
	var closeCalls atomic.Int32
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v3/sessions/s/operations/o/result/0":
			fetchCalls.Add(1)
			writeTestJSON(t, w, resultPageFixture("PAYLOAD", "/v3/sessions/s/operations/o/result/1?rowFormat=JSON", "0123456789abcdef0123456789abcdef", []map[string]any{
				{"kind": "INSERT", "fields": []any{1}},
				{"kind": "INSERT", "fields": []any{2}},
			}))
			return true
		case "/v3/sessions/s/operations/o/result/1":
			fetchCalls.Add(1)
			writeTestJSON(t, w, resultPageFixture("EOS", "", "0123456789abcdef0123456789abcdef", []map[string]any{
				{"kind": "DELETE", "fields": []any{3}},
			}))
			return true
		case "/v3/sessions/s/operations/o/close":
			closeCalls.Add(1)
			w.WriteHeader(http.StatusOK)
			return true
		}
		return false
	})
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})

	stream, err := client.ExecuteStream(context.Background(), "s", "SELECT * FROM source", StreamOptions{ExecuteOptions: ExecuteOptions{MaxRows: 10}})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	defer stream.Close()
	var kinds []RowKind
	for stream.Next() {
		kinds = append(kinds, stream.Row().Kind)
		if stream.Event().Type != ResultEventRow {
			t.Fatalf("Event() = %+v", stream.Event())
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("Err() = %v", err)
	}
	if len(kinds) != 3 || kinds[0] != RowInsert || kinds[2] != RowDelete {
		t.Fatalf("row kinds = %v", kinds)
	}
	if stream.JobID() != "0123456789abcdef0123456789abcdef" || fetchCalls.Load() != 2 || closeCalls.Load() != 1 {
		t.Fatalf("job=%q fetch=%d close=%d", stream.JobID(), fetchCalls.Load(), closeCalls.Load())
	}
	if stream.Event().Type != ResultEventEOS {
		t.Fatalf("final Event() = %+v", stream.Event())
	}
}

func TestResultStreamEarlyCloseCancelsAndCloses(t *testing.T) {
	var cancelCalls atomic.Int32
	var closeCalls atomic.Int32
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v3/sessions/s/operations/o/result/0":
			writeTestJSON(t, w, resultPageFixture("PAYLOAD", "/v3/sessions/s/operations/o/result/1", "", []map[string]any{{"kind": "INSERT", "fields": []any{1}}}))
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
	client := newTestClient(t, Config{BaseURL: server.URL})
	stream, err := client.ExecuteStream(context.Background(), "s", "SELECT * FROM source", StreamOptions{})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	if !stream.Next() {
		t.Fatalf("Next() = false, err=%v", stream.Err())
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if cancelCalls.Load() != 1 || closeCalls.Load() != 1 || stream.Next() {
		t.Fatalf("cancel=%d close=%d", cancelCalls.Load(), closeCalls.Load())
	}
}

func TestResultStreamRowLimitAndContextCancellation(t *testing.T) {
	t.Run("row limit", func(t *testing.T) {
		var cancelCalls atomic.Int32
		server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
			switch r.URL.Path {
			case "/v3/sessions/s/operations/o/result/0":
				writeTestJSON(t, w, resultPageFixture("EOS", "", "", []map[string]any{
					{"kind": "INSERT", "fields": []any{1}},
					{"kind": "INSERT", "fields": []any{2}},
				}))
				return true
			case "/v3/sessions/s/operations/o/cancel":
				cancelCalls.Add(1)
				w.WriteHeader(http.StatusOK)
				return true
			case "/v3/sessions/s/operations/o/close":
				w.WriteHeader(http.StatusOK)
				return true
			}
			return false
		})
		defer server.Close()
		client := newTestClient(t, Config{BaseURL: server.URL})
		stream, err := client.ExecuteStream(context.Background(), "s", "SELECT * FROM source", StreamOptions{ExecuteOptions: ExecuteOptions{MaxRows: 1}})
		if err != nil || !stream.Next() || stream.Next() || !errors.Is(stream.Err(), ErrResultLimit) {
			t.Fatalf("iterator state: create=%v err=%v cancel=%d", err, stream.Err(), cancelCalls.Load())
		}
		if cancelCalls.Load() != 1 {
			t.Fatalf("cancel calls = %d", cancelCalls.Load())
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var cancelCalls atomic.Int32
		server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
			switch r.URL.Path {
			case "/v3/sessions/s/operations/o/result/0":
				cancel()
				writeTestJSON(t, w, map[string]any{"resultType": "NOT_READY"})
				return true
			case "/v3/sessions/s/operations/o/cancel":
				cancelCalls.Add(1)
				w.WriteHeader(http.StatusOK)
				return true
			case "/v3/sessions/s/operations/o/close":
				w.WriteHeader(http.StatusOK)
				return true
			}
			return false
		})
		defer server.Close()
		client := newTestClient(t, Config{BaseURL: server.URL, CancelOnContextDone: true, PollInterval: time.Millisecond})
		stream, err := client.ExecuteStream(ctx, "s", "SELECT * FROM source", StreamOptions{})
		if err != nil {
			t.Fatalf("ExecuteStream() error = %v", err)
		}
		if stream.Next() || !errors.Is(stream.Err(), context.Canceled) || cancelCalls.Load() != 1 {
			t.Fatalf("Next/Err/cancel = %v, %v, %d", stream.Next(), stream.Err(), cancelCalls.Load())
		}
	})
}

func TestExecutionErrorPreservesPrimaryAndCleanupFailures(t *testing.T) {
	cancelFailure := errors.New("cancel cleanup failed")
	closeFailure := errors.New("close cleanup failed")
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v3/sessions/s/operations/o/result/0":
			writeTestJSON(t, w, resultPageFixture("EOS", "", "", []map[string]any{
				{"kind": "INSERT", "fields": []any{1}},
				{"kind": "INSERT", "fields": []any{2}},
			}))
			return true
		case "/v3/sessions/s/operations/o/cancel":
			http.Error(w, cancelFailure.Error(), http.StatusInternalServerError)
			return true
		case "/v3/sessions/s/operations/o/close":
			http.Error(w, closeFailure.Error(), http.StatusInternalServerError)
			return true
		}
		return false
	})
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})
	_, err := client.ExecuteAndWait(context.Background(), "s", "SELECT * FROM source", ExecuteOptions{MaxRows: 1})
	if !errors.Is(err, ErrResultLimit) {
		t.Fatalf("ExecuteAndWait() error = %v", err)
	}
	var executionErr *ExecutionError
	if !errors.As(err, &executionErr) || executionErr.CancelError == nil || executionErr.CloseError == nil {
		t.Fatalf("ExecutionError = %#v", executionErr)
	}
}

func TestResultStreamSnapshotsCannotChangeInternalRoutesOrRows(t *testing.T) {
	var fetchCalls atomic.Int32
	var closeCalls atomic.Int32
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v3/sessions/s/operations/o/result/0":
			fetchCalls.Add(1)
			writeTestJSON(t, w, resultPageFixture("EOS", "", "", []map[string]any{{"kind": "INSERT", "fields": []any{1}}}))
			return true
		case "/v3/sessions/s/operations/o/close":
			closeCalls.Add(1)
			w.WriteHeader(http.StatusOK)
			return true
		}
		return false
	})
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})

	stream, err := client.ExecuteStream(context.Background(), "s", "SELECT 1", StreamOptions{})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	operationEvent := stream.Event()
	operationEvent.Operation.Handle = "changed"
	if !stream.Next() {
		t.Fatalf("Next() = false, error = %v", stream.Err())
	}
	row := stream.Row()
	row.Fields[0][0] = '9'
	rowEvent := stream.Event()
	if string(rowEvent.Row.Fields[0]) != "1" {
		t.Fatalf("Event row changed through Row() = %s", rowEvent.Row.Fields[0])
	}
	rowEvent.Row.Fields[0][0] = '8'
	if got := stream.Row(); string(got.Fields[0]) != "1" {
		t.Fatalf("Row() changed through Event() = %s", got.Fields[0])
	}
	if stream.Next() || stream.Err() != nil {
		t.Fatalf("final Next/Err = true, %v", stream.Err())
	}
	if fetchCalls.Load() != 1 || closeCalls.Load() != 1 {
		t.Fatalf("fetch=%d close=%d", fetchCalls.Load(), closeCalls.Load())
	}
}
