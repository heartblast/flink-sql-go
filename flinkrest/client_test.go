package flinkrest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

const testJobID = "0123456789abcdef0123456789abcdef"

func TestClientJobAPIsFollowFlink120Contract(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test") != "value" {
			t.Errorf("X-Test header = %q", r.Header.Get("X-Test"))
		}
		calls.Add(1)
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/proxy/jobs/"+testJobID:
			writeJSON(t, w, http.StatusOK, map[string]any{"jid": testJobID, "name": "sql-job", "state": "RUNNING", "start-time": 10, "future": true})
		case r.Method == http.MethodGet && r.URL.Path == "/proxy/jobs/"+testJobID+"/status":
			writeJSON(t, w, http.StatusOK, map[string]string{"status": "RUNNING"})
		case r.Method == http.MethodPatch && r.URL.Path == "/proxy/jobs/"+testJobID:
			if r.URL.Query().Get("mode") != "cancel" {
				t.Errorf("cancel query = %q", r.URL.RawQuery)
			}
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodPost && r.URL.Path == "/proxy/jobs/"+testJobID+"/stop":
			var body struct {
				Drain           bool   `json:"drain"`
				FormatType      string `json:"formatType"`
				TargetDirectory string `json:"targetDirectory"`
				TriggerID       string `json:"triggerId"`
			}
			decodeJSON(t, r, &body)
			if !body.Drain || body.FormatType != "NATIVE" || body.TargetDirectory != "file:///savepoints" || body.TriggerID != "trigger-1" {
				t.Errorf("stop body = %+v", body)
			}
			writeJSON(t, w, http.StatusAccepted, map[string]string{"request-id": "request-1"})
		case r.Method == http.MethodGet && r.URL.Path == "/proxy/jobs/"+testJobID+"/exceptions":
			writeJSON(t, w, http.StatusOK, map[string]any{"root-exception": "failure", "timestamp": 12, "exceptionHistory": map[string]any{"truncated": false}})
		case r.Method == http.MethodGet && r.URL.Path == "/proxy/jobs/"+testJobID+"/checkpoints":
			writeJSON(t, w, http.StatusOK, map[string]any{"counts": map[string]any{"total": 2, "completed": 1}, "history": []map[string]any{{"id": 7, "status": "COMPLETED", "is_savepoint": false}}})
		case r.Method == http.MethodGet && r.URL.Path == "/proxy/jobs/"+testJobID+"/plan":
			writeJSON(t, w, http.StatusOK, map[string]any{"plan": map[string]any{"jid": testJobID, "nodes": []any{}}})
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := NewClient(Config{BaseURL: server.URL + "/proxy", Headers: map[string]string{"X-Test": "value"}, ValidateJobID: true})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	ctx := context.Background()
	job, err := client.GetJob(ctx, testJobID)
	if err != nil || job.JobID != testJobID || job.State != JobRunning || !json.Valid(job.Raw) {
		t.Fatalf("GetJob() = %+v, %v", job, err)
	}
	status, err := client.GetJobStatus(ctx, testJobID)
	if err != nil || status != JobRunning {
		t.Fatalf("GetJobStatus() = %s, %v", status, err)
	}
	if err := client.CancelJob(ctx, testJobID); err != nil {
		t.Fatalf("CancelJob() error = %v", err)
	}
	trigger, err := client.StopJob(ctx, testJobID, StopOptions{Drain: true, FormatType: SavepointNative, TargetDirectory: "file:///savepoints", TriggerID: "trigger-1"})
	if err != nil || trigger.RequestID != "request-1" || !json.Valid(trigger.Raw) {
		t.Fatalf("StopJob() = %+v, %v", trigger, err)
	}
	exceptions, err := client.GetJobExceptions(ctx, testJobID)
	if err != nil || exceptions.RootException != "failure" || len(exceptions.ExceptionHistory) == 0 {
		t.Fatalf("GetJobExceptions() = %+v, %v", exceptions, err)
	}
	checkpoints, err := client.GetCheckpoints(ctx, testJobID)
	if err != nil || checkpoints.Counts.Total != 2 || len(checkpoints.History) != 1 || checkpoints.History[0].ID != 7 {
		t.Fatalf("GetCheckpoints() = %+v, %v", checkpoints, err)
	}
	plan, err := client.GetJobPlan(ctx, testJobID)
	if err != nil || !json.Valid(plan.Plan) || !json.Valid(plan.Raw) {
		t.Fatalf("GetJobPlan() = %+v, %v", plan, err)
	}
	if calls.Load() != 7 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

func TestClientValidationLimitsAndClose(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"jid":"0123456789abcdef0123456789abcdef","padding":"abcdefghijklmnopqrstuvwxyz"}`)
	}))
	defer server.Close()
	client, err := NewClient(Config{BaseURL: server.URL, ValidateJobID: true, MaxResponseBytes: 16})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	if _, err := client.GetJob(context.Background(), "bad"); !errors.Is(err, ErrInvalidJobID) {
		t.Fatalf("invalid job ID error = %v", err)
	}
	if _, err := client.GetJob(context.Background(), testJobID); !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("response limit error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := client.GetJobStatus(context.Background(), testJobID); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("request after Close error = %v", err)
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, status int, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Errorf("Encode() error = %v", err)
	}
}

func decodeJSON(t *testing.T, request *http.Request, destination any) {
	t.Helper()
	defer request.Body.Close()
	if err := json.NewDecoder(request.Body).Decode(destination); err != nil {
		t.Errorf("Decode() error = %v", err)
	}
}
