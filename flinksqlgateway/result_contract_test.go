package flinksqlgateway

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestExecuteAndWaitPreservesJSONValuesAndSQLNull(t *testing.T) {
	t.Parallel()
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v3/sessions/s/operations/o/result/0":
			writeTestJSON(t, w, map[string]any{
				"resultType":    "EOS",
				"isQueryResult": true,
				"resultKind":    "SUCCESS_WITH_CONTENT",
				"results": map[string]any{
					"columns": []map[string]any{
						{"name": "id", "logicalType": map[string]any{"type": "INTEGER", "nullable": false}, "comment": nil},
						{"name": "name", "logicalType": map[string]any{"type": "VARCHAR", "nullable": true}, "comment": nil},
						{"name": "note", "logicalType": map[string]any{"type": "VARCHAR", "nullable": true}, "comment": nil},
					},
					"rowFormat": "JSON",
					"data":      []map[string]any{{"kind": "INSERT", "fields": []any{42, "flink", nil}}},
				},
			})
			return true
		case "/v3/sessions/s/operations/o/close":
			writeTestJSON(t, w, map[string]string{"status": "CLOSED"})
			return true
		}
		return false
	})
	defer server.Close()

	client := newTestClient(t, Config{BaseURL: server.URL, PollInterval: time.Millisecond})
	result, err := client.ExecuteAndWait(context.Background(), "s", "SELECT 42, 'flink', NULL", ExecuteOptions{})
	if err != nil {
		t.Fatalf("ExecuteAndWait() error = %v", err)
	}
	if len(result.Columns) != 3 || len(result.Rows) != 1 {
		t.Fatalf("result shape = columns %d, rows %d", len(result.Columns), len(result.Rows))
	}
	accessor := result.Rows[0].WithColumns(result.Columns, nil)
	id, null, err := accessor.Int64("id")
	if err != nil || null || id != 42 {
		t.Fatalf("id = %d, null = %v, error = %v", id, null, err)
	}
	name, null, err := accessor.String("name")
	if err != nil || null || name != "flink" {
		t.Fatalf("name = %q, null = %v, error = %v", name, null, err)
	}
	note, null, err := accessor.String("note")
	if err != nil || !null || note != "" {
		t.Fatalf("note = %q, null = %v, error = %v", note, null, err)
	}
	if got := string(result.Rows[0].Fields[1]); got != `"flink"` {
		t.Fatalf("raw name = %s", got)
	}
}

func TestExecuteAndWaitDecodesPlainTextValues(t *testing.T) {
	t.Parallel()
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v3/sessions/s/operations/o/result/0":
			if got := r.URL.Query().Get("rowFormat"); got != string(RowFormatPlainText) {
				t.Errorf("rowFormat = %q", got)
			}
			writeTestJSON(t, w, map[string]any{
				"resultType": "EOS",
				"results": map[string]any{
					"columns": []map[string]any{
						{"name": "id", "logicalType": map[string]any{"type": "INTEGER", "nullable": false}, "comment": nil},
						{"name": "name", "logicalType": map[string]any{"type": "VARCHAR", "nullable": false}, "comment": nil},
					},
					"rowFormat": "PLAIN_TEXT",
					"data":      []map[string]any{{"kind": "INSERT", "fields": []any{"42", "flink"}}},
				},
			})
			return true
		case "/v3/sessions/s/operations/o/close":
			writeTestJSON(t, w, map[string]string{"status": "CLOSED"})
			return true
		}
		return false
	})
	defer server.Close()

	client := newTestClient(t, Config{BaseURL: server.URL, PollInterval: time.Millisecond})
	result, err := client.ExecuteAndWait(context.Background(), "s", "SELECT 42, 'flink'", ExecuteOptions{RowFormat: RowFormatPlainText})
	if err != nil {
		t.Fatalf("ExecuteAndWait() error = %v", err)
	}
	accessor := result.Rows[0].WithColumns(result.Columns, nil)
	id, null, err := accessor.Int64("id")
	if err != nil || null || id != 42 {
		t.Fatalf("id = %d, null = %v, error = %v", id, null, err)
	}
	name, null, err := accessor.String("name")
	if err != nil || null || name != "flink" {
		t.Fatalf("name = %q, null = %v, error = %v", name, null, err)
	}
}

func TestExecuteAndWaitCarriesColumnsAcrossPages(t *testing.T) {
	t.Parallel()
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v3/sessions/s/operations/o/result/0":
			writeTestJSON(t, w, map[string]any{
				"resultType":    "PAYLOAD",
				"nextResultUri": "/v3/sessions/s/operations/o/result/1?rowFormat=JSON",
				"results": map[string]any{
					"columns":   []map[string]any{{"name": "value", "logicalType": map[string]any{"type": "INTEGER", "nullable": false}, "comment": nil}},
					"rowFormat": "JSON",
					"data":      []any{},
				},
			})
			return true
		case "/v3/sessions/s/operations/o/result/1":
			writeTestJSON(t, w, map[string]any{
				"resultType": "EOS",
				"results": map[string]any{
					"rowFormat": "JSON",
					"data":      []map[string]any{{"kind": "INSERT", "fields": []any{7}}},
				},
			})
			return true
		case "/v3/sessions/s/operations/o/close":
			writeTestJSON(t, w, map[string]string{"status": "CLOSED"})
			return true
		}
		return false
	})
	defer server.Close()

	client := newTestClient(t, Config{BaseURL: server.URL, PollInterval: time.Millisecond})
	result, err := client.ExecuteAndWait(context.Background(), "s", "SELECT 7", ExecuteOptions{})
	if err != nil {
		t.Fatalf("ExecuteAndWait() error = %v", err)
	}
	value, null, err := result.Rows[0].WithColumns(result.Columns, nil).Int64("value")
	if err != nil || null || value != 7 {
		t.Fatalf("value = %d, null = %v, error = %v", value, null, err)
	}
}

func TestExecuteAndWaitRejectsRowWithMissingField(t *testing.T) {
	t.Parallel()
	server := executionServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v3/sessions/s/operations/o/result/0":
			writeTestJSON(t, w, map[string]any{
				"resultType": "EOS",
				"results": map[string]any{
					"columns": []map[string]any{
						{"name": "first", "logicalType": map[string]any{"type": "INTEGER", "nullable": false}, "comment": nil},
						{"name": "missing", "logicalType": map[string]any{"type": "VARCHAR", "nullable": true}, "comment": nil},
					},
					"rowFormat": "JSON",
					"data":      []map[string]any{{"kind": "INSERT", "fields": []any{1}}},
				},
			})
			return true
		case "/v3/sessions/s/operations/o/close":
			writeTestJSON(t, w, map[string]string{"status": "CLOSED"})
			return true
		}
		return false
	})
	defer server.Close()

	client := newTestClient(t, Config{BaseURL: server.URL, PollInterval: time.Millisecond})
	_, err := client.ExecuteAndWait(context.Background(), "s", "SELECT malformed", ExecuteOptions{})
	if err == nil {
		t.Fatal("ExecuteAndWait() accepted a row with a missing field")
	}
}

func TestDecoderRejectsEmptyFieldInsteadOfTreatingItAsNull(t *testing.T) {
	t.Parallel()
	column := ColumnInfo{Name: "value", LogicalType: LogicalType{Type: "VARCHAR", Nullable: true}}
	decoder := DefaultValueDecoder{}
	if value, err := decoder.Decode(column, json.RawMessage{}); err == nil || value != nil {
		t.Fatalf("Decode(empty) = %#v, %v", value, err)
	}
	accessor := (Row{Kind: RowInsert, Fields: []json.RawMessage{nil}}).WithColumns([]ColumnInfo{column}, nil)
	if value, null, err := accessor.Value("value"); err == nil || null || value != nil {
		t.Fatalf("Value(empty) = %#v, null = %v, error = %v", value, null, err)
	}
	if raw, null, err := accessor.Raw("value"); err == nil || null || raw != nil {
		t.Fatalf("Raw(empty) = %s, null = %v, error = %v", raw, null, err)
	}
}
