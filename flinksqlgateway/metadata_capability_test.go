package flinksqlgateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestMetadataHelpersQuoteIdentifiersAndDecodeResults(t *testing.T) {
	var mu sync.Mutex
	nextOperation := 0
	operationRows := map[string][][]any{}
	statements := map[string][][]any{
		"SHOW CATALOGS":                        {{"cat1"}, {"cat2"}},
		"SHOW DATABASES IN `cat``alog`":        {{"db1"}},
		"SHOW TABLES IN `cat``alog`.`db name`": {{"orders"}},
		"SHOW VIEWS":                           {{"active_orders"}},
		"SHOW FUNCTIONS":                       {{"COUNT"}, {"custom_fn"}},
		"DESCRIBE `cat``alog`.`db name`.`orders; DROP TABLE secrets`": {
			{"id", "BIGINT", "FALSE", "PRI(id)", "", "", "primary key"},
			{"amount", "DECIMAL(38,9)", "TRUE", "", "", "", ""},
		},
		"EXPLAIN PLAN FOR SELECT * FROM orders": {{"== Abstract Syntax Tree =="}, {"TableScan"}},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions/s/statements":
			var body struct {
				Statement string `json:"statement"`
			}
			decodeTestJSON(t, r, &body)
			rows, ok := statements[body.Statement]
			if !ok {
				t.Errorf("unexpected metadata statement %q", body.Statement)
				writeTestJSONStatus(t, w, http.StatusBadRequest, map[string]string{"message": "unexpected statement"})
				return
			}
			mu.Lock()
			nextOperation++
			handle := "metadata-op-" + string(rune('0'+nextOperation))
			operationRows[handle] = rows
			mu.Unlock()
			writeTestJSON(t, w, map[string]string{"operationHandle": handle})
		case strings.Contains(r.URL.Path, "/operations/") && strings.HasSuffix(r.URL.Path, "/result/0"):
			parts := strings.Split(r.URL.Path, "/")
			handle := parts[5]
			mu.Lock()
			rows := operationRows[handle]
			mu.Unlock()
			writeTestJSON(t, w, metadataResultPage(rows))
		case strings.HasSuffix(r.URL.Path, "/close"):
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})
	ctx := context.Background()

	catalogs, err := client.ListCatalogs(ctx, "s")
	if err != nil || strings.Join(catalogs, ",") != "cat1,cat2" {
		t.Fatalf("ListCatalogs() = %v, %v", catalogs, err)
	}
	databases, err := client.ListDatabases(ctx, "s", "cat`alog")
	if err != nil || strings.Join(databases, ",") != "db1" {
		t.Fatalf("ListDatabases() = %v, %v", databases, err)
	}
	tables, err := client.ListTables(ctx, "s", Identifier{Catalog: "cat`alog", Database: "db name"})
	if err != nil || len(tables) != 1 || tables[0].Name != "orders" || tables[0].Kind != TableKindTable {
		t.Fatalf("ListTables() = %+v, %v", tables, err)
	}
	views, err := client.ListViews(ctx, "s", Identifier{})
	if err != nil || len(views) != 1 || views[0].Kind != TableKindView {
		t.Fatalf("ListViews() = %+v, %v", views, err)
	}
	functions, err := client.ListFunctions(ctx, "s")
	if err != nil || strings.Join(functions, ",") != "COUNT,custom_fn" {
		t.Fatalf("ListFunctions() = %v, %v", functions, err)
	}
	columns, err := client.DescribeTable(ctx, "s", Identifier{Catalog: "cat`alog", Database: "db name", Object: "orders; DROP TABLE secrets"})
	if err != nil || len(columns) != 2 || columns[0].Nullable || !columns[1].Nullable || columns[0].Comment != "primary key" {
		t.Fatalf("DescribeTable() = %+v, %v", columns, err)
	}
	explanation, err := client.Explain(ctx, "s", "SELECT * FROM orders")
	if err != nil || explanation != "== Abstract Syntax Tree ==\nTableScan" {
		t.Fatalf("Explain() = %q, %v", explanation, err)
	}
}

func TestQuoteIdentifierAndScopeValidation(t *testing.T) {
	quoted, err := QuoteIdentifier("카탈`로그; DROP TABLE x")
	if err != nil || quoted != "`카탈``로그; DROP TABLE x`" {
		t.Fatalf("QuoteIdentifier() = %q, %v", quoted, err)
	}
	if _, err := QuoteIdentifier(""); err == nil {
		t.Fatal("QuoteIdentifier(empty) succeeded")
	}
	client := &GatewayClient{}
	if _, err := client.ListTables(context.Background(), "s", Identifier{Catalog: "catalog"}); err == nil {
		t.Fatal("ListTables() accepted catalog without database")
	}
	if _, err := qualifiedObject(Identifier{Object: ""}); err == nil {
		t.Fatal("qualifiedObject() accepted empty object")
	}
}

func TestCapabilitiesAreVersionSpecificAndFutureConservative(t *testing.T) {
	tests := []struct {
		version      string
		configure    bool
		complete     bool
		rowFormat    bool
		materialized bool
	}{
		{"v1", false, false, false, false},
		{"v2", true, true, true, false},
		{"v3", true, true, true, true},
		{"v4", false, false, false, false},
	}
	for _, test := range tests {
		capabilities := capabilitiesForVersion(test.version)
		if capabilities.APIVersion != test.version || capabilities.ConfigureSession != test.configure || capabilities.CompleteStatement != test.complete || capabilities.RowFormat != test.rowFormat || capabilities.MaterializedTable != test.materialized {
			t.Errorf("capabilitiesForVersion(%q) = %+v", test.version, capabilities)
		}
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api_versions" {
			writeTestJSON(t, w, map[string]any{"versions": []string{"V4"}})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL, APIVersion: "v4"})
	capabilities, err := client.Capabilities(context.Background())
	if err != nil || capabilities.APIVersion != "v4" || capabilities.ConfigureSession {
		t.Fatalf("Capabilities() = %+v, %v", capabilities, err)
	}
}

func metadataResultPage(rows [][]any) map[string]any {
	data := make([]map[string]any, len(rows))
	maxFields := 1
	for index, fields := range rows {
		data[index] = map[string]any{"kind": "INSERT", "fields": fields}
		if len(fields) > maxFields {
			maxFields = len(fields)
		}
	}
	columns := make([]map[string]any, maxFields)
	for index := range columns {
		columns[index] = map[string]any{"name": "field", "logicalType": map[string]any{"type": "STRING", "nullable": true}, "comment": nil}
	}
	return map[string]any{
		"resultType":    "EOS",
		"isQueryResult": true,
		"resultKind":    "SUCCESS_WITH_CONTENT",
		"results": map[string]any{
			"columns":   columns,
			"rowFormat": "JSON",
			"data":      data,
		},
	}
}
