package flinksqlgateway

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"unicode/utf8"
)

func FuzzParseResultPage(f *testing.F) {
	f.Add([]byte(`{"resultType":"EOS","results":{"columns":[],"data":[]}}`))
	f.Add([]byte(`{"resultType":"PAYLOAD","nextResultUri":"?token=1&token=2"}`))
	f.Add([]byte{0xff, '{', '}'})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		var page ResultPage
		if err := json.Unmarshal(data, &page); err == nil && !json.Valid(page.Raw) {
			t.Fatalf("valid page did not preserve valid raw JSON")
		}
	})
}

func FuzzResolveNextResultURI(f *testing.F) {
	client, err := NewClient(Config{BaseURL: "https://flink.example/gateway"})
	if err != nil {
		f.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()
	f.Add("/v3/sessions/s/operations/o/result/1")
	f.Add("?token=1&token=2")
	f.Add("https://evil.example/result")
	f.Add("")
	f.Add(" ")
	f.Add("opaque/handle")
	f.Add("percent%2Fhandle")
	f.Add(string([]byte{0xff}))
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 1<<16 {
			t.Skip()
		}
		_, _ = client.validateNextResultURI(value)
		_ = validateOpaqueHandle("fuzz", value)
	})
}

func FuzzParseAPIError(f *testing.F) {
	client, err := NewClient(Config{BaseURL: "https://flink.example"})
	if err != nil {
		f.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()
	target, err := url.Parse("https://flink.example/v3/sessions/session/operations/operation")
	if err != nil {
		f.Fatalf("url.Parse() error = %v", err)
	}
	f.Add([]byte(`{"message":"실패🙂"}`), http.StatusInternalServerError)
	f.Add([]byte{0xff, 0xfe, '\n'}, http.StatusBadRequest)
	f.Fuzz(func(t *testing.T, data []byte, status int) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		apiErr := client.decodeAPIError(http.MethodPost, target, status, data)
		if !utf8.ValidString(apiErr.Message) {
			t.Fatalf("invalid UTF-8 API error message")
		}
	})
}

func FuzzDecodeRow(f *testing.F) {
	f.Add([]byte(`{"kind":"INSERT","fields":[{"known":1,"future":true}]}`))
	f.Add([]byte(`{"kind":"FUTURE","fields":[null]}`))
	f.Add([]byte{0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		var row Row
		if json.Unmarshal(data, &row) != nil || len(row.Fields) == 0 {
			return
		}
		column := ColumnInfo{Name: "value", LogicalType: LogicalType{Type: "ROW", Fields: []LogicalTypeField{{Name: "known", LogicalType: &LogicalType{Type: "INTEGER"}}}}}
		_, _, _ = row.WithColumns([]ColumnInfo{column}, nil).ValueAt(0)
	})
}

func FuzzQuoteIdentifier(f *testing.F) {
	f.Add("catalog.table")
	f.Add("with`quote")
	f.Add("")
	f.Add(string([]byte{0xff}))
	f.Fuzz(func(t *testing.T, identifier string) {
		if len(identifier) > 1<<16 {
			t.Skip()
		}
		quoted, err := QuoteIdentifier(identifier)
		if err == nil && quoted == "" {
			t.Fatalf("successful quote returned an empty identifier")
		}
	})
}
