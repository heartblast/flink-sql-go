package flinksqlgateway

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

// 소스 호환성을 위해 기존의 두 field Row 표현을 보존한다.
var _ = Row{RowInsert, nil}

func TestDefaultValueDecoderScalarAndTemporalTypes(t *testing.T) {
	decoder := DefaultValueDecoder{}
	tests := []struct {
		name     string
		typeName string
		raw      string
		assert   func(t *testing.T, value any)
	}{
		{"boolean", "BOOLEAN", `true`, func(t *testing.T, value any) {
			if value != true {
				t.Fatalf("value = %#v", value)
			}
		}},
		{"integer", "BIGINT", `9223372036854775807`, func(t *testing.T, value any) {
			if value != int64(9223372036854775807) {
				t.Fatalf("value = %#v", value)
			}
		}},
		{"double", "DOUBLE", `1.25`, func(t *testing.T, value any) {
			if value != 1.25 {
				t.Fatalf("value = %#v", value)
			}
		}},
		{"decimal precision", "DECIMAL", `"12345678901234567890123456789012345678.123456789"`, func(t *testing.T, value any) {
			if value.(Decimal).String() != "12345678901234567890123456789012345678.123456789" {
				t.Fatalf("value = %#v", value)
			}
		}},
		{"string", "STRING", `"service"`, func(t *testing.T, value any) {
			if value != "service" {
				t.Fatalf("value = %#v", value)
			}
		}},
		{"date", "DATE", `"2026-07-24"`, func(t *testing.T, value any) {
			if value.(LocalDate).Time.Format("2006-01-02") != "2026-07-24" {
				t.Fatalf("value = %#v", value)
			}
		}},
		{"time", "TIME", `"12:34:56.123"`, func(t *testing.T, value any) {
			if value.(LocalTime).Time.Hour() != 12 {
				t.Fatalf("value = %#v", value)
			}
		}},
		{"timestamp", "TIMESTAMP", `"2026-07-24 12:34:56.123"`, func(t *testing.T, value any) {
			if _, ok := value.(LocalTimestamp); !ok {
				t.Fatalf("value type = %T", value)
			}
		}},
		{"timestamp ltz", "TIMESTAMP_LTZ", `"2026-07-24T12:34:56+09:00"`, func(t *testing.T, value any) {
			if got := value.(TimestampLTZ).Time.UTC().Hour(); got != 3 {
				t.Fatalf("UTC hour = %d", got)
			}
		}},
		{"binary", "VARBINARY", `"aGVsbG8="`, func(t *testing.T, value any) {
			if string(value.([]byte)) != "hello" {
				t.Fatalf("value = %#v", value)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, err := decoder.Decode(Column{Name: test.name, LogicalType: LogicalType{Type: test.typeName}}, json.RawMessage(test.raw))
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			test.assert(t, value)
		})
	}

	value, err := decoder.Decode(Column{Name: "future", LogicalType: LogicalType{Type: "FUTURE_TYPE"}}, json.RawMessage(`{"x":1}`))
	if err != nil || string(value.(json.RawMessage)) != `{"x":1}` {
		t.Fatalf("unknown Decode() = %#v, %v", value, err)
	}
	value, err = decoder.Decode(Column{Name: "nullable", LogicalType: LogicalType{Type: "STRING"}}, json.RawMessage(`null`))
	if err != nil || value != nil {
		t.Fatalf("NULL Decode() = %#v, %v", value, err)
	}
}

func TestDefaultValueDecoderNestedTypes(t *testing.T) {
	integer := &LogicalType{Type: "INTEGER"}
	stringType := &LogicalType{Type: "STRING"}
	decoder := DefaultValueDecoder{}

	array, err := decoder.Decode(Column{Name: "array", LogicalType: LogicalType{Type: "ARRAY", ElementType: integer}}, json.RawMessage(`[1,2,3]`))
	if err != nil || !reflect.DeepEqual(array, []any{int64(1), int64(2), int64(3)}) {
		t.Fatalf("array = %#v, %v", array, err)
	}
	mapValue, err := decoder.Decode(Column{Name: "map", LogicalType: LogicalType{Type: "MAP", ValueType: integer}}, json.RawMessage(`{"one":1,"two":2}`))
	if err != nil || mapValue.(map[string]any)["two"] != int64(2) {
		t.Fatalf("map = %#v, %v", mapValue, err)
	}
	rowType := LogicalType{Type: "ROW", Fields: []LogicalTypeField{
		{Name: "id", LogicalType: integer},
		{Name: "name", LogicalType: stringType},
	}}
	rowValue, err := decoder.Decode(Column{Name: "nested", LogicalType: rowType}, json.RawMessage(`{"id":7,"name":"seven"}`))
	if err != nil || rowValue.(map[string]any)["id"] != int64(7) || rowValue.(map[string]any)["name"] != "seven" {
		t.Fatalf("row = %#v, %v", rowValue, err)
	}
}

func TestRowAccessorDeepCopiesSchemaAndPreservesUnknownNestedFields(t *testing.T) {
	integer := &LogicalType{Type: "INTEGER", Raw: json.RawMessage(`{"type":"INTEGER"}`)}
	columns := []ColumnInfo{{
		Name: "nested",
		LogicalType: LogicalType{Type: "ROW", Fields: []LogicalTypeField{
			{Name: "id", LogicalType: integer},
		}},
	}}
	row := Row{Kind: RowInsert, Fields: []json.RawMessage{json.RawMessage(`{"id":7,"future":{"x":1}}`)}}
	accessor := row.WithColumns(columns, nil)

	columns[0].Name = "changed"
	columns[0].LogicalType.Fields[0].Name = "changed"
	integer.Type = "STRING"
	integer.Raw[2] = 'X'
	row.Fields[0][2] = 'X'

	value, null, err := accessor.Value("nested")
	if err != nil || null {
		t.Fatalf("Value() = %#v, %v, %v", value, null, err)
	}
	nested, ok := value.(map[string]any)
	if !ok || nested["id"] != int64(7) {
		t.Fatalf("nested value = %#v", value)
	}
	unknown, ok := nested["future"].(json.RawMessage)
	if !ok || string(unknown) != `{"x":1}` {
		t.Fatalf("unknown field = %#v", nested["future"])
	}
}

func TestRowHelpersSupportNamesIndexesDuplicatesAndNull(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "name", LogicalType: LogicalType{Type: "STRING"}},
		{Name: "code", LogicalType: LogicalType{Type: "INTEGER"}},
		{Name: "enabled", LogicalType: LogicalType{Type: "BOOLEAN"}},
		{Name: "amount", LogicalType: LogicalType{Type: "DECIMAL"}},
		{Name: "event_time", LogicalType: LogicalType{Type: "TIMESTAMP_LTZ"}},
		{Name: "name", LogicalType: LogicalType{Type: "STRING"}},
		{Name: "nullable", LogicalType: LogicalType{Type: "STRING"}},
	}
	row := Row{Kind: RowInsert, Fields: []json.RawMessage{
		json.RawMessage(`"first"`),
		json.RawMessage(`200`),
		json.RawMessage(`true`),
		json.RawMessage(`"99999999999999999999.0001"`),
		json.RawMessage(`"2026-07-24T00:00:00Z"`),
		json.RawMessage(`"second"`),
		json.RawMessage(`null`),
	}}.WithColumns(columns, nil)

	name, null, err := row.String("name")
	if err != nil || null || name != "first" {
		t.Fatalf("String() = %q, %v, %v", name, null, err)
	}
	code, _, err := row.Int64("code")
	if err != nil || code != 200 {
		t.Fatalf("Int64() = %d, %v", code, err)
	}
	enabled, _, err := row.Bool("enabled")
	if err != nil || !enabled {
		t.Fatalf("Bool() = %v, %v", enabled, err)
	}
	amount, _, err := row.Decimal("amount")
	if err != nil || amount.String() != "99999999999999999999.0001" {
		t.Fatalf("Decimal() = %s, %v", amount, err)
	}
	eventTime, _, err := row.Time("event_time")
	if err != nil || !eventTime.Equal(time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("Time() = %s, %v", eventTime, err)
	}
	raw, isNull, err := row.Raw("nullable")
	if err != nil || !isNull || string(raw) != "null" {
		t.Fatalf("Raw() = %s, %v, %v", raw, isNull, err)
	}
	value, isNull, err := row.ValueAt(6)
	if err != nil || !isNull || value != nil {
		t.Fatalf("ValueAt() = %#v, %v, %v", value, isNull, err)
	}
}

func TestResultPageRowsUseExplicitAccessor(t *testing.T) {
	var page ResultPage
	if err := json.Unmarshal([]byte(`{
        "resultType":"EOS",
        "results":{
          "columns":[{"name":"value","logicalType":{"type":"INTEGER","nullable":false},"comment":null}],
          "rowFormat":"JSON",
          "data":[{"kind":"INSERT","fields":[42]}]
        }
    }`), &page); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	accessor := page.Results.Data[0].WithColumns(page.Results.Columns, nil)
	value, _, err := accessor.Int64("value")
	if err != nil || value != 42 {
		t.Fatalf("Int64() = %d, %v", value, err)
	}
}
