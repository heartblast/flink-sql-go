package flinksqlgateway

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ValueDecoder converts a single raw field only when explicitly requested.
type ValueDecoder interface {
	Decode(column Column, raw json.RawMessage) (any, error)
}

// DefaultValueDecoder supports Flink's common scalar, temporal, and nested
// logical types. Unknown future types are returned as json.RawMessage.
type DefaultValueDecoder struct{}

// Decimal retains its exact textual representation without float conversion.
type Decimal string

func (d Decimal) String() string { return string(d) }

// LocalDate represents a DATE without adding an application timezone.
type LocalDate struct {
	Time time.Time
	Text string
}

// LocalTime represents a TIME without a date or timezone.
type LocalTime struct {
	Time time.Time
	Text string
}

// LocalTimestamp represents TIMESTAMP, which has no instant/timezone meaning.
type LocalTimestamp struct {
	Time time.Time
	Text string
}

// TimestampLTZ represents TIMESTAMP_LTZ as an absolute instant.
type TimestampLTZ struct {
	Time time.Time
	Text string
}

// Decode implements ValueDecoder.
func (DefaultValueDecoder) Decode(column Column, raw json.RawMessage) (any, error) {
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) || len(trimmed) == 0 {
		return nil, nil
	}
	typeName := strings.ToUpper(strings.TrimSpace(column.LogicalType.Type))
	switch typeName {
	case "BOOLEAN":
		text, err := scalarText(trimmed)
		if err != nil {
			return nil, decodeError(column, err)
		}
		value, err := strconv.ParseBool(strings.ToLower(text))
		if err != nil {
			return nil, decodeError(column, err)
		}
		return value, nil

	case "TINYINT", "SMALLINT", "INTEGER", "INT", "BIGINT":
		text, err := scalarText(trimmed)
		if err != nil {
			return nil, decodeError(column, err)
		}
		value, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil, decodeError(column, err)
		}
		return value, nil

	case "FLOAT", "DOUBLE":
		text, err := scalarText(trimmed)
		if err != nil {
			return nil, decodeError(column, err)
		}
		value, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return nil, decodeError(column, err)
		}
		return value, nil

	case "DECIMAL":
		text, err := scalarText(trimmed)
		if err != nil {
			return nil, decodeError(column, err)
		}
		if !validJSONNumber(text) {
			return nil, decodeError(column, fmt.Errorf("invalid decimal"))
		}
		return Decimal(text), nil

	case "CHAR", "VARCHAR", "STRING":
		var value string
		if err := json.Unmarshal(trimmed, &value); err != nil {
			return nil, decodeError(column, err)
		}
		return value, nil

	case "DATE":
		text, err := quotedText(trimmed)
		if err != nil {
			return nil, decodeError(column, err)
		}
		value, err := time.Parse("2006-01-02", text)
		if err != nil {
			return nil, decodeError(column, err)
		}
		return LocalDate{Time: value, Text: text}, nil

	case "TIME", "TIME_WITHOUT_TIME_ZONE":
		text, err := quotedText(trimmed)
		if err != nil {
			return nil, decodeError(column, err)
		}
		value, err := parseLocalTime(text)
		if err != nil {
			return nil, decodeError(column, err)
		}
		return LocalTime{Time: value, Text: text}, nil

	case "TIMESTAMP", "TIMESTAMP_WITHOUT_TIME_ZONE":
		text, err := quotedText(trimmed)
		if err != nil {
			return nil, decodeError(column, err)
		}
		value, err := parseLocalTimestamp(text)
		if err != nil {
			return nil, decodeError(column, err)
		}
		return LocalTimestamp{Time: value, Text: text}, nil

	case "TIMESTAMP_LTZ", "TIMESTAMP_WITH_LOCAL_TIME_ZONE":
		text, err := quotedText(trimmed)
		if err != nil {
			return nil, decodeError(column, err)
		}
		value, err := parseTimestampLTZ(text)
		if err != nil {
			return nil, decodeError(column, err)
		}
		return TimestampLTZ{Time: value, Text: text}, nil

	case "BINARY", "VARBINARY", "BYTES":
		text, err := quotedText(trimmed)
		if err != nil {
			return nil, decodeError(column, err)
		}
		value, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			return nil, decodeError(column, err)
		}
		return value, nil

	case "ARRAY":
		return decodeArray(column, trimmed)

	case "MAP", "MULTISET":
		return decodeMap(column, trimmed)

	case "ROW", "STRUCTURED_TYPE":
		return decodeNestedRow(column, trimmed)

	default:
		return append(json.RawMessage(nil), trimmed...), nil
	}
}

// RowAccessor provides schema-aware access to a Row without changing Row's
// wire representation. Duplicate column names use the first occurrence.
type RowAccessor struct {
	row     Row
	columns []ColumnInfo
	decoder ValueDecoder
}

// WithColumns returns a schema-aware accessor for the row. The row and column
// slices are copied so later caller mutations cannot change the accessor.
func (r Row) WithColumns(columns []ColumnInfo, decoder ValueDecoder) RowAccessor {
	r.Fields = cloneRawMessages(r.Fields)
	if decoder == nil {
		decoder = DefaultValueDecoder{}
	}
	return RowAccessor{
		row:     r,
		columns: append([]ColumnInfo(nil), columns...),
		decoder: decoder,
	}
}

// NewRowAccessor is the function form of Row.WithColumns.
func NewRowAccessor(row Row, columns []ColumnInfo, decoder ValueDecoder) RowAccessor {
	return row.WithColumns(columns, decoder)
}

// Row returns a deep copy of the underlying raw row.
func (r RowAccessor) Row() Row {
	result := r.row
	result.Fields = cloneRawMessages(result.Fields)
	return result
}

// Raw returns a copied raw value by column name. The bool reports SQL NULL.
func (r RowAccessor) Raw(name string) (json.RawMessage, bool, error) {
	index, err := r.columnIndex(name)
	if err != nil {
		return nil, false, err
	}
	return r.RawAt(index)
}

// RawAt returns a copied raw value by zero-based column index.
func (r RowAccessor) RawAt(index int) (json.RawMessage, bool, error) {
	if index < 0 || index >= len(r.row.Fields) {
		return nil, false, fmt.Errorf("flinksqlgateway: column index %d is out of range", index)
	}
	raw := append(json.RawMessage(nil), r.row.Fields[index]...)
	return raw, bytes.Equal(bytes.TrimSpace(raw), []byte("null")), nil
}

// Value decodes a field by name.
func (r RowAccessor) Value(name string) (any, bool, error) {
	index, err := r.columnIndex(name)
	if err != nil {
		return nil, false, err
	}
	return r.ValueAt(index)
}

// ValueAt decodes a field by index.
func (r RowAccessor) ValueAt(index int) (any, bool, error) {
	if index < 0 || index >= len(r.row.Fields) || index >= len(r.columns) {
		return nil, false, fmt.Errorf("flinksqlgateway: column index %d is out of range", index)
	}
	raw := r.row.Fields[index]
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, true, nil
	}
	decoder := r.decoder
	if decoder == nil {
		decoder = DefaultValueDecoder{}
	}
	value, err := decoder.Decode(r.columns[index], raw)
	return value, false, err
}

func (r RowAccessor) String(name string) (string, bool, error) {
	value, null, err := r.Value(name)
	if err != nil || null {
		return "", null, err
	}
	result, ok := value.(string)
	if !ok {
		return "", false, typeMismatch(name, "string", value)
	}
	return result, false, nil
}

func (r RowAccessor) Int64(name string) (int64, bool, error) {
	value, null, err := r.Value(name)
	if err != nil || null {
		return 0, null, err
	}
	result, ok := value.(int64)
	if !ok {
		return 0, false, typeMismatch(name, "int64", value)
	}
	return result, false, nil
}

func (r RowAccessor) Bool(name string) (bool, bool, error) {
	value, null, err := r.Value(name)
	if err != nil || null {
		return false, null, err
	}
	result, ok := value.(bool)
	if !ok {
		return false, false, typeMismatch(name, "bool", value)
	}
	return result, false, nil
}

func (r RowAccessor) Decimal(name string) (Decimal, bool, error) {
	value, null, err := r.Value(name)
	if err != nil || null {
		return "", null, err
	}
	result, ok := value.(Decimal)
	if !ok {
		return "", false, typeMismatch(name, "Decimal", value)
	}
	return result, false, nil
}

func (r RowAccessor) Time(name string) (time.Time, bool, error) {
	value, null, err := r.Value(name)
	if err != nil || null {
		return time.Time{}, null, err
	}
	switch decoded := value.(type) {
	case LocalDate:
		return decoded.Time, false, nil
	case LocalTime:
		return decoded.Time, false, nil
	case LocalTimestamp:
		return decoded.Time, false, nil
	case TimestampLTZ:
		return decoded.Time, false, nil
	case time.Time:
		return decoded, false, nil
	default:
		return time.Time{}, false, typeMismatch(name, "temporal value", value)
	}
}

func (r RowAccessor) columnIndex(name string) (int, error) {
	if len(r.columns) == 0 {
		return 0, fmt.Errorf("flinksqlgateway: row has no bound column metadata")
	}
	for index := range r.columns {
		if r.columns[index].Name == name {
			if index >= len(r.row.Fields) {
				return 0, fmt.Errorf("flinksqlgateway: row has no field for column %q", name)
			}
			return index, nil
		}
	}
	return 0, fmt.Errorf("flinksqlgateway: column %q was not found", name)
}

func cloneRawMessages(fields []json.RawMessage) []json.RawMessage {
	result := make([]json.RawMessage, len(fields))
	for index := range fields {
		result[index] = append(json.RawMessage(nil), fields[index]...)
	}
	return result
}

func scalarText(raw []byte) (string, error) {
	if len(raw) > 0 && raw[0] == '"' {
		return quotedText(raw)
	}
	return string(raw), nil
}

func quotedText(raw []byte) (string, error) {
	var result string
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", err
	}
	return result, nil
}

func validJSONNumber(value string) bool {
	if !json.Valid([]byte(value)) {
		return false
	}
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return false
	}
	_, ok := decoded.(json.Number)
	return ok
}

func decodeArray(column Column, raw []byte) ([]any, error) {
	var fields []json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, decodeError(column, err)
	}
	result := make([]any, len(fields))
	if column.LogicalType.ElementType == nil {
		for index := range fields {
			result[index] = append(json.RawMessage(nil), fields[index]...)
		}
		return result, nil
	}
	element := Column{LogicalType: *column.LogicalType.ElementType}
	for index := range fields {
		value, err := (DefaultValueDecoder{}).Decode(element, fields[index])
		if err != nil {
			return nil, err
		}
		result[index] = value
	}
	return result, nil
}

func decodeMap(column Column, raw []byte) (any, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return append(json.RawMessage(nil), raw...), nil
	}
	result := make(map[string]any, len(fields))
	if column.LogicalType.ValueType == nil {
		for key, value := range fields {
			result[key] = append(json.RawMessage(nil), value...)
		}
		return result, nil
	}
	valueColumn := Column{LogicalType: *column.LogicalType.ValueType}
	for key, rawValue := range fields {
		value, err := (DefaultValueDecoder{}).Decode(valueColumn, rawValue)
		if err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, nil
}

func decodeNestedRow(column Column, raw []byte) (any, error) {
	if len(raw) > 0 && raw[0] == '{' {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, decodeError(column, err)
		}
		result := make(map[string]any, len(fields))
		for _, field := range column.LogicalType.Fields {
			rawValue, ok := fields[field.Name]
			if !ok {
				continue
			}
			logicalType := field.LogicalType
			if logicalType == nil {
				logicalType = field.Type
			}
			if logicalType == nil {
				result[field.Name] = append(json.RawMessage(nil), rawValue...)
				continue
			}
			value, err := (DefaultValueDecoder{}).Decode(Column{Name: field.Name, LogicalType: *logicalType}, rawValue)
			if err != nil {
				return nil, err
			}
			result[field.Name] = value
		}
		return result, nil
	}
	var fields []json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, decodeError(column, err)
	}
	result := make([]any, len(fields))
	for index := range fields {
		if index >= len(column.LogicalType.Fields) {
			result[index] = append(json.RawMessage(nil), fields[index]...)
			continue
		}
		field := column.LogicalType.Fields[index]
		logicalType := field.LogicalType
		if logicalType == nil {
			logicalType = field.Type
		}
		if logicalType == nil {
			result[index] = append(json.RawMessage(nil), fields[index]...)
			continue
		}
		value, err := (DefaultValueDecoder{}).Decode(Column{Name: field.Name, LogicalType: *logicalType}, fields[index])
		if err != nil {
			return nil, err
		}
		result[index] = value
	}
	return result, nil
}

func parseLocalTime(value string) (time.Time, error) {
	for _, layout := range []string{"15:04:05.999999999", "15:04:05"} {
		if parsed, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid TIME value")
}

func parseLocalTimestamp(value string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05", "2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05"} {
		if parsed, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid TIMESTAMP value")
}

func parseTimestampLTZ(value string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05Z07:00"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	if parsed, err := parseLocalTimestamp(value); err == nil {
		return parsed.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("invalid TIMESTAMP_LTZ value")
}

func decodeError(column Column, cause error) error {
	return fmt.Errorf("flinksqlgateway: decode column %q as %s: %w", column.Name, column.LogicalType.Type, cause)
}

func typeMismatch(name, expected string, value any) error {
	return fmt.Errorf("flinksqlgateway: column %q decoded as %T, want %s", name, value, expected)
}
