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

// ValueDecoder는 호출자가 명시적으로 요청할 때 하나의 원본 field를 논리 타입에 맞게 변환한다.
type ValueDecoder interface {
	// Decode는 column 논리 타입을 기준으로 원본 JSON field 하나를 변환한다.
	Decode(column Column, raw json.RawMessage) (any, error)
}

// DefaultValueDecoder는 Flink의 주요 scalar, temporal 및 nested 논리 타입을 지원한다.
// 알 수 없는 이후 타입은 json.RawMessage로 반환한다.
type DefaultValueDecoder struct{}

// Decimal은 부동소수점 변환 없이 정확한 10진수 문자열 표현을 보존한다.
type Decimal string

// String은 정밀도 손실 없이 원본 10진수 문자열을 반환한다.
func (d Decimal) String() string { return string(d) }

// LocalDate는 application timezone을 추가하지 않은 DATE 값을 나타낸다.
type LocalDate struct {
	Time time.Time
	Text string
}

// LocalTime은 날짜와 timezone이 없는 TIME 값을 나타낸다.
type LocalTime struct {
	Time time.Time
	Text string
}

// LocalTimestamp는 절대 시각이나 timezone 의미가 없는 TIMESTAMP 값을 나타낸다.
type LocalTimestamp struct {
	Time time.Time
	Text string
}

// TimestampLTZ는 TIMESTAMP_LTZ를 절대 시각으로 나타낸다.
type TimestampLTZ struct {
	Time time.Time
	Text string
}

// Decode는 ValueDecoder를 구현하며 알 수 없는 논리 타입의 원본 JSON을 그대로 보존한다.
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

// RowAccessor는 Row의 wire 표현을 바꾸지 않고 schema 기반 field 접근을 제공한다.
// column 이름이 중복되면 첫 번째 column을 사용한다.
type RowAccessor struct {
	row     Row
	columns []ColumnInfo
	decoder ValueDecoder
}

// WithColumns는 row에 schema를 연결한 accessor를 반환한다. 이후 호출자의 변경이 accessor에
// 영향을 주지 않도록 row와 column slice를 복사한다.
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

// NewRowAccessor는 Row.WithColumns와 같은 기능을 함수 형태로 제공한다.
func NewRowAccessor(row Row, columns []ColumnInfo, decoder ValueDecoder) RowAccessor {
	return row.WithColumns(columns, decoder)
}

// Row는 내부 원본 row의 깊은 복사본을 반환한다.
func (r RowAccessor) Row() Row {
	result := r.row
	result.Fields = cloneRawMessages(result.Fields)
	return result
}

// Raw는 column 이름에 해당하는 원본 값의 복사본을 반환하며 bool은 SQL NULL 여부이다.
func (r RowAccessor) Raw(name string) (json.RawMessage, bool, error) {
	index, err := r.columnIndex(name)
	if err != nil {
		return nil, false, err
	}
	return r.RawAt(index)
}

// RawAt은 0부터 시작하는 column index에 해당하는 원본 값의 복사본을 반환한다.
func (r RowAccessor) RawAt(index int) (json.RawMessage, bool, error) {
	if index < 0 || index >= len(r.row.Fields) {
		return nil, false, fmt.Errorf("flinksqlgateway: column index %d is out of range", index)
	}
	raw := append(json.RawMessage(nil), r.row.Fields[index]...)
	return raw, bytes.Equal(bytes.TrimSpace(raw), []byte("null")), nil
}

// Value는 이름으로 찾은 field를 설정된 ValueDecoder로 변환한다.
func (r RowAccessor) Value(name string) (any, bool, error) {
	index, err := r.columnIndex(name)
	if err != nil {
		return nil, false, err
	}
	return r.ValueAt(index)
}

// ValueAt은 index로 찾은 field를 설정된 ValueDecoder로 변환한다.
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

// String은 이름으로 찾은 field를 string으로 반환하며 bool은 SQL NULL 여부이다.
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

// Int64는 이름으로 찾은 정수 field를 int64로 반환하며 bool은 SQL NULL 여부이다.
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

// Bool은 이름으로 찾은 boolean field를 반환하며 bool 반환값 중 두 번째 값은 SQL NULL 여부이다.
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

// Decimal은 이름으로 찾은 decimal field를 정밀도 손실 없는 값으로 반환한다.
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

// Time은 이름으로 찾은 temporal field의 time.Time 값과 SQL NULL 여부를 반환한다.
// Local temporal 값에는 임의의 application timezone을 적용하지 않는다.
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

// columnIndex는 첫 번째로 일치하는 column 이름을 찾고 row field 존재 여부도 검증한다.
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

// cloneRawMessages는 원본 byte slice를 공유하지 않도록 JSON field를 깊게 복사한다.
func cloneRawMessages(fields []json.RawMessage) []json.RawMessage {
	result := make([]json.RawMessage, len(fields))
	for index := range fields {
		result[index] = append(json.RawMessage(nil), fields[index]...)
	}
	return result
}

// scalarText는 JSON 문자열과 숫자·boolean token을 공통 문자열 표현으로 읽는다.
func scalarText(raw []byte) (string, error) {
	if len(raw) > 0 && raw[0] == '"' {
		return quotedText(raw)
	}
	return string(raw), nil
}

// quotedText는 JSON 문자열 token을 Go 문자열로 해석한다.
func quotedText(raw []byte) (string, error) {
	var result string
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", err
	}
	return result, nil
}

// validJSONNumber는 decimal 문자열이 다른 JSON 타입이 아닌 유효한 숫자 token인지 검증한다.
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

// decodeArray는 element 논리 타입이 있으면 각 값을 변환하고 없으면 원본 JSON을 보존한다.
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

// decodeMap은 JSON object 형태의 MAP 값을 변환하며 표현을 알 수 없으면 원본 JSON을 보존한다.
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

// decodeNestedRow는 object와 array 두 ROW 표현을 지원하고 알 수 없는 field는 원본으로 보존한다.
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

// parseLocalTime은 timezone 의미를 추가하지 않고 Flink TIME 문자열을 UTC 기준 벽시계 값으로 담는다.
func parseLocalTime(value string) (time.Time, error) {
	for _, layout := range []string{"15:04:05.999999999", "15:04:05"} {
		if parsed, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid TIME value")
}

// parseLocalTimestamp는 timezone 의미를 추가하지 않고 Flink TIMESTAMP 형식을 해석한다.
func parseLocalTimestamp(value string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05", "2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05"} {
		if parsed, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid TIMESTAMP value")
}

// parseTimestampLTZ는 offset이 있는 값을 절대 시각으로 해석하고 없는 값은 UTC로 간주한다.
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

// decodeError는 column 이름과 논리 타입을 유지하면서 변환 원인 오류를 감싼다.
func decodeError(column Column, cause error) error {
	return fmt.Errorf("flinksqlgateway: decode column %q as %s: %w", column.Name, column.LogicalType.Type, cause)
}

// typeMismatch는 typed accessor가 기대한 Go 타입과 실제 변환 타입의 차이를 설명한다.
func typeMismatch(name, expected string, value any) error {
	return fmt.Errorf("flinksqlgateway: column %q decoded as %T, want %s", name, value, expected)
}
