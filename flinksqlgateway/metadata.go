package flinksqlgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Identifier는 Flink catalog 객체를 식별한다. 빈 Catalog나 Database는 현재 session 범위를
// 뜻하며 객체를 지정할 때 Catalog만 단독으로 제공할 수는 없다.
type Identifier struct {
	Catalog  string
	Database string
	Object   string
}

// TableKind는 metadata 목록에 포함된 객체가 table인지 view인지 구분한다.
type TableKind string

const (
	// TableKindTable은 실제 또는 temporary table을 나타낸다.
	TableKindTable TableKind = "TABLE"
	// TableKindView는 view를 나타낸다.
	TableKindView TableKind = "VIEW"
)

// TableMetadata는 조회 범위와 table 또는 view 이름을 보존한다.
type TableMetadata struct {
	Catalog  string
	Database string
	Name     string
	Kind     TableKind
}

// ColumnMetadata는 DESCRIBE 결과에서 얻은 column 정의를 보존한다.
type ColumnMetadata struct {
	Name      string
	DataType  string
	Nullable  bool
	Key       string
	Extras    string
	Watermark string
	Comment   string
}

// QuoteIdentifier는 Flink/Calcite backtick quoting을 적용하고 내부 backtick을 두 번 쓴다.
// 식별자 문자열을 SQL 구문으로 취급하지 않는다.
func QuoteIdentifier(identifier string) (string, error) {
	if strings.TrimSpace(identifier) == "" {
		return "", fmt.Errorf("flinksqlgateway: identifier is required")
	}
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`", nil
}

// ListCatalogs는 현재 session에서 사용할 수 있는 catalog 이름을 조회한다.
func (c *GatewayClient) ListCatalogs(ctx context.Context, sessionHandle string) ([]string, error) {
	return c.listMetadataNames(ctx, sessionHandle, "SHOW CATALOGS")
}

// ListDatabases는 현재 또는 지정한 catalog에서 database 이름을 조회한다.
func (c *GatewayClient) ListDatabases(ctx context.Context, sessionHandle, catalog string) ([]string, error) {
	statement := "SHOW DATABASES"
	if catalog != "" {
		quoted, err := QuoteIdentifier(catalog)
		if err != nil {
			return nil, err
		}
		statement += " IN " + quoted
	}
	return c.listMetadataNames(ctx, sessionHandle, statement)
}

// ListTables는 현재 또는 지정한 catalog와 database 범위의 table을 조회한다.
func (c *GatewayClient) ListTables(ctx context.Context, sessionHandle string, identifier Identifier) ([]TableMetadata, error) {
	return c.listTablesOrViews(ctx, sessionHandle, "SHOW TABLES", TableKindTable, identifier)
}

// ListViews는 현재 또는 지정한 catalog와 database 범위의 view를 조회한다.
func (c *GatewayClient) ListViews(ctx context.Context, sessionHandle string, identifier Identifier) ([]TableMetadata, error) {
	return c.listTablesOrViews(ctx, sessionHandle, "SHOW VIEWS", TableKindView, identifier)
}

// ListFunctions는 현재 session에서 사용할 수 있는 function 이름을 조회한다.
func (c *GatewayClient) ListFunctions(ctx context.Context, sessionHandle string) ([]string, error) {
	return c.listMetadataNames(ctx, sessionHandle, "SHOW FUNCTIONS")
}

// DescribeTable은 완전한 quoting을 적용한 객체의 DESCRIBE column metadata를 반환한다.
func (c *GatewayClient) DescribeTable(ctx context.Context, sessionHandle string, identifier Identifier) ([]ColumnMetadata, error) {
	qualified, err := qualifiedObject(identifier)
	if err != nil {
		return nil, err
	}
	result, err := c.ExecuteAndWait(ctx, sessionHandle, "DESCRIBE "+qualified, ExecuteOptions{})
	if err != nil {
		return nil, err
	}
	columns := make([]ColumnMetadata, 0, len(result.Rows))
	for _, row := range result.Rows {
		name, err := rowFieldText(row, 0)
		if err != nil {
			return nil, err
		}
		dataType, err := rowFieldText(row, 1)
		if err != nil {
			return nil, err
		}
		nullableText, err := rowFieldText(row, 2)
		if err != nil {
			return nil, err
		}
		metadata := ColumnMetadata{
			Name:     name,
			DataType: dataType,
			Nullable: strings.EqualFold(nullableText, "true") || strings.EqualFold(nullableText, "yes"),
		}
		optional := []*string{&metadata.Key, &metadata.Extras, &metadata.Watermark, &metadata.Comment}
		for offset, destination := range optional {
			if 3+offset >= len(row.Fields) {
				break
			}
			*destination, err = rowFieldText(row, 3+offset)
			if err != nil {
				return nil, err
			}
		}
		columns = append(columns, metadata)
	}
	return columns, nil
}

// Explain은 현재 session에서 EXPLAIN PLAN FOR를 실행하고 각 결과 row의 첫 field를 줄바꿈으로 합친다.
func (c *GatewayClient) Explain(ctx context.Context, sessionHandle, statement string) (string, error) {
	if strings.TrimSpace(statement) == "" {
		return "", fmt.Errorf("flinksqlgateway: statement is required")
	}
	result, err := c.ExecuteAndWait(ctx, sessionHandle, "EXPLAIN PLAN FOR "+statement, ExecuteOptions{})
	if err != nil {
		return "", err
	}
	lines := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		line, err := rowFieldText(row, 0)
		if err != nil {
			return "", err
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n"), nil
}

// listMetadataNames는 SHOW 계열 결과의 첫 field를 이름 목록으로 변환한다.
func (c *GatewayClient) listMetadataNames(ctx context.Context, sessionHandle, statement string) ([]string, error) {
	result, err := c.ExecuteAndWait(ctx, sessionHandle, statement, ExecuteOptions{})
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		name, err := rowFieldText(row, 0)
		if err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

// listTablesOrViews는 안전하게 quoting한 범위에서 table 또는 view metadata를 구성한다.
func (c *GatewayClient) listTablesOrViews(ctx context.Context, sessionHandle, command string, kind TableKind, identifier Identifier) ([]TableMetadata, error) {
	if identifier.Object != "" {
		return nil, fmt.Errorf("flinksqlgateway: list scope must not include an object name")
	}
	statement := command
	if identifier.Catalog != "" && identifier.Database == "" {
		return nil, fmt.Errorf("flinksqlgateway: database is required when catalog is set")
	}
	if identifier.Database != "" {
		scope, err := qualifiedScope(identifier.Catalog, identifier.Database)
		if err != nil {
			return nil, err
		}
		statement += " IN " + scope
	}
	names, err := c.listMetadataNames(ctx, sessionHandle, statement)
	if err != nil {
		return nil, err
	}
	result := make([]TableMetadata, len(names))
	for index, name := range names {
		result[index] = TableMetadata{Catalog: identifier.Catalog, Database: identifier.Database, Name: name, Kind: kind}
	}
	return result, nil
}

// qualifiedObject는 catalog, database와 object를 각각 quoting해 완전한 객체 이름을 만든다.
func qualifiedObject(identifier Identifier) (string, error) {
	if identifier.Object == "" {
		return "", fmt.Errorf("flinksqlgateway: object identifier is required")
	}
	if identifier.Catalog != "" && identifier.Database == "" {
		return "", fmt.Errorf("flinksqlgateway: database is required when catalog is set")
	}
	parts := make([]string, 0, 3)
	for _, value := range []string{identifier.Catalog, identifier.Database, identifier.Object} {
		if value == "" {
			continue
		}
		quoted, err := QuoteIdentifier(value)
		if err != nil {
			return "", err
		}
		parts = append(parts, quoted)
	}
	return strings.Join(parts, "."), nil
}

// qualifiedScope는 현재 catalog 또는 명시된 catalog의 database 범위를 안전하게 quoting한다.
func qualifiedScope(catalog, database string) (string, error) {
	parts := make([]string, 0, 2)
	for _, value := range []string{catalog, database} {
		if value == "" {
			continue
		}
		quoted, err := QuoteIdentifier(value)
		if err != nil {
			return "", err
		}
		parts = append(parts, quoted)
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("flinksqlgateway: database scope is required")
	}
	return strings.Join(parts, "."), nil
}

// rowFieldText는 metadata 결과 field를 문자열로 읽고 SQL NULL은 빈 문자열로 반환한다.
func rowFieldText(row Row, index int) (string, error) {
	if index < 0 || index >= len(row.Fields) {
		return "", fmt.Errorf("flinksqlgateway: metadata row field %d is missing", index)
	}
	raw := bytes.TrimSpace(row.Fields[index])
	if bytes.Equal(raw, []byte("null")) {
		return "", nil
	}
	if len(raw) > 0 && raw[0] == '"' {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return "", fmt.Errorf("flinksqlgateway: decode metadata field %d: %w", index, err)
		}
		return value, nil
	}
	if value, err := strconv.Unquote(string(raw)); err == nil {
		return value, nil
	}
	return string(raw), nil
}
