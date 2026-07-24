package flinksqlgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Identifier identifies a Flink catalog object. Empty Catalog/Database means
// the current session scope, but a Catalog cannot be supplied without a
// Database when an object is addressed.
type Identifier struct {
	Catalog  string
	Database string
	Object   string
}

type TableKind string

const (
	TableKindTable TableKind = "TABLE"
	TableKindView  TableKind = "VIEW"
)

type TableMetadata struct {
	Catalog  string
	Database string
	Name     string
	Kind     TableKind
}

type ColumnMetadata struct {
	Name      string
	DataType  string
	Nullable  bool
	Key       string
	Extras    string
	Watermark string
	Comment   string
}

// QuoteIdentifier applies Flink/Calcite backtick quoting and doubles embedded
// backticks. It never treats identifier text as SQL syntax.
func QuoteIdentifier(identifier string) (string, error) {
	if strings.TrimSpace(identifier) == "" {
		return "", fmt.Errorf("flinksqlgateway: identifier is required")
	}
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`", nil
}

func (c *GatewayClient) ListCatalogs(ctx context.Context, sessionHandle string) ([]string, error) {
	return c.listMetadataNames(ctx, sessionHandle, "SHOW CATALOGS")
}

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

func (c *GatewayClient) ListTables(ctx context.Context, sessionHandle string, identifier Identifier) ([]TableMetadata, error) {
	return c.listTablesOrViews(ctx, sessionHandle, "SHOW TABLES", TableKindTable, identifier)
}

func (c *GatewayClient) ListViews(ctx context.Context, sessionHandle string, identifier Identifier) ([]TableMetadata, error) {
	return c.listTablesOrViews(ctx, sessionHandle, "SHOW VIEWS", TableKindView, identifier)
}

func (c *GatewayClient) ListFunctions(ctx context.Context, sessionHandle string) ([]string, error) {
	return c.listMetadataNames(ctx, sessionHandle, "SHOW FUNCTIONS")
}

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

// Explain executes EXPLAIN PLAN FOR in the current session and joins the first
// field from each returned row with newlines.
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
