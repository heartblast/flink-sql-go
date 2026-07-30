package flinksqlgateway

import (
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const maxSessionSetupIdentifierBytes = 1024

// CompileSessionSetup은 network를 호출하지 않고 전체 plan을 검증한 뒤 결정적인 실행
// 순서의 step을 반환한다. 반환값에는 SQL이나 option 원문을 공개 field로 보관하지 않는다.
func CompileSessionSetup(plan SessionSetupPlan) ([]SessionSetupStep, error) {
	compiled, err := compileSessionSetupPlan(plan)
	if err != nil {
		return nil, err
	}
	steps := make([]SessionSetupStep, len(compiled))
	for index := range compiled {
		steps[index] = compiled[index].SessionSetupStep
	}
	return steps, nil
}

// compileSessionSetupPlan은 Apply가 사용할 SQL을 caller에게 반환하지 않는 내부 compile 경로이다.
func compileSessionSetupPlan(plan SessionSetupPlan) ([]compiledSessionSetupStep, error) {
	if len(plan.Catalogs) == 0 && len(plan.Databases) == 0 && len(plan.Tables) == 0 && plan.CurrentCatalog == "" && plan.CurrentDatabase == "" {
		return nil, fmt.Errorf("flinksqlgateway: session setup plan must not be empty")
	}

	steps := make([]compiledSessionSetupStep, 0, len(plan.Catalogs)+len(plan.Databases)+len(plan.Tables)+2)
	catalogs := make(map[string]struct{}, len(plan.Catalogs))
	for index, catalog := range plan.Catalogs {
		if err := validateSessionSetupIdentifier("catalog", catalog.Name); err != nil {
			return nil, fmt.Errorf("flinksqlgateway: catalog setup %d: %w", index, err)
		}
		if _, exists := catalogs[catalog.Name]; exists {
			return nil, fmt.Errorf("flinksqlgateway: duplicate catalog setup %q", catalog.Name)
		}
		catalogs[catalog.Name] = struct{}{}
		if len(catalog.Options) == 0 {
			return nil, fmt.Errorf("flinksqlgateway: catalog setup %d requires at least one option", index)
		}
		customSensitiveKeys, err := compileSensitiveKeys(catalog.SensitiveKeys)
		if err != nil {
			return nil, fmt.Errorf("flinksqlgateway: catalog setup %d: %w", index, err)
		}
		options, sensitiveValues, err := compileSessionSetupOptions(catalog.Options, customSensitiveKeys)
		if err != nil {
			return nil, fmt.Errorf("flinksqlgateway: catalog setup %d: %w", index, err)
		}
		name, _ := QuoteIdentifier(catalog.Name)
		statement := "CREATE CATALOG "
		if catalog.IfNotExists {
			statement += "IF NOT EXISTS "
		}
		statement += name + " WITH (" + options + ")"
		steps = appendCompiledSessionSetupStep(steps, SessionSetupCatalog, Identifier{Catalog: catalog.Name}, statement, false, sensitiveValues)
	}

	databases := make(map[string]struct{}, len(plan.Databases))
	for index, database := range plan.Databases {
		if err := validateSessionSetupIdentifier("database catalog", database.Catalog); err != nil {
			return nil, fmt.Errorf("flinksqlgateway: database setup %d: %w", index, err)
		}
		if err := validateSessionSetupIdentifier("database", database.Name); err != nil {
			return nil, fmt.Errorf("flinksqlgateway: database setup %d: %w", index, err)
		}
		key := database.Catalog + "\x00" + database.Name
		if _, exists := databases[key]; exists {
			return nil, fmt.Errorf("flinksqlgateway: duplicate database setup %s", formatSessionSetupTarget(Identifier{Catalog: database.Catalog, Database: database.Name}))
		}
		databases[key] = struct{}{}
		options, sensitiveValues, err := compileSessionSetupOptions(database.Options, nil)
		if err != nil {
			return nil, fmt.Errorf("flinksqlgateway: database setup %d: %w", index, err)
		}
		target := Identifier{Catalog: database.Catalog, Database: database.Name}
		qualified, _ := qualifiedSessionSetupTarget(target)
		statement := "CREATE DATABASE "
		if database.IfNotExists {
			statement += "IF NOT EXISTS "
		}
		statement += qualified
		if options != "" {
			statement += " WITH (" + options + ")"
		}
		steps = appendCompiledSessionSetupStep(steps, SessionSetupDatabase, target, statement, false, sensitiveValues)
	}

	tables := make(map[string]struct{}, len(plan.Tables))
	for index, table := range plan.Tables {
		if err := validateTableSetup(table); err != nil {
			return nil, fmt.Errorf("flinksqlgateway: table setup %d: %w", index, err)
		}
		key := table.Target.Catalog + "\x00" + table.Target.Database + "\x00" + table.Target.Object
		if _, exists := tables[key]; exists {
			return nil, fmt.Errorf("flinksqlgateway: duplicate table setup %s", formatSessionSetupTarget(table.Target))
		}
		tables[key] = struct{}{}
		qualified, _ := qualifiedSessionSetupTarget(table.Target)
		statement := "CREATE TABLE "
		if table.IfNotExists {
			statement += "IF NOT EXISTS "
		}
		statement += qualified + " " + strings.TrimSpace(table.Statement)
		steps = appendCompiledSessionSetupStep(steps, SessionSetupTable, table.Target, statement, table.Verify, nil)
		steps[len(steps)-1].redactAll = table.Sensitive
	}

	if plan.CurrentDatabase != "" && plan.CurrentCatalog == "" {
		return nil, fmt.Errorf("flinksqlgateway: CurrentCatalog is required when CurrentDatabase is set")
	}
	if plan.CurrentCatalog != "" {
		if err := validateSessionSetupIdentifier("current catalog", plan.CurrentCatalog); err != nil {
			return nil, err
		}
		catalog, _ := QuoteIdentifier(plan.CurrentCatalog)
		steps = appendCompiledSessionSetupStep(steps, SessionSetupUseCatalog, Identifier{Catalog: plan.CurrentCatalog}, "USE CATALOG "+catalog, false, nil)
	}
	if plan.CurrentDatabase != "" {
		if err := validateSessionSetupIdentifier("current database", plan.CurrentDatabase); err != nil {
			return nil, err
		}
		database, _ := QuoteIdentifier(plan.CurrentDatabase)
		steps = appendCompiledSessionSetupStep(steps, SessionSetupUseDatabase, Identifier{Catalog: plan.CurrentCatalog, Database: plan.CurrentDatabase}, "USE "+database, false, nil)
	}
	return steps, nil
}

// appendCompiledSessionSetupStep은 공개 index와 내부 실행 정보를 같은 순서로 고정한다.
func appendCompiledSessionSetupStep(steps []compiledSessionSetupStep, kind SessionSetupStepKind, target Identifier, statement string, verify bool, sensitiveValues []string) []compiledSessionSetupStep {
	return append(steps, compiledSessionSetupStep{
		SessionSetupStep: SessionSetupStep{Index: len(steps), Kind: kind, Target: target},
		statement:        statement,
		verify:           verify,
		sensitiveValues:  append([]string(nil), sensitiveValues...),
	})
}

// validateTableSetup은 parser 없이 완전 수식 대상을 생성할 수 있도록 Statement를 table
// 이름 뒤의 definition으로 제한한다.
func validateTableSetup(table TableSetup) error {
	for _, part := range []struct {
		kind  string
		value string
	}{
		{kind: "table catalog", value: table.Target.Catalog},
		{kind: "table database", value: table.Target.Database},
		{kind: "table name", value: table.Target.Object},
	} {
		if err := validateSessionSetupIdentifier(part.kind, part.value); err != nil {
			return err
		}
	}
	definition := strings.TrimSpace(table.Statement)
	if definition == "" {
		return fmt.Errorf("table definition statement is required")
	}
	if !utf8.ValidString(definition) {
		return fmt.Errorf("table definition statement is not valid UTF-8")
	}
	if definition[0] == '(' {
		return nil
	}
	fields := strings.Fields(definition)
	if len(fields) == 0 {
		return fmt.Errorf("table definition statement is required")
	}
	switch strings.ToUpper(fields[0]) {
	case "COMMENT", "PARTITIONED", "DISTRIBUTED", "WITH", "LIKE", "AS":
		return nil
	default:
		return fmt.Errorf("table Statement must be a definition after the fully qualified target, not a complete SQL statement")
	}
}

// validateSessionSetupIdentifier은 오류와 lifecycle target에 안전하게 포함할 식별자의
// 크기, UTF-8과 제어문자를 compile 단계에서 확인한다.
func validateSessionSetupIdentifier(kind, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s identifier is required", kind)
	}
	if len(value) > maxSessionSetupIdentifierBytes {
		return fmt.Errorf("%s identifier exceeds %d bytes", kind, maxSessionSetupIdentifierBytes)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s identifier is not valid UTF-8", kind)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("%s identifier contains a control character", kind)
		}
	}
	return nil
}

// qualifiedSessionSetupTarget은 compile에서 검증된 catalog.database.object를 모두 quoting한다.
func qualifiedSessionSetupTarget(target Identifier) (string, error) {
	parts := make([]string, 0, 3)
	for _, part := range []struct {
		kind  string
		value string
	}{
		{kind: "catalog", value: target.Catalog},
		{kind: "database", value: target.Database},
		{kind: "object", value: target.Object},
	} {
		if part.value == "" {
			continue
		}
		if err := validateSessionSetupIdentifier(part.kind, part.value); err != nil {
			return "", err
		}
		quoted, _ := QuoteIdentifier(part.value)
		parts = append(parts, quoted)
	}
	return strings.Join(parts, "."), nil
}

// formatSessionSetupTarget은 검증 가능한 식별자만 오류와 관측에 사용할 표현으로 만든다.
func formatSessionSetupTarget(target Identifier) string {
	formatted, err := qualifiedSessionSetupTarget(target)
	if err != nil {
		return ""
	}
	return formatted
}

// compileSessionSetupOptions은 key를 정렬하고 key/value를 SQL literal로 quoting한다.
func compileSessionSetupOptions(options map[string]string, customSensitiveKeys []string) (string, []string, error) {
	if len(options) == 0 {
		return "", nil, nil
	}
	keys := make([]string, 0, len(options))
	for key := range options {
		if strings.TrimSpace(key) == "" {
			return "", nil, fmt.Errorf("option key is required")
		}
		if !utf8.ValidString(key) {
			return "", nil, fmt.Errorf("option key is not valid UTF-8")
		}
		if !utf8.ValidString(options[key]) {
			return "", nil, fmt.Errorf("option %q value is not valid UTF-8", key)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	clauses := make([]string, 0, len(keys))
	sensitiveValues := make([]string, 0)
	for _, key := range keys {
		value := options[key]
		clauses = append(clauses, quoteStringLiteral(key)+" = "+quoteStringLiteral(value))
		if isSensitiveSessionSetupKey(key, customSensitiveKeys) {
			sensitiveValues = appendSensitiveValueVariants(sensitiveValues, value)
		}
	}
	return strings.Join(clauses, ", "), sensitiveValues, nil
}

// quoteStringLiteral은 작은따옴표를 두 번 써 Flink SQL 문자열 literal을 생성한다.
func quoteStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// compileSensitiveKeys은 호출자 지정 민감 key를 기본 key와 같은 비교 형태로 정규화한다.
func compileSensitiveKeys(keys []string) ([]string, error) {
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("SensitiveKeys must not contain an empty key")
		}
		normalized := normalizeSensitiveSessionSetupKey(key)
		if normalized == "" {
			return nil, fmt.Errorf("SensitiveKeys contains an invalid key")
		}
		result = append(result, normalized)
	}
	return result, nil
}

// isSensitiveSessionSetupKey는 대소문자와 '.', '_' 및 '-' 구분자 차이를 제거하고
// password, secret, token, access-key 계열을 보수적으로 탐지한다.
func isSensitiveSessionSetupKey(key string, custom []string) bool {
	normalized := normalizeSensitiveSessionSetupKey(key)
	for _, marker := range []string{"password", "secret", "token", "accesskey", "secretkey"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	for _, marker := range custom {
		if marker != "" && strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

// normalizeSensitiveSessionSetupKey는 문자와 숫자만 소문자로 남겨 구분자 변형을 통합한다.
func normalizeSensitiveSessionSetupKey(key string) string {
	var builder strings.Builder
	for _, character := range strings.ToLower(key) {
		if unicode.IsLetter(character) || unicode.IsDigit(character) {
			builder.WriteRune(character)
		}
	}
	return builder.String()
}

// appendSensitiveValueVariants는 server가 raw 값이나 SQL escape 값을 반사하는 두 경우를
// 모두 치환하도록 중복을 허용해 수집한다. transport가 최종 중복 제거를 수행한다.
func appendSensitiveValueVariants(values []string, value string) []string {
	if value == "" {
		return values
	}
	values = append(values, value)
	escaped := strings.ReplaceAll(value, "'", "''")
	if escaped != value {
		values = append(values, escaped)
	}
	return values
}
