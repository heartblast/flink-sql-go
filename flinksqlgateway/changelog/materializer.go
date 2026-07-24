package changelog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/heartblast/flink-sql-go/flinksqlgateway"
)

// DefaultMaxRows는 별도 설정이 없을 때 Materializer가 메모리에 보관할 최대 row 수이다.
const DefaultMaxRows = 100_000

var (
	// ErrPrimaryKeyRequired는 materialize에 사용할 primary key가 지정되지 않았음을 나타낸다.
	ErrPrimaryKeyRequired = errors.New("changelog primary key is required")
	// ErrColumnsRequired는 primary key를 찾을 column metadata가 없음을 나타낸다.
	ErrColumnsRequired = errors.New("changelog column metadata is required")
	// ErrPrimaryKeyMissing은 schema나 row에 primary key 값이 없음을 나타낸다.
	ErrPrimaryKeyMissing = errors.New("changelog primary key value is missing")
	// ErrDuplicateKey는 INSERT할 primary key가 현재 snapshot에 이미 있음을 나타낸다.
	ErrDuplicateKey = errors.New("changelog primary key already exists")
	// ErrRowNotFound는 update 또는 delete 대상 row가 현재 snapshot에 없음을 나타낸다.
	ErrRowNotFound = errors.New("changelog row was not found")
	// ErrUpdateOrder는 UPDATE_BEFORE와 UPDATE_AFTER의 순서나 값이 올바르지 않음을 나타낸다.
	ErrUpdateOrder = errors.New("invalid changelog update order")
	// ErrMaxRows는 snapshot row 수가 설정된 메모리 상한을 넘었음을 나타낸다.
	ErrMaxRows = errors.New("changelog materializer maximum rows exceeded")
	// ErrUnsupportedRowKind는 알 수 없는 changelog RowKind를 받았음을 나타낸다.
	ErrUnsupportedRowKind = errors.New("unsupported changelog row kind")
)

// config는 option 적용 중 사용하는 내부 설정으로 입력 slice의 복사본만 보관한다.
type config struct {
	primaryKey []string
	columns    []flinksqlgateway.ColumnInfo
	maxRows    int
}

// Option은 Materializer 생성 설정을 변경하고 유효하지 않은 값을 오류로 반환한다.
type Option func(*config) error

// PrimaryKey는 하나 이상의 column 이름을 요구하며 복합 key는 전달된 column 순서를 보존한다.
func PrimaryKey(columns ...string) Option {
	return func(cfg *config) error {
		if len(columns) == 0 {
			return ErrPrimaryKeyRequired
		}
		seen := make(map[string]struct{}, len(columns))
		for _, column := range columns {
			if strings.TrimSpace(column) == "" {
				return fmt.Errorf("%w: column name is empty", ErrPrimaryKeyRequired)
			}
			if _, exists := seen[column]; exists {
				return fmt.Errorf("%w: duplicate column %q", ErrPrimaryKeyRequired, column)
			}
			seen[column] = struct{}{}
		}
		cfg.primaryKey = append([]string(nil), columns...)
		return nil
	}
}

// Columns는 primary key 이름을 찾는 데 사용할 결과 schema를 지정한다.
func Columns(columns []flinksqlgateway.ColumnInfo) Option {
	return func(cfg *config) error {
		if len(columns) == 0 {
			return ErrColumnsRequired
		}
		cfg.columns = append([]flinksqlgateway.ColumnInfo(nil), columns...)
		return nil
	}
}

// MaxRows는 메모리에 보관할 row 수의 엄격한 상한을 지정하며 기존 row를 자동 축출하지 않는다.
func MaxRows(limit int) Option {
	return func(cfg *config) error {
		if limit <= 0 {
			return fmt.Errorf("%w: limit must be positive", ErrMaxRows)
		}
		cfg.maxRows = limit
		return nil
	}
}

// Materializer는 Apply와 Snapshot을 동시에 호출해도 안전하다.
type Materializer struct {
	mu         sync.RWMutex
	primaryKey []string
	columns    []flinksqlgateway.ColumnInfo
	maxRows    int
	rows       map[string]flinksqlgateway.Row
	order      []string
	orderIndex map[string]int
	tombstones int
	pending    map[string]struct{}
}

// NewMaterializer는 primary key와 schema를 검증하고 빈 snapshot을 생성한다.
func NewMaterializer(options ...Option) (*Materializer, error) {
	cfg := config{maxRows: DefaultMaxRows}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&cfg); err != nil {
			return nil, err
		}
	}
	if len(cfg.primaryKey) == 0 {
		return nil, ErrPrimaryKeyRequired
	}
	if len(cfg.columns) == 0 {
		return nil, ErrColumnsRequired
	}
	for _, primaryKey := range cfg.primaryKey {
		found := false
		for _, column := range cfg.columns {
			if column.Name == primaryKey {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("%w: column %q", ErrPrimaryKeyMissing, primaryKey)
		}
	}
	return &Materializer{
		primaryKey: cfg.primaryKey,
		columns:    cfg.columns,
		maxRows:    cfg.maxRows,
		rows:       make(map[string]flinksqlgateway.Row),
		orderIndex: make(map[string]int),
		pending:    make(map[string]struct{}),
	}, nil
}

// Apply는 하나의 Flink changelog row를 검증하고 현재 snapshot에 원자적으로 적용한다.
func (m *Materializer) Apply(row flinksqlgateway.Row) error {
	key, err := m.rowKey(row)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, exists := m.rows[key]
	_, updatePending := m.pending[key]

	switch row.Kind {
	case flinksqlgateway.RowInsert:
		if exists || updatePending {
			return fmt.Errorf("%w", ErrDuplicateKey)
		}
		if len(m.rows) >= m.maxRows {
			return fmt.Errorf("%w: limit=%d", ErrMaxRows, m.maxRows)
		}
		m.rows[key] = cloneRow(row)
		m.orderIndex[key] = len(m.order)
		m.order = append(m.order, key)
		return nil

	case flinksqlgateway.RowUpdateBefore:
		if !exists {
			return fmt.Errorf("%w: UPDATE_BEFORE", ErrRowNotFound)
		}
		if updatePending {
			return fmt.Errorf("%w: duplicate UPDATE_BEFORE", ErrUpdateOrder)
		}
		if !sameFields(existing, row) {
			return fmt.Errorf("%w: UPDATE_BEFORE does not match current row", ErrUpdateOrder)
		}
		m.pending[key] = struct{}{}
		return nil

	case flinksqlgateway.RowUpdateAfter:
		if !exists {
			return fmt.Errorf("%w: UPDATE_AFTER", ErrRowNotFound)
		}
		if !updatePending {
			return fmt.Errorf("%w: UPDATE_AFTER without UPDATE_BEFORE", ErrUpdateOrder)
		}
		m.rows[key] = cloneRow(row)
		delete(m.pending, key)
		return nil

	case flinksqlgateway.RowDelete:
		if !exists {
			return fmt.Errorf("%w: DELETE", ErrRowNotFound)
		}
		if updatePending {
			return fmt.Errorf("%w: DELETE during pending update", ErrUpdateOrder)
		}
		delete(m.rows, key)
		m.removeOrder(key)
		return nil

	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedRowKind, row.Kind)
	}
}

// ApplyUpdate는 UPDATE_BEFORE와 UPDATE_AFTER pair를 중간 pending 상태 노출 없이 원자적으로 적용한다.
func (m *Materializer) ApplyUpdate(before, after flinksqlgateway.Row) error {
	if before.Kind != flinksqlgateway.RowUpdateBefore || after.Kind != flinksqlgateway.RowUpdateAfter {
		return fmt.Errorf("%w: ApplyUpdate requires UPDATE_BEFORE and UPDATE_AFTER", ErrUpdateOrder)
	}
	beforeKey, err := m.rowKey(before)
	if err != nil {
		return err
	}
	afterKey, err := m.rowKey(after)
	if err != nil {
		return err
	}
	if beforeKey != afterKey {
		return fmt.Errorf("%w: primary key changed", ErrUpdateOrder)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, exists := m.rows[beforeKey]
	if !exists {
		return fmt.Errorf("%w: UPDATE_BEFORE", ErrRowNotFound)
	}
	if _, pending := m.pending[beforeKey]; pending {
		return fmt.Errorf("%w: update is already pending", ErrUpdateOrder)
	}
	if !sameFields(existing, before) {
		return fmt.Errorf("%w: UPDATE_BEFORE does not match current row", ErrUpdateOrder)
	}
	m.rows[beforeKey] = cloneRow(after)
	return nil
}

// RollbackPending은 불완전한 UPDATE pair를 모두 폐기하고 기존 snapshot row를 유지한다.
func (m *Materializer) RollbackPending() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := len(m.pending)
	m.pending = make(map[string]struct{})
	return count
}

// PendingUpdates는 UPDATE_AFTER를 기다리는 primary key 수를 반환한다.
func (m *Materializer) PendingUpdates() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.pending)
}

// Snapshot은 안정적인 삽입 순서로 row의 깊은 복사본을 반환한다. 현재 table 상태를
// 나타내므로 반환되는 RowKind는 INSERT로 정규화한다.
func (m *Materializer) Snapshot() []flinksqlgateway.Row {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]flinksqlgateway.Row, 0, len(m.rows))
	for _, key := range m.order {
		row, exists := m.rows[key]
		if !exists {
			continue
		}
		row = cloneRow(row)
		row.Kind = flinksqlgateway.RowInsert
		result = append(result, row)
	}
	return result
}

// rowKey는 각 primary key 원본 JSON의 길이와 값을 결합해 충돌 없는 내부 key를 만든다.
func (m *Materializer) rowKey(row flinksqlgateway.Row) (string, error) {
	accessor := row.WithColumns(m.columns, nil)
	var key bytes.Buffer
	for _, column := range m.primaryKey {
		raw, null, err := accessor.Raw(column)
		if err != nil {
			return "", fmt.Errorf("%w: column %q: %v", ErrPrimaryKeyMissing, column, err)
		}
		if null || len(bytes.TrimSpace(raw)) == 0 {
			return "", fmt.Errorf("%w: column %q is NULL", ErrPrimaryKeyMissing, column)
		}
		compact, err := canonicalJSON(raw)
		if err != nil {
			return "", fmt.Errorf("%w: column %q is invalid JSON: %v", ErrPrimaryKeyMissing, column, err)
		}
		key.WriteString(strconv.Itoa(len(compact)))
		key.WriteByte(':')
		key.Write(compact)
		key.WriteByte('|')
	}
	return key.String(), nil
}

// removeOrder는 삭제된 row key를 안정적인 snapshot 순서에서도 제거한다.
func (m *Materializer) removeOrder(key string) {
	index, exists := m.orderIndex[key]
	if !exists {
		return
	}
	delete(m.orderIndex, key)
	m.order[index] = ""
	m.tombstones++
	if m.tombstones > 1024 && m.tombstones*2 > len(m.order) {
		m.compactOrderLocked()
	}
}

// compactOrderLocked는 충분한 tombstone이 쌓였을 때 insertion order index를 한 번에 재구성한다.
func (m *Materializer) compactOrderLocked() {
	compacted := make([]string, 0, len(m.order)-m.tombstones)
	for _, key := range m.order {
		if key == "" {
			continue
		}
		m.orderIndex[key] = len(compacted)
		compacted = append(compacted, key)
	}
	m.order = compacted
	m.tombstones = 0
}

// canonicalJSON은 primary key의 공백, object key 순서와 문자열 escape 표현을 정규화한다.
func canonicalJSON(raw json.RawMessage) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

// cloneRow는 호출자가 snapshot 내부 JSON byte를 변경하지 못하도록 row를 깊게 복사한다.
func cloneRow(row flinksqlgateway.Row) flinksqlgateway.Row {
	fields := make([]json.RawMessage, len(row.Fields))
	for index := range row.Fields {
		fields[index] = append([]byte(nil), row.Fields[index]...)
	}
	row.Fields = fields
	return row
}

// sameFields는 JSON 주변 공백을 제외하고 두 row의 field 표현이 같은지 확인한다.
func sameFields(left, right flinksqlgateway.Row) bool {
	if len(left.Fields) != len(right.Fields) {
		return false
	}
	for index := range left.Fields {
		if !bytes.Equal(bytes.TrimSpace(left.Fields[index]), bytes.TrimSpace(right.Fields[index])) {
			return false
		}
	}
	return true
}
