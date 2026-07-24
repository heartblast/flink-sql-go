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

const DefaultMaxRows = 100_000

var (
	ErrPrimaryKeyRequired = errors.New("changelog primary key is required")
	ErrColumnsRequired    = errors.New("changelog column metadata is required")
	ErrPrimaryKeyMissing  = errors.New("changelog primary key value is missing")
	ErrDuplicateKey       = errors.New("changelog primary key already exists")
	ErrRowNotFound        = errors.New("changelog row was not found")
	ErrUpdateOrder        = errors.New("invalid changelog update order")
	ErrMaxRows            = errors.New("changelog materializer maximum rows exceeded")
	ErrUnsupportedRowKind = errors.New("unsupported changelog row kind")
)

type config struct {
	primaryKey []string
	columns    []flinksqlgateway.ColumnInfo
	maxRows    int
}

// Option configures a Materializer.
type Option func(*config) error

// PrimaryKey requires one or more column names. Composite keys preserve the
// supplied column order.
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

// Columns supplies the result schema used to resolve primary-key names.
func Columns(columns []flinksqlgateway.ColumnInfo) Option {
	return func(cfg *config) error {
		if len(columns) == 0 {
			return ErrColumnsRequired
		}
		cfg.columns = append([]flinksqlgateway.ColumnInfo(nil), columns...)
		return nil
	}
}

// MaxRows sets the hard memory row bound. The materializer never evicts rows.
func MaxRows(limit int) Option {
	return func(cfg *config) error {
		if limit <= 0 {
			return fmt.Errorf("%w: limit must be positive", ErrMaxRows)
		}
		cfg.maxRows = limit
		return nil
	}
}

// Materializer is safe for concurrent Apply and Snapshot calls.
type Materializer struct {
	mu         sync.RWMutex
	primaryKey []string
	columns    []flinksqlgateway.ColumnInfo
	maxRows    int
	rows       map[string]flinksqlgateway.Row
	order      []string
	pending    map[string]struct{}
}

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
		pending:    make(map[string]struct{}),
	}, nil
}

// Apply validates and applies one Flink changelog row atomically.
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

// Snapshot returns deep copies in stable insertion order. RowKind is
// normalized to INSERT because the result represents current table state.
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
		compact := bytes.TrimSpace(raw)
		key.WriteString(strconv.Itoa(len(compact)))
		key.WriteByte(':')
		key.Write(compact)
		key.WriteByte('|')
	}
	return key.String(), nil
}

func (m *Materializer) removeOrder(key string) {
	for index, candidate := range m.order {
		if candidate != key {
			continue
		}
		copy(m.order[index:], m.order[index+1:])
		m.order = m.order[:len(m.order)-1]
		return
	}
}

func cloneRow(row flinksqlgateway.Row) flinksqlgateway.Row {
	fields := make([]json.RawMessage, len(row.Fields))
	for index := range row.Fields {
		fields[index] = append([]byte(nil), row.Fields[index]...)
	}
	row.Fields = fields
	return row
}

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
