package flinksqlgateway

import (
	"context"
	"encoding/json"
	"time"
)

// RowFormat controls Flink's result value representation.
type RowFormat string

const (
	RowFormatJSON      RowFormat = "JSON"
	RowFormatPlainText RowFormat = "PLAIN_TEXT"
)

func (f RowFormat) valid() bool { return f == RowFormatJSON || f == RowFormatPlainText }

// ResultType describes result availability.
type ResultType string

const (
	ResultNotReady ResultType = "NOT_READY"
	ResultPayload  ResultType = "PAYLOAD"
	ResultEOS      ResultType = "EOS"
)

// ResultKind is the semantic outcome returned by Flink.
type ResultKind string

const (
	ResultSuccess            ResultKind = "SUCCESS"
	ResultSuccessWithContent ResultKind = "SUCCESS_WITH_CONTENT"
)

// RowKind preserves changelog semantics.
type RowKind string

const (
	RowInsert       RowKind = "INSERT"
	RowUpdateBefore RowKind = "UPDATE_BEFORE"
	RowUpdateAfter  RowKind = "UPDATE_AFTER"
	RowDelete       RowKind = "DELETE"
)

// OperationStatus is intentionally open-ended so newer server states survive
// decoding.
type OperationStatus string

const (
	OperationInitialized OperationStatus = "INITIALIZED"
	OperationPending     OperationStatus = "PENDING"
	OperationRunning     OperationStatus = "RUNNING"
	OperationFinished    OperationStatus = "FINISHED"
	OperationCanceled    OperationStatus = "CANCELED"
	OperationClosed      OperationStatus = "CLOSED"
	OperationError       OperationStatus = "ERROR"
	OperationTimeout     OperationStatus = "TIMEOUT"
)

// Terminal reports whether status is a known terminal state. Unknown values
// are never treated as successful terminal states.
func (s OperationStatus) Terminal() bool {
	switch s {
	case OperationFinished, OperationCanceled, OperationClosed, OperationError, OperationTimeout:
		return true
	default:
		return false
	}
}

// Successful reports whether status is the known successful terminal state.
func (s OperationStatus) Successful() bool { return s == OperationFinished }

// GatewayInfo is returned by /info.
type GatewayInfo struct {
	ProductName string `json:"productName"`
	Version     string `json:"version"`
}

// OpenSessionRequest defines initial session state.
type OpenSessionRequest struct {
	SessionName string            `json:"sessionName,omitempty"`
	Properties  map[string]string `json:"properties,omitempty"`
}

// Session represents an opaque Flink SQL Gateway session handle. Session
// fields are immutable and safe for concurrent reads. Callers should
// serialize statements that mutate session state when ordering matters.
type Session struct {
	Handle     string            `json:"sessionHandle"`
	Name       string            `json:"-"`
	Properties map[string]string `json:"-"`
	CreatedAt  time.Time         `json:"-"`
}

// ExecuteStatementRequest submits SQL without guessing its statement type.
type ExecuteStatementRequest struct {
	Statement        string            `json:"statement"`
	ExecutionTimeout time.Duration     `json:"-"`
	ExecutionConfig  map[string]string `json:"executionConfig,omitempty"`
}

// Operation represents an opaque asynchronous operation handle. It is safe
// for concurrent reads; concurrent cancel/fetch calls retain server semantics.
type Operation struct {
	Handle        string    `json:"operationHandle"`
	SessionHandle string    `json:"-"`
	CreatedAt     time.Time `json:"-"`
}

// LogicalType preserves Flink's structured logical type plus its raw JSON.
// Unknown type names and properties remain available through Raw.
type LogicalType struct {
	Type                string             `json:"type"`
	Nullable            bool               `json:"nullable"`
	Length              *int               `json:"length,omitempty"`
	Precision           *int               `json:"precision,omitempty"`
	Scale               *int               `json:"scale,omitempty"`
	FractionalPrecision *int               `json:"fractionalPrecision,omitempty"`
	Resolution          string             `json:"resolution,omitempty"`
	ElementType         *LogicalType       `json:"elementType,omitempty"`
	KeyType             *LogicalType       `json:"keyType,omitempty"`
	ValueType           *LogicalType       `json:"valueType,omitempty"`
	Fields              []LogicalTypeField `json:"fields,omitempty"`
	Raw                 json.RawMessage    `json:"-"`
}

// UnmarshalJSON retains the source representation for forward compatibility.
func (t *LogicalType) UnmarshalJSON(data []byte) error {
	type wire LogicalType
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*t = LogicalType(decoded)
	t.Raw = append(t.Raw[:0], data...)
	return nil
}

// LogicalTypeField describes a field in a ROW logical type.
type LogicalTypeField struct {
	Name        string       `json:"name"`
	LogicalType *LogicalType `json:"logicalType,omitempty"`
	Type        *LogicalType `json:"type,omitempty"`
	Description string       `json:"description,omitempty"`
}

// ColumnInfo is the actual Flink 1.20 result column representation.
type ColumnInfo struct {
	Name        string      `json:"name"`
	LogicalType LogicalType `json:"logicalType"`
	Comment     *string     `json:"comment"`
}

// Column is an alias retained for the ValueDecoder API terminology.
type Column = ColumnInfo

// Row contains raw JSON fields and their changelog kind. Keeping fields raw
// avoids lossy coercion of decimal, temporal, nested, and binary values.
type Row struct {
	Kind   RowKind           `json:"kind"`
	Fields []json.RawMessage `json:"fields"`
}

// ResultInfo is the actual Flink 1.20 serialized result payload.
type ResultInfo struct {
	Columns   []ColumnInfo `json:"columns"`
	RowFormat RowFormat    `json:"rowFormat"`
	Data      []Row        `json:"data"`
}

// ResultPage preserves result metadata and paging information.
type ResultPage struct {
	ResultType    ResultType      `json:"resultType"`
	ResultKind    ResultKind      `json:"resultKind"`
	QueryResult   bool            `json:"-"`
	JobID         string          `json:"jobID,omitempty"`
	NextResultURI string          `json:"nextResultUri,omitempty"`
	Results       *ResultInfo     `json:"results,omitempty"`
	Raw           json.RawMessage `json:"-"`
	ResponseBytes int64           `json:"-"`
}

// UnmarshalJSON accepts Flink 1.20's isQueryResult field and the queryResult
// spelling emitted by some generated OpenAPI clients.
func (p *ResultPage) UnmarshalJSON(data []byte) error {
	type pageWire struct {
		ResultType    ResultType  `json:"resultType"`
		ResultKind    ResultKind  `json:"resultKind"`
		IsQueryResult *bool       `json:"isQueryResult"`
		QueryResult   *bool       `json:"queryResult"`
		JobID         string      `json:"jobID"`
		NextResultURI string      `json:"nextResultUri"`
		Results       *ResultInfo `json:"results"`
	}
	var decoded pageWire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	p.ResultType = decoded.ResultType
	p.ResultKind = decoded.ResultKind
	p.JobID = decoded.JobID
	p.NextResultURI = decoded.NextResultURI
	p.Results = decoded.Results
	if decoded.IsQueryResult != nil {
		p.QueryResult = *decoded.IsQueryResult
	} else if decoded.QueryResult != nil {
		p.QueryResult = *decoded.QueryResult
	}
	p.Raw = append(p.Raw[:0], data...)
	return nil
}

// ExecuteOptions bounds a collected execution.
type ExecuteOptions struct {
	ExecutionConfig  map[string]string
	ExecutionTimeout time.Duration
	RowFormat        RowFormat
	MaxRows          int
	MaxPolls         int
}

// StreamOptions bounds a channel-based execution.
type StreamOptions struct {
	ExecuteOptions
	Buffer int
}

// ResultStream incrementally fetches rows without collecting the complete
// result in memory. Next is synchronous and does not start a producer
// goroutine.
type ResultStream interface {
	Next() bool
	Event() ResultEvent
	Row() Row
	Err() error
	JobID() string
	Close() error
}

// ExecutionResult is a bounded, collected execution result.
type ExecutionResult struct {
	Operation    *Operation
	Status       OperationStatus
	ResultKind   ResultKind
	QueryResult  bool
	JobID        string
	Columns      []ColumnInfo
	Rows         []Row
	RowsReceived int
	Pages        int
	Polls        int
}

// ResultEventType identifies a StreamResults event.
type ResultEventType string

const (
	ResultEventOperation ResultEventType = "OPERATION"
	ResultEventPage      ResultEventType = "PAGE"
	ResultEventRow       ResultEventType = "ROW"
	ResultEventEOS       ResultEventType = "EOS"
)

// ResultEvent is emitted in operation, page, row, EOS order.
type ResultEvent struct {
	Type      ResultEventType
	Operation *Operation
	Page      *ResultPage
	Row       *Row
}

// SessionContext is passed to an injected statement policy.
type SessionContext struct {
	Handle     string
	Name       string
	Properties map[string]string
}

// StatementValidator lets the containing service enforce ownership and SQL
// authorization without embedding product policy in this package.
type StatementValidator interface {
	Validate(ctx context.Context, session SessionContext, statement string) error
}

// RequestObservation is sanitized request telemetry.
type RequestObservation struct {
	Method     string
	Endpoint   string
	StatusCode int
	Duration   time.Duration
	Err        error
}

// Observer receives request telemetry. Implementations must be concurrency-safe.
type Observer interface {
	ObserveRequest(ctx context.Context, observation RequestObservation)
}

// ObservationEvent identifies an optional high-level lifecycle event.
type ObservationEvent string

const (
	ObservationSessionOpened             ObservationEvent = "SessionOpened"
	ObservationSessionHeartbeatSucceeded ObservationEvent = "SessionHeartbeatSucceeded"
	ObservationSessionHeartbeatFailed    ObservationEvent = "SessionHeartbeatFailed"
	ObservationSessionHealthChanged      ObservationEvent = "SessionHealthChanged"
	ObservationSessionClosed             ObservationEvent = "SessionClosed"
	ObservationStatementSubmitting       ObservationEvent = "StatementSubmitting"
	ObservationStatementSubmitted        ObservationEvent = "StatementSubmitted"
	ObservationStatementOutcomeUnknown   ObservationEvent = "StatementOutcomeUnknown"
	ObservationStatementCompleted        ObservationEvent = "StatementCompleted"
	ObservationStatementFailed           ObservationEvent = "StatementFailed"
	ObservationOperationCanceled         ObservationEvent = "OperationCanceled"
	ObservationOperationClosed           ObservationEvent = "OperationClosed"
	ObservationResultStreamClosed        ObservationEvent = "ResultStreamClosed"
)

// Observation is sanitized high-level telemetry. It intentionally omits SQL,
// headers, and URL query strings.
type Observation struct {
	Event           ObservationEvent
	Timestamp       time.Time
	Endpoint        string
	Method          string
	Duration        time.Duration
	StatusCode      int
	SessionHandle   string
	OperationHandle string
	JobID           string
	ResultRows      int
	ResponseBytes   int64
	Error           error
	PreviousHealth  SessionHealth
	CurrentHealth   SessionHealth
}

// LifecycleObserver receives optional high-level telemetry. Adding it as a
// separate interface preserves compatibility with existing Observer values.
type LifecycleObserver interface {
	ObserveLifecycle(ctx context.Context, observation Observation)
}
