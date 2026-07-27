package flinksqlgateway

import (
	"context"
	"encoding/json"
	"time"
)

// RowFormat은 Flink 결과값의 직렬화 표현을 지정한다.
type RowFormat string

const (
	// RowFormatJSON은 타입 손실을 줄이는 JSON 결과 형식이다.
	RowFormatJSON RowFormat = "JSON"
	// RowFormatPlainText는 값을 표시용 문자열로 받는 PLAIN_TEXT 결과 형식이다.
	RowFormatPlainText RowFormat = "PLAIN_TEXT"
)

// valid는 Flink 1.20에서 지원하는 결과 형식인지 확인한다.
func (f RowFormat) valid() bool { return f == RowFormatJSON || f == RowFormatPlainText }

// ResultType은 operation 결과의 준비와 종료 상태를 설명한다.
type ResultType string

const (
	// ResultNotReady는 결과가 아직 준비되지 않았음을 나타낸다.
	ResultNotReady ResultType = "NOT_READY"
	// ResultPayload는 현재 응답에 결과 page가 있음을 나타낸다.
	ResultPayload ResultType = "PAYLOAD"
	// ResultEOS는 더 가져올 결과가 없음을 나타낸다.
	ResultEOS ResultType = "EOS"
)

// ResultKind는 Flink가 반환하는 실행 결과의 의미를 나타낸다.
type ResultKind string

const (
	// ResultSuccess는 별도 결과 내용 없이 성공했음을 나타낸다.
	ResultSuccess ResultKind = "SUCCESS"
	// ResultSuccessWithContent는 결과 내용을 포함해 성공했음을 나타낸다.
	ResultSuccessWithContent ResultKind = "SUCCESS_WITH_CONTENT"
)

// RowKind는 Flink changelog row의 변경 의미를 보존한다.
type RowKind string

const (
	// RowInsert는 새 row 삽입을 나타낸다.
	RowInsert RowKind = "INSERT"
	// RowUpdateBefore는 update 적용 전 row 값을 나타낸다.
	RowUpdateBefore RowKind = "UPDATE_BEFORE"
	// RowUpdateAfter는 update 적용 후 row 값을 나타낸다.
	RowUpdateAfter RowKind = "UPDATE_AFTER"
	// RowDelete는 기존 row 삭제를 나타낸다.
	RowDelete RowKind = "DELETE"
)

// OperationStatus는 이후 server 상태도 decoding 후 보존할 수 있도록 열린 문자열 타입이다.
type OperationStatus string

const (
	// OperationInitialized는 operation이 초기화된 상태이다.
	OperationInitialized OperationStatus = "INITIALIZED"
	// OperationPending은 operation이 실행을 기다리는 상태이다.
	OperationPending OperationStatus = "PENDING"
	// OperationRunning은 operation이 실행 중인 상태이다.
	OperationRunning OperationStatus = "RUNNING"
	// OperationFinished는 operation이 성공적으로 끝난 상태이다.
	OperationFinished OperationStatus = "FINISHED"
	// OperationCanceled는 operation이 취소된 상태이다.
	OperationCanceled OperationStatus = "CANCELED"
	// OperationClosed는 operation 자원이 닫힌 상태이다.
	OperationClosed OperationStatus = "CLOSED"
	// OperationError는 operation이 오류로 끝난 상태이다.
	OperationError OperationStatus = "ERROR"
	// OperationTimeout은 operation이 제한 시간을 넘긴 상태이다.
	OperationTimeout OperationStatus = "TIMEOUT"
)

// Terminal은 알려진 종료 상태인지 반환한다. 알 수 없는 값은 성공 종료로 취급하지 않는다.
func (s OperationStatus) Terminal() bool {
	switch s {
	case OperationFinished, OperationCanceled, OperationClosed, OperationError, OperationTimeout:
		return true
	default:
		return false
	}
}

// Successful은 알려진 성공 종료 상태인지 반환한다.
func (s OperationStatus) Successful() bool { return s == OperationFinished }

// GatewayInfo는 /info endpoint가 반환하는 제품 정보이다.
type GatewayInfo struct {
	ProductName string `json:"productName"`
	Version     string `json:"version"`
}

// OpenSessionRequest는 새 session의 이름과 초기 property를 정의한다.
type OpenSessionRequest struct {
	SessionName string            `json:"sessionName,omitempty"`
	Properties  map[string]string `json:"properties,omitempty"`
}

// Session은 불투명한 Flink SQL Gateway session의 반환 시점 snapshot이다. caller가 field나
// Properties를 바꿔도 client 내부 상태에는 영향을 주지 않지만 같은 snapshot의 동시 변경은 동기화해야 한다.
type Session struct {
	Handle     string            `json:"sessionHandle"`
	Name       string            `json:"-"`
	Properties map[string]string `json:"-"`
	CreatedAt  time.Time         `json:"-"`
}

// ExecuteStatementRequest는 SQL 종류를 추측하지 않고 statement와 실행 설정을 전달한다.
type ExecuteStatementRequest struct {
	Statement string `json:"statement"`
	// ExecutionTimeout은 source compatibility를 위해 유지한다.
	// Deprecated: Flink 1.20.4는 REST executionTimeout을 지원하지 않으므로 이 값은 전송하지 않는다.
	// 제출 요청은 context 또는 Config.RequestTimeout으로, 전체 고수준 실행은
	// ExecuteOptions.ExecutionTimeout으로 제한한다.
	ExecutionTimeout time.Duration     `json:"-"`
	ExecutionConfig  map[string]string `json:"executionConfig,omitempty"`
}

// Operation은 불투명한 비동기 operation handle을 나타낸다. 동시에 읽어도 안전하며
// cancel과 fetch를 동시에 호출할 때의 결과는 server 의미를 따른다.
type Operation struct {
	Handle        string    `json:"operationHandle"`
	SessionHandle string    `json:"-"`
	CreatedAt     time.Time `json:"-"`
}

// LogicalType은 Flink의 구조화된 논리 타입과 원본 JSON을 보존한다. 알 수 없는 타입 이름과
// property도 Raw에서 확인할 수 있다.
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

// UnmarshalJSON은 이후 버전과의 호환성을 위해 원본 표현을 함께 보존한다.
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

// LogicalTypeField는 ROW 논리 타입 내부 field를 설명한다.
type LogicalTypeField struct {
	Name        string       `json:"name"`
	LogicalType *LogicalType `json:"logicalType,omitempty"`
	Type        *LogicalType `json:"type,omitempty"`
	Description string       `json:"description,omitempty"`
}

// ColumnInfo는 Flink 1.20 결과의 실제 column 표현이다.
type ColumnInfo struct {
	Name        string      `json:"name"`
	LogicalType LogicalType `json:"logicalType"`
	Comment     *string     `json:"comment"`
}

// Column은 ValueDecoder API의 용어를 유지하기 위한 ColumnInfo 별칭이다.
type Column = ColumnInfo

// Row는 원본 JSON field와 changelog 종류를 보존한다. 원본을 유지해 decimal, temporal,
// nested 및 binary 값을 손실이 있는 Go 타입으로 강제 변환하지 않는다.
type Row struct {
	Kind   RowKind           `json:"kind"`
	Fields []json.RawMessage `json:"fields"`
}

// ResultInfo는 Flink 1.20이 직렬화한 실제 결과 payload이다.
type ResultInfo struct {
	Columns   []ColumnInfo `json:"columns"`
	RowFormat RowFormat    `json:"rowFormat"`
	Data      []Row        `json:"data"`
}

// ResultPage는 결과 metadata, 원본 응답과 paging 정보를 보존한다.
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

// UnmarshalJSON은 Flink 1.20의 isQueryResult와 일부 생성형 OpenAPI client가 쓰는
// queryResult 표기를 모두 허용한다.
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

// ExecuteOptions는 메모리에 수집하는 실행의 client-side 시간, row와 polling 상한을 지정한다.
type ExecuteOptions struct {
	ExecutionConfig  map[string]string
	ExecutionTimeout time.Duration
	RowFormat        RowFormat
	MaxRows          int
	MaxPolls         int
}

// StreamOptions는 channel 기반 실행 제한과 backpressure buffer 크기를 지정한다.
type StreamOptions struct {
	ExecuteOptions
	Buffer int
}

// ResultStream은 전체 결과를 메모리에 모으지 않고 row를 점진적으로 가져온다. Next는
// 동기 방식이며 producer goroutine을 시작하지 않는다.
type ResultStream interface {
	// Next는 다음 row를 준비하며 진행할 수 있으면 true를 반환한다.
	Next() bool
	// Event는 최근 operation, row 또는 EOS event를 반환한다.
	Event() ResultEvent
	// Row는 최근 Next가 준비한 changelog row를 반환한다.
	Row() Row
	// Err는 iteration 종료 원인과 cleanup 오류를 반환한다.
	Err() error
	// JobID는 결과에서 확인한 Flink Job ID를 반환한다.
	JobID() string
	// Close는 소비를 중단하고 operation 자원을 정리한다.
	Close() error
}

// ExecutionResult는 설정된 제한 안에서 메모리에 수집한 실행 결과이다.
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

// ResultEventType은 StreamResults에서 전달하는 event 종류를 식별한다.
type ResultEventType string

const (
	// ResultEventOperation은 operation handle이 생성된 event이다.
	ResultEventOperation ResultEventType = "OPERATION"
	// ResultEventPage는 결과 page를 받은 event이다.
	ResultEventPage ResultEventType = "PAGE"
	// ResultEventRow는 changelog row를 받은 event이다.
	ResultEventRow ResultEventType = "ROW"
	// ResultEventEOS는 결과 stream이 끝난 event이다.
	ResultEventEOS ResultEventType = "EOS"
)

// ResultEvent는 operation, page, row, EOS 순서로 전달되는 stream event이다.
type ResultEvent struct {
	Type      ResultEventType
	Operation *Operation
	Page      *ResultPage
	Row       *Row
}

// SessionContext는 주입된 statement 정책에 전달하는 session 정보의 복사본이다.
type SessionContext struct {
	Handle     string
	Name       string
	Properties map[string]string
}

// StatementValidator는 제품 정책을 이 package에 내장하지 않고 상위 서비스가 소유권과
// SQL 권한을 검증하게 한다.
type StatementValidator interface {
	// Validate는 session 소유권과 statement 실행 정책을 확인한다.
	Validate(ctx context.Context, session SessionContext, statement string) error
}

// RequestObservation은 민감정보를 제거한 요청 telemetry이다.
type RequestObservation struct {
	Method     string
	Endpoint   string
	StatusCode int
	Duration   time.Duration
	Err        error
}

// Observer는 요청 telemetry를 받으며 구현체는 동시 호출에 안전해야 한다.
type Observer interface {
	// ObserveRequest는 민감정보가 제거된 요청 측정값을 받는다.
	ObserveRequest(ctx context.Context, observation RequestObservation)
}

// ObservationEvent는 선택적인 고수준 수명주기 event를 식별한다.
type ObservationEvent string

const (
	// ObservationSessionOpened는 session 생성 완료 event이다.
	ObservationSessionOpened ObservationEvent = "SessionOpened"
	// ObservationSessionHeartbeatSucceeded는 session heartbeat 성공 event이다.
	ObservationSessionHeartbeatSucceeded ObservationEvent = "SessionHeartbeatSucceeded"
	// ObservationSessionHeartbeatFailed는 session heartbeat 실패 event이다.
	ObservationSessionHeartbeatFailed ObservationEvent = "SessionHeartbeatFailed"
	// ObservationSessionHealthChanged는 managed session 건강 상태 변경 event이다.
	ObservationSessionHealthChanged ObservationEvent = "SessionHealthChanged"
	// ObservationSessionClosed는 session 종료 event이다.
	ObservationSessionClosed ObservationEvent = "SessionClosed"
	// ObservationStatementSubmitting은 statement 제출 직전 event이다.
	ObservationStatementSubmitting ObservationEvent = "StatementSubmitting"
	// ObservationStatementSubmitted는 operation handle을 받은 statement 제출 완료 event이다.
	ObservationStatementSubmitted ObservationEvent = "StatementSubmitted"
	// ObservationStatementOutcomeUnknown은 statement 제출 결과를 판단할 수 없는 event이다.
	ObservationStatementOutcomeUnknown ObservationEvent = "StatementOutcomeUnknown"
	// ObservationStatementCompleted는 statement 실행과 cleanup 완료 event이다.
	ObservationStatementCompleted ObservationEvent = "StatementCompleted"
	// ObservationStatementFailed는 statement 제출 또는 실행 실패 event이다.
	ObservationStatementFailed ObservationEvent = "StatementFailed"
	// ObservationOperationCanceled는 operation 취소 요청 결과 event이다.
	ObservationOperationCanceled ObservationEvent = "OperationCanceled"
	// ObservationOperationClosed는 operation 자원 종료 결과 event이다.
	ObservationOperationClosed ObservationEvent = "OperationClosed"
	// ObservationResultStreamClosed는 result stream과 해당 operation 종료 event이다.
	ObservationResultStreamClosed ObservationEvent = "ResultStreamClosed"
)

// Observation은 민감정보를 제거한 고수준 telemetry이며 SQL, header와 URL query를
// 의도적으로 포함하지 않는다.
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

// LifecycleObserver는 선택적인 고수준 telemetry를 받는다. 별도 interface로 정의하여
// 기존 Observer 구현과의 호환성을 유지한다.
type LifecycleObserver interface {
	// ObserveLifecycle은 민감정보가 제거된 고수준 수명주기 event를 받는다.
	ObserveLifecycle(ctx context.Context, observation Observation)
}
