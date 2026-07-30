package flinksqlgateway

import (
	"errors"
	"fmt"
)

var (
	// ErrSessionNotFound는 참조한 session이 존재하지 않음을 나타낸다.
	ErrSessionNotFound = errors.New("flink sql session not found")
	// ErrSessionExpired는 session 상태가 만료되었고 자동 재생성하지 않았음을 나타낸다.
	ErrSessionExpired = errors.New("flink sql session expired")
	// ErrOperationNotFound는 참조한 operation이 존재하지 않음을 나타낸다.
	ErrOperationNotFound = errors.New("flink sql operation not found")
	// ErrOperationFailed는 Flink가 operation의 실패 종료 상태를 보고했음을 나타낸다.
	ErrOperationFailed = errors.New("flink sql operation failed")
	// ErrResultLimit는 설정한 row 또는 polling 제한에 도달했음을 나타낸다.
	ErrResultLimit = errors.New("flink sql result limit exceeded")
	// ErrUnsupportedAPI는 server 또는 선택한 protocol이 필요한 API를 지원하지 않음을 나타낸다.
	ErrUnsupportedAPI = errors.New("unsupported sql gateway api version")
	// ErrInvalidFlinkVersion은 /info의 Flink 제품 버전을 안전하게 파싱할 수 없음을 나타낸다.
	ErrInvalidFlinkVersion = errors.New("invalid Flink version")
	// ErrUnsupportedFlinkVersion은 파싱된 release line에 등록된 profile이 없음을 나타낸다.
	ErrUnsupportedFlinkVersion = errors.New("unsupported Flink version")
	// ErrNoCompatibleAPIVersion은 server와 profile 사이에 선택 가능한 REST API 버전이 없음을 나타낸다.
	ErrNoCompatibleAPIVersion = fmt.Errorf("%w: no compatible SQL Gateway API version", ErrUnsupportedAPI)
	// ErrExplicitAPIVersionUnsupportedByServer는 명시한 API 버전을 server가 광고하지 않음을 나타낸다.
	ErrExplicitAPIVersionUnsupportedByServer = fmt.Errorf("%w: explicit version is not advertised by server", ErrUnsupportedAPI)
	// ErrExplicitAPIVersionUnsupportedByProfile은 명시한 API 버전을 release profile이 허용하지 않음을 나타낸다.
	ErrExplicitAPIVersionUnsupportedByProfile = fmt.Errorf("%w: explicit version is not supported by profile", ErrUnsupportedAPI)
	// ErrUnsupportedCapability는 현재 release와 protocol 조합이 요청 기능을 제공하지 않음을 나타낸다.
	ErrUnsupportedCapability = fmt.Errorf("%w: unsupported capability", ErrUnsupportedAPI)
	// ErrCompatibilityDetection은 /info 또는 /api_versions 감지 요청이 완료되지 않았음을 나타낸다.
	ErrCompatibilityDetection = errors.New("Flink compatibility detection failed")
	// ErrResponseTooLarge는 응답이 MaxResponseBytes를 초과했음을 나타낸다.
	ErrResponseTooLarge = errors.New("flink sql gateway response too large")
	// ErrUnsafeNextResultURI는 paging URI가 Gateway origin 밖을 가리켰음을 나타낸다.
	ErrUnsafeNextResultURI = errors.New("unsafe flink sql next result URI")
	// ErrExecutionOutcomeUnknown은 statement가 Flink에 도달했을 수 있지만 client가
	// operation handle을 받지 못했음을 나타낸다. 이 오류는 자동 재시도하면 안 된다.
	ErrExecutionOutcomeUnknown = errors.New("statement execution outcome is unknown")
	// ErrConfigurationOutcomeUnknown은 session 설정이 Flink에 도달했을 수 있지만 client가
	// 완료 응답을 확인하지 못했음을 나타낸다. 같은 설정을 자동 재시도하면 안 된다.
	ErrConfigurationOutcomeUnknown = errors.New("session configuration outcome is unknown")
	// ErrSessionSetupVerification은 적용된 setup 객체를 metadata 조회로 확인하지 못했음을 나타낸다.
	// DDL이 적용되지 않았다는 의미로 사용하면 안 된다.
	ErrSessionSetupVerification = errors.New("session setup metadata verification failed")
	// ErrClientClosed는 client가 종료되어 새 작업을 받지 않음을 나타낸다.
	ErrClientClosed = errors.New("flink sql gateway client is closed")
	// ErrSessionClosed는 고수준 session wrapper가 로컬에서 종료되었음을 나타낸다.
	ErrSessionClosed = errors.New("flink sql session is closed")
)

// CompatibilityError는 Flink release 감지와 REST API 선택 실패를 typed context와 함께
// 보고한다. SQL, 인증 header, URL query는 보관하지 않는다.
type CompatibilityError struct {
	Kind         error
	Operation    string
	FlinkVersion string
	ReleaseLine  ReleaseLine
	APIVersion   string
	Cause        error
}

// Error는 compatibility 선택에 필요한 비민감 metadata만 포함한다.
func (e *CompatibilityError) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := "flink sql gateway compatibility failed"
	if e.Operation != "" {
		message += ": " + e.Operation
	}
	if e.FlinkVersion != "" {
		message += " flink=" + sanitizeServerMessage(e.FlinkVersion)
	}
	if e.ReleaseLine != "" {
		message += " release=" + string(e.ReleaseLine)
	}
	if e.APIVersion != "" {
		message += " api=" + e.APIVersion
	}
	if e.Kind != nil {
		message += ": " + e.Kind.Error()
	}
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

// Unwrap은 분류 sentinel과 transport/context 원인을 errors.Is와 errors.As에 모두 제공한다.
func (e *CompatibilityError) Unwrap() []error {
	if e == nil {
		return nil
	}
	result := make([]error, 0, 2)
	if e.Kind != nil {
		result = append(result, e.Kind)
	}
	if e.Cause != nil {
		result = append(result, e.Cause)
	}
	return result
}

// RequestPhase는 실패 전에 HTTP 요청이 진행된 단계를 나타낸다. Flink의 statement 실행
// 여부가 아니라 transport 진행 상태를 설명한다.
type RequestPhase string

const (
	// RequestNotSent는 요청 byte가 전송되지 않은 상태이다.
	RequestNotSent RequestPhase = "NOT_SENT"
	// RequestPossiblySent는 요청이 server에 도달했을 가능성이 있는 상태이다.
	RequestPossiblySent RequestPhase = "POSSIBLY_SENT"
	// ResponseHeaderMissing은 요청 후 응답 header를 받지 못한 상태이다.
	ResponseHeaderMissing RequestPhase = "RESPONSE_HEADER_MISSING"
	// ResponseBodyIncomplete는 응답 header 뒤 body 수신이 완료되지 않은 상태이다.
	ResponseBodyIncomplete RequestPhase = "RESPONSE_BODY_INCOMPLETE"
	// ResponseReceived는 server가 완전한 non-2xx HTTP 응답을 반환한 상태이다.
	ResponseReceived RequestPhase = "RESPONSE_RECEIVED"
)

// ExecutionOutcomeUnknownError는 server 처리 결과를 안전하게 판단할 수 없는 statement
// 제출을 보고하며 statement 원문은 포함하지 않는다.
type ExecutionOutcomeUnknownError struct {
	SessionHandle string
	Method        string
	Endpoint      string
	RequestPhase  RequestPhase
	Cause         error
}

// ConfigurationOutcomeUnknownError는 server 처리 결과를 안전하게 판단할 수 없는 session
// 설정을 보고한다. SQL과 catalog option은 보관하지 않는다.
type ConfigurationOutcomeUnknownError struct {
	SessionHandle string
	StepIndex     int
	StepKind      SessionSetupStepKind
	RequestPhase  RequestPhase
	Cause         error
}

// Error는 SQL 없이 설정 결과 불명확 단계와 마스킹한 session을 반환한다.
func (e *ConfigurationOutcomeUnknownError) Error() string {
	if e == nil {
		return "<nil>"
	}
	step := ""
	if e.StepIndex >= 0 {
		step = fmt.Sprintf(" step=%d kind=%s", e.StepIndex, e.StepKind)
	}
	return fmt.Sprintf("%v:%s phase=%s session=%s", ErrConfigurationOutcomeUnknown, step, e.RequestPhase, MaskHandle(e.SessionHandle))
}

// Unwrap은 원인이 된 정제된 transport 또는 API 오류를 보존한다.
func (e *ConfigurationOutcomeUnknownError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Is는 원인 오류와 함께 ErrConfigurationOutcomeUnknown 분류를 제공한다.
func (e *ConfigurationOutcomeUnknownError) Is(target error) bool {
	return target == ErrConfigurationOutcomeUnknown
}

// SessionSetupError는 setup의 주 오류와 선택적인 session cleanup 오류를 함께 보존한다.
// SQL과 option 원문은 포함하지 않으며 Target은 compile 단계에서 검증된 식별자만 담는다.
type SessionSetupError struct {
	Cause          error
	CloseError     error
	SessionHandle  string
	FailedIndex    int
	StepKind       SessionSetupStepKind
	Target         Identifier
	OutcomeUnknown bool
}

// Error는 민감한 원인 메시지를 반복하지 않고 안전한 setup 실패 문맥만 반환한다.
func (e *SessionSetupError) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := "flink sql session setup failed"
	if e.FailedIndex >= 0 {
		message += fmt.Sprintf(" at step %d kind=%s", e.FailedIndex, e.StepKind)
	}
	if target := formatSessionSetupTarget(e.Target); target != "" {
		message += " target=" + target
	}
	if e.OutcomeUnknown {
		message += ": outcome unknown"
	}
	if e.CloseError != nil {
		message += "; session cleanup failed"
	}
	return message
}

// Unwrap은 setup 원인과 cleanup 오류를 errors.Is와 errors.As가 모두 탐색하게 한다.
func (e *SessionSetupError) Unwrap() []error {
	if e == nil {
		return nil
	}
	result := make([]error, 0, 2)
	if e.Cause != nil {
		result = append(result, e.Cause)
	}
	if e.CloseError != nil {
		result = append(result, e.CloseError)
	}
	return result
}

// Error는 session handle을 마스킹한 실행 결과 불명확 오류 메시지를 반환한다.
func (e *ExecutionOutcomeUnknownError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v: %s %s phase=%s session=%s", ErrExecutionOutcomeUnknown, e.Method, e.Endpoint, e.RequestPhase, MaskHandle(e.SessionHandle))
}

// Unwrap은 원인이 된 timeout 또는 transport 오류를 보존한다.
func (e *ExecutionOutcomeUnknownError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Is는 원인 오류를 잃지 않고 ErrExecutionOutcomeUnknown으로 분류하게 한다.
func (e *ExecutionOutcomeUnknownError) Is(target error) bool {
	return target == ErrExecutionOutcomeUnknown
}

// APIError는 응답 stack trace나 요청 query parameter를 노출하지 않고 transport 또는
// SQL Gateway 오류를 설명한다.
type APIError struct {
	Method       string
	Endpoint     string
	StatusCode   int
	Code         string
	Message      string
	Retryable    bool
	RequestPhase RequestPhase
	Cause        error
}

// ExecutionError는 주 실행 오류와 operation cleanup 오류를 함께 보존하며 statement 원문과
// 인증정보는 보관하지 않는다.
type ExecutionError struct {
	Cause           error
	CancelError     error
	CloseError      error
	SessionHandle   string
	OperationHandle string
	JobID           string
}

// Error는 주 실행 오류 뒤에 보존된 cleanup 오류를 구분해 반환한다.
func (e *ExecutionError) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := "flink sql execution failed"
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	if e.CancelError != nil {
		message += "; cancel cleanup: " + e.CancelError.Error()
	}
	if e.CloseError != nil {
		message += "; close cleanup: " + e.CloseError.Error()
	}
	return message
}

// Unwrap은 보존한 모든 오류를 errors.Is와 errors.As가 탐색하게 한다.
func (e *ExecutionError) Unwrap() []error {
	if e == nil {
		return nil
	}
	result := make([]error, 0, 3)
	for _, err := range []error{e.Cause, e.CancelError, e.CloseError} {
		if err != nil {
			result = append(result, err)
		}
	}
	return result
}

// ResultLimitError는 어떤 고수준 실행 제한에 도달했는지 기록한다.
type ResultLimitError struct {
	Kind     string
	Limit    int
	Received int
}

// Error는 제한 종류, 상한과 실제 수신량을 포함한 오류 메시지를 반환한다.
func (e *ResultLimitError) Error() string {
	return fmt.Sprintf("%v: %s limit=%d received=%d", ErrResultLimit, e.Kind, e.Limit, e.Received)
}

// Unwrap은 ResultLimitError가 ErrResultLimit과 일치하게 한다.
func (e *ResultLimitError) Unwrap() error { return ErrResultLimit }

// Error는 민감한 query 정보를 제외한 Gateway 오류 메시지를 반환한다.
func (e *APIError) Error() string {
	if e == nil {
		return "<nil>"
	}
	status := ""
	if e.StatusCode != 0 {
		status = fmt.Sprintf(" status=%d", e.StatusCode)
	}
	message := e.Message
	if message == "" && e.Cause != nil {
		message = e.Cause.Error()
	}
	if message == "" {
		message = "request failed"
	}
	return fmt.Sprintf("flink sql gateway %s %s%s: %s", e.Method, e.Endpoint, status, message)
}

// Unwrap은 errors.Is와 errors.As가 원인 오류를 탐색하게 한다.
func (e *APIError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
