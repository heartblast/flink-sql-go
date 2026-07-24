package flinksqlgateway

import (
	"errors"
	"fmt"
)

var (
	// ErrSessionNotFound means the referenced session does not exist.
	ErrSessionNotFound = errors.New("flink sql session not found")
	// ErrSessionExpired means session state expired and was not recreated.
	ErrSessionExpired = errors.New("flink sql session expired")
	// ErrOperationNotFound means the referenced operation does not exist.
	ErrOperationNotFound = errors.New("flink sql operation not found")
	// ErrOperationFailed means Flink reported a terminal unsuccessful operation.
	ErrOperationFailed = errors.New("flink sql operation failed")
	// ErrResultLimit means a configured row or polling limit was reached.
	ErrResultLimit = errors.New("flink sql result limit exceeded")
	// ErrUnsupportedAPI means the server or selected protocol lacks an API.
	ErrUnsupportedAPI = errors.New("unsupported sql gateway api version")
	// ErrResponseTooLarge means a response exceeded MaxResponseBytes.
	ErrResponseTooLarge = errors.New("flink sql gateway response too large")
	// ErrUnsafeNextResultURI means paging attempted to leave the gateway origin.
	ErrUnsafeNextResultURI = errors.New("unsafe flink sql next result URI")
	// ErrExecutionOutcomeUnknown means a statement may have reached Flink but
	// the client did not receive an operation handle. It must not be retried
	// automatically.
	ErrExecutionOutcomeUnknown = errors.New("statement execution outcome is unknown")
	// ErrClientClosed means the client no longer accepts new work.
	ErrClientClosed = errors.New("flink sql gateway client is closed")
	// ErrSessionClosed means a high-level session wrapper was closed locally.
	ErrSessionClosed = errors.New("flink sql session is closed")
)

// RequestPhase identifies how far an HTTP request progressed before failure.
// It describes transport progress, not whether Flink executed a statement.
type RequestPhase string

const (
	RequestNotSent         RequestPhase = "NOT_SENT"
	RequestPossiblySent    RequestPhase = "POSSIBLY_SENT"
	ResponseHeaderMissing  RequestPhase = "RESPONSE_HEADER_MISSING"
	ResponseBodyIncomplete RequestPhase = "RESPONSE_BODY_INCOMPLETE"
)

// ExecutionOutcomeUnknownError reports a statement submission whose server
// outcome cannot be inferred safely. It never includes statement text.
type ExecutionOutcomeUnknownError struct {
	SessionHandle string
	Method        string
	Endpoint      string
	RequestPhase  RequestPhase
	Cause         error
}

func (e *ExecutionOutcomeUnknownError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v: %s %s phase=%s session=%s", ErrExecutionOutcomeUnknown, e.Method, e.Endpoint, e.RequestPhase, MaskHandle(e.SessionHandle))
}

// Unwrap preserves the underlying timeout or transport error.
func (e *ExecutionOutcomeUnknownError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Is lets callers classify the error without losing the underlying cause.
func (e *ExecutionOutcomeUnknownError) Is(target error) bool {
	return target == ErrExecutionOutcomeUnknown
}

// APIError describes a transport or SQL Gateway error without exposing a
// response stack trace or request query parameters.
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

// ExecutionError preserves both the primary execution failure and any
// operation cleanup failures. No statement text or credentials are retained.
type ExecutionError struct {
	Cause           error
	CancelError     error
	CloseError      error
	SessionHandle   string
	OperationHandle string
	JobID           string
}

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

// Unwrap exposes every retained error to errors.Is and errors.As.
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

// ResultLimitError records which high-level execution bound was reached.
type ResultLimitError struct {
	Kind     string
	Limit    int
	Received int
}

func (e *ResultLimitError) Error() string {
	return fmt.Sprintf("%v: %s limit=%d received=%d", ErrResultLimit, e.Kind, e.Limit, e.Received)
}

// Unwrap makes ResultLimitError match ErrResultLimit.
func (e *ResultLimitError) Unwrap() error { return ErrResultLimit }

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

// Unwrap supports errors.Is and errors.As.
func (e *APIError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
