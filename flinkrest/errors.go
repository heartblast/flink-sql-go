package flinkrest

import (
	"errors"
	"fmt"
)

var (
	// ErrClientClosed는 종료된 client로 새 요청을 시작했음을 나타낸다.
	ErrClientClosed = errors.New("flink job REST client is closed")
	// ErrResponseTooLarge는 응답 body가 설정된 최대 크기를 넘었음을 나타낸다.
	ErrResponseTooLarge = errors.New("flink job REST response too large")
	// ErrInvalidJobID는 비어 있거나 형식이 올바르지 않은 Flink Job ID를 나타낸다.
	ErrInvalidJobID = errors.New("invalid Flink job ID")
)

// APIError는 query string이나 인증정보를 포함하지 않고 JobManager REST 오류를 설명한다.
type APIError struct {
	Method     string
	Endpoint   string
	StatusCode int
	Message    string
	Cause      error
}

// Error는 민감정보가 제거된 REST 오류 메시지를 반환한다.
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
	return fmt.Sprintf("flink job REST %s %s%s: %s", e.Method, e.Endpoint, status, message)
}

// Unwrap은 errors.Is와 errors.As가 원인 오류를 탐색할 수 있게 한다.
func (e *APIError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
