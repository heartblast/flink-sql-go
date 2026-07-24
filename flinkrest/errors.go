package flinkrest

import (
	"errors"
	"fmt"
)

var (
	ErrClientClosed     = errors.New("flink job REST client is closed")
	ErrResponseTooLarge = errors.New("flink job REST response too large")
	ErrInvalidJobID     = errors.New("invalid Flink job ID")
)

// APIError describes a JobManager REST error without query strings or
// credentials.
type APIError struct {
	Method     string
	Endpoint   string
	StatusCode int
	Message    string
	Cause      error
}

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

func (e *APIError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
