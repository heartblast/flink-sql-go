package flinksqlgateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ExecuteStatement submits SQL and returns an asynchronous operation handle.
// The POST is intentionally never retried.
func (c *GatewayClient) ExecuteStatement(ctx context.Context, sessionHandle string, req ExecuteStatementRequest) (*Operation, error) {
	if err := c.validateStatement(ctx, sessionHandle, req.Statement); err != nil {
		return nil, err
	}
	if err := c.CheckAPIVersion(ctx); err != nil {
		return nil, err
	}
	timeout := req.ExecutionTimeout
	if timeout <= 0 {
		timeout = c.cfg.ExecutionTimeout
	}
	body := struct {
		Statement        string            `json:"statement"`
		ExecutionTimeout int64             `json:"executionTimeout"`
		ExecutionConfig  map[string]string `json:"executionConfig,omitempty"`
	}{req.Statement, timeout.Milliseconds(), req.ExecutionConfig}
	target, _ := c.endpointURL(true, "/sessions/"+pathSegment(sessionHandle)+"/statements")
	endpoint := sanitizeEndpointPath(target.EscapedPath())
	c.observeLifecycle(ctx, Observation{
		Event:         ObservationStatementSubmitting,
		Method:        http.MethodPost,
		Endpoint:      endpoint,
		SessionHandle: sessionHandle,
	})
	var response struct {
		Handle string `json:"operationHandle"`
	}
	if _, err := c.doJSON(ctx, http.MethodPost, target, body, &response, false); err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.RequestPhase != "" && apiErr.RequestPhase != RequestNotSent {
			unknown := &ExecutionOutcomeUnknownError{
				SessionHandle: sessionHandle,
				Method:        http.MethodPost,
				Endpoint:      endpoint,
				RequestPhase:  apiErr.RequestPhase,
				Cause:         err,
			}
			c.observeLifecycle(ctx, Observation{
				Event:         ObservationStatementOutcomeUnknown,
				Method:        http.MethodPost,
				Endpoint:      endpoint,
				SessionHandle: sessionHandle,
				Error:         unknown,
			})
			return nil, unknown
		}
		c.observeLifecycle(ctx, Observation{
			Event:         ObservationStatementFailed,
			Method:        http.MethodPost,
			Endpoint:      endpoint,
			SessionHandle: sessionHandle,
			Error:         err,
		})
		return nil, err
	}
	if response.Handle == "" {
		cause := fmt.Errorf("flinksqlgateway: execute response has no operationHandle")
		unknown := &ExecutionOutcomeUnknownError{
			SessionHandle: sessionHandle,
			Method:        http.MethodPost,
			Endpoint:      endpoint,
			RequestPhase:  RequestPossiblySent,
			Cause:         cause,
		}
		c.observeLifecycle(ctx, Observation{Event: ObservationStatementOutcomeUnknown, Method: http.MethodPost, Endpoint: endpoint, SessionHandle: sessionHandle, Error: unknown})
		return nil, unknown
	}
	operation := &Operation{Handle: response.Handle, SessionHandle: sessionHandle, CreatedAt: time.Now()}
	c.observeLifecycle(ctx, Observation{
		Event:           ObservationStatementSubmitted,
		Method:          http.MethodPost,
		Endpoint:        endpoint,
		SessionHandle:   sessionHandle,
		OperationHandle: response.Handle,
	})
	return operation, nil
}

// GetOperationStatus returns an open-ended Flink operation status.
func (c *GatewayClient) GetOperationStatus(ctx context.Context, sessionHandle, operationHandle string) (OperationStatus, error) {
	if err := c.CheckAPIVersion(ctx); err != nil {
		return "", err
	}
	target, _ := c.endpointURL(true, operationRoute(sessionHandle, operationHandle)+"/status")
	var response struct {
		Status OperationStatus `json:"status"`
	}
	if _, err := c.doJSON(ctx, http.MethodGet, target, nil, &response, true); err != nil {
		return "", err
	}
	return response.Status, nil
}

// FetchResults fetches a result token. In Flink v1 the server always uses its
// JSON default and does not accept rowFormat; PLAIN_TEXT therefore requires v2.
func (c *GatewayClient) FetchResults(ctx context.Context, sessionHandle, operationHandle string, token int64, rowFormat RowFormat) (*ResultPage, error) {
	if err := c.CheckAPIVersion(ctx); err != nil {
		return nil, err
	}
	if rowFormat == "" {
		rowFormat = c.cfg.DefaultRowFormat
	}
	if !rowFormat.valid() {
		return nil, fmt.Errorf("flinksqlgateway: unsupported row format %q", rowFormat)
	}
	if apiVersionNumber(c.cfg.APIVersion) == 1 && rowFormat != RowFormatJSON {
		return nil, fmt.Errorf("%w: PLAIN_TEXT results require v2 or newer", ErrUnsupportedAPI)
	}
	target, _ := c.endpointURL(true, fmt.Sprintf("%s/result/%d", operationRoute(sessionHandle, operationHandle), token))
	if apiVersionNumber(c.cfg.APIVersion) >= 2 {
		query := target.Query()
		query.Set("rowFormat", string(rowFormat))
		target.RawQuery = query.Encode()
	}
	return c.fetchResultsURL(ctx, target)
}

func (c *GatewayClient) fetchResultsURL(ctx context.Context, target *url.URL) (*ResultPage, error) {
	var page ResultPage
	responseBytes, err := c.doJSON(ctx, http.MethodGet, target, nil, &page, true)
	if err != nil {
		return nil, err
	}
	page.ResponseBytes = responseBytes
	return &page, nil
}

// CancelOperation requests cancellation and never retries the POST.
func (c *GatewayClient) CancelOperation(ctx context.Context, sessionHandle, operationHandle string) error {
	if err := c.CheckAPIVersion(ctx); err != nil {
		return err
	}
	target, _ := c.endpointURL(true, operationRoute(sessionHandle, operationHandle)+"/cancel")
	_, err := c.doJSON(ctx, http.MethodPost, target, struct{}{}, nil, false)
	return err
}

// CloseOperation releases operation resources and never retries the DELETE.
// A not-found response is treated as an idempotent close.
func (c *GatewayClient) CloseOperation(ctx context.Context, sessionHandle, operationHandle string) error {
	if err := c.CheckAPIVersion(ctx); err != nil {
		return err
	}
	target, _ := c.endpointURL(true, operationRoute(sessionHandle, operationHandle)+"/close")
	_, err := c.doJSON(ctx, http.MethodDelete, target, nil, nil, false)
	if errors.Is(err, ErrOperationNotFound) {
		return nil
	}
	return err
}

func operationRoute(sessionHandle, operationHandle string) string {
	return "/sessions/" + pathSegment(sessionHandle) + "/operations/" + pathSegment(operationHandle)
}

func (c *GatewayClient) operationFailure(ctx context.Context, sessionHandle, operationHandle string, fetchErr error) error {
	if isContextError(fetchErr) || errors.Is(fetchErr, ErrResultLimit) {
		return fetchErr
	}
	status, err := c.GetOperationStatus(ctx, sessionHandle, operationHandle)
	if err == nil && status.Terminal() && !status.Successful() {
		return fmt.Errorf("%w: status=%s", ErrOperationFailed, status)
	}
	return fetchErr
}

func nextURLString(page *ResultPage) string {
	if page == nil {
		return ""
	}
	return strings.TrimSpace(page.NextResultURI)
}
