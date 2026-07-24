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

// ExecuteStatement는 SQL을 제출하고 비동기 operation handle을 반환한다. 중복 실행 위험 때문에
// POST를 의도적으로 자동 재시도하지 않는다.
func (c *GatewayClient) ExecuteStatement(ctx context.Context, sessionHandle string, req ExecuteStatementRequest) (*Operation, error) {
	if err := validateSessionHandle(sessionHandle); err != nil {
		return nil, err
	}
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
		if errors.As(err, &apiErr) && statementOutcomeIsUnknown(apiErr) {
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
	if handleErr := validateOperationHandle(response.Handle); handleErr != nil {
		cause := fmt.Errorf("flinksqlgateway: execute response has invalid operationHandle: %w", handleErr)
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

// statementOutcomeIsUnknown은 비멱등 SQL 제출이 server에서 실행됐을 가능성을 보수적으로 판별한다.
func statementOutcomeIsUnknown(apiErr *APIError) bool {
	if apiErr == nil || apiErr.RequestPhase == "" || apiErr.RequestPhase == RequestNotSent {
		return false
	}
	if apiErr.StatusCode == 0 {
		return true
	}
	if apiErr.StatusCode >= http.StatusOK && apiErr.StatusCode < http.StatusMultipleChoices {
		return true
	}
	return apiErr.StatusCode == http.StatusRequestTimeout ||
		apiErr.StatusCode == http.StatusTooManyRequests ||
		apiErr.StatusCode >= http.StatusInternalServerError
}

// GetOperationStatus는 이후 Flink 상태도 보존하는 열린 operation 상태값을 반환한다.
func (c *GatewayClient) GetOperationStatus(ctx context.Context, sessionHandle, operationHandle string) (OperationStatus, error) {
	if err := validateSessionHandle(sessionHandle); err != nil {
		return "", err
	}
	if err := validateOperationHandle(operationHandle); err != nil {
		return "", err
	}
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

// FetchResults는 지정한 결과 token을 가져온다. Flink v1은 server 기본 JSON만 사용하고
// rowFormat을 받지 않으므로 PLAIN_TEXT는 v2 이상이 필요하다.
func (c *GatewayClient) FetchResults(ctx context.Context, sessionHandle, operationHandle string, token int64, rowFormat RowFormat) (*ResultPage, error) {
	if err := validateSessionHandle(sessionHandle); err != nil {
		return nil, err
	}
	if err := validateOperationHandle(operationHandle); err != nil {
		return nil, err
	}
	if token < 0 {
		return nil, fmt.Errorf("flinksqlgateway: result token must not be negative")
	}
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

// fetchResultsURL은 검증을 마친 paging URL에서 결과와 실제 응답 byte 수를 함께 반환한다.
func (c *GatewayClient) fetchResultsURL(ctx context.Context, target *url.URL) (*ResultPage, error) {
	var page ResultPage
	responseBytes, err := c.doJSON(ctx, http.MethodGet, target, nil, &page, true)
	if err != nil {
		return nil, err
	}
	page.ResponseBytes = responseBytes
	return &page, nil
}

// CancelOperation은 operation 취소를 요청하며 POST를 자동 재시도하지 않는다.
func (c *GatewayClient) CancelOperation(ctx context.Context, sessionHandle, operationHandle string) error {
	if err := validateSessionHandle(sessionHandle); err != nil {
		return err
	}
	if err := validateOperationHandle(operationHandle); err != nil {
		return err
	}
	if err := c.CheckAPIVersion(ctx); err != nil {
		return err
	}
	target, _ := c.endpointURL(true, operationRoute(sessionHandle, operationHandle)+"/cancel")
	_, err := c.doJSON(ctx, http.MethodPost, target, struct{}{}, nil, false)
	return err
}

// CloseOperation은 operation 자원을 해제하며 DELETE를 자동 재시도하지 않는다.
// not-found 응답은 멱등인 종료 성공으로 취급한다.
func (c *GatewayClient) CloseOperation(ctx context.Context, sessionHandle, operationHandle string) error {
	if err := validateSessionHandle(sessionHandle); err != nil {
		return err
	}
	if err := validateOperationHandle(operationHandle); err != nil {
		return err
	}
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

// operationRoute는 session과 operation handle을 path segment로 escape해 공통 route를 만든다.
func operationRoute(sessionHandle, operationHandle string) string {
	return "/sessions/" + pathSegment(sessionHandle) + "/operations/" + pathSegment(operationHandle)
}

// operationFailure는 fetch 오류가 terminal operation 실패에서 비롯됐는지 추가로 분류한다.
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

// nextURLString은 nil page와 주변 공백을 안전하게 처리한 다음 paging URI를 반환한다.
func nextURLString(page *ResultPage) string {
	if page == nil {
		return ""
	}
	return strings.TrimSpace(page.NextResultURI)
}
