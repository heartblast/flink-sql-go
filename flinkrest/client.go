package flinkrest

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"unicode/utf8"
)

// maxExposedErrorBytes는 API 오류에 노출할 서버 메시지의 최대 byte 수이다.
const maxExposedErrorBytes = 512

// Client는 Flink JobManager REST API를 호출하며 여러 goroutine에서 안전하게 사용할 수 있다.
type Client struct {
	cfg        Config
	baseURL    *url.URL
	httpClient *http.Client
	owned      bool
	mu         sync.RWMutex
	closed     bool
	closeOnce  sync.Once
}

// NewClient는 네트워크 요청 없이 설정을 검증하고 JobManager REST client를 생성한다.
func NewClient(cfg Config) (*Client, error) {
	normalized, base, httpClient, owned, err := cfg.normalize()
	if err != nil {
		return nil, err
	}
	return &Client{cfg: normalized, baseURL: base, httpClient: httpClient, owned: owned}, nil
}

// GetJob은 Job 상세 정보와 서버가 반환한 원본 JSON을 조회한다.
func (c *Client) GetJob(ctx context.Context, jobID string) (*Job, error) {
	var result Job
	if err := c.doJSON(ctx, http.MethodGet, c.jobURL(jobID, ""), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetJobStatus는 Job의 현재 실행 상태를 조회한다.
func (c *Client) GetJobStatus(ctx context.Context, jobID string) (JobStatus, error) {
	var result struct {
		Status JobStatus `json:"status"`
	}
	if err := c.doJSON(ctx, http.MethodGet, c.jobURL(jobID, "/status"), nil, &result); err != nil {
		return "", err
	}
	return result.Status, nil
}

// CancelJob은 Flink Job을 명시적으로 종료한다. SQL operation 취소는 이 메서드를 자동 호출하지 않는다.
func (c *Client) CancelJob(ctx context.Context, jobID string) error {
	target := c.jobURL(jobID, "")
	if target == nil {
		return c.validateJobID(jobID)
	}
	query := target.Query()
	query.Set("mode", "cancel")
	target.RawQuery = query.Encode()
	return c.doJSON(ctx, http.MethodPatch, target, nil, nil)
}

// StopJob은 선택적으로 savepoint를 생성하며 Job 중지 작업을 시작한다.
func (c *Client) StopJob(ctx context.Context, jobID string, options StopOptions) (*TriggerResponse, error) {
	if options.FormatType != "" && options.FormatType != SavepointCanonical && options.FormatType != SavepointNative {
		return nil, fmt.Errorf("flinkrest: unsupported savepoint format %q", options.FormatType)
	}
	body := struct {
		Drain           bool            `json:"drain"`
		FormatType      SavepointFormat `json:"formatType,omitempty"`
		TargetDirectory string          `json:"targetDirectory,omitempty"`
		TriggerID       string          `json:"triggerId,omitempty"`
	}{options.Drain, options.FormatType, options.TargetDirectory, options.TriggerID}
	var result TriggerResponse
	if err := c.doJSON(ctx, http.MethodPost, c.jobURL(jobID, "/stop"), body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetJobExceptions는 Job의 예외 이력과 원본 응답을 조회한다.
func (c *Client) GetJobExceptions(ctx context.Context, jobID string) (*JobExceptions, error) {
	var result JobExceptions
	if err := c.doJSON(ctx, http.MethodGet, c.jobURL(jobID, "/exceptions"), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetCheckpoints는 Job의 checkpoint 통계와 이력을 조회한다.
func (c *Client) GetCheckpoints(ctx context.Context, jobID string) (*Checkpoints, error) {
	var result Checkpoints
	if err := c.doJSON(ctx, http.MethodGet, c.jobURL(jobID, "/checkpoints"), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetJobPlan은 Job 실행 계획과 원본 응답을 조회한다.
func (c *Client) GetJobPlan(ctx context.Context, jobID string) (*JobPlan, error) {
	var result JobPlan
	if err := c.doJSON(ctx, http.MethodGet, c.jobURL(jobID, "/plan"), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Close는 중복 호출에 안전하며 client가 소유한 transport의 idle connection만 정리한다.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		if c.owned {
			c.httpClient.CloseIdleConnections()
		}
	})
	return nil
}

// jobURL은 검증된 Job ID와 endpoint suffix를 결합해 요청 URL을 만든다.
func (c *Client) jobURL(jobID, suffix string) *url.URL {
	if c.validateJobID(jobID) != nil {
		return nil
	}
	target := *c.baseURL
	target.Path = strings.TrimRight(c.baseURL.Path, "/") + "/jobs/" + url.PathEscape(jobID) + suffix
	return &target
}

// validateJobID는 설정에 따라 Job ID가 32자리 16진수인지 검증한다.
func (c *Client) validateJobID(jobID string) error {
	if jobID == "" {
		return fmt.Errorf("%w: value is empty", ErrInvalidJobID)
	}
	if !c.cfg.ValidateJobID {
		return nil
	}
	if len(jobID) != 32 {
		return fmt.Errorf("%w: expected 32 hexadecimal characters", ErrInvalidJobID)
	}
	if _, err := hex.DecodeString(jobID); err != nil {
		return fmt.Errorf("%w: expected hexadecimal characters", ErrInvalidJobID)
	}
	return nil
}

// doJSON은 공통 제한과 header를 적용해 단일 REST 요청을 수행하고 JSON 응답을 해석한다.
func (c *Client) doJSON(ctx context.Context, method string, target *url.URL, body, destination any) error {
	if target == nil {
		return ErrInvalidJobID
	}
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return ErrClientClosed
	}
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("flinkrest: encode request: %w", err)
		}
	}
	requestCtx := ctx
	cancel := func() {}
	if c.cfg.RequestTimeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, c.cfg.RequestTimeout)
	}
	defer cancel()
	var reader io.Reader
	if payload != nil {
		reader = bytes.NewReader(payload)
	}
	request, err := http.NewRequestWithContext(requestCtx, method, target.String(), reader)
	if err != nil {
		return fmt.Errorf("flinkrest: create request: %w", err)
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", c.cfg.UserAgent)
	for key, value := range c.cfg.Headers {
		request.Header.Set(key, value)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		cause := err
		if requestCtx.Err() != nil {
			cause = requestCtx.Err()
		}
		return &APIError{Method: method, Endpoint: target.EscapedPath(), Cause: cause}
	}
	defer response.Body.Close()
	data, err := readLimited(response.Body, c.cfg.MaxResponseBytes)
	if err != nil {
		return &APIError{Method: method, Endpoint: target.EscapedPath(), StatusCode: response.StatusCode, Cause: err}
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return decodeAPIError(method, target, response.StatusCode, data)
	}
	if destination != nil && len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, destination); err != nil {
			return &APIError{Method: method, Endpoint: target.EscapedPath(), StatusCode: response.StatusCode, Message: "invalid JSON response", Cause: err}
		}
	}
	return nil
}

// readLimited는 limit를 넘는 응답을 메모리에 계속 읽지 않고 크기 초과 오류로 반환한다.
func readLimited(reader io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return data, err
	}
	if int64(len(data)) > limit {
		return data[:limit], ErrResponseTooLarge
	}
	return data, nil
}

// decodeAPIError는 크기가 제한된 응답에서 사용자에게 안전하게 노출할 오류를 구성한다.
func decodeAPIError(method string, target *url.URL, status int, data []byte) *APIError {
	var payload struct {
		Message string   `json:"message"`
		Error   string   `json:"error"`
		Errors  []string `json:"errors"`
	}
	_ = json.Unmarshal(data, &payload)
	message := payload.Message
	if message == "" {
		message = payload.Error
	}
	if message == "" && len(payload.Errors) > 0 {
		message = payload.Errors[0]
	}
	if message == "" {
		message = string(data)
	}
	message = strings.TrimSpace(message)
	message = strings.ToValidUTF8(message, "�")
	if newline := strings.IndexAny(message, "\r\n"); newline >= 0 {
		message = message[:newline]
	}
	if len(message) > maxExposedErrorBytes {
		limit := maxExposedErrorBytes
		for limit > 0 && !utf8.ValidString(message[:limit]) {
			limit--
		}
		message = message[:limit] + "..."
	}
	return &APIError{Method: method, Endpoint: target.EscapedPath(), StatusCode: status, Message: message}
}
