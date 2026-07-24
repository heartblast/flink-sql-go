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
)

const maxExposedErrorBytes = 512

// Client is safe for concurrent use.
type Client struct {
	cfg        Config
	baseURL    *url.URL
	httpClient *http.Client
	owned      bool
	mu         sync.RWMutex
	closed     bool
	closeOnce  sync.Once
}

// NewClient validates configuration without making a network request.
func NewClient(cfg Config) (*Client, error) {
	normalized, base, httpClient, owned, err := cfg.normalize()
	if err != nil {
		return nil, err
	}
	return &Client{cfg: normalized, baseURL: base, httpClient: httpClient, owned: owned}, nil
}

func (c *Client) GetJob(ctx context.Context, jobID string) (*Job, error) {
	var result Job
	if err := c.doJSON(ctx, http.MethodGet, c.jobURL(jobID, ""), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) GetJobStatus(ctx context.Context, jobID string) (JobStatus, error) {
	var result struct {
		Status JobStatus `json:"status"`
	}
	if err := c.doJSON(ctx, http.MethodGet, c.jobURL(jobID, "/status"), nil, &result); err != nil {
		return "", err
	}
	return result.Status, nil
}

// CancelJob explicitly terminates a Flink job. SQL operation cancellation
// never calls this method automatically.
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

func (c *Client) GetJobExceptions(ctx context.Context, jobID string) (*JobExceptions, error) {
	var result JobExceptions
	if err := c.doJSON(ctx, http.MethodGet, c.jobURL(jobID, "/exceptions"), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) GetCheckpoints(ctx context.Context, jobID string) (*Checkpoints, error) {
	var result Checkpoints
	if err := c.doJSON(ctx, http.MethodGet, c.jobURL(jobID, "/checkpoints"), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) GetJobPlan(ctx context.Context, jobID string) (*JobPlan, error) {
	var result JobPlan
	if err := c.doJSON(ctx, http.MethodGet, c.jobURL(jobID, "/plan"), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Close is idempotent and only closes idle connections on an owned transport.
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

func (c *Client) jobURL(jobID, suffix string) *url.URL {
	if c.validateJobID(jobID) != nil {
		return nil
	}
	target := *c.baseURL
	target.Path = strings.TrimRight(c.baseURL.Path, "/") + "/jobs/" + url.PathEscape(jobID) + suffix
	return &target
}

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
	if newline := strings.IndexAny(message, "\r\n"); newline >= 0 {
		message = message[:newline]
	}
	if len(message) > maxExposedErrorBytes {
		message = message[:maxExposedErrorBytes] + "..."
	}
	return &APIError{Method: method, Endpoint: target.EscapedPath(), StatusCode: status, Message: message}
}
