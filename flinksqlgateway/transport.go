package flinksqlgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

const maxExposedErrorBytes = 512

type requestProgress int32

const (
	requestProgressNotSent requestProgress = iota
	requestProgressPossiblySent
	requestProgressWritten
	requestProgressResponseStarted
)

func advanceRequestProgress(progress *atomic.Int32, next requestProgress) {
	for {
		current := progress.Load()
		if current >= int32(next) || progress.CompareAndSwap(current, int32(next)) {
			return
		}
	}
}

func requestPhaseForProgress(progress requestProgress) RequestPhase {
	switch progress {
	case requestProgressPossiblySent:
		return RequestPossiblySent
	case requestProgressWritten:
		return ResponseHeaderMissing
	case requestProgressResponseStarted:
		return ResponseBodyIncomplete
	default:
		return RequestNotSent
	}
}

func (c *GatewayClient) endpointURL(versioned bool, route string) (*url.URL, error) {
	base := strings.TrimRight(c.baseURL.String(), "/")
	prefix := ""
	if versioned {
		prefix = "/" + c.cfg.APIVersion
	}
	return url.Parse(base + prefix + "/" + strings.TrimLeft(route, "/"))
}

func (c *GatewayClient) doJSON(
	ctx context.Context,
	method string,
	target *url.URL,
	body any,
	destination any,
	safeRetry bool,
) (int64, error) {
	if err := c.ensureOpen(ctx); err != nil {
		return 0, err
	}
	if !isInternalCleanup(ctx) {
		requestCtx, cancel := mergeContext(ctx, c.lifecycleCtx)
		defer cancel()
		ctx = requestCtx
	}
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return 0, fmt.Errorf("flinksqlgateway: encode request: %w", err)
		}
		if int64(len(payload)) > c.cfg.MaxResponseBytes {
			return 0, fmt.Errorf("%w: request body exceeds %d bytes", ErrResponseTooLarge, c.cfg.MaxResponseBytes)
		}
	}

	attempts := 1
	if safeRetry {
		attempts = 2
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			if err := waitContext(ctx, c.cfg.PollInterval); err != nil {
				return 0, err
			}
		}
		n, retry, requestErr := c.doJSONOnce(ctx, method, target, payload, destination)
		if requestErr == nil {
			return n, nil
		}
		lastErr = requestErr
		if !retry || !safeRetry {
			return n, requestErr
		}
	}
	return 0, lastErr
}

func (c *GatewayClient) doJSONOnce(
	ctx context.Context,
	method string,
	target *url.URL,
	payload []byte,
	destination any,
) (responseBytes int64, retry bool, resultErr error) {
	endpoint := sanitizeEndpointPath(target.EscapedPath())
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
	req, err := http.NewRequestWithContext(requestCtx, method, target.String(), reader)
	if err != nil {
		return 0, false, fmt.Errorf("flinksqlgateway: create request: %w", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.cfg.UserAgent)
	for key, value := range c.cfg.Headers {
		req.Header.Set(key, value)
	}
	var progress atomic.Int32
	trace := &httptrace.ClientTrace{
		WroteHeaders: func() {
			advanceRequestProgress(&progress, requestProgressPossiblySent)
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				advanceRequestProgress(&progress, requestProgressWritten)
				return
			}
			advanceRequestProgress(&progress, requestProgressPossiblySent)
		},
		GotFirstResponseByte: func() {
			advanceRequestProgress(&progress, requestProgressResponseStarted)
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	started := time.Now()
	resp, err := c.httpClient.Do(req)
	if err != nil {
		cause := err
		if requestCtx.Err() != nil {
			cause = requestCtx.Err()
		}
		apiErr := &APIError{
			Method:       method,
			Endpoint:     endpoint,
			Retryable:    ctx.Err() == nil,
			RequestPhase: requestPhaseForProgress(requestProgress(progress.Load())),
			Cause:        cause,
		}
		c.observe(ctx, RequestObservation{Method: method, Endpoint: endpoint, Duration: time.Since(started), Err: apiErr})
		return 0, ctx.Err() == nil, apiErr
	}
	defer resp.Body.Close()

	data, readErr := readLimited(resp.Body, c.cfg.MaxResponseBytes)
	responseBytes = int64(len(data))
	if readErr != nil {
		apiErr := &APIError{
			Method:       method,
			Endpoint:     endpoint,
			StatusCode:   resp.StatusCode,
			Retryable:    false,
			RequestPhase: ResponseBodyIncomplete,
			Cause:        readErr,
		}
		c.observe(ctx, RequestObservation{Method: method, Endpoint: endpoint, StatusCode: resp.StatusCode, Duration: time.Since(started), Err: apiErr})
		return responseBytes, false, apiErr
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		apiErr := c.decodeAPIError(method, target, resp.StatusCode, data)
		c.observe(ctx, RequestObservation{Method: method, Endpoint: endpoint, StatusCode: resp.StatusCode, Duration: time.Since(started), Err: apiErr})
		return responseBytes, apiErr.Retryable, apiErr
	}

	if destination != nil && len(bytes.TrimSpace(data)) != 0 {
		if err := json.Unmarshal(data, destination); err != nil {
			apiErr := &APIError{
				Method:       method,
				Endpoint:     endpoint,
				StatusCode:   resp.StatusCode,
				Message:      "invalid JSON response",
				RequestPhase: ResponseBodyIncomplete,
				Cause:        err,
			}
			c.observe(ctx, RequestObservation{Method: method, Endpoint: endpoint, StatusCode: resp.StatusCode, Duration: time.Since(started), Err: apiErr})
			return responseBytes, false, apiErr
		}
	}
	c.observe(ctx, RequestObservation{Method: method, Endpoint: endpoint, StatusCode: resp.StatusCode, Duration: time.Since(started)})
	return responseBytes, false, nil
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

func (c *GatewayClient) decodeAPIError(method string, target *url.URL, status int, data []byte) *APIError {
	var payload struct {
		Code    string   `json:"code"`
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
	message = sanitizeServerMessage(message)

	retryable := status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
	var kind error
	lowerPath := strings.ToLower(target.EscapedPath())
	lowerMessage := strings.ToLower(message)
	switch {
	case status == http.StatusGone || strings.Contains(lowerMessage, "session") && strings.Contains(lowerMessage, "expired"):
		kind = ErrSessionExpired
	case status == http.StatusNotFound && strings.Contains(lowerPath, "/operations/"):
		kind = ErrOperationNotFound
	case status == http.StatusNotFound && strings.Contains(lowerPath, "/sessions/"):
		if c.knownSessionInPath(target.Path) {
			kind = ErrSessionExpired
		} else {
			kind = ErrSessionNotFound
		}
	}
	return &APIError{
		Method:     method,
		Endpoint:   sanitizeEndpointPath(target.EscapedPath()),
		StatusCode: status,
		Code:       payload.Code,
		Message:    message,
		Retryable:  retryable,
		Cause:      kind,
	}
}

func sanitizeEndpointPath(endpointPath string) string {
	parts := strings.Split(endpointPath, "/")
	for index := 1; index < len(parts); index++ {
		if parts[index-1] != "sessions" && parts[index-1] != "operations" {
			continue
		}
		handle, err := url.PathUnescape(parts[index])
		if err != nil {
			parts[index] = "********"
			continue
		}
		parts[index] = MaskHandle(handle)
	}
	return strings.Join(parts, "/")
}

func (c *GatewayClient) knownSessionInPath(endpointPath string) bool {
	parts := strings.Split(strings.Trim(endpointPath, "/"), "/")
	for index := 0; index+1 < len(parts); index++ {
		if parts[index] != "sessions" {
			continue
		}
		handle, err := url.PathUnescape(parts[index+1])
		if err != nil {
			return false
		}
		c.stateMu.Lock()
		_, known := c.sessions[handle]
		c.stateMu.Unlock()
		return known
	}
	return false
}

func sanitizeServerMessage(message string) string {
	message = strings.TrimSpace(message)
	if index := strings.IndexAny(message, "\r\n"); index >= 0 {
		message = message[:index]
	}
	if len(message) > maxExposedErrorBytes {
		message = message[:maxExposedErrorBytes] + "..."
	}
	return message
}

func (c *GatewayClient) observe(ctx context.Context, observation RequestObservation) {
	if c.cfg.Observer == nil {
		return
	}
	defer func() { _ = recover() }()
	c.cfg.Observer.ObserveRequest(ctx, observation)
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func pathSegment(value string) string { return url.PathEscape(value) }

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
