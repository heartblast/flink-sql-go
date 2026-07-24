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

// maxExposedErrorBytes는 server 오류에서 첫 줄만 노출할 때 허용하는 최대 byte 수이다.
const maxExposedErrorBytes = 512

// requestProgress는 transport 오류 시 요청이 server에 도달했을 가능성을 보수적으로 추적한다.
type requestProgress int32

const (
	// 다음 단계는 요청 미전송부터 응답 body 시작까지 단방향으로만 전이한다.
	requestProgressNotSent requestProgress = iota
	requestProgressPossiblySent
	requestProgressWritten
	requestProgressResponseStarted
)

// advanceRequestProgress는 concurrent httptrace callback에서 진행 단계를 뒤로 돌리지 않고 갱신한다.
func advanceRequestProgress(progress *atomic.Int32, next requestProgress) {
	for {
		current := progress.Load()
		if current >= int32(next) || progress.CompareAndSwap(current, int32(next)) {
			return
		}
	}
}

// requestPhaseForProgress는 내부 transport 진행 상태를 공개 오류 분류값으로 변환한다.
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

// endpointURL은 configured base URL에 선택적인 API 버전과 route를 결합한다.
func (c *GatewayClient) endpointURL(versioned bool, route string) (*url.URL, error) {
	base := strings.TrimRight(c.baseURL.String(), "/")
	prefix := ""
	if versioned {
		prefix = "/" + c.cfg.APIVersion
	}
	return url.Parse(base + prefix + "/" + strings.TrimLeft(route, "/"))
}

// doJSON은 payload와 응답 제한을 적용하며 safeRetry 요청만 최대 한 번 다시 호출한다.
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

// doJSONOnce는 하나의 HTTP 요청 진행 단계를 추적하고 정제된 오류와 관측값을 반환한다.
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

// readLimited는 limit보다 한 byte만 더 읽어 응답이 상한을 넘었는지 판별한다.
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

// decodeAPIError는 제한된 server 메시지와 endpoint 문맥을 typed error로 변환한다.
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

// sanitizeEndpointPath는 관측값과 오류에 포함된 session 및 operation handle을 마스킹한다.
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

// knownSessionInPath는 not-found 응답을 session 만료와 미존재로 구분할 로컬 문맥을 확인한다.
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

// sanitizeServerMessage는 stack trace를 제거하고 노출 가능한 첫 줄의 크기를 제한한다.
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

// observe는 Observer panic을 client 동작에서 격리해 정제된 요청 telemetry를 전달한다.
func (c *GatewayClient) observe(ctx context.Context, observation RequestObservation) {
	if c.cfg.Observer == nil {
		return
	}
	defer func() { _ = recover() }()
	c.cfg.Observer.ObserveRequest(ctx, observation)
}

// waitContext는 timer를 누수하지 않고 지정 시간 또는 context 취소까지 기다린다.
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

// pathSegment는 불투명한 handle을 URL path 한 segment로 escape한다.
func pathSegment(value string) string { return url.PathEscape(value) }

// isContextError는 오류 chain에서 취소 또는 deadline 초과를 분류한다.
func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
