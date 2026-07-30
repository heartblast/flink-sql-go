package flinksqlgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sort"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// maxExposedErrorBytes는 server 오류에서 첫 줄만 노출할 때 허용하는 최대 byte 수이다.
const maxExposedErrorBytes = 512

// requestProgress는 transport 오류 시 요청이 server에 도달했을 가능성을 보수적으로 추적한다.
type requestProgress int32

// serverMessageRedaction은 server가 request SQL이나 secret을 오류에 반사할 때 반환 전에
// 치환할 값을 전달한다. context 밖으로 원본 값을 보관하지 않는다.
type serverMessageRedaction struct {
	fragments []string
	redactAll bool
}

type serverMessageRedactionContextKey struct{}

// withServerMessageRedaction은 단일 HTTP 요청 범위에만 server 오류 redaction 규칙을 연결한다.
func withServerMessageRedaction(ctx context.Context, redaction serverMessageRedaction) context.Context {
	return context.WithValue(ctx, serverMessageRedactionContextKey{}, redaction)
}

// serverMessageRedactionFromContext는 transport가 관측 전에 적용할 redaction 규칙을 읽는다.
func serverMessageRedactionFromContext(ctx context.Context) serverMessageRedaction {
	redaction, _ := ctx.Value(serverMessageRedactionContextKey{}).(serverMessageRedaction)
	return redaction
}

// requestMessageRedaction은 request별 SQL과 client 공통 header 값을 합쳐 server 반사
// 메시지와 transport 오류가 오류 또는 Observer로 전달되기 전에 치환하게 한다.
func (c *GatewayClient) requestMessageRedaction(ctx context.Context, target *url.URL) serverMessageRedaction {
	source := serverMessageRedactionFromContext(ctx)
	redaction := serverMessageRedaction{
		fragments: append([]string(nil), source.fragments...),
		redactAll: source.redactAll,
	}
	for _, value := range c.cfg.Headers {
		if value != "" {
			redaction.fragments = append(redaction.fragments, value)
		}
	}
	if target != nil && target.RawQuery != "" {
		redaction.fragments = append(redaction.fragments, target.String(), target.RawQuery)
	}
	return redaction
}

// sanitizedTransportError는 원래 network 오류의 errors.Is 및 net.Error 분류는 유지하되,
// query나 secret을 포함할 수 있는 원문 Error와 url.Error를 외부 chain에 노출하지 않는다.
type sanitizedTransportError struct {
	message   string
	cause     error
	timeout   bool
	temporary bool
}

// Error는 redaction과 길이 제한을 적용한 transport 오류 설명을 반환한다.
func (e *sanitizedTransportError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return e.message
}

// Is는 원본 오류를 unwrap하지 않고도 sentinel 분류를 유지한다.
func (e *sanitizedTransportError) Is(target error) bool {
	return e != nil && errors.Is(e.cause, target)
}

// Timeout은 원본 net.Error의 timeout 분류를 보존한다.
func (e *sanitizedTransportError) Timeout() bool {
	return e != nil && e.timeout
}

// Temporary는 원본 net.Error의 temporary 분류를 보존한다.
func (e *sanitizedTransportError) Temporary() bool {
	return e != nil && e.temporary
}

// sanitizeTransportError는 요청 URL, SQL 및 header 반사값을 제거한 안전한 오류를 만든다.
func sanitizeTransportError(cause error, redaction serverMessageRedaction) error {
	if cause == nil {
		return nil
	}
	if cause == context.Canceled || cause == context.DeadlineExceeded {
		return cause
	}
	message := ""
	var urlErr *url.Error
	if errors.As(cause, &urlErr) {
		inner := "request failed"
		if urlErr.Err != nil {
			inner = sanitizeServerMessage(redactServerMessage(urlErr.Err.Error(), redaction))
		}
		if inner == "" {
			inner = "request failed"
		}
		message = fmt.Sprintf("%s %q: %s", sanitizeServerMessage(urlErr.Op), sanitizeTransportURL(urlErr.URL), inner)
	} else {
		message = sanitizeServerMessage(redactServerMessage(cause.Error(), redaction))
	}
	if message == "" {
		message = "request failed"
	}
	sanitized := &sanitizedTransportError{message: message, cause: cause}
	if netErr, ok := cause.(net.Error); ok {
		sanitized.timeout = netErr.Timeout()
		sanitized.temporary = netErr.Temporary()
	}
	return sanitized
}

// sanitizeTransportURL은 url.Error에 포함된 query, fragment, userinfo와 handle 원문을 제거한다.
func sanitizeTransportURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "********"
	}
	path := sanitizeEndpointPath(parsed.EscapedPath())
	if path == "" {
		return "/"
	}
	return path
}

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
		prefix = "/" + c.selectedAPIVersion()
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
		if !safeRetry {
			var apiErr *APIError
			if errors.As(requestErr, &apiErr) {
				apiErr.Retryable = false
			}
		}
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
	redaction := c.requestMessageRedaction(req.Context(), target)

	started := time.Now()
	resp, err := c.httpClient.Do(req)
	if err != nil {
		cause := err
		if requestCtx.Err() != nil {
			cause = requestCtx.Err()
		}
		cause = sanitizeTransportError(cause, redaction)
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
		apiErr := c.decodeAPIErrorWithRedaction(method, target, resp.StatusCode, data, redaction)
		apiErr.RequestPhase = ResponseReceived
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
	return c.decodeAPIErrorWithRedaction(method, target, status, data, serverMessageRedaction{})
}

// decodeAPIErrorWithRedaction은 server 메시지를 분류한 뒤 request별 secret을 관측 전에 제거한다.
func (c *GatewayClient) decodeAPIErrorWithRedaction(method string, target *url.URL, status int, data []byte, redaction serverMessageRedaction) *APIError {
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
	classificationMessage := sanitizeServerMessage(message)
	message = sanitizeServerMessage(redactServerMessage(message, redaction))

	retryable := status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
	var kind error
	lowerPath := strings.ToLower(target.EscapedPath())
	lowerMessage := strings.ToLower(classificationMessage)
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
		Code:       sanitizeServerMessage(redactServerMessage(payload.Code, redaction)),
		Message:    message,
		Retryable:  retryable,
		Cause:      kind,
	}
}

// redactServerMessage는 긴 값을 먼저 치환해 서로 겹치는 secret도 원문이 남지 않게 한다.
// redactAll은 raw table DDL처럼 개별 secret을 안전하게 추출할 수 없는 경우에 사용한다.
func redactServerMessage(message string, redaction serverMessageRedaction) string {
	if message == "" {
		return ""
	}
	if redaction.redactAll {
		return "********"
	}
	fragments := make([]string, 0, len(redaction.fragments))
	seen := make(map[string]struct{}, len(redaction.fragments))
	for _, fragment := range redaction.fragments {
		if fragment == "" {
			continue
		}
		if _, exists := seen[fragment]; exists {
			continue
		}
		seen[fragment] = struct{}{}
		fragments = append(fragments, fragment)
	}
	sort.Slice(fragments, func(left, right int) bool {
		return len(fragments[left]) > len(fragments[right])
	})
	for _, fragment := range fragments {
		message = strings.ReplaceAll(message, fragment, "********")
	}
	return message
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
	message = strings.ToValidUTF8(message, "�")
	message = strings.TrimSpace(message)
	if index := strings.IndexAny(message, "\r\n"); index >= 0 {
		message = message[:index]
	}
	if len(message) > maxExposedErrorBytes {
		limit := maxExposedErrorBytes
		for limit > 0 && !utf8.ValidString(message[:limit]) {
			limit--
		}
		message = message[:limit] + "..."
	}
	return message
}

// observe는 Observer panic을 client 동작에서 격리해 정제된 요청 telemetry를 전달한다.
func (c *GatewayClient) observe(ctx context.Context, observation RequestObservation) {
	if c.cfg.Observer == nil {
		return
	}
	c.runObserver(func() { c.cfg.Observer.ObserveRequest(ctx, observation) })
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
