package flinksqlgateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// maxRememberedClosedSessions는 Close 멱등성을 위해 보관할 최근 session handle의 상한이다.
const maxRememberedClosedSessions = 1024

// sessionCloseCall은 같은 session을 동시에 닫는 호출들이 첫 결과를 공유하게 한다.
type sessionCloseCall struct {
	done chan struct{}
	err  error
}

// GetInfo는 SQL Gateway 제품 metadata를 조회한다.
func (c *GatewayClient) GetInfo(ctx context.Context) (*GatewayInfo, error) {
	target, _ := c.endpointURL(false, "/info")
	var result GatewayInfo
	if _, err := c.doJSON(ctx, http.MethodGet, target, nil, &result, true); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetAPIVersions는 /api_versions가 광고하는 API 버전을 조회한다.
func (c *GatewayClient) GetAPIVersions(ctx context.Context) ([]string, error) {
	return c.getAPIVersions(ctx)
}

// getAPIVersions는 알려진 버전은 vN 형식으로 정규화하고 알 수 없는 값은 원문을 보존한다.
func (c *GatewayClient) getAPIVersions(ctx context.Context) ([]string, error) {
	target, _ := c.endpointURL(false, "/api_versions")
	var response struct {
		Versions []string `json:"versions"`
	}
	if _, err := c.doJSON(ctx, http.MethodGet, target, nil, &response, true); err != nil {
		return nil, err
	}
	versions := make([]string, 0, len(response.Versions))
	for _, version := range response.Versions {
		normalized, err := normalizeVersion(version)
		if err == nil {
			versions = append(versions, normalized)
		} else {
			versions = append(versions, version)
		}
	}
	return versions, nil
}

// OpenSession은 상태를 보존하는 Flink session을 만들며 POST를 자동 재시도하지 않는다.
func (c *GatewayClient) OpenSession(ctx context.Context, req OpenSessionRequest) (*Session, error) {
	if err := c.CheckAPIVersion(ctx); err != nil {
		return nil, err
	}
	target, _ := c.endpointURL(true, "/sessions")
	var response struct {
		Handle string `json:"sessionHandle"`
	}
	if _, err := c.doJSON(ctx, http.MethodPost, target, req, &response, false); err != nil {
		return nil, err
	}
	if response.Handle == "" {
		return nil, fmt.Errorf("flinksqlgateway: open session response has no sessionHandle")
	}
	if err := validateSessionHandle(response.Handle); err != nil {
		return nil, fmt.Errorf("flinksqlgateway: invalid open session response: %w", err)
	}
	record := &sessionRecord{
		handle:     response.Handle,
		name:       req.SessionName,
		properties: cloneMap(req.Properties),
		createdAt:  time.Now(),
	}
	c.stateMu.Lock()
	c.sessions[record.handle] = record
	c.forgetClosedSessionLocked(record.handle)
	c.stateMu.Unlock()
	return record.snapshot(), nil
}

// GetSessionConfig는 현재 server-side session property를 조회한다.
func (c *GatewayClient) GetSessionConfig(ctx context.Context, sessionHandle string) (map[string]string, error) {
	if err := validateSessionHandle(sessionHandle); err != nil {
		return nil, err
	}
	if err := c.CheckAPIVersion(ctx); err != nil {
		return nil, err
	}
	target, _ := c.endpointURL(true, "/sessions/"+pathSegment(sessionHandle))
	var response struct {
		Properties map[string]string `json:"properties"`
	}
	if _, err := c.doJSON(ctx, http.MethodGet, target, nil, &response, true); err != nil {
		return nil, err
	}
	return response.Properties, nil
}

// ConfigureSession은 profile과 protocol이 허용할 때 session 설정 statement를 실행하며 POST를
// 자동 재시도하지 않는다. executionTimeout은 항상 client-side 제한으로 적용한다.
func (c *GatewayClient) ConfigureSession(ctx context.Context, sessionHandle, statement string, executionTimeout time.Duration) error {
	if err := validateSessionHandle(sessionHandle); err != nil {
		return err
	}
	if err := c.validateStatement(ctx, sessionHandle, statement); err != nil {
		return err
	}
	if configured, known, err := c.configuredCompatibility(); err != nil {
		return err
	} else if known && !configured.Capabilities.ConfigureSession {
		return newCompatibilityError(ErrUnsupportedCapability, "configure-session", configured.FlinkVersion, configured.ReleaseLine, configured.APIVersion, nil)
	}
	if err := c.CheckAPIVersion(ctx); err != nil {
		return err
	}
	compatibility := c.compatibilitySnapshot()
	if !compatibility.Capabilities.ConfigureSession {
		return newCompatibilityError(ErrUnsupportedCapability, "configure-session", compatibility.FlinkVersion, compatibility.ReleaseLine, compatibility.APIVersion, nil)
	}
	release, err := c.acquireSessionSetup(ctx, sessionHandle)
	if err != nil {
		return err
	}
	defer release()
	return c.configureSessionRequest(ctx, sessionHandle, statement, executionTimeout, serverMessageRedaction{redactAll: true})
}

// configureSessionRequest는 검증과 session별 직렬화를 마친 설정 POST를 한 번만 전송한다.
// server가 SQL을 반사할 수 있으므로 응답 message 전체를 관측 전에 치환한다.
func (c *GatewayClient) configureSessionRequest(ctx context.Context, sessionHandle, statement string, executionTimeout time.Duration, redaction serverMessageRedaction) error {
	if executionTimeout <= 0 {
		executionTimeout = c.cfg.ExecutionTimeout
	}
	requestCtx := ctx
	cancel := func() {}
	if executionTimeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, executionTimeout)
	}
	defer cancel()

	body := struct {
		Statement        string `json:"statement"`
		ExecutionTimeout *int64 `json:"executionTimeout,omitempty"`
	}{Statement: statement}
	if c.selectedCapabilities().WireExecutionTimeout && executionTimeout > 0 {
		milliseconds := executionTimeout.Milliseconds()
		if milliseconds == 0 {
			milliseconds = 1
		}
		body.ExecutionTimeout = &milliseconds
	}
	target, _ := c.endpointURL(true, "/sessions/"+pathSegment(sessionHandle)+"/configure-session")
	requestCtx = withServerMessageRedaction(requestCtx, redaction)
	_, err := c.doJSON(requestCtx, http.MethodPost, target, body, nil, false)
	if err == nil {
		return nil
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || !statementOutcomeIsUnknown(apiErr) {
		return err
	}
	return &ConfigurationOutcomeUnknownError{
		SessionHandle: sessionHandle,
		StepIndex:     -1,
		RequestPhase:  apiErr.RequestPhase,
		Cause:         err,
	}
}

// CompleteStatement는 현재 profile과 protocol이 지원할 때 SQL 자동완성 후보를 반환한다.
func (c *GatewayClient) CompleteStatement(ctx context.Context, sessionHandle, statement string, position int) ([]string, error) {
	if err := validateSessionHandle(sessionHandle); err != nil {
		return nil, err
	}
	if configured, known, err := c.configuredCompatibility(); err != nil {
		return nil, err
	} else if known && !configured.Capabilities.CompleteStatement {
		return nil, newCompatibilityError(ErrUnsupportedCapability, "complete-statement", configured.FlinkVersion, configured.ReleaseLine, configured.APIVersion, nil)
	}
	if err := c.CheckAPIVersion(ctx); err != nil {
		return nil, err
	}
	compatibility := c.compatibilitySnapshot()
	if !compatibility.Capabilities.CompleteStatement {
		return nil, newCompatibilityError(ErrUnsupportedCapability, "complete-statement", compatibility.FlinkVersion, compatibility.ReleaseLine, compatibility.APIVersion, nil)
	}
	target, _ := c.endpointURL(true, "/sessions/"+pathSegment(sessionHandle)+"/complete-statement")
	query := target.Query()
	query.Set("statement", statement)
	query.Set("position", strconv.Itoa(position))
	target.RawQuery = query.Encode()
	var response struct {
		Candidates []string `json:"candidates"`
	}
	requestCtx := withServerMessageRedaction(ctx, serverMessageRedaction{fragments: []string{statement}})
	if _, err := c.doJSON(requestCtx, http.MethodGet, target, nil, &response, true); err != nil {
		return nil, err
	}
	return response.Candidates, nil
}

// Heartbeat는 session을 활성 상태로 유지한다. 멱등인 heartbeat POST는 일시적인 transport
// 오류에 한해 한 번 재시도할 수 있다.
func (c *GatewayClient) Heartbeat(ctx context.Context, sessionHandle string) error {
	if err := validateSessionHandle(sessionHandle); err != nil {
		return err
	}
	if err := c.CheckAPIVersion(ctx); err != nil {
		return err
	}
	target, _ := c.endpointURL(true, "/sessions/"+pathSegment(sessionHandle)+"/heartbeat")
	_, err := c.doJSON(ctx, http.MethodPost, target, struct{}{}, nil, true)
	return err
}

// CloseSession은 heartbeat를 멈추고 session을 닫는다. 동시에 중복 호출하면 첫 종료 결과를
// 공유하며 server-side not-found는 멱등 성공으로 취급한다.
func (c *GatewayClient) CloseSession(ctx context.Context, sessionHandle string) error {
	if err := validateSessionHandle(sessionHandle); err != nil {
		return err
	}
	c.stateMu.Lock()
	if _, ok := c.closed[sessionHandle]; ok {
		c.stateMu.Unlock()
		return nil
	}
	if call := c.closeCalls[sessionHandle]; call != nil {
		c.stateMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-call.done:
			return call.err
		}
	}
	call := &sessionCloseCall{done: make(chan struct{})}
	c.closeCalls[sessionHandle] = call
	c.stateMu.Unlock()

	c.StopHeartbeat(sessionHandle)
	err := c.CheckAPIVersion(ctx)
	if err == nil {
		var target *url.URL
		target, _ = c.endpointURL(true, "/sessions/"+pathSegment(sessionHandle))
		_, err = c.doJSON(ctx, http.MethodDelete, target, nil, nil, false)
		if errors.Is(err, ErrSessionNotFound) || errors.Is(err, ErrSessionExpired) {
			err = nil
		}
	}

	c.stateMu.Lock()
	delete(c.closeCalls, sessionHandle)
	if err == nil {
		c.rememberClosedSessionLocked(sessionHandle)
		delete(c.sessions, sessionHandle)
	}
	call.err = err
	close(call.done)
	c.stateMu.Unlock()
	return err
}

// rememberClosedSessionLocked는 stateMu를 보유한 상태에서 최근 Close 결과를 bounded cache에 기록한다.
func (c *GatewayClient) rememberClosedSessionLocked(sessionHandle string) {
	if _, exists := c.closed[sessionHandle]; exists {
		return
	}
	c.closed[sessionHandle] = struct{}{}
	c.closedOrder = append(c.closedOrder, sessionHandle)
	if len(c.closedOrder) <= maxRememberedClosedSessions {
		return
	}
	oldest := c.closedOrder[0]
	c.closedOrder = c.closedOrder[1:]
	delete(c.closed, oldest)
}

// forgetClosedSessionLocked는 server가 같은 handle을 다시 발급했을 때 이전 Close cache 순서를 함께 제거한다.
func (c *GatewayClient) forgetClosedSessionLocked(sessionHandle string) {
	if _, exists := c.closed[sessionHandle]; !exists {
		return
	}
	delete(c.closed, sessionHandle)
	for index, candidate := range c.closedOrder {
		if candidate != sessionHandle {
			continue
		}
		copy(c.closedOrder[index:], c.closedOrder[index+1:])
		c.closedOrder = c.closedOrder[:len(c.closedOrder)-1]
		return
	}
}

// validateStatement는 빈 SQL을 거부하고 주입된 소유권 및 SQL 정책을 실행 전에 적용한다.
func (c *GatewayClient) validateStatement(ctx context.Context, sessionHandle, statement string) error {
	if strings.TrimSpace(statement) == "" {
		return fmt.Errorf("flinksqlgateway: statement is required")
	}
	if c.cfg.Validator == nil {
		return nil
	}
	return c.cfg.Validator.Validate(ctx, c.sessionContext(sessionHandle), statement)
}
