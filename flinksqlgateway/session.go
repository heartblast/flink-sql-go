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
	session := &Session{
		Handle:     response.Handle,
		Name:       req.SessionName,
		Properties: cloneMap(req.Properties),
		CreatedAt:  time.Now(),
	}
	c.stateMu.Lock()
	c.sessions[session.Handle] = session
	delete(c.closed, session.Handle)
	c.stateMu.Unlock()
	return session, nil
}

// GetSessionConfig는 현재 server-side session property를 조회한다.
func (c *GatewayClient) GetSessionConfig(ctx context.Context, sessionHandle string) (map[string]string, error) {
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

// ConfigureSession은 v2 이상에서 session 설정 statement를 실행하며 POST를 자동 재시도하지 않는다.
func (c *GatewayClient) ConfigureSession(ctx context.Context, sessionHandle, statement string, executionTimeout time.Duration) error {
	if apiVersionNumber(c.cfg.APIVersion) < 2 {
		return fmt.Errorf("%w: configure-session requires v2 or newer", ErrUnsupportedAPI)
	}
	if err := c.validateStatement(ctx, sessionHandle, statement); err != nil {
		return err
	}
	if err := c.CheckAPIVersion(ctx); err != nil {
		return err
	}
	if executionTimeout <= 0 {
		executionTimeout = c.cfg.ExecutionTimeout
	}
	body := struct {
		Statement        string `json:"statement"`
		ExecutionTimeout int64  `json:"executionTimeout"`
	}{statement, executionTimeout.Milliseconds()}
	target, _ := c.endpointURL(true, "/sessions/"+pathSegment(sessionHandle)+"/configure-session")
	_, err := c.doJSON(ctx, http.MethodPost, target, body, nil, false)
	return err
}

// CompleteStatement는 v2 이상에서 SQL 자동완성 후보를 반환한다.
func (c *GatewayClient) CompleteStatement(ctx context.Context, sessionHandle, statement string, position int) ([]string, error) {
	if apiVersionNumber(c.cfg.APIVersion) < 2 {
		return nil, fmt.Errorf("%w: complete-statement requires v2 or newer", ErrUnsupportedAPI)
	}
	if err := c.CheckAPIVersion(ctx); err != nil {
		return nil, err
	}
	target, _ := c.endpointURL(true, "/sessions/"+pathSegment(sessionHandle)+"/complete-statement")
	query := target.Query()
	query.Set("statement", statement)
	query.Set("position", strconv.Itoa(position))
	target.RawQuery = query.Encode()
	var response struct {
		Candidates []string `json:"candidates"`
	}
	if _, err := c.doJSON(ctx, http.MethodGet, target, nil, &response, true); err != nil {
		return nil, err
	}
	return response.Candidates, nil
}

// Heartbeat는 session을 활성 상태로 유지한다. 멱등인 heartbeat POST는 일시적인 transport
// 오류에 한해 한 번 재시도할 수 있다.
func (c *GatewayClient) Heartbeat(ctx context.Context, sessionHandle string) error {
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
		c.closed[sessionHandle] = struct{}{}
		delete(c.sessions, sessionHandle)
	}
	call.err = err
	close(call.done)
	c.stateMu.Unlock()
	return err
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
