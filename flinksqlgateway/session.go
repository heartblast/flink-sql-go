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

type sessionCloseCall struct {
	done chan struct{}
	err  error
}

// GetInfo returns SQL Gateway product metadata.
func (c *GatewayClient) GetInfo(ctx context.Context) (*GatewayInfo, error) {
	target, _ := c.endpointURL(false, "/info")
	var result GatewayInfo
	if _, err := c.doJSON(ctx, http.MethodGet, target, nil, &result, true); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetAPIVersions returns the versions advertised by /api_versions.
func (c *GatewayClient) GetAPIVersions(ctx context.Context) ([]string, error) {
	return c.getAPIVersions(ctx)
}

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

// OpenSession creates stateful Flink session state. It never retries the POST.
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

// GetSessionConfig gets the current server-side session properties.
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

// ConfigureSession executes a v2+ session configuration statement. The POST
// is never retried.
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

// CompleteStatement returns v2+ SQL completion candidates.
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

// Heartbeat keeps a session alive. The idempotent heartbeat POST may be
// retried once for transient transport errors.
func (c *GatewayClient) Heartbeat(ctx context.Context, sessionHandle string) error {
	if err := c.CheckAPIVersion(ctx); err != nil {
		return err
	}
	target, _ := c.endpointURL(true, "/sessions/"+pathSegment(sessionHandle)+"/heartbeat")
	_, err := c.doJSON(ctx, http.MethodPost, target, struct{}{}, nil, true)
	return err
}

// CloseSession stops heartbeat and closes a session. Concurrent repeated
// calls share the first close result; a server-side not-found is idempotent.
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

func (c *GatewayClient) validateStatement(ctx context.Context, sessionHandle, statement string) error {
	if strings.TrimSpace(statement) == "" {
		return fmt.Errorf("flinksqlgateway: statement is required")
	}
	if c.cfg.Validator == nil {
		return nil
	}
	return c.cfg.Validator.Validate(ctx, c.sessionContext(sessionHandle), statement)
}
