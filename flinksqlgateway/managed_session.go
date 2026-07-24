package flinksqlgateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// SessionHealth describes the local managed-session view. Expired sessions
// are never recreated automatically.
type SessionHealth string

const (
	SessionHealthy  SessionHealth = "HEALTHY"
	SessionDegraded SessionHealth = "DEGRADED"
	SessionExpired  SessionHealth = "EXPIRED"
	SessionClosed   SessionHealth = "CLOSED"
)

// ManagedSession owns a Flink session heartbeat and local lifecycle.
type ManagedSession interface {
	Handle() string
	Execute(ctx context.Context, statement string, options ExecuteOptions) (*ExecutionResult, error)
	Stream(ctx context.Context, statement string, options StreamOptions) (ResultStream, error)
	Health() SessionHealth
	Close(ctx context.Context) error
}

// ManagedSessionOptions configures a managed session.
type ManagedSessionOptions struct {
	HeartbeatInterval time.Duration
	HeartbeatJitter   time.Duration
	FailureThreshold  int
	CleanupTimeout    time.Duration
	Serialize         bool
}

type managedSession struct {
	client  *GatewayClient
	session *Session
	options ManagedSessionOptions

	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	runner          *HeartbeatRunner
	serializer      *sessionSerializer
	monitorDone     chan struct{}

	mu        sync.RWMutex
	health    SessionHealth
	closeErr  error
	closeOnce sync.Once
	closeDone chan struct{}
}

var _ ManagedSession = (*managedSession)(nil)

// OpenManagedSession creates a session and starts its independent heartbeat
// lifecycle. The open-call context does not own the heartbeat goroutine.
func (c *GatewayClient) OpenManagedSession(ctx context.Context, req OpenSessionRequest, options ManagedSessionOptions) (ManagedSession, error) {
	if err := c.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if options.HeartbeatInterval <= 0 {
		options.HeartbeatInterval = c.cfg.HeartbeatInterval
	}
	if options.HeartbeatJitter < 0 {
		return nil, fmt.Errorf("flinksqlgateway: heartbeat jitter must not be negative")
	}
	if options.FailureThreshold <= 0 {
		options.FailureThreshold = 3
	}
	if options.CleanupTimeout <= 0 {
		options.CleanupTimeout = c.cfg.RequestTimeout
	}

	session, err := c.OpenSession(ctx, req)
	if err != nil {
		return nil, err
	}
	lifecycleCtx, lifecycleCancel := context.WithCancel(c.lifecycleCtx)
	managed := &managedSession{
		client:          c,
		session:         session,
		options:         options,
		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: lifecycleCancel,
		monitorDone:     make(chan struct{}),
		health:          SessionHealthy,
		closeDone:       make(chan struct{}),
	}
	if options.Serialize {
		managed.serializer = newSessionSerializer(c, session.Handle, lifecycleCtx)
	}
	if err := c.registerManaged(managed); err != nil {
		lifecycleCancel()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), options.CleanupTimeout)
		_ = c.CloseSession(withInternalCleanup(cleanupCtx), session.Handle)
		cancel()
		return nil, err
	}

	runner, err := c.startHeartbeat(lifecycleCtx, session.Handle, options.HeartbeatInterval, options.HeartbeatJitter)
	if err != nil {
		c.unregisterManaged(managed)
		lifecycleCancel()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), options.CleanupTimeout)
		_ = c.CloseSession(withInternalCleanup(cleanupCtx), session.Handle)
		cancel()
		return nil, err
	}
	managed.runner = runner
	go managed.monitorHeartbeat()
	c.observeLifecycle(ctx, Observation{Event: ObservationSessionOpened, SessionHandle: session.Handle, CurrentHealth: SessionHealthy})
	return managed, nil
}

func (m *managedSession) Handle() string { return m.session.Handle }

func (m *managedSession) Health() SessionHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.health
}

func (m *managedSession) Execute(ctx context.Context, statement string, options ExecuteOptions) (*ExecutionResult, error) {
	if err := m.executionAllowed(); err != nil {
		return nil, err
	}
	if m.serializer != nil {
		return m.serializer.execute(ctx, statement, options)
	}
	executionCtx, cancel := mergeContext(ctx, m.lifecycleCtx)
	defer cancel()
	return m.client.ExecuteAndWait(executionCtx, m.session.Handle, statement, options)
}

func (m *managedSession) Stream(ctx context.Context, statement string, options StreamOptions) (ResultStream, error) {
	if err := m.executionAllowed(); err != nil {
		return nil, err
	}
	if m.serializer != nil {
		return m.serializer.stream(ctx, statement, options)
	}
	executionCtx, cancel := mergeContext(ctx, m.lifecycleCtx)
	stream, err := m.client.ExecuteStream(executionCtx, m.session.Handle, statement, options)
	if err != nil {
		cancel()
		return nil, err
	}
	return &callbackResultStream{ResultStream: stream, onClose: cancel}, nil
}

func (m *managedSession) executionAllowed() error {
	switch m.Health() {
	case SessionClosed:
		return ErrSessionClosed
	case SessionExpired:
		return ErrSessionExpired
	default:
		return nil
	}
}

func (m *managedSession) monitorHeartbeat() {
	defer close(m.monitorDone)
	for event := range m.runner.Events() {
		if event.Error == nil {
			m.client.observeLifecycle(m.lifecycleCtx, Observation{Event: ObservationSessionHeartbeatSucceeded, SessionHandle: m.session.Handle})
			if m.Health() == SessionDegraded {
				m.setHealth(SessionHealthy)
			}
			continue
		}
		m.client.observeLifecycle(m.lifecycleCtx, Observation{Event: ObservationSessionHeartbeatFailed, SessionHandle: m.session.Handle, Error: event.Error})
		if errors.Is(event.Error, ErrSessionNotFound) || errors.Is(event.Error, ErrSessionExpired) {
			m.setHealth(SessionExpired)
			continue
		}
		if event.ConsecutiveFailures >= m.options.FailureThreshold {
			m.setHealth(SessionDegraded)
		}
	}
}

func (m *managedSession) setHealth(next SessionHealth) {
	m.mu.Lock()
	previous := m.health
	if previous == SessionClosed || previous == next {
		m.mu.Unlock()
		return
	}
	m.health = next
	m.mu.Unlock()
	m.client.observeLifecycle(m.lifecycleCtx, Observation{
		Event:          ObservationSessionHealthChanged,
		SessionHandle:  m.session.Handle,
		PreviousHealth: previous,
		CurrentHealth:  next,
	})
}

func (m *managedSession) Close(ctx context.Context) error {
	return m.closeWithContext(ctx)
}

func (m *managedSession) closeWithContext(ctx context.Context) error {
	m.closeOnce.Do(func() {
		defer close(m.closeDone)
		m.lifecycleCancel()
		if m.runner != nil {
			m.runner.Stop()
			<-m.monitorDone
		}
		if m.serializer != nil {
			m.serializer.closeLocal()
		}
		closeCtx := ctx
		cancel := func() {}
		if m.options.CleanupTimeout > 0 {
			closeCtx, cancel = context.WithTimeout(ctx, m.options.CleanupTimeout)
		}
		m.closeErr = m.client.CloseSession(closeCtx, m.session.Handle)
		cancel()
		m.mu.Lock()
		previous := m.health
		m.health = SessionClosed
		m.mu.Unlock()
		m.client.unregisterManaged(m)
		m.client.observeLifecycle(ctx, Observation{Event: ObservationSessionClosed, SessionHandle: m.session.Handle, PreviousHealth: previous, CurrentHealth: SessionClosed, Error: m.closeErr})
	})
	select {
	case <-m.closeDone:
		return m.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
