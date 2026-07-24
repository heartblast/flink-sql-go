package flinksqlgateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// SessionHealth는 managed session을 로컬에서 관찰한 건강 상태이다. 만료된 session은
// 자동으로 다시 만들지 않는다.
type SessionHealth string

const (
	// SessionHealthy는 최근 heartbeat가 성공한 상태이다.
	SessionHealthy SessionHealth = "HEALTHY"
	// SessionDegraded는 연속 heartbeat 실패가 임계값에 도달한 상태이다.
	SessionDegraded SessionHealth = "DEGRADED"
	// SessionExpired는 server에서 session이 없거나 만료됐다고 확인된 상태이다.
	SessionExpired SessionHealth = "EXPIRED"
	// SessionClosed는 wrapper가 로컬에서 종료된 상태이다.
	SessionClosed SessionHealth = "CLOSED"
)

// ManagedSession은 Flink session, heartbeat와 로컬 수명주기를 함께 소유한다.
type ManagedSession interface {
	// Handle은 server가 발급한 session handle을 반환한다.
	Handle() string
	// Execute는 managed session에서 bounded 수집형 실행을 수행한다.
	Execute(ctx context.Context, statement string, options ExecuteOptions) (*ExecutionResult, error)
	// Stream은 managed session에서 동기식 점진 결과 stream을 시작한다.
	Stream(ctx context.Context, statement string, options StreamOptions) (ResultStream, error)
	// Health는 heartbeat로 판단한 현재 local session 건강 상태를 반환한다.
	Health() SessionHealth
	// Close는 heartbeat와 실행을 중단하고 server-side session을 닫는다.
	Close(ctx context.Context) error
}

// ManagedSessionOptions는 managed session의 heartbeat, cleanup과 실행 직렬화를 설정한다.
type ManagedSessionOptions struct {
	HeartbeatInterval time.Duration
	HeartbeatJitter   time.Duration
	FailureThreshold  int
	CleanupTimeout    time.Duration
	Serialize         bool
}

// managedSession은 공개 ManagedSession 계약을 구현하며 모든 종료 상태를 한 번만 전이한다.
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

// 컴파일 시 managedSession이 공개 ManagedSession 계약을 모두 구현하는지 확인한다.
var _ ManagedSession = (*managedSession)(nil)

// OpenManagedSession은 session을 만들고 독립적인 heartbeat 수명주기를 시작한다.
// open 호출의 context는 heartbeat goroutine을 소유하지 않는다.
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

// Handle은 server가 발급한 불투명한 session handle을 반환한다.
func (m *managedSession) Handle() string { return m.session.Handle }

// Health는 동시 호출에 안전하게 현재 로컬 session 건강 상태를 반환한다.
func (m *managedSession) Health() SessionHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.health
}

// Execute는 session 건강 상태를 확인하고 설정에 따라 하나의 전체 실행을 직렬화한다.
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

// Stream은 session 수명주기에 연결된 동기 result stream을 생성한다.
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

// executionAllowed는 닫히거나 만료된 session에서 새 실행을 시작하지 못하게 한다.
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

// monitorHeartbeat는 heartbeat event를 local health 전이와 관측 event로 변환한다.
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

// setHealth는 종료 상태를 되돌리지 않으며 실제 변경이 있을 때만 event를 발생시킨다.
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

// Close는 heartbeat와 실행 wrapper를 멈춘 뒤 server session을 닫으며 중복 호출에 안전하다.
func (m *managedSession) Close(ctx context.Context) error {
	return m.closeWithContext(ctx)
}

// closeWithContext는 외부 호출과 client 주도 cleanup이 공유하는 실제 종료 절차를 수행한다.
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
