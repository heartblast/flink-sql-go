package flinksqlgateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// HeartbeatEvent는 최근 heartbeat 결과를 보고하며 성공하면 ConsecutiveFailures가 0으로 초기화된다.
type HeartbeatEvent struct {
	Timestamp           time.Time
	Error               error
	ConsecutiveFailures int
}

// HeartbeatRunner는 한 session의 heartbeat goroutine을 소유한다. Errors channel은
// 크기가 제한되며 runner가 멈추면 닫힌다.
type HeartbeatRunner struct {
	cancel   context.CancelFunc
	done     chan struct{}
	errors   chan error
	events   chan HeartbeatEvent
	once     sync.Once
	interval time.Duration
	jitter   time.Duration
}

// Errors는 heartbeat loop를 막지 않고 실패를 보고하는 bounded channel이다.
func (h *HeartbeatRunner) Errors() <-chan error { return h.errors }

// Events는 managed session 건강 상태에 사용할 성공과 실패 event를 보고한다. bounded
// channel은 최근 event를 유지하며 heartbeat를 막지 않는다.
func (h *HeartbeatRunner) Events() <-chan HeartbeatEvent { return h.events }

// Stop은 runner를 취소하고 heartbeat goroutine이 끝날 때까지 기다린다.
func (h *HeartbeatRunner) Stop() {
	if h == nil {
		return
	}
	h.once.Do(h.cancel)
	<-h.done
}

// StartHeartbeat는 session handle마다 runner를 최대 하나만 시작하며 중복 호출은 기존 runner를 반환한다.
func (c *GatewayClient) StartHeartbeat(ctx context.Context, sessionHandle string) (*HeartbeatRunner, error) {
	return c.startHeartbeat(ctx, sessionHandle, c.cfg.HeartbeatInterval, 0)
}

// startHeartbeat는 주기와 jitter를 검증하고 session별 단일 runner를 등록한다.
func (c *GatewayClient) startHeartbeat(ctx context.Context, sessionHandle string, interval, jitter time.Duration) (*HeartbeatRunner, error) {
	if sessionHandle == "" {
		return nil, fmt.Errorf("flinksqlgateway: session handle is required")
	}
	if err := c.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := c.CheckAPIVersion(ctx); err != nil {
		return nil, err
	}
	if interval <= 0 {
		interval = c.cfg.HeartbeatInterval
	}
	if jitter < 0 {
		return nil, fmt.Errorf("flinksqlgateway: heartbeat jitter must not be negative")
	}

	c.stateMu.Lock()
	if runner := c.heartbeats[sessionHandle]; runner != nil {
		c.stateMu.Unlock()
		return runner, nil
	}
	runnerCtx, cancel := context.WithCancel(ctx)
	runner := &HeartbeatRunner{
		cancel:   cancel,
		done:     make(chan struct{}),
		errors:   make(chan error, 1),
		events:   make(chan HeartbeatEvent, 1),
		interval: interval,
		jitter:   jitter,
	}
	c.heartbeats[sessionHandle] = runner
	c.stateMu.Unlock()

	go c.runHeartbeat(runnerCtx, sessionHandle, runner)
	return runner, nil
}

// runHeartbeat는 취소되거나 session이 만료될 때까지 heartbeat를 전송하고 최신 상태를 게시한다.
func (c *GatewayClient) runHeartbeat(ctx context.Context, sessionHandle string, runner *HeartbeatRunner) {
	defer func() {
		c.stateMu.Lock()
		if c.heartbeats[sessionHandle] == runner {
			delete(c.heartbeats, sessionHandle)
		}
		c.stateMu.Unlock()
		close(runner.errors)
		close(runner.events)
		close(runner.done)
	}()

	consecutiveFailures := 0
	for {
		if err := waitContext(ctx, heartbeatDelay(runner.interval, runner.jitter)); err != nil {
			return
		}
		err := c.Heartbeat(ctx, sessionHandle)
		if err == nil {
			consecutiveFailures = 0
			publishHeartbeatEvent(runner.events, HeartbeatEvent{Timestamp: time.Now()})
			continue
		}
		if ctx.Err() != nil {
			return
		}
		consecutiveFailures++
		publishHeartbeatEvent(runner.events, HeartbeatEvent{Timestamp: time.Now(), Error: err, ConsecutiveFailures: consecutiveFailures})
		select {
		case runner.errors <- err:
		default:
		}
		if errors.Is(err, ErrSessionNotFound) || errors.Is(err, ErrSessionExpired) || isContextError(err) {
			return
		}
	}
}

// heartbeatDelay는 interval의 절반을 넘지 않는 범위에서 양방향 jitter를 적용한다.
func heartbeatDelay(interval, jitter time.Duration) time.Duration {
	if jitter <= 0 {
		return interval
	}
	if jitter > interval/2 {
		jitter = interval / 2
	}
	span := int64(jitter)*2 + 1
	offset := time.Now().UnixNano()%span - int64(jitter)
	delay := interval + time.Duration(offset)
	if delay <= 0 {
		return time.Nanosecond
	}
	return delay
}

// publishHeartbeatEvent는 느린 소비자가 heartbeat를 막지 않도록 기존 event를 최신 값으로 교체한다.
func publishHeartbeatEvent(channel chan HeartbeatEvent, event HeartbeatEvent) {
	select {
	case channel <- event:
		return
	default:
	}
	select {
	case <-channel:
	default:
	}
	select {
	case channel <- event:
	default:
	}
}

// StopHeartbeat는 session의 runner가 있으면 중지하며 중복 호출에 안전하다.
func (c *GatewayClient) StopHeartbeat(sessionHandle string) {
	c.stateMu.Lock()
	runner := c.heartbeats[sessionHandle]
	c.stateMu.Unlock()
	if runner != nil {
		runner.Stop()
	}
}
