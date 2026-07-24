package flinksqlgateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// HeartbeatEvent reports the latest heartbeat result. ConsecutiveFailures is
// reset to zero by a successful heartbeat.
type HeartbeatEvent struct {
	Timestamp           time.Time
	Error               error
	ConsecutiveFailures int
}

// HeartbeatRunner owns one session heartbeat goroutine. Errors is bounded and
// closes when the runner stops.
type HeartbeatRunner struct {
	cancel   context.CancelFunc
	done     chan struct{}
	errors   chan error
	events   chan HeartbeatEvent
	once     sync.Once
	interval time.Duration
	jitter   time.Duration
}

// Errors reports heartbeat failures without blocking the heartbeat loop.
func (h *HeartbeatRunner) Errors() <-chan error { return h.errors }

// Events reports success and failure state for managed-session health. The
// bounded channel retains the latest event and never blocks heartbeats.
func (h *HeartbeatRunner) Events() <-chan HeartbeatEvent { return h.events }

// Stop cancels the runner and waits for its goroutine to exit.
func (h *HeartbeatRunner) Stop() {
	if h == nil {
		return
	}
	h.once.Do(h.cancel)
	<-h.done
}

// StartHeartbeat starts at most one runner per session handle. A duplicate
// call returns the existing runner.
func (c *GatewayClient) StartHeartbeat(ctx context.Context, sessionHandle string) (*HeartbeatRunner, error) {
	return c.startHeartbeat(ctx, sessionHandle, c.cfg.HeartbeatInterval, 0)
}

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

// StopHeartbeat stops a runner if one exists. It is idempotent.
func (c *GatewayClient) StopHeartbeat(sessionHandle string) {
	c.stateMu.Lock()
	runner := c.heartbeats[sessionHandle]
	c.stateMu.Unlock()
	if runner != nil {
		runner.Stop()
	}
}
