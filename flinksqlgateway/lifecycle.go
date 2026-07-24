package flinksqlgateway

import (
	"context"
	"errors"
	"time"
)

// internalCleanupContextKey는 client 종료 후에도 이미 소유한 자원을 정리하도록 허용하는 내부 context key이다.
type internalCleanupContextKey struct{}

// withInternalCleanup은 새 사용자 작업이 아닌 내부 cleanup 요청임을 context에 표시한다.
func withInternalCleanup(ctx context.Context) context.Context {
	return context.WithValue(ctx, internalCleanupContextKey{}, true)
}

// isInternalCleanup은 종료된 client에서도 허용할 내부 cleanup 요청인지 확인한다.
func isInternalCleanup(ctx context.Context) bool {
	allowed, _ := ctx.Value(internalCleanupContextKey{}).(bool)
	return allowed
}

// ensureOpen은 새 작업을 거부하되 client가 시작한 내부 cleanup은 계속 허용한다.
func (c *GatewayClient) ensureOpen(ctx context.Context) error {
	if isInternalCleanup(ctx) {
		return nil
	}
	c.stateMu.Lock()
	closed := c.clientClosed
	c.stateMu.Unlock()
	if closed {
		return ErrClientClosed
	}
	return nil
}

// Close는 client가 소유한 수명주기 작업을 중단하고 새 요청을 거부한다. idle connection은
// client가 transport를 소유한 경우에만 닫는다.
func (c *GatewayClient) Close() error {
	if c == nil {
		return nil
	}
	c.clientCloseOnce.Do(func() {
		c.stateMu.Lock()
		c.clientClosed = true
		streams := make([]*resultStream, 0, len(c.streams))
		for stream := range c.streams {
			streams = append(streams, stream)
		}
		managed := make([]*managedSession, 0, len(c.managed))
		for session := range c.managed {
			managed = append(managed, session)
		}
		runners := make([]*HeartbeatRunner, 0, len(c.heartbeats))
		for _, runner := range c.heartbeats {
			runners = append(runners, runner)
		}
		c.lifecycleCancel()
		c.stateMu.Unlock()

		cleanupTimeout := c.cfg.RequestTimeout
		if cleanupTimeout <= 0 {
			cleanupTimeout = defaultRequestTimeout
		}
		var closeErrors []error
		for _, stream := range streams {
			ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			err := stream.closeWithContext(withInternalCleanup(ctx), ErrClientClosed)
			cancel()
			if err != nil {
				closeErrors = append(closeErrors, err)
			}
		}
		for _, session := range managed {
			ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			err := session.closeWithContext(withInternalCleanup(ctx))
			cancel()
			if err != nil {
				closeErrors = append(closeErrors, err)
			}
		}
		for _, runner := range runners {
			runner.Stop()
		}
		if c.ownsHTTPTransport {
			c.httpClient.CloseIdleConnections()
		}
		c.clientCloseErr = errors.Join(closeErrors...)
		close(c.clientCloseDone)
	})
	<-c.clientCloseDone
	return c.clientCloseErr
}

// registerManaged는 client 종료와 경쟁하지 않도록 managed session을 원자적으로 등록한다.
func (c *GatewayClient) registerManaged(session *managedSession) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.clientClosed {
		return ErrClientClosed
	}
	c.managed[session] = struct{}{}
	return nil
}

// unregisterManaged는 종료된 managed session을 client 소유 목록에서 제거한다.
func (c *GatewayClient) unregisterManaged(session *managedSession) {
	c.stateMu.Lock()
	delete(c.managed, session)
	c.stateMu.Unlock()
}

// registerStream은 client 종료와 경쟁하지 않도록 result stream을 원자적으로 등록한다.
func (c *GatewayClient) registerStream(stream *resultStream) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.clientClosed {
		return ErrClientClosed
	}
	c.streams[stream] = struct{}{}
	return nil
}

// unregisterStream은 종료된 result stream을 client 소유 목록에서 제거한다.
func (c *GatewayClient) unregisterStream(stream *resultStream) {
	c.stateMu.Lock()
	delete(c.streams, stream)
	c.stateMu.Unlock()
}

// observeLifecycle은 Observer panic이 client 동작을 중단하지 않게 격리해 정제된 event를 전달한다.
func (c *GatewayClient) observeLifecycle(ctx context.Context, observation Observation) {
	observer := c.cfg.LifecycleObserver
	if observer == nil {
		observer, _ = c.cfg.Observer.(LifecycleObserver)
	}
	if observer == nil {
		return
	}
	if observation.Timestamp.IsZero() {
		observation.Timestamp = time.Now()
	}
	defer func() { _ = recover() }()
	observer.ObserveLifecycle(ctx, observation)
}
