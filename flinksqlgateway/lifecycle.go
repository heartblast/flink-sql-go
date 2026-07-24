package flinksqlgateway

import (
	"context"
	"errors"
	"time"
)

type internalCleanupContextKey struct{}

func withInternalCleanup(ctx context.Context) context.Context {
	return context.WithValue(ctx, internalCleanupContextKey{}, true)
}

func isInternalCleanup(ctx context.Context) bool {
	allowed, _ := ctx.Value(internalCleanupContextKey{}).(bool)
	return allowed
}

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

// Close stops client-owned lifecycle work, rejects new requests, and closes
// idle connections only when the transport is owned by this client.
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

func (c *GatewayClient) registerManaged(session *managedSession) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.clientClosed {
		return ErrClientClosed
	}
	c.managed[session] = struct{}{}
	return nil
}

func (c *GatewayClient) unregisterManaged(session *managedSession) {
	c.stateMu.Lock()
	delete(c.managed, session)
	c.stateMu.Unlock()
}

func (c *GatewayClient) registerStream(stream *resultStream) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.clientClosed {
		return ErrClientClosed
	}
	c.streams[stream] = struct{}{}
	return nil
}

func (c *GatewayClient) unregisterStream(stream *resultStream) {
	c.stateMu.Lock()
	delete(c.streams, stream)
	c.stateMu.Unlock()
}

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
