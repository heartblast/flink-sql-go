package flinksqlgateway

import (
	"context"
	"fmt"
	"sync"
)

// SerializedSession serializes complete executions for one session while
// leaving other sessions and the base GatewayClient concurrent.
type SerializedSession struct {
	serializer *sessionSerializer
	closeOnce  sync.Once
	closeErr   error
}

// NewSerializedSession creates a local execution gate for sessionHandle. The
// wrapper owns CloseSession, but does not create the server-side session.
func NewSerializedSession(client *GatewayClient, sessionHandle string) *SerializedSession {
	parent := context.Background()
	if client != nil {
		parent = client.lifecycleCtx
	}
	return &SerializedSession{serializer: newSessionSerializer(client, sessionHandle, parent)}
}

func (s *SerializedSession) Handle() string {
	if s == nil || s.serializer == nil {
		return ""
	}
	return s.serializer.sessionHandle
}

func (s *SerializedSession) Execute(ctx context.Context, statement string, options ExecuteOptions) (*ExecutionResult, error) {
	if s == nil || s.serializer == nil {
		return nil, fmt.Errorf("flinksqlgateway: serialized session is nil")
	}
	return s.serializer.execute(ctx, statement, options)
}

func (s *SerializedSession) Stream(ctx context.Context, statement string, options StreamOptions) (ResultStream, error) {
	if s == nil || s.serializer == nil {
		return nil, fmt.Errorf("flinksqlgateway: serialized session is nil")
	}
	return s.serializer.stream(ctx, statement, options)
}

// Close rejects queued work, cancels in-flight work, and closes the underlying
// Flink session. Repeated calls return the first result.
func (s *SerializedSession) Close(ctx context.Context) error {
	if s == nil || s.serializer == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.serializer.closeLocal()
		if s.serializer.client == nil {
			s.closeErr = fmt.Errorf("flinksqlgateway: client is required")
			return
		}
		s.closeErr = s.serializer.client.CloseSession(ctx, s.serializer.sessionHandle)
	})
	return s.closeErr
}

type sessionSerializer struct {
	client        *GatewayClient
	sessionHandle string
	gate          chan struct{}
	lifecycleCtx  context.Context
	cancel        context.CancelFunc
	mu            sync.RWMutex
	closed        bool
	closeOnce     sync.Once
}

func newSessionSerializer(client *GatewayClient, sessionHandle string, parent context.Context) *sessionSerializer {
	lifecycleCtx, cancel := context.WithCancel(parent)
	serializer := &sessionSerializer{
		client:        client,
		sessionHandle: sessionHandle,
		gate:          make(chan struct{}, 1),
		lifecycleCtx:  lifecycleCtx,
		cancel:        cancel,
	}
	serializer.gate <- struct{}{}
	return serializer
}

func (s *sessionSerializer) execute(ctx context.Context, statement string, options ExecuteOptions) (*ExecutionResult, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	executionCtx, cancel := mergeContext(ctx, s.lifecycleCtx)
	defer cancel()
	if err := s.acquire(executionCtx); err != nil {
		return nil, err
	}
	defer s.release()
	return s.client.ExecuteAndWait(executionCtx, s.sessionHandle, statement, options)
}

func (s *sessionSerializer) stream(ctx context.Context, statement string, options StreamOptions) (ResultStream, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	executionCtx, cancel := mergeContext(ctx, s.lifecycleCtx)
	if err := s.acquire(executionCtx); err != nil {
		cancel()
		return nil, err
	}
	stream, err := s.client.ExecuteStream(executionCtx, s.sessionHandle, statement, options)
	if err != nil {
		s.release()
		cancel()
		return nil, err
	}
	return &callbackResultStream{
		ResultStream: stream,
		onClose: func() {
			s.release()
			cancel()
		},
	}, nil
}

func (s *sessionSerializer) validate() error {
	if s.client == nil {
		return fmt.Errorf("flinksqlgateway: client is required")
	}
	if s.sessionHandle == "" {
		return fmt.Errorf("flinksqlgateway: session handle is required")
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return ErrSessionClosed
	}
	return nil
}

func (s *sessionSerializer) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		if s.isClosed() {
			return ErrSessionClosed
		}
		return ctx.Err()
	case <-s.gate:
		if s.isClosed() {
			s.release()
			return ErrSessionClosed
		}
		return nil
	}
}

func (s *sessionSerializer) release() {
	s.gate <- struct{}{}
}

func (s *sessionSerializer) isClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

func (s *sessionSerializer) closeLocal() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.cancel()
	})
}

func mergeContext(primary, lifecycle context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(primary)
	stop := context.AfterFunc(lifecycle, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

type callbackResultStream struct {
	ResultStream
	onClose func()
	once    sync.Once
}

func (s *callbackResultStream) finish() {
	s.once.Do(func() {
		if s.onClose != nil {
			s.onClose()
		}
	})
}

func (s *callbackResultStream) Next() bool {
	ok := s.ResultStream.Next()
	if !ok {
		s.finish()
	}
	return ok
}

func (s *callbackResultStream) Close() error {
	err := s.ResultStream.Close()
	s.finish()
	return err
}
