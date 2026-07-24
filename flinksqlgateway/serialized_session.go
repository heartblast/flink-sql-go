package flinksqlgateway

import (
	"context"
	"fmt"
	"sync"
)

// SerializedSession은 한 session의 전체 실행을 직렬화하되 다른 session과 기반 GatewayClient의
// 동시 실행은 허용한다.
type SerializedSession struct {
	serializer *sessionSerializer
	closeOnce  sync.Once
	closeErr   error
}

// NewSerializedSession은 sessionHandle에 로컬 실행 gate를 만든다. wrapper는 CloseSession을
// 소유하지만 server-side session을 생성하지는 않는다.
func NewSerializedSession(client *GatewayClient, sessionHandle string) *SerializedSession {
	parent := context.Background()
	if client != nil {
		parent = client.lifecycleCtx
	}
	return &SerializedSession{serializer: newSessionSerializer(client, sessionHandle, parent)}
}

// Handle은 wrapper가 직렬화하는 session handle을 반환한다.
func (s *SerializedSession) Handle() string {
	if s == nil || s.serializer == nil {
		return ""
	}
	return s.serializer.sessionHandle
}

// Execute는 앞선 실행이 완전히 끝난 뒤 수집형 statement 실행을 시작한다.
func (s *SerializedSession) Execute(ctx context.Context, statement string, options ExecuteOptions) (*ExecutionResult, error) {
	if s == nil || s.serializer == nil {
		return nil, fmt.Errorf("flinksqlgateway: serialized session is nil")
	}
	return s.serializer.execute(ctx, statement, options)
}

// Stream은 앞선 실행이 끝난 뒤 stream을 시작하고 stream 종료 시 다음 실행에 gate를 넘긴다.
func (s *SerializedSession) Stream(ctx context.Context, statement string, options StreamOptions) (ResultStream, error) {
	if s == nil || s.serializer == nil {
		return nil, fmt.Errorf("flinksqlgateway: serialized session is nil")
	}
	return s.serializer.stream(ctx, statement, options)
}

// Close는 대기 중인 작업을 거부하고 실행 중 작업을 취소한 뒤 기반 Flink session을 닫는다.
// 중복 호출은 첫 결과를 반환한다.
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

// sessionSerializer는 channel token 하나로 session 실행권을 관리하고 종료 시 대기자를 취소한다.
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

// newSessionSerializer는 parent 수명주기에 연결된 단일 실행권 gate를 만든다.
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

// execute는 gate를 얻은 호출 하나가 수집형 실행과 cleanup을 마칠 때까지 실행권을 유지한다.
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

// stream은 반환한 stream이 닫히거나 EOS가 될 때까지 session 실행권을 유지한다.
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

// validate는 client, session handle과 로컬 종료 상태를 확인한다.
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

// acquire는 context 취소를 존중하면서 단일 실행권을 가져온다.
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

// release는 다음 대기자에게 단일 실행권을 돌려준다.
func (s *sessionSerializer) release() {
	s.gate <- struct{}{}
}

// isClosed는 동시 호출에 안전하게 로컬 종료 상태를 반환한다.
func (s *sessionSerializer) isClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

// closeLocal은 새 실행을 막고 실행 및 대기 중인 context를 취소한다.
func (s *sessionSerializer) closeLocal() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.cancel()
	})
}

// mergeContext는 호출 context와 상위 수명주기 중 하나가 끝나면 함께 취소되는 context를 만든다.
func mergeContext(primary, lifecycle context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(primary)
	stop := context.AfterFunc(lifecycle, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

// callbackResultStream은 기반 stream이 끝날 때 실행권 같은 상위 자원을 한 번만 해제한다.
type callbackResultStream struct {
	ResultStream
	onClose func()
	once    sync.Once
}

// finish는 등록된 종료 callback을 최대 한 번 실행한다.
func (s *callbackResultStream) finish() {
	s.once.Do(func() {
		if s.onClose != nil {
			s.onClose()
		}
	})
}

// Next는 기반 stream을 진행하고 종료된 경우 상위 자원을 해제한다.
func (s *callbackResultStream) Next() bool {
	ok := s.ResultStream.Next()
	if !ok {
		s.finish()
	}
	return ok
}

// Close는 기반 stream과 상위 자원을 모두 종료한다.
func (s *callbackResultStream) Close() error {
	err := s.ResultStream.Close()
	s.finish()
	return err
}
