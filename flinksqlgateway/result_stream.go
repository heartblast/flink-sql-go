package flinksqlgateway

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"
)

// ExecuteStream은 statement를 제출하고 동기식 점진 iterator를 반환한다. StreamResults와
// 달리 producer goroutine을 생성하지 않는다.
func (c *GatewayClient) ExecuteStream(ctx context.Context, sessionHandle, statement string, options StreamOptions) (ResultStream, error) {
	settings, err := c.executionSettings(options.ExecuteOptions)
	if err != nil {
		return nil, err
	}
	executionTimeout := options.ExecutionTimeout
	if executionTimeout <= 0 {
		executionTimeout = c.cfg.ExecutionTimeout
	}
	lifecycleCtx, lifecycleCancel := mergeContext(ctx, c.lifecycleCtx)
	executionCtx := lifecycleCtx
	timeoutCancel := func() {}
	if executionTimeout > 0 {
		executionCtx, timeoutCancel = context.WithTimeout(lifecycleCtx, executionTimeout)
	}
	cancel := func() {
		timeoutCancel()
		lifecycleCancel()
	}

	operation, err := c.ExecuteStatement(executionCtx, sessionHandle, ExecuteStatementRequest{
		Statement:        statement,
		ExecutionConfig:  options.ExecutionConfig,
		ExecutionTimeout: executionTimeout,
	})
	if err != nil {
		cancel()
		return nil, err
	}
	stream := &resultStream{
		client:       c,
		ctx:          executionCtx,
		cancel:       cancel,
		operation:    operation,
		limits:       settings,
		interval:     c.cfg.PollInterval,
		currentEvent: ResultEvent{Type: ResultEventOperation, Operation: cloneOperation(operation)},
		cleanupDone:  make(chan struct{}),
	}
	if err := c.registerStream(stream); err != nil {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), c.cfg.RequestTimeout)
		_ = c.cleanupOperationContext(withInternalCleanup(cleanupCtx), operation, err)
		cleanupCancel()
		cancel()
		return nil, err
	}
	return stream, nil
}

// resultStream은 동기 Next 호출 사이의 paging, 제한과 operation cleanup 상태를 소유한다.
type resultStream struct {
	client    *GatewayClient
	ctx       context.Context
	cancel    context.CancelFunc
	operation *Operation
	limits    executionLimits

	nextMu       sync.Mutex
	stateMu      sync.RWMutex
	closed       bool
	streamErr    error
	cleanupErr   error
	cleanupOnce  sync.Once
	cleanupDone  chan struct{}
	currentEvent ResultEvent
	currentRow   Row
	jobID        string
	columns      []ColumnInfo
	rowsYielded  int
	polls        int
	interval     time.Duration
	nextURL      *url.URL
	pendingRows  []Row
	pendingIndex int
	pendingEOS   bool
	pendingPage  *ResultPage
}

// 컴파일 시 resultStream이 공개 ResultStream 계약을 모두 구현하는지 확인한다.
var _ ResultStream = (*resultStream)(nil)

// Next는 다음 row를 준비하며 EOS, 오류 또는 제한에 도달하면 false를 반환하고 자원을 정리한다.
func (s *resultStream) Next() bool {
	s.nextMu.Lock()
	defer s.nextMu.Unlock()

	for {
		if s.isClosed() {
			return false
		}
		if err := s.ctx.Err(); err != nil {
			s.finishFromNext(err, s.client.cfg.CancelOnContextDone)
			return false
		}
		if s.pendingIndex < len(s.pendingRows) {
			if s.rowsYielded >= s.limits.maxRows {
				err := &ResultLimitError{Kind: "rows", Limit: s.limits.maxRows, Received: s.rowsYielded}
				s.finishFromNext(err, true)
				return false
			}
			row := s.pendingRows[s.pendingIndex]
			s.pendingIndex++
			s.rowsYielded++
			s.stateMu.Lock()
			s.currentRow = row
			rowCopy := row
			s.currentEvent = ResultEvent{Type: ResultEventRow, Row: &rowCopy}
			s.stateMu.Unlock()
			return true
		}
		if s.pendingEOS {
			s.stateMu.Lock()
			s.currentEvent = ResultEvent{Type: ResultEventEOS, Page: pageWithoutRows(s.pendingPage)}
			s.stateMu.Unlock()
			s.finishFromNext(nil, false)
			return false
		}
		if s.polls >= s.limits.maxPolls {
			err := &ResultLimitError{Kind: "polls", Limit: s.limits.maxPolls, Received: s.polls}
			s.finishFromNext(err, true)
			return false
		}

		page, err := s.fetchNextPage()
		s.polls++
		if err != nil {
			err = s.client.operationFailure(s.ctx, s.operation.SessionHandle, s.operation.Handle, err)
			s.finishFromNext(err, s.client.cfg.CancelOnContextDone && isContextError(err))
			return false
		}
		if page.JobID != "" {
			s.stateMu.Lock()
			s.jobID = page.JobID
			s.stateMu.Unlock()
		}

		switch page.ResultType {
		case ResultNotReady:
			if err := s.setNextURL(page, false); err != nil {
				s.finishFromNext(err, false)
				return false
			}
			if err := waitContext(s.ctx, s.interval); err != nil {
				s.finishFromNext(err, s.client.cfg.CancelOnContextDone)
				return false
			}
			s.interval = nextPollInterval(s.interval, s.client.cfg.MaxPollInterval)
			continue

		case ResultPayload, ResultEOS:
			s.interval = s.client.cfg.PollInterval
			s.pendingPage = page
			s.pendingRows = nil
			s.pendingIndex = 0
			if page.Results != nil {
				if len(s.columns) == 0 && len(page.Results.Columns) > 0 {
					s.columns = cloneColumns(page.Results.Columns)
				}
				if err := validateResultRows(s.columns, page.Results.Data); err != nil {
					s.finishFromNext(err, false)
					return false
				}
				s.pendingRows = page.Results.Data
			}
			s.pendingEOS = page.ResultType == ResultEOS
			if !s.pendingEOS {
				if err := s.setNextURL(page, true); err != nil {
					s.finishFromNext(err, false)
					return false
				}
			}
			continue

		default:
			err := fmt.Errorf("flinksqlgateway: unknown resultType %q", page.ResultType)
			s.finishFromNext(err, false)
			return false
		}
	}
}

// fetchNextPage는 첫 token 또는 검증을 마친 server paging URL에서 다음 page를 가져온다.
func (s *resultStream) fetchNextPage() (*ResultPage, error) {
	if s.nextURL == nil {
		return s.client.FetchResults(s.ctx, s.operation.SessionHandle, s.operation.Handle, 0, s.limits.rowFormat)
	}
	return s.client.fetchResultsURL(s.ctx, s.nextURL)
}

// setNextURL은 page의 nextResultUri를 검증하고 필요할 때 누락을 protocol 오류로 처리한다.
func (s *resultStream) setNextURL(page *ResultPage, required bool) error {
	value := nextURLString(page)
	if value == "" {
		if required {
			return fmt.Errorf("flinksqlgateway: PAYLOAD result omitted nextResultUri")
		}
		s.nextURL = nil
		return nil
	}
	resolved, err := s.client.validateNextResultURI(value)
	if err != nil {
		return err
	}
	s.nextURL = resolved
	return nil
}

// Event는 최근 operation, row 또는 EOS event의 복사본을 반환한다.
func (s *resultStream) Event() ResultEvent {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return cloneResultEvent(s.currentEvent)
}

// Row는 최근 Next가 준비한 changelog row의 복사본을 반환한다.
func (s *resultStream) Row() Row {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return cloneRow(s.currentRow)
}

// Err는 iteration을 끝낸 주 오류와 보존된 cleanup 오류를 반환한다.
func (s *resultStream) Err() error {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.streamErr
}

// JobID는 결과 page에서 마지막으로 확인한 Flink Job ID를 반환한다.
func (s *resultStream) JobID() string {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.jobID
}

// Close는 iteration을 중지한다. EOS 전에는 operation을 명시적으로 취소하고 닫으며 EOS
// 후에는 추가 작업을 하지 않는다. 중복 호출에 안전하다.
func (s *resultStream) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.client.cfg.RequestTimeout)
	defer cancel()
	return s.closeWithContext(ctx, nil)
}

// closeWithContext는 client 종료와 사용자 Close가 공유하는 제한 시간 내 cleanup을 수행한다.
func (s *resultStream) closeWithContext(ctx context.Context, terminalErr error) error {
	s.cancel()
	s.finish(ctx, terminalErr, true)
	<-s.cleanupDone
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.cleanupErr
}

// finishFromNext는 Next 내부 종료 원인을 보존하면서 독립된 cleanup context로 정리한다.
func (s *resultStream) finishFromNext(primary error, forceCancel bool) {
	ctx, cancel := context.WithTimeout(context.Background(), s.client.cfg.RequestTimeout)
	defer cancel()
	s.finish(withInternalCleanup(ctx), primary, forceCancel)
	<-s.cleanupDone
}

// finish는 cancel과 close를 최대 한 번 실행하고 주 오류와 cleanup 오류를 함께 저장한다.
func (s *resultStream) finish(ctx context.Context, primary error, forceCancel bool) {
	s.cleanupOnce.Do(func() {
		defer close(s.cleanupDone)
		s.cancel()
		var cancelErr error
		if forceCancel || errors.Is(primary, ErrResultLimit) {
			cancelCtx, cancel := s.client.cancelStageContext(ctx)
			cancelErr = s.client.CancelOperation(cancelCtx, s.operation.SessionHandle, s.operation.Handle)
			cancel()
			if errors.Is(cancelErr, ErrOperationNotFound) {
				cancelErr = nil
			}
			s.client.observeLifecycle(ctx, Observation{Event: ObservationOperationCanceled, SessionHandle: s.operation.SessionHandle, OperationHandle: s.operation.Handle, Error: cancelErr})
		}
		closeErr := s.client.CloseOperation(ctx, s.operation.SessionHandle, s.operation.Handle)
		s.client.observeLifecycle(ctx, Observation{Event: ObservationOperationClosed, SessionHandle: s.operation.SessionHandle, OperationHandle: s.operation.Handle, Error: closeErr})

		var streamErr error = primary
		var cleanupErr error
		if cancelErr != nil || closeErr != nil {
			cleanupErr = &ExecutionError{
				CancelError:     cancelErr,
				CloseError:      closeErr,
				SessionHandle:   s.operation.SessionHandle,
				OperationHandle: s.operation.Handle,
				JobID:           s.JobID(),
			}
			streamErr = &ExecutionError{
				Cause:           primary,
				CancelError:     cancelErr,
				CloseError:      closeErr,
				SessionHandle:   s.operation.SessionHandle,
				OperationHandle: s.operation.Handle,
				JobID:           s.JobID(),
			}
		}
		s.stateMu.Lock()
		s.closed = true
		s.streamErr = streamErr
		s.cleanupErr = cleanupErr
		rows := s.rowsYielded
		jobID := s.jobID
		s.stateMu.Unlock()
		s.client.unregisterStream(s)
		s.client.observeLifecycle(ctx, Observation{Event: ObservationResultStreamClosed, SessionHandle: s.operation.SessionHandle, OperationHandle: s.operation.Handle, JobID: jobID, ResultRows: rows, Error: streamErr})
	})
}

// isClosed는 동시 호출에 안전하게 stream 종료 상태를 반환한다.
func (s *resultStream) isClosed() bool {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.closed
}
