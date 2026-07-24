package flinksqlgateway

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"
)

// ExecuteStream submits a statement and returns a synchronous, incremental
// iterator. Unlike StreamResults it does not create a producer goroutine.
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
		ExecutionTimeout: executionTimeout,
		ExecutionConfig:  options.ExecutionConfig,
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
		currentEvent: ResultEvent{Type: ResultEventOperation, Operation: operation},
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
	rowsYielded  int
	polls        int
	interval     time.Duration
	nextURL      *url.URL
	pendingRows  []Row
	pendingIndex int
	pendingEOS   bool
	pendingPage  *ResultPage
}

var _ ResultStream = (*resultStream)(nil)

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

func (s *resultStream) fetchNextPage() (*ResultPage, error) {
	if s.nextURL == nil {
		return s.client.FetchResults(s.ctx, s.operation.SessionHandle, s.operation.Handle, 0, s.limits.rowFormat)
	}
	return s.client.fetchResultsURL(s.ctx, s.nextURL)
}

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

func (s *resultStream) Event() ResultEvent {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.currentEvent
}

func (s *resultStream) Row() Row {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.currentRow
}

func (s *resultStream) Err() error {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.streamErr
}

func (s *resultStream) JobID() string {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.jobID
}

// Close stops iteration. Before EOS it explicitly cancels and closes the
// operation; after EOS it is a no-op. It is safe to call repeatedly.
func (s *resultStream) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.client.cfg.RequestTimeout)
	defer cancel()
	return s.closeWithContext(ctx, nil)
}

func (s *resultStream) closeWithContext(ctx context.Context, terminalErr error) error {
	s.cancel()
	s.finish(ctx, terminalErr, true)
	<-s.cleanupDone
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.cleanupErr
}

func (s *resultStream) finishFromNext(primary error, forceCancel bool) {
	ctx, cancel := context.WithTimeout(context.Background(), s.client.cfg.RequestTimeout)
	defer cancel()
	s.finish(withInternalCleanup(ctx), primary, forceCancel)
	<-s.cleanupDone
}

func (s *resultStream) finish(ctx context.Context, primary error, forceCancel bool) {
	s.cleanupOnce.Do(func() {
		defer close(s.cleanupDone)
		s.cancel()
		var cancelErr error
		if forceCancel || errors.Is(primary, ErrResultLimit) {
			cancelErr = s.client.CancelOperation(ctx, s.operation.SessionHandle, s.operation.Handle)
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

func (s *resultStream) isClosed() bool {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.closed
}
