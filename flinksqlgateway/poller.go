package flinksqlgateway

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ExecuteAndWait는 statement를 제출하고 server가 제공한 paging URI를 따라 MaxRows까지
// 수집한 뒤 operation을 닫는다.
func (c *GatewayClient) ExecuteAndWait(
	ctx context.Context,
	sessionHandle string,
	statement string,
	options ExecuteOptions,
) (*ExecutionResult, error) {
	if err := c.executions.begin(); err != nil {
		return nil, err
	}
	defer c.executions.end()
	settings, err := c.executionSettings(options)
	if err != nil {
		return nil, err
	}
	executionCtx, cancel := mergeContext(ctx, c.lifecycleCtx)
	defer cancel()
	return c.runExecution(executionCtx, sessionHandle, statement, options, settings, true, nil)
}

// StreamResults는 statement를 제출하고 bounded channel로 event를 전달한다. 소비를 중단하면
// producer가 backpressure로 남지 않도록 ctx를 취소해야 한다.
func (c *GatewayClient) StreamResults(
	ctx context.Context,
	sessionHandle string,
	statement string,
	options StreamOptions,
) (<-chan ResultEvent, <-chan error) {
	buffer := options.Buffer
	if buffer <= 0 {
		buffer = c.cfg.StreamBuffer
	}
	events := make(chan ResultEvent, buffer)
	errorsChannel := make(chan error, 1)
	if err := c.executions.begin(); err != nil {
		errorsChannel <- err
		close(events)
		close(errorsChannel)
		return events, errorsChannel
	}
	streamCtx, cancel := mergeContext(ctx, c.lifecycleCtx)

	go func() {
		defer c.executions.end()
		defer cancel()
		defer close(events)
		defer close(errorsChannel)
		settings, err := c.executionSettings(options.ExecuteOptions)
		if err != nil {
			errorsChannel <- err
			return
		}
		emit := func(event ResultEvent) error {
			select {
			case <-streamCtx.Done():
				return streamCtx.Err()
			case events <- event:
				return nil
			}
		}
		_, err = c.runExecution(streamCtx, sessionHandle, statement, options.ExecuteOptions, settings, false, emit)
		if err != nil {
			errorsChannel <- err
		}
	}()
	return events, errorsChannel
}

// executionSettings는 호출별 값과 client 기본값을 합쳐 유효한 실행 제한을 만든다.
func (c *GatewayClient) executionSettings(options ExecuteOptions) (executionLimits, error) {
	rowFormat := options.RowFormat
	if rowFormat == "" {
		rowFormat = c.cfg.DefaultRowFormat
	}
	if !rowFormat.valid() {
		return executionLimits{}, fmt.Errorf("flinksqlgateway: unsupported row format %q", rowFormat)
	}
	maxRows := options.MaxRows
	if maxRows == 0 {
		maxRows = c.cfg.MaxResultRows
	}
	maxPolls := options.MaxPolls
	if maxPolls == 0 {
		maxPolls = c.cfg.MaxPolls
	}
	if maxRows <= 0 || maxPolls <= 0 {
		return executionLimits{}, fmt.Errorf("flinksqlgateway: execution limits must be positive")
	}
	return executionLimits{rowFormat: rowFormat, maxRows: maxRows, maxPolls: maxPolls}, nil
}

// runExecution은 제출, 결과 소비와 operation cleanup의 전체 수명주기를 소유한다.
func (c *GatewayClient) runExecution(
	ctx context.Context,
	sessionHandle string,
	statement string,
	options ExecuteOptions,
	limits executionLimits,
	collect bool,
	emit func(ResultEvent) error,
) (*ExecutionResult, error) {
	executionTimeout := options.ExecutionTimeout
	if executionTimeout <= 0 {
		executionTimeout = c.cfg.ExecutionTimeout
	}
	executionCtx := ctx
	cancelExecution := func() {}
	if executionTimeout > 0 {
		executionCtx, cancelExecution = context.WithTimeout(ctx, executionTimeout)
	}
	defer cancelExecution()

	operation, err := c.ExecuteStatement(executionCtx, sessionHandle, ExecuteStatementRequest{
		Statement:        statement,
		ExecutionTimeout: executionTimeout,
		ExecutionConfig:  options.ExecutionConfig,
	})
	if err != nil {
		return nil, err
	}
	if emit != nil {
		if err := emit(ResultEvent{Type: ResultEventOperation, Operation: cloneOperation(operation)}); err != nil {
			return &ExecutionResult{Operation: operation}, c.cleanupOperation(operation, err)
		}
	}

	result, runErr := c.consumeResults(executionCtx, operation, limits, collect, emit)
	if runErr != nil {
		return result, c.cleanupOperation(operation, runErr)
	}

	cleanupCtx, cancel := context.WithTimeout(context.Background(), c.cfg.RequestTimeout)
	defer cancel()
	if err := c.CloseOperation(cleanupCtx, operation.SessionHandle, operation.Handle); err != nil {
		return result, &ExecutionError{
			CloseError:      err,
			SessionHandle:   operation.SessionHandle,
			OperationHandle: operation.Handle,
			JobID:           result.JobID,
		}
	}
	c.observeLifecycle(ctx, Observation{Event: ObservationStatementCompleted, SessionHandle: operation.SessionHandle, OperationHandle: operation.Handle, JobID: result.JobID, ResultRows: result.RowsReceived})
	return result, nil
}

// cleanupOperation은 사용자 context와 분리된 제한 시간 안에서 operation 자원을 정리한다.
func (c *GatewayClient) cleanupOperation(operation *Operation, reason error) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), c.cfg.RequestTimeout)
	defer cancel()
	return c.cleanupOperationContext(withInternalCleanup(cleanupCtx), operation, reason)
}

// cleanupOperationContext는 원래 실패 원인을 보존하면서 필요한 cancel과 close 오류를 함께 반환한다.
func (c *GatewayClient) cleanupOperationContext(ctx context.Context, operation *Operation, reason error) error {
	if operation == nil {
		return reason
	}
	var cancelErr error
	if errors.Is(reason, ErrResultLimit) || c.cfg.CancelOnContextDone && isContextError(reason) {
		cancelCtx, cancel := c.cancelStageContext(ctx)
		cancelErr = c.CancelOperation(cancelCtx, operation.SessionHandle, operation.Handle)
		cancel()
		if errors.Is(cancelErr, ErrOperationNotFound) {
			cancelErr = nil
		}
		c.observeLifecycle(ctx, Observation{Event: ObservationOperationCanceled, SessionHandle: operation.SessionHandle, OperationHandle: operation.Handle, Error: cancelErr})
	}
	closeErr := c.CloseOperation(ctx, operation.SessionHandle, operation.Handle)
	c.observeLifecycle(ctx, Observation{Event: ObservationOperationClosed, SessionHandle: operation.SessionHandle, OperationHandle: operation.Handle, Error: closeErr})
	if cancelErr == nil && closeErr == nil {
		return reason
	}
	return &ExecutionError{
		Cause:           reason,
		CancelError:     cancelErr,
		CloseError:      closeErr,
		SessionHandle:   operation.SessionHandle,
		OperationHandle: operation.Handle,
	}
}

// cancelStageContext는 전체 cleanup deadline의 절반만 cancel에 배정해 close 실행 시간을 남긴다.
func (c *GatewayClient) cancelStageContext(parent context.Context) (context.Context, context.CancelFunc) {
	budget := c.cfg.RequestTimeout / 2
	if deadline, ok := parent.Deadline(); ok {
		remaining := time.Until(deadline)
		if half := remaining / 2; budget <= 0 || half < budget {
			budget = half
		}
	}
	if budget <= 0 {
		budget = time.Nanosecond
	}
	return context.WithTimeout(parent, budget)
}
