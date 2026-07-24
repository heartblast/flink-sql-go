package flinksqlgateway

import (
	"context"
	"errors"
	"fmt"
)

// ExecuteAndWait submits a statement, follows server-provided paging URIs,
// collects at most MaxRows, and closes the operation.
func (c *GatewayClient) ExecuteAndWait(
	ctx context.Context,
	sessionHandle string,
	statement string,
	options ExecuteOptions,
) (*ExecutionResult, error) {
	settings, err := c.executionSettings(options)
	if err != nil {
		return nil, err
	}
	executionCtx, cancel := mergeContext(ctx, c.lifecycleCtx)
	defer cancel()
	return c.runExecution(executionCtx, sessionHandle, statement, options, settings, true, nil)
}

// StreamResults submits a statement and emits bounded events. Cancel ctx when
// the consumer stops reading so producer backpressure cannot retain a goroutine.
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
	streamCtx, cancel := mergeContext(ctx, c.lifecycleCtx)

	go func() {
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
		if err := emit(ResultEvent{Type: ResultEventOperation, Operation: operation}); err != nil {
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

func (c *GatewayClient) cleanupOperation(operation *Operation, reason error) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), c.cfg.RequestTimeout)
	defer cancel()
	return c.cleanupOperationContext(withInternalCleanup(cleanupCtx), operation, reason)
}

func (c *GatewayClient) cleanupOperationContext(ctx context.Context, operation *Operation, reason error) error {
	if operation == nil {
		return reason
	}
	var cancelErr error
	if errors.Is(reason, ErrResultLimit) || c.cfg.CancelOnContextDone && isContextError(reason) {
		cancelErr = c.CancelOperation(ctx, operation.SessionHandle, operation.Handle)
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
