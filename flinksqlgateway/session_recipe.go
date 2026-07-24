package flinksqlgateway

import (
	"context"
	"fmt"
	"time"
)

// SessionRecipe는 호출자가 제어하는 session 재구성 절차이다. 명시적으로 요청할 때만
// 실행하며 client가 session을 자동 복원하지 않는다.
type SessionRecipe struct {
	Name            string
	Properties      map[string]string
	SetupStatements []string
}

// SessionRecipeOptions는 recipe 실행 제한과 실패 시 session cleanup 정책을 지정한다.
type SessionRecipeOptions struct {
	ExecuteOptions       ExecuteOptions
	KeepSessionOnFailure bool
	CleanupTimeout       time.Duration
}

// StatementResult는 민감정보가 제거된 recipe 진행 상태이며 statement 원문은 의도적으로 제외한다.
type StatementResult struct {
	Index           int
	OperationHandle string
	Status          OperationStatus
	ResultKind      ResultKind
	JobID           string
}

// RecipeReplayResult는 recipe가 어느 단계까지 진행됐는지 정확히 보고한다.
type RecipeReplayResult struct {
	SessionHandle string
	Applied       []StatementResult
	FailedIndex   int
	Complete      bool
}

// RecipeReplayError는 statement 원문을 보관하지 않고 setup 오류와 선택적인 session 종료
// cleanup 오류를 함께 보존한다.
type RecipeReplayError struct {
	Cause         error
	CloseError    error
	SessionHandle string
	FailedIndex   int
}

// Error는 실패한 statement index와 cleanup 오류를 포함하되 statement 원문은 제외한다.
func (e *RecipeReplayError) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := fmt.Sprintf("flink sql session recipe failed at index %d", e.FailedIndex)
	if e.Cause != nil {
		message += ": setup execution failed"
	}
	if e.CloseError != nil {
		message += "; session cleanup: " + e.CloseError.Error()
	}
	return message
}

// Unwrap은 setup과 session cleanup 오류를 errors.Is와 errors.As가 모두 탐색하게 한다.
func (e *RecipeReplayError) Unwrap() []error {
	if e == nil {
		return nil
	}
	errors := make([]error, 0, 2)
	if e.Cause != nil {
		errors = append(errors, e.Cause)
	}
	if e.CloseError != nil {
		errors = append(errors, e.CloseError)
	}
	return errors
}

// OpenSessionFromRecipe는 기본 실행 및 cleanup 설정으로 명시적인 session recipe를 실행한다.
func (c *GatewayClient) OpenSessionFromRecipe(ctx context.Context, recipe SessionRecipe) (*RecipeReplayResult, error) {
	return c.OpenSessionFromRecipeWithOptions(ctx, recipe, SessionRecipeOptions{})
}

// OpenSessionFromRecipeWithOptions는 session을 만들고 입력 순서대로 setup statement를 실행한다.
// 실패해도 자동으로 다시 실행하지 않는다.
func (c *GatewayClient) OpenSessionFromRecipeWithOptions(ctx context.Context, recipe SessionRecipe, options SessionRecipeOptions) (*RecipeReplayResult, error) {
	if options.CleanupTimeout <= 0 {
		options.CleanupTimeout = c.cfg.RequestTimeout
	}
	session, err := c.OpenSession(ctx, OpenSessionRequest{
		SessionName: recipe.Name,
		Properties:  cloneMap(recipe.Properties),
	})
	if err != nil {
		return nil, err
	}
	result := &RecipeReplayResult{
		SessionHandle: session.Handle,
		Applied:       make([]StatementResult, 0, len(recipe.SetupStatements)),
		FailedIndex:   -1,
	}

	// base client가 동시 실행을 지원해도 recipe 순서를 보장하도록 직렬 실행한다.
	// 지속적인 wrapper나 자동 replay는 만들지 않는다.
	for index, statement := range recipe.SetupStatements {
		execution, executeErr := c.ExecuteAndWait(ctx, session.Handle, statement, options.ExecuteOptions)
		if executeErr != nil {
			result.FailedIndex = index
			var closeErr error
			if !options.KeepSessionOnFailure {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), options.CleanupTimeout)
				closeErr = c.CloseSession(withInternalCleanup(cleanupCtx), session.Handle)
				cancel()
			}
			return result, &RecipeReplayError{
				Cause:         executeErr,
				CloseError:    closeErr,
				SessionHandle: session.Handle,
				FailedIndex:   index,
			}
		}
		statementResult := StatementResult{Index: index}
		if execution != nil {
			statementResult.Status = execution.Status
			statementResult.ResultKind = execution.ResultKind
			statementResult.JobID = execution.JobID
			if execution.Operation != nil {
				statementResult.OperationHandle = execution.Operation.Handle
			}
		}
		result.Applied = append(result.Applied, statementResult)
	}
	result.Complete = true
	return result, nil
}
