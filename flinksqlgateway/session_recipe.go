package flinksqlgateway

import (
	"context"
	"fmt"
	"time"
)

// SessionRecipe describes caller-controlled session reconstruction. Recipes
// are replayed only when explicitly requested; the client never restores a
// session automatically.
type SessionRecipe struct {
	Name            string
	Properties      map[string]string
	SetupStatements []string
}

// SessionRecipeOptions controls replay and failure cleanup.
type SessionRecipeOptions struct {
	ExecuteOptions       ExecuteOptions
	KeepSessionOnFailure bool
	CleanupTimeout       time.Duration
}

// StatementResult contains non-sensitive replay progress. Statement text is
// intentionally omitted.
type StatementResult struct {
	Index           int
	OperationHandle string
	Status          OperationStatus
	ResultKind      ResultKind
	JobID           string
}

// RecipeReplayResult reports exactly how far a recipe progressed.
type RecipeReplayResult struct {
	SessionHandle string
	Applied       []StatementResult
	FailedIndex   int
	Complete      bool
}

// RecipeReplayError preserves the setup failure and optional session-close
// cleanup failure without retaining statement text.
type RecipeReplayError struct {
	Cause         error
	CloseError    error
	SessionHandle string
	FailedIndex   int
}

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

// OpenSessionFromRecipe uses default execution and cleanup settings.
func (c *GatewayClient) OpenSessionFromRecipe(ctx context.Context, recipe SessionRecipe) (*RecipeReplayResult, error) {
	return c.OpenSessionFromRecipeWithOptions(ctx, recipe, SessionRecipeOptions{})
}

// OpenSessionFromRecipeWithOptions creates a session and executes setup
// statements in input order. A failure never triggers automatic replay.
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

	// The loop is deliberately sequential even though the base client remains
	// concurrent. No persistent wrapper or automatic replay is created.
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
