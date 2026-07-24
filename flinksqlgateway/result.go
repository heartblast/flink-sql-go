package flinksqlgateway

import (
	"context"
	"fmt"
	"net/url"
	"time"
)

type executionLimits struct {
	rowFormat RowFormat
	maxRows   int
	maxPolls  int
}

func (c *GatewayClient) consumeResults(
	ctx context.Context,
	operation *Operation,
	limits executionLimits,
	collect bool,
	emit func(ResultEvent) error,
) (*ExecutionResult, error) {
	result := &ExecutionResult{Operation: operation}
	interval := c.cfg.PollInterval
	var next *url.URL

	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if result.Polls >= limits.maxPolls {
			return result, &ResultLimitError{Kind: "polls", Limit: limits.maxPolls, Received: result.Polls}
		}

		var page *ResultPage
		var err error
		if next == nil {
			page, err = c.FetchResults(ctx, operation.SessionHandle, operation.Handle, 0, limits.rowFormat)
		} else {
			page, err = c.fetchResultsURL(ctx, next)
		}
		result.Polls++
		if err != nil {
			return result, c.operationFailure(ctx, operation.SessionHandle, operation.Handle, err)
		}

		switch page.ResultType {
		case ResultNotReady:
			if uri := nextURLString(page); uri != "" {
				next, err = c.validateNextResultURI(uri)
				if err != nil {
					return result, err
				}
			} else {
				next = nil
			}
			if err := waitContext(ctx, interval); err != nil {
				return result, err
			}
			interval = nextPollInterval(interval, c.cfg.MaxPollInterval)
			continue

		case ResultPayload, ResultEOS:
			interval = c.cfg.PollInterval
			result.Pages++
			result.ResultKind = page.ResultKind
			result.QueryResult = page.QueryResult
			if page.JobID != "" {
				result.JobID = page.JobID
			}
			if page.Results != nil && len(result.Columns) == 0 {
				result.Columns = append([]ColumnInfo(nil), page.Results.Columns...)
			}

			if emit != nil {
				metadata := pageWithoutRows(page)
				if err := emit(ResultEvent{Type: ResultEventPage, Page: metadata}); err != nil {
					return result, err
				}
			}
			if page.Results != nil {
				for index := range page.Results.Data {
					received := result.RowsReceived
					if received >= limits.maxRows {
						return result, &ResultLimitError{Kind: "rows", Limit: limits.maxRows, Received: received}
					}
					row := page.Results.Data[index]
					if collect {
						result.Rows = append(result.Rows, row)
					}
					result.RowsReceived++
					if emit != nil {
						if err := emit(ResultEvent{Type: ResultEventRow, Row: &row}); err != nil {
							return result, err
						}
					}
				}
			}

			if page.ResultType == ResultEOS {
				result.Status = OperationFinished
				if emit != nil {
					if err := emit(ResultEvent{Type: ResultEventEOS, Page: pageWithoutRows(page)}); err != nil {
						return result, err
					}
				}
				return result, nil
			}

			uri := nextURLString(page)
			if uri == "" {
				return result, fmt.Errorf("flinksqlgateway: PAYLOAD result omitted nextResultUri")
			}
			next, err = c.validateNextResultURI(uri)
			if err != nil {
				return result, err
			}

		default:
			return result, fmt.Errorf("flinksqlgateway: unknown resultType %q", page.ResultType)
		}
	}
}

func pageWithoutRows(page *ResultPage) *ResultPage {
	if page == nil {
		return nil
	}
	copyPage := *page
	copyPage.Raw = nil
	if page.Results != nil {
		copyInfo := *page.Results
		copyInfo.Data = nil
		copyPage.Results = &copyInfo
	}
	return &copyPage
}

func nextPollInterval(current, maximum time.Duration) time.Duration {
	next := current * 2
	if next < current || next > maximum {
		return maximum
	}
	return next
}
