package flinksqlgateway

import (
	"context"
	"fmt"
	"net/url"
	"time"
)

// executionLimits는 한 실행에 적용할 row 형식과 메모리 및 polling 상한을 정규화해 보관한다.
type executionLimits struct {
	rowFormat RowFormat
	maxRows   int
	maxPolls  int
}

// consumeResults는 server paging URI를 검증하며 EOS까지 결과를 수집하거나 event로 전달한다.
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
				result.Columns = cloneColumns(page.Results.Columns)
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
						rowSnapshot := cloneRow(row)
						if err := emit(ResultEvent{Type: ResultEventRow, Row: &rowSnapshot}); err != nil {
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

// pageWithoutRows는 stream page event가 row 전체를 중복 보관하지 않도록 metadata만 복사한다.
func pageWithoutRows(page *ResultPage) *ResultPage {
	copyPage := cloneResultPage(page)
	if copyPage == nil {
		return nil
	}
	copyPage.Raw = nil
	if copyPage.Results != nil {
		copyPage.Results.Data = nil
	}
	return copyPage
}

// nextPollInterval은 overflow와 설정 상한을 지키며 polling 간격을 두 배로 늘린다.
func nextPollInterval(current, maximum time.Duration) time.Duration {
	next := current * 2
	if next < current || next > maximum {
		return maximum
	}
	return next
}
