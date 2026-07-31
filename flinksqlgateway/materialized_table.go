package flinksqlgateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// MaterializedTableRefresher는 기존 Client 계약을 확장하지 않고 선택한 REST API가 제공하는
// Materialized Table refresh endpoint를 노출한다.
type MaterializedTableRefresher interface {
	// RefreshMaterializedTable은 v3 refresh 요청을 제출하고 기존 Operation 모델을 반환한다.
	RefreshMaterializedTable(ctx context.Context, sessionHandle, identifier string, req RefreshMaterializedTableRequest) (*Operation, error)
}

// RefreshMaterializedTableRequest는 Materialized Table refresh의 schedule 및 실행 설정이다.
type RefreshMaterializedTableRequest struct {
	// Periodic은 주기적 refresh 여부이며 wire에서는 canonical isPeriodic key로 전송된다.
	Periodic bool
	// ScheduleTime은 Flink가 해석할 schedule 표현이다. 빈 값이면 JSON field를 생략한다.
	ScheduleTime string
	// DynamicOptions는 현재 refresh에 적용할 동적 table option이다. nil과 빈 map을 구분한다.
	DynamicOptions map[string]string
	// StaticPartitions는 refresh할 정적 partition 값이다. nil과 빈 map을 구분한다.
	StaticPartitions map[string]string
	// ExecutionConfig는 refresh 실행 설정이다. nil과 빈 map을 구분한다.
	ExecutionConfig map[string]string
}

// RefreshMaterializedTable은 비멱등 POST를 자동 재시도하지 않으며 응답이 불명확하면
// MaterializedTableRefreshOutcomeUnknownError를 반환한다.
func (c *GatewayClient) RefreshMaterializedTable(
	ctx context.Context,
	sessionHandle string,
	identifier string,
	req RefreshMaterializedTableRequest,
) (*Operation, error) {
	if err := validateSessionHandle(sessionHandle); err != nil {
		return nil, err
	}
	if err := validateMaterializedTableIdentifier(identifier); err != nil {
		return nil, err
	}
	if _, err := c.compatibilityForCapability(ctx, "materialized-table-refresh", func(capabilities Capabilities) bool {
		return capabilities.MaterializedTable
	}); err != nil {
		return nil, err
	}

	type wireRequest struct {
		IsPeriodic       bool               `json:"isPeriodic"`
		ScheduleTime     *string            `json:"scheduleTime,omitempty"`
		DynamicOptions   *map[string]string `json:"dynamicOptions,omitempty"`
		StaticPartitions *map[string]string `json:"staticPartitions,omitempty"`
		ExecutionConfig  *map[string]string `json:"executionConfig,omitempty"`
	}
	body := wireRequest{
		IsPeriodic:       req.Periodic,
		DynamicOptions:   optionalMapCopy(req.DynamicOptions),
		StaticPartitions: optionalMapCopy(req.StaticPartitions),
		ExecutionConfig:  optionalMapCopy(req.ExecutionConfig),
	}
	if req.ScheduleTime != "" {
		scheduleTime := req.ScheduleTime
		body.ScheduleTime = &scheduleTime
	}
	target, _ := c.endpointURL(true, "/sessions/"+pathSegment(sessionHandle)+"/materialized-tables/"+pathSegment(identifier)+"/refresh")
	endpoint := sanitizeEndpointPath(target.EscapedPath())
	var response struct {
		Handle string `json:"operationHandle"`
	}
	requestCtx := withServerMessageRedaction(ctx, serverMessageRedaction{redactAll: true})
	if _, err := c.doJSON(requestCtx, http.MethodPost, target, body, &response, false); err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && nonIdempotentOutcomeIsUnknown(apiErr) {
			return nil, &MaterializedTableRefreshOutcomeUnknownError{
				SessionHandle: sessionHandle,
				Method:        http.MethodPost,
				Endpoint:      endpoint,
				RequestPhase:  apiErr.RequestPhase,
				Cause:         err,
			}
		}
		return nil, err
	}
	if err := validateOperationHandle(response.Handle); err != nil {
		return nil, &MaterializedTableRefreshOutcomeUnknownError{
			SessionHandle: sessionHandle,
			Method:        http.MethodPost,
			Endpoint:      endpoint,
			RequestPhase:  ResponseBodyIncomplete,
			Cause:         fmt.Errorf("flinksqlgateway: refresh response has invalid operationHandle: %w", err),
		}
	}
	return &Operation{Handle: response.Handle, SessionHandle: sessionHandle, CreatedAt: time.Now()}, nil
}

// optionalMapCopy는 nil map은 JSON field 생략으로, non-nil 빈 map은 빈 object로 유지하면서
// caller와 wire payload가 같은 map을 공유하지 않게 한다.
func optionalMapCopy(input map[string]string) *map[string]string {
	if input == nil {
		return nil
	}
	copy := cloneMap(input)
	return &copy
}
