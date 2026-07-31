package flinksqlgateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"unicode/utf8"
)

// ScriptDeployer는 기존 Client와 JobManager lifecycle 계약을 확장하지 않고 Application Mode
// SQL Script 배포 endpoint를 노출한다.
type ScriptDeployer interface {
	// DeployScript는 inline Script 또는 Script URI 하나를 Application Mode로 배포한다.
	DeployScript(ctx context.Context, sessionHandle string, req DeployScriptRequest) (*ScriptDeployment, error)
}

// DeployScriptRequest는 inline Script와 URI 중 정확히 하나 및 별도 실행 설정을 전달한다.
type DeployScriptRequest struct {
	// Script는 변경 없이 전송할 inline SQL Script이다.
	Script string
	// ScriptURI는 Flink가 지원하는 URI이며 client가 scheme이나 query를 재작성하지 않는다.
	ScriptURI string
	// ExecutionConfig는 배포 실행 설정이며 전송 전에 caller map에서 복사된다.
	ExecutionConfig map[string]string
}

// ScriptDeployment는 Application Mode 배포 응답의 opaque cluster identifier이다.
type ScriptDeployment struct {
	// ClusterID는 Flink가 반환한 값을 그대로 보존하며 JobID나 UUID 형식으로 해석하지 않는다.
	ClusterID string
}

// DeployScript는 비멱등 POST를 자동 재시도하지 않으며 응답이 불명확하면
// ScriptDeploymentOutcomeUnknownError를 반환한다.
func (c *GatewayClient) DeployScript(ctx context.Context, sessionHandle string, req DeployScriptRequest) (*ScriptDeployment, error) {
	if err := validateSessionHandle(sessionHandle); err != nil {
		return nil, err
	}
	hasScript := req.Script != ""
	hasScriptURI := req.ScriptURI != ""
	if hasScript == hasScriptURI {
		return nil, fmt.Errorf("flinksqlgateway: exactly one of Script and ScriptURI is required")
	}
	if hasScript {
		if strings.TrimSpace(req.Script) == "" {
			return nil, fmt.Errorf("flinksqlgateway: Script must contain non-whitespace content")
		}
		if !utf8.ValidString(req.Script) {
			return nil, fmt.Errorf("flinksqlgateway: Script is not valid UTF-8")
		}
	} else if err := validateScriptURI(req.ScriptURI); err != nil {
		return nil, err
	}
	if _, err := c.compatibilityForCapability(ctx, "deploy-script", func(capabilities Capabilities) bool {
		return capabilities.DeployScript
	}); err != nil {
		return nil, err
	}

	type wireRequest struct {
		Script          *string           `json:"script"`
		ScriptURI       *string           `json:"scriptUri"`
		ExecutionConfig map[string]string `json:"executionConfig"`
	}
	body := wireRequest{ExecutionConfig: cloneMap(req.ExecutionConfig)}
	if body.ExecutionConfig == nil {
		body.ExecutionConfig = map[string]string{}
	}
	if hasScript {
		script := req.Script
		body.Script = &script
	} else {
		scriptURI := req.ScriptURI
		body.ScriptURI = &scriptURI
	}
	target, _ := c.endpointURL(true, "/sessions/"+pathSegment(sessionHandle)+"/scripts")
	endpoint := sanitizeEndpointPath(target.EscapedPath())
	var response struct {
		ClusterID string `json:"clusterID"`
	}
	requestCtx := withServerMessageRedaction(ctx, serverMessageRedaction{redactAll: true})
	if _, err := c.doJSON(requestCtx, http.MethodPost, target, body, &response, false); err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && nonIdempotentOutcomeIsUnknown(apiErr) {
			return nil, &ScriptDeploymentOutcomeUnknownError{
				SessionHandle: sessionHandle,
				Method:        http.MethodPost,
				Endpoint:      endpoint,
				RequestPhase:  apiErr.RequestPhase,
				Cause:         err,
			}
		}
		return nil, err
	}
	if strings.TrimSpace(response.ClusterID) == "" {
		return nil, &ScriptDeploymentOutcomeUnknownError{
			SessionHandle: sessionHandle,
			Method:        http.MethodPost,
			Endpoint:      endpoint,
			RequestPhase:  ResponseBodyIncomplete,
			Cause:         fmt.Errorf("flinksqlgateway: deploy response has an empty clusterID"),
		}
	}
	return &ScriptDeployment{ClusterID: response.ClusterID}, nil
}
