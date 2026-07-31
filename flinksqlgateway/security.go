package flinksqlgateway

import (
	"fmt"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"
)

// maxHandleBytes는 비정상적으로 큰 handle이 URL과 내부 상태를 점유하지 못하게 하는 상한이다.
const maxHandleBytes = 4096

// validateSessionHandle은 network 호출 전에 불투명한 session handle의 최소 안전 조건을 확인한다.
func validateSessionHandle(handle string) error {
	return validateOpaqueHandle("session", handle)
}

// validateOperationHandle은 network 호출 전에 불투명한 operation handle의 최소 안전 조건을 확인한다.
func validateOperationHandle(handle string) error {
	return validateOpaqueHandle("operation", handle)
}

// validateMaterializedTableIdentifier는 fully qualified identifier를 SQL로 해석하지 않고 하나의
// REST path parameter로 전달하기 위한 최소 안전 조건만 확인한다.
func validateMaterializedTableIdentifier(identifier string) error {
	if strings.TrimSpace(identifier) == "" {
		return fmt.Errorf("flinksqlgateway: materialized table identifier is required")
	}
	if len(identifier) > maxHandleBytes {
		return fmt.Errorf("flinksqlgateway: materialized table identifier exceeds %d bytes", maxHandleBytes)
	}
	if !utf8.ValidString(identifier) {
		return fmt.Errorf("flinksqlgateway: materialized table identifier is not valid UTF-8")
	}
	for _, value := range identifier {
		if unicode.IsControl(value) {
			return fmt.Errorf("flinksqlgateway: materialized table identifier contains a control character")
		}
	}
	return nil
}

// validateScriptURI는 Flink가 해석할 URI scheme을 제한하지 않되 빈 값, 잘못된 UTF-8과
// URL/오류 경계를 흐릴 수 있는 제어문자는 network 호출 전에 거부한다.
func validateScriptURI(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("flinksqlgateway: script URI is required")
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("flinksqlgateway: script URI is not valid UTF-8")
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("flinksqlgateway: script URI contains a control character")
		}
	}
	return nil
}

// validateOpaqueHandle은 slash와 일반 Unicode는 허용하고 빈 값, 제어문자와 과도한 크기를 차단한다.
func validateOpaqueHandle(kind, handle string) error {
	if strings.TrimSpace(handle) == "" {
		return fmt.Errorf("flinksqlgateway: %s handle is required", kind)
	}
	if len(handle) > maxHandleBytes {
		return fmt.Errorf("flinksqlgateway: %s handle exceeds %d bytes", kind, maxHandleBytes)
	}
	if !utf8.ValidString(handle) {
		return fmt.Errorf("flinksqlgateway: %s handle is not valid UTF-8", kind)
	}
	for _, value := range handle {
		if unicode.IsControl(value) {
			return fmt.Errorf("flinksqlgateway: %s handle contains a control character", kind)
		}
	}
	return nil
}

// validateNextResultURI는 paging URI가 인증정보 없이 최초 Gateway와 같은 scheme 및 host를
// 유지하는지 검증해 SSRF와 header 유출을 방지한다.
func (c *GatewayClient) validateNextResultURI(value string) (*url.URL, error) {
	ref, err := url.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("%w: parse: %v", ErrUnsafeNextResultURI, err)
	}
	if ref.User != nil || ref.Fragment != "" {
		return nil, fmt.Errorf("%w: user information and fragments are not allowed", ErrUnsafeNextResultURI)
	}

	base := *c.baseURL
	base.Path = strings.TrimRight(base.Path, "/") + "/"
	resolved := base.ResolveReference(ref)
	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		return nil, fmt.Errorf("%w: scheme %q", ErrUnsafeNextResultURI, resolved.Scheme)
	}
	if !strings.EqualFold(resolved.Scheme, c.baseURL.Scheme) || !strings.EqualFold(resolved.Host, c.baseURL.Host) {
		return nil, fmt.Errorf("%w: origin %s", ErrUnsafeNextResultURI, resolved.Host)
	}
	return resolved, nil
}

// MaskHandle은 전체 session 또는 operation handle을 노출하지 않는 관측용 표현을 반환한다.
func MaskHandle(handle string) string {
	if len(handle) <= 8 {
		return "********"
	}
	return handle[:4] + "..." + handle[len(handle)-4:]
}
