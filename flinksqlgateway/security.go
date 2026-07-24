package flinksqlgateway

import (
	"fmt"
	"net/url"
	"strings"
)

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
