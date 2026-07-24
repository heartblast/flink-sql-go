package flinksqlgateway

import (
	"fmt"
	"net/url"
	"strings"
)

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

// MaskHandle returns a correlation-safe representation without exposing a
// complete session or operation handle.
func MaskHandle(handle string) string {
	if len(handle) <= 8 {
		return "********"
	}
	return handle[:4] + "..." + handle[len(handle)-4:]
}
