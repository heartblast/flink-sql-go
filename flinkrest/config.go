package flinkrest

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// 다음 값들은 호출자가 값을 생략했을 때 적용하는 안전한 기본 제한이다.
	defaultRequestTimeout   = 10 * time.Second
	defaultMaxResponseBytes = 8 << 20
	defaultUserAgent        = "flink-sql-go/flinkrest"
)

// Config는 독립적인 Flink JobManager REST client의 연결과 자원 제한을 설정한다.
type Config struct {
	BaseURL          string
	HTTPClient       *http.Client
	RequestTimeout   time.Duration
	MaxResponseBytes int64
	Headers          map[string]string
	UserAgent        string
	OwnHTTPTransport bool
	ValidateJobID    bool
}

// normalize는 설정을 검증하고 호출자 설정을 변경하지 않은 채 사용할 HTTP client를 준비한다.
func (cfg Config) normalize() (Config, *url.URL, *http.Client, bool, error) {
	base, err := url.Parse(strings.TrimSpace(cfg.BaseURL))
	if err != nil {
		return Config{}, nil, nil, false, fmt.Errorf("flinkrest: parse BaseURL: %w", err)
	}
	if (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return Config{}, nil, nil, false, fmt.Errorf("flinkrest: BaseURL must be an http or https URL with a host")
	}
	if base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return Config{}, nil, nil, false, fmt.Errorf("flinkrest: BaseURL must not contain user information, query, or fragment")
	}
	base.Path = strings.TrimRight(base.Path, "/")
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}
	if cfg.MaxResponseBytes == 0 {
		cfg.MaxResponseBytes = defaultMaxResponseBytes
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = defaultUserAgent
	}
	if cfg.RequestTimeout < 0 || cfg.MaxResponseBytes <= 0 {
		return Config{}, nil, nil, false, fmt.Errorf("flinkrest: timeout must not be negative and response limit must be positive")
	}
	headers := make(map[string]string, len(cfg.Headers))
	for key, value := range cfg.Headers {
		if strings.EqualFold(key, "Host") {
			return Config{}, nil, nil, false, fmt.Errorf("flinkrest: Host must not be supplied through Headers")
		}
		headers[key] = value
	}
	cfg.Headers = headers

	// owned는 Close가 idle connection을 정리해도 되는 client 소유권을 나타낸다.
	owned := cfg.HTTPClient == nil || cfg.OwnHTTPTransport
	var httpClient http.Client
	if cfg.HTTPClient == nil {
		httpClient.Transport = http.DefaultTransport.(*http.Transport).Clone()
	} else {
		httpClient = *cfg.HTTPClient
	}
	originalRedirect := httpClient.CheckRedirect
	httpClient.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if !strings.EqualFold(request.URL.Scheme, base.Scheme) || !strings.EqualFold(request.URL.Host, base.Host) {
			return fmt.Errorf("flinkrest: redirect left configured origin")
		}
		if originalRedirect != nil {
			return originalRedirect(request, via)
		}
		if len(via) >= 10 {
			return fmt.Errorf("flinkrest: stopped after 10 redirects")
		}
		return nil
	}
	return cfg, base, &httpClient, owned, nil
}
