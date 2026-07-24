package flinkrest

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultRequestTimeout   = 10 * time.Second
	defaultMaxResponseBytes = 8 << 20
	defaultUserAgent        = "flink-sql-go/flinkrest"
)

// Config configures the independent Flink JobManager REST client.
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
