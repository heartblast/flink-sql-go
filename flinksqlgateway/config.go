package flinksqlgateway

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultAPIVersion is the default REST API selected by Flink 1.20.4.
	DefaultAPIVersion = "v3"

	defaultRequestTimeout    = 10 * time.Second
	defaultExecutionTimeout  = 30 * time.Second
	defaultPollInterval      = 250 * time.Millisecond
	defaultMaxPollInterval   = 3 * time.Second
	defaultHeartbeatInterval = 30 * time.Second
	defaultMaxResultRows     = 1_000
	defaultMaxResponseBytes  = 8 << 20
	defaultMaxPolls          = 10_000
	defaultStreamBuffer      = 16
	defaultUserAgent         = "flink-sql-go/0.1"
)

// Config configures a Flink SQL Gateway client. Durations use Go's
// time.Duration. ExecutionTimeout is sent to Flink as milliseconds.
type Config struct {
	BaseURL    string
	APIVersion string
	HTTPClient *http.Client
	// OwnHTTPTransport permits Close to close idle connections on an injected
	// HTTP client's transport. Internally-created transports are always owned.
	OwnHTTPTransport    bool
	RequestTimeout      time.Duration
	ExecutionTimeout    time.Duration
	PollInterval        time.Duration
	MaxPollInterval     time.Duration
	HeartbeatInterval   time.Duration
	MaxResultRows       int
	MaxResponseBytes    int64
	DefaultRowFormat    RowFormat
	UserAgent           string
	Headers             map[string]string
	CancelOnContextDone bool

	// MaxPolls bounds a single high-level execution's result polling.
	MaxPolls int
	// StreamBuffer bounds StreamResults backpressure buffering.
	StreamBuffer int
	// Validator lets the containing service enforce ownership and SQL policy.
	Validator StatementValidator
	// Observer receives sanitized request measurements. It never receives SQL
	// text, authorization headers, or URL query strings.
	Observer Observer
	// LifecycleObserver receives optional sanitized high-level events. When it
	// is nil, Observer is type-asserted to LifecycleObserver.
	LifecycleObserver LifecycleObserver
}

func (cfg Config) normalize() (Config, *url.URL, *http.Client, error) {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return Config{}, nil, nil, fmt.Errorf("flinksqlgateway: BaseURL is required")
	}

	base, err := url.Parse(strings.TrimSpace(cfg.BaseURL))
	if err != nil {
		return Config{}, nil, nil, fmt.Errorf("flinksqlgateway: parse BaseURL: %w", err)
	}
	if (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return Config{}, nil, nil, fmt.Errorf("flinksqlgateway: BaseURL must be an http or https URL with a host")
	}
	if base.User != nil {
		return Config{}, nil, nil, fmt.Errorf("flinksqlgateway: BaseURL must not contain user information")
	}
	if base.RawQuery != "" || base.Fragment != "" {
		return Config{}, nil, nil, fmt.Errorf("flinksqlgateway: BaseURL must not contain a query or fragment")
	}
	base.Path = strings.TrimRight(base.Path, "/")

	version, err := normalizeVersion(cfg.APIVersion)
	if err != nil {
		return Config{}, nil, nil, err
	}
	cfg.APIVersion = version

	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}
	if cfg.ExecutionTimeout == 0 {
		cfg.ExecutionTimeout = defaultExecutionTimeout
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.MaxPollInterval == 0 {
		cfg.MaxPollInterval = defaultMaxPollInterval
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = defaultHeartbeatInterval
	}
	if cfg.MaxResultRows == 0 {
		cfg.MaxResultRows = defaultMaxResultRows
	}
	if cfg.MaxResponseBytes == 0 {
		cfg.MaxResponseBytes = defaultMaxResponseBytes
	}
	if cfg.MaxPolls == 0 {
		cfg.MaxPolls = defaultMaxPolls
	}
	if cfg.StreamBuffer == 0 {
		cfg.StreamBuffer = defaultStreamBuffer
	}
	if cfg.DefaultRowFormat == "" {
		cfg.DefaultRowFormat = RowFormatJSON
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = defaultUserAgent
	}

	if cfg.RequestTimeout < 0 || cfg.ExecutionTimeout < 0 || cfg.PollInterval <= 0 || cfg.MaxPollInterval <= 0 || cfg.HeartbeatInterval <= 0 {
		return Config{}, nil, nil, fmt.Errorf("flinksqlgateway: timeouts and intervals must be positive")
	}
	if cfg.MaxPollInterval < cfg.PollInterval {
		return Config{}, nil, nil, fmt.Errorf("flinksqlgateway: MaxPollInterval must be at least PollInterval")
	}
	if cfg.MaxResultRows < 0 || cfg.MaxResponseBytes <= 0 || cfg.MaxPolls <= 0 || cfg.StreamBuffer <= 0 {
		return Config{}, nil, nil, fmt.Errorf("flinksqlgateway: result, response, poll, and stream limits must be positive")
	}
	if !cfg.DefaultRowFormat.valid() {
		return Config{}, nil, nil, fmt.Errorf("flinksqlgateway: unsupported default row format %q", cfg.DefaultRowFormat)
	}

	headers := make(map[string]string, len(cfg.Headers))
	for key, value := range cfg.Headers {
		if strings.EqualFold(key, "Host") {
			return Config{}, nil, nil, fmt.Errorf("flinksqlgateway: Host must not be supplied through Headers")
		}
		headers[key] = value
	}
	cfg.Headers = headers

	var hc http.Client
	if cfg.HTTPClient != nil {
		hc = *cfg.HTTPClient
	} else {
		hc.Transport = http.DefaultTransport.(*http.Transport).Clone()
	}
	originalRedirect := hc.CheckRedirect
	hc.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if !strings.EqualFold(req.URL.Host, base.Host) || !strings.EqualFold(req.URL.Scheme, base.Scheme) {
			return ErrUnsafeNextResultURI
		}
		if originalRedirect != nil {
			return originalRedirect(req, via)
		}
		if len(via) >= 10 {
			return fmt.Errorf("flinksqlgateway: stopped after 10 redirects")
		}
		return nil
	}

	return cfg, base, &hc, nil
}

func normalizeVersion(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return DefaultAPIVersion, nil
	}
	value = strings.TrimPrefix(value, "v")
	n, err := strconv.Atoi(value)
	if err != nil || n < 1 {
		return "", fmt.Errorf("flinksqlgateway: invalid API version %q", value)
	}
	return "v" + strconv.Itoa(n), nil
}

func apiVersionNumber(version string) int {
	n, _ := strconv.Atoi(strings.TrimPrefix(strings.ToLower(version), "v"))
	return n
}
