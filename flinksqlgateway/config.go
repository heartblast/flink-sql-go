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
	// DefaultAPIVersion은 Flink 1.20.4 client가 기본으로 선택하는 REST API 버전이다.
	DefaultAPIVersion = "v3"

	// 다음 값들은 호출자가 설정을 생략할 때 자원 사용을 제한하는 기본값이다.
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

// Config는 Flink SQL Gateway client의 연결, 실행과 자원 제한을 설정한다.
// 시간 값은 time.Duration을 사용하며 ExecutionTimeout은 millisecond로 Flink에 전송한다.
type Config struct {
	BaseURL    string
	APIVersion string
	HTTPClient *http.Client
	// OwnHTTPTransport는 주입된 HTTP client transport의 idle connection을 Close가
	// 정리하도록 허용한다. 내부에서 생성한 transport는 항상 client가 소유한다.
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

	// MaxPolls는 한 번의 고수준 실행에서 허용하는 결과 polling 횟수의 상한이다.
	MaxPolls int
	// StreamBuffer는 StreamResults가 backpressure 중 보관할 event 수의 상한이다.
	StreamBuffer int
	// Validator는 상위 서비스가 session 소유권과 SQL 정책을 적용하게 한다.
	Validator StatementValidator
	// Observer는 정제된 요청 측정값을 받으며 SQL 원문, 인증 header와 URL query를 받지 않는다.
	Observer Observer
	// LifecycleObserver는 선택적인 고수준 수명주기 event를 받는다. nil이면 Observer가
	// LifecycleObserver도 구현하는지 확인하여 사용한다.
	LifecycleObserver LifecycleObserver
}

// normalize는 설정을 검증하고 호출자의 HTTP client 값을 직접 변경하지 않은 채 사용할 복사본을 만든다.
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

// normalizeVersion은 생략된 값과 대소문자를 정규화해 vN 형식의 API 버전을 반환한다.
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

// apiVersionNumber는 정규화된 API 버전에서 기능 비교에 사용할 정수 부분을 반환한다.
func apiVersionNumber(version string) int {
	n, _ := strconv.Atoi(strings.TrimPrefix(strings.ToLower(version), "v"))
	return n
}
