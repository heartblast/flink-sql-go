package flinksqlgateway

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Client exposes low-level REST operations and bounded high-level execution.
type Client interface {
	GetInfo(ctx context.Context) (*GatewayInfo, error)
	GetAPIVersions(ctx context.Context) ([]string, error)
	CheckAPIVersion(ctx context.Context) error
	OpenSession(ctx context.Context, req OpenSessionRequest) (*Session, error)
	GetSessionConfig(ctx context.Context, sessionHandle string) (map[string]string, error)
	ConfigureSession(ctx context.Context, sessionHandle, statement string, executionTimeout time.Duration) error
	CompleteStatement(ctx context.Context, sessionHandle, statement string, position int) ([]string, error)
	Heartbeat(ctx context.Context, sessionHandle string) error
	ExecuteStatement(ctx context.Context, sessionHandle string, req ExecuteStatementRequest) (*Operation, error)
	GetOperationStatus(ctx context.Context, sessionHandle, operationHandle string) (OperationStatus, error)
	FetchResults(ctx context.Context, sessionHandle, operationHandle string, token int64, rowFormat RowFormat) (*ResultPage, error)
	CancelOperation(ctx context.Context, sessionHandle, operationHandle string) error
	CloseOperation(ctx context.Context, sessionHandle, operationHandle string) error
	CloseSession(ctx context.Context, sessionHandle string) error
	StatementExecutor
}

// StatementExecutor provides bounded convenience APIs above Client's REST
// operations.
type StatementExecutor interface {
	ExecuteAndWait(ctx context.Context, sessionHandle, statement string, options ExecuteOptions) (*ExecutionResult, error)
	StreamResults(ctx context.Context, sessionHandle, statement string, options StreamOptions) (<-chan ResultEvent, <-chan error)
}

// GatewayClient is safe for concurrent use. A Session's state-changing SQL
// ordering remains the caller's responsibility.
type GatewayClient struct {
	cfg               Config
	baseURL           *url.URL
	httpClient        *http.Client
	ownsHTTPTransport bool
	lifecycleCtx      context.Context
	lifecycleCancel   context.CancelFunc
	clientCloseOnce   sync.Once
	clientCloseDone   chan struct{}
	clientCloseErr    error

	versionMu      sync.Mutex
	versionChecked bool
	versionErr     error

	stateMu      sync.Mutex
	sessions     map[string]*Session
	closed       map[string]struct{}
	closeCalls   map[string]*sessionCloseCall
	heartbeats   map[string]*HeartbeatRunner
	clientClosed bool
	managed      map[*managedSession]struct{}
	streams      map[*resultStream]struct{}
}

var _ Client = (*GatewayClient)(nil)

// NewClient validates configuration and creates a reusable client. It does
// not perform network I/O; the first versioned call verifies APIVersion.
func NewClient(cfg Config) (*GatewayClient, error) {
	ownsHTTPTransport := cfg.HTTPClient == nil || cfg.OwnHTTPTransport
	normalized, base, hc, err := cfg.normalize()
	if err != nil {
		return nil, err
	}
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	return &GatewayClient{
		cfg:               normalized,
		baseURL:           base,
		httpClient:        hc,
		ownsHTTPTransport: ownsHTTPTransport,
		lifecycleCtx:      lifecycleCtx,
		lifecycleCancel:   lifecycleCancel,
		clientCloseDone:   make(chan struct{}),
		sessions:          make(map[string]*Session),
		closed:            make(map[string]struct{}),
		closeCalls:        make(map[string]*sessionCloseCall),
		heartbeats:        make(map[string]*HeartbeatRunner),
		managed:           make(map[*managedSession]struct{}),
		streams:           make(map[*resultStream]struct{}),
	}, nil
}

// CheckAPIVersion verifies that the configured version is advertised by the
// gateway. Successful and unsupported results are cached.
func (c *GatewayClient) CheckAPIVersion(ctx context.Context) error {
	c.versionMu.Lock()
	defer c.versionMu.Unlock()
	if c.versionChecked {
		return c.versionErr
	}
	versions, err := c.getAPIVersions(ctx)
	if err != nil {
		return err
	}
	for _, version := range versions {
		if strings.EqualFold(version, c.cfg.APIVersion) || strings.EqualFold(version, strings.ToUpper(c.cfg.APIVersion)) {
			c.versionChecked = true
			return nil
		}
	}
	c.versionChecked = true
	c.versionErr = fmt.Errorf("%w: configured=%s advertised=%v", ErrUnsupportedAPI, c.cfg.APIVersion, versions)
	return c.versionErr
}

func (c *GatewayClient) sessionContext(handle string) SessionContext {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	context := SessionContext{Handle: handle}
	if session := c.sessions[handle]; session != nil {
		context.Name = session.Name
		context.Properties = cloneMap(session.Properties)
	}
	return context
}

func cloneMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
