package flinksqlgateway

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// Client는 저수준 REST operation과 제한이 적용된 고수준 실행 API를 제공한다.
type Client interface {
	// GetInfo는 SQL Gateway 제품 metadata를 조회한다.
	GetInfo(ctx context.Context) (*GatewayInfo, error)
	// GetAPIVersions는 Gateway가 광고하는 REST API 버전을 조회한다.
	GetAPIVersions(ctx context.Context) ([]string, error)
	// CheckAPIVersion은 설정된 API 버전의 server 지원 여부를 검증한다.
	CheckAPIVersion(ctx context.Context) error
	// OpenSession은 상태를 보존하는 server-side session을 만든다.
	OpenSession(ctx context.Context, req OpenSessionRequest) (*Session, error)
	// GetSessionConfig는 현재 session property를 조회한다.
	GetSessionConfig(ctx context.Context, sessionHandle string) (map[string]string, error)
	// ConfigureSession은 v2 이상 session에 설정 statement를 적용하며 executionTimeout을
	// client-side 제한으로 사용한다.
	ConfigureSession(ctx context.Context, sessionHandle, statement string, executionTimeout time.Duration) error
	// CompleteStatement는 v2 이상에서 SQL 자동완성 후보를 조회한다.
	CompleteStatement(ctx context.Context, sessionHandle, statement string, position int) ([]string, error)
	// Heartbeat는 session 만료를 방지하는 heartbeat를 한 번 전송한다.
	Heartbeat(ctx context.Context, sessionHandle string) error
	// ExecuteStatement는 SQL을 제출하고 비동기 operation handle을 반환한다.
	ExecuteStatement(ctx context.Context, sessionHandle string, req ExecuteStatementRequest) (*Operation, error)
	// GetOperationStatus는 현재 operation 상태를 조회한다.
	GetOperationStatus(ctx context.Context, sessionHandle, operationHandle string) (OperationStatus, error)
	// FetchResults는 지정한 token의 결과 page를 조회한다.
	FetchResults(ctx context.Context, sessionHandle, operationHandle string, token int64, rowFormat RowFormat) (*ResultPage, error)
	// CancelOperation은 SQL operation 취소를 요청한다.
	CancelOperation(ctx context.Context, sessionHandle, operationHandle string) error
	// CloseOperation은 SQL operation 자원을 해제한다.
	CloseOperation(ctx context.Context, sessionHandle, operationHandle string) error
	// CloseSession은 heartbeat를 멈추고 server-side session을 닫는다.
	CloseSession(ctx context.Context, sessionHandle string) error
	StatementExecutor
}

// StatementExecutor는 Client의 REST operation 위에 제한이 적용된 편의 실행 API를 제공한다.
type StatementExecutor interface {
	// ExecuteAndWait는 bounded 결과를 수집하고 operation을 정리한다.
	ExecuteAndWait(ctx context.Context, sessionHandle, statement string, options ExecuteOptions) (*ExecutionResult, error)
	// StreamResults는 bounded channel로 실행 수명주기와 row event를 전달한다.
	StreamResults(ctx context.Context, sessionHandle, statement string, options StreamOptions) (<-chan ResultEvent, <-chan error)
}

// SessionSetupExecutor는 기존 Client 구현자의 호환성을 깨지 않고 선언형 session setup을
// 적용하는 별도 공개 계약이다.
type SessionSetupExecutor interface {
	// ApplySessionSetup은 기존 session에 검증된 catalog, database, table과 현재 scope를 적용한다.
	ApplySessionSetup(ctx context.Context, sessionHandle string, plan SessionSetupPlan, options SessionSetupOptions) (*SessionSetupResult, error)
}

// GatewayClient는 여러 goroutine에서 안전하게 사용할 수 있다. Session 상태를 바꾸는
// SQL의 실행 순서를 보장하는 책임은 호출자에게 있다.
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
	executions        *activityGroup
	observerSlots     chan struct{}

	versionMu      sync.Mutex
	versionChecked bool
	versionCall    *compatibilityCheckCall
	versionErr     error
	compatibility  CompatibilityInfo

	stateMu      sync.Mutex
	sessions     map[string]*sessionRecord
	closed       map[string]struct{}
	closedOrder  []string
	closeCalls   map[string]*sessionCloseCall
	heartbeats   map[string]*HeartbeatRunner
	clientClosed bool
	managed      map[*managedSession]struct{}
	streams      map[*resultStream]struct{}
	setupGates   map[string]*sessionSetupGate
}

// sessionRecord는 공개 Session과 memory를 공유하지 않는 client 내부 session 상태이다.
type sessionRecord struct {
	handle     string
	name       string
	properties map[string]string
	createdAt  time.Time
}

// compatibilityCheckCall은 동시에 시작된 compatibility 검사 호출자가 하나의 일시적
// 성공 또는 실패 결과를 공유하게 한다. 완료 뒤 시작된 호출만 새 감지를 시도한다.
type compatibilityCheckCall struct {
	done            chan struct{}
	err             error
	retryForWaiters bool
}

// snapshot은 호출자가 자유롭게 변경할 수 있는 공개 Session 복사본을 만든다.
func (s *sessionRecord) snapshot() *Session {
	if s == nil {
		return nil
	}
	return &Session{
		Handle:     s.handle,
		Name:       s.name,
		Properties: cloneMap(s.properties),
		CreatedAt:  s.createdAt,
	}
}

// 컴파일 시 GatewayClient가 공개 Client 계약을 모두 구현하는지 확인한다.
var _ Client = (*GatewayClient)(nil)

// 컴파일 시 GatewayClient가 기존 Client와 독립적인 setup 계약을 구현하는지 확인한다.
var _ SessionSetupExecutor = (*GatewayClient)(nil)

// 컴파일 시 GatewayClient가 기존 Client와 분리된 compatibility 조회 계약을 구현하는지 확인한다.
var _ CompatibilityProvider = (*GatewayClient)(nil)

// 컴파일 시 GatewayClient가 기존 Client와 분리된 Materialized Table refresh 계약을 구현하는지 확인한다.
var _ MaterializedTableRefresher = (*GatewayClient)(nil)

// 컴파일 시 GatewayClient가 기존 Client와 분리된 Script 배포 계약을 구현하는지 확인한다.
var _ ScriptDeployer = (*GatewayClient)(nil)

// NewClient는 설정을 검증하고 재사용 가능한 client를 생성한다. 생성 중에는 네트워크를
// 호출하지 않으며 첫 versioned 요청에서 APIVersion을 검증한다.
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
		executions:        newActivityGroup(ErrClientClosed),
		observerSlots:     make(chan struct{}, normalized.ObserverMaxInFlight),
		sessions:          make(map[string]*sessionRecord),
		closed:            make(map[string]struct{}),
		closeCalls:        make(map[string]*sessionCloseCall),
		heartbeats:        make(map[string]*HeartbeatRunner),
		managed:           make(map[*managedSession]struct{}),
		streams:           make(map[*resultStream]struct{}),
		setupGates:        make(map[string]*sessionSetupGate),
	}, nil
}

// CheckAPIVersion은 lazy compatibility 감지와 REST API 선택을 수행한다. 성공 결과와
// 결정적인 미지원 결과는 저장하고 transport 또는 context 실패는 다음 호출에서 재시도한다.
func (c *GatewayClient) CheckAPIVersion(ctx context.Context) error {
	if _, known, err := c.configuredCompatibility(); known && err != nil {
		c.versionMu.Lock()
		if !c.versionChecked {
			c.versionChecked = true
			c.versionErr = err
		}
		cachedErr := c.versionErr
		c.versionMu.Unlock()
		return cachedErr
	}

	for {
		c.versionMu.Lock()
		if c.versionChecked {
			err := c.versionErr
			c.versionMu.Unlock()
			return err
		}
		if call := c.versionCall; call != nil {
			c.versionMu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-call.done:
				if call.retryForWaiters {
					continue
				}
				return call.err
			}
		}
		call := &compatibilityCheckCall{done: make(chan struct{})}
		c.versionCall = call
		c.versionMu.Unlock()

		compatibility, err := c.resolveCompatibility(ctx)
		ownerContextErr := ctx.Err()
		c.versionMu.Lock()
		if err != nil {
			if !errors.Is(err, ErrCompatibilityDetection) {
				c.versionChecked = true
				c.versionErr = err
			}
		} else {
			c.compatibility = compatibility
			c.versionChecked = true
		}
		call.err = err
		call.retryForWaiters = ownerContextErr != nil && errors.Is(err, ownerContextErr)
		c.versionCall = nil
		close(call.done)
		c.versionMu.Unlock()
		return err
	}
}

// sessionContext는 정책 검증에 전달할 session 정보를 복사해 내부 상태 변경을 차단한다.
func (c *GatewayClient) sessionContext(handle string) SessionContext {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	context := SessionContext{Handle: handle}
	if session := c.sessions[handle]; session != nil {
		context.Name = session.name
		context.Properties = cloneMap(session.properties)
	}
	return context
}

// cloneMap은 호출자와 내부 상태가 같은 map을 공유하지 않도록 문자열 map을 복사한다.
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
