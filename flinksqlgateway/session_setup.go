package flinksqlgateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// SessionSetupStepKind는 선언형 setup의 실행 단계 종류를 나타낸다.
type SessionSetupStepKind string

const (
	// SessionSetupCatalog는 완전 수식 scope의 CREATE CATALOG 단계이다.
	SessionSetupCatalog SessionSetupStepKind = "CATALOG"
	// SessionSetupDatabase는 완전 수식 scope의 CREATE DATABASE 단계이다.
	SessionSetupDatabase SessionSetupStepKind = "DATABASE"
	// SessionSetupTable은 완전 수식 target의 CREATE TABLE 단계이다.
	SessionSetupTable SessionSetupStepKind = "TABLE"
	// SessionSetupUseCatalog는 현재 catalog를 변경하는 단계이다.
	SessionSetupUseCatalog SessionSetupStepKind = "USE_CATALOG"
	// SessionSetupUseDatabase는 현재 database를 변경하는 단계이다.
	SessionSetupUseDatabase SessionSetupStepKind = "USE_DATABASE"
)

// SessionSetupPlan은 catalog, database, table과 setup 완료 후 현재 scope를 선언한다.
type SessionSetupPlan struct {
	// Catalogs는 먼저 생성할 catalog 정의이다.
	Catalogs []CatalogSetup
	// Databases는 catalog 이름을 포함해 생성할 database 정의이다.
	Databases []DatabaseSetup
	// Tables는 완전 수식 target에 생성할 table 정의이다.
	Tables []TableSetup
	// CurrentCatalog는 모든 CREATE 단계 후 USE CATALOG로 선택할 catalog이다.
	CurrentCatalog string
	// CurrentDatabase는 CurrentCatalog 선택 후 USE로 선택할 database이다.
	CurrentDatabase string
}

// CatalogSetup은 CREATE CATALOG에 필요한 이름과 결정적인 option을 정의한다.
type CatalogSetup struct {
	// Name은 생성할 catalog 이름이다.
	Name string
	// IfNotExists는 기존 catalog가 있을 때 오류 없이 유지하도록 요청한다.
	IfNotExists bool
	// Options는 CREATE CATALOG WITH의 문자열 option이며 compile 시 복사해 SQL로 만든다.
	Options map[string]string
	// SensitiveKeys는 기본 password/secret/token 계열 외에 민감하게 취급할 option key이다.
	SensitiveKeys []string
}

// DatabaseSetup은 완전 수식 CREATE DATABASE를 정의한다.
type DatabaseSetup struct {
	// Catalog는 database를 생성할 catalog 이름이며 생략할 수 없다.
	Catalog string
	// Name은 생성할 database 이름이다.
	Name string
	// IfNotExists는 기존 database가 있을 때 오류 없이 유지하도록 요청한다.
	IfNotExists bool
	// Options는 선택적인 CREATE DATABASE WITH 문자열 option이다.
	Options map[string]string
}

// TableSetup은 library가 완전 수식 CREATE TABLE을 생성할 target과 정의 부분을 제공한다.
type TableSetup struct {
	// Target은 Catalog, Database와 Object가 모두 채워진 table 식별자이다.
	Target Identifier
	// Statement는 완전 수식 table 이름 뒤에 붙는 정의 부분이다. 완전한 CREATE TABLE SQL은
	// 허용하지 않으며 예시는 "(id BIGINT) WITH ('connector' = 'datagen')"이다.
	Statement string
	// IfNotExists는 기존 table이 있을 때 오류 없이 유지하도록 요청한다.
	IfNotExists bool
	// Verify는 전체 metadata 검증 설정과 별개로 이 table을 검증한다.
	Verify bool
	// Sensitive는 Statement 안의 값을 안전하게 구조화할 수 없을 때 server 오류 message
	// 전체를 치환하도록 요청한다.
	Sensitive bool
}

// SessionSetupStep은 compile된 공개 실행 순서와 안전한 target을 나타낸다. 실제 SQL과
// secret은 외부에 노출하지 않는다.
type SessionSetupStep struct {
	// Index는 전체 plan에서 0부터 시작하는 실행 순서이다.
	Index int
	// Kind는 CREATE 또는 USE 단계 종류이다.
	Kind SessionSetupStepKind
	// Target은 오류와 검증에 사용할 안전한 식별자이다.
	Target Identifier
}

// compiledSessionSetupStep은 공개 step과 분리해 SQL과 secret을 Apply 호출 stack 안에만 둔다.
type compiledSessionSetupStep struct {
	SessionSetupStep
	statement       string
	verify          bool
	sensitiveValues []string
	redactAll       bool
}

// SessionSetupOptions는 setup 실행 제한, metadata 검증과 생성 session cleanup 정책을 정의한다.
type SessionSetupOptions struct {
	// ExecutionTimeout은 각 configure-session POST의 client-side 제한이다. 0이면 Config 값을 사용한다.
	ExecutionTimeout time.Duration
	// VerifyMetadata는 catalog, database와 table 생성 결과를 read-only metadata로 검증한다.
	VerifyMetadata bool
	// VerifyTableSchema는 검증 대상 table에 DescribeTable까지 수행한다.
	VerifyTableSchema bool
	// KeepSessionOnFailure는 OpenSessionWithSetup 실패 시 생성한 session을 유지한다.
	KeepSessionOnFailure bool
	// CleanupTimeout은 실패 session 종료에 사용하는 background context 제한이다.
	CleanupTimeout time.Duration
}

// SessionSetupStepResult는 DDL 적용, metadata 검증과 결과 불명확 상태를 분리해 보고한다.
type SessionSetupStepResult struct {
	// Index는 compile된 step 순서이다.
	Index int
	// Kind는 실행한 setup 단계 종류이다.
	Kind SessionSetupStepKind
	// Target은 SQL과 option을 제외한 안전한 대상 식별자이다.
	Target Identifier
	// Applied는 configure-session 성공 응답을 확인했음을 나타낸다.
	Applied bool
	// Verified는 요청된 metadata 검증이 성공했음을 나타낸다.
	Verified bool
	// OutcomeUnknown은 POST 처리 결과를 안전하게 판단할 수 없음을 나타낸다.
	OutcomeUnknown bool
}

// SessionSetupResult는 부분 성공과 session cleanup 결과를 보존한다.
type SessionSetupResult struct {
	// SessionHandle은 setup 대상 session의 불투명 handle이다.
	SessionHandle string
	// Steps는 SQL과 option을 포함하지 않는 단계별 결과이다.
	Steps []SessionSetupStepResult
	// FailedIndex는 실패 또는 검증 실패 step이며 성공 시 -1이다.
	FailedIndex int
	// Complete는 모든 configure 단계와 요청된 metadata 검증이 끝났음을 나타낸다.
	Complete bool
	// SessionClosed는 OpenSessionWithSetup 실패 cleanup에서 session 종료가 성공했음을 나타낸다.
	SessionClosed bool
	// PersistentChangesMayRemain은 자동 rollback하지 않은 catalog 객체가 남을 수 있음을 나타낸다.
	PersistentChangesMayRemain bool
}

// sessionSetupGate는 같은 session의 ConfigureSession과 setup plan이 서로 끼어들지 않게 한다.
type sessionSetupGate struct {
	token chan struct{}
	refs  int
}

// setupVerificationError는 DDL 성공과 metadata 검증 실패를 구분하면서 원인 chain을 보존한다.
type setupVerificationError struct {
	cause  error
	kind   SessionSetupStepKind
	target Identifier
}

func (e *setupVerificationError) Error() string {
	return fmt.Sprintf("%v: kind=%s target=%s", ErrSessionSetupVerification, e.kind, formatSessionSetupTarget(e.target))
}

func (e *setupVerificationError) Unwrap() []error {
	if e.cause == nil {
		return []error{ErrSessionSetupVerification}
	}
	return []error{ErrSessionSetupVerification, e.cause}
}

// ApplySessionSetup은 기존 session에 compile된 step을 configure-session으로 순서대로 적용한다.
// configure POST와 결과 불명확 step은 자동 재시도하지 않는다.
func (c *GatewayClient) ApplySessionSetup(ctx context.Context, sessionHandle string, plan SessionSetupPlan, options SessionSetupOptions) (*SessionSetupResult, error) {
	steps, err := compileSessionSetupPlan(plan)
	if err != nil {
		return nil, err
	}
	if err := validateSessionSetupOptions(options); err != nil {
		return nil, err
	}
	if err := validateSessionHandle(sessionHandle); err != nil {
		return nil, err
	}
	if !capabilitiesForVersion(c.cfg.APIVersion).ConfigureSession {
		return nil, fmt.Errorf("%w: session setup requires configure-session v2 or newer", ErrUnsupportedAPI)
	}
	if err := c.CheckAPIVersion(ctx); err != nil {
		return nil, err
	}
	return c.applyCompiledSessionSetup(ctx, sessionHandle, steps, options)
}

// OpenSessionWithSetup은 plan 전체를 먼저 compile한 뒤 session을 열고 setup을 적용한다.
// 실패 시 기본적으로 제한된 background context로 session을 닫지만 객체 DROP rollback은 하지 않는다.
func (c *GatewayClient) OpenSessionWithSetup(ctx context.Context, request OpenSessionRequest, plan SessionSetupPlan, options SessionSetupOptions) (*SessionSetupResult, error) {
	steps, err := compileSessionSetupPlan(plan)
	if err != nil {
		return nil, err
	}
	if err := validateSessionSetupOptions(options); err != nil {
		return nil, err
	}
	if !capabilitiesForVersion(c.cfg.APIVersion).ConfigureSession {
		return nil, fmt.Errorf("%w: session setup requires configure-session v2 or newer", ErrUnsupportedAPI)
	}
	session, err := c.OpenSession(ctx, request)
	if err != nil {
		return nil, err
	}
	result, setupErr := c.applyCompiledSessionSetup(ctx, session.Handle, steps, options)
	if setupErr == nil {
		return result, nil
	}
	if options.KeepSessionOnFailure {
		return result, setupErr
	}

	cleanupTimeout := options.CleanupTimeout
	if cleanupTimeout <= 0 {
		cleanupTimeout = c.cfg.RequestTimeout
	}
	if cleanupTimeout <= 0 {
		cleanupTimeout = defaultRequestTimeout
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	closeErr := c.CloseSession(withInternalCleanup(cleanupCtx), session.Handle)
	cancel()
	result.SessionClosed = closeErr == nil
	return result, attachSessionSetupCleanup(setupErr, closeErr, session.Handle)
}

// applyCompiledSessionSetup은 같은 session의 설정을 직렬화하고 create/use 뒤에 검증을 수행한다.
func (c *GatewayClient) applyCompiledSessionSetup(ctx context.Context, sessionHandle string, steps []compiledSessionSetupStep, options SessionSetupOptions) (*SessionSetupResult, error) {
	result := newSessionSetupResult(sessionHandle, steps)
	release, err := c.acquireSessionSetup(ctx, sessionHandle)
	if err != nil {
		return result, &SessionSetupError{Cause: err, SessionHandle: sessionHandle, FailedIndex: -1}
	}
	defer release()

	for index := range steps {
		step := &steps[index]
		c.observeLifecycle(ctx, Observation{
			Event:          ObservationSessionSetupStepApplying,
			SessionHandle:  sessionHandle,
			SetupStepIndex: step.Index,
			SetupStepKind:  step.Kind,
			SetupTarget:    step.Target,
		})
		if err := c.validateStatement(ctx, sessionHandle, step.statement); err != nil {
			return c.failSessionSetup(ctx, result, *step, err)
		}
		redaction := serverMessageRedaction{
			fragments: append([]string{step.statement}, step.sensitiveValues...),
			redactAll: step.redactAll || len(step.sensitiveValues) > 0,
		}
		err := c.configureSessionRequest(ctx, sessionHandle, step.statement, options.ExecutionTimeout, redaction)
		if err != nil {
			var unknown *ConfigurationOutcomeUnknownError
			if errors.As(err, &unknown) {
				err = &ConfigurationOutcomeUnknownError{
					SessionHandle: sessionHandle,
					StepIndex:     step.Index,
					StepKind:      step.Kind,
					RequestPhase:  unknown.RequestPhase,
					Cause:         unknown.Cause,
				}
				result.Steps[step.Index].OutcomeUnknown = true
			}
			return c.failSessionSetup(ctx, result, *step, err)
		}
		result.Steps[step.Index].Applied = true
		c.observeLifecycle(ctx, Observation{
			Event:          ObservationSessionSetupStepApplied,
			SessionHandle:  sessionHandle,
			SetupStepIndex: step.Index,
			SetupStepKind:  step.Kind,
			SetupTarget:    step.Target,
		})
	}

	for index := range steps {
		step := &steps[index]
		if !sessionSetupStepRequiresVerification(*step, options) {
			continue
		}
		if err := c.verifySessionSetupStep(ctx, sessionHandle, *step, options); err != nil {
			verificationErr := &setupVerificationError{cause: err, kind: step.Kind, target: step.Target}
			return c.failSessionSetup(ctx, result, *step, verificationErr)
		}
		result.Steps[step.Index].Verified = true
		c.observeLifecycle(ctx, Observation{
			Event:          ObservationSessionSetupStepVerified,
			SessionHandle:  sessionHandle,
			SetupStepIndex: step.Index,
			SetupStepKind:  step.Kind,
			SetupTarget:    step.Target,
		})
	}
	result.Complete = true
	return result, nil
}

// newSessionSetupResult는 SQL과 option을 제외한 결과 slot을 compile 순서대로 만든다.
func newSessionSetupResult(sessionHandle string, steps []compiledSessionSetupStep) *SessionSetupResult {
	result := &SessionSetupResult{
		SessionHandle: sessionHandle,
		Steps:         make([]SessionSetupStepResult, len(steps)),
		FailedIndex:   -1,
	}
	for index, step := range steps {
		result.Steps[index] = SessionSetupStepResult{Index: step.Index, Kind: step.Kind, Target: step.Target}
	}
	return result
}

// failSessionSetup은 부분 적용과 outcome unknown을 기록하고 SQL 없는 typed error를 만든다.
func (c *GatewayClient) failSessionSetup(ctx context.Context, result *SessionSetupResult, step compiledSessionSetupStep, cause error) (*SessionSetupResult, error) {
	result.FailedIndex = step.Index
	result.PersistentChangesMayRemain = sessionSetupPersistentChangesMayRemain(result, step)
	setupErr := &SessionSetupError{
		Cause:          cause,
		SessionHandle:  result.SessionHandle,
		FailedIndex:    step.Index,
		StepKind:       step.Kind,
		Target:         step.Target,
		OutcomeUnknown: result.Steps[step.Index].OutcomeUnknown,
	}
	event := ObservationSessionSetupStepFailed
	if setupErr.OutcomeUnknown {
		event = ObservationSessionSetupStepOutcomeUnknown
	}
	c.observeLifecycle(ctx, Observation{
		Event:          event,
		SessionHandle:  result.SessionHandle,
		SetupStepIndex: step.Index,
		SetupStepKind:  step.Kind,
		SetupTarget:    step.Target,
		Error:          setupErr,
	})
	return result, setupErr
}

// sessionSetupPersistentChangesMayRemain은 CREATE 성공 또는 CREATE 결과 불명확이 하나라도
// 있으면 session 종료와 무관하게 영속 객체가 남을 수 있음을 보수적으로 표시한다.
func sessionSetupPersistentChangesMayRemain(result *SessionSetupResult, failed compiledSessionSetupStep) bool {
	for _, step := range result.Steps {
		if !isSessionSetupCreateKind(step.Kind) {
			continue
		}
		if step.Applied || step.OutcomeUnknown {
			return true
		}
	}
	return isSessionSetupCreateKind(failed.Kind) && result.Steps[failed.Index].OutcomeUnknown
}

// isSessionSetupCreateKind는 session 밖에 metadata가 남을 수 있는 CREATE 단계를 구분한다.
func isSessionSetupCreateKind(kind SessionSetupStepKind) bool {
	return kind == SessionSetupCatalog || kind == SessionSetupDatabase || kind == SessionSetupTable
}

// attachSessionSetupCleanup은 기존 setup 오류를 복사해 cleanup 오류를 원인과 함께 보존한다.
func attachSessionSetupCleanup(setupErr, closeErr error, sessionHandle string) error {
	var typed *SessionSetupError
	if errors.As(setupErr, &typed) {
		return &SessionSetupError{
			Cause:          typed.Cause,
			CloseError:     closeErr,
			SessionHandle:  typed.SessionHandle,
			FailedIndex:    typed.FailedIndex,
			StepKind:       typed.StepKind,
			Target:         typed.Target,
			OutcomeUnknown: typed.OutcomeUnknown,
		}
	}
	return &SessionSetupError{Cause: setupErr, CloseError: closeErr, SessionHandle: sessionHandle, FailedIndex: -1}
}

// validateSessionSetupOptions은 network나 session 생성 전에 duration 조합을 검증한다.
func validateSessionSetupOptions(options SessionSetupOptions) error {
	if options.ExecutionTimeout < 0 {
		return fmt.Errorf("flinksqlgateway: SessionSetupOptions.ExecutionTimeout must not be negative")
	}
	if options.CleanupTimeout < 0 {
		return fmt.Errorf("flinksqlgateway: SessionSetupOptions.CleanupTimeout must not be negative")
	}
	return nil
}

// sessionSetupStepRequiresVerification은 create step만 global 또는 table별 검증 대상으로 삼는다.
func sessionSetupStepRequiresVerification(step compiledSessionSetupStep, options SessionSetupOptions) bool {
	switch step.Kind {
	case SessionSetupCatalog, SessionSetupDatabase:
		return options.VerifyMetadata
	case SessionSetupTable:
		return options.VerifyMetadata || options.VerifyTableSchema || step.verify
	default:
		return false
	}
}

// verifySessionSetupStep은 Flink가 반환한 이름의 대소문자를 바꾸지 않고 exact match한다.
func (c *GatewayClient) verifySessionSetupStep(ctx context.Context, sessionHandle string, step compiledSessionSetupStep, options SessionSetupOptions) error {
	switch step.Kind {
	case SessionSetupCatalog:
		catalogs, err := c.ListCatalogs(ctx, sessionHandle)
		if err != nil {
			return err
		}
		if !containsExactName(catalogs, step.Target.Catalog) {
			return fmt.Errorf("catalog was not found")
		}
	case SessionSetupDatabase:
		databases, err := c.ListDatabases(ctx, sessionHandle, step.Target.Catalog)
		if err != nil {
			return err
		}
		if !containsExactName(databases, step.Target.Database) {
			return fmt.Errorf("database was not found")
		}
	case SessionSetupTable:
		scope := Identifier{Catalog: step.Target.Catalog, Database: step.Target.Database}
		tables, err := c.ListTables(ctx, sessionHandle, scope)
		if err != nil {
			return err
		}
		views, err := c.ListViews(ctx, sessionHandle, scope)
		if err != nil {
			return err
		}
		tableFound := containsTableMetadata(tables, step.Target.Object)
		viewFound := containsTableMetadata(views, step.Target.Object)
		if viewFound {
			return fmt.Errorf("target name resolves to a view")
		}
		if !tableFound {
			return fmt.Errorf("table was not found")
		}
		if options.VerifyTableSchema {
			if _, err := c.DescribeTable(ctx, sessionHandle, step.Target); err != nil {
				return err
			}
		}
	}
	return nil
}

// containsExactName은 catalog 구현의 case 정책을 추측하지 않고 byte 단위 이름을 비교한다.
func containsExactName(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

// containsTableMetadata는 helper가 반환한 이름을 case 변환 없이 확인한다.
func containsTableMetadata(values []TableMetadata, expected string) bool {
	for _, value := range values {
		if value.Name == expected {
			return true
		}
	}
	return false
}

// acquireSessionSetup은 같은 session의 configure 흐름에 context-aware gate를 제공한다.
func (c *GatewayClient) acquireSessionSetup(ctx context.Context, sessionHandle string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.stateMu.Lock()
	if c.clientClosed {
		c.stateMu.Unlock()
		return nil, ErrClientClosed
	}
	if c.setupGates == nil {
		c.setupGates = make(map[string]*sessionSetupGate)
	}
	gate := c.setupGates[sessionHandle]
	if gate == nil {
		gate = &sessionSetupGate{token: make(chan struct{}, 1)}
		gate.token <- struct{}{}
		c.setupGates[sessionHandle] = gate
	}
	gate.refs++
	c.stateMu.Unlock()

	select {
	case <-ctx.Done():
		c.releaseSessionSetupGateRef(sessionHandle, gate)
		return nil, ctx.Err()
	case <-c.lifecycleCtx.Done():
		c.releaseSessionSetupGateRef(sessionHandle, gate)
		return nil, ErrClientClosed
	case <-gate.token:
	}
	if err := ctx.Err(); err != nil {
		gate.token <- struct{}{}
		c.releaseSessionSetupGateRef(sessionHandle, gate)
		return nil, err
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			gate.token <- struct{}{}
			c.releaseSessionSetupGateRef(sessionHandle, gate)
		})
	}, nil
}

// releaseSessionSetupGateRef은 마지막 실행자와 대기자가 떠나면 handle별 gate를 제거한다.
func (c *GatewayClient) releaseSessionSetupGateRef(sessionHandle string, gate *sessionSetupGate) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	gate.refs--
	if gate.refs == 0 && c.setupGates[sessionHandle] == gate {
		delete(c.setupGates, sessionHandle)
	}
}
