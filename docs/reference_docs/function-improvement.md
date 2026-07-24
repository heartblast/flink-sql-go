# flink-sql-go 후속 개선 요구사항

## 1. 목적

현재 `flink-sql-go`는 Apache Flink 1.20.4 SQL Gateway의 세션, SQL 실행, Operation 관리, 결과 조회, 스트리밍 처리 기능을 제공한다.

후속 개선의 목적은 단순 REST API 래퍼를 넘어 다음 용도로 안정적으로 사용할 수 있는 Go SDK로 발전시키는 것이다.

* Flink SQL 워크스페이스
* 웹 기반 Flink SQL Client
* 장시간 유지되는 사용자 세션
* SQL 실행 이력 및 Job 추적
* 스트리밍 조회 결과 처리
* 운영 환경의 장애 및 보안 대응

## 2. 기본 원칙

1. Apache Flink 1.20.4 SQL Gateway를 기준으로 구현한다.
2. 기존 저수준 REST Client API와 하위 호환성을 유지한다.
3. JDBC, JVM, GraalVM, CGO, `database/sql`은 사용하지 않는다.
4. 자동 복구나 자동 재시도로 SQL이 중복 실행되지 않도록 한다.
5. 세션, SQL Operation, Flink Job의 생명주기를 서로 구분한다.
6. 기본 Client에는 제품별 사용자 권한 정책을 하드코딩하지 않는다.
7. 고수준 편의 기능은 기존 저수준 API 위에 별도 계층으로 구현한다.
8. 모든 장시간 실행 기능은 `context.Context` 취소와 자원 정리를 지원해야 한다.

---

# 3. 우선 적용 요구사항

## 3.1 SQL 실행 결과 불명확 오류 처리

### 요구사항

SQL 실행 요청을 Gateway에 전송한 뒤 응답을 받기 전에 연결이 끊어진 경우, 일반적인 네트워크 오류와 구분할 수 있어야 한다.

다음 상황을 처리한다.

```text
SQL 실행 POST 전송
→ Gateway가 Operation 또는 Job 생성
→ 응답 수신 전 연결 종료
→ 클라이언트가 Operation Handle을 받지 못함
```

이 상태에서는 SQL이 실행되었는지 확인할 수 없으므로 자동 재실행해서는 안 된다.

### 구현 요구사항

다음과 같은 별도 오류를 제공한다.

```go
var ErrExecutionOutcomeUnknown = errors.New(
    "statement execution outcome is unknown",
)
```

필요하면 typed error를 제공한다.

```go
type ExecutionOutcomeUnknownError struct {
    SessionHandle string
    Method        string
    Endpoint      string
    RequestPhase  RequestPhase
    Cause         error
}
```

요청 상태는 최소한 다음을 구분한다.

```go
type RequestPhase string

const (
    RequestNotSent        RequestPhase = "NOT_SENT"
    RequestPossiblySent   RequestPhase = "POSSIBLY_SENT"
    ResponseHeaderMissing RequestPhase = "RESPONSE_HEADER_MISSING"
)
```

### 처리 정책

* SQL 실행 결과 불명확 오류 발생 시 자동 재시도하지 않는다.
* 상위 서비스가 사용자에게 실행 상태 확인을 안내할 수 있어야 한다.
* 오류에 전체 SQL이나 인증정보를 포함하지 않는다.
* Observer 또는 감사 로그에 실행 결과가 불명확하다는 상태를 남길 수 있어야 한다.

### 완료 기준

* 응답 전 연결 종료 상황에 대한 단위 테스트가 존재한다.
* 상위 서비스에서 `errors.Is` 또는 `errors.As`로 오류를 판별할 수 있다.
* 해당 오류가 일반 timeout이나 단순 연결 실패와 구분된다.

---

## 3.2 Managed Session 기능

### 요구사항

세션 생성, heartbeat, 상태 관리, 종료를 하나의 관리 객체로 제공한다.

현재처럼 호출자가 직접 `StartHeartbeat`와 `CloseSession`을 각각 관리하는 방식도 유지하되, 장시간 세션을 위한 고수준 API를 추가한다.

### 공개 API 예시

```go
type ManagedSession interface {
    Handle() string

    Execute(
        ctx context.Context,
        statement string,
        options ExecuteOptions,
    ) (*ExecutionResult, error)

    Stream(
        ctx context.Context,
        statement string,
        options StreamOptions,
    ) (ResultStream, error)

    Health() SessionHealth

    Close(ctx context.Context) error
}
```

```go
type ManagedSessionOptions struct {
    HeartbeatInterval time.Duration
    FailureThreshold  int
    CleanupTimeout    time.Duration
    Serialize         bool
}
```

### 세션 상태

```go
type SessionHealth string

const (
    SessionHealthy  SessionHealth = "HEALTHY"
    SessionDegraded SessionHealth = "DEGRADED"
    SessionExpired  SessionHealth = "EXPIRED"
    SessionClosed   SessionHealth = "CLOSED"
)
```

### 구현 요구사항

* heartbeat는 SQL 실행 context와 별도의 lifecycle context를 사용한다.
* 동일 세션에 heartbeat runner를 중복 생성하지 않는다.
* heartbeat 간격에 선택적으로 jitter를 적용한다.
* 연속 heartbeat 실패 횟수를 관리한다.
* 설정된 실패 임계치를 초과하면 `DEGRADED` 또는 `EXPIRED` 상태로 전환한다.
* 세션 종료 시 heartbeat를 먼저 중단한다.
* `Close`는 중복 호출해도 안전해야 한다.
* 세션 만료 시 자동으로 새 세션을 생성하지 않는다.
* Managed Session의 생성과 종료 시 Observer 이벤트를 제공한다.

### 완료 기준

* heartbeat와 `Close` 간 race condition 테스트가 존재한다.
* `go test -race ./...`를 통과한다.
* Managed Session 종료 후 goroutine이 남지 않는다.

---

## 3.3 동일 세션 직렬 실행 기능

### 요구사항

`USE`, `SET`, Temporary Object 생성과 같이 순서가 중요한 SQL을 동일 세션에서 직렬로 실행할 수 있는 선택적 기능을 제공한다.

### 적용 대상 예시

```sql
SET 'execution.runtime-mode' = 'streaming';

USE CATALOG kafka_catalog;

USE kafka_database;

CREATE TEMPORARY TABLE source_table (...);

SELECT * FROM source_table;
```

### 공개 API 예시

```go
serialized := flinksqlgateway.NewSerializedSession(
    client,
    sessionHandle,
)
```

또는 Managed Session 옵션으로 제공한다.

```go
ManagedSessionOptions{
    Serialize: true,
}
```

### 구현 요구사항

* 기본 `GatewayClient`의 병렬 호출 기능은 유지한다.
* 직렬 실행은 특정 세션 단위로만 적용한다.
* 한 세션의 대기열이 다른 세션 실행을 차단하지 않도록 한다.
* 대기 중 context가 취소되면 실행 대기열에서 제거한다.
* 실행 중 Operation 취소와 대기열 취소를 구분한다.
* 세션 종료 후 신규 SQL 실행을 거부한다.

### 완료 기준

* 동일 세션에서 SQL 실행 순서가 보장된다.
* 서로 다른 세션은 병렬 실행할 수 있다.
* 대기 context 취소 시 goroutine과 lock이 남지 않는다.

---

# 4. 세션 복원 기능

## 4.1 Session Recipe

### 요구사항

세션 만료 후 호출자가 명시적으로 세션 상태를 재구성할 수 있도록 `SessionRecipe` 기능을 제공한다.

자동 복구는 수행하지 않는다.

### 데이터 구조 예시

```go
type SessionRecipe struct {
    Name            string
    Properties      map[string]string
    SetupStatements []string
}
```

### 사용 예시

```go
recipe := SessionRecipe{
    Name: "workspace-user-123",
    Properties: map[string]string{
        "execution.runtime-mode": "streaming",
    },
    SetupStatements: []string{
        "USE CATALOG kafka_catalog",
        "USE kafka_database",
    },
}

result, err := client.OpenSessionFromRecipe(ctx, recipe)
```

### 처리 요구사항

* 먼저 세션을 생성하고 Properties를 적용한다.
* Setup Statement는 입력 순서대로 실행한다.
* 중간 실패 시 이후 문장을 실행하지 않는다.
* 어느 문장까지 성공했는지 반환한다.
* 실패 시 생성된 세션을 유지할지 종료할지 옵션으로 제공한다.
* Recipe에 SQL 비밀번호나 민감정보가 포함될 가능성을 고려해 로그에 원문을 남기지 않는다.

### 결과 구조 예시

```go
type RecipeReplayResult struct {
    SessionHandle string
    Applied       []StatementResult
    FailedIndex   int
    Complete      bool
}
```

### 완료 기준

* Recipe 전체 성공, 중간 실패, context 취소 테스트가 존재한다.
* 자동 세션 복원은 수행하지 않는다.
* 호출자가 명시적으로 복원 결과를 확인할 수 있다.

---

# 5. 결과 처리 개선

## 5.1 Result Iterator API

### 요구사항

기존 channel 기반 `StreamResults`를 유지하면서, 사용하기 쉬운 iterator 기반 결과 조회 API를 추가한다.

### 공개 API 예시

```go
type ResultStream interface {
    Next() bool
    Event() ResultEvent
    Row() Row
    Err() error
    JobID() string
    Close() error
}
```

### 사용 예시

```go
stream, err := client.ExecuteStream(
    ctx,
    sessionHandle,
    statement,
    options,
)
if err != nil {
    return err
}
defer stream.Close()

for stream.Next() {
    row := stream.Row()
    _ = row
}

if err := stream.Err(); err != nil {
    return err
}
```

### 구현 요구사항

* `Next` 호출 시 필요한 결과 페이지를 순차 조회한다.
* `Close` 호출 시 필요하면 Operation 취소와 종료를 수행한다.
* `Close`는 중복 호출해도 안전해야 한다.
* `Next` 종료 후 `Err`에서 최종 오류를 확인할 수 있어야 한다.
* context 취소 시 polling을 즉시 중단한다.
* 행 수, polling 횟수, 실행 시간 제한을 기존 정책과 동일하게 적용한다.
* 전체 결과를 메모리에 적재하지 않는다.

### 완료 기준

* 정상 EOS, 사용자 취소, 결과 제한, 네트워크 오류 테스트가 존재한다.
* 소비자가 중간에 조회를 중단해도 자원이 정리된다.
* iterator 사용 시 별도 goroutine이 필요하지 않거나, 사용한다면 누수가 없어야 한다.

---

## 5.2 Flink 타입 변환 Helper

### 요구사항

기본 결과는 현재와 같이 `json.RawMessage`를 유지하되, 호출자가 명시적으로 타입 변환을 요청할 수 있는 Helper를 제공한다.

### 지원 대상

* BOOLEAN
* TINYINT, SMALLINT, INTEGER, BIGINT
* FLOAT, DOUBLE
* DECIMAL
* STRING
* DATE
* TIME
* TIMESTAMP
* TIMESTAMP_LTZ
* BINARY, VARBINARY
* ARRAY
* MAP
* ROW

### 공개 API 예시

```go
type ValueDecoder interface {
    Decode(
        column Column,
        raw json.RawMessage,
    ) (any, error)
}
```

Row helper도 제공할 수 있다.

```go
row.String("service_name")
row.Int64("status_code")
row.Bool("enabled")
row.Decimal("amount")
row.Time("event_time")
row.Raw("payload")
```

### 처리 원칙

* 기본 결과 구조는 `json.RawMessage`를 유지한다.
* 정밀도 손실이 가능한 DECIMAL은 기본적으로 문자열 또는 전용 타입으로 변환한다.
* TIMESTAMP와 TIMESTAMP_LTZ의 의미를 구분한다.
* 알 수 없는 타입은 오류로 전체 행 파싱을 중단하지 않고 Raw 값을 반환할 수 있어야 한다.
* 컬럼 인덱스와 컬럼명 기반 접근을 모두 지원한다.
* 중복 컬럼명에 대한 처리 정책을 문서화한다.
* NULL 값을 명시적으로 구분한다.

### 완료 기준

* 주요 Flink 논리 타입별 단위 테스트가 존재한다.
* 큰 DECIMAL 값에서 정밀도 손실이 발생하지 않는다.
* 알 수 없는 타입 추가 시 기존 결과 파싱이 실패하지 않는다.

---

## 5.3 Changelog Materializer

### 요구사항

Flink 결과의 RowKind를 현재 상태의 테이블 형태로 변환할 수 있는 선택적 유틸리티를 제공한다.

이 기능은 REST Client 핵심 패키지와 분리한다.

```text
flinksqlgateway/changelog
```

### 처리 대상

* INSERT
* UPDATE_BEFORE
* UPDATE_AFTER
* DELETE

### 공개 API 예시

```go
materializer, err := changelog.NewMaterializer(
    changelog.PrimaryKey("id"),
)
```

```go
err = materializer.Apply(row)
rows := materializer.Snapshot()
```

### 구현 요구사항

* Primary Key가 명시된 경우에만 materialization을 허용한다.
* Primary Key가 없는 결과를 임의로 병합하지 않는다.
* 복합키를 지원한다.
* `UPDATE_BEFORE`, `UPDATE_AFTER` 순서를 검증한다.
* 메모리 최대 행 수를 설정할 수 있어야 한다.
* 최대 행 수 초과 시 eviction을 임의 수행하지 않고 명확한 오류를 반환한다.
* Snapshot 반환 시 내부 상태가 외부에서 변경되지 않도록 한다.

### 완료 기준

* INSERT, UPDATE, DELETE 전체 시나리오 테스트가 존재한다.
* 복합 Primary Key를 지원한다.
* Primary Key 누락 시 명확한 오류를 반환한다.

---

# 6. SQL Client 편의 기능

## 6.1 Metadata Helper

### 요구사항

호출자가 `SHOW`, `DESCRIBE`, `EXPLAIN` SQL 결과를 직접 해석하지 않아도 되도록 고수준 메타데이터 API를 제공한다.

### 제공 기능

```go
ListCatalogs(ctx, sessionHandle)
ListDatabases(ctx, sessionHandle, catalog)
ListTables(ctx, sessionHandle, identifier)
ListViews(ctx, sessionHandle, identifier)
ListFunctions(ctx, sessionHandle)
DescribeTable(ctx, sessionHandle, identifier)
Explain(ctx, sessionHandle, statement)
```

### 식별자 타입

```go
type Identifier struct {
    Catalog  string
    Database string
    Object   string
}
```

### 구현 요구사항

* Catalog, Database, Table 이름을 단순 문자열 연결로 SQL에 삽입하지 않는다.
* Flink SQL 식별자 quoting 규칙을 적용한다.
* 식별자 내부 backtick을 안전하게 처리한다.
* 빈 Catalog 또는 Database를 허용하는 경우의 의미를 문서화한다.
* 메타데이터 SQL은 동일 세션의 현재 Catalog와 Database 상태를 고려한다.
* 결과를 typed 구조체로 변환한다.
* 메타데이터 Helper가 별도의 세션을 자동 생성하지 않는다.

### 반환 타입 예시

```go
type TableMetadata struct {
    Catalog  string
    Database string
    Name     string
    Kind     TableKind
}
```

```go
type ColumnMetadata struct {
    Name     string
    DataType string
    Nullable bool
    Comment  string
}
```

### 완료 기준

* Catalog, Database, Table, View 조회 통합 테스트가 존재한다.
* 특수문자가 포함된 식별자 테스트가 존재한다.
* SQL Injection이 발생하지 않도록 식별자 escaping이 검증된다.

---

## 6.2 Capability API

### 요구사항

상위 서비스가 API 버전 문자열을 직접 비교하지 않고, Gateway가 지원하는 기능을 capability로 조회할 수 있어야 한다.

### 공개 API 예시

```go
type Capabilities struct {
    APIVersion        string
    ConfigureSession  bool
    CompleteStatement bool
    RowFormat         bool
    MaterializedTable bool
}
```

```go
caps, err := client.Capabilities(ctx)
```

### 처리 요구사항

* `/api_versions` 결과와 설정된 API 버전을 기준으로 capability를 계산한다.
* v1, v2, v3별 기능 차이를 한 곳에서 관리한다.
* 상위 코드가 `if version >= v2` 같은 문자열 비교를 하지 않도록 한다.
* 서버가 알 수 없는 미래 버전을 반환해도 안전하게 처리한다.
* 현재 모듈 범위 밖 기능도 capability에는 표시할 수 있다.

### 완료 기준

* v1, v2, v3별 capability 테스트가 존재한다.
* 알 수 없는 버전에 대한 처리 정책이 문서화된다.

---

# 7. 네트워크 및 Reverse Proxy 지원

## 7.1 NextResultURI Resolver

### 요구사항

현재 동일 origin의 `nextResultUri`만 허용하는 보안 정책을 유지하면서, Reverse Proxy 환경의 내부 주소와 외부 주소 차이를 안전하게 처리할 수 있어야 한다.

### 공개 API 예시

```go
type NextResultURIResolver interface {
    Resolve(
        baseURL *url.URL,
        nextURI string,
    ) (*url.URL, error)
}
```

### 기본 구현

```text
StrictSameOriginResolver
RelativeOnlyResolver
TrustedProxyRewriteResolver
```

### 보안 요구사항

* 기본값은 `StrictSameOriginResolver`로 유지한다.
* 임의 외부 호스트 허용 옵션을 제공하지 않는다.
* HTTP 또는 HTTPS 이외 scheme은 차단한다.
* 사용자정보가 포함된 URI를 차단한다.
* fragment가 포함된 URI는 제거하거나 차단한다.
* Rewrite 대상 origin은 명시적인 allowlist로만 설정한다.
* 인증 헤더가 외부 호스트로 전달되지 않도록 한다.
* Redirect 처리와 `nextResultUri` 처리에 동일한 origin 정책을 적용한다.

### 설정 예시

```go
TrustedProxyRewriteResolver{
    OriginMappings: map[string]string{
        "http://sql-gateway-01:8083":
            "https://flink-gateway.internal",
    },
}
```

### 완료 기준

* 상대 URI, 동일 origin 절대 URI, 허용된 proxy rewrite가 동작한다.
* 외부 호스트, localhost 우회, userinfo URI, scheme 우회가 차단된다.
* URI Resolver에 대한 fuzz test가 존재한다.

---

# 8. Flink Job REST Companion

## 8.1 별도 패키지 구성

### 요구사항

SQL Gateway에서 반환한 Job ID를 이용해 Flink Job 상태를 조회하고 제어할 수 있는 별도 REST Client를 제공한다.

SQL Gateway Client와 패키지를 분리한다.

```text
flinksqlgateway/
flinkrest/
```

### 제공 기능

```go
GetJob(ctx, jobID)
GetJobStatus(ctx, jobID)
CancelJob(ctx, jobID)
StopJob(ctx, jobID, options)
GetJobExceptions(ctx, jobID)
GetCheckpoints(ctx, jobID)
GetJobPlan(ctx, jobID)
```

### 처리 원칙

* SQL Operation과 Flink Job을 동일한 객체로 취급하지 않는다.
* Operation 취소가 Job 취소를 의미한다고 가정하지 않는다.
* Job 취소는 호출자가 명시적으로 요청한 경우에만 수행한다.
* JobManager REST 주소는 SQL Gateway 주소와 별도로 설정한다.
* Job ID는 원문을 보존하며 형식 검증만 선택적으로 수행한다.
* Job REST 호출에도 context, timeout, 응답 크기 제한을 적용한다.
* Job 취소와 Stop with Savepoint를 구분한다.

### 연계 구조 예시

```go
result, err := sqlClient.ExecuteAndWait(...)
if err != nil {
    return err
}

if result.JobID != "" {
    job, err := jobClient.GetJob(ctx, result.JobID)
}
```

### 완료 기준

* SQL 실행 결과의 Job ID로 Job 상태를 조회할 수 있다.
* Operation 취소와 Job 취소 API가 명확히 분리된다.
* Mock 또는 실제 Flink 1.20.4 환경의 통합 테스트가 존재한다.

---

# 9. Client 전체 자원 관리

## 9.1 Client.Close

### 요구사항

Client가 생성한 heartbeat, stream, polling 작업과 HTTP connection을 정리할 수 있도록 `Close` 기능을 제공한다.

### 공개 API 예시

```go
func (c *GatewayClient) Close() error
```

### 처리 요구사항

* 관리 중인 heartbeat runner를 중단한다.
* 관리 중인 Result Stream을 종료한다.
* 신규 요청을 거부한다.
* 중복 호출해도 안전해야 한다.
* 내부에서 생성한 Transport의 idle connection을 정리한다.
* 외부에서 주입받은 `http.Client`와 Transport의 소유권을 문서화한다.
* 외부 Transport를 Client가 임의로 종료하지 않도록 한다.

### 설정 예시

```go
type Config struct {
    HTTPClient       *http.Client
    OwnHTTPTransport bool
}
```

### 완료 기준

* `Close` 이후 신규 요청이 명확한 오류를 반환한다.
* 중복 `Close`가 panic을 발생시키지 않는다.
* heartbeat와 stream이 모두 종료된다.

---

## 9.2 Cleanup 오류 보존

### 요구사항

원래 실행 오류와 Operation 취소·종료 과정의 오류를 함께 확인할 수 있어야 한다.

### 오류 구조 예시

```go
type ExecutionError struct {
    Cause           error
    CancelError     error
    CloseError      error
    SessionHandle   string
    OperationHandle string
    JobID           string
}
```

### 처리 원칙

* context timeout 또는 SQL 오류를 cleanup 오류로 덮어쓰지 않는다.
* cleanup 오류를 로그에서만 버리지 않는다.
* `errors.Is`, `errors.As`, `errors.Unwrap`을 지원한다.
* 필요하면 `errors.Join`을 사용할 수 있다.
* 인증정보와 전체 SQL은 오류에 포함하지 않는다.

### 완료 기준

* 원래 오류와 cleanup 오류를 모두 확인할 수 있다.
* Cancel 실패와 Close 실패가 각각 구분된다.

---

# 10. 관측성 개선

## 10.1 Observer 이벤트 확장

### 요구사항

기존 주입형 `Observer`를 확장하여 세션, SQL, Operation, 결과 조회, Job ID를 연계할 수 있어야 한다.

### 권장 이벤트

```text
SessionOpened
SessionHeartbeatSucceeded
SessionHeartbeatFailed
SessionHealthChanged
SessionClosed

StatementSubmitting
StatementSubmitted
StatementOutcomeUnknown
StatementCompleted
StatementFailed

OperationPolling
OperationCanceled
OperationClosed

ResultPageFetched
ResultLimitReached
ResultStreamClosed
```

### 공통 필드

```go
type Observation struct {
    Timestamp       time.Time
    Endpoint        string
    Method          string
    Duration        time.Duration
    StatusCode      int
    SessionHandle   string
    OperationHandle string
    JobID           string
    ResultRows      int
    ResponseBytes   int64
    Error           error
}
```

### 보안 요구사항

* Handle은 Observer 구현에서 마스킹할 수 있어야 한다.
* SQL 원문은 기본 이벤트에 포함하지 않는다.
* URL query string과 인증 헤더를 포함하지 않는다.
* 사용자 ID와 워크스페이스 ID는 상위 서비스가 context 또는 metadata로 전달할 수 있어야 한다.

---

# 11. 테스트 요구사항

## 11.1 단위 테스트 추가

다음 테스트를 추가한다.

* SQL 실행 결과 불명확 오류
* Managed Session heartbeat 성공과 실패
* Managed Session 종료
* Serialized Session 실행 순서
* 실행 대기 중 context 취소
* Session Recipe 전체 성공
* Session Recipe 중간 실패
* Result Iterator 정상 EOS
* Result Iterator 사용자 취소
* Result Iterator 결과 제한
* 타입 Decoder
* Capability 판별
* NextResultURI proxy rewrite
* Client.Close
* cleanup 오류 보존
* Changelog Materializer
* Metadata Identifier escaping

## 11.2 Fuzz Test

다음 fuzz test를 작성한다.

```go
func FuzzParseResultPage(f *testing.F)
func FuzzResolveNextResultURI(f *testing.F)
func FuzzParseAPIError(f *testing.F)
func FuzzDecodeRow(f *testing.F)
func FuzzQuoteIdentifier(f *testing.F)
```

검증 대상:

* 비정상 JSON
* 깊게 중첩된 JSON
* 큰 숫자
* 알 수 없는 RowKind
* 알 수 없는 논리 타입
* 악성 nextResultUri
* Unicode 식별자
* backtick 포함 식별자
* 빈 Handle
* 중복 query parameter
* 과도하게 큰 오류 응답

## 11.3 Flink 1.20.4 통합 테스트

API 버전별로 다음 테스트를 실행한다.

```text
Flink 1.20.4 / v1
Flink 1.20.4 / v2
Flink 1.20.4 / v3
```

테스트 SQL 예시:

```sql
SELECT 1;
```

```sql
SET 'execution.runtime-mode' = 'streaming';
```

```sql
SHOW CATALOGS;
```

```sql
SHOW DATABASES;
```

```sql
SHOW TABLES;
```

```sql
EXPLAIN SELECT 1;
```

스트리밍 테스트:

```sql
CREATE TEMPORARY TABLE source_table (
    id BIGINT,
    value STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10'
);

SELECT * FROM source_table;
```

검증 내용:

* 행 수 제한 도달 후 Operation 정리
* context 취소
* 느린 소비자
* heartbeat와 SQL 실행 동시 처리
* session close와 fetch의 race condition
* operation cancel과 EOS의 race condition
* v1, v2, v3 결과 구조 차이
* Job ID 반환 및 Job REST 조회

## 11.4 품질 검사

다음을 모두 통과해야 한다.

```bash
go test ./...
go test -race ./...
go vet ./...
govulncheck ./...
```

프로젝트에 lint 도구가 있다면 해당 검사도 포함한다.

---

# 12. 배포 및 모듈 품질

## 12.1 크로스플랫폼 빌드

PowerShell 외에 Linux 환경에서도 동일한 검증이 가능해야 한다.

다음 중 하나를 적용한다.

```text
build.ps1 + build.sh
```

또는:

```text
cmd/build 내부 Go 기반 빌드 도구
```

빌드 절차와 결과가 운영체제에 따라 달라지지 않도록 한다.

## 12.2 CI Matrix

최소한 다음 CI를 구성한다.

```text
Windows
Linux
단위 테스트
race detector
govulncheck
integration test
```

통합 테스트는 Flink 1.20.4 환경이 제공될 때만 실행하거나 별도 workflow로 분리한다.

## 12.3 릴리스 산출물

현재 산출물에 다음을 추가한다.

* CHANGELOG
* SPDX 또는 CycloneDX SBOM
* API 호환성 보고서
* 서명된 Git Tag
* 릴리스 provenance
* SHA-256 checksum

## 12.4 Module Path 검증

릴리스 빌드에서는 임시 module path 사용을 차단한다.

차단 대상 예시:

```text
flink-sql-go
github.com/<owner>/flink-sql-go
example.com/...
```

릴리스 전에 실제 원격 저장소의 module path로 변경되었는지 검사한다.

---

# 13. 범위 제외

이번 개선 범위에 다음 기능은 포함하지 않는다.

* JDBC 드라이버 구현
* JVM 또는 GraalVM 연동
* `database/sql` 호환 계층
* SQL Parser 자체 개발
* Kafka 연결 검증
* Elasticsearch 연결 검증
* UI 구현
* 사용자 인증 시스템 구현
* 제품별 SQL 권한 정책 하드코딩
* Flink Materialized Table v3 전체 지원

---

# 14. 권장 구현 순서

## 1단계: 실행 안정성

* `ErrExecutionOutcomeUnknown`
* Managed Session
* Client.Close
* Cleanup 오류 보존

## 2단계: 세션 사용성

* Serialized Session
* Session Recipe
* Capability API

## 3단계: 결과 사용성

* Result Iterator
* 타입 Decoder
* Metadata Helper
* NextResultURI Resolver

## 4단계: 운영 기능

* Flink Job REST Companion
* Changelog Materializer
* Observer 확장

## 5단계: 품질 강화

* Fuzz Test
* API 버전별 Contract Test
* 크로스플랫폼 CI
* SBOM과 릴리스 검증

---

# 15. 최종 결과 보고 형식

구현 완료 후 다음 형식으로 결과를 보고한다.

## 구현 요약

* 추가된 기능
* 신규 공개 API
* 기존 API 변경 여부
* 하위 호환성 여부

## 주요 설계 결정

* 세션 lifecycle
* heartbeat 관리
* 동일 세션 직렬화
* 결과 iterator
* 자동 재시도 정책
* cleanup 정책
* Job REST 연계 방식

## 변경 파일

| 파일 | 변경 내용 |
| -- | ----- |

## 테스트 결과

| 테스트 | 결과 |
| --- | -- |

## API 호환성

| API | 기존 | 변경 후 | 호환 여부 |
| --- | -- | ---- | ----- |

## 남은 제약

* 미지원 기능
* 운영 적용 전 추가 검증 사항
* Flink 버전 업그레이드 시 확인할 사항

우선 구현 범위는 **실행 결과 불명확 오류, Managed Session, 세션 직렬 실행, Result Iterator, Job REST 연계**까지 잡는 것이 적절합니다.
