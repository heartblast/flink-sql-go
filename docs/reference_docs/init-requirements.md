# Flink SQL Gateway REST Client Go 모듈 개발 지침

현재 서비스에 **Apache Flink 1.20.4 SQL Gateway REST API 전용 Go Client 모듈**을 구현하라.

이 모듈의 목적은 JDBC 드라이버를 사용하지 않고 Go 백엔드에서 Flink SQL Gateway에 직접 연결하여 다음 기능을 제공하는 것이다.

* SQL 세션 생성 및 유지
* Flink SQL 실행
* 비동기 Operation 상태 조회
* SELECT 결과의 반복 조회
* 스트리밍 쿼리 결과 처리
* SQL Operation 취소 및 종료
* 세션 heartbeat 및 종료
* Catalog, Database, Table 등의 메타데이터 SQL 실행
* SQL 자동완성 지원
* Flink Job ID 반환 및 추적

## 1. 개발 원칙

다음 원칙을 반드시 준수하라.

1. 대상 버전은 **Apache Flink 1.20.4**이다.
2. JDBC, JVM, GraalVM, CGO를 사용하지 않는다.
3. Go 표준 `net/http`를 기반으로 SQL Gateway REST API를 직접 호출한다.
4. 일반 RDBMS용 `database/sql` 인터페이스에 억지로 맞추지 않는다.
5. Flink SQL Gateway의 세션과 Operation 생명주기를 명시적으로 모델링한다.
6. 기존 프로젝트의 HTTP Client, 로깅, 설정, 오류 처리, 테스트 방식을 먼저 확인하고 이에 맞춰 통합한다.
7. 독립된 예제 프로그램만 만들지 말고 현재 서비스에서 재사용할 수 있는 패키지로 구현한다.
8. 모든 네트워크 호출은 `context.Context`를 지원해야 한다.
9. Flink SQL Gateway의 응답 구조를 임의로 단순화하지 말고 원본 의미를 보존한다.
10. Flink 1.20 계열의 SQL Gateway OpenAPI는 실험적이므로 DTO와 전송 계층을 분리하여 향후 변경에 대응할 수 있게 한다.

## 2. 권장 패키지 구조

현재 프로젝트 구조를 우선 따르되, 별도 구조가 필요하면 다음과 같이 구성한다.

```text
internal/flinksqlgateway/
├── client.go
├── config.go
├── transport.go
├── session.go
├── operation.go
├── result.go
├── types.go
├── errors.go
├── poller.go
├── security.go
├── client_test.go
├── session_test.go
├── operation_test.go
└── testdata/
```

역할은 다음과 같이 분리한다.

* `client.go`: 공개 Client 인터페이스와 생성자
* `transport.go`: HTTP 요청, JSON 직렬화, 공통 헤더 처리
* `session.go`: 세션 생성, 조회, heartbeat, 종료
* `operation.go`: SQL 실행, 상태 조회, 취소, 종료
* `result.go`: 결과 조회, 페이지 처리, RowKind 처리
* `poller.go`: 상태 polling과 backoff
* `errors.go`: Flink 및 HTTP 오류의 타입화
* `security.go`: 민감정보 마스킹과 URL 검증

## 3. Client 설정

다음과 같은 설정을 지원하라.

```go
type Config struct {
    BaseURL              string
    APIVersion           string
    HTTPClient           *http.Client
    RequestTimeout       time.Duration
    ExecutionTimeout     time.Duration
    PollInterval         time.Duration
    MaxPollInterval      time.Duration
    HeartbeatInterval    time.Duration
    MaxResultRows        int
    MaxResponseBytes     int64
    DefaultRowFormat     RowFormat
    UserAgent            string
    Headers              map[string]string
    CancelOnContextDone  bool
}
```

추가 요구사항:

* `BaseURL` 마지막 `/` 유무와 관계없이 정상 동작해야 한다.
* API 버전을 URL 문자열 곳곳에 하드코딩하지 않는다.
* 기본 API 버전은 설정할 수 있게 한다.
* `/info`와 `/api_versions`를 이용한 버전 확인 기능을 제공한다.
* 설정된 API 버전을 서버가 지원하지 않으면 명확한 오류를 반환한다.
* `http.Client`를 외부에서 주입할 수 있어야 한다.
* 운영 코드에서 `http.DefaultClient`에 직접 의존하지 않는다.
* TLS 검증을 기본적으로 활성화한다.
* `InsecureSkipVerify`를 기본값으로 사용하지 않는다.
* Bearer Token, Reverse Proxy 인증 헤더, mTLS 등을 확장할 수 있도록 HTTP Transport 주입을 허용한다.

## 4. 공개 인터페이스

최소한 다음 수준의 인터페이스를 제공하라.

```go
type Client interface {
    GetInfo(ctx context.Context) (*GatewayInfo, error)
    GetAPIVersions(ctx context.Context) ([]string, error)

    OpenSession(
        ctx context.Context,
        req OpenSessionRequest,
    ) (*Session, error)

    GetSessionConfig(
        ctx context.Context,
        sessionHandle string,
    ) (map[string]string, error)

    ConfigureSession(
        ctx context.Context,
        sessionHandle string,
        statement string,
        executionTimeout time.Duration,
    ) error

    CompleteStatement(
        ctx context.Context,
        sessionHandle string,
        statement string,
        position int,
    ) ([]string, error)

    Heartbeat(
        ctx context.Context,
        sessionHandle string,
    ) error

    ExecuteStatement(
        ctx context.Context,
        sessionHandle string,
        req ExecuteStatementRequest,
    ) (*Operation, error)

    GetOperationStatus(
        ctx context.Context,
        sessionHandle string,
        operationHandle string,
    ) (OperationStatus, error)

    FetchResults(
        ctx context.Context,
        sessionHandle string,
        operationHandle string,
        token int64,
        rowFormat RowFormat,
    ) (*ResultPage, error)

    CancelOperation(
        ctx context.Context,
        sessionHandle string,
        operationHandle string,
    ) error

    CloseOperation(
        ctx context.Context,
        sessionHandle string,
        operationHandle string,
    ) error

    CloseSession(
        ctx context.Context,
        sessionHandle string,
    ) error
}
```

편의 기능은 기본 API 위에 별도 고수준 메서드로 구현한다.

```go
type StatementExecutor interface {
    ExecuteAndWait(
        ctx context.Context,
        sessionHandle string,
        statement string,
        options ExecuteOptions,
    ) (*ExecutionResult, error)

    StreamResults(
        ctx context.Context,
        sessionHandle string,
        statement string,
        options StreamOptions,
    ) (<-chan ResultEvent, <-chan error)
}
```

저수준 REST 메서드와 고수준 실행 편의 기능을 섞지 않는다.

## 5. 세션 관리

Flink SQL Gateway 세션에는 다음 상태가 유지될 수 있다.

* `SET`으로 지정한 설정
* 현재 Catalog
* 현재 Database
* Temporary Table
* Temporary View
* Temporary Function
* 추가된 JAR
* Module 설정

따라서 다음 규칙을 적용하라.

1. SQL 문마다 새 세션을 생성하지 않는다.
2. 사용자 또는 워크스페이스 단위로 세션을 분리할 수 있게 한다.
3. 서로 다른 사용자의 세션을 공유하지 않는다.
4. 세션 핸들은 외부에서 임의로 조작하지 못하도록 소유권을 확인한다.
5. heartbeat goroutine은 중복 생성되지 않아야 한다.
6. heartbeat 종료를 위한 `context.CancelFunc` 또는 명시적인 `Close()`를 제공한다.
7. 세션 종료 시 관련 heartbeat를 반드시 중단한다.
8. 세션 만료 시 자동으로 새 세션을 만들고 실행을 재시도하지 않는다.
9. 자동 재생성 시 Temporary Table, `SET`, Catalog 상태가 유실되므로 `ErrSessionExpired`를 반환한다.
10. 세션 종료는 여러 번 호출해도 안전한 형태로 구현하는 것을 검토한다.

다음과 같은 관리 객체를 고려한다.

```go
type ManagedSession struct {
    Handle       string
    Name         string
    Properties   map[string]string
    CreatedAt    time.Time
    LastActiveAt time.Time

    cancelHeartbeat context.CancelFunc
}
```

## 6. SQL 실행과 Operation 처리

SQL 실행은 동기 DB 호출처럼 가정하지 않는다.

실행 흐름은 다음과 같아야 한다.

```text
Open Session
    ↓
POST Statement
    ↓
Operation Handle 수신
    ↓
Operation 상태 또는 결과 조회
    ↓
NOT_READY이면 대기 후 재조회
    ↓
PAYLOAD이면 결과 처리
    ↓
nextResultUri 또는 다음 token 조회
    ↓
EOS이면 결과 조회 완료
    ↓
Close Operation
```

다음 요구사항을 적용한다.

* `operationHandle`을 받은 이후부터 취소와 정리가 가능해야 한다.
* Operation 상태 문자열은 알 수 없는 값도 보존할 수 있게 구현한다.
* 알 수 없는 상태를 성공 상태로 취급하지 않는다.
* polling에는 고정 간격 또는 제한된 exponential backoff를 적용한다.
* polling 최대 간격을 제한한다.
* 서버 오류 발생 시 무한 반복하지 않는다.
* `context.Context`가 취소되면 polling을 즉시 중단한다.
* 설정에 따라 context 취소 시 Operation 취소 API를 호출할 수 있게 한다.
* Operation 취소 실패가 원래 context 오류를 가리지 않도록 한다.
* 결과를 모두 읽기 전에 Operation을 닫지 않는다.
* 종료, 실패 또는 취소 후에는 가능한 범위에서 Operation 자원을 정리한다.

## 7. 결과 조회

다음 ResultType을 명시적으로 처리한다.

```go
type ResultType string

const (
    ResultNotReady ResultType = "NOT_READY"
    ResultPayload  ResultType = "PAYLOAD"
    ResultEOS      ResultType = "EOS"
)
```

처리 방식:

* `NOT_READY`: 결과가 아직 준비되지 않았으므로 일정 시간 후 재조회
* `PAYLOAD`: 현재 결과 페이지 처리
* `EOS`: 더 이상 조회할 결과 없음

결과 응답에서 다음 항목을 보존한다.

```go
type ResultPage struct {
    ResultType    ResultType
    ResultKind    ResultKind
    QueryResult   bool
    JobID         string
    NextResultURI string
    Results       *ResultInfo
}
```

다음 정보를 누락하지 않는다.

* 컬럼명
* 컬럼 논리 타입
* nullable 여부
* 컬럼 설명
* 결과 데이터
* RowKind
* Job ID
* ResultKind
* QueryResult 여부
* 다음 결과 URI
* RowFormat

## 8. RowKind 처리

Flink SQL 결과는 일반적인 append-only 결과가 아닐 수 있다.

다음 RowKind를 모두 지원하라.

```go
type RowKind string

const (
    RowInsert       RowKind = "INSERT"
    RowUpdateBefore RowKind = "UPDATE_BEFORE"
    RowUpdateAfter  RowKind = "UPDATE_AFTER"
    RowDelete       RowKind = "DELETE"
)
```

주의사항:

* 모든 행을 단순 INSERT 행으로 변환하지 않는다.
* `UPDATE_BEFORE`, `UPDATE_AFTER`, `DELETE` 의미를 결과 객체에서 보존한다.
* UI가 changelog 결과를 표현할 수 있도록 RowKind를 상위 계층에 전달한다.
* 필요하면 별도의 materialized-view 변환 유틸리티를 제공하되 기본 Client에 포함하지 않는다.

## 9. RowFormat과 타입 처리

기본 RowFormat은 `JSON`을 우선 사용한다.

```go
type RowFormat string

const (
    RowFormatJSON      RowFormat = "JSON"
    RowFormatPlainText RowFormat = "PLAIN_TEXT"
)
```

적용 원칙:

* `JSON`은 컬럼 타입을 최대한 유지하는 용도로 사용한다.
* `PLAIN_TEXT`는 모든 값을 문자열로 다루는 표시용 옵션으로 사용한다.
* Decimal, Timestamp, Date, Time, Array, Map, Row, Binary 타입을 임의의 Go 기본 타입으로 강제 변환하지 않는다.
* 원본 JSON 값을 `json.RawMessage`로 보존할 수 있는 계층을 둔다.
* 타입 변환은 별도 변환기에서 수행한다.
* Flink 논리 타입에 없는 값이 추가되어도 전체 응답 파싱이 실패하지 않게 한다.

## 10. nextResultUri 보안 처리

서버가 반환한 `nextResultUri`를 무조건 요청하지 않는다.

다음 검증을 수행하라.

* URI 파싱 오류 확인
* `http` 또는 `https` 이외 스킴 차단
* 기본적으로 최초 Gateway와 동일한 host인지 확인
* 외부 주소나 localhost 등으로 변조된 URI를 따라가지 않음
* 설정에 따라 상대 URI와 절대 URI를 모두 지원
* 인증 헤더가 외부 호스트로 유출되지 않도록 처리

가능하면 `nextResultUri`를 그대로 따라가는 방식과 token 증가 방식 중 Flink 1.20.4 실제 응답을 기준으로 구현하고, integration test로 검증하라.

## 11. SQL 종류를 추정하지 말 것

다음과 같은 정규식 기반 처리를 핵심 로직으로 사용하지 않는다.

```text
SELECT로 시작하면 조회
INSERT로 시작하면 Job
CREATE로 시작하면 DDL
```

주석, CTE, Statement Set, EXPLAIN, 다중 행 SQL 등에서 잘못 판단할 수 있다.

대신 다음 서버 응답을 우선 사용한다.

* `queryResult`
* `resultKind`
* `resultType`
* `jobID`
* 결과 스키마
* Operation 상태

SQL 분류가 UI 표시를 위해 필요하면 별도 보조 기능으로 분리하고, 실행 제어에는 사용하지 않는다.

## 12. Job ID 처리

`INSERT INTO`, Statement Set 등의 실행 결과에 Job ID가 포함될 수 있다.

다음 요구사항을 적용하라.

* 응답의 `jobID`를 반환 객체에 보존한다.
* Job ID는 32자리 hex 문자열인지 검증할 수 있으나 원본 값도 보존한다.
* SQL Operation과 Flink Job을 동일한 생명주기로 가정하지 않는다.
* Operation 취소가 이미 제출된 Flink Job까지 항상 취소한다고 가정하지 않는다.
* Flink Job 제어가 필요하면 Flink Cluster REST API 전용 Client를 별도 모듈로 구현한다.
* SQL Gateway Client 내부에 JobManager REST API 기능을 혼합하지 않는다.

## 13. 재시도 정책

HTTP 메서드와 작업 의미를 고려하여 재시도한다.

자동 재시도를 제한적으로 허용할 수 있는 요청:

* Gateway 정보 조회
* API 버전 조회
* 세션 설정 조회
* Operation 상태 조회
* 결과 조회
* heartbeat

원칙적으로 자동 재시도하지 않는 요청:

* 세션 생성
* SQL 실행
* 세션 설정 SQL 실행
* Operation 취소
* Operation 종료
* 세션 종료

특히 SQL 실행 POST를 네트워크 오류만 보고 재호출하면 동일 Job이 중복 제출될 수 있으므로 자동 재시도하지 않는다.

필요하면 호출자가 재시도를 결정할 수 있도록 다음 정보를 오류에 포함한다.

* HTTP status
* endpoint
* method
* 재시도 가능 여부
* 서버 오류 메시지
* 원인 오류

## 14. 오류 모델

오류를 문자열 하나로 반환하지 않는다.

예시:

```go
type APIError struct {
    Method     string
    Endpoint   string
    StatusCode int
    Code       string
    Message    string
    Retryable  bool
    Cause      error
}
```

다음 sentinel 또는 typed error를 제공하는 것을 검토한다.

```go
var (
    ErrSessionNotFound   = errors.New("flink sql session not found")
    ErrSessionExpired    = errors.New("flink sql session expired")
    ErrOperationNotFound = errors.New("flink sql operation not found")
    ErrOperationFailed   = errors.New("flink sql operation failed")
    ErrResultLimit       = errors.New("flink sql result limit exceeded")
    ErrUnsupportedAPI    = errors.New("unsupported sql gateway api version")
)
```

추가 요구사항:

* HTTP 응답 body가 JSON이 아닐 수 있는 경우도 처리한다.
* 오류 응답 body 크기를 제한한다.
* 서버 stack trace 전체를 일반 사용자에게 직접 노출하지 않는다.
* 내부 로그에는 상세 오류를 남기되 비밀번호와 인증정보를 제거한다.
* SQL 문 전체를 기본 로그에 남기지 않는다.
* SQL을 로그에 기록해야 하면 길이를 제한하고 민감한 connector 옵션을 마스킹한다.

## 15. 결과 제한

무한 스트리밍 SELECT에 대비하여 다음 제한을 제공한다.

* 최대 결과 행 수
* 최대 응답 바이트
* 최대 실행 시간
* 최대 polling 횟수
* 사용자 취소
* 결과 소비자 처리 지연 감지
* 채널 buffer 제한

제한 도달 시 다음 순서로 처리한다.

1. 결과 수신 중단
2. 필요하면 Operation 취소
3. Operation 정리
4. 제한 초과 오류 반환
5. 이미 수신한 결과 건수와 종료 이유 기록

전체 결과를 무조건 메모리에 저장하는 API만 제공하지 않는다.

다음 두 방식을 모두 고려한다.

```go
func ExecuteAndCollect(...) (*ExecutionResult, error)
```

```go
func StreamResults(...) (<-chan ResultEvent, <-chan error)
```

대량 또는 스트리밍 결과에는 callback이나 channel 기반 API를 사용한다.

## 16. 동시성 안전성

다음 사항을 검토하고 테스트하라.

* 하나의 Client를 여러 goroutine이 공유할 수 있는지 명시
* 동일 세션에서 여러 SQL을 동시에 실행할 때의 정책
* heartbeat와 세션 종료의 race condition 방지
* Operation 취소와 결과 조회의 race condition 방지
* channel을 두 번 닫지 않도록 처리
* goroutine leak 방지
* context 종료 후 polling goroutine이 남지 않도록 처리

공개 문서에 Client, Session, Operation 각각의 thread safety 수준을 명시한다.

## 17. 보안 요구사항

Flink SQL Gateway가 자체적으로 충분한 사용자 인증과 권한 분리를 제공한다고 가정하지 않는다.

다음 통제를 적용할 수 있게 설계한다.

* Gateway URL allowlist
* 사용자별 세션 소유권 검증
* 포털 권한에 따른 SQL 실행 허용
* SQL 실행 감사 로그
* 실행 사용자, 세션, Operation, Job ID 연계
* Reverse Proxy 인증 헤더
* TLS 또는 mTLS
* 요청 및 응답 크기 제한
* `ADD JAR` 경로 제한
* 민감한 `WITH` 옵션 마스킹
* SQL Gateway 주소를 브라우저에 직접 노출하지 않음

Client 모듈 자체에서 복잡한 SQL 권한 정책을 구현하지 말고, 상위 서비스가 정책을 주입하거나 실행 전 검증할 수 있는 hook을 제공한다.

```go
type StatementValidator interface {
    Validate(
        ctx context.Context,
        session SessionContext,
        statement string,
    ) error
}
```

## 18. 관측성

다음 항목을 구조화 로그 또는 metric으로 수집할 수 있게 한다.

* Gateway 요청 수
* API별 응답 시간
* HTTP 오류 수
* 활성 세션 수
* 활성 Operation 수
* SQL 실행 시간
* 결과 polling 횟수
* 반환 행 수
* 반환 데이터 크기
* 취소 횟수
* 세션 만료 횟수
* heartbeat 실패 횟수

로그에는 다음 correlation 정보를 포함한다.

* 내부 사용자 ID
* 워크스페이스 ID
* session handle의 마스킹 값
* operation handle의 마스킹 값
* Flink Job ID
* request trace ID

비밀번호, 인증 헤더, 전체 SQL, Kafka SASL 비밀번호, Elasticsearch 비밀번호는 로그에 남기지 않는다.

## 19. 테스트 요구사항

### 단위 테스트

`httptest.Server`를 이용해 최소한 다음을 검증한다.

* API 버전 조회
* Gateway 정보 조회
* 세션 생성
* 세션 설정 조회
* heartbeat
* SQL 실행
* Operation 상태 조회
* `NOT_READY` 처리
* `PAYLOAD` 처리
* `EOS` 처리
* 여러 페이지 결과 조회
* JSON RowFormat
* PLAIN_TEXT RowFormat
* RowKind별 파싱
* Job ID 파싱
* Operation 취소
* Operation 종료
* 세션 종료
* HTTP 4xx
* HTTP 5xx
* 잘못된 JSON
* 응답 크기 초과
* timeout
* context 취소
* 외부 host의 `nextResultUri` 차단
* POST 중복 재시도 방지

### 통합 테스트

가능한 경우 실제 **Flink 1.20.4 SQL Gateway**를 대상으로 다음 시나리오를 검증한다.

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

검증 항목:

* `/info`에서 실제 Flink 버전 확인
* 세션 생성 및 종료
* 동일 세션에서 `SET` 유지
* 결과의 `NOT_READY`, `PAYLOAD`, `EOS` 흐름
* `nextResultUri` 형식
* 결과 스키마와 데이터 구조
* 장시간 실행 SELECT 취소
* INSERT 실행 시 Job ID 반환
* 세션 idle timeout과 heartbeat
* Operation 종료 후 자원 정리

Flink 문서 예제만 기준으로 DTO를 추측하지 말고 실제 1.20.4 응답을 testdata로 저장하여 검증한다.

### Go 품질 검사

다음을 통과해야 한다.

```bash
go test ./...
go test -race ./...
go vet ./...
```

프로젝트에서 사용하는 lint 도구가 있다면 해당 검사도 통과시킨다.

## 20. 사용 예제

README에 다음 수준의 예제를 제공한다.

```go
client, err := flinksqlgateway.NewClient(
    flinksqlgateway.Config{
        BaseURL:          "https://flink-gateway.internal:8083",
        APIVersion:       "v1",
        RequestTimeout:   10 * time.Second,
        ExecutionTimeout: 30 * time.Second,
        PollInterval:     300 * time.Millisecond,
        MaxPollInterval:  3 * time.Second,
        MaxResultRows:    1000,
        DefaultRowFormat: flinksqlgateway.RowFormatJSON,
    },
)
if err != nil {
    return err
}

session, err := client.OpenSession(
    ctx,
    flinksqlgateway.OpenSessionRequest{
        SessionName: "workspace-user-123",
        Properties: map[string]string{
            "execution.runtime-mode": "streaming",
        },
    },
)
if err != nil {
    return err
}
defer client.CloseSession(context.Background(), session.Handle)

result, err := client.ExecuteAndWait(
    ctx,
    session.Handle,
    "SHOW TABLES",
    flinksqlgateway.ExecuteOptions{
        MaxRows: 1000,
    },
)
if err != nil {
    return err
}
```

실제 공개 API와 예제 코드가 일치해야 한다.

## 21. 비기능 요구사항

* Go 코드에 불필요한 전역 변수를 사용하지 않는다.
* 외부 의존성 추가는 최소화한다.
* 인터페이스를 지나치게 세분화하지 않는다.
* DTO와 도메인 모델을 분리한다.
* 공개 타입과 메서드에 GoDoc을 작성한다.
* 모든 응답 body를 반드시 닫는다.
* HTTP keep-alive와 connection pooling을 활용한다.
* 응답 전체를 무제한으로 `io.ReadAll` 하지 않는다.
* 시간 단위는 문서화하고 `time.Duration`으로 표현한다.
* Handle은 불필요하게 UUID로 강제 파싱하지 않고 opaque string으로 취급한다.
* 알 수 없는 enum 값이 추가되어도 가능한 범위에서 하위 호환성을 유지한다.

## 22. 구현하지 말아야 할 것

이번 모듈에 다음 기능을 포함하지 않는다.

* JDBC JAR 로딩
* JVM 실행
* GraalVM 연동
* `database/sql` 드라이버 구현
* SQL 자체 파서 개발
* Flink JobManager REST Client 전체 구현
* Kafka 또는 Elasticsearch 연결 검증
* UI 코드
* 사용자 인증 시스템 자체 구현
* SQL 권한 정책의 하드코딩

필요한 경우 이 기능들은 별도 계층 또는 별도 모듈로 분리한다.

## 23. 작업 절차

다음 순서로 작업하라.

1. 현재 저장소 구조와 기존 HTTP Client 구현을 분석한다.
2. Flink 1.20.4 SQL Gateway REST API와 실제 응답 구조를 확인한다.
3. 설계안을 먼저 제시한다.
4. 공개 인터페이스와 오류 모델을 정의한다.
5. 저수준 REST Client를 구현한다.
6. 세션 관리와 heartbeat를 구현한다.
7. Operation 실행, polling, 취소, 종료를 구현한다.
8. 결과 paging과 RowKind를 구현한다.
9. 결과 제한과 context 취소를 구현한다.
10. 단위 테스트를 작성한다.
11. 실제 Flink 1.20.4 통합 테스트를 수행한다.
12. README와 사용 예제를 작성한다.
13. race condition과 goroutine leak 여부를 점검한다.
14. 변경 파일, 핵심 설계, 테스트 결과, 남은 제약을 정리한다.

## 24. 최종 결과 보고 형식

작업 완료 후 다음 형식으로 보고하라.

### 구현 요약

* 추가한 패키지
* 주요 기능
* 공개 API
* 사용한 SQL Gateway API 버전

### 주요 설계 결정

* 세션 관리 방식
* Operation polling 방식
* 결과 paging 방식
* context 취소 방식
* 재시도 정책
* 보안 처리 방식

### 변경 파일

| 파일 | 변경 내용 |
| -- | ----- |

### 테스트 결과

| 테스트 | 결과 |
| --- | -- |

### 확인된 Flink 1.20.4 응답 특성

* API 버전
* Operation 상태
* ResultType
* nextResultUri
* Job ID
* RowKind
* 오류 응답 구조

### 남은 제약

* 미지원 기능
* 운영 적용 전 추가 검증 사항
* 향후 API 버전 변경 대응 사항

특히 Claude가 **SQL 실행 POST를 자동 재시도하거나, 모든 결과를 INSERT 행으로 처리하거나, 세션 만료 시 몰래 새 세션으로 교체하지 않도록** 명시하는 것이 중요합니다. Flink SQL Gateway는 상태를 가진 세션과 비동기 Operation 구조이며, API 버전도 런타임에서 확인할 수 있습니다. ([nightlies.apache.org][1])

[1]: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql-gateway/rest/ "REST Endpoint | Apache Flink"
