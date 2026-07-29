# flink-sql-go v0.1.5 Session Setup 기능 개발 요청

## 1. 작업 대상

* 저장소: `github.com/heartblast/flink-sql-go`
* 기준 Apache Flink: `1.20.4`
* 작업 브랜치: `0.1.5`
* 목표 버전: `v0.1.5`

반드시 원격 `0.1.5` 브랜치를 fetch한 후 해당 브랜치에서 작업한다. `main` 브랜치에는 직접 커밋하지 않는다.

기능 구현, 테스트, 문서 및 버전 정합성 반영이 모두 끝나면 원격 `0.1.5` 브랜치에 commit 및 push한다.

권장 최종 커밋 메시지:

```text
feat: add declarative session setup for v0.1.5
```

## 2. 개발 목적

현재 `flinksqlgateway` 패키지는 다음 기능을 제공한다.

* `ConfigureSession`을 통한 SQL Gateway `configure-session` 호출
* `SessionRecipe`를 통한 세션 생성 및 setup SQL 순차 실행
* `ListCatalogs`, `ListDatabases`, `ListTables`, `DescribeTable` 등의 metadata helper
* `QuoteIdentifier` 기반 식별자 quoting
* 비멱등 SQL 실행 POST의 자동 재시도 차단
* typed error와 session cleanup 처리

이번 작업에서는 catalog, database, table 및 현재 session scope를 선언형으로 구성할 수 있는 고수준 Session Setup API를 추가한다.

기존 `SessionRecipe`는 `/statements`와 `ExecuteAndWait`를 사용하는 범용 SQL replay 기능으로 유지하고, 신규 Session Setup 기능은 `/configure-session`을 호출하는 `ConfigureSession`을 기반으로 구현한다.

## 3. 공개 API 설계

다음 역할을 제공하는 타입을 추가한다. 정확한 필드명은 기존 코드 스타일에 맞춰 조정할 수 있지만 역할과 안전 요구사항은 유지해야 한다.

```go
type SessionSetupPlan struct {
    Catalogs        []CatalogSetup
    Databases       []DatabaseSetup
    Tables          []TableSetup
    CurrentCatalog  string
    CurrentDatabase string
}

type CatalogSetup struct {
    Name          string
    IfNotExists   bool
    Options       map[string]string
    SensitiveKeys []string
}

type DatabaseSetup struct {
    Catalog     string
    Name        string
    IfNotExists bool
    Options     map[string]string
}

type TableSetup struct {
    Target    Identifier
    Statement string
    Verify    bool
    Sensitive bool
}
```

최소 다음 기능을 제공한다.

```go
func CompileSessionSetup(
    plan SessionSetupPlan,
) ([]SessionSetupStep, error)

func (c *GatewayClient) ApplySessionSetup(
    ctx context.Context,
    sessionHandle string,
    plan SessionSetupPlan,
    options SessionSetupOptions,
) (*SessionSetupResult, error)

func (c *GatewayClient) OpenSessionWithSetup(
    ctx context.Context,
    request OpenSessionRequest,
    plan SessionSetupPlan,
    options SessionSetupOptions,
) (*SessionSetupResult, error)
```

`CompileSessionSetup`은 네트워크를 호출하지 않고 전체 입력을 먼저 검증해야 한다. 일부 DDL을 실행한 후 입력 오류를 발견하는 구조로 구현하지 않는다.

## 4. 기존 인터페이스 호환성

기존 공개 타입이나 메서드를 제거하거나 이름을 바꾸지 않는다.

기존 `Client` 인터페이스에 신규 메서드를 직접 추가하면 외부에서 자체 mock 또는 wrapper로 `Client`를 구현한 코드가 깨질 수 있으므로 별도 인터페이스를 추가한다.

```go
type SessionSetupExecutor interface {
    ApplySessionSetup(
        ctx context.Context,
        sessionHandle string,
        plan SessionSetupPlan,
        options SessionSetupOptions,
    ) (*SessionSetupResult, error)
}
```

`GatewayClient`가 기존 `Client`와 신규 `SessionSetupExecutor`를 모두 구현하도록 한다.

## 5. Setup 실행 순서

가능하면 catalog, database 및 table 이름을 완전 수식하여 현재 session scope에 대한 의존성을 줄인다.

기본 실행 순서:

1. `CREATE CATALOG`
2. `CREATE DATABASE catalog.database`
3. `CREATE TABLE catalog.database.table`
4. `USE CATALOG`
5. `USE database`
6. 선택적 metadata 검증

Raw `CREATE TABLE` SQL이 현재 catalog/database에 의존한다면 다음 중 안전한 방식을 선택하고 문서화한다.

* `TableSetup.Target`을 필수로 받고 라이브러리가 완전 수식된 DDL을 생성한다.
* scope 의존 SQL을 `USE` 실행 이후 단계로 분류한다.
* SQL parser 없이 안전하게 판별할 수 없는 statement는 compile 단계에서 거부한다.

단순 문자열 치환으로 SQL 객체명을 추측하거나 재작성하지 않는다. 이 프로젝트가 SQL parser를 포함하지 않는다는 기존 패키지 경계를 유지한다.

## 6. 식별자와 문자열 값 처리

* 식별자는 기존 `QuoteIdentifier`를 재사용한다.
* catalog/database option 값은 SQL 문자열 literal로 안전하게 quoting한다.
* 문자열 내부 작은따옴표는 두 번 써서 escape한다.
* option map의 key를 정렬해 항상 결정적인 SQL을 생성한다.
* 빈 식별자, 잘못된 target 조합, 중복 객체 정의 및 모순된 current scope는 네트워크 호출 전에 거부한다.

예시:

```go
func quoteStringLiteral(value string) string {
    return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
```

이 helper를 공개 API로 노출할지는 API 최소화 원칙에 따라 결정한다.

## 7. ConfigureSession 결과 불명확 오류

현재 `ExecuteStatement`는 요청이 서버에 도달했을 가능성이 있으나 결과를 확인하지 못한 경우 `ErrExecutionOutcomeUnknown`을 반환한다.

`ConfigureSession`에도 이에 대응하는 typed error를 추가한다.

```go
var ErrConfigurationOutcomeUnknown =
    errors.New("session configuration outcome is unknown")

type ConfigurationOutcomeUnknownError struct {
    SessionHandle string
    StepIndex     int
    StepKind      SessionSetupStepKind
    RequestPhase  RequestPhase
    Cause         error
}
```

필수 처리 기준:

* `RequestNotSent`는 결과 불명확 오류로 분류하지 않는다.
* 요청이 전송됐을 가능성이 있고 응답 유실, 408, 429 또는 5xx가 발생했다면 보수적으로 결과 불명확 상태로 분류한다.
* `ConfigureSession`과 setup step을 자동 재시도하지 않는다.
* 결과 불명확 상태에서 동일 DDL을 자동 재실행하지 않는다.
* 객체가 명확한 경우 read-only metadata 조회를 통해 상태 확인을 시도할 수 있다.
* metadata로 확인할 수 없으면 `OutcomeUnknown` 상태를 유지한다.
* typed error에 SQL 원문이나 catalog option을 포함하지 않는다.

## 8. 결과 및 오류 모델

Operation이 없는 `ConfigureSession` 경로에 맞는 별도 결과 타입을 사용한다.

```go
type SessionSetupStepResult struct {
    Index          int
    Kind           SessionSetupStepKind
    Target         Identifier
    Applied        bool
    Verified       bool
    OutcomeUnknown bool
}

type SessionSetupResult struct {
    SessionHandle              string
    Steps                      []SessionSetupStepResult
    FailedIndex                int
    Complete                   bool
    SessionClosed              bool
    PersistentChangesMayRemain bool
}
```

실패 오류에는 다음 정보만 포함한다.

* 원인 오류
* cleanup 오류
* session handle
* 실패 step index
* step kind
* 안전하게 노출할 수 있는 target
* 결과 불명확 여부

SQL과 option 원문은 저장하지 않는다.

`errors.Is`와 `errors.As`가 원인 및 cleanup 오류를 탐색할 수 있도록 기존 `ExecutionError`, `RecipeReplayError` 스타일을 따른다.

## 9. 비밀정보 보호

catalog option에는 password, token, access key 등이 포함될 수 있다.

다음을 반드시 구현한다.

* 기본 민감 key:

  * `password`
  * `secret`
  * `token`
  * `access-key`
  * `secret-key`
  * 대소문자 및 `_`, `-`, `.` 구분자 변형
* 호출자가 `SensitiveKeys`로 추가 민감 key를 지정할 수 있게 한다.
* SQL 원문과 option map을 result, typed error, Observer 및 lifecycle event에 포함하지 않는다.
* Gateway 오류 응답에 SQL 또는 비밀번호가 반사되면 민감 값을 `********`로 치환한다.
* 결과 구조체에 원본 option map을 보관하지 않는다.
* 빈 secret, 동일한 secret 값, 작은따옴표가 포함된 secret도 안전하게 처리한다.

## 10. Metadata 검증

`SessionSetupOptions`에 선택적인 생성 결과 검증 옵션을 제공한다.

기존 helper를 재사용한다.

* catalog: `ListCatalogs`
* database: `ListDatabases`
* table: `ListTables`
* table schema: `DescribeTable`

검증 기준:

* DDL 요청 성공과 metadata 검증 성공을 별도로 기록한다.
* 검증 실패만으로 DDL이 적용되지 않았다고 단정하지 않는다.
* metadata 조회는 read-only이므로 기존 safe retry 정책을 사용할 수 있다.
* Flink가 반환한 객체명의 대소문자를 임의로 변경하지 않는다.
* 같은 이름을 가진 table과 view의 충돌 가능성을 고려한다.

## 11. 실패 처리와 cleanup

* `OpenSessionWithSetup`은 setup 실패 시 기본적으로 생성한 session을 닫는다.
* 필요하면 `KeepSessionOnFailure` 옵션을 제공한다.
* 취소된 caller context가 아닌 제한된 background context로 cleanup을 수행한다.
* session 종료를 rollback으로 표현하지 않는다.
* HiveCatalog 등 영속 catalog에 생성된 객체는 session 종료 후에도 남을 수 있다.
* 자동 `DROP` rollback은 구현하지 않는다.
* 일부 step이 성공한 후 실패하면 `PersistentChangesMayRemain=true`를 반환한다.

## 12. API 버전 처리

* `ConfigureSession`은 REST API v2 이상에서만 지원된다.
* v1에서 `ApplySessionSetup` 또는 `OpenSessionWithSetup`을 호출하면 네트워크 요청 전에 `ErrUnsupportedAPI`를 반환한다.
* 기존 `Capabilities.ConfigureSession`을 활용한다.
* 알 수 없는 미래 API 버전을 임의로 지원한다고 판단하지 않는다.

## 13. 권장 파일 구성

신규 파일 후보:

```text
flinksqlgateway/session_setup.go
flinksqlgateway/session_setup_compile.go
flinksqlgateway/session_setup_test.go
flinksqlgateway/session_setup_integration_test.go
```

기존 파일 변경 후보:

```text
flinksqlgateway/session.go
flinksqlgateway/errors.go
flinksqlgateway/metadata.go
flinksqlgateway/client.go
README.md
docs/design.md
docs/build.md
docs/releases/v0.1.5.md
VERSION
```

현재 저장소 구조와 코드 스타일을 먼저 분석하고, 불필요하게 파일을 세분화하지 않는다.

## 14. 필수 테스트

`httptest.Server` 기반 단위 테스트에 최소 다음을 포함한다.

1. catalog → database → table → `USE CATALOG` → `USE database` 순서
2. setup SQL이 `/configure-session`으로 전송되는지 확인
3. v1에서 네트워크 호출 전 거부
4. option key 정렬 및 결정적인 SQL 생성
5. backtick과 작은따옴표 escaping
6. 중간 실패 시 후속 step 미실행
7. 성공 step과 실패 index 정확성
8. session cleanup과 `KeepSessionOnFailure`
9. setup POST 무재시도
10. 요청 미전송과 결과 불명확 오류 구분
11. 서버 반사 오류의 secret redaction
12. Error, Observer, lifecycle event의 SQL 및 secret 비노출
13. metadata 검증 성공과 실패
14. 검증 실패 시 DDL 미적용으로 단정하지 않음
15. 영속 변경 잔존 가능성 표시
16. 기존 `SessionRecipe` 회귀 방지
17. 기존 외부 `Client` 구현 호환성
18. context 취소와 cleanup context 분리
19. empty plan과 invalid identifier 사전 검증
20. 동시 호출 및 race 안전성

## 15. 문서 및 버전 변경

기능 구현 완료 후 다음을 `0.1.5`로 정합화한다.

* `VERSION`
* README의 현재 릴리스 및 빌드 산출물 예시
* `docs/build.md`의 버전 및 tag 예시
* `docs/releases/v0.1.5.md`
* 필요한 build metadata 문서

Flink 지원 버전은 `1.20.4`로 유지한다.

README에는 다음 예제를 추가한다.

* 기존 session에 `ApplySessionSetup` 적용
* `OpenSessionWithSetup` 사용
* catalog option과 secret 처리
* metadata 검증 활성화
* partial failure 처리
* `ErrConfigurationOutcomeUnknown` 처리
* in-memory catalog와 persistent catalog의 수명 차이

## 16. 검증 명령

저장소의 기본 빌드 절차를 우선 사용한다.

```powershell
.\build.ps1
```

최소 검증:

```text
go test ./...
go vet ./...
go test -race ./...
go test -tags=integration ./...
```

통합 테스트 환경변수가 없어 skip된 경우 결과에 명시한다. Race detector가 C compiler 부재로 실행되지 못한 경우에도 원인을 기록한다.

## 17. 완료 조건

다음 조건을 모두 만족해야 한다.

* 신규 setup API가 `ConfigureSession`을 사용한다.
* 기존 `SessionRecipe` 동작이 유지된다.
* catalog, database, table과 current scope를 선언형으로 설정할 수 있다.
* 실행 전 전체 plan 검증이 수행된다.
* 부분 성공, 실패 위치 및 결과 불명확 상태를 확인할 수 있다.
* setup POST를 자동 재시도하지 않는다.
* 자동 rollback을 수행하지 않는다.
* SQL, password, token과 secret이 오류 및 관측 이벤트에 노출되지 않는다.
* 기존 `Client` 인터페이스 구현 호환성이 유지된다.
* metadata 기반 선택적 검증을 지원한다.
* 테스트와 vet가 통과한다.
* 버전 관련 파일이 `0.1.5`로 정합화된다.
* 모든 변경이 원격 `0.1.5` 브랜치에 commit 및 push된다.

## 18. 최종 보고 형식

개발 완료 후 다음 항목을 보고한다.

1. 추가한 공개 API
2. Session Setup 실행 흐름
3. 결과 불명확 오류 처리
4. secret redaction 방식
5. 변경 파일 목록
6. 실행한 테스트와 결과
7. 실행하지 못한 테스트와 이유
8. 원격 브랜치명
9. 최종 commit SHA
10. 남은 제한사항과 후속 권고

## Git 반영 결과

원격 저장소에 **`0.1.5` 브랜치**를 생성했습니다. 개발요청 문서에는 해당 브랜치 사용, `ConfigureSession` 기반 설계, 오류·보안·테스트·커밋 기준까지 포함되어 있습니다.

`VERSION`은 `0.1.4`에서 **`0.1.5`**로 변경했습니다.

반영된 커밋은 다음 두 개입니다.

* `4194d8d5c84c52c0ed8f09b5e5842c62e51c89ab` — 개발요청 프롬프트 추가
* `7294ab078a8ea701d5ec21ab7d10a57de8a13b89` — 개발 버전 `0.1.5` 반영

현재 브랜치에는 **개발요청 문서와 버전 변경만 반영되어 있으며, Session Setup 기능 코드는 아직 구현되지 않은 상태**입니다. 실제 코드 구현은 이 프롬프트를 기준으로 `0.1.5` 브랜치에서 진행해야 합니다.
