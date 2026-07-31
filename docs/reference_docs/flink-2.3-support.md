# Flink 2.3.0 SQL Gateway 및 Flink SQL 기능 지원 확장 요청

다음 GitHub 저장소를 분석하고, 기존 설계 원칙과 하위 호환성을 유지하면서 Apache Flink 2.3.0 SQL Gateway REST API 및 Flink 2.3 SQL 기능을 정식으로 지원하도록 코드를 수정하라.

## 대상 저장소

`https://github.com/heartblast/flink-sql-go`

## 반드시 참고할 공식 자료

아래 문서와 저장소의 현재 코드를 함께 비교하라.

* Flink 2.3 SQL Gateway REST 문서
  `https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/interfaces/sql-gateway/rest/`
* Flink 2.3 SQL Gateway OpenAPI v1
  `https://nightlies.apache.org/flink/flink-docs-release-2.3/generated/rest_v1_sql_gateway.yml`
* Flink 2.3 SQL Gateway OpenAPI v2
  `https://nightlies.apache.org/flink/flink-docs-release-2.3/generated/rest_v2_sql_gateway.yml`
* Flink 2.3 SQL Gateway OpenAPI v3
  `https://nightlies.apache.org/flink/flink-docs-release-2.3/generated/rest_v3_sql_gateway.yml`
* Flink 2.3 SQL Gateway OpenAPI v4
  `https://nightlies.apache.org/flink/flink-docs-release-2.3/generated/rest_v4_sql_gateway.yml`
* Flink 2.3.0 릴리스 안내
  `https://flink.apache.org/2026/06/25/apache-flink-2.3.0-release-announcement/`

Nightly 문서만 신뢰하지 말고 Apache Flink `release-2.3.0` 소스의 실제 RequestBody, serializer, handler, MessageHeaders 구현도 확인하라. OpenAPI와 실제 serializer가 다를 경우 저장소의 기존 원칙에 따라 실제 Flink 2.3.0 구현을 우선하되, 차이는 문서와 테스트에 기록하라.

---

# 1. 개발 목표

현재 `flink-sql-go`가 보유한 Flink 1.20.x 및 2.0.x~2.3.x compatibility profile 구조를 유지하면서 다음 기능을 구현한다.

1. Flink 2.3.0 SQL Gateway REST v1~v4의 정확한 endpoint capability 지원
2. REST API v3 Materialized Table refresh 공개 API 구현
3. REST API v4 Application Mode SQL Script 배포 공개 API 구현
4. Flink 2.3에서 추가된 SQL 문법을 변경 없이 전송할 수 있는지 검증
5. Flink 2.3.0 실제 SQL Gateway 통합 테스트 환경과 검증 절차 추가
6. 호환성 manifest, README, 설계 문서 및 예제 갱신

단순히 `2.3` 버전 문자열이나 capability flag만 추가하지 말고 실제 API 호출, wire DTO, 오류 처리, 보안 통제, 단위 테스트 및 통합 테스트까지 완성하라.

---

# 2. 기존 설계 원칙 유지

다음 프로젝트 원칙을 변경하지 않는다.

* 공개 import 경로는 `github.com/heartblast/flink-sql-go/flinksqlgateway`로 유지한다.
* JDBC, JVM, GraalVM, CGO, `database/sql`을 도입하지 않는다.
* Go 표준 `net/http` 기반 구조와 런타임 외부 의존성 없음 원칙을 유지한다.
* `NewClient`는 네트워크를 호출하지 않아야 한다.
* `/info`와 `/api_versions`를 이용한 lazy compatibility 감지를 유지한다.
* SQL, Script 본문, 인증 헤더와 민감한 실행 설정을 로그나 Observer에 노출하지 않는다.
* 비멱등 POST 요청은 자동 재시도하지 않는다.
* Flink SQL parser를 새로 구현하지 않는다.
* SQL Operation과 JobManager Job 또는 Application lifecycle을 암묵적으로 결합하지 않는다.
* 기존 `Client` interface 구현자의 컴파일 호환성을 가능하면 유지한다.
* Flink 버전별 별도 Go package, module, build tag 또는 장기 브랜치를 만들지 않는다.

기존 공개 `Client` interface에 메서드를 직접 추가하여 외부 mock과 wrapper를 깨지 말고, 기존 `SessionSetupExecutor`, `CompatibilityProvider`와 같은 별도 capability interface 방식을 우선 사용하라.

---

# 3. 먼저 수행할 코드 분석

코드를 수정하기 전에 다음 파일과 관련 테스트를 분석하라.

* `compatibility.yaml`
* `flinksqlgateway/compatibility.go`
* `flinksqlgateway/capability.go`
* `flinksqlgateway/client.go`
* `flinksqlgateway/types.go`
* `flinksqlgateway/operation.go`
* `flinksqlgateway/session.go`
* `flinksqlgateway/transport.go`
* `flinksqlgateway/security.go`
* `flinksqlgateway/errors.go`
* `flinksqlgateway/observer.go`
* `docs/design.md`
* `docs/compatibility.md`
* integration build tag가 적용된 테스트
* compatibility manifest contract test
* `build.ps1`

분석 후 다음 사항을 명확히 확인하라.

* Flink 2.3 profile은 이미 존재하지만 실제 검증 상태가 `experimental`인지
* `MaterializedTable`, `DeployScript` capability가 실제 endpoint 존재 여부가 아니라 API 버전 숫자 비교로 계산되고 있는지
* Materialized Table 및 Deploy Script용 공개 helper가 아직 없는지
* `executionTimeout`이 Flink 2.3 v1~v4에서 어떤 JSON wire 형태로 전송되는지
* 현재 typed error와 Observer 패턴을 신규 API에 재사용할 수 있는지

분석만 보고하고 종료하지 말고, 확인한 내용을 기준으로 실제 구현을 진행하라.

---

# 4. REST API capability 모델 수정

현재와 같이 다음 조건으로 capability를 계산해서는 안 된다.

```go
MaterializedTable = versionNumber >= 3
DeployScript      = versionNumber >= 4
```

Flink SQL Gateway REST API 버전은 반드시 누적 기능이라고 볼 수 없다. Flink 2.3 기준으로 endpoint path 존재 여부에 따라 정확한 capability matrix를 구성하라.

최소한 다음 matrix를 만족해야 한다.

```text
v1
- ConfigureSession: false
- CompleteStatement: false
- RowFormat: false
- MaterializedTable: false
- DeployScript: false

v2
- ConfigureSession: true
- CompleteStatement: true
- RowFormat: true
- MaterializedTable: false
- DeployScript: false

v3
- ConfigureSession: true
- CompleteStatement: true
- RowFormat: true
- MaterializedTable: true
- DeployScript: false

v4
- ConfigureSession: true
- CompleteStatement: true
- RowFormat: true
- MaterializedTable: false
- DeployScript: true
```

`WireExecutionTimeout`은 release와 각 OpenAPI 명세를 기준으로 별도로 계산한다. Flink 2.3의 v1에도 `executionTimeout`이 존재하므로 단순히 v2 이상으로 제한하지 말라.

권장 구현 방향은 다음과 같다.

* release capability와 protocol endpoint capability를 분리한다.
* API 버전 숫자 대소 비교 대신 정확한 API별 descriptor 또는 map을 사용한다.
* `compatibility.yaml`을 schema version 2 또는 그에 준하는 구조로 개선한다.
* manifest와 Go registry가 같은 내용을 중복 수작업으로 관리하지 않도록 contract test를 강화한다.
* 공개 `Capabilities` 구조는 가능한 한 하위 호환성을 유지한다.
* 알려지지 않은 API 버전은 지원 기능을 추측하지 않고 모두 보수적으로 비활성화한다.

`APIVersionStable`은 Flink 2.3에서 계속 v3를 선택하고, `APIVersionHighest`는 v4를 선택하도록 한다.

다만 선택된 API 버전과 다른 endpoint를 호출하기 위해 내부적으로 API 버전을 몰래 변경하거나 fallback하지 않는다.

예를 들어 다음과 같이 처리한다.

* v3 client에서 Deploy Script 호출: 네트워크 호출 전 typed unsupported capability 오류
* v4 client에서 Materialized Table refresh 호출: 네트워크 호출 전 typed unsupported capability 오류
* 두 기능이 모두 필요한 사용자는 v3 client와 v4 client를 명시적으로 구성할 수 있도록 문서화

---

# 5. Materialized Table Refresh API 구현

다음 endpoint를 구현한다.

```text
POST /v3/sessions/{session_handle}/materialized-tables/{identifier}/refresh
```

요청 및 응답 모델은 Flink 2.3.0 실제 소스와 OpenAPI를 확인하여 구현한다.

공개 API는 다음과 유사하게 설계하되, 저장소의 기존 naming 규칙을 우선한다.

```go
type MaterializedTableRefresher interface {
    RefreshMaterializedTable(
        ctx context.Context,
        sessionHandle string,
        identifier string,
        req RefreshMaterializedTableRequest,
    ) (*Operation, error)
}

type RefreshMaterializedTableRequest struct {
    Periodic         bool
    ScheduleTime     string
    DynamicOptions   map[string]string
    StaticPartitions map[string]string
    ExecutionConfig  map[string]string
}
```

구현 요구사항:

* session handle과 identifier를 검증한다.
* identifier는 fully qualified materialized table identifier를 받을 수 있어야 한다.
* REST path parameter와 SQL identifier quoting을 혼동하지 않는다.
* path parameter를 정확히 한 번만 URL escape한다.
* 빈 identifier와 제어문자가 포함된 identifier는 거부한다.
* 응답의 `operationHandle`을 기존 validator로 검증한다.
* 반환값은 기존 `Operation` 모델을 재사용한다.
* 반환된 Operation은 기존 status, cancel, close API로 관리할 수 있어야 한다.
* POST 요청을 자동 재시도하지 않는다.
* 응답 유실 또는 408, 429, 5xx 등 실행 여부가 불명확한 경우 typed outcome-unknown 오류를 제공한다.
* request map을 호출자와 내부에서 공유하지 않도록 필요한 경우 복사한다.
* DynamicOptions와 ExecutionConfig의 비밀값이 오류나 Observer에 노출되지 않게 한다.

OpenAPI에는 `isPeriodic`과 `periodic`이 동시에 나타날 수 있으므로 두 필드를 무조건 함께 보내지 않는다.

Apache Flink 2.3.0의 실제 `RefreshMaterializedTableRequestBody` Jackson annotation과 serializer를 확인하여 canonical JSON key 하나만 전송하고, 이에 대한 wire serialization 테스트를 작성하라. OpenAPI와 실제 소스의 차이가 있다면 문서에 기록한다.

Materialized Table refresh helper는 release 이름을 `2.3`으로 하드코딩하지 말고, 선택된 profile과 API v3 capability가 지원하는 모든 release에서 사용할 수 있도록 구현한다.

---

# 6. Deploy Script API 구현

다음 endpoint를 구현한다.

```text
POST /v4/sessions/{session_handle}/scripts
```

공개 API는 다음과 유사하게 설계한다.

```go
type ScriptDeployer interface {
    DeployScript(
        ctx context.Context,
        sessionHandle string,
        req DeployScriptRequest,
    ) (*ScriptDeployment, error)
}

type DeployScriptRequest struct {
    Script          string
    ScriptURI       string
    ExecutionConfig map[string]string
}

type ScriptDeployment struct {
    ClusterID string
}
```

구현 요구사항:

* `Script`와 `ScriptURI` 중 정확히 하나만 입력하도록 사전 검증한다.
* 둘 다 비어 있거나 둘 다 지정되면 네트워크 호출 전에 오류를 반환한다.
* Script 본문은 공백만으로 구성될 수 없다.
* ScriptURI는 Flink가 지원하는 URI를 그대로 전달하되 userinfo, query string 또는 credential이 로그에 노출되지 않게 한다.
* `clusterID`는 opaque identifier로 취급한다.
* `clusterID`를 UUID나 Flink JobID 형식으로 가정하지 않는다.
* 빈 `clusterID`가 반환되면 배포 성공 여부가 불명확한 typed error로 처리한다.
* POST 요청은 절대 자동 재시도하지 않는다.
* Script 본문, ScriptURI 전체, ExecutionConfig와 민감정보를 error, Observation, debug 문자열에 저장하지 않는다.
* API v4 capability가 없으면 네트워크 호출 전에 `ErrUnsupportedCapability` 계열 typed error를 반환한다.
* Deploy Script 성공이 개별 Job 생성이나 JobManager lifecycle 추적을 의미한다고 추측하지 않는다.
* `flinkrest`와 자동 결합하지 않는다.

기존 `ExecutionOutcomeUnknownError` 구현을 무리하게 재사용하여 의미를 혼동하지 말고, 필요하면 다음과 같이 기능별 오류를 분리한다.

```text
ErrMaterializedTableRefreshOutcomeUnknown
ErrScriptDeploymentOutcomeUnknown
```

공통 내부 로직을 재사용하되 호출자가 `errors.Is`와 `errors.As`로 각 오류를 구분할 수 있어야 한다.

---

# 7. Flink 2.3 SQL 문법 호환성

이 라이브러리는 SQL parser가 아니라 SQL Gateway REST client이다. 따라서 Flink 2.3 SQL 문법을 자체적으로 해석하거나 재작성하지 말고 원문을 변경 없이 전송해야 한다.

다음 Flink 2.3 SQL 기능을 대표적인 pass-through 또는 integration test 대상으로 추가하라.

* `FROM_CHANGELOG`
* `TO_CHANGELOG`
* `CREATE MATERIALIZED TABLE`의 명시적 컬럼, watermark 및 primary key
* `ALTER MATERIALIZED TABLE`
* Materialized Table refresh 시작 위치를 제어하는 `START_MODE`
* `INSERT ... ON CONFLICT DO NOTHING`
* `INSERT ... ON CONFLICT DO ERROR`
* `INSERT ... ON CONFLICT DO DEDUPLICATE`
* `CREATE FUNCTION ... USING ARTIFACT`
* Process Table Function의 table argument `ORDER BY`

모든 문법에 대해 별도 Go builder를 만들 필요는 없다.

다음 사항만 보장한다.

* `ExecuteStatementRequest.Statement`를 변경 없이 전송
* validator가 명시적으로 거부하지 않는 한 신규 SQL 키워드를 허용
* SQL 종류를 정규식이나 prefix로 추측하여 차단하지 않음
* 세미콜론과 여러 줄 SQL을 손상시키지 않음
* SQL 원문을 오류, Observer 또는 로그에 노출하지 않음

실제 실행에 catalog, connector 또는 test fixture가 필요한 SQL은 `integration` build tag와 환경변수로 분리하고, 기본 단위 테스트에서는 request body가 정확히 보존되는지를 검증하라.

---

# 8. 단위 테스트

`httptest.Server` 기반으로 최소한 다음 테스트를 추가한다.

## Capability 테스트

* Flink 2.3 + Stable → v3
* Flink 2.3 + Highest → v4
* v3에서 Materialized Table true, Deploy Script false
* v4에서 Materialized Table false, Deploy Script true
* v4에서 Materialized Table helper 호출 시 HTTP 요청이 발생하지 않음
* v3에서 Deploy Script helper 호출 시 HTTP 요청이 발생하지 않음
* unknown API version에서는 신규 기능을 지원한다고 추측하지 않음

## Materialized Table 테스트

* 정확한 v3 endpoint path
* session handle과 identifier URL escape
* canonical periodic JSON field
* nil map과 empty map 직렬화 차이
* 응답 operationHandle 검증
* invalid handle과 빈 identifier 사전 차단
* 성공, 4xx, 408, 429, 5xx, transport failure
* 응답 유실 시 outcome-unknown 분류
* 자동 재시도가 발생하지 않음
* 오류와 Observer에 option 값이 노출되지 않음

## Deploy Script 테스트

* 정확한 v4 endpoint path
* inline script 요청
* scriptUri 요청
* 둘 다 입력하거나 둘 다 비운 요청 차단
* executionConfig 직렬화
* clusterID 응답 처리
* 빈 clusterID 처리
* 자동 재시도가 발생하지 않음
* Script 및 URI credential redaction
* Observer event에 Script 본문이 포함되지 않음

## SQL pass-through 테스트

다음과 같은 SQL이 request body에 byte-for-byte 또는 의미적으로 동일하게 포함되는지 검증한다.

```sql
INSERT INTO target_table
SELECT * FROM source_table
ON CONFLICT DO DEDUPLICATE;
```

```sql
CREATE FUNCTION my_func
AS 'com.example.MyFunction'
USING ARTIFACT 's3://bucket/my-function.jar';
```

SQL을 formatter로 변경하거나 keyword allow-list로 거부하지 않아야 한다.

---

# 9. Flink 2.3.0 실제 통합 테스트

기존 integration test 구조를 확장하여 다음 환경변수를 지원한다.

```text
FLINK_SQL_GATEWAY_URL
FLINK_TEST_VERSION=2.3.0
FLINK_TEST_RELEASE_LINE=2.3
FLINK_TEST_API_VERSION=v3 또는 v4
```

가능하면 동일한 Flink 2.3.0 Gateway에 대해 v3와 v4를 각각 실행할 수 있는 PowerShell 또는 Go 기반 matrix runner를 추가한다.

최소 live integration 검증 항목:

* `/info`가 2.3.0을 반환하는지
* `/api_versions`가 v1~v4를 광고하는지
* Auto mode가 `Flink23` profile을 선택하는지
* Stable policy가 v3를 선택하는지
* Highest policy가 v4를 선택하는지
* 세션 open/config 조회/heartbeat/close
* SQL 실행 및 결과 fetch
* v3 Materialized Table refresh happy path
* v4 Deploy Script happy path
* Operation status/cancel/close
* `executionTimeout` wire field
* JSON 및 PLAIN_TEXT 결과

Materialized Table과 Script 배포 happy path를 위한 환경이 기본 SQL Gateway만으로 구성되지 않는다면 별도 opt-in 환경변수와 fixture 문서를 제공한다.

실제 Flink 2.3.0 통합 테스트를 실행하지 않은 상태에서 다음 변경을 해서는 안 된다.

* `testedVersions`에 `2.3.0` 추가
* `ReleaseExperimental`을 `ReleaseSupported`로 변경
* README에서 “검증 완료”라고 표현

통합 테스트가 실제로 성공한 경우에만 `compatibility.yaml`과 Go registry의 `testedVersions`에 `2.3.0`을 추가하고 지원 상태 변경 여부를 프로젝트 정책에 따라 결정한다.

---

# 10. 보안 및 안정성 요구사항

신규 API에도 기존 transport와 보안 정책을 그대로 적용한다.

* 동일 origin 정책
* response 크기 제한
* request timeout
* context 취소
* TLS 기본 검증 유지
* redirect 제한
* 제어문자 제거
* 서버 오류 메시지 길이 제한
* 민감정보 redaction
* Observer 동시 실행 제한
* client close 이후 신규 요청 차단

다음 정보는 어떤 오류나 Observer에도 포함하지 않는다.

* SQL 원문
* Script 원문
* 인증 헤더
* ScriptURI query string과 userinfo
* ExecutionConfig 전체
* DynamicOptions 전체
* access key, secret key, password, token 등 민감값

비멱등 요청은 response를 받지 못했더라도 자동으로 다시 호출하지 않는다.

---

# 11. 문서 갱신

다음 문서를 갱신한다.

* `README.md`
* `docs/design.md`
* `docs/compatibility.md`
* `docs/build.md`
* 필요한 경우 신규 `docs/flink-2.3.md`
* `compatibility.yaml`

README 지원 표에는 단순히 “v1~v4 지원”이라고만 표시하지 말고 다음 내용을 구분한다.

* Flink release line
* 실제 검증한 patch version
* Stable API
* 각 API 버전별 Materialized Table/Deploy Script 지원
* `executionTimeout` wire 지원
* 지원 상태

사용 예제를 추가한다.

* Flink 2.3 Stable/v3 client 생성
* Materialized Table refresh
* Flink 2.3 Highest 또는 Explicit v4 client 생성
* inline Script 배포
* ScriptURI 배포
* 두 기능을 사용할 때 v3와 v4 client를 명시적으로 분리하는 예제

문서에서 다음 사실을 명확히 설명한다.

* REST API 버전은 반드시 누적 기능이 아니다.
* Stable v3와 Highest v4의 기능 범위가 다르다.
* 라이브러리는 Flink SQL parser가 아니며 SQL을 pass-through한다.
* Deploy Script의 clusterID는 JobID와 같은 개념으로 가정하지 않는다.
* SQL Operation 취소와 Flink Job/Application 취소는 자동 결합되지 않는다.

---

# 12. 빌드 및 검증

수정 후 다음 명령을 실행한다.

```powershell
go test ./...
go vet ./...
go test -race ./...
.\build.ps1
```

실제 Gateway가 준비된 경우 다음 테스트도 실행한다.

```powershell
go test -tags=integration ./...
```

다음 조건을 모두 만족해야 완료로 판단한다.

* 기존 공개 API의 불필요한 breaking change 없음
* 기존 Flink 1.20.4 테스트 통과
* Flink 2.0~2.2 compatibility profile 회귀 없음
* Flink 2.3 v1~v4 capability contract 테스트 통과
* v3 Materialized Table helper 구현 완료
* v4 Deploy Script helper 구현 완료
* 비멱등 요청 자동 재시도 없음
* 신규 API typed error 제공
* secret redaction 테스트 통과
* `go test ./...` 및 `go vet ./...` 통과
* 문서와 compatibility manifest가 실제 구현과 일치

---

# 13. 최종 보고 형식

작업 완료 후 다음 형식으로 결과를 보고하라.

1. 기존 구현에서 발견한 문제
2. Flink 2.3 OpenAPI와 현재 코드의 차이
3. 선택한 capability 모델과 설계 이유
4. 변경한 파일 목록
5. 추가한 공개 API
6. 추가한 단위 테스트
7. 실행한 통합 테스트와 실제 결과
8. Flink 1.20 및 2.0~2.2 회귀 검증 결과
9. 아직 검증하지 못한 항목
10. breaking change 여부
11. 운영자가 v3와 v4를 선택하는 방법

분석이나 코드 예시만 제시하지 말고 실제 저장소 파일을 수정하고 테스트까지 수행하라. 테스트를 실행하지 못한 항목은 성공했다고 표현하지 말고 그 이유와 재현 가능한 실행 명령을 남겨라.
