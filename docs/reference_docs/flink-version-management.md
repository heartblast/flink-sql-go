아래 프롬프트를 그대로 Codex 또는 개발 에이전트에 입력하면 됩니다.

다음 GitHub 저장소를 분석한 뒤, Apache Flink 버전별 SQL Gateway 호환성을 하나의 Go 모듈에서 관리할 수 있도록 소스 구조, 호환성 모델, 테스트, 빌드 및 릴리스 체계를 개선해줘.

대상 저장소:

```text
https://github.com/heartblast/flink-sql-go
```

## 1. 개선 배경

현재 프로젝트는 Apache Flink 1.20.4 SQL Gateway를 기준으로 개발되어 있으며, 다음과 같이 단일 Flink 버전을 전제로 구성되어 있다.

* 루트 `FLINK_VERSION` 파일에 지원 Flink 버전 하나를 기록
* `flinksqlgateway.SupportedFlinkVersion` 상수에 동일 버전을 중복 기록
* `DefaultAPIVersion`과 일부 요청 처리 로직이 Flink 1.20.4 동작을 기준으로 고정
* 빌드 산출물 파일명에 단일 Flink patch 버전이 포함
* `executionTimeout`과 같이 Flink 1.20.4에서만 필요한 예외 처리가 공통 클라이언트 로직에 포함
* 릴리스 하나가 특정 Flink 버전 하나만 지원하는 것처럼 표현됨

앞으로 다음 Flink 릴리스 계열을 하나의 프로젝트에서 지원하려고 한다.

```text
Flink 1.20.x
Flink 2.0.x
Flink 2.1.x
Flink 2.2.x
Flink 2.3.x
```

향후에는 Flink 2.3.x 등 새로운 릴리스 계열도 쉽게 추가할 수 있어야 한다.

## 2. 핵심 설계 원칙

다음 원칙을 반드시 적용해줘.

### 2.1 단일 저장소와 단일 Go 모듈 유지

다음 공개 모듈 경로와 import 경로는 변경하지 않는다.

```text
github.com/heartblast/flink-sql-go
github.com/heartblast/flink-sql-go/flinksqlgateway
github.com/heartblast/flink-sql-go/flinkrest
```

Flink 버전별로 다음과 같은 별도 공개 모듈이나 공개 패키지를 만들지 않는다.

```text
flinksqlgateway120
flinksqlgateway20
flinksqlgateway21
github.com/heartblast/flink-sql-go/flink120
```

사용자는 Flink 버전과 관계없이 기존 공개 API를 동일하게 사용해야 한다.

### 2.2 Flink 버전별 장기 브랜치를 만들지 않음

다음과 같은 장기 유지 브랜치 구조는 사용하지 않는다.

```text
flink-1.20
flink-2.0
flink-2.1
```

공통 개발은 `main`에서 진행하고, 필요한 경우 다음과 같은 단기 기능 브랜치를 사용한다.

```text
feature/flink-2.0-support
feature/flink-2.1-support
feature/sql-gateway-v4
```

### 2.3 Flink 릴리스와 REST API 버전을 분리

호환성은 다음 두 계층으로 분리한다.

1. Flink 릴리스 계열

   * 1.20.x
   * 2.0.x
   * 2.1.x

2. SQL Gateway REST API 버전

   * v1
   * v2
   * v3
   * v4

Flink 버전마다 전체 클라이언트를 복제하지 말고 다음과 같이 구성한다.

```text
공통 공개 Client API
    ├── REST protocol 구현
    │     ├── v1
    │     ├── v2
    │     ├── v3
    │     └── v4
    │
    └── Flink compatibility profile
          ├── 1.20.x
          ├── 2.0.x
          └── 2.1.x
```

## 3. 목표 소스 구조

현재 저장소 구조를 분석한 뒤, 필요하면 다음과 유사한 구조로 개선해줘.

```text
flink-sql-go/
├── flinksqlgateway/
│   ├── client.go
│   ├── config.go
│   ├── session.go
│   ├── operation.go
│   ├── result.go
│   ├── capabilities.go
│   ├── compatibility.go
│   └── ...
│
├── flinkrest/
│
├── internal/
│   ├── protocol/
│   │   ├── common/
│   │   ├── v1/
│   │   ├── v2/
│   │   ├── v3/
│   │   └── v4/
│   │
│   ├── compatibility/
│   │   ├── registry.go
│   │   ├── flink120.go
│   │   ├── flink20.go
│   │   └── flink21.go
│   │
│   └── wire/
│       ├── request_encoder.go
│       └── response_decoder.go
│
├── integration/
│   ├── gateway_contract_test.go
│   └── fixtures/
│       ├── flink-1.20/
│       ├── flink-2.0/
│       └── flink-2.1/
│
├── testdata/
│   └── openapi/
│       ├── flink-1.20/
│       ├── flink-2.0/
│       └── flink-2.1/
│
├── docs/
│   ├── compatibility.md
│   ├── release-policy.md
│   └── releases/
│
├── compatibility.yaml
├── VERSION
├── go.mod
└── build.ps1
```

현재 코드 규모와 구조를 고려해 불필요하게 패키지를 과도하게 분리하지는 말고, 실제 버전 차이가 존재하는 경계만 분리해줘.

## 4. 호환성 프로파일 도입

Flink 릴리스별로 다음 정보를 관리할 수 있는 호환성 모델을 설계해줘.

```go
type ReleaseLine string

const (
    Flink120 ReleaseLine = "1.20"
    Flink20  ReleaseLine = "2.0"
    Flink21  ReleaseLine = "2.1"
)
```

호환성 정보에는 최소한 다음 항목이 포함되어야 한다.

```go
type Capabilities struct {
    SupportedAPIVersions []string
    DefaultAPIVersion    string

    ConfigureSession  bool
    CompleteStatement bool
    MaterializedTable bool
    DeployScript      bool

    WireExecutionTimeout bool
}
```

필요하면 다음 항목도 추가해도 된다.

```text
지원 endpoint
지원 row format
지원 operation 종류
지원 result field
요청 DTO 차이
응답 DTO 차이
알려진 버전별 예외
deprecated 기능
experimental 기능
```

호환성 프로파일은 내부 구현으로 관리하되, 사용자가 현재 선택되거나 감지된 호환성 정보를 조회할 수 있는 읽기 전용 공개 API를 제공해줘.

예:

```go
type CompatibilityInfo struct {
    FlinkVersion       string
    ReleaseLine        string
    APIVersion         string
    Capabilities       Capabilities
    DetectionSource    string
}
```

외부 호출자가 내부 slice나 map을 변경하지 못하도록 반드시 복사본을 반환해줘.

## 5. 자동 감지 및 명시적 설정

`Config`에 다음과 유사한 설정을 추가해줘.

```go
type CompatibilityMode string

const (
    CompatibilityAuto     CompatibilityMode = "auto"
    CompatibilityFlink120 CompatibilityMode = "flink-1.20"
    CompatibilityFlink20  CompatibilityMode = "flink-2.0"
    CompatibilityFlink21  CompatibilityMode = "flink-2.1"
)

type APIVersionPolicy string

const (
    APIVersionStable   APIVersionPolicy = "stable"
    APIVersionHighest  APIVersionPolicy = "highest"
    APIVersionExplicit APIVersionPolicy = "explicit"
)
```

`Config` 예시:

```go
type Config struct {
    BaseURL string

    CompatibilityMode CompatibilityMode
    APIVersionPolicy  APIVersionPolicy
    APIVersion        string

    // 기존 설정 유지
}
```

기본 동작은 다음과 같이 한다.

```text
CompatibilityMode = auto
APIVersionPolicy = stable
```

자동 감지 과정은 다음 순서를 기준으로 구현해줘.

```text
1. GET /info
2. Flink 제품 버전 파싱
3. GET /api_versions
4. Flink 릴리스 계열 호환성 프로파일 선택
5. 프로파일과 서버가 광고한 REST API 버전의 교집합 계산
6. API 버전 정책에 따라 사용할 REST API 버전 선택
7. 선택 결과와 capability를 클라이언트 내부에 저장
```

단, 기존 프로젝트의 lazy API version 검증 방식은 유지해야 한다.

`NewClient` 호출 시 네트워크 요청을 발생시키지 말고, 최초 versioned 요청이나 명시적 compatibility 검사 시점에 감지하도록 해줘.

## 6. API 버전 선택 정책

다음 동작을 명확하게 구현해줘.

### Stable

여러 버전이 지원되더라도 프로젝트에서 안정적으로 검증된 API 버전을 선택한다.

예:

```text
Flink 1.20.x → v3
Flink 2.0.x → 기본 v3
Flink 2.1.x → 기본 v3
```

### Highest

서버와 클라이언트가 공통으로 지원하는 가장 높은 REST API 버전을 선택한다.

예:

```text
서버 지원: v1, v2, v3, v4
클라이언트 지원: v1, v2, v3, v4
선택: v4
```

### Explicit

사용자가 지정한 `APIVersion`만 사용한다.

지원되지 않으면 fallback하지 말고 명시적인 typed error를 반환한다.

## 7. Flink 1.20 전용 예외 분리

현재 Flink 1.20.4에서는 SQL Gateway 요청의 양수 `executionTimeout`이 지원되지 않아 해당 필드를 wire request에 보내지 않고 client-side context timeout으로 처리하고 있다.

이와 같은 버전 전용 처리를 공통 실행 코드에 직접 하드코딩하지 말고 다음 capability 또는 quirk를 사용하도록 변경해줘.

```go
WireExecutionTimeout bool
```

예:

```go
if profile.Capabilities().WireExecutionTimeout {
    body.ExecutionTimeout = timeout.Milliseconds()
}
```

`false`인 경우에는 현재처럼 client-side context timeout만 적용해야 한다.

기존 Flink 1.20.4 동작은 변경되면 안 된다.

## 8. 기존 API 호환성

현재 공개 API 사용자의 코드가 가능한 한 깨지지 않도록 해줘.

특히 다음을 유지해야 한다.

```text
flinksqlgateway.NewClient
flinksqlgateway.Config
Client 인터페이스
GatewayClient
OpenSession
ExecuteStatement
ExecuteAndWait
StreamResults
ExecuteStream
ManagedSession
SerializedSession
SessionRecipe
Row 및 Decoder API
flinkrest 패키지
```

현재 존재하는 다음 상수는 즉시 제거하지 말고 Deprecated 처리해줘.

```go
SupportedFlinkVersion
```

대신 다음과 같은 새로운 API를 제공해줘.

```go
func SupportedFlinkVersions() []SupportedFlinkRelease
func CompatibilityMatrix() CompatibilityMatrix
```

기존 상수는 현재 호환성을 위해 `"1.20.4"` 값을 유지하되, 신규 코드에서는 사용하지 않도록 문서화해줘.

## 9. 호환성 설정 파일

기존의 단일 `FLINK_VERSION` 파일을 다음과 같은 `compatibility.yaml` 또는 동등한 구조로 교체해줘.

```yaml
schemaVersion: 1

defaultReleaseLine: "1.20"
defaultApiVersion: "v3"

supportedReleases:
  - releaseLine: "1.20"
    status: maintenance
    testedVersions:
      - "1.20.4"
    restApiVersions:
      - "v1"
      - "v2"
      - "v3"

  - releaseLine: "2.0"
    status: supported
    testedVersions: []
    restApiVersions:
      - "v1"
      - "v2"
      - "v3"
      - "v4"

  - releaseLine: "2.1"
    status: supported
    testedVersions: []
    restApiVersions:
      - "v1"
      - "v2"
      - "v3"
      - "v4"
```

단, 실제 통합 테스트를 완료하지 않은 버전은 `testedVersions`에 임의로 넣지 말아줘.

지원 상태는 최소한 다음과 같이 구분할 수 있어야 한다.

```text
planned
experimental
supported
maintenance
unsupported
```

또한 다음 두 개념을 구분해줘.

```text
release line 지원: Flink 2.0.x 지원
직접 검증 버전: Flink 2.0.3에서 실제 테스트 완료
```

설정 파일과 Go 코드에 호환성 정보가 중복되지 않도록 단일 원천을 정해줘.

가능하면 `compatibility.yaml`을 기준으로 Go 코드 또는 빌드 산출물을 생성하거나 검증하는 방식을 사용해줘.

런타임에 YAML 파서 의존성을 추가하는 방식은 피하고, 필요하면 빌드 시 코드 생성 또는 테스트 검증 방식을 사용해줘.

## 10. 테스트 개선

### 10.1 단위 테스트

버전별로 다음을 검증해줘.

```text
Flink 버전 파싱
release line 선택
지원하지 않는 버전 오류
API 버전 교집합 계산
Stable/Highest/Explicit 정책
capability 선택
executionTimeout wire 포함 여부
v1/v2/v3/v4 endpoint 선택
기존 1.20.4 요청 및 응답 회귀 테스트
```

### 10.2 Fixture 기반 contract test

실제 Flink 응답 형식을 버전별 fixture로 관리해줘.

```text
integration/fixtures/flink-1.20
integration/fixtures/flink-2.0
integration/fixtures/flink-2.1
```

최소한 다음 응답을 포함해줘.

```text
/info
/api_versions
session 생성
session config
statement 실행
operation status
NOT_READY
PAYLOAD
EOS
JSON row format
PLAIN_TEXT row format
operation cancel
operation close
session close
```

### 10.3 실제 Gateway 통합 테스트

기존 `integration` build tag와 `FLINK_SQL_GATEWAY_URL` 방식을 유지하면서 다음 환경변수를 추가하거나 동등한 방식을 제공해줘.

```text
FLINK_SQL_GATEWAY_URL
FLINK_TEST_VERSION
FLINK_TEST_RELEASE_LINE
FLINK_TEST_API_VERSION
```

예:

```powershell
$env:FLINK_SQL_GATEWAY_URL = 'http://localhost:8083'
$env:FLINK_TEST_VERSION = '1.20.4'
$env:FLINK_TEST_RELEASE_LINE = '1.20'

go test -tags=integration ./...
```

테스트 시작 시 실제 `/info`의 Flink 버전과 `FLINK_TEST_VERSION`이 다르면 명확하게 실패하도록 해줘.

## 11. CI 매트릭스

GitHub Actions가 존재하거나 추가할 수 있다면 다음과 같은 테스트 매트릭스를 구성해줘.

```yaml
strategy:
  fail-fast: false
  matrix:
    flink-line:
      - "1.20"
      - "2.0"
      - "2.1"
```

각 버전별 실제 Docker 이미지 또는 테스트 환경을 실행할 수 있는 경우 SQL Gateway를 구동해 contract test를 수행해줘.

CI 비용이나 안정성 문제로 모든 버전을 실제 구동하기 어렵다면 다음처럼 단계적으로 분리해도 된다.

```text
PR:
- 단위 테스트
- fixture contract test
- compile
- go vet
- govulncheck

main 또는 scheduled:
- Flink 1.20 실제 Gateway
- Flink 2.0 실제 Gateway
- Flink 2.1 실제 Gateway
```

검증하지 않은 버전을 CI 통과한 것처럼 문서화하지 말아줘.

## 12. 빌드 및 산출물 변경

현재 다음과 같이 단일 Flink 버전이 포함된 산출물 이름은 멀티 버전 지원 구조에 맞지 않는다.

```text
flink-sql-go-0.1.4-flink-1.20.4-source.zip
```

멀티 버전 지원 이후에는 다음과 같이 변경해줘.

```text
flink-sql-go-0.2.0-source.zip
flink-sql-go-0.2.0.build-info.json
flink-sql-go-0.2.0.compatibility.json
flink-sql-go-0.2.0.modules.txt
flink-sql-go-0.2.0.govulncheck.txt
flink-sql-go-0.2.0.govulncheck-modules.txt
flink-sql-go-0.2.0.coverage.out
flink-sql-go-0.2.0.sha256
```

가능하면 다음 산출물도 추가해줘.

```text
flink-sql-go-0.2.0.sbom.json
```

`build-info.json`은 최소한 다음 정보를 포함해야 한다.

```json
{
  "libraryVersion": "0.2.0",
  "commit": "...",
  "buildDate": "...",
  "dirty": false,
  "goVersion": "go1.26.5",
  "supportedFlinkReleaseLines": [
    "1.20",
    "2.0",
    "2.1"
  ],
  "securityGatePassed": true
}
```

`compatibility.json`에는 다음을 포함해줘.

```json
{
  "libraryVersion": "0.2.0",
  "defaultApiVersion": "v3",
  "supportedFlinkReleaseLines": [
    {
      "releaseLine": "1.20.x",
      "status": "maintenance",
      "testedVersions": ["1.20.4"],
      "apiVersions": ["v1", "v2", "v3"]
    },
    {
      "releaseLine": "2.0.x",
      "status": "supported",
      "testedVersions": [],
      "apiVersions": ["v1", "v2", "v3", "v4"]
    },
    {
      "releaseLine": "2.1.x",
      "status": "supported",
      "testedVersions": [],
      "apiVersions": ["v1", "v2", "v3", "v4"]
    }
  ]
}
```

## 13. 릴리스 정책

라이브러리 버전과 Flink 버전을 완전히 분리해줘.

다음 릴리스는 멀티 Flink 지원을 도입하는 기능 릴리스이므로 다음 버전을 사용한다.

```text
v0.2.0
```

Git 태그는 다음과 같이 생성되어야 한다.

```text
v0.2.0
```

다음과 같은 태그를 만들지 않는다.

```text
v0.2.0-flink-1.20
v0.2.0-flink-2.0
v0.2.0-flink-2.1
v1.20.4
v2.0.0
v2.1.0
```

`v2.0.0`은 Flink 2.0 지원을 뜻하는 것이 아니라 Go 모듈 자체의 major version 2를 의미하므로 사용하지 않는다.

릴리스 하나가 여러 Flink 릴리스 계열을 지원하는 구조로 만들어줘.

```text
flink-sql-go v0.2.0
    ├── Flink 1.20.x
    ├── Flink 2.0.x
    ├── Flink 2.1.x
    ├── Flink 2.2.x
    └── Flink 2.3.x
```

사용자는 다음과 같이 설치한다.

```bash
go get github.com/heartblast/flink-sql-go@v0.2.0
```

## 14. 릴리스 노트 작성

`docs/releases/v0.2.0.md`를 작성해줘.

제목 예:

```text
flink-sql-go v0.2.0 — Multi-Flink Support
```

다음 내용을 포함해야 한다.

```text
개요
지원 Flink 릴리스 계열
직접 검증한 patch 버전
지원 REST API 버전
주요 변경사항
기존 공개 API 호환성
동작 변경사항
업그레이드 방법
알려진 제한사항
검증 결과
릴리스 산출물
```

호환성 표 예:

| Flink 릴리스 | 지원 상태                     | 직접 검증 버전   | REST API   | 비고                        |
| --------- | ------------------------- | ---------- | ---------- | ------------------------- |
| 1.20.x    | maintenance 또는 supported  | 1.20.4     | v1, v2, v3 | wire executionTimeout 미사용 |
| 2.0.x     | supported 또는 experimental | 실제 검증값만 기재 | v1~v4      | v3 기본                     |
| 2.1.x     | supported 또는 experimental | 실제 검증값만 기재 | v1~v4      | v3 기본                     |
| 2.2.x     | supported 또는 experimental | 실제 검증값만 기재 | v1~v4      | v3 기본                     |
| 2.3.x     | supported 또는 experimental | 실제 검증값만 기재 | v1~v4      | v3 기본                     |


실제 테스트하지 않은 버전을 검증 완료로 표시하지 말아줘.

## 15. 문서 개선

README와 설계 문서에서 다음 표현을 수정해줘.

기존:

```text
Apache Flink 1.20.4 SQL Gateway 전용 Go REST Client
```

변경 예:

```text
Apache Flink 1.20.x, 2.0.x 및 2.1.x, 2.2.x, 2.3.x SQL Gateway를 지원하는 Go REST Client
```

단, 지원 구현과 테스트가 완료되지 않은 시점에는 다음처럼 정확하게 표시해줘.

```text
현재 검증 완료:
- Flink 1.20.4

지원 구현 또는 검증 진행 중:
- Flink 2.0.x
- Flink 2.1.x
```

다음 문서를 추가하거나 보완해줘.

```text
docs/compatibility.md
docs/release-policy.md
docs/design.md
docs/build.md
README.md
```

`docs/compatibility.md`에는 다음을 설명해줘.

```text
release line과 tested patch version 차이
REST API 버전 선택 정책
자동 감지 방식
수동 CompatibilityMode 사용법
미지원 버전 오류 처리
지원 상태 정의
버전 지원 종료 정책
```

## 16. 오류 모델

다음 상황을 구분할 수 있는 typed error를 추가해줘.

```text
Flink 버전을 파싱할 수 없음
지원하지 않는 Flink 릴리스
서버와 클라이언트 간 공통 REST API 버전 없음
명시한 REST API 버전이 서버에서 지원되지 않음
명시한 REST API 버전이 해당 Flink profile에서 지원되지 않음
요청 기능이 현재 profile에서 지원되지 않음
자동 감지 요청 실패
```

예:

```go
var (
    ErrUnsupportedFlinkVersion = errors.New("unsupported Flink version")
    ErrNoCompatibleAPIVersion  = errors.New("no compatible SQL Gateway API version")
    ErrUnsupportedCapability   = errors.New("unsupported capability")
)
```

`errors.Is`와 `errors.As`를 사용할 수 있도록 구현해줘.

## 17. 보안 및 운영 요구사항

기존 보안 설계는 유지해야 한다.

```text
외부 nextResultUri 차단
다른 origin redirect 차단
인증 헤더 외부 유출 방지
비멱등 POST 자동 재시도 금지
response body 크기 제한
SQL 원문 및 인증 헤더 Observer 노출 금지
TLS 검증 비활성화 옵션 미제공
context 취소와 cleanup 분리
```

버전 자동 감지 과정에서도 `/info`와 `/api_versions` 요청이 기존 origin 검증, timeout, retry 정책을 우회하지 않도록 해줘.

## 18. 구현 시 주의사항

다음 방식은 피해야 한다.

```text
Flink 버전별 전체 소스 복사
공개 패키지를 버전별로 분리
build tag로 사용자가 Flink 버전을 선택하게 하는 방식
Flink 버전별 go.mod 생성
Flink 버전별 장기 브랜치 운영
실제 테스트하지 않은 버전을 supported 또는 tested로 표시
모든 DTO를 버전별로 무조건 복제
런타임 YAML 의존성 추가
```

REST DTO가 완전히 동일한 경우 공통 구조를 재사용하고, wire 형식이 실제로 달라지는 경우에만 버전별 구조체 또는 encoder를 분리해줘.

## 19. 구현 진행 방식

다음 순서로 작업해줘.

1. 현재 저장소 구조와 공개 API 분석
2. Flink 버전에 종속된 코드 위치 목록 작성
3. 변경 설계 요약 작성
4. 호환성 모델과 registry 구현
5. 기존 Flink 1.20 로직을 profile로 이동
6. API 버전 선택 정책 구현
7. 자동 감지 구현
8. 버전별 protocol 차이 분리
9. 단위 테스트 및 fixture contract test 추가
10. 실제 integration test 구조 확장
11. 빌드 스크립트와 산출물 변경
12. README 및 문서 갱신
13. `v0.2.0` 릴리스 노트 작성
14. 전체 테스트와 정적 분석 수행

## 20. 완료 조건

다음 조건을 모두 충족해야 한다.

* 기존 공개 import 경로가 유지됨
* 기존 Flink 1.20.4 동작이 회귀하지 않음
* 단일 `SupportedFlinkVersion` 중심 구조가 제거됨
* Flink 1.20.x, 2.0.x, 2.1.x profile을 표현할 수 있음
* REST API v1~v4를 구조적으로 구분할 수 있음
* 자동 감지와 명시적 버전 설정을 모두 지원함
* Stable, Highest, Explicit 정책이 테스트됨
* 버전별 capability가 실제 요청 생성에 반영됨
* Flink 1.20의 `executionTimeout` 예외가 profile 기반으로 처리됨
* 버전별 fixture contract test가 존재함
* 실제 Gateway integration test를 버전별로 실행할 수 있음
* 빌드 산출물에서 단일 Flink 버전 suffix가 제거됨
* `compatibility.json`이 생성됨
* 릴리스 태그 정책이 `v0.2.0` 기준으로 정리됨
* README와 호환성 문서가 현재 구현 상태를 정확히 설명함
* `go test ./...` 통과
* integration test compile 통과
* `go vet ./...` 통과
* `gofmt` 적용
* `go mod tidy -diff` 통과
* `govulncheck` 정책 유지

## 21. 최종 결과 보고 형식

작업 완료 후 다음 형식으로 결과를 보고해줘.

```text
1. 기존 구조 분석
2. 발견된 Flink 버전 종속 코드
3. 적용한 아키텍처
4. 변경된 파일 목록
5. 공개 API 변경사항
6. 하위 호환성 유지 여부
7. Flink 버전별 지원 상태
8. REST API 버전별 지원 상태
9. 테스트 추가 및 실행 결과
10. 빌드 산출물 변경
11. 릴리스 정책
12. 남아 있는 제한사항
```

코드만 변경하지 말고, 왜 해당 구조를 선택했는지와 향후 Flink 2.3.x를 추가할 때 변경해야 하는 범위도 함께 설명해줘.