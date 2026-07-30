# Flink SQL Gateway Go Client 설계

## 목표와 패키지 경계

`flinksqlgateway`는 Flink 1.20.x와 2.0.x~2.3.x SQL Gateway compatibility profile을 하나의 공개 패키지에서 관리한다. 저수준 REST API 위에 Managed/Serialized Session과 iterator 결과 계층을 제공한다. `flinkrest`는 JobManager REST lifecycle을 별도 패키지로 제공하며 SQL Operation과 Job 취소를 자동 결합하지 않는다. JDBC·인증 시스템·SQL parser는 포함하지 않는다.

명시적 고수준 계층으로 Session Recipe, 선언형 Session Setup, 타입 Decoder, Metadata/Capability helper를 제공한다. `flinksqlgateway/changelog`는 상태를 보유하는 선택형 materializer로 핵심 REST client와 분리한다.

```text
Config / Client interface
        |
        +-- compatibility.go release profile, lazy detection, API policy
        +-- capability.go    release/protocol capability intersection
        +-- session.go       stateful session lifecycle
        +-- session_setup.go declarative configure-session orchestration
        +-- operation.go     asynchronous operation lifecycle
        +-- result.go        NOT_READY/PAYLOAD/EOS and paging
        +-- poller.go        collect/stream orchestration and cleanup
        +-- transport.go     bounded JSON HTTP and retry classification
        +-- security.go      next URI validation and handle masking
```

공개 import 경로는 Flink 버전과 관계없이 `github.com/heartblast/flink-sql-go/flinksqlgateway` 하나를 유지한다. 버전별 package, module, build tag 또는 장기 브랜치를 만들지 않으며 실제 wire 차이가 있는 encoder와 capability만 profile 경계에서 분리한다. 외부 런타임 의존성은 사용하지 않는다.

## Compatibility 모델과 lazy 감지

Flink 제품 release와 SQL Gateway REST API 버전은 독립적인 축으로 취급한다. release profile은 release line, 지원 상태, 직접 검증한 patch, 허용 REST API, stable API와 release별 capability를 보관한다. 선택된 REST protocol은 endpoint와 DTO 기능을 제한하며 최종 capability는 profile과 protocol의 교집합이다.

```text
NewClient (no network)
        |
        v 최초 versioned 요청 또는 명시적 compatibility 조회
GET /info                 # Auto mode에서만 실행
        |
        v release line profile 선택
GET /api_versions
        |
        v profile/server 교집합 계산
Stable | Highest | Explicit
        |
        v immutable CompatibilityInfo cache
/vN/sessions/...
```

- 기본 mode는 `CompatibilityAuto`, 기본 policy는 `APIVersionStable`이다.
- Auto는 `/info` 다음 `/api_versions` 순서를 지키며, 수동 mode는 `/info`를 생략하되 `/api_versions` 검증은 유지한다.
- 기존처럼 `APIVersion`만 설정하면 하위 호환성을 위해 `Explicit`으로 해석한다.
- 성공과 확정적인 incompatibility는 client에 저장한다. 감지 GET의 일시적인 transport/context 오류는 다음 호출에서 다시 시도할 수 있다.
- `GetCompatibilityInfo`, `SupportedFlinkVersions`, `CompatibilityMatrix`는 slice를 복사해 내부 registry를 외부 변경에서 격리한다.
- compatibility 감지 GET도 공통 transport의 origin, timeout, retry, 응답 크기와 관측 정책을 우회하지 않는다.

`compatibility.yaml`은 release metadata의 단일 원천이다. Go registry와 build의 `compatibility.json` 산출물은 contract 검증으로 manifest와 일치시킨다. Flink 1.20.4만 직접 검증했으며 2.0.x~2.3.x profile은 `experimental`이고 `TestedVersions`가 비어 있다. 자세한 상태와 선택 규칙은 [compatibility.md](compatibility.md)에 정의한다.

## API와 DTO 결정

공통 DTO의 보수적인 기준은 Flink 1.20.4 태그 `release-1.20.4`의 다음 구현이다.

- `SqlGatewayRestAPIVersion`: v1/v2/v3, v3 기본
- `FetchResultsHandler`: `nextResultUri`를 server token으로 생성
- `FetchResultsResponseBodySerializer`: `isQueryResult`, `jobID`, `resultKind`, `results`
- `ResultInfoSerializer`: `columns`, `rowFormat`, `data[].kind`, `data[].fields`
- `OperationStatus`: 8개 상태와 terminal 여부

OpenAPI의 추상 schema보다 실제 serializer를 우선했다. Decimal·시간·배열·맵·ROW·binary는 Go 기본 타입으로 강제 변환하지 않고 `json.RawMessage`로 유지한다. `LogicalType.Raw`는 알려지지 않은 type/property도 보존한다.

이후 release에서 같은 wire 구조를 유지하면 공통 DTO를 재사용한다. 실제 field나 endpoint 의미가 달라질 때만 private encoder 또는 protocol descriptor를 분리하며 public DTO를 release마다 복제하지 않는다. `OperationStatus`, `ResultType`과 원본 JSON은 알려지지 않은 이후 값을 가능한 한 보존한다.

## 세션

- `OpenSession`은 한 번의 비재시도 POST만 보낸다.
- 클라이언트가 직접 연 세션 handle과 이름/property를 보관해 Validator context로 제공한다.
- 알려진 세션이 404가 되면 `ErrSessionExpired`, 처음 보는 handle의 404는 `ErrSessionNotFound`로 분류한다.
- 세션 만료 시 state loss를 피하기 위해 자동 생성이나 SQL 재실행을 하지 않는다.
- `StartHeartbeat`은 handle마다 runner를 하나만 만든다. `CloseSession`은 heartbeat를 먼저 취소하고, 동시 close 호출은 같은 결과를 기다린다.

소유권은 클라이언트가 임의의 사용자 모델을 만들지 않고 `StatementValidator`에 `SessionContext`를 넘겨 상위 서비스가 검사한다.

## 선언형 Session Setup

`SessionSetupPlan`은 catalog, database, table과 최종 current scope를 network 호출 전에 compile한다. 기존 `SessionRecipe`는 `/statements`와 `ExecuteAndWait`를 사용하는 범용 replay로 유지하고, Session Setup은 선택된 compatibility의 `ConfigureSession` capability가 참일 때만 `/configure-session`을 사용한다. 기존 `Client`에 메서드를 추가하지 않고 `SessionSetupExecutor`를 별도 interface로 제공해 외부 mock과 wrapper의 compile 호환성을 유지한다. Compatibility 조회도 같은 이유로 `GatewayClient` 메서드와 별도 `CompatibilityProvider` 계약으로 제공한다.

```text
CompileSessionSetup (no network)
        |
        v
CREATE CATALOG -> CREATE DATABASE -> CREATE TABLE
        |
        v
USE CATALOG -> USE database
        |
        v
optional read-only metadata verification
```

- catalog/database/table target은 compile 단계에서 모두 검증하며 CREATE 문은 완전 수식 이름을 사용한다.
- `TableSetup.Statement`는 완전한 SQL이 아니라 library가 생성한 target 뒤의 definition만 허용한다. SQL parser나 정규식 기반 객체명 추측은 하지 않는다.
- option key를 정렬하고 식별자는 backtick, option 문자열은 작은따옴표를 두 번 써 escape한다.
- 같은 session의 `ConfigureSession`과 setup plan은 context-aware gate로 직렬화한다. 다른 session은 병렬로 진행할 수 있다.
- configure POST는 자동 재시도하지 않고 408, 429, 5xx 또는 response 유실을 `ErrConfigurationOutcomeUnknown`으로 분류한다.
- metadata 검증은 DDL 적용 뒤 별도 단계이며 `Applied`와 `Verified`를 구분한다. 검증 실패는 DDL 미적용으로 해석하지 않는다.
- `OpenSessionWithSetup` 실패 cleanup은 제한된 background context로 session만 닫는다. 자동 `DROP` rollback은 하지 않으며 영속 변경 가능성을 결과에 표시한다.

## Operation과 결과

```text
ExecuteStatement (no retry)
        |
        v
operationHandle
        |
        v
Fetch token 0 / validated nextResultUri
        |
        +-- NOT_READY -> bounded exponential backoff
        +-- PAYLOAD   -> metadata + changelog rows -> next URI
        +-- EOS       -> final metadata/rows -> close operation
```

poll interval은 `PollInterval`에서 시작해 2배로 증가하고 `MaxPollInterval`에서 제한된다. `MaxPolls`, `MaxResultRows`, context deadline이 전체 실행을 제한한다. 알 수 없는 ResultType/OperationStatus는 문자열로 보존하되 성공으로 간주하지 않는다.

`ExecuteAndWait`는 설정된 최대 행까지만 메모리에 모은다. `StreamResults`는 bounded channel로 operation/page/row/EOS 이벤트를 보내며 page 이벤트에는 행을 중복 포함하지 않는다. 소비를 중단한 호출자는 반드시 context를 취소해야 한다.

## context와 cleanup

- 모든 HTTP request는 호출자 context와 `RequestTimeout`을 함께 적용한다.
- 고수준 실행은 모든 profile에서 client-side `ExecutionTimeout`으로 전체 제출/polling 시간을 제한한다. `WireExecutionTimeout`이 거짓인 Flink 1.20 profile은 REST field를 생략하고, 참인 2.x profile은 같은 제한을 millisecond 값으로도 전송한다.
- context 취소 또는 결과 제한 뒤 cleanup은 이미 취소된 context가 아닌 짧은 background context를 사용한다.
- `CancelOnContextDone`이 true이면 context 취소 시 Operation cancel을 시도한다.
- cancel/close 실패가 원래 context 또는 result-limit 오류를 덮지 않는다.
- 성공 시 결과를 모두 받은 뒤에만 Operation을 닫는다.

Operation cancel이 이미 제출된 Flink Job을 항상 취소한다고 가정하지 않는다. 반환된 `JobID`는 correlation 정보일 뿐 JobManager 생명주기는 별도 모듈의 책임이다.

## 재시도

안전하게 반복 가능한 `/info`, `/api_versions`, 기타 조회와 heartbeat만 408/429/502/503/504 또는 transport 오류에 최대 1회 재시도한다. SQL 실행, 세션 생성·구성, Session Setup step, 취소·종료는 재시도하지 않는다. 비멱등 statement 제출은 `ErrExecutionOutcomeUnknown`, session 구성은 `ErrConfigurationOutcomeUnknown`으로 별도 분류한다. `APIError.Retryable`은 호출자가 상위 정책을 적용할 때 사용한다.

## 보안

- BaseURL은 http/https와 host를 요구하고 userinfo/query/fragment를 거부한다.
- compatibility 감지의 `/info`와 `/api_versions`도 같은 BaseURL과 공통 HTTP transport만 사용한다.
- next URI와 redirect는 최초 BaseURL과 scheme/host/port가 같아야 한다.
- response body는 `MaxResponseBytes+1`까지만 읽는다.
- 오류 message는 첫 줄, 최대 512 bytes로 제한해 stack trace 전체 노출을 막는다.
- Observer에는 query string/SQL/header가 없는 path와 timing만 보낸다.
- Session Setup 결과와 event에는 SQL이나 option map을 저장하지 않는다. 기본 및 caller 지정 민감 key를 탐지하고 Gateway 반사 오류는 관측 전에 `********`로 치환한다.
- TLS 검증을 끄는 option은 제공하지 않는다. 필요한 custom Transport는 호출자가 명시적으로 주입한다.

## 테스트 전략

`httptest.Server` 단위 테스트가 다음을 검증한다.

- info/version/session/configuration/completion/heartbeat lifecycle
- release version 파싱, profile 선택, API 교집합과 Stable/Highest/Explicit 정책
- NewClient 무통신, Auto 감지 순서, 수동 mode와 감지 결과 cache
- release별 capability 및 `executionTimeout` wire 포함/생략
- SQL 제출 POST 무재시도, safe GET 재시도
- Operation status/cancel/close
- 실제 Flink serializer 형태와 네 RowKind
- NOT_READY/PAYLOAD/EOS 및 여러 token
- collect 제한과 streaming event order
- context 취소, timeout, response 크기, invalid/non-JSON 오류
- 외부 next URI 차단, known session 만료 분류
- heartbeat 중복/종료 race와 idempotent session close

실제 Gateway 검증은 `integration` build tag와 `FLINK_SQL_GATEWAY_URL`을 사용한다. 현재 직접 검증한 patch는 Flink 1.20.4뿐이다. 2.0.x~2.3.x profile은 실제 Gateway matrix를 통과하기 전까지 `experimental`로 유지하며 `TestedVersions`를 채우지 않는다. 운영 적용 전에는 동일 reverse proxy/TLS/auth 환경에서 선택 API 응답과 idle timeout, long-running query cancel, INSERT Job ID를 추가 확인해야 한다.
