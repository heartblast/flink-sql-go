# Flink SQL Gateway Go Client 설계

## 목표와 패키지 경계

`flinksqlgateway`는 Flink 1.20.4 SQL Gateway REST protocol을 담당하는 공개 패키지다. 저수준 REST API 위에 Managed/Serialized Session과 iterator 결과 계층을 제공한다. `flinkrest`는 JobManager REST lifecycle을 별도 패키지로 제공하며 SQL Operation과 Job 취소를 자동 결합하지 않는다. JDBC·인증 시스템·SQL parser는 포함하지 않는다.

명시적 고수준 계층으로 Session Recipe, 타입 Decoder, Metadata/Capability helper를 제공한다. `flinksqlgateway/changelog`는 상태를 보유하는 선택형 materializer로 핵심 REST client와 분리한다.

```text
Config / Client interface
        |
        +-- session.go       stateful session lifecycle
        +-- operation.go     asynchronous operation lifecycle
        +-- result.go        NOT_READY/PAYLOAD/EOS and paging
        +-- poller.go        collect/stream orchestration and cleanup
        +-- transport.go     bounded JSON HTTP and retry classification
        +-- security.go      next URI validation and handle masking
```

저장소가 비어 있었기 때문에 서비스 외부에서도 재사용할 수 있도록 `internal` 대신 최상위 공개 패키지로 구성했다. 외부 의존성은 사용하지 않는다.

## API와 DTO 결정

Flink 1.20.4 태그 `release-1.20.4`의 다음 구현을 기준으로 DTO를 결정했다.

- `SqlGatewayRestAPIVersion`: v1/v2/v3, v3 기본
- `FetchResultsHandler`: `nextResultUri`를 server token으로 생성
- `FetchResultsResponseBodySerializer`: `isQueryResult`, `jobID`, `resultKind`, `results`
- `ResultInfoSerializer`: `columns`, `rowFormat`, `data[].kind`, `data[].fields`
- `OperationStatus`: 8개 상태와 terminal 여부

OpenAPI의 추상 schema보다 실제 serializer를 우선했다. Decimal·시간·배열·맵·ROW·binary는 Go 기본 타입으로 강제 변환하지 않고 `json.RawMessage`로 유지한다. `LogicalType.Raw`는 알려지지 않은 type/property도 보존한다.

## 세션

- `OpenSession`은 한 번의 비재시도 POST만 보낸다.
- 클라이언트가 직접 연 세션 handle과 이름/property를 보관해 Validator context로 제공한다.
- 알려진 세션이 404가 되면 `ErrSessionExpired`, 처음 보는 handle의 404는 `ErrSessionNotFound`로 분류한다.
- 세션 만료 시 state loss를 피하기 위해 자동 생성이나 SQL 재실행을 하지 않는다.
- `StartHeartbeat`은 handle마다 runner를 하나만 만든다. `CloseSession`은 heartbeat를 먼저 취소하고, 동시 close 호출은 같은 결과를 기다린다.

소유권은 클라이언트가 임의의 사용자 모델을 만들지 않고 `StatementValidator`에 `SessionContext`를 넘겨 상위 서비스가 검사한다.

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
- 고수준 실행은 client-side `ExecutionTimeout`으로 전체 제출/polling 시간을 제한한다. Flink 1.20.4는 REST 요청의 양수 `executionTimeout`을 지원하지 않으므로 이 field는 전송하지 않는다.
- context 취소 또는 결과 제한 뒤 cleanup은 이미 취소된 context가 아닌 짧은 background context를 사용한다.
- `CancelOnContextDone`이 true이면 context 취소 시 Operation cancel을 시도한다.
- cancel/close 실패가 원래 context 또는 result-limit 오류를 덮지 않는다.
- 성공 시 결과를 모두 받은 뒤에만 Operation을 닫는다.

Operation cancel이 이미 제출된 Flink Job을 항상 취소한다고 가정하지 않는다. 반환된 `JobID`는 correlation 정보일 뿐 JobManager 생명주기는 별도 모듈의 책임이다.

## 재시도

안전하게 반복 가능한 조회와 heartbeat만 408/429/502/503/504 또는 transport 오류에 최대 1회 재시도한다. SQL 실행, 세션 생성·구성, 취소·종료는 재시도하지 않는다. `APIError.Retryable`은 호출자가 상위 정책을 적용할 때 사용한다.

## 보안

- BaseURL은 http/https와 host를 요구하고 userinfo/query/fragment를 거부한다.
- next URI와 redirect는 최초 BaseURL과 scheme/host/port가 같아야 한다.
- response body는 `MaxResponseBytes+1`까지만 읽는다.
- 오류 message는 첫 줄, 최대 512 bytes로 제한해 stack trace 전체 노출을 막는다.
- Observer에는 query string/SQL/header가 없는 path와 timing만 보낸다.
- TLS 검증을 끄는 option은 제공하지 않는다. 필요한 custom Transport는 호출자가 명시적으로 주입한다.

## 테스트 전략

`httptest.Server` 단위 테스트가 다음을 검증한다.

- info/version/session/configuration/completion/heartbeat lifecycle
- SQL 제출 POST 무재시도, safe GET 재시도
- Operation status/cancel/close
- 실제 Flink serializer 형태와 네 RowKind
- NOT_READY/PAYLOAD/EOS 및 여러 token
- collect 제한과 streaming event order
- context 취소, timeout, response 크기, invalid/non-JSON 오류
- 외부 next URI 차단, known session 만료 분류
- heartbeat 중복/종료 race와 idempotent session close

실제 Gateway 검증은 `integration` build tag와 `FLINK_SQL_GATEWAY_URL`을 사용한다. 운영 적용 전에는 동일 reverse proxy/TLS/auth 환경에서 v3 응답과 idle timeout, long-running query cancel, INSERT Job ID를 추가 확인해야 한다.
