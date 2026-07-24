# 후속 개선 우선 범위 구현

`docs/reference_docs/function-improvement.md`가 권장한 1차 범위 중 다음 기능을 구현했다.

- SQL 제출 결과 불명확 오류
- Managed Session
- 동일 세션 직렬 실행
- Result Iterator
- Flink Job REST Companion
- 위 기능을 지원하는 Client.Close, cleanup 오류 보존, 선택형 lifecycle Observer

후속 요청으로 다음 기능도 구현했다.

- Session Recipe
- Flink 타입 Decoder와 Row helper
- Metadata/Capability Helper
- Changelog Materializer

실행에 사용한 재사용 프롬프트는 `prompt_management/function-improvement-development-prompt.md`에 있다.

## SQL 제출 안전성

SQL 제출 POST는 자동 재시도하지 않는다. `httptrace`로 요청 진행 단계를 관찰하고, 요청이 전송되었을 가능성이 있지만 operation handle을 받지 못한 경우 `ExecutionOutcomeUnknownError`를 반환한다.

```go
var unknown *flinksqlgateway.ExecutionOutcomeUnknownError
if errors.Is(err, flinksqlgateway.ErrExecutionOutcomeUnknown) && errors.As(err, &unknown) {
    // 자동 재실행하지 않고 사용자에게 Gateway/Job 상태 확인을 안내한다.
}
```

연결 자체가 성립하지 않아 요청이 전송되지 않은 오류는 일반 `APIError`이며 outcome-unknown으로 분류하지 않는다. 오류에는 SQL 원문이 포함되지 않는다.

## Managed Session lifecycle

`OpenManagedSession`은 세션을 생성한 뒤 호출 context와 별도인 client lifecycle context로 heartbeat를 시작한다. 연속 heartbeat 실패는 `DEGRADED`, Gateway의 명시적 session expired/not-found 응답은 `EXPIRED`로 전환한다. 만료된 세션을 자동으로 재생성하지 않는다.

`Close` 순서:

1. managed lifecycle 취소
2. heartbeat runner 중단 및 monitor 종료 대기
3. serialized gate가 있으면 신규 실행 거부
4. Flink session 종료
5. 건강 상태를 `CLOSED`로 전환하고 client registry에서 제거

`GatewayClient.Close`는 신규 요청을 거부하고 활성 iterator, managed session, heartbeat를 정리한다. 내부에서 생성했거나 `OwnHTTPTransport`가 설정된 transport만 idle connection을 닫는다.

## 세션별 직렬화

`SerializedSession`은 크기 1의 channel semaphore를 사용한다. gate 대기는 호출 context와 session lifecycle을 동시에 관찰하므로 대기 중 취소에 별도 goroutine이 필요하지 않다.

- 같은 wrapper의 `Execute`는 전체 실행과 operation 정리가 끝날 때까지 gate를 보유한다.
- `Stream`은 iterator가 EOS에 도달하거나 `Close`될 때까지 gate를 보유한다.
- wrapper별 gate이므로 다른 session은 병렬 실행할 수 있다.
- wrapper 종료 후 신규 실행은 `ErrSessionClosed`를 반환한다.

기본 `GatewayClient`의 병렬 동작은 변경하지 않았다.

## Result Iterator

`ExecuteStream`은 statement를 제출한 뒤 `ResultStream`을 반환한다. `Next`가 필요한 result page를 직접 가져오므로 producer goroutine과 전체 결과 메모리 적재가 없다.

- `Next() bool`: 다음 행 조회
- `Row() Row`: 현재 행
- `Event() ResultEvent`: 현재 operation/row/EOS event
- `JobID() string`: 조회 과정에서 발견한 Flink Job ID
- `Err() error`: 최종 조회/제한/context/cleanup 오류
- `Close() error`: 조기 중단 시 cancel 및 close

row/poll/execution timeout 제한과 same-origin `nextResultUri` 검증은 기존 실행 정책을 공유한다. EOS에서는 operation close, 결과 제한이나 명시적 조기 Close에서는 cancel 후 close를 수행한다.

`ExecutionError`는 원래 오류, cancel 오류, close 오류를 별도 필드로 보존하고 `Unwrap() []error`로 모두 탐색 가능하게 한다.

## Flink Job REST 분리

`flinkrest`는 JobManager 주소와 HTTP lifecycle을 별도로 설정하는 독립 패키지다.

| 메서드 | Flink 1.20 endpoint |
| --- | --- |
| `GetJob` | `GET /jobs/:jobid` |
| `GetJobStatus` | `GET /jobs/:jobid/status` |
| `CancelJob` | `PATCH /jobs/:jobid?mode=cancel` |
| `StopJob` | `POST /jobs/:jobid/stop` |
| `GetJobExceptions` | `GET /jobs/:jobid/exceptions` |
| `GetCheckpoints` | `GET /jobs/:jobid/checkpoints` |
| `GetJobPlan` | `GET /jobs/:jobid/plan` |

참조: [Apache Flink 1.20 JobManager REST API](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/rest_api/)

SQL Gateway operation cancel은 Job cancel을 호출하지 않는다. Job cancel과 stop-with-savepoint는 호출자가 `flinkrest`에 명시적으로 요청해야 한다.

## Observer 호환성

기존 `Observer.ObserveRequest`는 변경하지 않았다. 고수준 이벤트는 별도 `LifecycleObserver`로 주입하므로 기존 구현이 깨지지 않는다. 이벤트에는 SQL, 인증 헤더, URL query string이 포함되지 않는다.

## Session Recipe

`OpenSessionFromRecipe`는 Properties를 포함해 세션을 연 뒤 setup SQL을 입력 순서대로 실행한다. 실패하면 이후 문장을 실행하지 않고 `Applied`, `FailedIndex`, `Complete`로 진행 범위를 반환한다. 기본값은 생성 세션을 닫는 것이며 `KeepSessionOnFailure`로 명시적으로 유지할 수 있다. 자동 재생성이나 자동 replay는 없다.

## 타입 Decoder

`DefaultValueDecoder`는 BOOLEAN, 정수, 부동소수점, DECIMAL, 문자열, DATE/TIME/TIMESTAMP/TIMESTAMP_LTZ, binary, ARRAY/MAP/ROW를 지원한다. DECIMAL은 float로 변환하지 않고 정확한 문자열을 보존한다. `TIMESTAMP`와 `TIMESTAMP_LTZ`는 서로 다른 wrapper type으로 반환하며 미래 타입은 `json.RawMessage`로 보존한다.

`Row`의 기존 JSON 모델과 구조체 표현은 변경하지 않았다. `row.WithColumns(result.Columns, decoder)`로 `RowAccessor`를 만든 뒤 컬럼명/인덱스 helper를 사용한다. 중복 컬럼명은 첫 번째 항목을 사용한다.

## Metadata와 Capability

Metadata helper는 `SHOW`, `DESCRIBE`, `EXPLAIN PLAN FOR`를 기존 bounded 실행 API 위에서 수행한다. Catalog/Database/Object는 모두 backtick quoting하며 내부 backtick을 두 번 써서 escape한다. 별도 세션은 생성하지 않는다.

Capability는 v1/v2/v3 차이를 한 함수에서 관리한다. 설정한 버전이 `/api_versions`에 있는지 먼저 검증하며 알 수 없는 미래 버전은 기능 지원을 추측하지 않는다.

## Changelog Materializer

`flinksqlgateway/changelog`는 `PrimaryKey(...)`와 `Columns(result.Columns)`를 명시적으로 받아 단일/복합 PK를 기반으로 INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE를 적용한다. update 순서를 검증하고 최대 행 초과 시 eviction 없이 `ErrMaxRows`를 반환한다. Snapshot은 stable insertion order의 deep copy이며 현재 상태이므로 RowKind를 INSERT로 정규화한다.

## API 호환성

| API | 변경 |
| --- | --- |
| 기존 `Client` 저수준 메서드 | 유지 |
| `ExecuteAndWait` | 시그니처 유지, cleanup 오류를 보존하도록 오류 의미 강화 |
| `StreamResults` | 유지, Client.Close lifecycle 취소 연계 추가 |
| 기존 `Observer` | 유지 |
| `Config` | `OwnHTTPTransport`, `LifecycleObserver` 선택 필드 추가 |
| 신규 고수준 API | concrete `GatewayClient` 위에 추가 |

## 남은 요구사항

다음은 이번 우선 범위에 포함하지 않았다.

- Trusted proxy rewrite URI resolver
- 전체 fuzz suite 및 API version별 실제 Flink contract matrix
- Linux build script/CI, SBOM, provenance, API compatibility 자동 보고

실제 Flink 1.20.4 SQL Gateway와 JobManager가 제공되는 환경에서 장시간 streaming, session expiry, Job 조회 통합 검증이 추가로 필요하다.
