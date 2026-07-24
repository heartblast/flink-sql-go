# flink-sql-go 후속 개선 개발 실행 프롬프트

## 역할

당신은 Go SDK와 Apache Flink REST API에 숙련된 시니어 엔지니어다. 현재 저장소의 기존 구현과 테스트를 먼저 분석한 뒤 `docs/reference_docs/init-requirements.md` 및 `docs/reference_docs/function-improvement.md`를 준수하여 후속 개선을 구현한다.

## 목표와 이번 실행 범위

Apache Flink 1.20.4를 기준으로 다음 우선 기능을 완성한다.

1. SQL 실행 결과 불명확 오류
2. Managed Session
3. 동일 세션 직렬 실행
4. Result Iterator
5. Flink Job REST Companion

위 기능의 안전한 lifecycle을 위해 `GatewayClient.Close`, cleanup 오류 보존, 필요한 Observer lifecycle 이벤트도 함께 구현한다. Session Recipe, 타입 Decoder, Metadata Helper, Capability API, Changelog Materializer, 전체 CI/SBOM은 이번 우선 범위 밖이며 후속 작업으로 명확히 기록한다.

## 필수 원칙

- Go 1.26.5를 사용한다.
- Apache Flink 1.20.4 REST 계약을 따른다.
- 기존 `flinksqlgateway` 저수준 공개 API와 channel 기반 `StreamResults`를 유지한다.
- JDBC, JVM, GraalVM, CGO, `database/sql`을 사용하지 않는다.
- SQL 제출 POST는 자동 재시도하지 않는다.
- SQL 원문, 인증 헤더, URL query string을 오류나 기본 Observer 이벤트에 남기지 않는다.
- 세션, SQL Operation, Flink Job의 lifecycle을 분리한다.
- 모든 장시간 기능은 `context.Context` 취소, 중복 Close 안전성, goroutine/lock 정리를 보장한다.
- 외부에서 주입된 HTTP transport의 소유권을 침해하지 않는다.
- 임의 외부 origin으로 인증 헤더가 전달되지 않도록 기존 same-origin 보안 정책을 유지한다.

## 상세 구현 요구사항

### 1. 실행 결과 불명확 오류

- `ErrExecutionOutcomeUnknown`, `RequestPhase`, `ExecutionOutcomeUnknownError`를 공개한다.
- 요청 미전송, 전송 가능성 있음, 응답 헤더 미수신을 구분한다.
- SQL 제출 요청이 전송되었을 가능성이 있는데 operation handle을 받지 못한 경우에만 typed error로 변환한다.
- 연결 자체가 성립하지 않은 일반 오류는 outcome-unknown으로 오분류하지 않는다.
- `errors.Is`와 `errors.As`로 sentinel, typed error, 원인 timeout을 각각 판별할 수 있게 한다.
- 전체 SQL을 오류에 포함하지 않는다.

### 2. Managed Session과 Client.Close

- `ManagedSession`, `ManagedSessionOptions`, `SessionHealth`를 공개한다.
- 세션 생성 API는 세션을 연 뒤 호출 context와 독립된 client lifecycle context로 heartbeat를 관리한다.
- heartbeat runner는 세션마다 하나만 존재하며 성공/실패를 추적한다.
- 연속 실패 임계값에서 `DEGRADED`, 명시적인 세션 만료에서 `EXPIRED`, 종료 후 `CLOSED`로 전환한다.
- heartbeat interval에 선택적 jitter를 제공하되 테스트 가능한 구조로 만든다.
- Managed Session Close는 heartbeat를 먼저 중지하고 세션을 닫으며 중복 호출에 안전해야 한다.
- Client.Close는 신규 요청을 거부하고 관리 중인 heartbeat, managed session, result stream을 종료하며 소유한 transport의 idle connection만 정리한다.
- `ErrClientClosed`를 공개한다.

### 3. 동일 세션 직렬 실행

- `NewSerializedSession(client, sessionHandle)`을 제공한다.
- `Execute`, `Stream`, `Close`를 지원한다.
- context 취소 가능한 세션별 gate를 사용하고 goroutine-per-waiter 큐를 만들지 않는다.
- 같은 세션의 실행 순서를 보장하고 서로 다른 wrapper/session은 병렬 실행할 수 있어야 한다.
- stream은 EOS 또는 Close까지 gate를 보유한다.
- 대기 중 context 취소와 실행 중 operation 취소를 구분한다.
- 세션 종료 후 신규 실행을 `ErrSessionClosed`로 거부한다.

### 4. Result Iterator

- 기존 `StreamResults`를 유지하고 `ResultStream` 및 `ExecuteStream`을 추가한다.
- `Next`, `Event`, `Row`, `Err`, `JobID`, `Close`를 구현한다.
- 별도 producer goroutine 없이 `Next`가 필요한 페이지를 순차 조회한다.
- 전체 결과를 메모리에 모으지 않는다.
- 기존 row/poll/timeout 제한과 `nextResultUri` same-origin 검증을 그대로 적용한다.
- EOS에서는 operation을 닫고, 조기 Close·context 취소·결과 제한에서는 정책에 따라 cancel 후 close한다.
- 원래 오류, cancel 오류, close 오류를 `ExecutionError`로 함께 보존한다.
- Close는 중복 호출에 안전해야 한다.

### 5. Flink Job REST Companion

- 별도 `flinkrest` 패키지를 만든다.
- JobManager BaseURL, HTTP client, timeout, 최대 응답 크기, headers, transport 소유권을 독립 설정한다.
- 다음 API를 구현한다: `GetJob`, `GetJobStatus`, `CancelJob`, `StopJob`, `GetJobExceptions`, `GetCheckpoints`, `GetJobPlan`, `Close`.
- Flink 1.20.4 계약을 사용한다: GET/PATCH `/jobs/:jobid`, GET `/status`, POST `/stop`, GET `/exceptions`, `/checkpoints`, `/plan`.
- Job ID는 URL path segment로 escape하며 선택적 32자리 hex 검증을 제공한다.
- Stop with Savepoint는 cancel과 별도 typed 옵션 및 trigger 응답으로 제공한다.
- 응답 JSON의 향후 필드를 보존할 수 있도록 핵심 typed 필드와 raw JSON을 함께 제공한다.
- SQL Gateway operation cancel이 Job cancel을 암시하지 않도록 두 패키지 사이에 자동 취소 연계를 만들지 않는다.

### 6. 관측성과 오류

- 기존 Observer interface 구현을 깨지 않는 optional lifecycle observer를 추가한다.
- 최소 이벤트: SessionOpened, SessionHeartbeatSucceeded/Failed, SessionHealthChanged, SessionClosed, StatementSubmitting/Submitted/OutcomeUnknown, ResultStreamClosed.
- 이벤트에는 SQL 원문과 인증정보를 넣지 않는다.
- `ExecutionError`는 `errors.Is`, `errors.As`, `errors.Unwrap`에서 원인 및 cleanup 오류를 모두 탐색 가능하게 한다.

## 테스트 완료 기준

다음 단위 테스트를 반드시 추가한다.

- 응답 헤더 전 연결 종료 시 outcome-unknown 판별
- 연결 거부 같은 미전송 오류가 outcome-unknown이 아님
- Managed Session heartbeat 성공, 연속 실패, 만료, 중복 Close
- heartbeat와 Close 동시 실행
- 같은 serialized session의 실행 순서
- 서로 다른 serialized session의 병렬 실행
- 직렬 gate 대기 중 context 취소
- iterator 정상 EOS, multi-page, Job ID, 사용자 조기 Close, context 취소, row/poll 제한, 네트워크 오류
- 원인 오류와 cancel/close cleanup 오류 동시 보존
- Client.Close 이후 요청 거부와 반복 Close
- Job REST 각 메서드의 method/path/body/응답 decoding 및 응답 크기 제한

검증 명령:

```text
go test ./...
go test -race ./...
go vet ./...
go tool govulncheck -test ./...
```

race detector가 로컬 C toolchain 부재로 실행되지 않으면 이를 명확히 보고하고 일반 테스트로 race-sensitive 동작을 최대한 검증한다. 실제 Flink 환경이 없으면 mock HTTP 통합 테스트를 수행하고 실제 Flink 1.20.4 검증이 남았음을 보고한다.

## 문서와 결과 보고

- README 또는 설계 문서에 새 API 사용 예제, lifecycle, 자동 재시도 금지, Job REST 분리 정책을 기록한다.
- 기존 공개 API의 변경 여부와 하위 호환성을 표로 정리한다.
- 변경 파일, 테스트 결과, 남은 제한을 `function-improvement.md`의 최종 결과 보고 형식으로 보고한다.
- 범위 밖 요구사항을 구현 완료로 표시하지 않는다.
