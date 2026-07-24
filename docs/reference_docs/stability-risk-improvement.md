# flink-sql-go 안정성 진단 및 개선 요구사항

## 1. 문서 목적

이 문서는 현재 `flink-sql-go` 구현에서 운영 장애, 오류 전파, 자원 누수 또는 데이터
불일치를 일으킬 수 있는 잠재 요소를 정리하고 후속 개선 요구사항과 완료 기준을 정의한다.

기존 문서의 역할은 다음과 같이 구분한다.

- `init-requirements.md`: 최초 기능과 아키텍처 요구사항
- `function-improvement.md`: Session Recipe, Decoder, Metadata Helper 등 기능 확장 요구사항
- 이 문서: 현재 구현을 기준으로 한 안정성 진단과 장애 예방 요구사항

진단 기준은 다음과 같다.

- 기준 module: `github.com/heartblast/flink-sql-go`
- 기준 source version: `v0.1.1` 이후 현재 `main`
- 기준 Go: `1.26.5`
- 지원 Flink: `1.20.4`
- 진단일: `2026-07-24`

## 2. 진단 결과 요약

일반 단위 테스트와 정적 검사는 통과하지만, 정상 경로 테스트만으로 확인하기 어려운
생명주기·동시성·오류 후 복구 문제가 남아 있다.

| ID | 우선순위 | 진단 항목 | 예상 영향 |
| --- | --- | --- | --- |
| STAB-001 | P0 | heartbeat 요청 timeout 후 runner 종료 | 실제 session은 만료되지만 local health는 `HEALTHY`로 유지될 수 있음 |
| STAB-002 | P0 | 공개 `Session`과 내부 상태 공유 | data race, map panic, 정책 정보 변조 |
| STAB-003 | P0 | session Close가 실행 중 operation 정리를 기다리지 않음 | cleanup 충돌, Close 이후 goroutine 잔존 |
| STAB-004 | P1 | 닫힌 session handle 무제한 보관 | 장기 실행 시 메모리 증가 또는 메모리 DoS |
| STAB-005 | P1 | SQL 제출 5xx의 재시도·결과 불명확 분류 | 중복 SQL 또는 Job 제출 가능성 |
| STAB-006 | P1 | operation cancel과 close가 하나의 timeout 공유 | cancel 지연 시 close 미실행 및 server 자원 잔존 |
| STAB-007 | P1 | changelog pending update 복구 수단 부재 | 특정 primary key가 영구적으로 update 불가 |
| STAB-008 | P1 | stream consumer와 Observer block 전파 | goroutine 누수 또는 Client.Close 정체 |
| STAB-009 | P1 | session·operation handle 공통 검증 부재 | 잘못된 route 호출과 내부 상태 오염 |
| STAB-010 | P2 | Decoder의 nested metadata 얕은 복사와 미지 field 유실 | data race 또는 forward compatibility 저하 |
| STAB-011 | P2 | 내부 operation/row 객체의 mutable 참조 노출 | fetch·cleanup 대상 변경 또는 비정상 결과 |
| STAB-012 | P2 | UTF-8을 고려하지 않은 오류 메시지 절단 | 깨진 로그 문자열과 JSON 인코딩 문제 |

## 3. 상세 진단 및 개선 요구사항

### STAB-001. Heartbeat timeout과 runner 생명주기 분리

#### 현재 동작

[`runHeartbeat`](../../flinksqlgateway/heartbeat.go#L91)는 heartbeat 오류가
`context.Canceled` 또는 `context.DeadlineExceeded`와 일치하면 runner를 종료한다.
이 분류에는 runner context 종료뿐 아니라 개별 HTTP 요청의 `RequestTimeout`도 포함된다.

기본 `FailureThreshold`는 3이므로 첫 요청 timeout 후 다음 상태가 가능하다.

1. heartbeat 실패 event 한 번 발생
2. runner 종료
3. `ManagedSession` health는 계속 `HEALTHY`
4. 후속 heartbeat가 없어 server session 만료

#### 개선 요구사항

- runner 종료 여부는 `ctx.Err()`로 판단한다.
- 개별 heartbeat 요청 timeout은 연속 실패로 집계한 뒤 다음 주기에 다시 시도한다.
- `ErrSessionNotFound`와 `ErrSessionExpired`만 즉시 `SessionExpired`로 전이한다.
- runner가 예기치 않게 종료되면 managed session이 계속 `HEALTHY`로 남지 않게 한다.
- 정상 heartbeat가 성공하면 연속 실패 횟수와 `DEGRADED` 상태를 초기화한다.

#### 완료 기준

- 연속 두 번의 request timeout 뒤에도 runner가 다음 heartbeat를 시도한다.
- 실패 임계값에 도달하면 `DEGRADED`가 된다.
- 이후 heartbeat 성공 시 `HEALTHY`로 복구된다.
- runner context 취소 시에는 추가 heartbeat가 발생하지 않는다.

### STAB-002. Session 내부 상태와 공개 DTO 분리

#### 현재 동작

[`OpenSession`](../../flinksqlgateway/session.go#L57)은 생성한 `*Session`을 내부
`sessions` map에 저장하고 같은 pointer를 호출자에게 반환한다. `Session.Properties`는
공개 map이므로 호출자가 변경할 수 있다.

동시에 [`sessionContext`](../../flinksqlgateway/client.go#L134)가 해당 map을 복사하면
`concurrent map iteration and map write` panic 또는 data race가 발생할 수 있다.
또한 `StatementValidator`에 전달되는 소유권·정책 property가 호출자 변경으로 오염될 수 있다.

#### 개선 요구사항

- client 내부 session 상태는 공개 `Session`과 별도 구조체로 보관한다.
- 공개 반환값과 내부 상태가 map, slice 또는 pointer를 공유하지 않게 깊게 복사한다.
- 공개 API 호환성을 유지해야 한다면 `Session.Properties`는 snapshot으로만 제공한다.
- validator에 전달하는 `SessionContext`는 항상 내부의 신뢰 가능한 원본에서 생성한다.
- `Session`의 동시성 계약을 실제 구현과 일치하도록 문서화한다.

#### 완료 기준

- 반환된 `Session.Properties`를 수정해도 내부 `SessionContext`가 변하지 않는다.
- 반환 객체를 여러 goroutine에서 읽고 별도 복사본을 수정해도 race가 없다.
- caller가 `Session.Handle`이나 `Name`을 바꿔도 client 내부 route와 정책 정보가 바뀌지 않는다.

### STAB-003. Close와 in-flight 실행 동기화

#### 현재 동작

[`managedSession.closeWithContext`](../../flinksqlgateway/managed_session.go#L229)와
[`SerializedSession.Close`](../../flinksqlgateway/serialized_session.go#L53)는 lifecycle
context를 취소한 직후 server session을 닫는다. 실행 중인 `ExecuteAndWait` 또는 stream이
operation cancel과 close를 완료할 때까지 기다리는 장치가 없다.

첫 `Close(ctx)`가 취소된 context로 실패하더라도 `sync.Once`에 결과가 고정되므로
server session 종료를 다시 시도할 수 없다.

#### 개선 요구사항

- session wrapper별 in-flight execution 수를 추적한다.
- Close는 다음 순서로 수행한다.

  1. 신규 실행 거부
  2. 대기 및 실행 중 context 취소
  3. operation cleanup 완료 대기
  4. heartbeat 종료
  5. server session 종료

- Close 대기는 context로 제한하되 timeout 이후의 미정리 상태를 명확한 typed error로 반환한다.
- 첫 Close의 context 오류 때문에 server-side cleanup 재시도가 영구 차단되지 않게 한다.
- `GatewayClient.Close`도 추적 중인 수집형 실행과 stream이 종료될 때까지 기다린다.

#### 완료 기준

- session DELETE가 operation cancel/close보다 먼저 전송되지 않는다.
- Close가 성공한 뒤 관련 goroutine과 operation이 남지 않는다.
- 첫 Close가 timeout되어도 bounded cleanup 재시도 경로가 존재한다.
- 여러 goroutine의 중복 Close가 panic 또는 중복 DELETE를 만들지 않는다.

### STAB-004. 닫힌 session 기록의 메모리 상한

#### 현재 동작

`GatewayClient.closed`는 성공적으로 닫은 모든 session handle을 보관한다.
[`CloseSession`](../../flinksqlgateway/session.go#L157)은 not-found 또는 expired 응답도
성공 처리하여 전달받은 handle을 `closed` map에 추가한다. 제거 또는 크기 제한은 없다.

#### 개선 요구사항

- 영구 `closed` map에 의존하지 않고 server not-found를 멱등 성공으로 처리하는 방식을 우선한다.
- local 중복 Close 결합에는 현재 진행 중인 `closeCalls`만 사용한다.
- 완료 결과 cache가 필요하면 TTL과 최대 개수를 가진 bounded cache를 사용한다.
- 빈 값과 형식이 잘못된 handle은 네트워크 호출 전에 거부한다.

#### 완료 기준

- 수십만 개의 서로 다른 handle을 닫아도 client의 retained memory가 선형 증가하지 않는다.
- 동시에 같은 session을 닫으면 server DELETE는 최대 한 번만 실행된다.
- 이미 닫힌 session에 대한 반복 Close는 계속 멱등이다.

### STAB-005. 비멱등 SQL 제출의 오류 분류 강화

#### 현재 동작

HTTP 408, 429, 502, 503, 504는 method와 무관하게 `APIError.Retryable=true`가 된다.
SQL 제출 POST에서 5xx 응답을 받으면 자동 재시도는 하지 않지만, 호출자에게는 retryable
오류로 노출된다. HTTP 오류에는 `RequestPhase`가 설정되지 않아
`ErrExecutionOutcomeUnknown`으로도 분류되지 않는다.

#### 개선 요구사항

- `Retryable`은 HTTP status뿐 아니라 method와 operation 의미를 반영한다.
- SQL 제출 POST는 자동 재시도와 일반적인 caller 재시도를 모두 권장하지 않는다.
- Flink 1.20.4 contract test로 각 4xx·5xx가 operation 미생성을 보장하는지 확인한다.
- server 처리 여부를 확정할 수 없는 5xx 또는 불완전 응답은
  `ExecutionOutcomeUnknownError`로 분류한다.
- typed error에 transport 재연결 가능성과 application operation 재실행 가능성을 구분한
  별도 정보가 필요한지 검토한다.

#### 완료 기준

- 503 응답을 받은 INSERT를 오류 정보만 보고 자동 재제출하지 않는다.
- pre-send failure, possibly-sent failure, 확정적인 SQL rejection이 각각 구분된다.
- 오류와 관측 event에 SQL 원문이나 인증정보가 포함되지 않는다.

### STAB-006. Operation cleanup 예산 분리

#### 현재 동작

[`cleanupOperationContext`](../../flinksqlgateway/poller.go#L152)와 result stream cleanup은
하나의 context로 `CancelOperation`과 `CloseOperation`을 순차 호출한다. Cancel이 전체
deadline을 소비하면 Close는 이미 만료된 context로 호출된다.

#### 개선 요구사항

- cleanup 전체 deadline 안에서 cancel과 close에 별도 예산을 배분한다.
- cancel 실패 또는 timeout과 관계없이 close를 시도한다.
- 이미 없는 operation에 대한 cancel과 close의 멱등 정책을 일관되게 정의한다.
- 주 실행 오류, cancel 오류와 close 오류를 계속 개별적으로 보존한다.

#### 완료 기준

- cancel endpoint가 timeout되어도 close endpoint 호출이 관찰된다.
- cleanup 전체 시간은 설정된 상한을 넘지 않는다.
- `errors.Is`와 `errors.As`로 원래 오류와 두 cleanup 오류를 모두 확인할 수 있다.

### STAB-007. Changelog pending update 복구와 성능

#### 현재 동작

`UPDATE_BEFORE`가 적용되면 primary key가 `pending`에 저장된다. stream 종료, decode 오류
또는 `UPDATE_AFTER` 유실 시 pending 상태를 해제하거나 rollback할 API가 없다.

삭제 시에는 insertion order slice를 선형 탐색하고 뒤 원소를 이동하므로 큰 snapshot에서
반복 insert/delete가 발생하면 비용이 커진다.

#### 개선 요구사항

- update pair를 transaction처럼 적용할 batch API를 검토한다.
- stream 종료 또는 resync 시 pending update를 rollback하거나 명시적으로 폐기할 수 있게 한다.
- pending update가 있는 snapshot의 의미를 문서화한다.
- primary key의 raw JSON 표현이 아니라 정규화된 값 기준 key가 필요한지 검토한다.
- 대량 delete가 O(n) 이동을 반복하지 않도록 order index 또는 linked 구조를 검토한다.

#### 완료 기준

- `UPDATE_BEFORE` 이후 stream이 중단되어도 동일 key를 복구할 수 있다.
- rollback 뒤 snapshot은 update 이전 상태와 동일하다.
- 100,000 row churn benchmark에서 지연과 메모리가 설정한 운영 기준을 만족한다.

### STAB-008. Stream backpressure와 Observer 격리

#### 현재 동작

`StreamResults` consumer가 channel 읽기를 중단하고 context도 취소하지 않으면 producer가
event 전송에서 계속 대기한다. `Observer`와 `LifecycleObserver`는 동기 호출이므로 구현체가
block하면 요청 반환, cleanup과 Client.Close도 함께 지연된다.

#### 개선 요구사항

- 신규 사용에는 종료를 명시할 수 있는 `ResultStream`을 우선 권장한다.
- channel API 유지 시 cancel handle, idle consumer timeout 또는 명확한 wrapper를 제공한다.
- Observer 호출은 client 핵심 lock 밖에서 수행한다.
- Observer의 최대 실행 시간, 비동기 queue와 event drop 정책을 선택 가능하게 한다.
- Observer panic뿐 아니라 장시간 block이 핵심 cleanup을 막지 않게 한다.

#### 완료 기준

- consumer가 읽기를 중단해도 context 취소 또는 Client.Close 후 producer가 종료된다.
- block하는 Observer가 HTTP 요청과 Client.Close를 무기한 정지시키지 않는다.
- 비동기 관측 queue가 가득 찰 때의 drop 또는 backpressure 정책이 문서화된다.

### STAB-009. Handle 검증 일원화

#### 현재 동작

`StartHeartbeat`와 `SerializedSession` 일부 경로만 빈 session handle을 검증한다.
대부분의 저수준 session·operation API는 빈 handle을 URL에 넣어 `/sessions//...` 형태의
요청을 만들 수 있다.

#### 개선 요구사항

- session handle과 operation handle의 공통 validator를 만든다.
- 모든 공개 저수준 API는 version check와 network 호출 전에 handle을 검증한다.
- 길이 상한과 제어문자·공백만 있는 값을 제한한다.
- handle은 계속 URL path segment로 escape하며 오류와 관측값에서는 마스킹한다.

#### 완료 기준

- 빈 handle, whitespace, slash, percent encoding과 Unicode 입력에 대해 table test와 fuzz test를 통과한다.
- 잘못된 handle 입력은 server에 요청을 보내지 않는다.

### STAB-010. Decoder의 deep copy와 미지 field 보존

#### 현재 동작

`Row.WithColumns`는 `ColumnInfo` slice만 복사하며 내부 `LogicalType` pointer, field slice와
raw JSON은 공유한다. object형 nested ROW decoding은 schema에 선언된 field만 결과에 넣어
미지 field를 버린다.

#### 개선 요구사항

- RowAccessor가 보관하는 column metadata를 깊게 복사하거나 immutable snapshot으로 만든다.
- object형 ROW의 미지 field는 `json.RawMessage`로 결과에 보존한다.
- MAP의 non-string key와 Flink 1.20.4 실제 wire representation을 contract test로 확인한다.
- schema가 없거나 부분적인 nested type은 원본 JSON을 잃지 않게 한다.

#### 완료 기준

- accessor 생성 후 원본 schema를 수정해도 decode 결과가 변하지 않는다.
- schema에 없는 nested object field가 결과에서 유지된다.
- nested schema를 동시에 읽고 외부 복사본을 수정해도 race가 없다.

### STAB-011. 내부 mutable 객체 노출 차단

#### 현재 동작

`StreamResults`의 operation event와 `ResultStream.Event`는 내부 fetch와 cleanup에서 사용하는
`*Operation`을 그대로 노출한다. `ResultStream.Row`와 일부 event도 nested slice를 깊게
복사하지 않는다.

#### 개선 요구사항

- event에는 내부 operation pointer가 아닌 value snapshot 또는 깊은 복사본을 전달한다.
- `Row`, `ResultPage`, `ColumnInfo` 반환 시 어느 수준까지 immutable인지 공개 계약을 정한다.
- 동시 호출을 지원하는 accessor는 nested slice와 raw JSON도 공유하지 않게 한다.

#### 완료 기준

- consumer가 event의 operation handle을 변경해도 실제 fetch와 cleanup route가 바뀌지 않는다.
- 반환 row를 수정해도 이후 event와 내부 상태가 변하지 않는다.

### STAB-012. UTF-8 안전한 오류 메시지 제한

#### 현재 동작

SQL Gateway와 Job REST client 모두 server message를 byte index로 잘라 최대 길이를 제한한다.
다중 byte 문자의 중간에서 잘리면 유효하지 않은 UTF-8 문자열이 만들어질 수 있다.

#### 개선 요구사항

- byte 상한을 유지하되 마지막 UTF-8 rune 경계를 찾아 절단한다.
- JSON log encoder와 Observer로 전달되는 오류 문자열이 항상 유효한 UTF-8인지 확인한다.

#### 완료 기준

- 한글, emoji와 잘못된 UTF-8 byte가 포함된 큰 오류 응답을 안전하게 제한한다.
- 결과 문자열은 `utf8.ValidString`을 만족한다.

## 4. 테스트 및 검증 보강

### 4.1 현재 확인 결과

| 검사 | 결과 | 비고 |
| --- | --- | --- |
| `go test ./...` | 통과 | 일반 단위 테스트 |
| `go vet ./...` | 통과 | 정적 검사 |
| `go test -race ./...` | 미실행 | 현재 Windows 환경에 C compiler `gcc`가 없음 |
| fuzz test | 미구현 | 요구 문서에 정의된 5개 fuzz target이 없음 |
| Flink 1.20.4 integration test | 조건부 | `integration` build tag가 있어 일반 테스트에서는 제외됨 |

### 4.2 필수 추가 단위 테스트

- heartbeat request timeout 후 runner 지속 및 health 전이
- 반환된 Session 수정과 내부 SessionContext 격리
- 실행 중 ManagedSession/SerializedSession Close 순서
- 첫 Close timeout 후 bounded cleanup 재시도
- 대량 session Close 후 retained memory 상한
- SQL 제출 408, 429, 500, 502, 503, 504 오류 분류
- cancel timeout 이후 CloseOperation 호출 확인
- UPDATE_BEFORE 중단 후 materializer rollback
- block하는 Observer와 Client.Close
- operation event 및 row 반환값 수정 격리
- 빈 session·operation handle network 호출 차단
- nested ROW 미지 field 보존
- UTF-8 오류 메시지 절단

### 4.3 Fuzz test

`function-improvement.md`에 정의된 다음 fuzz test를 구현한다.

```go
func FuzzParseResultPage(f *testing.F)
func FuzzResolveNextResultURI(f *testing.F)
func FuzzParseAPIError(f *testing.F)
func FuzzDecodeRow(f *testing.F)
func FuzzQuoteIdentifier(f *testing.F)
```

추가 seed에는 빈 handle, 중복 query parameter, 깊게 중첩된 logical type, 잘못된 UTF-8,
불완전한 UPDATE pair와 비정상 RowKind를 포함한다.

### 4.4 Race test

- Linux CI 또는 C compiler가 준비된 Windows runner에서 `go test -race ./...`를 필수 gate로 실행한다.
- Session snapshot 수정, Close와 Execute 경쟁, ResultStream Next와 Close 경쟁,
  Observer 동시 호출을 race scenario에 포함한다.

### 4.5 Flink 1.20.4 contract test

- v1, v2, v3 API별로 정상 응답뿐 아니라 4xx·5xx와 연결 중단을 검증한다.
- SQL 제출 직후 연결 종료와 5xx에서 operation 생성 여부를 실제 Gateway 기준으로 확인한다.
- heartbeat request timeout, session expiry와 operation cleanup 순서를 검증한다.

## 5. 권장 구현 순서

### 1단계: 즉시 장애 예방

1. STAB-001 heartbeat timeout 처리
2. STAB-002 Session 내부 상태 격리
3. STAB-003 Close와 in-flight 실행 동기화

### 2단계: 자원 정리와 중복 실행 방지

1. STAB-004 closed session 기록 상한
2. STAB-005 SQL 제출 오류 분류
3. STAB-006 cleanup timeout 분리

### 3단계: 장시간 실행 안정성

1. STAB-007 Materializer 복구와 성능
2. STAB-008 stream 및 Observer 격리
3. STAB-009 handle 검증

### 4단계: 데이터 보존과 품질 gate

1. STAB-010 Decoder deep copy와 미지 field 보존
2. STAB-011 mutable 반환값 격리
3. STAB-012 UTF-8 안전 처리
4. fuzz, race 및 Flink contract test 필수화

## 6. 호환성 원칙

- 기존 공개 API를 우선 유지하고 내부 상태 분리로 문제를 해결한다.
- public field를 즉시 제거해야 하는 경우에는 deprecation 기간과 migration API를 제공한다.
- SQL 제출 POST의 자동 재시도 금지 원칙을 유지한다.
- Session, Operation과 Flink Job 생명주기를 계속 독립적으로 취급한다.
- 원래 실행 오류를 cancel·close 오류로 덮어쓰지 않는다.
- 보안 검증과 오류 개선 과정에서 SQL 원문, 인증 header와 전체 handle을 노출하지 않는다.

## 7. 최종 완료 기준

- P0와 P1 항목에 대한 회귀 테스트가 모두 추가되고 통과한다.
- `go test ./...`, `go test -race ./...`, `go vet ./...`, `govulncheck ./...`가 통과한다.
- Flink 1.20.4 v1·v2·v3 contract test 결과가 기록된다.
- Client 및 session Close 후 관련 goroutine, operation과 heartbeat가 남지 않는다.
- 장기 실행 시 closed session 기록과 stream 상태가 무제한 증가하지 않는다.
- 비멱등 SQL 제출의 결과 불명확 오류가 호출자의 안전한 의사결정을 지원한다.

## 8. 구현 결과

2026-07-24에 P0, P1과 P2 항목을 우선순위 순서로 구현했다.

| ID | 상태 | 구현 결과 |
| --- | --- | --- |
| STAB-001 | 완료 | 개별 heartbeat timeout을 runner 종료와 분리하고 실패 누적 및 정상 복구를 검증했다. |
| STAB-002 | 완료 | 내부 `sessionRecord`와 공개 `Session` snapshot을 분리하고 property를 깊게 복사한다. |
| STAB-003 | 완료 | client와 session wrapper가 실행·stream을 추적하며 Close context 안에서 cleanup 순서를 보장한다. 실패한 server session Close는 다시 시도할 수 있다. |
| STAB-004 | 완료 | 최근 성공 Close cache를 1,024개로 제한하고 재발급된 동일 handle의 이전 항목을 제거한다. |
| STAB-005 | 완료 | 비멱등 SQL POST의 408, 429와 5xx를 재시도 불가·결과 불명확 오류로 분류한다. |
| STAB-006 | 완료 | cancel에 전체 cleanup 예산의 최대 절반만 배정하여 close 실행 시간을 남긴다. |
| STAB-007 | 완료 | `ApplyUpdate`, `RollbackPending`, `PendingUpdates`, key 정규화와 tombstone 기반 order index를 추가했다. |
| STAB-008 | 완료 | channel producer를 client 실행 추적에 포함하고 Observer를 timeout 및 동시 실행 상한으로 격리한다. |
| STAB-009 | 완료 | 모든 공개 session·operation 경로에서 공통 handle 검증을 network 호출 전에 수행한다. |
| STAB-010 | 완료 | nested logical type metadata를 깊게 복사하고 ROW의 미지 field를 `json.RawMessage`로 보존한다. |
| STAB-011 | 완료 | operation, event, page와 row 반환값을 내부 fetch·cleanup 상태에서 깊게 분리한다. |
| STAB-012 | 완료 | SQL Gateway와 Job REST 오류 메시지를 유효한 UTF-8 rune 경계에서 제한한다. |

### 8.1 공개 API와 호환성

- `Config.ObserverTimeout`과 `Config.ObserverMaxInFlight`를 추가했다. 기본값은 각각 100ms와 16이다.
- 관측 callback이 동시 실행 상한을 초과하면 핵심 HTTP·cleanup 경로를 보호하기 위해 해당 event를 버린다.
- `Materializer.ApplyUpdate`, `RollbackPending`과 `PendingUpdates`를 추가했다.
- `RequestPhase`에 응답 수신을 나타내는 `ResponseReceived`를 추가했다.
- 기존 공개 field와 method는 제거하지 않았다. `Session`은 반환 시점 snapshot이며 caller의 변경은 내부 client 상태에 반영되지 않는다.

### 8.2 검증 결과

Go 1.26.5 환경에서 다음 검증을 완료했다.

- `go mod verify`, `go mod tidy -diff`, `gofmt`, `go vet ./...` 통과
- `go test -count=1 ./...` 통과
- 5개 fuzz target을 실제 실행하여 모두 통과
- `build.ps1` 통과: coverage, package compile, reachable-symbol 및 module 취약점 검사, 배포 archive와 checksum 생성
- `govulncheck` 두 검사 모두 `No vulnerabilities found`

다음 검증은 환경 조건 때문에 남아 있다.

- `go test -race ./...`: Windows 환경에 C compiler `gcc`가 없어 실행하지 못했다.
- Flink 1.20.4 실서버 contract test: mock server 회귀 테스트는 통과했으나 실제 Gateway 연결 검증은 수행하지 않았다.
