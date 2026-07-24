# flink-sql-go 안정성 개선 개발 실행 프롬프트

## 역할

당신은 Go SDK, 동시성 제어와 Apache Flink 1.20.4 REST API에 숙련된 시니어 엔지니어다.
현재 저장소의 구현과 테스트를 먼저 분석하고
`docs/reference_docs/stability-risk-improvement.md`의 진단 결과를 가장 시급한 순서대로
재현 테스트와 함께 개선한다.

## 목표

운영 중 session 만료 오판, data race, goroutine·server 자원 누수와 SQL 중복 제출로
이어질 수 있는 위험을 제거한다. 공개 API 호환성과 기존 보안 경계를 유지하면서 P0,
P1, P2 순서로 구현하고 각 단계가 끝날 때 관련 테스트를 통과시킨다.

## 필수 원칙

- Go 1.26.5와 Apache Flink 1.20.4 계약을 기준으로 한다.
- module 경로 `github.com/heartblast/flink-sql-go`를 유지한다.
- JDBC, JVM, GraalVM, CGO와 `database/sql`을 추가하지 않는다.
- SQL 제출 POST를 자동 재시도하지 않는다.
- session, SQL operation과 Flink Job 생명주기를 분리한다.
- 원래 실행 오류를 cancel 또는 close 오류로 덮어쓰지 않는다.
- SQL 원문, 인증 header, URL query와 전체 handle을 오류·관측 event에 노출하지 않는다.
- 외부에서 주입된 HTTP transport의 소유권을 침해하지 않는다.
- 공개 API는 가능한 한 유지한다. 기존 field를 유지해야 하면 내부 상태와 깊은 복사본으로 격리한다.
- 운영 Go 소스와 새 테스트의 설명 주석은 저장소 `AGENTS.md`에 따라 한글로 작성한다.
- 수정 전 실패 또는 결함을 보여주는 테스트를 우선 추가하고 수정 후 회귀 테스트로 유지한다.

## 1단계: P0 즉시 장애 예방

### STAB-001 Heartbeat timeout

- runner context가 종료된 경우와 개별 HTTP RequestTimeout을 구분한다.
- request timeout은 연속 실패로 집계하고 다음 주기에 heartbeat를 계속한다.
- 실패 임계값에서 `DEGRADED`, 이후 성공하면 `HEALTHY`로 전환한다.
- session not-found 또는 expired만 `EXPIRED`로 전환하고 runner를 종료한다.
- runner가 예기치 않게 종료돼도 health가 잘못 `HEALTHY`로 남지 않게 한다.

### STAB-002 Session 상태 격리

- 내부 session record와 공개 `Session` 반환 객체를 분리한다.
- 공개 `Properties` map과 내부 validator용 property가 memory를 공유하지 않게 한다.
- 반환된 Session의 Handle, Name, Properties 수정이 client route와 `SessionContext`에 영향을 주지 않게 한다.
- 기존 `Session` public field를 유지하여 source compatibility를 보존한다.

### STAB-003 Close와 in-flight 실행

- ManagedSession과 SerializedSession의 수집형 실행 및 stream을 추적한다.
- Close는 신규 실행 거부, 실행 context 취소, operation cleanup 대기, heartbeat 종료,
  server session 종료 순서를 보장한다.
- Close 대기는 caller context와 cleanup timeout으로 제한한다.
- 첫 Close가 timeout돼도 server session cleanup을 다시 수행할 수 있는 상태 모델을 사용한다.
- 중복 Close는 server DELETE를 중복 실행하지 않으며 동일한 최종 결과를 반환한다.

## 2단계: P1 자원과 중복 실행 방지

### STAB-004 Closed session memory

- 완료된 handle을 영구 보관하는 unbounded `closed` map을 제거한다.
- 동시 Close 결합에는 진행 중 call을 사용하고 server not-found는 멱등 성공으로 처리한다.
- 기존 반복 Close 호환성을 위해 완료 cache가 필요하면 최대 개수와 명확한 eviction을 적용한다.

### STAB-005 SQL 제출 오류 분류

- `Retryable`이 method와 operation 의미를 반영하게 한다.
- 비멱등 SQL 제출의 408, 429, 5xx는 caller 재실행을 권장하는 retryable 오류로 노출하지 않는다.
- 요청이 server에 도달했을 수 있고 operation 미생성을 확정할 수 없으면
  `ExecutionOutcomeUnknownError`로 분류한다.
- pre-send failure와 확정적인 4xx rejection은 outcome-unknown으로 오분류하지 않는다.

### STAB-006 Cleanup 예산

- cancel과 close가 각각 실행 기회를 갖도록 별도 context 예산을 사용한다.
- cancel timeout 후에도 CloseOperation을 시도한다.
- 원인, cancel과 close 오류를 `ExecutionError`에서 모두 보존한다.

### STAB-007 Changelog 복구

- `UPDATE_BEFORE` 이후 중단된 pending update를 rollback 또는 reset할 공개 API를 제공한다.
- 가능하면 update pair를 원자적으로 적용하는 batch API를 제공한다.
- primary key의 JSON 표현을 안정적으로 정규화한다.
- 반복 delete의 O(n) slice 이동을 줄이는 구조를 적용한다.

### STAB-008 Stream과 Observer

- channel consumer가 context를 취소하면 producer가 즉시 종료되는지 강화한다.
- Client.Close가 channel producer와 observer 때문에 무기한 정체되지 않게 한다.
- Observer 실행은 핵심 lock 밖에서 이루어져야 하며 bounded 비동기 전달 또는 timeout
  격리 정책을 도입한다. event 유실 정책을 문서화한다.

### STAB-009 Handle 검증

- 모든 공개 session·operation API에 공통 handle validator를 적용한다.
- 빈 값, whitespace, 제어문자와 과도한 길이를 network 호출 전에 거부한다.
- slash와 Unicode 같은 유효한 불투명 handle은 path segment로 안전하게 escape한다.

## 3단계: P2 데이터 보존과 반환값 안전성

### STAB-010 Decoder

- `RowAccessor`가 column metadata와 nested LogicalType을 깊게 복사한다.
- object형 nested ROW의 schema 미지 field를 `json.RawMessage`로 보존한다.
- 부분 schema와 알 수 없는 nested type에서 원본 JSON을 잃지 않는다.

### STAB-011 Mutable 반환값

- stream event에는 내부 fetch·cleanup용 Operation pointer를 직접 노출하지 않는다.
- Event, Row와 metadata 반환값은 문서화된 수준까지 깊게 복사한다.
- consumer의 반환값 수정이 이후 paging과 cleanup route를 바꾸지 않게 한다.

### STAB-012 UTF-8 오류 메시지

- byte 상한 안에서 마지막 유효 UTF-8 경계까지 server message를 자른다.
- 잘못된 UTF-8 입력도 반환 문자열은 `utf8.ValidString`을 만족하게 정리한다.
- SQL Gateway와 flinkrest에 같은 정책을 적용한다.

## 테스트 요구사항

최소한 다음 테스트를 추가한다.

- heartbeat request timeout 후 runner 지속, DEGRADED와 HEALTHY 복구
- 공개 Session 수정과 내부 SessionContext 격리 및 동시 접근
- 실행 중 ManagedSession·SerializedSession Close의 cancel/close/session-delete 순서
- 첫 Close timeout 이후 cleanup 재시도와 중복 Close
- 대량 session Close 후 내부 상태가 증가하지 않음
- SQL 제출 4xx·5xx·pre-send·응답 중단 오류 분류
- cancel timeout 후 CloseOperation 실행
- Materializer pending rollback, atomic update와 대량 churn
- consumer 중단, Client.Close와 block하는 Observer
- 모든 저수준 API의 빈 session·operation handle 차단
- nested schema deep copy와 미지 field 보존
- event Operation과 Row 수정이 내부 실행에 영향을 주지 않음
- 한글·emoji·잘못된 UTF-8 오류 메시지 제한

기존 fuzz target도 구현한다.

```go
func FuzzParseResultPage(f *testing.F)
func FuzzResolveNextResultURI(f *testing.F)
func FuzzParseAPIError(f *testing.F)
func FuzzDecodeRow(f *testing.F)
func FuzzQuoteIdentifier(f *testing.F)
```

## 검증 명령

```text
gofmt -w <변경 Go 파일>
go mod verify
go mod tidy -diff
go test ./...
go vet ./...
go test -race ./...
./build.ps1
```

race detector가 로컬 C compiler 부재로 실행되지 않으면 실패 원인을 기록하고 Linux 또는
C toolchain이 있는 CI에서 필수 gate로 남긴다. 실제 Flink가 없으면 mock server contract
test를 수행하고 Flink 1.20.4 integration test가 남았음을 명시한다.

## 결과 보고

- 구현한 STAB ID와 핵심 설계 결정을 요약한다.
- 변경된 공개 API와 source compatibility 여부를 적는다.
- 실행한 테스트와 실행하지 못한 검증을 구분한다.
- 아직 남은 위험은 숨기지 않고 다음 우선순위로 기록한다.
- 사용자가 명시적으로 요청하지 않으면 commit, tag 또는 push하지 않는다.
