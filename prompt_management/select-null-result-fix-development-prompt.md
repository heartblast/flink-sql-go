# Flink SQL SELECT 결과 NULL 오인 문제 분석·수정 실행 프롬프트

## 역할

당신은 Go JSON decoding과 Apache Flink 1.20.4 SQL Gateway REST protocol에 숙련된 시니어
라이브러리 엔지니어다. `flink-sql-go`가 SELECT 결과의 실제 non-NULL column 값을 SQL NULL로
노출하는지 근거를 통해 확인하고, client 결함이면 하위 호환성과 원본 JSON 보존 원칙을 지키며
수정한다.

## 목표

SQL Gateway의 operation result response가 다음 경로를 통과할 때 값이 손실되는 지점을 찾는다.

```text
HTTP response body
  -> ResultPage.UnmarshalJSON
  -> ResultInfo / Row JSON decoding
  -> ExecuteAndWait 또는 ResultStream 수집
  -> RowAccessor.Raw/Value/String/Int64 등 공개 접근 API
```

실제 SQL NULL과 다음 상태를 구분해야 한다.

- non-NULL JSON scalar, object, array
- column metadata가 아직 없는 상태
- row field 수와 column 수가 다른 protocol 응답
- JSON과 PLAIN_TEXT row format 차이
- response page별 metadata 제공 여부 차이
- 알 수 없는 미래 logical type

## 조사 절차

1. 작업 시작 시 `git status --short`를 확인하고 기존 사용자 변경을 보존한다.
2. Apache Flink 1.20.4 공식 REST 문서와 `ResultSet` serialization source에서 실제 result JSON
   shape, row format별 field 표현, column metadata 위치를 확인한다.
3. `types.go`, `result.go`, `result_stream.go`, `decoder.go`와 관련 테스트를 따라 값의 byte-level
   변화를 추적한다.
4. `json.RawMessage`가 `nil`, 빈 byte slice 또는 literal `null`이 되는 모든 경로를 찾는다.
5. 추측으로 수정하지 말고 실제 Flink response fixture 또는 공식 source와 동등한 fixture로
   실패 테스트를 먼저 만든다.

## 구현 원칙

- 공개 `Row`, `ResultPage`, `ExecutionResult`의 기존 field를 제거하거나 의미를 바꾸지 않는다.
- server가 보낸 원본 JSON 값과 알 수 없는 logical type을 가능한 한 보존한다.
- 값이 없거나 malformed인 응답을 SQL NULL로 조용히 변환하지 않는다. protocol/decoding 오류로
  명확하게 반환한다.
- SQL NULL은 JSON literal `null`일 때만 NULL로 판정한다.
- JSON field 이름이나 응답 shape의 호환 표기가 필요하면 custom `UnmarshalJSON`에서 명시적으로
  처리하고 원본 응답을 보존한다.
- SQL 문자열, 인증 header, query string을 오류나 로그에 노출하지 않는다.
- 운영 Go source의 공개 식별자와 비자명한 동작 주석은 한글 Go doc 규칙을 따른다.
- 변경 파일은 patch로 수정하고 Go source는 `gofmt`한다.

## 필수 회귀 테스트

- 실제 non-NULL SELECT scalar 값이 `Row.Fields`와 `RowAccessor`에서 보존된다.
- 실제 SQL NULL은 `(value=nil, null=true)`로 반환된다.
- 빈 또는 누락 field가 NULL로 오인되지 않고 오류가 된다.
- 여러 page에서 column metadata와 row data가 분리되어 도착해도 결과 schema와 값이 연결된다.
- JSON과 PLAIN_TEXT fixture가 Flink 1.20.4 wire 계약대로 decoding된다.
- malformed JSON, column/field 개수 불일치와 알 수 없는 logical type 동작을 검증한다.

## 검증

최소 다음 명령을 실행한다.

```text
go test ./...
go vet ./...
```

실제 Gateway URL이 없으면 mock contract test를 사용하고 실제 Flink integration 검증이 남았음을
명시한다. 원인이 client가 아니라 호출자가 `Row.Fields` 또는 `RowAccessor`의 NULL boolean을
해석하는 방식이라면 불필요한 동작 변경을 하지 말고, 오용을 방지하는 문서와 테스트만 보강한다.

## 결과 보고

- client source 결함 여부와 정확한 근거
- 원본 response부터 공개 반환값까지 값이 변하는 지점
- 수정 파일과 하위 호환성 영향
- 실행한 테스트와 실제 Gateway 검증 여부
- 실제 NULL과 decoding 오류를 구분하는 호출 예제

