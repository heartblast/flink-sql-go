# flink-sql-go

Apache Flink 1.20.4 SQL Gateway 전용 Go REST Client입니다. JDBC, JVM, GraalVM, CGO, `database/sql` 없이 표준 `net/http`로 동작합니다.

현재 모듈 경로는 저장소 초기화를 위해 `flink-sql-go`로 설정되어 있습니다. 원격 Git 저장소를 정한 뒤 실제 경로로 바꾸십시오.

```powershell
go mod edit -module github.com/<owner>/flink-sql-go
go mod tidy
git remote add origin <repository-url>
```

Go 1.26.5를 기준으로 개발합니다. 런타임 Go 패키지 의존성은 없으며, 빌드 도구로 고정된 `govulncheck`만 별도 module graph에 포함됩니다.

## 빌드와 버전 관리

PowerShell 5.1 이상에서 루트 빌드 스크립트를 실행합니다.

```powershell
.\build.ps1
```

스크립트는 다음 순서로 실행됩니다.

1. `go.mod`에 선언된 Go 버전과 실제 toolchain 일치 확인
2. Git tag와 `VERSION`을 이용한 안정 SemVer 및 `FLINK_VERSION` 확인
3. `go mod verify`, `go mod tidy -diff`, `gofmt`, `go vet`
4. 단위 테스트와 coverage, 전체 package compile
5. `govulncheck` symbol scan 및 전체 module graph scan
6. 라이브러리/Flink 버전 suffix가 포함된 소스 ZIP, build-info, 의존성 목록, SHA-256 생성

라이브러리 버전 결정 우선순위는 `-Version`, `BUILD_VERSION`, 현재 commit의 정확한 `v*` tag, `VERSION` 순입니다. 기본 빌드는 `-dev`를 붙이지 않으며 현재 기준 산출물 이름은 `flink-sql-go-0.1.0-flink-1.20.4-*`입니다. Git commit과 dirty 상태는 버전 문자열 대신 build-info에 기록됩니다.

```powershell
# 명시적 preview 버전
.\build.ps1 -Version 0.1.0-rc.1

# 다른 Flink patch release용 별도 검증 빌드
.\build.ps1 -FlinkVersion 1.20.5

# 깨끗한 worktree와 tag/명시 버전을 요구하는 릴리스 빌드
.\build.ps1 -Release

# C compiler가 준비된 환경에서 race detector까지 실행
.\build.ps1 -Race
```

기본 지원 Flink 버전은 루트 `FLINK_VERSION`에서 관리합니다. CI에서는 `SUPPORTED_FLINK_VERSION` 환경변수로 덮어쓸 수도 있습니다.

기본 보안 gate는 취약점 또는 scan 오류가 하나라도 있으면 빌드를 실패시킵니다. 조사 목적으로만 비릴리스 빌드에 `-AllowVulnerabilities`를 사용할 수 있으며, 결과 manifest의 `securityGatePassed`는 `false`로 기록됩니다. 릴리스 빌드에서는 이 우회를 허용하지 않습니다.

Go 1.26.5에는 이전 빌드에서 검출된 표준 라이브러리 취약점 `GO-2026-5856`, `GO-2026-4970`의 수정이 포함됩니다. 상세한 빌드·배포 절차는 [docs/build.md](docs/build.md)를 참고하십시오.

`v0.1.0`의 기능, 호환성, 검증 결과와 공개 전 확인사항은 [릴리스 노트](docs/releases/v0.1.0.md)에 정리되어 있습니다.

## 주요 기능

- `/info`, `/api_versions` 조회 및 선택 API 버전의 lazy 검증
- 세션 생성·설정 조회·heartbeat·종료
- v2+ 세션 구성 및 SQL 자동완성
- SQL 제출, Operation 상태·취소·종료
- `NOT_READY` → `PAYLOAD` → `EOS` 결과 paging
- JSON/PLAIN_TEXT(v2+) 결과와 모든 RowKind 보존
- 수집형 `ExecuteAndWait`와 bounded channel형 `StreamResults`
- goroutine 없이 페이지를 가져오는 iterator형 `ExecuteStream`
- heartbeat·건강 상태·종료를 소유하는 `ManagedSession`
- context 취소 가능한 세션별 `SerializedSession`
- SQL 제출 결과 불명확 상태를 구분하는 typed error
- 독립된 `flinkrest` JobManager REST client
- client/stream/session의 중복 호출에 안전한 `Close`
- 명시적 세션 재구성을 위한 `SessionRecipe`
- 정밀도 보존 DECIMAL·temporal·nested 타입 Decoder와 Row helper
- 식별자 quoting이 적용된 Metadata Helper와 Capability API
- 복합 PK와 bounded snapshot을 지원하는 `changelog.Materializer`
- context timeout/취소, 행 수·poll 횟수·응답 크기 제한
- 동일 origin의 `nextResultUri`만 허용하는 SSRF/헤더 유출 방어
- SQL 실행 POST 등 비멱등 요청의 자동 재시도 차단
- 주입형 `StatementValidator`, `Observer`, `http.Client`, 헤더/Transport 지원

## 빠른 사용 예제

```go
package service

import (
    "context"
    "time"

    "flink-sql-go/flinksqlgateway"
)

func query(ctx context.Context) error {
    client, err := flinksqlgateway.NewClient(flinksqlgateway.Config{
        BaseURL:             "https://flink-gateway.internal:8083",
        APIVersion:          "v3",
        RequestTimeout:      10 * time.Second,
        ExecutionTimeout:    30 * time.Second,
        PollInterval:        300 * time.Millisecond,
        MaxPollInterval:     3 * time.Second,
        MaxResultRows:       1000,
        MaxResponseBytes:    8 << 20,
        DefaultRowFormat:    flinksqlgateway.RowFormatJSON,
        CancelOnContextDone: true,
    })
    if err != nil {
        return err
    }
    defer client.Close()

    session, err := client.OpenSession(ctx, flinksqlgateway.OpenSessionRequest{
        SessionName: "workspace-user-123",
        Properties: map[string]string{
            "execution.runtime-mode": "streaming",
        },
    })
    if err != nil {
        return err
    }
    defer func() {
        _ = client.CloseSession(context.Background(), session.Handle)
    }()

    // 선택 사항입니다. 동일 세션에 중복 runner를 만들지 않으며,
    // CloseSession이 runner를 먼저 중지합니다.
    if _, err := client.StartHeartbeat(ctx, session.Handle); err != nil {
        return err
    }

    result, err := client.ExecuteAndWait(
        ctx,
        session.Handle,
        "SHOW TABLES",
        flinksqlgateway.ExecuteOptions{MaxRows: 1000},
    )
    if err != nil {
        return err
    }

    for _, row := range result.Rows {
        // row.Kind는 INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE 중 하나이며
        // row.Fields는 손실 없는 json.RawMessage 배열입니다.
        _ = row
    }
    return nil
}
```

원격 저장소의 module path로 변경한 뒤 import 경로도 함께 바꾸면 됩니다.

## Managed Session과 직렬 실행

장시간 세션은 heartbeat lifecycle과 건강 상태를 한 객체에서 관리할 수 있습니다. SQL 실행 context가 취소되어도 heartbeat는 유지되며, 세션 만료 시 자동으로 새 세션을 만들지 않습니다.

```go
managed, err := client.OpenManagedSession(
    ctx,
    flinksqlgateway.OpenSessionRequest{SessionName: "workspace-user-123"},
    flinksqlgateway.ManagedSessionOptions{
        HeartbeatInterval: 30 * time.Second,
        FailureThreshold:  3,
        CleanupTimeout:    10 * time.Second,
        Serialize:         true,
    },
)
if err != nil {
    return err
}
defer managed.Close(context.Background())

result, err := managed.Execute(ctx, "SHOW TABLES", flinksqlgateway.ExecuteOptions{})
```

이미 생성된 세션만 직렬화하려면 `NewSerializedSession(client, sessionHandle)`을 사용할 수 있습니다. 직렬화는 해당 wrapper의 세션에만 적용되며 다른 세션의 실행은 차단하지 않습니다.

## Iterator 결과

대량 결과를 channel producer 없이 순차 처리하려면 `ExecuteStream`을 사용합니다. `Close`를 defer하여 소비를 중간에 중단한 경우에도 Operation을 정리하십시오.

```go
rows, err := client.ExecuteStream(
    ctx,
    session.Handle,
    "SELECT * FROM changelog_table",
    flinksqlgateway.StreamOptions{
        ExecuteOptions: flinksqlgateway.ExecuteOptions{MaxRows: 10_000},
    },
)
if err != nil {
    return err
}
defer rows.Close()

for rows.Next() {
    row := rows.Row()
    _ = row
}
if err := rows.Err(); err != nil {
    return err
}
jobID := rows.JobID()
_ = jobID
```

## Flink Job REST Companion

JobManager REST 주소는 SQL Gateway 주소와 별도로 설정합니다. SQL Operation 취소는 Job 취소를 자동 실행하지 않습니다.

```go
jobClient, err := flinkrest.NewClient(flinkrest.Config{
    BaseURL:       "https://flink-jobmanager.internal:8081",
    ValidateJobID: true,
})
if err != nil {
    return err
}
defer jobClient.Close()

if result.JobID != "" {
    job, err := jobClient.GetJob(ctx, result.JobID)
    if err != nil {
        return err
    }
    _ = job
}
```

위 예제는 `"flink-sql-go/flinkrest"`를 import해야 하며, 원격 저장소 경로 확정 후 실제 module path로 바꾸어야 합니다.

## Session Recipe

세션이 만료된 뒤 호출자가 명시적으로 상태를 재구성할 때 사용합니다. 자동 복구는 수행하지 않으며 setup 문장은 입력 순서대로 실행됩니다.

```go
replay, err := client.OpenSessionFromRecipe(ctx, flinksqlgateway.SessionRecipe{
    Name:       "workspace-user-123",
    Properties: map[string]string{"execution.runtime-mode": "streaming"},
    SetupStatements: []string{
        "USE CATALOG kafka_catalog",
        "USE kafka_database",
    },
})
if err != nil {
    // replay.FailedIndex와 replay.Applied로 성공 범위를 확인합니다.
    return err
}
defer client.CloseSession(context.Background(), replay.SessionHandle)
```

기본 정책은 중간 실패 시 생성한 세션을 닫습니다. 세션을 조사 목적으로 유지하려면 `OpenSessionFromRecipeWithOptions`와 `KeepSessionOnFailure`를 명시하십시오. 오류와 Observer에는 setup SQL 원문이 포함되지 않습니다.

## 타입 Decoder와 Row helper

기본 `Row.Fields`는 계속 `json.RawMessage`입니다. 변환은 명시적으로 helper를 호출할 때만 수행됩니다.

```go
decoded := row.WithColumns(result.Columns, nil) // nil은 DefaultValueDecoder 사용
name, isNull, err := decoded.String("service_name")
code, _, err := decoded.Int64("status_code")
amount, _, err := decoded.Decimal("amount") // 정확한 문자열 기반 Decimal
eventTime, _, err := decoded.Time("event_time")
raw, _, err := decoded.Raw("future_type")
```

컬럼명과 인덱스 접근을 모두 지원합니다. 중복 컬럼명은 첫 번째 컬럼을 선택하고, SQL NULL은 별도 `isNull` 값으로 구분합니다. `TIMESTAMP`는 `LocalTimestamp`, `TIMESTAMP_LTZ`는 `TimestampLTZ`로 의미를 구분하며 알 수 없는 미래 타입은 raw JSON으로 반환합니다.

## Metadata와 Capability

Metadata helper는 새 세션을 만들지 않고 전달받은 세션의 현재 catalog/database 상태를 사용합니다.

```go
caps, err := client.Capabilities(ctx)
tables, err := client.ListTables(ctx, session.Handle, flinksqlgateway.Identifier{
    Catalog:  "catalog-name",
    Database: "database-name",
})
columns, err := client.DescribeTable(ctx, session.Handle, flinksqlgateway.Identifier{
    Catalog: "catalog-name", Database: "database-name", Object: "orders",
})
plan, err := client.Explain(ctx, session.Handle, "SELECT * FROM orders")
```

식별자는 `QuoteIdentifier`로 backtick quoting되며 내부 backtick은 두 번 써서 escape합니다. 알 수 없는 미래 API 버전의 capability는 지원을 추측하지 않고 보수적으로 `false`를 반환합니다.

## Changelog Materializer

Flink changelog를 현재 테이블 snapshot으로 변환해야 할 때만 별도 패키지를 사용합니다.

```go
materializer, err := changelog.NewMaterializer(
	changelog.PrimaryKey("tenant_id", "id"),
	changelog.Columns(result.Columns),
	changelog.MaxRows(100_000),
)
if err != nil {
    return err
}
if err := materializer.Apply(row); err != nil {
    return err
}
snapshot := materializer.Snapshot()
```

PK 또는 컬럼 메타데이터 없는 materialization은 거부하며 `UPDATE_BEFORE`/`UPDATE_AFTER` 순서를 검증합니다. 최대 행을 넘으면 임의 eviction 없이 오류를 반환하고 snapshot은 내부 상태의 deep copy입니다.

## 스트리밍 결과

무한 또는 대량 SELECT는 `StreamResults`를 사용하십시오. 채널 buffer는 제한되며 소비를 중단할 때 반드시 context를 취소해야 합니다.

```go
streamCtx, cancel := context.WithCancel(ctx)
defer cancel()

events, errs := client.StreamResults(
    streamCtx,
    session.Handle,
    "SELECT * FROM changelog_table",
    flinksqlgateway.StreamOptions{
        ExecuteOptions: flinksqlgateway.ExecuteOptions{MaxRows: 10_000},
        Buffer:         32,
    },
)

for event := range events {
    if event.Type == flinksqlgateway.ResultEventRow {
        // event.Row.Kind와 event.Row.Fields 처리
    }
}
if err := <-errs; err != nil {
    return err
}
```

## Flink 1.20.4 API 특성

Flink 1.20.4 소스 태그와 공식 OpenAPI를 기준으로 구현했습니다.

| 항목 | 확인 내용 |
| --- | --- |
| 지원 버전 | `V1`, `V2`, `V3`; 서버 기본은 `v3` |
| 공통 경로 | `/info`, `/api_versions`는 버전 prefix 없음 |
| 버전 경로 | 나머지는 `/v1/...`, `/v2/...`, `/v3/...` |
| v1 | 기본 세션/Operation/결과 API, 결과는 기본 JSON |
| v2 | `configure-session`, `complete-statement`, `rowFormat` 추가 |
| v3 | Materialized Table API 추가(이 모듈 범위 밖) |
| Operation 상태 | `INITIALIZED`, `PENDING`, `RUNNING`, `FINISHED`, `CANCELED`, `CLOSED`, `ERROR`, `TIMEOUT` |
| 결과 | `resultType`, `isQueryResult`, `jobID`, `resultKind`, `results`, `nextResultUri` |
| 행 | `{"kind":"INSERT","fields":[...]}` 형태 |
| next URI | v1은 rowFormat query 없음, v2+는 `?rowFormat=...` 포함 |

OpenAPI는 결과 body 일부를 `any`로 표시하고 `queryResult`라고 기술하는 부분이 있지만, 실제 1.20.4 serializer는 `isQueryResult`, `results.columns`, `data[].kind`, `data[].fields`를 사용합니다. 클라이언트는 실제 serializer 형식을 우선하며 `queryResult`도 호환 입력으로 허용합니다.

공식 자료:

- [Flink 1.20 SQL Gateway REST Endpoint](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql-gateway/rest/)
- [Flink 1.20 v1 OpenAPI](https://nightlies.apache.org/flink/flink-docs-release-1.20/generated/rest_v1_sql_gateway.yml)

## 재시도와 정리 정책

다음 요청만 네트워크/일시 HTTP 오류에 한해 최대 1회 자동 재시도합니다.

- `/info`, `/api_versions`
- 세션 설정 조회와 자동완성
- Operation 상태 및 결과 조회
- heartbeat

세션 생성, SQL 실행, 세션 구성 SQL, Operation 취소/종료, 세션 종료는 자동 재시도하지 않습니다. 특히 SQL 실행 POST는 응답 유실 시 이미 Job이 제출됐을 수 있으므로 절대 자동 재호출하지 않습니다. 이 경우 `errors.Is(err, flinksqlgateway.ErrExecutionOutcomeUnknown)` 또는 `errors.As`로 상태를 판별할 수 있습니다.

Operation handle을 받은 뒤 context가 취소되면 `CancelOnContextDone` 설정에 따라 별도 cleanup context로 취소를 요청하고, 원래 context 오류를 그대로 반환합니다. 결과 제한 도달 시에는 취소 후 Operation을 닫습니다. 세션 만료 시 새 세션을 몰래 생성하지 않습니다.

## 보안과 동시성

- 기본 TLS 검증을 끄지 않습니다. mTLS나 인증은 주입한 `http.Client.Transport`/`Headers`에서 구성합니다.
- redirect와 `nextResultUri`는 최초 Gateway와 scheme/host/port가 같은 경우만 허용합니다.
- 오류와 Observer endpoint에는 query string, SQL, 인증 헤더가 포함되지 않습니다.
- `GatewayClient`는 여러 goroutine에서 공유할 수 있습니다.
- `Session`과 `Operation`은 immutable handle 모델이라 동시 읽기가 안전합니다.
- 같은 세션의 기본 병렬 호출은 유지됩니다. `USE`, `SET`, temporary object처럼 순서가 중요한 문장은 `SerializedSession` 또는 `ManagedSessionOptions.Serialize`로 직렬화할 수 있습니다.
- 사용자/워크스페이스 소유권과 SQL 권한은 `StatementValidator`에서 검사하십시오. 클라이언트가 제품 정책을 추측하지 않습니다.

## 테스트

```powershell
go test ./...
go vet ./...
go test -race ./...
```

Windows의 race detector는 검사 도구 실행에 CGO와 C compiler가 필요합니다. 라이브러리 자체는 CGO를 import하거나 링크하지 않습니다.

실제 Flink 1.20.4 SQL Gateway 통합 테스트는 다음 환경변수로 실행합니다.

```powershell
$env:FLINK_SQL_GATEWAY_URL = 'http://localhost:8083'
$env:FLINK_SQL_GATEWAY_API_VERSION = 'v3'
go test -tags=integration ./...
```

환경변수가 없으면 integration test는 skip됩니다.

## 현재 범위 밖

- Materialized Table v3 API
- reverse-proxy URI rewrite resolver
- 전체 API version contract CI, SBOM, provenance 자동화
- 사용자 인증 시스템 및 SQL parser/권한 정책
- JDBC/JVM/GraalVM/CGO/database/sql
- Kafka/Elasticsearch 연결 검증과 UI

상세 설계는 [docs/design.md](docs/design.md)를 참고하십시오.
