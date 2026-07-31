# Flink 2.3 SQL Gateway 지원

## 구현 상태

Flink 2.3.x profile은 REST v1~v4, v3 Materialized Table refresh, v4 Application Mode Script 배포와 Flink 2.3 SQL pass-through를 구현한다. `release-2.3.0` source와 공식 OpenAPI를 기준으로 단위·fixture 검증을 마쳤지만 실제 Flink 2.3.0 Gateway integration은 이 변경에서 실행하지 않았으므로 상태는 `experimental`, `testedVersions`는 빈 배열이다.

## Source와 OpenAPI 대조 결과

확인한 실제 source는 다음 클래스다.

- `RefreshMaterializedTableRequestBody`, `RefreshMaterializedTableResponseBody`
- `RefreshMaterializedTableHeaders`, `RefreshMaterializedTableHandler`
- `DeployScriptRequestBody`, `DeployScriptResponseBody`
- `DeployScriptHeaders`, `DeployScriptHandler`
- `ExecuteStatementRequestBody`

대조 결과:

| 항목 | `release-2.3.0` 실제 계약 | client 동작 |
| --- | --- | --- |
| Refresh path | v3에만 `/sessions/:session_handle/materialized-tables/:identifier/refresh` | 선택 API v3에서만 활성 |
| periodic key | field와 constructor 모두 `@JsonProperty("isPeriodic")` | `isPeriodic`만 전송 |
| OpenAPI 차이 | v3/v4 schema에 `isPeriodic`, `periodic` 동시 노출 | getter 유도 `periodic`은 전송하지 않음 |
| Script path | v4 이상 descriptor이며 2.3에서 v4 path에 노출 | 선택 API v4에서만 활성 |
| Script request | `script`, `scriptUri`, `executionConfig`; inactive source는 null, nil config는 `{}` | 동일 wire 형태 |
| Script response | `clusterID` string | 형식을 추측하지 않는 opaque 값 |
| `executionTimeout` | nullable millisecond `Long` | 2.3 v1~v4에서 양수 값 전송 |

OpenAPI v4에는 refresh request schema component가 생성되지만 refresh path는 없다. capability는 schema 존재가 아니라 MessageHeaders/OpenAPI path의 실제 endpoint 등록을 따른다.

공식 자료:

- [Flink 2.3 SQL Gateway REST](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/interfaces/sql-gateway/rest/)
- [OpenAPI v1](https://nightlies.apache.org/flink/flink-docs-release-2.3/generated/rest_v1_sql_gateway.yml), [v2](https://nightlies.apache.org/flink/flink-docs-release-2.3/generated/rest_v2_sql_gateway.yml), [v3](https://nightlies.apache.org/flink/flink-docs-release-2.3/generated/rest_v3_sql_gateway.yml), [v4](https://nightlies.apache.org/flink/flink-docs-release-2.3/generated/rest_v4_sql_gateway.yml)
- [Apache Flink 2.3.0 release announcement](https://flink.apache.org/2026/06/25/apache-flink-2.3.0-release-announcement/)
- [Apache Flink `release-2.3.0` tag](https://github.com/apache/flink/tree/release-2.3.0)

## API 선택

| 선택 | 결과 | 제공 기능 |
| --- | --- | --- |
| `APIVersionStable` | v3 | Materialized Table refresh |
| `APIVersionHighest` | v4 | Application Mode Script 배포 |
| Explicit v3 | v3 고정 | refresh, Script 배포 미지원 |
| Explicit v4 | v4 고정 | Script 배포, refresh 미지원 |

한 client는 하나의 선택 API만 사용한다. helper는 API를 몰래 변경하거나 fallback하지 않는다. 두 endpoint가 모두 필요하면 v3 client와 v4 client를 별도로 구성한다.

## 오류와 보안 계약

- 두 POST 모두 자동 재시도하지 않는다.
- 408, 429, 5xx, response header/body 유실과 유효하지 않은 성공 응답은 기능별 outcome-unknown error로 분류한다.
- `errors.Is`는 `ErrMaterializedTableRefreshOutcomeUnknown` 또는 `ErrScriptDeploymentOutcomeUnknown`, `errors.As`는 대응 typed error를 사용한다.
- SQL/Script 원문, Script URI, DynamicOptions, StaticPartitions, ExecutionConfig와 인증 header는 오류나 Observer에 포함하지 않는다.
- Materialized Table identifier는 SQL identifier로 quote하지 않고 REST path segment로 정확히 한 번 escape한다.
- Script URI는 Flink가 해석하도록 원문을 유지하며 scheme allow-list를 만들지 않는다.

## SQL pass-through

client는 SQL parser나 formatter가 아니다. 단위 테스트는 다음 Flink 2.3 문법이 `ExecuteStatementRequest.Statement`에서 변경되지 않는지 확인한다.

- `FROM_CHANGELOG`, `TO_CHANGELOG`
- 명시적 column/watermark/primary key를 포함한 `CREATE MATERIALIZED TABLE`
- `ALTER MATERIALIZED TABLE`과 `START_MODE`
- `ON CONFLICT DO NOTHING`, `DO ERROR`, `DO DEDUPLICATE`
- `CREATE FUNCTION ... USING ARTIFACT`
- Process Table Function table argument의 `ORDER BY`

## 실제 Gateway 실행

기본 matrix:

```powershell
.\integration\run-flink-2.3-matrix.ps1 -GatewayURL 'http://localhost:8083'
```

직접 실행:

```powershell
$env:FLINK_SQL_GATEWAY_URL = 'http://localhost:8083'
$env:FLINK_TEST_VERSION = '2.3.0'
$env:FLINK_TEST_RELEASE_LINE = '2.3'
$env:FLINK_TEST_API_VERSION = 'v3' # 또는 v4
go test -tags=integration -count=1 ./...
```

v3 refresh happy path는 `FLINK_TEST_MATERIALIZED_TABLE_IDENTIFIER`와 선택적인 `FLINK_TEST_MATERIALIZED_TABLE_*` JSON map 변수를 설정한다. v4 배포 happy path는 `FLINK_TEST_DEPLOY_SCRIPT` 또는 `FLINK_TEST_DEPLOY_SCRIPT_URI` 중 하나와 선택적인 `FLINK_TEST_DEPLOY_SCRIPT_EXECUTION_CONFIG`를 설정한다. 이 fixture는 catalog, connector, scheduler 또는 Application Mode cluster 준비가 필요하므로 기본 run에서는 skip 로그를 남긴다.

integration이 실제로 성공한 commit에서만 `compatibility.yaml`과 Go registry의 `testedVersions`에 `2.3.0`을 추가한다. 실행하지 않은 경우 문서에서 검증 완료로 표현하지 않는다.
