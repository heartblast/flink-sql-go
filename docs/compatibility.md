# Apache Flink 호환성 정책

이 문서는 `flinksqlgateway`가 Flink release line과 SQL Gateway REST API 버전을 선택하는 방법, 현재 검증 수준과 지원 상태를 정의한다. 공개 module과 import 경로는 Flink 버전과 관계없이 다음 하나를 유지한다.

```text
github.com/heartblast/flink-sql-go/flinksqlgateway
```

버전별 공개 package, 별도 `go.mod`, build tag 또는 장기 유지 브랜치를 만들지 않는다.

## Release line과 직접 검증한 patch

두 개념은 서로 다르다.

- release line은 같은 major/minor 계열을 표현한다. 예: Flink `1.20.x`, `2.3.x`
- tested patch는 실제 SQL Gateway를 실행해 통합 검증한 정확한 제품 버전이다. 예: `1.20.4`

profile이 존재한다는 사실만으로 해당 계열의 모든 patch를 실제 검증했다고 해석하면 안 된다. `SupportedFlinkVersions`와 `CompatibilityMatrix`의 `TestedVersions`가 비어 있으면 구현 모델은 있지만 직접 검증한 patch가 없다는 뜻이다.

## 현재 호환성 matrix

| Flink release line | 상태 | 직접 검증한 patch | profile REST API | Stable API | wire `executionTimeout` |
| --- | --- | --- | --- | --- | --- |
| 1.20.x | maintenance | 1.20.4 | v1, v2, v3 | v3 | false |
| 2.0.x | experimental | 없음 | v1, v2, v3, v4 | v3 | true |
| 2.1.x | experimental | 없음 | v1, v2, v3, v4 | v3 | true |
| 2.2.x | experimental | 없음 | v1, v2, v3, v4 | v3 | true |
| 2.3.x | experimental | 없음 | v1, v2, v3, v4 | v3 | true |

Flink 2.0.x~2.3.x의 `experimental` 표시는 release/API profile과 요청 encoder가 구현되었다는 뜻이다. 실제 2.x Gateway 검증 완료 또는 운영 안정성 보장을 뜻하지 않는다. 검증하지 않은 patch 버전은 문서, manifest 또는 빌드 산출물의 `TestedVersions`에 기록하지 않는다.

루트 `compatibility.yaml`이 release metadata의 기준이다. 빌드와 contract test는 Go registry 및 `flink-sql-go-<version>.compatibility.json` 산출물이 이 manifest와 일치하는지 검증한다. library 버전은 Flink 버전과 독립적이며 릴리스 하나가 위 release line 전체를 표현한다.

schema 2는 release capability와 protocol endpoint descriptor를 분리한다. REST API 번호가 증가해도 이전 endpoint가 그대로 존재한다고 추측하지 않는다.

| API | ConfigureSession | CompleteStatement | RowFormat | MaterializedTable | DeployScript | Flink 2.3 wire `executionTimeout` |
| --- | --- | --- | --- | --- | --- | --- |
| v1 | false | false | false | false | false | true |
| v2 | true | true | true | false | false | true |
| v3 | true | true | true | true | false | true |
| v4 | true | true | true | false | true | true |

Flink 1.20 release profile은 release quirk의 `WireExecutionTimeout=false`와 protocol descriptor를 교차하므로 모든 선택 API에서 field를 생략한다. 알 수 없는 API version은 모든 endpoint/wire capability를 false로 둔다.

## 기본 동작과 lazy 감지

기본 설정은 다음과 같다.

```go
flinksqlgateway.Config{
    BaseURL:            gatewayURL,
    CompatibilityMode: flinksqlgateway.CompatibilityAuto,
    APIVersionPolicy:  flinksqlgateway.APIVersionStable,
}
```

두 필드를 생략해도 같은 기본값을 사용한다. `NewClient`는 URL과 설정을 검증하고 client를 구성할 뿐 network를 호출하지 않는다. 최초 versioned 요청이나 `CheckCompatibility`, `GetCompatibilityInfo` 호출 시 다음 순서로 감지한다.

```text
1. GET /info
2. 제품 version에서 major.minor release line 파싱
3. release profile 선택
4. GET /api_versions
5. profile과 server가 광고한 REST API의 교집합 계산
6. APIVersionPolicy에 따라 한 버전 선택
7. 선택 결과와 capability를 client에 저장
```

성공 결과와 지원하지 않는 release/API처럼 결정적인 오류는 client에 저장한다. `/info` 또는 `/api_versions`의 transport 오류, context 취소와 deadline 초과는 영구 저장하지 않으므로 다음 호출에서 다시 감지할 수 있다. 동시에 여러 최초 요청이 들어와도 한 요청만 감지를 수행하고, 기다리는 호출자는 자신의 context로 대기를 취소할 수 있다.

감지 요청은 일반 요청과 같은 `BaseURL`, `http.Client`, header, `RequestTimeout`, `MaxResponseBytes`, 동일 origin redirect 및 관측 정책을 사용한다. 인증 header, URL query와 SQL을 오류나 Observer에 추가로 노출하지 않는다.

## 수동 release profile

Gateway의 release line을 이미 알고 있거나 proxy가 `/info`를 제공하지 않으면 mode를 명시할 수 있다.

```go
client, err := flinksqlgateway.NewClient(flinksqlgateway.Config{
    BaseURL:            gatewayURL,
    CompatibilityMode: flinksqlgateway.CompatibilityFlink120,
    APIVersionPolicy:  flinksqlgateway.APIVersionStable,
})
```

지원하는 selector는 `CompatibilityFlink120`, `CompatibilityFlink20`, `CompatibilityFlink21`, `CompatibilityFlink22`, `CompatibilityFlink23`이다. 수동 mode는 `/info`를 생략하지만 선택 API가 실제 server에 있는지 확인하기 위해 `/api_versions`는 최초 versioned 요청 시 호출한다. 수동 mode는 server 제품 버전을 검증했다는 뜻이 아니므로 배포 환경의 release line을 호출자가 정확히 관리해야 한다.

## REST API 선택 정책

### Stable

`APIVersionStable`은 profile의 stable 버전 이하인 공통 API 중 가장 높은 버전을 선택한다. 모든 현재 profile의 stable 기준은 v3이다.

- server가 v1, v2, v3, v4를 광고해도 v3 선택
- server가 v1, v2만 광고하면 v2 선택
- stable 기준 이하의 공통 버전이 없으면 `ErrNoCompatibleAPIVersion`

Stable은 검증 없이 최신 protocol로 자동 상승하지 않는 기본 정책이다.

### Highest

`APIVersionHighest`는 profile과 server의 공통 버전 중 숫자가 가장 높은 버전을 선택한다.

```go
APIVersionPolicy: flinksqlgateway.APIVersionHighest
```

2.x profile과 server가 모두 v4를 제공하면 v4를 선택한다. 하지만 profile 상태가 `experimental`이라는 사실은 바뀌지 않으며 Highest 선택 자체가 실제 Gateway 검증을 의미하지 않는다.

### Explicit

`APIVersionExplicit`은 `APIVersion`에 지정한 버전만 사용하며 fallback하지 않는다.

```go
flinksqlgateway.Config{
    BaseURL:           gatewayURL,
    APIVersionPolicy: flinksqlgateway.APIVersionExplicit,
    APIVersion:       "v3",
}
```

profile이 허용하지 않으면 `ErrExplicitAPIVersionUnsupportedByProfile`, server가 광고하지 않으면 `ErrExplicitAPIVersionUnsupportedByServer`를 반환한다. 기존 공개 사용법인 `Config{APIVersion: "v3"}`은 policy가 생략되어도 하위 호환성을 위해 Explicit으로 해석한다.

## 선택 결과와 capability 조회

```go
info, err := client.GetCompatibilityInfo(ctx)
if err != nil {
    return err
}

fmt.Println(info.FlinkVersion)
fmt.Println(info.ReleaseLine)
fmt.Println(info.APIVersion)
fmt.Println(info.DetectionSource)

if info.Capabilities.ConfigureSession {
    // /configure-session 사용 가능
}
```

`DetectionSourceAuto`는 `/info`에서 release를 감지했음을, `DetectionSourceConfigured`는 명시한 mode를 사용했음을 나타낸다. 수동 mode에서는 정확한 patch를 조회하지 않으므로 `FlinkVersion`이 비어 있을 수 있다.

registry 전체는 network 없이 조회할 수 있다.

```go
releases := flinksqlgateway.SupportedFlinkVersions()
matrix := flinksqlgateway.CompatibilityMatrix()
```

`GetCompatibilityInfo`, `Capabilities`, `SupportedFlinkVersions`, `CompatibilityMatrix`는 내부 slice나 registry와 memory를 공유하지 않는 복사본을 반환한다. 반환값을 변경해도 client의 선택 결과는 바뀌지 않는다.

## Capability와 executionTimeout

Capability는 release profile과 선택한 REST protocol의 교집합이다. 호출자는 API 버전 문자열을 직접 비교하는 대신 `Capabilities`를 확인할 수 있다.

- `ConfigureSession`, `CompleteStatement`, `RowFormat`
- `MaterializedTable`, `DeployScript`
- `WireExecutionTimeout`

`ExecutionTimeout`은 모든 profile에서 client-side context 제한으로 적용된다. Flink 1.20 profile은 `WireExecutionTimeout=false`이므로 statement와 configure-session JSON에서 `executionTimeout` field 자체를 생략한다. Flink 2.3은 v1을 포함한 v1~v4에서 capability가 참이고 값이 양수일 때 millisecond wire field도 전송한다. wire 전송 여부와 관계없이 client-side timeout, polling 상한과 cleanup 제한은 유지된다.

기능을 제공하지 않는 profile/protocol 조합에서 요청하면 `ErrUnsupportedCapability`를 반환하며 해당 기능 요청은 보내지 않는다.

Stable v3는 Materialized Table refresh를 제공하지만 Deploy Script를 제공하지 않는다. Highest v4는 Deploy Script를 제공하지만 Materialized Table refresh를 제공하지 않는다. helper가 선택 API를 바꾸거나 다른 버전으로 fallback하지 않으므로 두 기능이 모두 필요한 운영자는 같은 Gateway를 가리키는 v3/v4 client를 각각 구성해야 한다.

## Typed error 처리

Compatibility 실패는 `errors.Is`로 분류하고 `errors.As`로 안전한 context를 확인할 수 있다.

| 오류 | 의미 |
| --- | --- |
| `ErrInvalidFlinkVersion` | `/info.version`에서 release line을 파싱할 수 없음 |
| `ErrUnsupportedFlinkVersion` | 파싱한 release line에 등록된 profile이 없음 |
| `ErrNoCompatibleAPIVersion` | profile과 server 사이에 선택 가능한 API가 없음 |
| `ErrExplicitAPIVersionUnsupportedByServer` | Explicit 버전을 server가 광고하지 않음 |
| `ErrExplicitAPIVersionUnsupportedByProfile` | Explicit 버전을 profile이 허용하지 않음 |
| `ErrUnsupportedCapability` | 선택한 release/protocol이 기능을 제공하지 않음 |
| `ErrCompatibilityDetection` | `/info` 또는 `/api_versions` 요청이 완료되지 않음 |
| `ErrMaterializedTableRefreshOutcomeUnknown` | refresh POST가 처리됐을 수 있으나 operation handle을 확인하지 못함 |
| `ErrScriptDeploymentOutcomeUnknown` | deploy POST가 처리됐을 수 있으나 clusterID를 확인하지 못함 |

```go
info, err := client.GetCompatibilityInfo(ctx)
if err != nil {
    var compatibilityErr *flinksqlgateway.CompatibilityError
    switch {
    case errors.Is(err, flinksqlgateway.ErrUnsupportedFlinkVersion):
        // 지원하지 않는 release line
    case errors.Is(err, flinksqlgateway.ErrNoCompatibleAPIVersion):
        // 공통 REST API 없음
    case errors.Is(err, flinksqlgateway.ErrCompatibilityDetection):
        // context/transport 원인을 확인하고 안전한 정책으로 재시도 가능
    }
    if errors.As(err, &compatibilityErr) {
        _ = compatibilityErr.ReleaseLine
        _ = compatibilityErr.APIVersion
    }
    return err
}
_ = info
```

`CompatibilityError`는 SQL, 인증 header와 URL query를 보관하지 않으며 원인이 된 context 또는 `APIError`를 unwrap한다. 기존 `ErrUnsupportedAPI` 분류도 API/capability 오류에서 유지한다.

## 지원 상태 정의

| 상태 | 정의 |
| --- | --- |
| `planned` | 지원 범위만 계획되었고 사용할 구현이 아직 없음 |
| `experimental` | profile 또는 protocol 구현은 있으나 실제 Gateway 검증과 운영 근거가 충분하지 않음 |
| `supported` | 구현과 정기적인 실제 Gateway 검증을 유지함 |
| `maintenance` | 기존 사용자의 회귀와 보안 수정 중심으로 유지함 |
| `unsupported` | 새 사용을 지원하지 않으며 제거 또는 종료 절차 대상임 |

상태와 `TestedVersions`는 독립적이다. 예를 들어 experimental profile에 fixture test가 있어도 실제 Gateway를 실행하지 않았다면 tested patch를 기록하지 않는다.

## 지원 종료 정책

release line의 상태 변경은 library SemVer와 분리해 관리한다.

1. `supported` 또는 `experimental`에서 바로 제거하지 않고 먼저 `maintenance` 또는 `unsupported`로 전환한다.
2. 상태 변경, 영향받는 capability와 대체 release를 릴리스 노트에 기록한다.
3. `TestedVersions`는 실제 검증 이력으로 유지하되 현재 지원 상태와 혼동하지 않게 표시한다.
4. 공개 상수, Config 값이나 동작 제거는 library의 하위 호환성 정책에 맞는 릴리스에서만 수행한다.
5. 보안상 즉시 차단이 필요한 예외는 근거와 영향을 별도 공지한다.

구체적인 library tag와 산출물 정책은 [release-policy.md](release-policy.md)를 따른다.

## Flink 2.3.x와 이후 release 추가 범위

Flink 2.3.x를 표현하기 위해 전체 client를 복사하지 않고 다음 범위를 추가했다.

- `ReleaseLine`과 `CompatibilityMode` selector
- `compatibility.yaml` schema 2의 release profile과 정확한 API별 descriptor
- v3 `MaterializedTableRefresher`와 기존 `Operation` lifecycle 연계
- v4 `ScriptDeployer`와 opaque `ScriptDeployment.ClusterID`
- 기능별 outcome-unknown typed error와 secret redaction
- profile/API 선택, v1 포함 `executionTimeout`, SQL pass-through 단위 테스트
- 문서와 compatibility 산출물 항목

2.3.x의 특정 patch를 실제 검증한 뒤에는 해당 환경의 `/info` version 일치, session/operation/result/cleanup contract와 보안 회귀를 통과시킨 후에만 `TestedVersions`에 추가한다. 현재는 검증된 2.3.x patch가 없다.

향후 2.4.x 같은 새 release line을 추가할 때도 같은 절차를 사용한다. 기존 DTO와 endpoint가 같으면 profile entry와 test만 추가하고, 실제 wire 차이가 있을 때만 private encoder 또는 protocol 구현을 확장한다. 공개 import 경로, `Client` interface와 기존 1.20 동작은 유지한다.
