# 빌드, 버전 및 호환성 산출물

## 필수 환경

- `go.mod`에 선언된 버전과 동일한 Go toolchain
- PowerShell 5.1 이상
- Git: commit, exact tag, dirty 상태 기록과 릴리스 gate에 사용
- 기본 취약점 검사 시 `https://vuln.go.dev` 접근 권한
- `-Race` 사용 시 Windows용 C compiler와 CGO

`govulncheck`는 Go tool directive로 고정되어 있으므로 별도 전역 설치 없이 `go tool govulncheck`로 실행한다.

## 버전의 단일 원천

라이브러리 버전은 루트 `VERSION`과 `flinksqlgateway.SourceVersion`에 기록하고 테스트로 일치를 강제한다. v0.2.0부터 라이브러리 버전은 Flink 제품 버전과 완전히 분리한다.

Flink 호환성의 단일 원천은 루트 `compatibility.yaml`이다. 이 파일은 YAML 1.2에서 유효한 JSON 문법을 사용하므로 build에는 YAML parser나 런타임 외부 의존성이 필요하지 않다. 다음 정보를 한곳에서 관리한다.

- 기본 Flink release line과 Stable REST API 버전
- release line별 지원 상태
- 직접 검증한 patch 버전
- 지원하는 REST API 버전과 Stable 버전
- endpoint 및 wire quirk capability

`SupportedFlinkVersion`과 `BuildInfo.FlinkVersion`은 기존 호출자의 source 호환성을 위해 `1.20.4`를 유지하는 Deprecated 필드다. 신규 코드는 `SupportedFlinkVersions`, `CompatibilityMatrix` 또는 `BuildInfo.SupportedFlinkReleaseLines`을 사용한다.

release line 지원과 patch 검증은 다른 의미다. 예를 들어 `2.0.x`를 experimental profile로 표현할 수 있어도 실제 Gateway 검증 전에는 `testedVersions`가 비어 있어야 한다.

## 일반 빌드

```powershell
.\build.ps1
```

빌드는 다음 순서로 실행한다.

1. Go toolchain 및 `VERSION` 검증
2. `compatibility.yaml` schema, release line, API 버전, capability 검증
3. Git commit과 dirty 상태 수집
4. `go mod verify`, `go mod tidy -diff`, `gofmt`, `go vet`
5. 단위·fixture 테스트, integration-tagged package compile, 전체 package build
6. reachable symbol 및 전체 module graph 취약점 검사
7. source ZIP, build-info, compatibility manifest, module 목록, coverage와 checksum 생성

라이브러리 버전 선택 우선순위는 `-Version`, `BUILD_VERSION`, HEAD의 단일 exact `v*` tag, `VERSION` 순이다. preview build에는 다음처럼 명시할 수 있다.

```powershell
.\build.ps1 -Version 0.2.1-rc.1
.\build.ps1 -Race
```

v0.2.0부터 `-FlinkVersion`과 `SUPPORTED_FLINK_VERSION`으로 배포물의 의미를 바꾸지 않는다. 특정 Gateway를 검증할 버전은 integration test의 `FLINK_TEST_*` 환경변수로 전달한다.

## 취약점 gate

두 검사를 모두 통과해야 기본 build가 성공한다.

```powershell
go tool govulncheck -test ./...
go tool govulncheck -C flinksqlgateway -scan=module
```

사내 mirror나 offline database는 `-VulnerabilityDatabase`로 지정한다. 조사 목적의 비릴리스 build만 `-AllowVulnerabilities`를 사용할 수 있고, 이 경우 build-info의 `securityGatePassed`는 `false`다. 릴리스에서는 이 우회를 허용하지 않는다.

## 릴리스 build

v0.2.0 릴리스는 깨끗한 `v0.2.0` exact tag에서 실행한다.

```powershell
git status --short
git tag -a v0.2.0 -m "v0.2.0"
.\build.ps1 -Release
```

`-Release`는 다음을 모두 강제한다.

- committed Git HEAD
- `VERSION`과 정확히 같은 단 하나의 `v<version>` tag
- resolve된 library version과 `VERSION`의 일치
- 완전히 깨끗한 worktree
- `-AllowDirty`와 `-AllowVulnerabilities` 사용 금지

`v0.2.0-flink-1.20`, `v2.0.0` 같은 tag는 만들지 않는다. 전자는 library와 Flink 버전을 다시 결합하고, 후자는 Go module major version 2를 뜻하기 때문이다.

## 산출물

산출물 이름에는 Flink patch suffix를 넣지 않는다.

| 파일 | 내용 |
| --- | --- |
| `flink-sql-go-<version>-source.zip` | source, canonical manifest와 생성된 JSON manifest |
| `flink-sql-go-<version>.build-info.json` | library version, 전체 release line, commit, toolchain, security gate |
| `flink-sql-go-<version>.compatibility.json` | 배포 시점의 release line, tested patch, API와 capability snapshot |
| `flink-sql-go-<version>.modules.txt` | module version과 checksum 목록 |
| `flink-sql-go-<version>.govulncheck.txt` | reachable symbol 검사 |
| `flink-sql-go-<version>.govulncheck-modules.txt` | 전체 module graph 검사 |
| `flink-sql-go-<version>.coverage.out` | coverage profile |
| `flink-sql-go-<version>.sha256` | 위 주요 산출물의 SHA-256 |

build-info schema 3은 기존 `version` 필드와 함께 `libraryVersion`, `defaultFlinkReleaseLine`, `defaultApiVersion`, `supportedFlinkReleaseLines`을 기록한다. compatibility JSON은 `compatibility.yaml`에서 직접 만들며, source ZIP과 checksum에도 포함한다.

## 실제 Gateway 검증

```powershell
$env:FLINK_SQL_GATEWAY_URL = 'http://localhost:8083'
$env:FLINK_TEST_VERSION = '1.20.4'
$env:FLINK_TEST_RELEASE_LINE = '1.20'
$env:FLINK_TEST_API_VERSION = 'v3'
go test -tags=integration -count=1 ./...
```

PR CI는 네트워크가 필요 없는 단위·fixture contract test와 integration-tagged compile을 수행한다. 예약 workflow는 실제 endpoint secret이 있는 `testedVersions`만 matrix에 포함한다. endpoint가 없으면 실패하며, 검증하지 않은 2.x를 통과로 기록하지 않는다.
