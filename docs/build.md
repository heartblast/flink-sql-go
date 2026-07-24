# 빌드, 버전, 취약점 관리

## 필수 환경

- Go `go.mod` 선언 버전과 동일한 toolchain
- PowerShell 5.1 이상
- Git 권장: tag, commit, dirty 상태 기록에 사용
- `-Race` 사용 시 Windows용 C compiler
- 기본 취약점 검사 시 `https://vuln.go.dev` 접근 권한

`govulncheck`는 Go 1.24+의 `tool` directive로 `go.mod`에 고정되어 있다. 별도 전역 설치 대신 다음 명령과 동일한 도구를 빌드가 사용한다.

```powershell
go tool govulncheck -version
```

## 버전 규칙

기준 release line은 루트 `VERSION`과 `flinksqlgateway.SourceVersion`에 함께 기록되며 테스트가 두 값을 비교한다. 지원 Flink release는 `FLINK_VERSION`과 `flinksqlgateway.SupportedFlinkVersion`에 기록한다.

빌드 version 우선순위:

1. `-Version 1.2.3`
2. `$env:BUILD_VERSION`
3. 현재 HEAD를 가리키는 `v1.2.3` 형식 tag
4. `VERSION`

기본 빌드도 `VERSION`의 안정 버전(현재 `0.1.0`)을 사용하고 `-dev`를 자동 추가하지 않는다. commit과 dirty 여부는 build-info에서 추적한다. 모든 version은 SemVer 2.0.0 검증을 통과해야 한다.

지원 Flink version 우선순위:

1. `-FlinkVersion 1.20.4`
2. `$env:SUPPORTED_FLINK_VERSION`
3. 루트 `FLINK_VERSION`

배포 산출물에는 `-flink-<version>` suffix가 붙는다. 예: `flink-sql-go-0.1.0-flink-1.20.4-source.zip`.

빌드 중 다음 linker metadata가 주입된다.

- `flinksqlgateway.Version()`
- `flinksqlgateway.GetBuildInfo().FlinkVersion`
- `flinksqlgateway.GetBuildInfo().Commit`
- RFC 3339 build time
- dirty worktree 여부

라이브러리 source를 빌드 스크립트 밖에서 직접 compile해도 `SourceVersion`과 `SupportedFlinkVersion`이 반환되며 `-dev`는 붙지 않는다. commit/date는 unknown 상태다. Go module의 공개 release version은 Git tag가 최종 기준이다.

## 릴리스 정책

```powershell
git tag -a v0.1.0 -m "v0.1.0"
.\build.ps1 -Release
```

`-Release`는 다음을 강제한다.

- exact Git tag, `-Version`, `BUILD_VERSION` 중 하나
- 깨끗한 worktree (`-AllowDirty`를 명시하지 않은 경우)
- 취약점 우회 금지

CI에서 version을 주입하려면 다음처럼 실행할 수 있다.

```powershell
$env:BUILD_VERSION = '0.1.0'
.\build.ps1 -Release
```

## 취약점 검사

빌드에는 두 검사가 모두 포함된다.

```powershell
# 실제 source/test에서 도달 가능한 vulnerable symbol
go tool govulncheck -test ./...

# 표준 라이브러리와 전체 module graph의 broad inventory
go tool govulncheck -C flinksqlgateway -scan=module
```

기본 database는 Go 팀이 관리하는 `https://vuln.go.dev`다. 사내 mirror나 offline database는 다음처럼 지정한다.

```powershell
.\build.ps1 -VulnerabilityDatabase 'https://go-vuln-mirror.internal'
```

scan이 취약점 또는 도구/network 오류로 non-zero를 반환하면 build도 non-zero로 종료된다. 두 scan 보고서는 실패 시에도 `dist`에 남는다.

Go 1.26.4에서 다음 표준 라이브러리 취약점이 검출되어 프로젝트 toolchain을 Go 1.26.5로 올렸다.

| ID | 영향 | 수정 버전 |
| --- | --- | --- |
| `GO-2026-5856` | `crypto/tls`, symbol scan에서 실제 도달 | Go 1.26.5 |
| `GO-2026-4970` | `os`, module scan에서 검출 | Go 1.26.5 |

현재 `go.mod`와 빌드 스크립트의 기준은 Go 1.26.5다. 이전 1.26.4 toolchain으로는 정확한 버전 검사를 통과하지 않으며, 취약점 gate도 우회하지 않는다.

## 산출물

`dist/flink-sql-go-<version>-flink-<flink-version>*` 이름으로 생성된다.

| 파일 | 내용 |
| --- | --- |
| `*-source.zip` | source와 build-info |
| `*.build-info.json` | version, 지원 Flink version, commit, toolchain, security gate 결과 |
| `*.modules.txt` | 선택된 module version과 checksum |
| `*.govulncheck.txt` | symbol-level 결과 |
| `*.govulncheck-modules.txt` | module-level 결과 |
| `*.coverage.out` | Go coverage profile |
| `*.sha256` | 모든 주요 산출물의 SHA-256 |

조사 목적으로 산출물 생성을 끝까지 검증해야 할 때만 다음을 사용할 수 있다.

```powershell
.\build.ps1 -AllowVulnerabilities
```

이 옵션은 `-Release`와 함께 사용할 수 없으며 build-info에 보안 gate 실패가 기록된다.
