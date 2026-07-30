# 릴리스 및 Flink 호환성 정책

## 독립적인 두 버전 축

`flink-sql-go`의 semantic version은 Go module 공개 API와 동작의 변화를 나타낸다. Apache Flink 버전은 서버 호환성 축이며 library tag에 포함하지 않는다.

- 올바른 library tag: `v0.2.0`
- 사용하지 않는 tag: `v0.2.0-flink-1.20`, `v1.20.4`, `v2.0.0`

`v2.0.0`은 Flink 2.0 지원이 아니라 Go module major version 2를 의미한다. 모든 release line은 단일 `main` 개발 흐름과 하나의 module에서 관리하며 Flink 버전별 장기 브랜치를 만들지 않는다.

## 호환성 단위

호환성은 다음 단위를 분리해 기록한다.

1. release line: `1.20.x`, `2.0.x`처럼 구현이 표현할 수 있는 계열
2. tested patch: 실제 Gateway에서 직접 검증한 `1.20.4` 같은 버전
3. SQL Gateway REST API: `v1`부터 `v4`까지의 wire protocol

release line profile이 존재한다는 사실만으로 모든 patch가 검증됐다고 주장하지 않는다. 직접 검증하지 않은 버전은 `testedVersions`에 추가하지 않는다. canonical 정보는 `compatibility.yaml`에서 관리하고 runtime registry, 문서와 배포 JSON의 일치는 contract test와 build가 검증한다.

## 지원 상태

| 상태 | 의미 |
| --- | --- |
| `planned` | 설계 대상이지만 선택 가능한 구현이나 호환성 보장은 없음 |
| `experimental` | profile과 protocol 경계는 구현됐지만 실제 환경 검증 또는 안정성 근거가 부족함 |
| `supported` | fixture와 실제 Gateway matrix를 통과하고 일반 사용을 지원함 |
| `maintenance` | 기존 호환성, 보안 및 중요 결함 수정을 유지하되 신규 기능 범위는 제한함 |
| `unsupported` | 신규 수정과 호환성 보장을 제공하지 않음 |

experimental release를 supported로 올리려면 최소 한 patch를 `testedVersions`에 기록하고, 해당 버전의 실제 Gateway contract workflow가 성공해야 한다. CI endpoint가 없거나 integration test가 skip된 결과는 검증 성공으로 계산하지 않는다.

## REST API 선택 정책

- Stable: release profile의 검증된 기본 API를 선택한다. 현재 기본값은 `v3`이다.
- Highest: server와 profile의 교집합 중 가장 높은 API를 선택한다.
- Explicit: 호출자가 지정한 API만 사용하고 지원되지 않으면 fallback 없이 typed error를 반환한다.

Flink 제품 release와 REST API 버전은 독립적이다. 새 release line을 추가할 때 기존 client를 복제하지 않고 실제 wire 차이와 capability만 profile 또는 protocol 경계에 추가한다.

## 지원 추가와 종료

새 Flink release line은 다음 순서로 추가한다.

1. `compatibility.yaml`에 보수적인 상태와 빈 `testedVersions`로 등록
2. runtime registry와 manifest contract test 갱신
3. version별 response fixture 및 unit contract test 추가
4. 실제 Gateway endpoint에서 integration test 수행
5. 성공한 patch만 `testedVersions`에 추가하고 문서 상태 검토

지원 종료 시 먼저 release note와 compatibility 문서에 일정을 알리고 `maintenance`를 거쳐 `unsupported`로 전환한다. 보안 문제나 upstream 지원 종료처럼 예외적인 경우에는 즉시 unsupported로 전환할 수 있으며 이유를 릴리스 노트에 기록한다.

## 릴리스 gate

릴리스 후보는 다음 조건을 모두 만족해야 한다.

- `VERSION`, `SourceVersion`과 tag의 일치
- 깨끗한 worktree와 정확히 하나의 `v<VERSION>` tag
- `go mod verify`와 `go mod tidy -diff`
- `gofmt`, `go vet ./...`, `go test ./...`
- integration-tagged test compile
- reachable 및 module graph 취약점 gate
- compatibility/build-info JSON과 checksum 생성
- 실제 수행한 검증만 기록한 release note

`-AllowDirty`와 `-AllowVulnerabilities`은 릴리스 build에서 사용할 수 없다. tag, commit, push와 GitHub Release 생성은 별도의 명시적 릴리스 작업에서 수행한다.
