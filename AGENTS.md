# flink-sql-go 에이전트 개발 지침

## 적용 범위와 우선순위

- 이 파일은 저장소 전체에 적용되는 기본 지침이다.
- 시스템·개발자·사용자 지시가 이 파일보다 우선한다.
- 특정 디렉터리에 별도 `AGENTS.md`가 생기면 해당 디렉터리에서는 더 가까운 파일의 지침이 우선한다. 공통 규칙을 하위 파일에 복제하지 않는다.
- 작업을 시작할 때 `git status --short`와 관련 문서를 확인하고, 사용자가 만든 변경과 무관한 파일을 수정하거나 되돌리지 않는다.

## 프로젝트 기준

- Go module 경로는 `github.com/heartblast/flink-sql-go`이다.
- 기준 도구 체인은 `go.mod`에 선언된 Go 1.26.5이며 지원 대상은 `FLINK_VERSION`에 기록된 Apache Flink 1.20.4이다.
- SQL Gateway와 JobManager REST client는 표준 `net/http`를 사용한다. JDBC, JVM, GraalVM, CGO 및 `database/sql` 호환 계층을 추가하지 않는다.
- 공개 DTO와 전송 계층을 분리하고, 서버가 반환한 미지의 상태·필드와 원본 JSON의 의미를 가능한 한 보존한다.
- SQL Gateway의 Session, Operation과 Flink Job의 생명주기를 서로 독립적으로 취급한다. SQL 문자열을 정규식으로 분류해 실행 흐름을 결정하지 않는다.

## 구현 원칙

- 모든 네트워크 API는 `context.Context`를 받고 취소와 시간 제한을 전파한다.
- SQL 제출, 세션 생성 등 비멱등 요청은 자동 재시도하지 않는다. 실행 결과가 불명확하면 typed error로 호출자에게 알린다.
- polling과 결과 수집은 횟수·간격·행 수·응답 크기에 상한을 둔다. goroutine과 channel의 종료 경로를 명시하고 누수를 방지한다.
- 세션, operation, stream과 client의 `Close`는 가능한 경우 중복 호출에 안전하게 유지한다. 원래 오류를 cleanup 오류로 덮어쓰지 않는다.
- 주입받은 `http.Client` 또는 Transport의 소유권을 존중하며, 내부에서 소유한 자원만 종료한다.
- `nextResultUri`는 허용된 scheme과 origin을 검증한 뒤 사용한다. 인증 헤더, 전체 SQL, query string, 비밀번호 같은 민감정보를 오류·로그·관측 이벤트에 노출하지 않는다.
- 공개 API의 하위 호환성을 우선한다. 불가피한 변경은 관련 문서와 릴리스 노트에 명시한다.

## 한글 주석 규칙

- 운영 Go 소스의 package 주석과 공개 식별자(type, const, var, func, method)는 모두 한글로 설명한다. Go doc 규칙에 맞게 주석은 대상 식별자 이름으로 시작한다. 예: `// Client는 ...`.
- 비공개 함수·메서드는 생명주기, 동시성, 재시도, 보안 검증, 오류 변환처럼 동작이나 계약이 자명하지 않으면 한글 주석을 남긴다.
- package 수준 변수·상수와 struct field는 기본값, 단위, 소유권, nil 의미, 동시성 또는 상태 전이를 알아야 안전하게 사용할 수 있으면 한글 주석을 남긴다.
- 지역 변수는 역할이 이름과 코드만으로 분명하지 않을 때만 한글 주석을 남긴다. loop index나 단순 임시값마다 코드를 되풀이하는 주석을 붙이지 않는다.
- 주석은 “무엇을 실행하는지”만 반복하지 말고 제약, 이유, 부작용과 호출자가 지켜야 할 계약을 설명한다. 식별자, HTTP endpoint, Flink 상태값과 protocol 용어는 원문 표기를 유지할 수 있다.
- 코드를 수정할 때 같은 영역의 낡은 영문 주석이나 동작과 불일치한 주석도 함께 고친다. 테스트의 설명 주석도 새로 작성하거나 수정할 때는 한글을 사용한다.

## 변경 및 검증 절차

- 파일 수정에는 패치를 사용하고 Go 파일은 `gofmt`로 정렬한다.
- 의존성 변경 후 `go mod tidy -diff`와 `go mod verify`로 module 상태를 확인한다. 런타임 의존성 추가는 필요성과 보안 영향을 검토한다.
- 최소 검증은 `go test ./...`와 `go vet ./...`이다. 동시성 변경은 가능한 환경에서 `go test -race ./...`도 실행한다.
- 배포 가능한 전체 검증과 취약점 검사는 `./build.ps1`로 수행한다. 취약점 우회 옵션은 조사 목적의 비릴리스 빌드에서만 사용한다.
- 테스트는 정상 경로뿐 아니라 context 취소, 크기·행 수 제한, 비정상 JSON, 알 수 없는 enum, cleanup 실패와 외부 origin 차단을 포함한다.

## 버전과 Git

- 라이브러리 버전은 `VERSION`, 지원 Flink 버전은 `FLINK_VERSION`, 소스의 값은 `flinksqlgateway/version.go`에서 일관되게 관리한다.
- 릴리스 전에는 깨끗한 worktree와 정확한 `v*` tag에서 `./build.ps1 -Release`를 실행하고 산출물의 checksum과 build-info를 확인한다.
- 사용자가 명시적으로 요청하지 않으면 commit, tag, push, release 생성 또는 기존 이력 변경을 하지 않는다.
