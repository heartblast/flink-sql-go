package flinksqlgateway

import (
	"strconv"
	"time"
)

const (
	// SourceVersion은 소스 release 버전이며 루트 VERSION과 일치해야 한다.
	SourceVersion = "0.1.1"
	// SupportedFlinkVersion은 검증된 Flink release이며 루트 FLINK_VERSION과 일치해야 한다.
	SupportedFlinkVersion = "1.20.4"
)

var (
	// 다음 변수는 build.ps1의 ldflags로 덮어쓰며 일반 소스 빌드에서는 선언된 기본값을 사용한다.
	buildVersion      = SourceVersion
	buildFlinkVersion = SupportedFlinkVersion
	buildCommit       = "unknown"
	buildDate         = "unknown"
	buildDirty        = "true"
)

// BuildInfo는 build script가 주입한 소스와 도구 체인 metadata를 설명한다. 일반 library
// 사용자는 안정적인 소스 버전과 소스 트리에 선언된 지원 Flink 버전을 받는다.
type BuildInfo struct {
	Version      string
	FlinkVersion string
	Commit       string
	Date         string
	Dirty        bool
}

// Version은 build가 주입한 semantic version을 반환하며 주입 값이 없으면 SourceVersion이다.
func Version() string { return buildVersion }

// GetBuildInfo는 변경할 수 없는 build metadata 값을 반환한다. build.ps1가 주입한 Date는 RFC 3339 형식이다.
func GetBuildInfo() BuildInfo {
	dirty, _ := strconv.ParseBool(buildDirty)
	return BuildInfo{
		Version:      buildVersion,
		FlinkVersion: buildFlinkVersion,
		Commit:       buildCommit,
		Date:         buildDate,
		Dirty:        dirty,
	}
}

// BuildTime은 주입된 RFC 3339 build 시간을 해석한다. timestamp가 없는 일반 소스 빌드이면
// 두 번째 반환값은 false이다.
func (info BuildInfo) BuildTime() (time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339, info.Date)
	return parsed, err == nil
}
