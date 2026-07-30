package flinksqlgateway

import (
	"strconv"
	"time"
)

const (
	// SourceVersion은 소스 release 버전이며 루트 VERSION과 일치해야 한다.
	SourceVersion = "0.2.0"
	// SupportedFlinkVersion은 v0.2.0 이전 단일 버전 API와의 호환성을 위한 Flink release이다.
	//
	// Deprecated: SupportedFlinkVersions 또는 CompatibilityMatrix를 사용한다.
	SupportedFlinkVersion = "1.20.4"
)

var (
	// 다음 변수는 build.ps1의 ldflags로 덮어쓰며 일반 소스 빌드에서는 선언된 기본값을 사용한다.
	buildVersion = SourceVersion
	buildCommit  = "unknown"
	buildDate    = "unknown"
	buildDirty   = "true"
)

// BuildInfo는 build script가 주입한 소스 metadata와 지원 Flink release line을 설명한다.
type BuildInfo struct {
	// Version은 library semantic version이다.
	Version string
	// FlinkVersion은 v0.2.0 이전 단일 버전 API와의 호환성을 위해 1.20.4를 반환한다.
	//
	// Deprecated: SupportedFlinkReleaseLines 또는 CompatibilityMatrix를 사용한다.
	FlinkVersion string
	// SupportedFlinkReleaseLines는 compatibility registry에 등록된 release line의 복사본이다.
	SupportedFlinkReleaseLines []string
	// Commit은 build 대상 Git commit이다.
	Commit string
	// Date는 build script가 주입한 RFC 3339 UTC 시각이다.
	Date string
	// Dirty는 build 당시 worktree에 추적 또는 미추적 변경이 있었는지 나타낸다.
	Dirty bool
}

// Version은 build가 주입한 semantic version을 반환하며 주입 값이 없으면 SourceVersion이다.
func Version() string { return buildVersion }

// GetBuildInfo는 변경할 수 없는 build metadata 값을 반환한다. build.ps1가 주입한 Date는 RFC 3339 형식이다.
func GetBuildInfo() BuildInfo {
	dirty, _ := strconv.ParseBool(buildDirty)
	releases := SupportedFlinkVersions()
	releaseLines := make([]string, len(releases))
	for index, release := range releases {
		releaseLines[index] = string(release.ReleaseLine)
	}
	return BuildInfo{
		Version:                    buildVersion,
		FlinkVersion:               SupportedFlinkVersion,
		SupportedFlinkReleaseLines: releaseLines,
		Commit:                     buildCommit,
		Date:                       buildDate,
		Dirty:                      dirty,
	}
}

// BuildTime은 주입된 RFC 3339 build 시간을 해석한다. timestamp가 없는 일반 소스 빌드이면
// 두 번째 반환값은 false이다.
func (info BuildInfo) BuildTime() (time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339, info.Date)
	return parsed, err == nil
}
