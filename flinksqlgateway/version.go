package flinksqlgateway

import (
	"strconv"
	"time"
)

const (
	// SourceVersion is the source release line and must match VERSION.
	SourceVersion = "0.1.1"
	// SupportedFlinkVersion is the tested Flink release and must match FLINK_VERSION.
	SupportedFlinkVersion = "1.20.4"
)

var (
	buildVersion      = SourceVersion
	buildFlinkVersion = SupportedFlinkVersion
	buildCommit       = "unknown"
	buildDate         = "unknown"
	buildDirty        = "true"
)

// BuildInfo describes the source and toolchain metadata injected by the build
// script. A normal library consumer receives the stable source version and
// the source tree's declared supported Flink version.
type BuildInfo struct {
	Version      string
	FlinkVersion string
	Commit       string
	Date         string
	Dirty        bool
}

// Version returns the semantic version injected by the build or SourceVersion.
func Version() string { return buildVersion }

// GetBuildInfo returns immutable build metadata. Date is RFC 3339 when it was
// injected by build.ps1.
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

// BuildTime parses the injected RFC 3339 build time. The boolean is false for
// ordinary source builds where no timestamp was injected.
func (info BuildInfo) BuildTime() (time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339, info.Date)
	return parsed, err == nil
}
