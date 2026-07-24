package flinksqlgateway

import (
	"strconv"
	"time"
)

// SourceVersion is the source release line. It must match the repository's
// VERSION file and is overridden with full Git-derived metadata by build.ps1.
const SourceVersion = "0.1.0"

var (
	buildVersion = SourceVersion + "-dev"
	buildCommit  = "unknown"
	buildDate    = "unknown"
	buildDirty   = "true"
)

// BuildInfo describes the source and toolchain metadata injected by the build
// script. A normal library consumer that does not pass linker flags receives
// the SourceVersion development defaults.
type BuildInfo struct {
	Version string
	Commit  string
	Date    string
	Dirty   bool
}

// Version returns the semantic version injected by the build or the source
// development version when no build metadata was injected.
func Version() string { return buildVersion }

// GetBuildInfo returns immutable build metadata. Date is RFC 3339 when it was
// injected by build.ps1.
func GetBuildInfo() BuildInfo {
	dirty, _ := strconv.ParseBool(buildDirty)
	return BuildInfo{
		Version: buildVersion,
		Commit:  buildCommit,
		Date:    buildDate,
		Dirty:   dirty,
	}
}

// BuildTime parses the injected RFC 3339 build time. The boolean is false for
// ordinary source builds where no timestamp was injected.
func (info BuildInfo) BuildTime() (time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339, info.Date)
	return parsed, err == nil
}
