package flinksqlgateway

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestSourceVersionMatchesVersionFile(t *testing.T) {
	data, err := os.ReadFile("../VERSION")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(data)); got != SourceVersion {
		t.Fatalf("VERSION = %q, SourceVersion = %q", got, SourceVersion)
	}
}

func TestSupportedFlinkVersionRemainsLegacyTestedVersion(t *testing.T) {
	if SupportedFlinkVersion != "1.20.4" {
		t.Fatalf("SupportedFlinkVersion = %q, want legacy value 1.20.4", SupportedFlinkVersion)
	}

	data, err := os.ReadFile("../compatibility.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		SupportedReleases []struct {
			ReleaseLine    string
			TestedVersions []string
		}
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("compatibility.yaml: %v", err)
	}
	for _, release := range manifest.SupportedReleases {
		for _, testedVersion := range release.TestedVersions {
			if testedVersion == SupportedFlinkVersion {
				return
			}
		}
	}
	t.Fatalf("legacy SupportedFlinkVersion %q is not a tested version in compatibility.yaml", SupportedFlinkVersion)
}

func TestBuildInfo(t *testing.T) {
	info := GetBuildInfo()
	if info.Version == "" || info.FlinkVersion == "" || len(info.SupportedFlinkReleaseLines) == 0 || info.Commit == "" || info.Date == "" {
		t.Fatalf("incomplete build info: %+v", info)
	}
	if info.Commit == "unknown" && info.Version != SourceVersion {
		t.Fatalf("source build version = %q, want stable %q", info.Version, SourceVersion)
	}
	if info.Date != "unknown" {
		if _, ok := info.BuildTime(); !ok {
			t.Fatalf("invalid injected build date: %q", info.Date)
		}
	}

	originalReleaseLine := info.SupportedFlinkReleaseLines[0]
	info.SupportedFlinkReleaseLines[0] = "modified"
	if got := GetBuildInfo().SupportedFlinkReleaseLines[0]; got != originalReleaseLine {
		t.Fatalf("GetBuildInfo release lines share mutable storage: got %q, want %q", got, originalReleaseLine)
	}
}
