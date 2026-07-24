package flinksqlgateway

import (
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

func TestSupportedFlinkVersionMatchesVersionFile(t *testing.T) {
	data, err := os.ReadFile("../FLINK_VERSION")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(data)); got != SupportedFlinkVersion {
		t.Fatalf("FLINK_VERSION = %q, SupportedFlinkVersion = %q", got, SupportedFlinkVersion)
	}
}

func TestBuildInfo(t *testing.T) {
	info := GetBuildInfo()
	if info.Version == "" || info.FlinkVersion == "" || info.Commit == "" || info.Date == "" {
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
}
