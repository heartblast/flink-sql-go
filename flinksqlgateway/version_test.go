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

func TestBuildInfo(t *testing.T) {
	info := GetBuildInfo()
	if info.Version == "" || info.Commit == "" || info.Date == "" {
		t.Fatalf("incomplete build info: %+v", info)
	}
	if info.Date != "unknown" {
		if _, ok := info.BuildTime(); !ok {
			t.Fatalf("invalid injected build date: %q", info.Date)
		}
	}
}
