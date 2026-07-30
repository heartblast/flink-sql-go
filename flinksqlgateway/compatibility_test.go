package flinksqlgateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCompatibilityConfigDefaultsAndLegacyExplicit(t *testing.T) {
	automatic := newTestClient(t, Config{BaseURL: "https://flink.example"})
	if automatic.cfg.CompatibilityMode != CompatibilityFlink120 {
		t.Fatalf("test helper mode = %q", automatic.cfg.CompatibilityMode)
	}
	direct, err := NewClient(Config{BaseURL: "https://flink.example"})
	if err != nil {
		t.Fatal(err)
	}
	if direct.cfg.CompatibilityMode != CompatibilityAuto || direct.cfg.APIVersionPolicy != APIVersionStable || direct.cfg.APIVersion != "v3" {
		t.Fatalf("default compatibility config = %+v", direct.cfg)
	}
	if direct.cfg.UserAgent != "flink-sql-go/"+SourceVersion {
		t.Fatalf("default User-Agent = %q", direct.cfg.UserAgent)
	}

	legacy, err := NewClient(Config{BaseURL: "https://flink.example", APIVersion: "V4"})
	if err != nil {
		t.Fatal(err)
	}
	if legacy.cfg.APIVersionPolicy != APIVersionExplicit || legacy.cfg.APIVersion != "v4" {
		t.Fatalf("legacy APIVersion config = %+v", legacy.cfg)
	}

	invalid := []Config{
		{BaseURL: "https://flink.example", APIVersionPolicy: APIVersionExplicit},
		{BaseURL: "https://flink.example", APIVersionPolicy: APIVersionHighest, APIVersion: "v3"},
		{BaseURL: "https://flink.example", CompatibilityMode: "flink-9.9"},
		{BaseURL: "https://flink.example", APIVersionPolicy: "newest"},
	}
	for _, cfg := range invalid {
		if _, err := NewClient(cfg); err == nil {
			t.Fatalf("NewClient(%+v) succeeded", cfg)
		}
	}
}

func TestParseFlinkReleaseLineAndTypedErrors(t *testing.T) {
	tests := []struct {
		version string
		want    ReleaseLine
	}{
		{"1.20.4", Flink120},
		{"v2.0.0", Flink20},
		{"2.1.0-preview1", Flink21},
		{"2.1.0-rc.1", Flink21},
		{"2.2.3+vendor", Flink22},
		{"2.3.0+vendor.2", Flink23},
		{"2.3", Flink23},
	}
	for _, test := range tests {
		got, err := parseFlinkReleaseLine(test.version)
		if err != nil || got != test.want {
			t.Errorf("parseFlinkReleaseLine(%q) = %q, %v", test.version, got, err)
		}
	}

	for _, version := range []string{"", "2", "release-2.1.0", "2.x.0", "2.1.x", "2.1.0.extra", "2-beta.1.0", "2.1-beta.0", "2.1.0-rc..1"} {
		_, err := parseFlinkReleaseLine(version)
		if !errors.Is(err, ErrInvalidFlinkVersion) {
			t.Errorf("parseFlinkReleaseLine(%q) error = %v", version, err)
		}
		var typed *CompatibilityError
		if !errors.As(err, &typed) {
			t.Errorf("parseFlinkReleaseLine(%q) did not return CompatibilityError", version)
		}
	}

	injected := (&CompatibilityError{Kind: ErrInvalidFlinkVersion, FlinkVersion: "2.1.0\nforged-log-line"}).Error()
	if strings.Contains(injected, "forged-log-line") {
		t.Fatalf("CompatibilityError exposed a control-character suffix: %q", injected)
	}

	line, err := parseFlinkReleaseLine("1.19.2")
	if err != nil {
		t.Fatal(err)
	}
	_, err = profileForReleaseLine(line)
	if !errors.Is(err, ErrUnsupportedFlinkVersion) {
		t.Fatalf("profileForReleaseLine(%q) error = %v", line, err)
	}
}

func TestReleaseStatusSelectionBoundary(t *testing.T) {
	tests := []struct {
		status ReleaseStatus
		want   bool
	}{
		{ReleasePlanned, false},
		{ReleaseExperimental, true},
		{ReleaseSupported, true},
		{ReleaseMaintenance, true},
		{ReleaseUnsupported, false},
	}
	for _, test := range tests {
		if got := releaseStatusSelectable(test.status); got != test.want {
			t.Errorf("releaseStatusSelectable(%q) = %t, want %t", test.status, got, test.want)
		}
	}
}

func TestCompatibilityRegistryReturnsDeepCopies(t *testing.T) {
	releases := SupportedFlinkVersions()
	if len(releases) != 5 {
		t.Fatalf("supported releases = %d", len(releases))
	}
	if releases[0].ReleaseLine != Flink120 || releases[0].Status != ReleaseMaintenance || !reflect.DeepEqual(releases[0].TestedVersions, []string{"1.20.4"}) {
		t.Fatalf("Flink 1.20 release = %+v", releases[0])
	}
	for _, release := range releases[1:] {
		if release.Status != ReleaseExperimental || len(release.TestedVersions) != 0 {
			t.Errorf("unverified release = %+v", release)
		}
	}

	releases[0].TestedVersions[0] = "changed"
	releases[0].RESTAPIVersions[0] = "v99"
	releases[0].Capabilities.SupportedAPIVersions[0] = "v99"
	matrix := CompatibilityMatrix()
	matrix.SupportedReleases[0].TestedVersions[0] = "also-changed"

	fresh := SupportedFlinkVersions()[0]
	if fresh.TestedVersions[0] != "1.20.4" || fresh.RESTAPIVersions[0] != "v1" || fresh.Capabilities.SupportedAPIVersions[0] != "v1" {
		t.Fatalf("registry was mutated through public snapshot: %+v", fresh)
	}
}

func TestAPIVersionSelectionPoliciesAndErrors(t *testing.T) {
	profile, err := profileForReleaseLine(Flink20)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name       string
		advertised []string
		policy     APIVersionPolicy
		explicit   string
		want       string
	}{
		{"stable-default", []string{"V4", "V3", "V1"}, APIVersionStable, "", "v3"},
		{"stable-fallback", []string{"v4", "v2", "v1"}, APIVersionStable, "", "v2"},
		{"highest", []string{"v2", "V4", "v3"}, APIVersionHighest, "", "v4"},
		{"explicit", []string{"v4", "v3"}, APIVersionExplicit, "v4", "v4"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := selectAPIVersion(profile, test.advertised, test.policy, test.explicit)
			if err != nil || got != test.want {
				t.Fatalf("selectAPIVersion() = %q, %v", got, err)
			}
		})
	}

	_, err = selectAPIVersion(profile, []string{"v1", "v2"}, APIVersionExplicit, "v4")
	if !errors.Is(err, ErrExplicitAPIVersionUnsupportedByServer) || !errors.Is(err, ErrUnsupportedAPI) {
		t.Fatalf("server explicit error = %v", err)
	}
	profile120, _ := profileForReleaseLine(Flink120)
	_, err = selectAPIVersion(profile120, []string{"v4"}, APIVersionExplicit, "v4")
	if !errors.Is(err, ErrExplicitAPIVersionUnsupportedByProfile) || !errors.Is(err, ErrUnsupportedAPI) {
		t.Fatalf("profile explicit error = %v", err)
	}
	_, err = selectAPIVersion(profile, []string{"future", "v9"}, APIVersionHighest, "")
	if !errors.Is(err, ErrNoCompatibleAPIVersion) || !errors.Is(err, ErrUnsupportedAPI) {
		t.Fatalf("no intersection error = %v", err)
	}
}

func TestExplicitProfileErrorDoesNotUseNetwork(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()
	client, err := NewClient(Config{
		BaseURL:           server.URL,
		CompatibilityMode: CompatibilityFlink120,
		APIVersionPolicy:  APIVersionExplicit,
		APIVersion:        "v4",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = client.CheckCompatibility(context.Background())
	if !errors.Is(err, ErrExplicitAPIVersionUnsupportedByProfile) {
		t.Fatalf("profile error = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("local profile rejection made %d network calls", calls.Load())
	}
}

func TestCompatibilityAutoDetectionIsLazyAndCached(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		switch r.URL.Path {
		case "/info":
			writeTestJSON(t, w, map[string]any{"productName": "Apache Flink", "version": "2.1.3"})
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V1", "V2", "V3", "V4"}})
		case "/v3/sessions":
			writeTestJSON(t, w, map[string]string{"sessionHandle": "s-auto"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewClient(Config{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if len(paths) != 0 {
		t.Fatalf("NewClient network paths = %v", paths)
	}
	mu.Unlock()
	if _, err := client.OpenSession(context.Background(), OpenSessionRequest{}); err != nil {
		t.Fatal(err)
	}
	info, err := client.GetCompatibilityInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.FlinkVersion != "2.1.3" || info.ReleaseLine != Flink21 || info.APIVersion != "v3" || info.DetectionSource != DetectionSourceAuto {
		t.Fatalf("compatibility info = %+v", info)
	}
	info.Capabilities.SupportedAPIVersions[0] = "changed"
	fresh, _ := client.GetCompatibilityInfo(context.Background())
	if fresh.Capabilities.SupportedAPIVersions[0] != "v1" {
		t.Fatal("client compatibility slice was mutated")
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(paths, []string{"/info", "/api_versions", "/v3/sessions"}) {
		t.Fatalf("request order = %v", paths)
	}
}

func TestCompatibilityHighestAndManualEndpointSelection(t *testing.T) {
	tests := []struct {
		name       string
		mode       CompatibilityMode
		policy     APIVersionPolicy
		apiVersion string
		flink      string
		advertised []string
		wantPath   string
		wantInfo   bool
	}{
		{"auto-highest", CompatibilityAuto, APIVersionHighest, "", "2.0.1", []string{"v1", "v3", "v4"}, "/v4/sessions", true},
		{"manual-v1", CompatibilityFlink120, APIVersionExplicit, "v1", "", []string{"v1", "v3"}, "/v1/sessions", false},
		{"manual-v2", CompatibilityFlink120, APIVersionExplicit, "v2", "", []string{"v2", "v3"}, "/v2/sessions", false},
		{"manual-v3", CompatibilityFlink120, APIVersionExplicit, "v3", "", []string{"v3"}, "/v3/sessions", false},
		{"manual-v4", CompatibilityFlink20, APIVersionExplicit, "v4", "", []string{"v3", "v4"}, "/v4/sessions", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var infoCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/info":
					infoCalls.Add(1)
					writeTestJSON(t, w, map[string]string{"productName": "Apache Flink", "version": test.flink})
				case "/api_versions":
					writeTestJSON(t, w, map[string]any{"versions": test.advertised})
				case test.wantPath:
					writeTestJSON(t, w, map[string]string{"sessionHandle": "s"})
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client, err := NewClient(Config{
				BaseURL:           server.URL,
				CompatibilityMode: test.mode,
				APIVersionPolicy:  test.policy,
				APIVersion:        test.apiVersion,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.OpenSession(context.Background(), OpenSessionRequest{}); err != nil {
				t.Fatal(err)
			}
			if got := infoCalls.Load(); (got != 0) != test.wantInfo {
				t.Fatalf("/info calls = %d", got)
			}
		})
	}
}

func TestCompatibilityDetectionFailureRetriesAndUnwraps(t *testing.T) {
	var infoCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/info":
			if infoCalls.Add(1) <= 2 {
				http.Error(w, "temporary", http.StatusServiceUnavailable)
				return
			}
			writeTestJSON(t, w, map[string]string{"productName": "Apache Flink", "version": "1.20.4"})
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"v3"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(Config{BaseURL: server.URL, PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	err = client.CheckCompatibility(context.Background())
	if !errors.Is(err, ErrCompatibilityDetection) {
		t.Fatalf("first detection error = %v", err)
	}
	if !strings.Contains(err.Error(), "status=503") {
		t.Fatalf("detection error has no actionable HTTP cause: %v", err)
	}
	var typed *CompatibilityError
	var apiErr *APIError
	if !errors.As(err, &typed) || !errors.As(err, &apiErr) {
		t.Fatalf("detection error chain = %v", err)
	}
	if err := client.CheckCompatibility(context.Background()); err != nil {
		t.Fatalf("retry detection error = %v", err)
	}
	if infoCalls.Load() != 3 {
		t.Fatalf("/info calls = %d", infoCalls.Load())
	}
}

func TestConcurrentCompatibilityDetectionRunsOnce(t *testing.T) {
	var infoCalls atomic.Int32
	var versionCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/info":
			infoCalls.Add(1)
			writeTestJSON(t, w, map[string]string{"productName": "Apache Flink", "version": "1.20.4"})
		case "/api_versions":
			versionCalls.Add(1)
			writeTestJSON(t, w, map[string]any{"versions": []string{"v3"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(Config{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- client.CheckCompatibility(context.Background())
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if infoCalls.Load() != 1 || versionCalls.Load() != 1 {
		t.Fatalf("detection calls = info:%d versions:%d", infoCalls.Load(), versionCalls.Load())
	}
}

func TestConcurrentTransientDetectionFailureIsShared(t *testing.T) {
	var infoCalls atomic.Int32
	firstRequest := make(chan struct{})
	releaseFirst := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/info" {
			http.NotFound(w, r)
			return
		}
		if infoCalls.Add(1) == 1 {
			close(firstRequest)
			<-releaseFirst
		}
		writeTestJSONStatus(t, w, http.StatusServiceUnavailable, map[string]string{"message": "temporary"})
	}))
	defer server.Close()
	client, err := NewClient(Config{BaseURL: server.URL, PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	const callers = 8
	start := make(chan struct{})
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(callers)
	done.Add(callers)
	errs := make(chan error, callers)
	for range callers {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			errs <- client.CheckCompatibility(context.Background())
		}()
	}
	ready.Wait()
	close(start)
	<-firstRequest
	time.Sleep(20 * time.Millisecond)
	close(releaseFirst)
	done.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, ErrCompatibilityDetection) {
			t.Fatalf("shared detection error = %v", err)
		}
	}
	if infoCalls.Load() != 2 {
		t.Fatalf("one shared detection should use two safe attempts, calls = %d", infoCalls.Load())
	}
	if err := client.CheckCompatibility(context.Background()); !errors.Is(err, ErrCompatibilityDetection) {
		t.Fatalf("later retry error = %v", err)
	}
	if infoCalls.Load() != 4 {
		t.Fatalf("later caller did not start exactly one new detection, calls = %d", infoCalls.Load())
	}
}

func TestDetectionLeaderCancellationDoesNotFailLiveWaiter(t *testing.T) {
	var infoCalls atomic.Int32
	firstRequest := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/info":
			if infoCalls.Add(1) == 1 {
				close(firstRequest)
				<-r.Context().Done()
				return
			}
			writeTestJSON(t, w, map[string]string{"productName": "Apache Flink", "version": "1.20.4"})
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"v3"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(Config{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}

	leaderCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	leader := make(chan error, 1)
	go func() { leader <- client.CheckCompatibility(leaderCtx) }()
	<-firstRequest
	waiter := make(chan error, 1)
	go func() { waiter <- client.CheckCompatibility(context.Background()) }()
	if err := <-leader; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("leader error = %v", err)
	}
	if err := <-waiter; err != nil {
		t.Fatalf("live waiter error = %v", err)
	}
	if infoCalls.Load() != 2 {
		t.Fatalf("/info calls = %d", infoCalls.Load())
	}
}

func TestCompatibilityDetectionWaiterHonorsContext(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/info":
			select {
			case <-started:
			default:
				close(started)
			}
			<-release
			writeTestJSON(t, w, map[string]string{"productName": "Apache Flink", "version": "1.20.4"})
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"v3"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(Config{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { first <- client.CheckCompatibility(context.Background()) }()
	<-started
	waiterCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := client.CheckCompatibility(waiterCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting detection error = %v", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}

func TestWireExecutionTimeoutDependsOnReleaseProfile(t *testing.T) {
	var statementTimeout any
	var configureTimeout any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"v3"}})
		case "/v3/sessions/s/statements":
			var body map[string]any
			decodeTestJSON(t, r, &body)
			statementTimeout = body["executionTimeout"]
			writeTestJSON(t, w, map[string]string{"operationHandle": "op"})
		case "/v3/sessions/s/configure-session":
			var body map[string]any
			decodeTestJSON(t, r, &body)
			configureTimeout = body["executionTimeout"]
			writeTestJSON(t, w, map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL, CompatibilityMode: CompatibilityFlink20, APIVersion: "v3"})
	if _, err := client.ExecuteStatement(context.Background(), "s", ExecuteStatementRequest{Statement: "SELECT 1", ExecutionTimeout: 1500 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err := client.ConfigureSession(context.Background(), "s", "USE CATALOG cat", 2250*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if statementTimeout != float64(1500) || configureTimeout != float64(2250) {
		t.Fatalf("wire timeouts = statement:%v configure:%v", statementTimeout, configureTimeout)
	}
}

func TestDetectionRedactsReflectedHeadersFromErrorsAndObserver(t *testing.T) {
	const secret = "Bearer detection-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/info" {
			http.NotFound(w, r)
			return
		}
		writeTestJSONStatus(t, w, http.StatusServiceUnavailable, map[string]string{"message": "proxy echoed " + secret})
	}))
	defer server.Close()
	observer := &compatibilityObservationRecorder{}
	client, err := NewClient(Config{
		BaseURL:      server.URL,
		Headers:      map[string]string{"Authorization": secret},
		Observer:     observer,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = client.CheckCompatibility(context.Background())
	if !errors.Is(err, ErrCompatibilityDetection) {
		t.Fatalf("detection error = %v", err)
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("detection cause = %v", err)
	}
	for label, value := range map[string]string{
		"error":    err.Error(),
		"APIError": apiErr.Message,
		"observer": observer.String(),
	} {
		if strings.Contains(value, secret) {
			t.Fatalf("%s exposed an authorization header: %q", label, value)
		}
	}
}

func TestDetectionRedirectErrorRedactsExternalQuery(t *testing.T) {
	const redirectSecret = "redirect-query-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://outside.example/collect?token="+redirectSecret, http.StatusFound)
	}))
	defer server.Close()
	observer := &compatibilityObservationRecorder{}
	client, err := NewClient(Config{BaseURL: server.URL, Observer: observer, PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	err = client.CheckCompatibility(context.Background())
	if !errors.Is(err, ErrCompatibilityDetection) || !errors.Is(err, ErrUnsafeNextResultURI) {
		t.Fatalf("redirect error = %v", err)
	}
	if strings.Contains(err.Error(), redirectSecret) || strings.Contains(observer.String(), redirectSecret) {
		t.Fatalf("redirect query was exposed: error=%q observer=%q", err, observer.String())
	}
}

func TestStatementAndCompletionErrorsRedactSQLAndQuery(t *testing.T) {
	const statement = "SELECT 'statement-secret'"
	observer := &compatibilityObservationRecorder{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"v3"}})
		case "/v3/sessions/s/statements":
			writeTestJSONStatus(t, w, http.StatusBadRequest, map[string]string{"message": "rejected " + statement})
		default:
			http.NotFound(w, r)
		}
	}))
	client, err := NewClient(Config{
		BaseURL:           server.URL,
		CompatibilityMode: CompatibilityFlink120,
		APIVersionPolicy:  APIVersionExplicit,
		APIVersion:        "v3",
		Observer:          observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ExecuteStatement(context.Background(), "s", ExecuteStatementRequest{Statement: statement})
	server.Close()
	if err == nil {
		t.Fatal("ExecuteStatement succeeded")
	}
	if strings.Contains(err.Error(), statement) || strings.Contains(observer.String(), statement) {
		t.Fatalf("statement was exposed: error=%q observer=%q", err, observer.String())
	}

	const headerSecret = "completion-header-secret"
	completionObserver := &compatibilityObservationRecorder{}
	httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/api_versions" {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"versions":["v3"]}`)),
				Request:    req,
			}, nil
		}
		return nil, fmt.Errorf("transport echoed %s and %s", req.URL.String(), req.Header.Get("Authorization"))
	})}
	completionClient, err := NewClient(Config{
		BaseURL:           "https://flink.example",
		CompatibilityMode: CompatibilityFlink120,
		APIVersionPolicy:  APIVersionExplicit,
		APIVersion:        "v3",
		HTTPClient:        httpClient,
		Headers:           map[string]string{"Authorization": headerSecret},
		Observer:          completionObserver,
		PollInterval:      time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = completionClient.CompleteStatement(context.Background(), "s", statement, len(statement))
	if err == nil {
		t.Fatal("CompleteStatement succeeded")
	}
	for label, value := range map[string]string{
		"error":    err.Error(),
		"observer": completionObserver.String(),
	} {
		if strings.Contains(value, statement) || strings.Contains(value, headerSecret) || strings.Contains(value, "statement=") {
			t.Fatalf("%s exposed completion request data: %q", label, value)
		}
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		t.Fatalf("sanitized error chain exposed url.Error: %v", urlErr)
	}
}

func TestCompatibilityManifestMatchesRuntimeRegistry(t *testing.T) {
	type manifestCapabilities struct {
		ConfigureSession     bool `json:"configureSession"`
		CompleteStatement    bool `json:"completeStatement"`
		RowFormat            bool `json:"rowFormat"`
		MaterializedTable    bool `json:"materializedTable"`
		DeployScript         bool `json:"deployScript"`
		WireExecutionTimeout bool `json:"wireExecutionTimeout"`
	}
	type manifestRelease struct {
		ReleaseLine      ReleaseLine          `json:"releaseLine"`
		Status           ReleaseStatus        `json:"status"`
		TestedVersions   []string             `json:"testedVersions"`
		RESTAPIVersions  []string             `json:"restApiVersions"`
		StableAPIVersion string               `json:"stableApiVersion"`
		Capabilities     manifestCapabilities `json:"capabilities"`
	}
	var manifest struct {
		SchemaVersion      int               `json:"schemaVersion"`
		DefaultReleaseLine ReleaseLine       `json:"defaultReleaseLine"`
		DefaultAPIVersion  string            `json:"defaultApiVersion"`
		SupportedReleases  []manifestRelease `json:"supportedReleases"`
	}
	data, err := os.ReadFile(filepath.Join("..", "compatibility.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("compatibility.yaml must use dependency-free JSON-compatible YAML: %v", err)
	}
	matrix := CompatibilityMatrix()
	if manifest.SchemaVersion != matrix.SchemaVersion || manifest.DefaultReleaseLine != matrix.DefaultReleaseLine || manifest.DefaultAPIVersion != matrix.DefaultAPIVersion {
		t.Fatalf("manifest defaults = %+v, runtime = %+v", manifest, matrix)
	}
	if len(manifest.SupportedReleases) != len(matrix.SupportedReleases) {
		t.Fatalf("manifest releases = %d, runtime = %d", len(manifest.SupportedReleases), len(matrix.SupportedReleases))
	}
	for index, expected := range manifest.SupportedReleases {
		actual := matrix.SupportedReleases[index]
		if expected.ReleaseLine != actual.ReleaseLine || expected.Status != actual.Status || expected.StableAPIVersion != actual.StableAPIVersion ||
			!reflect.DeepEqual(expected.TestedVersions, actual.TestedVersions) || !reflect.DeepEqual(expected.RESTAPIVersions, actual.RESTAPIVersions) {
			t.Errorf("manifest release[%d] = %+v, runtime = %+v", index, expected, actual)
		}
		wantCapabilities := expected.Capabilities
		gotCapabilities := actual.Capabilities
		if wantCapabilities.ConfigureSession != gotCapabilities.ConfigureSession ||
			wantCapabilities.CompleteStatement != gotCapabilities.CompleteStatement ||
			wantCapabilities.RowFormat != gotCapabilities.RowFormat ||
			wantCapabilities.MaterializedTable != gotCapabilities.MaterializedTable ||
			wantCapabilities.DeployScript != gotCapabilities.DeployScript ||
			wantCapabilities.WireExecutionTimeout != gotCapabilities.WireExecutionTimeout {
			t.Errorf("manifest capabilities[%d] = %+v, runtime = %+v", index, wantCapabilities, gotCapabilities)
		}
	}
}

type compatibilityObservationRecorder struct {
	mu       sync.Mutex
	requests []RequestObservation
}

func (r *compatibilityObservationRecorder) ObserveRequest(_ context.Context, observation RequestObservation) {
	r.mu.Lock()
	r.requests = append(r.requests, observation)
	r.mu.Unlock()
}

func (r *compatibilityObservationRecorder) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fmt.Sprintf("%+v", r.requests)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func ExampleGatewayClient_GetCompatibilityInfo() {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api_versions":
			_ = json.NewEncoder(w).Encode(map[string]any{"versions": []string{"v1", "v2", "v3"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, _ := NewClient(Config{BaseURL: server.URL, CompatibilityMode: CompatibilityFlink120})
	info, _ := client.GetCompatibilityInfo(context.Background())
	fmt.Println(info.ReleaseLine, info.APIVersion, info.DetectionSource)
	// Output: 1.20 v3 configured
}
