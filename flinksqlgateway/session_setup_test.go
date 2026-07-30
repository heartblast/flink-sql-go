package flinksqlgateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCompileSessionSetupBuildsDeterministicQualifiedSteps(t *testing.T) {
	plan := SessionSetupPlan{
		Catalogs: []CatalogSetup{{
			Name:        "cat`alog",
			IfNotExists: true,
			Options: map[string]string{
				"z-option": "last",
				"password": "pa'ss",
				"a-option": "first'value",
			},
		}},
		Databases: []DatabaseSetup{{
			Catalog:     "cat`alog",
			Name:        "db name",
			IfNotExists: true,
			Options:     map[string]string{"owner": "team's"},
		}},
		Tables: []TableSetup{{
			Target:      Identifier{Catalog: "cat`alog", Database: "db name", Object: "orders`raw"},
			Statement:   "(id BIGINT) WITH ('connector' = 'datagen')",
			IfNotExists: true,
			Verify:      true,
		}},
		CurrentCatalog:  "cat`alog",
		CurrentDatabase: "db name",
	}

	publicSteps, err := CompileSessionSetup(plan)
	if err != nil {
		t.Fatalf("CompileSessionSetup() error = %v", err)
	}
	compiled, err := compileSessionSetupPlan(plan)
	if err != nil {
		t.Fatalf("compileSessionSetupPlan() error = %v", err)
	}
	wantKinds := []SessionSetupStepKind{
		SessionSetupCatalog,
		SessionSetupDatabase,
		SessionSetupTable,
		SessionSetupUseCatalog,
		SessionSetupUseDatabase,
	}
	wantStatements := []string{
		"CREATE CATALOG IF NOT EXISTS `cat``alog` WITH ('a-option' = 'first''value', 'password' = 'pa''ss', 'z-option' = 'last')",
		"CREATE DATABASE IF NOT EXISTS `cat``alog`.`db name` WITH ('owner' = 'team''s')",
		"CREATE TABLE IF NOT EXISTS `cat``alog`.`db name`.`orders``raw` (id BIGINT) WITH ('connector' = 'datagen')",
		"USE CATALOG `cat``alog`",
		"USE `db name`",
	}
	if len(publicSteps) != len(wantKinds) || len(compiled) != len(wantKinds) {
		t.Fatalf("steps = %d public, %d compiled", len(publicSteps), len(compiled))
	}
	for index := range wantKinds {
		if publicSteps[index].Index != index || publicSteps[index].Kind != wantKinds[index] {
			t.Errorf("public step %d = %+v", index, publicSteps[index])
		}
		if compiled[index].statement != wantStatements[index] {
			t.Errorf("statement %d = %q, want %q", index, compiled[index].statement, wantStatements[index])
		}
	}
	if exposed := fmt.Sprintf("%+v", publicSteps); strings.Contains(exposed, "pa'ss") || strings.Contains(exposed, "CREATE CATALOG") {
		t.Fatalf("public compiled steps expose SQL or secret: %s", exposed)
	}
	if len(compiled[0].sensitiveValues) == 0 {
		t.Fatal("default sensitive key was not detected")
	}
}

func TestCompileSessionSetupRejectsInvalidPlansBeforeExecution(t *testing.T) {
	tests := []struct {
		name string
		plan SessionSetupPlan
	}{
		{name: "empty"},
		{name: "database without catalog", plan: SessionSetupPlan{Databases: []DatabaseSetup{{Name: "db"}}}},
		{name: "table without database", plan: SessionSetupPlan{Tables: []TableSetup{{Target: Identifier{Catalog: "c", Object: "t"}, Statement: "(id INT)"}}}},
		{name: "complete table SQL", plan: SessionSetupPlan{Tables: []TableSetup{{Target: Identifier{Catalog: "c", Database: "d", Object: "t"}, Statement: "CREATE TABLE c.d.t (id INT)"}}}},
		{name: "duplicate catalog", plan: SessionSetupPlan{Catalogs: []CatalogSetup{{Name: "c", Options: map[string]string{"type": "x"}}, {Name: "c", Options: map[string]string{"type": "x"}}}}},
		{name: "duplicate database", plan: SessionSetupPlan{Databases: []DatabaseSetup{{Catalog: "c", Name: "d"}, {Catalog: "c", Name: "d"}}}},
		{name: "duplicate table", plan: SessionSetupPlan{Tables: []TableSetup{
			{Target: Identifier{Catalog: "c", Database: "d", Object: "t"}, Statement: "(id INT)"},
			{Target: Identifier{Catalog: "c", Database: "d", Object: "t"}, Statement: "(id INT)"},
		}}},
		{name: "database scope without catalog", plan: SessionSetupPlan{CurrentDatabase: "d"}},
		{name: "control character", plan: SessionSetupPlan{CurrentCatalog: "bad\nname"}},
		{name: "empty sensitive key", plan: SessionSetupPlan{Catalogs: []CatalogSetup{{Name: "c", Options: map[string]string{"type": "x"}, SensitiveKeys: []string{" "}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := CompileSessionSetup(test.plan); err == nil {
				t.Fatal("CompileSessionSetup() succeeded")
			}
		})
	}
}

func TestApplySessionSetupUsesConfigureEndpointInRequiredOrder(t *testing.T) {
	var mu sync.Mutex
	var statements []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case "/v3/sessions/s/configure-session":
			if r.Method != http.MethodPost {
				t.Errorf("method = %s", r.Method)
			}
			var body struct {
				Statement string `json:"statement"`
			}
			decodeTestJSON(t, r, &body)
			mu.Lock()
			statements = append(statements, body.Statement)
			mu.Unlock()
			writeTestJSON(t, w, struct{}{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})
	plan := completeSessionSetupPlan()

	result, err := client.ApplySessionSetup(context.Background(), "s", plan, SessionSetupOptions{})
	if err != nil {
		t.Fatalf("ApplySessionSetup() error = %v", err)
	}
	if !result.Complete || result.FailedIndex != -1 || len(result.Steps) != 5 {
		t.Fatalf("result = %+v", result)
	}
	for _, step := range result.Steps {
		if !step.Applied || step.Verified || step.OutcomeUnknown {
			t.Errorf("step = %+v", step)
		}
	}
	mu.Lock()
	got := append([]string(nil), statements...)
	mu.Unlock()
	want := []string{
		"CREATE CATALOG IF NOT EXISTS `cat` WITH ('type' = 'generic_in_memory')",
		"CREATE DATABASE IF NOT EXISTS `cat`.`db`",
		"CREATE TABLE IF NOT EXISTS `cat`.`db`.`orders` (id BIGINT) WITH ('connector' = 'datagen')",
		"USE CATALOG `cat`",
		"USE `db`",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("statements = %#v", got)
	}
}

func TestSessionSetupRejectsUnsupportedAPIBeforeNetwork(t *testing.T) {
	for _, version := range []string{"v1", "v4"} {
		t.Run(version, func(t *testing.T) {
			var calls atomic.Int32
			httpClient := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return nil, fmt.Errorf("unexpected network call")
			})}
			client := newTestClient(t, Config{BaseURL: "http://gateway.invalid", APIVersion: version, HTTPClient: httpClient})
			plan := SessionSetupPlan{CurrentCatalog: "cat"}
			if _, err := client.ApplySessionSetup(context.Background(), "s", plan, SessionSetupOptions{}); !errors.Is(err, ErrUnsupportedAPI) {
				t.Fatalf("ApplySessionSetup() error = %v", err)
			}
			if _, err := client.OpenSessionWithSetup(context.Background(), OpenSessionRequest{}, plan, SessionSetupOptions{}); !errors.Is(err, ErrUnsupportedAPI) {
				t.Fatalf("OpenSessionWithSetup() error = %v", err)
			}
			if calls.Load() != 0 {
				t.Fatalf("network calls = %d", calls.Load())
			}
		})
	}
}

func TestApplySessionSetupStopsAfterDefiniteFailure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case "/v3/sessions/s/configure-session":
			if calls.Add(1) == 2 {
				writeTestJSONStatus(t, w, http.StatusBadRequest, map[string]string{"message": "invalid database"})
				return
			}
			writeTestJSON(t, w, struct{}{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})
	result, err := client.ApplySessionSetup(context.Background(), "s", completeSessionSetupPlan(), SessionSetupOptions{})
	var setupErr *SessionSetupError
	if err == nil || !errors.As(err, &setupErr) {
		t.Fatalf("ApplySessionSetup() error = %v", err)
	}
	if setupErr.FailedIndex != 1 || setupErr.StepKind != SessionSetupDatabase || setupErr.OutcomeUnknown {
		t.Fatalf("setup error = %+v", setupErr)
	}
	if calls.Load() != 2 || result.FailedIndex != 1 || !result.Steps[0].Applied || result.Steps[1].Applied || !result.PersistentChangesMayRemain {
		t.Fatalf("result=%+v calls=%d", result, calls.Load())
	}
}

func TestApplySessionSetupDoesNotRetryUnknownOutcome(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case "/v3/sessions/s/configure-session":
			calls.Add(1)
			writeTestJSONStatus(t, w, http.StatusServiceUnavailable, map[string]string{"message": "temporarily unavailable"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL, PollInterval: time.Millisecond})
	plan := SessionSetupPlan{Catalogs: []CatalogSetup{{Name: "cat", Options: map[string]string{"type": "generic_in_memory"}}}}
	result, err := client.ApplySessionSetup(context.Background(), "s", plan, SessionSetupOptions{})
	var unknown *ConfigurationOutcomeUnknownError
	if !errors.Is(err, ErrConfigurationOutcomeUnknown) || !errors.As(err, &unknown) {
		t.Fatalf("ApplySessionSetup() error = %v", err)
	}
	if calls.Load() != 1 || unknown.StepIndex != 0 || unknown.StepKind != SessionSetupCatalog || !result.Steps[0].OutcomeUnknown || !result.PersistentChangesMayRemain {
		t.Fatalf("result=%+v unknown=%+v calls=%d", result, unknown, calls.Load())
	}
}

func TestApplySessionSetupPreSendFailureIsNotOutcomeUnknown(t *testing.T) {
	var calls atomic.Int32
	httpClient := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, fmt.Errorf("dial refused")
	})}
	client := newTestClient(t, Config{BaseURL: "http://gateway.invalid", HTTPClient: httpClient})
	client.versionChecked = true
	result, err := client.ApplySessionSetup(context.Background(), "s", SessionSetupPlan{CurrentCatalog: "cat"}, SessionSetupOptions{})
	if err == nil || errors.Is(err, ErrConfigurationOutcomeUnknown) {
		t.Fatalf("ApplySessionSetup() error = %v", err)
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.RequestPhase != RequestNotSent {
		t.Fatalf("API error = %+v", apiErr)
	}
	if calls.Load() != 1 || result.Steps[0].OutcomeUnknown {
		t.Fatalf("result=%+v calls=%d", result, calls.Load())
	}
}

func TestSessionSetupRedactsReflectedSQLAndSecretsFromErrorsAndObservers(t *testing.T) {
	const secret = "same'password"
	var observations setupObservationRecorder
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case "/v3/sessions/s/configure-session":
			var body struct {
				Statement string `json:"statement"`
			}
			decodeTestJSON(t, r, &body)
			writeTestJSONStatus(t, w, http.StatusBadRequest, map[string]string{
				"code":    secret,
				"message": "rejected " + body.Statement + " value=" + secret + " escaped=" + strings.ReplaceAll(secret, "'", "''"),
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL, Observer: &observations, LifecycleObserver: &observations})
	plan := SessionSetupPlan{Catalogs: []CatalogSetup{{
		Name: "secret_catalog",
		Options: map[string]string{
			"password":          secret,
			"api_token":         secret,
			"empty-secret":      "",
			"custom.credential": secret,
			"type":              "custom",
		},
		SensitiveKeys: []string{"custom-credential"},
	}}}
	result, err := client.ApplySessionSetup(context.Background(), "s", plan, SessionSetupOptions{})
	if err == nil {
		t.Fatal("ApplySessionSetup() succeeded")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error chain has no APIError: %v", err)
	}
	for label, value := range map[string]string{
		"error":       err.Error(),
		"api message": apiErr.Message,
		"api code":    apiErr.Code,
		"result":      fmt.Sprintf("%+v", result),
		"observers":   observations.String(),
	} {
		if strings.Contains(value, secret) || strings.Contains(value, "same''password") || strings.Contains(value, "CREATE CATALOG") {
			t.Fatalf("%s leaks SQL or secret: %s", label, value)
		}
	}
	if apiErr.Message != "********" || apiErr.Code != "********" {
		t.Fatalf("redacted API error = %+v", apiErr)
	}
}

func TestSessionSetupMetadataVerificationSuccess(t *testing.T) {
	server := newSessionSetupMetadataServer(t, map[string][][]any{
		"SHOW CATALOGS":                {{"cat"}},
		"SHOW DATABASES IN `cat`":      {{"db"}},
		"SHOW TABLES IN `cat`.`db`":    {{"orders"}},
		"SHOW VIEWS IN `cat`.`db`":     {},
		"DESCRIBE `cat`.`db`.`orders`": {{"id", "BIGINT", "TRUE", "", "", "", ""}},
	})
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})
	result, err := client.ApplySessionSetup(context.Background(), "s", completeSessionSetupPlan(), SessionSetupOptions{VerifyMetadata: true, VerifyTableSchema: true})
	if err != nil {
		t.Fatalf("ApplySessionSetup() error = %v", err)
	}
	for index, step := range result.Steps {
		wantVerified := index < 3
		if step.Verified != wantVerified {
			t.Errorf("step %d = %+v", index, step)
		}
	}
}

func TestSessionSetupMetadataVerificationFailurePreservesAppliedState(t *testing.T) {
	server := newSessionSetupMetadataServer(t, map[string][][]any{
		"SHOW CATALOGS": {},
	})
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})
	result, err := client.ApplySessionSetup(context.Background(), "s", completeSessionSetupPlan(), SessionSetupOptions{VerifyMetadata: true})
	if !errors.Is(err, ErrSessionSetupVerification) {
		t.Fatalf("ApplySessionSetup() error = %v", err)
	}
	if result.FailedIndex != 0 || result.Complete || result.Steps[0].Verified || !result.PersistentChangesMayRemain {
		t.Fatalf("result = %+v", result)
	}
	for _, step := range result.Steps {
		if !step.Applied {
			t.Errorf("DDL success was lost after verification failure: %+v", step)
		}
	}
}

func TestOpenSessionWithSetupCleanupAndKeepPolicy(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(fmt.Sprintf("keep=%t", keep), func(t *testing.T) {
			var closeCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/api_versions":
					writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
				case r.URL.Path == "/v3/sessions" && r.Method == http.MethodPost:
					writeTestJSON(t, w, map[string]string{"sessionHandle": "opened"})
				case r.URL.Path == "/v3/sessions/opened/configure-session":
					writeTestJSONStatus(t, w, http.StatusBadRequest, map[string]string{"message": "setup rejected"})
				case r.URL.Path == "/v3/sessions/opened" && r.Method == http.MethodDelete:
					closeCalls.Add(1)
					w.WriteHeader(http.StatusOK)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client := newTestClient(t, Config{BaseURL: server.URL})
			result, err := client.OpenSessionWithSetup(
				context.Background(),
				OpenSessionRequest{SessionName: "setup"},
				SessionSetupPlan{CurrentCatalog: "cat"},
				SessionSetupOptions{KeepSessionOnFailure: keep},
			)
			if err == nil || result.SessionHandle != "opened" {
				t.Fatalf("result=%+v error=%v", result, err)
			}
			wantCloseCalls := int32(1)
			if keep {
				wantCloseCalls = 0
			}
			if closeCalls.Load() != wantCloseCalls || result.SessionClosed == keep {
				t.Fatalf("result=%+v close calls=%d", result, closeCalls.Load())
			}
		})
	}
}

func TestOpenSessionWithSetupUsesIndependentCleanupContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var closeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions" && r.Method == http.MethodPost:
			writeTestJSON(t, w, map[string]string{"sessionHandle": "opened"})
		case r.URL.Path == "/v3/sessions/opened/configure-session":
			cancel()
			writeTestJSONStatus(t, w, http.StatusBadRequest, map[string]string{"message": "caller canceled"})
		case r.URL.Path == "/v3/sessions/opened" && r.Method == http.MethodDelete:
			closeCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL, RequestTimeout: time.Second})
	result, err := client.OpenSessionWithSetup(ctx, OpenSessionRequest{}, SessionSetupPlan{CurrentCatalog: "cat"}, SessionSetupOptions{})
	if err == nil || result == nil || !result.SessionClosed || closeCalls.Load() != 1 {
		t.Fatalf("result=%+v error=%v closes=%d", result, err, closeCalls.Load())
	}
}

func TestOpenSessionWithSetupPreservesCleanupFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions" && r.Method == http.MethodPost:
			writeTestJSON(t, w, map[string]string{"sessionHandle": "opened"})
		case r.URL.Path == "/v3/sessions/opened/configure-session":
			writeTestJSONStatus(t, w, http.StatusBadRequest, map[string]string{"message": "setup rejected"})
		case r.URL.Path == "/v3/sessions/opened" && r.Method == http.MethodDelete:
			writeTestJSONStatus(t, w, http.StatusInternalServerError, map[string]string{"message": "close failed"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})
	result, err := client.OpenSessionWithSetup(context.Background(), OpenSessionRequest{}, SessionSetupPlan{CurrentCatalog: "cat"}, SessionSetupOptions{})
	var setupErr *SessionSetupError
	if !errors.As(err, &setupErr) || setupErr.CloseError == nil {
		t.Fatalf("OpenSessionWithSetup() error = %v", err)
	}
	if result.SessionClosed || !strings.Contains(err.Error(), "session cleanup failed") {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestSessionSetupTableVerificationRejectsViewCollision(t *testing.T) {
	server := newSessionSetupMetadataServer(t, map[string][][]any{
		"SHOW TABLES IN `cat`.`db`": {{"orders"}},
		"SHOW VIEWS IN `cat`.`db`":  {{"orders"}},
	})
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})
	plan := SessionSetupPlan{Tables: []TableSetup{{
		Target:    Identifier{Catalog: "cat", Database: "db", Object: "orders"},
		Statement: "(id BIGINT) WITH ('connector' = 'datagen')",
		Verify:    true,
	}}}
	result, err := client.ApplySessionSetup(context.Background(), "s", plan, SessionSetupOptions{})
	if !errors.Is(err, ErrSessionSetupVerification) || result.Steps[0].Verified || !result.Steps[0].Applied {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestApplySessionSetupSerializesConcurrentPlansPerSession(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{}, 1)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case "/v3/sessions/s/configure-session":
			call := calls.Add(1)
			if call == 1 {
				close(firstStarted)
				<-releaseFirst
			} else {
				secondStarted <- struct{}{}
			}
			writeTestJSON(t, w, struct{}{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, Config{BaseURL: server.URL})
	errCh := make(chan error, 2)
	go func() {
		_, err := client.ApplySessionSetup(context.Background(), "s", SessionSetupPlan{CurrentCatalog: "first"}, SessionSetupOptions{})
		errCh <- err
	}()
	<-firstStarted
	go func() {
		_, err := client.ApplySessionSetup(context.Background(), "s", SessionSetupPlan{CurrentCatalog: "second"}, SessionSetupOptions{})
		errCh <- err
	}()
	select {
	case <-secondStarted:
		t.Fatal("second plan entered configure-session before first plan completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)
	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatalf("ApplySessionSetup() error = %v", err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("configure calls = %d", calls.Load())
	}
}

func completeSessionSetupPlan() SessionSetupPlan {
	return SessionSetupPlan{
		Catalogs:  []CatalogSetup{{Name: "cat", IfNotExists: true, Options: map[string]string{"type": "generic_in_memory"}}},
		Databases: []DatabaseSetup{{Catalog: "cat", Name: "db", IfNotExists: true}},
		Tables: []TableSetup{{
			Target:      Identifier{Catalog: "cat", Database: "db", Object: "orders"},
			Statement:   "(id BIGINT) WITH ('connector' = 'datagen')",
			IfNotExists: true,
		}},
		CurrentCatalog:  "cat",
		CurrentDatabase: "db",
	}
}

type setupObservationRecorder struct {
	mu        sync.Mutex
	requests  []RequestObservation
	lifecycle []Observation
}

func (r *setupObservationRecorder) ObserveRequest(_ context.Context, observation RequestObservation) {
	r.mu.Lock()
	r.requests = append(r.requests, observation)
	r.mu.Unlock()
}

func (r *setupObservationRecorder) ObserveLifecycle(_ context.Context, observation Observation) {
	r.mu.Lock()
	r.lifecycle = append(r.lifecycle, observation)
	r.mu.Unlock()
}

func (r *setupObservationRecorder) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fmt.Sprintf("requests=%+v lifecycle=%+v", r.requests, r.lifecycle)
}

func newSessionSetupMetadataServer(t *testing.T, rowsByStatement map[string][][]any) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	nextOperation := 0
	operationRows := make(map[string][][]any)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api_versions":
			writeTestJSON(t, w, map[string]any{"versions": []string{"V3"}})
		case r.URL.Path == "/v3/sessions/s/configure-session":
			writeTestJSON(t, w, struct{}{})
		case r.URL.Path == "/v3/sessions/s/statements":
			var body struct {
				Statement string `json:"statement"`
			}
			decodeTestJSON(t, r, &body)
			rows, exists := rowsByStatement[body.Statement]
			if !exists {
				writeTestJSONStatus(t, w, http.StatusBadRequest, map[string]string{"message": "unexpected metadata statement"})
				return
			}
			mu.Lock()
			nextOperation++
			handle := fmt.Sprintf("metadata-%d", nextOperation)
			operationRows[handle] = rows
			mu.Unlock()
			writeTestJSON(t, w, map[string]string{"operationHandle": handle})
		case strings.Contains(r.URL.Path, "/operations/") && strings.HasSuffix(r.URL.Path, "/result/0"):
			parts := strings.Split(r.URL.Path, "/")
			handle := parts[5]
			mu.Lock()
			rows := operationRows[handle]
			mu.Unlock()
			writeTestJSON(t, w, metadataResultPage(rows))
		case strings.HasSuffix(r.URL.Path, "/close"):
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
}
