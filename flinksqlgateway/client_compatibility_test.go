package flinksqlgateway_test

import (
	"context"
	"time"

	"github.com/heartblast/flink-sql-go/flinksqlgateway"
)

// legacyClientStub은 v0.1.4 Client 계약만 구현해 SessionSetupExecutor가 별도 interface로
// 유지되는지 compile 단계에서 확인한다.
type legacyClientStub struct{}

func (legacyClientStub) GetInfo(context.Context) (*flinksqlgateway.GatewayInfo, error) {
	return nil, nil
}

func (legacyClientStub) GetAPIVersions(context.Context) ([]string, error) { return nil, nil }

func (legacyClientStub) CheckAPIVersion(context.Context) error { return nil }

func (legacyClientStub) OpenSession(context.Context, flinksqlgateway.OpenSessionRequest) (*flinksqlgateway.Session, error) {
	return nil, nil
}

func (legacyClientStub) GetSessionConfig(context.Context, string) (map[string]string, error) {
	return nil, nil
}

func (legacyClientStub) ConfigureSession(context.Context, string, string, time.Duration) error {
	return nil
}

func (legacyClientStub) CompleteStatement(context.Context, string, string, int) ([]string, error) {
	return nil, nil
}

func (legacyClientStub) Heartbeat(context.Context, string) error { return nil }

func (legacyClientStub) ExecuteStatement(context.Context, string, flinksqlgateway.ExecuteStatementRequest) (*flinksqlgateway.Operation, error) {
	return nil, nil
}

func (legacyClientStub) GetOperationStatus(context.Context, string, string) (flinksqlgateway.OperationStatus, error) {
	return "", nil
}

func (legacyClientStub) FetchResults(context.Context, string, string, int64, flinksqlgateway.RowFormat) (*flinksqlgateway.ResultPage, error) {
	return nil, nil
}

func (legacyClientStub) CancelOperation(context.Context, string, string) error { return nil }

func (legacyClientStub) CloseOperation(context.Context, string, string) error { return nil }

func (legacyClientStub) CloseSession(context.Context, string) error { return nil }

func (legacyClientStub) ExecuteAndWait(context.Context, string, string, flinksqlgateway.ExecuteOptions) (*flinksqlgateway.ExecutionResult, error) {
	return nil, nil
}

func (legacyClientStub) StreamResults(context.Context, string, string, flinksqlgateway.StreamOptions) (<-chan flinksqlgateway.ResultEvent, <-chan error) {
	return nil, nil
}

var _ flinksqlgateway.Client = legacyClientStub{}
