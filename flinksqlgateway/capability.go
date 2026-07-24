package flinksqlgateway

import "context"

// Capabilities는 호출자가 API 버전 문자열을 직접 비교하지 않아도 기능 지원 여부를 확인하게 한다.
type Capabilities struct {
	APIVersion        string
	ConfigureSession  bool
	CompleteStatement bool
	RowFormat         bool
	MaterializedTable bool
}

// Capabilities는 설정된 API 버전을 검증하고 보수적인 기능 flag를 반환한다.
// 알 수 없는 이후 버전은 지원한다고 확인된 기능이 없는 것으로 처리한다.
func (c *GatewayClient) Capabilities(ctx context.Context) (Capabilities, error) {
	if err := c.CheckAPIVersion(ctx); err != nil {
		return Capabilities{}, err
	}
	return capabilitiesForVersion(c.cfg.APIVersion), nil
}

// capabilitiesForVersion은 검증된 API 버전을 알려진 Flink 1.20 기능 집합으로 변환한다.
func capabilitiesForVersion(version string) Capabilities {
	capabilities := Capabilities{APIVersion: version}
	switch version {
	case "v1":
		return capabilities
	case "v2":
		capabilities.ConfigureSession = true
		capabilities.CompleteStatement = true
		capabilities.RowFormat = true
	case "v3":
		capabilities.ConfigureSession = true
		capabilities.CompleteStatement = true
		capabilities.RowFormat = true
		capabilities.MaterializedTable = true
	}
	return capabilities
}
