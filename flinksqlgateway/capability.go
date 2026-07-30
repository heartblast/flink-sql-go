package flinksqlgateway

import "context"

// Capabilities는 호출자가 Flink release와 API 버전을 직접 비교하지 않아도 기능 지원 여부를
// 확인하게 한다. slice는 조회할 때마다 복사되어 client 내부 registry와 memory를 공유하지 않는다.
type Capabilities struct {
	// APIVersion은 현재 client가 선택한 REST API 버전이다.
	APIVersion string
	// SupportedAPIVersions는 release profile이 허용하는 REST API 버전의 복사본이다.
	SupportedAPIVersions []string
	// DefaultAPIVersion은 release profile의 stable 기본 REST API 버전이다.
	DefaultAPIVersion string
	// ConfigureSession은 선택한 protocol이 configure-session endpoint를 제공함을 나타낸다.
	ConfigureSession bool
	// CompleteStatement는 선택한 protocol이 complete-statement endpoint를 제공함을 나타낸다.
	CompleteStatement bool
	// RowFormat은 결과 요청에서 rowFormat 선택을 지원함을 나타낸다.
	RowFormat bool
	// MaterializedTable은 materialized table endpoint 집합을 지원함을 나타낸다.
	MaterializedTable bool
	// DeployScript는 application mode script 배포 endpoint를 지원함을 나타낸다.
	DeployScript bool
	// WireExecutionTimeout은 양수 executionTimeout을 REST request body에 포함할 수 있음을 나타낸다.
	WireExecutionTimeout bool
}

// Capabilities는 설정된 API 버전을 검증하고 보수적인 기능 flag를 반환한다.
// 알 수 없는 이후 버전은 지원한다고 확인된 기능이 없는 것으로 처리한다.
func (c *GatewayClient) Capabilities(ctx context.Context) (Capabilities, error) {
	if err := c.CheckAPIVersion(ctx); err != nil {
		return Capabilities{}, err
	}
	return c.selectedCapabilities(), nil
}

// capabilitiesForVersion은 기존 내부 테스트와 source 호환성을 위해 Flink 1.20 profile에 API
// 버전 제약을 적용한다. 신규 코드는 release profile을 함께 사용하는 capabilitiesForProfile을 쓴다.
func capabilitiesForVersion(version string) Capabilities {
	profile, err := profileForReleaseLine(Flink120)
	if err != nil {
		return Capabilities{APIVersion: version}
	}
	return capabilitiesForProfile(profile, version)
}
