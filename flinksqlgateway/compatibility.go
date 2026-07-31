package flinksqlgateway

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// ReleaseLine은 patch 버전과 독립적인 Apache Flink release 계열을 나타낸다.
type ReleaseLine string

const (
	// Flink120은 Apache Flink 1.20.x release 계열이다.
	Flink120 ReleaseLine = "1.20"
	// Flink20은 Apache Flink 2.0.x release 계열이다.
	Flink20 ReleaseLine = "2.0"
	// Flink21은 Apache Flink 2.1.x release 계열이다.
	Flink21 ReleaseLine = "2.1"
	// Flink22는 Apache Flink 2.2.x release 계열이다.
	Flink22 ReleaseLine = "2.2"
	// Flink23은 Apache Flink 2.3.x release 계열이다.
	Flink23 ReleaseLine = "2.3"
)

// ReleaseStatus는 release line의 구현 및 검증 수준을 구분한다.
type ReleaseStatus string

const (
	// ReleasePlanned는 구현 전에 지원 범위만 계획된 상태이다.
	ReleasePlanned ReleaseStatus = "planned"
	// ReleaseExperimental은 구현은 있지만 실제 Gateway 검증이 충분하지 않은 상태이다.
	ReleaseExperimental ReleaseStatus = "experimental"
	// ReleaseSupported는 구현과 정기 검증이 모두 유지되는 상태이다.
	ReleaseSupported ReleaseStatus = "supported"
	// ReleaseMaintenance는 회귀 및 보안 수정 중심으로 유지되는 상태이다.
	ReleaseMaintenance ReleaseStatus = "maintenance"
	// ReleaseUnsupported는 더 이상 지원하지 않는 상태이다.
	ReleaseUnsupported ReleaseStatus = "unsupported"
)

// CompatibilityMode는 Flink release line을 자동 감지할지 명시적으로 고정할지 결정한다.
type CompatibilityMode string

const (
	// CompatibilityAuto는 /info 응답에서 Flink release line을 감지한다.
	CompatibilityAuto CompatibilityMode = "auto"
	// CompatibilityFlink120은 Flink 1.20.x profile을 명시적으로 선택한다.
	CompatibilityFlink120 CompatibilityMode = "flink-1.20"
	// CompatibilityFlink20은 Flink 2.0.x profile을 명시적으로 선택한다.
	CompatibilityFlink20 CompatibilityMode = "flink-2.0"
	// CompatibilityFlink21은 Flink 2.1.x profile을 명시적으로 선택한다.
	CompatibilityFlink21 CompatibilityMode = "flink-2.1"
	// CompatibilityFlink22는 Flink 2.2.x profile을 명시적으로 선택한다.
	CompatibilityFlink22 CompatibilityMode = "flink-2.2"
	// CompatibilityFlink23은 Flink 2.3.x profile을 명시적으로 선택한다.
	CompatibilityFlink23 CompatibilityMode = "flink-2.3"
)

// APIVersionPolicy는 profile과 server가 공통으로 지원하는 REST API 버전의 선택 규칙이다.
type APIVersionPolicy string

const (
	// APIVersionStable은 검증된 기본 버전 이하에서 가장 높은 공통 버전을 선택한다.
	APIVersionStable APIVersionPolicy = "stable"
	// APIVersionHighest는 server와 profile의 공통 버전 중 가장 높은 값을 선택한다.
	APIVersionHighest APIVersionPolicy = "highest"
	// APIVersionExplicit은 APIVersion에 지정한 값만 허용하고 fallback하지 않는다.
	APIVersionExplicit APIVersionPolicy = "explicit"
)

// DetectionSource는 현재 compatibility profile이 선택된 근거를 나타낸다.
type DetectionSource string

const (
	// DetectionSourceAuto는 /info의 제품 버전에서 profile을 감지했음을 나타낸다.
	DetectionSourceAuto DetectionSource = "auto"
	// DetectionSourceConfigured는 Config의 명시적 mode로 profile을 선택했음을 나타낸다.
	DetectionSourceConfigured DetectionSource = "configured"
)

// SupportedFlinkRelease는 구현된 release line과 직접 검증한 patch 버전을 구분해 설명한다.
type SupportedFlinkRelease struct {
	// ReleaseLine은 지원 모델에 등록된 major.minor 계열이다.
	ReleaseLine ReleaseLine
	// Status는 구현과 실제 검증의 현재 수준이다.
	Status ReleaseStatus
	// TestedVersions는 실제 Gateway 통합 테스트를 완료한 patch 버전만 포함한다.
	TestedVersions []string
	// RESTAPIVersions는 release profile이 허용하는 protocol 버전이다.
	RESTAPIVersions []string
	// StableAPIVersion은 stable 정책이 우선 선택하는 버전이다.
	StableAPIVersion string
	// Capabilities는 release 수준의 기능과 quirk snapshot이다.
	Capabilities Capabilities
}

// CompatibilityMatrixInfo는 library가 표현할 수 있는 Flink 및 REST API 조합의 snapshot이다.
type CompatibilityMatrixInfo struct {
	// SchemaVersion은 compatibility manifest schema 버전이다.
	SchemaVersion int
	// DefaultReleaseLine은 수동 fallback과 legacy metadata의 기준 release line이다.
	DefaultReleaseLine ReleaseLine
	// DefaultAPIVersion은 stable 정책의 project 기본값이다.
	DefaultAPIVersion string
	// SupportedReleases는 호출자가 변경할 수 있는 registry 복사본이다.
	SupportedReleases []SupportedFlinkRelease
}

// CompatibilityInfo는 client가 선택한 release profile과 REST API 버전의 읽기 전용 snapshot이다.
type CompatibilityInfo struct {
	// FlinkVersion은 auto mode에서 /info가 반환한 원본 제품 버전이며 수동 mode에서는 비어 있다.
	FlinkVersion string
	// ReleaseLine은 선택된 compatibility profile의 major.minor 계열이다.
	ReleaseLine ReleaseLine
	// APIVersion은 server와 profile의 교집합에서 선택한 REST API 버전이다.
	APIVersion string
	// Capabilities는 release profile과 선택 protocol을 결합한 기능 snapshot이다.
	Capabilities Capabilities
	// DetectionSource는 profile이 /info 또는 Config 중 어디에서 선택됐는지 나타낸다.
	DetectionSource DetectionSource
}

// CompatibilityProvider는 기존 Client 계약을 확장하지 않고 compatibility 감지와 조회를 제공한다.
type CompatibilityProvider interface {
	// CheckCompatibility는 lazy 감지를 수행하고 선택 결과가 유효한지 검증한다.
	CheckCompatibility(ctx context.Context) error
	// GetCompatibilityInfo는 lazy 감지를 수행한 뒤 호출자가 변경할 수 있는 snapshot을 반환한다.
	GetCompatibilityInfo(ctx context.Context) (CompatibilityInfo, error)
}

// compatibilityProfile은 compatibility.yaml의 release 항목을 런타임 선택에 맞게 정규화한 값이다.
type compatibilityProfile struct {
	releaseLine      ReleaseLine
	status           ReleaseStatus
	testedVersions   []string
	apiVersions      []string
	stableAPIVersion string
	capabilities     Capabilities
}

// compatibilityProfiles는 compatibility.yaml과 일치하는 생성 대상 registry이다. 일치 여부는
// manifest contract test가 검증하며 런타임 YAML parser는 사용하지 않는다.
var compatibilityProfiles = []compatibilityProfile{
	newCompatibilityProfile(Flink120, ReleaseMaintenance, []string{"1.20.4"}, []string{"v1", "v2", "v3"}, "v3", true, true, true, true, false, false),
	newCompatibilityProfile(Flink20, ReleaseExperimental, []string{}, []string{"v1", "v2", "v3", "v4"}, "v3", true, true, true, true, true, true),
	newCompatibilityProfile(Flink21, ReleaseExperimental, []string{}, []string{"v1", "v2", "v3", "v4"}, "v3", true, true, true, true, true, true),
	newCompatibilityProfile(Flink22, ReleaseExperimental, []string{}, []string{"v1", "v2", "v3", "v4"}, "v3", true, true, true, true, true, true),
	newCompatibilityProfile(Flink23, ReleaseExperimental, []string{}, []string{"v1", "v2", "v3", "v4"}, "v3", true, true, true, true, true, true),
}

// sqlGatewayProtocolCapabilities는 REST API 번호의 순서를 기능 누적으로 해석하지 않고 각
// protocol 문서에 실제로 등록된 endpoint와 wire field만 기술한다. 특히 v3의 Materialized
// Table refresh와 v4의 Deploy Script는 서로 배타적인 endpoint 집합이다.
var sqlGatewayProtocolCapabilities = map[string]Capabilities{
	"v1": {
		WireExecutionTimeout: true,
	},
	"v2": {
		ConfigureSession:     true,
		CompleteStatement:    true,
		RowFormat:            true,
		WireExecutionTimeout: true,
	},
	"v3": {
		ConfigureSession:     true,
		CompleteStatement:    true,
		RowFormat:            true,
		MaterializedTable:    true,
		WireExecutionTimeout: true,
	},
	"v4": {
		ConfigureSession:     true,
		CompleteStatement:    true,
		RowFormat:            true,
		DeployScript:         true,
		WireExecutionTimeout: true,
	},
}

// newCompatibilityProfile은 registry 선언에서 profile capability와 API 목록을 한곳에 결합한다.
func newCompatibilityProfile(
	releaseLine ReleaseLine,
	status ReleaseStatus,
	testedVersions []string,
	apiVersions []string,
	stableAPIVersion string,
	configureSession bool,
	completeStatement bool,
	rowFormat bool,
	materializedTable bool,
	deployScript bool,
	wireExecutionTimeout bool,
) compatibilityProfile {
	return compatibilityProfile{
		releaseLine:      releaseLine,
		status:           status,
		testedVersions:   cloneStrings(testedVersions),
		apiVersions:      cloneStrings(apiVersions),
		stableAPIVersion: stableAPIVersion,
		capabilities: Capabilities{
			SupportedAPIVersions: cloneStrings(apiVersions),
			DefaultAPIVersion:    stableAPIVersion,
			ConfigureSession:     configureSession,
			CompleteStatement:    completeStatement,
			RowFormat:            rowFormat,
			MaterializedTable:    materializedTable,
			DeployScript:         deployScript,
			WireExecutionTimeout: wireExecutionTimeout,
		},
	}
}

// SupportedFlinkVersions는 지원 모델에 등록된 release line을 독립적인 복사본으로 반환한다.
func SupportedFlinkVersions() []SupportedFlinkRelease {
	result := make([]SupportedFlinkRelease, 0, len(compatibilityProfiles))
	for _, profile := range compatibilityProfiles {
		result = append(result, profile.public())
	}
	return result
}

// CompatibilityMatrix는 기본 선택값과 전체 release registry의 독립적인 snapshot을 반환한다.
func CompatibilityMatrix() CompatibilityMatrixInfo {
	return CompatibilityMatrixInfo{
		SchemaVersion:      2,
		DefaultReleaseLine: Flink120,
		DefaultAPIVersion:  DefaultAPIVersion,
		SupportedReleases:  SupportedFlinkVersions(),
	}
}

// public은 내부 profile과 slice memory를 공유하지 않는 공개 값을 만든다.
func (profile compatibilityProfile) public() SupportedFlinkRelease {
	return SupportedFlinkRelease{
		ReleaseLine:      profile.releaseLine,
		Status:           profile.status,
		TestedVersions:   cloneStrings(profile.testedVersions),
		RESTAPIVersions:  cloneStrings(profile.apiVersions),
		StableAPIVersion: profile.stableAPIVersion,
		Capabilities:     cloneCapabilities(profile.capabilities),
	}
}

// CheckCompatibility는 기존 CheckAPIVersion과 같은 lazy 검증을 명시적인 이름으로 제공한다.
func (c *GatewayClient) CheckCompatibility(ctx context.Context) error {
	return c.CheckAPIVersion(ctx)
}

// GetCompatibilityInfo는 감지 결과와 capability slice를 호출자 전용 복사본으로 반환한다.
func (c *GatewayClient) GetCompatibilityInfo(ctx context.Context) (CompatibilityInfo, error) {
	if err := c.CheckCompatibility(ctx); err != nil {
		return CompatibilityInfo{}, err
	}
	return c.compatibilitySnapshot(), nil
}

// resolveCompatibility는 Config mode와 server metadata를 이용해 하나의 immutable 선택 결과를 만든다.
func (c *GatewayClient) resolveCompatibility(ctx context.Context) (CompatibilityInfo, error) {
	var (
		profile      compatibilityProfile
		flinkVersion string
		source       DetectionSource
	)
	if c.cfg.CompatibilityMode == CompatibilityAuto {
		info, err := c.GetInfo(ctx)
		if err != nil {
			return CompatibilityInfo{}, newCompatibilityError(ErrCompatibilityDetection, "GET /info", "", "", "", err)
		}
		line, err := parseFlinkReleaseLine(info.Version)
		if err != nil {
			return CompatibilityInfo{}, err
		}
		flinkVersion = strings.TrimSpace(info.Version)
		profile, err = profileForReleaseLine(line)
		if err != nil {
			return CompatibilityInfo{}, err
		}
		source = DetectionSourceAuto
	} else {
		line, err := releaseLineForMode(c.cfg.CompatibilityMode)
		if err != nil {
			return CompatibilityInfo{}, err
		}
		profile, err = profileForReleaseLine(line)
		if err != nil {
			return CompatibilityInfo{}, err
		}
		source = DetectionSourceConfigured
	}

	advertised, err := c.getAPIVersions(ctx)
	if err != nil {
		return CompatibilityInfo{}, newCompatibilityError(ErrCompatibilityDetection, "GET /api_versions", flinkVersion, profile.releaseLine, "", err)
	}
	selected, err := selectAPIVersion(profile, advertised, c.cfg.APIVersionPolicy, c.cfg.APIVersion)
	if err != nil {
		return CompatibilityInfo{}, err
	}
	capabilities := capabilitiesForProfile(profile, selected)
	return CompatibilityInfo{
		FlinkVersion:    flinkVersion,
		ReleaseLine:     profile.releaseLine,
		APIVersion:      selected,
		Capabilities:    capabilities,
		DetectionSource: source,
	}, nil
}

// compatibilitySnapshot은 이미 검증된 결과를 복사한다. 기존 테스트가 검증 cache만 직접
// 설정한 경우에는 1.20 profile과 Config API 버전으로 보수적인 호환 값을 만든다.
func (c *GatewayClient) compatibilitySnapshot() CompatibilityInfo {
	c.versionMu.Lock()
	defer c.versionMu.Unlock()
	if c.compatibility.APIVersion != "" {
		return cloneCompatibilityInfo(c.compatibility)
	}
	profile, err := profileForModeOrDefault(c.cfg.CompatibilityMode)
	if err != nil {
		return CompatibilityInfo{APIVersion: c.cfg.APIVersion}
	}
	return CompatibilityInfo{
		ReleaseLine:     profile.releaseLine,
		APIVersion:      c.cfg.APIVersion,
		Capabilities:    capabilitiesForProfile(profile, c.cfg.APIVersion),
		DetectionSource: DetectionSourceConfigured,
	}
}

// selectedAPIVersion은 lazy 검증 전에는 기존 기본값을, 검증 후에는 실제 선택값을 반환한다.
func (c *GatewayClient) selectedAPIVersion() string {
	c.versionMu.Lock()
	defer c.versionMu.Unlock()
	if c.compatibility.APIVersion != "" {
		return c.compatibility.APIVersion
	}
	return c.cfg.APIVersion
}

// selectedCapabilities는 검증 결과에서 독립적인 capability snapshot을 반환한다.
func (c *GatewayClient) selectedCapabilities() Capabilities {
	return c.compatibilitySnapshot().Capabilities
}

// configuredCompatibility는 수동 release line과 explicit API가 모두 정해진 경우 network 전에도
// 확정할 수 있는 profile 결과를 반환한다. auto 및 선택 정책은 server 교집합이 필요하다.
func (c *GatewayClient) configuredCompatibility() (CompatibilityInfo, bool, error) {
	if c.cfg.CompatibilityMode == CompatibilityAuto || c.cfg.APIVersionPolicy != APIVersionExplicit {
		return CompatibilityInfo{}, false, nil
	}
	profile, err := profileForModeOrDefault(c.cfg.CompatibilityMode)
	if err != nil {
		return CompatibilityInfo{}, true, err
	}
	if !containsVersion(profile.apiVersions, c.cfg.APIVersion) {
		return CompatibilityInfo{}, true, newCompatibilityError(ErrExplicitAPIVersionUnsupportedByProfile, "select explicit API version", "", profile.releaseLine, c.cfg.APIVersion, nil)
	}
	return CompatibilityInfo{
		ReleaseLine:     profile.releaseLine,
		APIVersion:      c.cfg.APIVersion,
		Capabilities:    capabilitiesForProfile(profile, c.cfg.APIVersion),
		DetectionSource: DetectionSourceConfigured,
	}, true, nil
}

// compatibilityForCapability는 explicit profile에서 미지원 기능을 metadata network 호출 전에
// 차단하고, 그 밖의 경우에는 lazy compatibility 감지를 마친 뒤 같은 조건을 다시 검증한다.
func (c *GatewayClient) compatibilityForCapability(
	ctx context.Context,
	operation string,
	supported func(Capabilities) bool,
) (CompatibilityInfo, error) {
	configured, known, err := c.configuredCompatibility()
	if err != nil {
		return CompatibilityInfo{}, err
	}
	if known && !supported(configured.Capabilities) {
		return CompatibilityInfo{}, newCompatibilityError(
			ErrUnsupportedCapability,
			operation,
			configured.FlinkVersion,
			configured.ReleaseLine,
			configured.APIVersion,
			nil,
		)
	}
	if err := c.CheckCompatibility(ctx); err != nil {
		return CompatibilityInfo{}, err
	}
	compatibility := c.compatibilitySnapshot()
	if !supported(compatibility.Capabilities) {
		return CompatibilityInfo{}, newCompatibilityError(
			ErrUnsupportedCapability,
			operation,
			compatibility.FlinkVersion,
			compatibility.ReleaseLine,
			compatibility.APIVersion,
			nil,
		)
	}
	return compatibility, nil
}

// parseFlinkReleaseLine은 정식 및 prerelease Flink 제품 버전에서 major.minor 계열을 추출한다.
func parseFlinkReleaseLine(value string) (ReleaseLine, error) {
	original := strings.TrimSpace(value)
	version := strings.TrimPrefix(strings.ToLower(original), "v")
	core := version
	hasSuffix := false
	if suffixIndex := strings.IndexAny(version, "-+"); suffixIndex >= 0 {
		hasSuffix = true
		core = version[:suffixIndex]
		if err := validateVersionSuffix(version[suffixIndex:]); err != nil {
			return "", newCompatibilityError(ErrInvalidFlinkVersion, "parse Flink version", original, "", "", err)
		}
	}
	parts := strings.Split(core, ".")
	if len(parts) < 2 || len(parts) > 3 {
		return "", newCompatibilityError(ErrInvalidFlinkVersion, "parse Flink version", original, "", "", nil)
	}
	if hasSuffix && len(parts) != 3 {
		return "", newCompatibilityError(ErrInvalidFlinkVersion, "parse Flink version", original, "", "", fmt.Errorf("version suffix requires a patch component"))
	}
	major, err := versionNumber(parts[0])
	if err != nil {
		return "", newCompatibilityError(ErrInvalidFlinkVersion, "parse Flink version", original, "", "", err)
	}
	minor, err := versionNumber(parts[1])
	if err != nil {
		return "", newCompatibilityError(ErrInvalidFlinkVersion, "parse Flink version", original, "", "", err)
	}
	if len(parts) >= 3 {
		if _, err := versionNumber(parts[2]); err != nil {
			return "", newCompatibilityError(ErrInvalidFlinkVersion, "parse Flink version", original, "", "", err)
		}
	}
	return ReleaseLine(strconv.Itoa(major) + "." + strconv.Itoa(minor)), nil
}

// versionNumber는 release core의 한 component가 숫자로만 구성됐는지 확인한다.
func versionNumber(part string) (int, error) {
	if part == "" {
		return 0, fmt.Errorf("missing numeric component")
	}
	for index := range len(part) {
		if part[index] < '0' || part[index] > '9' {
			return 0, fmt.Errorf("invalid numeric component")
		}
	}
	return strconv.Atoi(part)
}

// validateVersionSuffix는 prerelease와 build metadata의 dotted identifier를 허용하되
// 제어문자나 빈 identifier가 version metadata로 들어오는 것을 거부한다.
func validateVersionSuffix(suffix string) error {
	if len(suffix) < 2 {
		return fmt.Errorf("empty version suffix")
	}
	previousSeparator := true
	plusSeen := false
	for index, char := range suffix {
		switch {
		case index == 0 && (char == '-' || char == '+'):
			plusSeen = char == '+'
		case char == '+':
			if plusSeen || previousSeparator {
				return fmt.Errorf("invalid version suffix")
			}
			plusSeen = true
			previousSeparator = true
		case char == '.':
			if previousSeparator {
				return fmt.Errorf("invalid version suffix")
			}
			previousSeparator = true
		case char == '-' || char >= '0' && char <= '9' || char >= 'a' && char <= 'z':
			previousSeparator = false
		default:
			return fmt.Errorf("invalid version suffix")
		}
	}
	if previousSeparator {
		return fmt.Errorf("invalid version suffix")
	}
	return nil
}

// profileForReleaseLine은 등록되지 않은 계열을 typed error로 구분한다.
func profileForReleaseLine(line ReleaseLine) (compatibilityProfile, error) {
	for _, profile := range compatibilityProfiles {
		if profile.releaseLine == line {
			if !releaseStatusSelectable(profile.status) {
				return compatibilityProfile{}, newCompatibilityError(ErrUnsupportedFlinkVersion, "select compatibility profile", "", line, "", nil)
			}
			return profile, nil
		}
	}
	return compatibilityProfile{}, newCompatibilityError(ErrUnsupportedFlinkVersion, "select compatibility profile", "", line, "", nil)
}

// releaseStatusSelectable은 실제 요청에 사용할 수 있는 구현 상태만 허용한다.
func releaseStatusSelectable(status ReleaseStatus) bool {
	switch status {
	case ReleaseExperimental, ReleaseSupported, ReleaseMaintenance:
		return true
	default:
		return false
	}
}

// profileForModeOrDefault는 수동 mode를 profile로 바꾸며 auto의 미감지 fallback은 1.20이다.
func profileForModeOrDefault(mode CompatibilityMode) (compatibilityProfile, error) {
	if mode == "" || mode == CompatibilityAuto {
		return profileForReleaseLine(Flink120)
	}
	line, err := releaseLineForMode(mode)
	if err != nil {
		return compatibilityProfile{}, err
	}
	return profileForReleaseLine(line)
}

// releaseLineForMode는 Config의 명시적 mode를 release line으로 변환한다.
func releaseLineForMode(mode CompatibilityMode) (ReleaseLine, error) {
	switch mode {
	case CompatibilityFlink120:
		return Flink120, nil
	case CompatibilityFlink20:
		return Flink20, nil
	case CompatibilityFlink21:
		return Flink21, nil
	case CompatibilityFlink22:
		return Flink22, nil
	case CompatibilityFlink23:
		return Flink23, nil
	default:
		return "", fmt.Errorf("flinksqlgateway: unsupported CompatibilityMode %q", mode)
	}
}

// selectAPIVersion은 server와 profile의 교집합에서 정책에 맞는 한 버전을 선택한다.
func selectAPIVersion(profile compatibilityProfile, advertised []string, policy APIVersionPolicy, explicit string) (string, error) {
	serverSet := make(map[string]struct{}, len(advertised))
	for _, version := range advertised {
		normalized, err := normalizeVersion(version)
		if err == nil {
			serverSet[normalized] = struct{}{}
		}
	}
	profileSet := make(map[string]struct{}, len(profile.apiVersions))
	for _, version := range profile.apiVersions {
		profileSet[version] = struct{}{}
	}

	if policy == APIVersionExplicit {
		if _, ok := profileSet[explicit]; !ok {
			return "", newCompatibilityError(ErrExplicitAPIVersionUnsupportedByProfile, "select explicit API version", "", profile.releaseLine, explicit, nil)
		}
		if _, ok := serverSet[explicit]; !ok {
			return "", newCompatibilityError(ErrExplicitAPIVersionUnsupportedByServer, "select explicit API version", "", profile.releaseLine, explicit, nil)
		}
		return explicit, nil
	}

	common := make([]string, 0, len(profile.apiVersions))
	for _, version := range profile.apiVersions {
		if _, ok := serverSet[version]; ok {
			common = append(common, version)
		}
	}
	if len(common) == 0 {
		return "", newCompatibilityError(ErrNoCompatibleAPIVersion, "select API version", "", profile.releaseLine, "", nil)
	}
	sort.Slice(common, func(i, j int) bool {
		return apiVersionNumber(common[i]) < apiVersionNumber(common[j])
	})
	if policy == APIVersionHighest {
		return common[len(common)-1], nil
	}
	stableNumber := apiVersionNumber(profile.stableAPIVersion)
	for index := len(common) - 1; index >= 0; index-- {
		if apiVersionNumber(common[index]) <= stableNumber {
			return common[index], nil
		}
	}
	return "", newCompatibilityError(ErrNoCompatibleAPIVersion, "select stable API version", "", profile.releaseLine, "", nil)
}

// capabilitiesForProfile은 release capability를 선택한 protocol 버전의 endpoint 집합으로 제한한다.
func capabilitiesForProfile(profile compatibilityProfile, apiVersion string) Capabilities {
	result := cloneCapabilities(profile.capabilities)
	result.APIVersion = apiVersion
	if !containsVersion(profile.apiVersions, apiVersion) {
		disableProtocolCapabilities(&result)
		return result
	}
	protocol, known := sqlGatewayProtocolCapabilities[apiVersion]
	if !known {
		disableProtocolCapabilities(&result)
		return result
	}
	result.ConfigureSession = result.ConfigureSession && protocol.ConfigureSession
	result.CompleteStatement = result.CompleteStatement && protocol.CompleteStatement
	result.RowFormat = result.RowFormat && protocol.RowFormat
	result.MaterializedTable = result.MaterializedTable && protocol.MaterializedTable
	result.DeployScript = result.DeployScript && protocol.DeployScript
	result.WireExecutionTimeout = result.WireExecutionTimeout && protocol.WireExecutionTimeout
	return result
}

// disableProtocolCapabilities는 release metadata는 보존하면서 확인되지 않은 protocol 기능을
// 모두 보수적으로 비활성화한다.
func disableProtocolCapabilities(capabilities *Capabilities) {
	capabilities.ConfigureSession = false
	capabilities.CompleteStatement = false
	capabilities.RowFormat = false
	capabilities.MaterializedTable = false
	capabilities.DeployScript = false
	capabilities.WireExecutionTimeout = false
}

// containsVersion은 정규화된 API 버전 목록의 membership을 확인한다.
func containsVersion(versions []string, target string) bool {
	for _, version := range versions {
		if version == target {
			return true
		}
	}
	return false
}

// cloneCompatibilityInfo는 공개 slice가 client 내부 상태를 변경하지 못하게 한다.
func cloneCompatibilityInfo(info CompatibilityInfo) CompatibilityInfo {
	info.Capabilities = cloneCapabilities(info.Capabilities)
	return info
}

// cloneCapabilities는 capability가 보유한 모든 slice를 복사한다.
func cloneCapabilities(capabilities Capabilities) Capabilities {
	capabilities.SupportedAPIVersions = cloneStrings(capabilities.SupportedAPIVersions)
	return capabilities
}

// cloneStrings는 nil 의미를 유지하면서 문자열 slice memory를 분리한다.
func cloneStrings(values []string) []string {
	if values == nil {
		return nil
	}
	result := make([]string, len(values))
	copy(result, values)
	return result
}

// newCompatibilityError는 typed 분류와 선택 문맥을 함께 보존한다.
func newCompatibilityError(kind error, operation, flinkVersion string, line ReleaseLine, apiVersion string, cause error) error {
	return &CompatibilityError{Kind: kind, Operation: operation, FlinkVersion: sanitizeServerMessage(flinkVersion), ReleaseLine: line, APIVersion: apiVersion, Cause: cause}
}
