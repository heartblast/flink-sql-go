package flinksqlgateway

import "context"

// Capabilities exposes feature support without requiring callers to compare
// API version strings.
type Capabilities struct {
	APIVersion        string
	ConfigureSession  bool
	CompleteStatement bool
	RowFormat         bool
	MaterializedTable bool
}

// Capabilities verifies the configured API version and returns conservative
// feature flags. Unknown future versions expose no assumed capabilities.
func (c *GatewayClient) Capabilities(ctx context.Context) (Capabilities, error) {
	if err := c.CheckAPIVersion(ctx); err != nil {
		return Capabilities{}, err
	}
	return capabilitiesForVersion(c.cfg.APIVersion), nil
}

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
