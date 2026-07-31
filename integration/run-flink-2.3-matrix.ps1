[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$GatewayURL,
    [string]$FlinkVersion = "2.3.0"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$saved = @{
    FLINK_SQL_GATEWAY_URL = $env:FLINK_SQL_GATEWAY_URL
    FLINK_TEST_VERSION = $env:FLINK_TEST_VERSION
    FLINK_TEST_RELEASE_LINE = $env:FLINK_TEST_RELEASE_LINE
    FLINK_TEST_API_VERSION = $env:FLINK_TEST_API_VERSION
}

try {
    $env:FLINK_SQL_GATEWAY_URL = $GatewayURL
    $env:FLINK_TEST_VERSION = $FlinkVersion
    $env:FLINK_TEST_RELEASE_LINE = "2.3"
    foreach ($apiVersion in @("v3", "v4")) {
        $env:FLINK_TEST_API_VERSION = $apiVersion
        Write-Host "==> Flink $FlinkVersion SQL Gateway $apiVersion integration"
        & go test -tags=integration -count=1 ./flinksqlgateway
        if ($LASTEXITCODE -ne 0) {
            throw "Flink $FlinkVersion $apiVersion integration failed with exit code $LASTEXITCODE."
        }
    }
} finally {
    foreach ($name in $saved.Keys) {
        [Environment]::SetEnvironmentVariable($name, $saved[$name], "Process")
    }
}
