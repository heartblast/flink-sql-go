[CmdletBinding()]
param(
    [string]$Version,
    [string]$OutputDirectory = "dist",
    [string]$VulnerabilityDatabase = "https://vuln.go.dev",
    [switch]$Release,
    [switch]$AllowDirty,
    [switch]$AllowVulnerabilities,
    [switch]$Race
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepositoryRoot = [IO.Path]::GetFullPath($PSScriptRoot)
if ([IO.Path]::IsPathRooted($OutputDirectory)) {
    $DistDirectory = [IO.Path]::GetFullPath($OutputDirectory)
} else {
    $DistDirectory = [IO.Path]::GetFullPath((Join-Path $RepositoryRoot $OutputDirectory))
}

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments
    )

    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $Command $($Arguments -join ' ')"
    }
}

function Assert-SemVer {
    param([Parameter(Mandatory = $true)][string]$Value)

    $pattern = '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$'
    if ($Value -notmatch $pattern) {
        throw "Version '$Value' is not valid SemVer 2.0.0."
    }
}

function Read-CompatibilityManifest {
    param([Parameter(Mandatory = $true)][string]$Path)

    try {
        $manifest = Get-Content -Raw -LiteralPath $Path -Encoding UTF8 | ConvertFrom-Json
    } catch {
        throw "Compatibility manifest '$Path' must use the repository's JSON-compatible YAML format: $($_.Exception.Message)"
    }

    foreach ($propertyName in @("schemaVersion", "defaultReleaseLine", "defaultApiVersion", "protocolCapabilities", "supportedReleases")) {
        if ($null -eq $manifest.PSObject.Properties[$propertyName]) {
            throw "Compatibility manifest is missing '$propertyName'."
        }
    }
    if ($manifest.schemaVersion -ne 2) {
        throw "Unsupported compatibility manifest schema version '$($manifest.schemaVersion)'."
    }

    $releases = @($manifest.supportedReleases)
    if ($releases.Count -eq 0) {
        throw "Compatibility manifest must declare at least one supported release line."
    }

    $allowedStatuses = @("planned", "experimental", "supported", "maintenance", "unsupported")
    $requiredReleaseProperties = @(
        "releaseLine",
        "status",
        "testedVersions",
        "restApiVersions",
        "stableApiVersion",
        "capabilities"
    )
    $requiredCapabilities = @(
        "configureSession",
        "completeStatement",
        "rowFormat",
        "materializedTable",
        "deployScript",
        "wireExecutionTimeout"
    )
    $protocols = @($manifest.protocolCapabilities.PSObject.Properties)
    if ($protocols.Count -eq 0) {
        throw "Compatibility manifest must declare protocolCapabilities."
    }
    foreach ($protocol in $protocols) {
        if ($protocol.Name -notmatch '^v[1-9][0-9]*$') {
            throw "Compatibility protocol capability '$($protocol.Name)' has an invalid API version."
        }
        foreach ($capabilityName in $requiredCapabilities) {
            $capability = $protocol.Value.PSObject.Properties[$capabilityName]
            if ($null -eq $capability -or $capability.Value -isnot [bool]) {
                throw "Compatibility protocol '$($protocol.Name)' capability '$capabilityName' must be boolean."
            }
        }
    }
    $seenReleaseLines = @{}
    $defaultProfile = $null

    foreach ($release in $releases) {
        foreach ($propertyName in $requiredReleaseProperties) {
            if ($null -eq $release.PSObject.Properties[$propertyName]) {
                throw "Compatibility release entry is missing '$propertyName'."
            }
        }

        $releaseLine = "$($release.releaseLine)"
        if ($releaseLine -notmatch '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$') {
            throw "Compatibility release line '$releaseLine' must use major.minor format."
        }
        if ($seenReleaseLines.ContainsKey($releaseLine)) {
            throw "Compatibility release line '$releaseLine' is duplicated."
        }
        $seenReleaseLines[$releaseLine] = $true

        if ($allowedStatuses -notcontains "$($release.status)") {
            throw "Compatibility release line '$releaseLine' has invalid status '$($release.status)'."
        }

        $apiVersions = @($release.restApiVersions)
        if ($apiVersions.Count -eq 0) {
            throw "Compatibility release line '$releaseLine' must declare REST API versions."
        }
        foreach ($apiVersion in $apiVersions) {
            if ("$apiVersion" -notmatch '^v[1-9][0-9]*$') {
                throw "Compatibility release line '$releaseLine' has invalid REST API version '$apiVersion'."
            }
            if ($null -eq $manifest.protocolCapabilities.PSObject.Properties["$apiVersion"]) {
                throw "Compatibility release line '$releaseLine' references API version '$apiVersion' without a protocol capability descriptor."
            }
        }
        if ($apiVersions -notcontains "$($release.stableApiVersion)") {
            throw "Compatibility release line '$releaseLine' stable API version is not in restApiVersions."
        }

        foreach ($testedVersion in @($release.testedVersions)) {
            Assert-SemVer "$testedVersion"
        }
        foreach ($capabilityName in $requiredCapabilities) {
            $capability = $release.capabilities.PSObject.Properties[$capabilityName]
            if ($null -eq $capability -or $capability.Value -isnot [bool]) {
                throw "Compatibility release line '$releaseLine' capability '$capabilityName' must be boolean."
            }
        }

        if ($releaseLine -eq "$($manifest.defaultReleaseLine)") {
            $defaultProfile = $release
        }
    }

    if ($null -eq $defaultProfile) {
        throw "Default release line '$($manifest.defaultReleaseLine)' is not declared."
    }
    if (@($defaultProfile.restApiVersions) -notcontains "$($manifest.defaultApiVersion)") {
        throw "Default API version '$($manifest.defaultApiVersion)' is not supported by the default release line."
    }

    return $manifest
}

function Invoke-VulnerabilityScan {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$ReportPath,
        [Parameter(Mandatory = $true)][string]$Description
    )

    Write-Host "==> $Description"
    $goExecutable = (Get-Command go -ErrorAction Stop).Source
    $stdoutPath = "$ReportPath.stdout.tmp"
    $stderrPath = "$ReportPath.stderr.tmp"
    foreach ($temporaryPath in @($stdoutPath, $stderrPath)) {
        if (Test-Path -LiteralPath $temporaryPath) {
            Remove-Item -LiteralPath $temporaryPath -Force
        }
    }

    try {
        $process = Start-Process `
            -FilePath $goExecutable `
            -ArgumentList (@("tool", "govulncheck") + $Arguments) `
            -NoNewWindow `
            -Wait `
            -PassThru `
            -RedirectStandardOutput $stdoutPath `
            -RedirectStandardError $stderrPath

        $output = @()
        if (Test-Path -LiteralPath $stdoutPath) {
            $output += @(Get-Content -LiteralPath $stdoutPath)
        }
        if (Test-Path -LiteralPath $stderrPath) {
            $output += @(Get-Content -LiteralPath $stderrPath)
        }
        $output | Set-Content -LiteralPath $ReportPath -Encoding UTF8
        $output | ForEach-Object { Write-Host $_ }
        return $process.ExitCode
    } finally {
        foreach ($temporaryPath in @($stdoutPath, $stderrPath)) {
            if (Test-Path -LiteralPath $temporaryPath) {
                Remove-Item -LiteralPath $temporaryPath -Force
            }
        }
    }
}

Push-Location $RepositoryRoot
try {
    if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
        throw "Go is not available on PATH."
    }

    $RequiredGoVersion = (Select-String -LiteralPath "go.mod" -Pattern '^go\s+(.+)$').Matches[0].Groups[1].Value.Trim()
    $ActualGoVersion = (& go env GOVERSION).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to determine the Go version."
    }
    if ($ActualGoVersion -ne "go$RequiredGoVersion") {
        throw "Go toolchain mismatch: required go$RequiredGoVersion, found $ActualGoVersion."
    }

    $BaseVersion = (Get-Content -Raw -LiteralPath "VERSION").Trim()
    Assert-SemVer $BaseVersion

    $CompatibilityManifestPath = Join-Path $RepositoryRoot "compatibility.yaml"
    $CompatibilityManifest = Read-CompatibilityManifest $CompatibilityManifestPath
    $SupportedFlinkReleaseLines = @(
        $CompatibilityManifest.supportedReleases | ForEach-Object { "$($_.releaseLine)" }
    )

    $HasGit = $null -ne (Get-Command git -ErrorAction SilentlyContinue)
    $HasCommit = $false
    $Commit = "uncommitted"
    $ExactTags = @()
    $ExactTag = ""
    $Dirty = $true

    if ($HasGit) {
        $insideWorkTree = (& git rev-parse --is-inside-work-tree 2>$null)
        if ($LASTEXITCODE -eq 0 -and "$insideWorkTree".Trim() -eq "true") {
            $commitOutput = (& git rev-parse --verify --quiet HEAD)
            $HasCommit = $LASTEXITCODE -eq 0
            if ($HasCommit) {
                $Commit = "$commitOutput".Trim()
                $ExactTags = @(& git tag --points-at HEAD --list "v[0-9]*")
                if ($LASTEXITCODE -ne 0) {
                    throw "Unable to inspect Git tags."
                }
                if ($ExactTags.Count -eq 1) {
                    $ExactTag = "$($ExactTags[0])".Trim()
                }
            }
            $gitStatus = @(& git status --porcelain --untracked-files=normal)
            if ($LASTEXITCODE -ne 0) {
                throw "Unable to inspect Git worktree status."
            }
            $Dirty = $gitStatus.Count -gt 0
        }
    }

    $VersionSource = "VERSION"
    if (-not [string]::IsNullOrWhiteSpace($Version)) {
        $ResolvedVersion = $Version.Trim().TrimStart('v')
        $VersionSource = "parameter"
    } elseif (-not [string]::IsNullOrWhiteSpace($env:BUILD_VERSION)) {
        $ResolvedVersion = $env:BUILD_VERSION.Trim().TrimStart('v')
        $VersionSource = "BUILD_VERSION"
    } elseif (-not [string]::IsNullOrWhiteSpace($ExactTag)) {
        $ResolvedVersion = $ExactTag.TrimStart('v')
        $VersionSource = "git-tag"
    } else {
        $ResolvedVersion = $BaseVersion
    }
    Assert-SemVer $ResolvedVersion

    if ($Release) {
        if (-not $HasGit -or -not $HasCommit) {
            throw "Release builds require a Git worktree with a committed HEAD."
        }
        if ($AllowDirty) {
            throw "Release builds cannot use -AllowDirty."
        }
        if ($Dirty) {
            throw "Release builds require a clean worktree."
        }
        $ExpectedReleaseTag = "v$BaseVersion"
        if ($ExactTags.Count -ne 1 -or "$($ExactTags[0])".Trim() -ne $ExpectedReleaseTag) {
            throw "Release builds require HEAD to have exactly the tag '$ExpectedReleaseTag'."
        }
        if ($ResolvedVersion -ne $BaseVersion) {
            throw "Release version '$ResolvedVersion' must match VERSION '$BaseVersion'."
        }
        if ($AllowVulnerabilities) {
            throw "Release builds cannot use -AllowVulnerabilities."
        }
    }

    New-Item -ItemType Directory -Path $DistDirectory -Force | Out-Null

    $ArtifactVersion = $ResolvedVersion
    $ArtifactBaseName = "flink-sql-go-$ArtifactVersion"
    $ReachableReport = Join-Path $DistDirectory "$ArtifactBaseName.govulncheck.txt"
    $ModuleReport = Join-Path $DistDirectory "$ArtifactBaseName.govulncheck-modules.txt"
    $CoverageReport = Join-Path $DistDirectory "$ArtifactBaseName.coverage.out"
    $ModuleList = Join-Path $DistDirectory "$ArtifactBaseName.modules.txt"
    $BuildInfoPath = Join-Path $DistDirectory "$ArtifactBaseName.build-info.json"
    $CompatibilityInfoPath = Join-Path $DistDirectory "$ArtifactBaseName.compatibility.json"
    $ArchivePath = Join-Path $DistDirectory "$ArtifactBaseName-source.zip"
    $ChecksumPath = Join-Path $DistDirectory "$ArtifactBaseName.sha256"

    $BuildDate = [DateTime]::UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'")
    $DirtyText = $Dirty.ToString().ToLowerInvariant()
    $ModulePath = (& go list -m -f '{{.Path}}').Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($ModulePath)) {
        throw "Unable to resolve the Go module path."
    }
    $LinkerFlags = @(
        "-X $ModulePath/flinksqlgateway.buildVersion=$ResolvedVersion",
        "-X $ModulePath/flinksqlgateway.buildCommit=$Commit",
        "-X $ModulePath/flinksqlgateway.buildDate=$BuildDate",
        "-X $ModulePath/flinksqlgateway.buildDirty=$DirtyText"
    ) -join ' '

    Write-Host "==> Build version: $ResolvedVersion ($VersionSource)"
    Write-Host "==> Flink release lines: $($SupportedFlinkReleaseLines -join ', ')"
    Write-Host "==> Go toolchain: $ActualGoVersion"
    Write-Host "==> Verifying module checksums"
    Invoke-Checked go mod verify

    Write-Host "==> Verifying go.mod/go.sum are tidy"
    $tidyDiff = @(& go mod tidy -diff 2>&1)
    if ($LASTEXITCODE -ne 0 -or $tidyDiff.Count -gt 0) {
        $tidyDiff | ForEach-Object { Write-Host $_ }
        throw "Module files are not tidy. Run 'go mod tidy' and commit the result."
    }

    Write-Host "==> Checking formatting"
    $FormatTargets = @("flinksqlgateway", "flinkrest", "integration", "internal") |
        Where-Object { Test-Path -LiteralPath $_ }
    $unformatted = @(& gofmt -l @FormatTargets)
    if ($LASTEXITCODE -ne 0) {
        throw "gofmt failed."
    }
    if ($unformatted.Count -gt 0) {
        throw "The following files require gofmt: $($unformatted -join ', ')"
    }

    Write-Host "==> Running go vet"
    Invoke-Checked go vet ./...

    Write-Host "==> Running unit tests"
    Invoke-Checked go test -count=1 "-coverprofile=$CoverageReport" "-ldflags=$LinkerFlags" ./...

    Write-Host "==> Compiling integration-tagged tests"
    Invoke-Checked go test -tags=integration -run=^$ "-ldflags=$LinkerFlags" ./...

    if ($Race) {
        Write-Host "==> Running race detector"
        Invoke-Checked go test -race -count=1 "-ldflags=$LinkerFlags" ./...
    }

    Write-Host "==> Compiling all packages"
    Invoke-Checked go build -trimpath "-ldflags=$LinkerFlags" ./...

    $ReachableScanExitCode = Invoke-VulnerabilityScan `
        -Arguments @("-db=$VulnerabilityDatabase", "-test", "-show=version", "./...") `
        -ReportPath $ReachableReport `
        -Description "Scanning reachable source and test symbols for vulnerabilities"

    $ModuleScanExitCode = Invoke-VulnerabilityScan `
        -Arguments @("-db=$VulnerabilityDatabase", "-C=flinksqlgateway", "-scan=module") `
        -ReportPath $ModuleReport `
        -Description "Scanning the complete module dependency graph for vulnerabilities"

    $SecurityGatePassed = $ReachableScanExitCode -eq 0 -and $ModuleScanExitCode -eq 0
    if (-not $SecurityGatePassed -and -not $AllowVulnerabilities) {
        throw "Known vulnerabilities or vulnerability scan errors were found. Review $ReachableReport and $ModuleReport"
    }
    if (-not $SecurityGatePassed) {
        Write-Warning "Security findings were explicitly allowed for this non-release build. Reports are included in the artifacts."
    }

    & go list -m -f "{{.Path}}`t{{.Version}}`t{{.Sum}}" all | Set-Content -LiteralPath $ModuleList -Encoding UTF8
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to write the module dependency inventory."
    }

    $GovulncheckVersion = ((& go tool govulncheck -version) -join "`n").Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to determine govulncheck version."
    }

    $CompatibilityReleases = @(
        # PowerShell 변수명은 대소문자를 구분하지 않으므로 script 매개변수 $Release와 충돌하지 않게 한다.
        foreach ($compatibilityRelease in @($CompatibilityManifest.supportedReleases)) {
            [ordered]@{
                releaseLine = "$($compatibilityRelease.releaseLine).x"
                status = "$($compatibilityRelease.status)"
                testedVersions = @($compatibilityRelease.testedVersions)
                apiVersions = @($compatibilityRelease.restApiVersions)
                stableApiVersion = "$($compatibilityRelease.stableApiVersion)"
                capabilities = [ordered]@{
                    configureSession = [bool]$compatibilityRelease.capabilities.configureSession
                    completeStatement = [bool]$compatibilityRelease.capabilities.completeStatement
                    rowFormat = [bool]$compatibilityRelease.capabilities.rowFormat
                    materializedTable = [bool]$compatibilityRelease.capabilities.materializedTable
                    deployScript = [bool]$compatibilityRelease.capabilities.deployScript
                    wireExecutionTimeout = [bool]$compatibilityRelease.capabilities.wireExecutionTimeout
                }
            }
        }
    )
    $CompatibilityProtocolCapabilities = [ordered]@{}
    foreach ($protocol in @($CompatibilityManifest.protocolCapabilities.PSObject.Properties)) {
        $CompatibilityProtocolCapabilities[$protocol.Name] = [ordered]@{
            configureSession = [bool]$protocol.Value.configureSession
            completeStatement = [bool]$protocol.Value.completeStatement
            rowFormat = [bool]$protocol.Value.rowFormat
            materializedTable = [bool]$protocol.Value.materializedTable
            deployScript = [bool]$protocol.Value.deployScript
            wireExecutionTimeout = [bool]$protocol.Value.wireExecutionTimeout
        }
    }
    $CompatibilityInfo = [ordered]@{
        schemaVersion = [int]$CompatibilityManifest.schemaVersion
        libraryVersion = $ResolvedVersion
        defaultReleaseLine = "$($CompatibilityManifest.defaultReleaseLine).x"
        defaultApiVersion = "$($CompatibilityManifest.defaultApiVersion)"
        protocolCapabilities = $CompatibilityProtocolCapabilities
        supportedFlinkReleaseLines = $CompatibilityReleases
    }
    $CompatibilityInfo | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $CompatibilityInfoPath -Encoding UTF8

    $BuildInfo = [ordered]@{
        schemaVersion = 3
        module = $ModulePath
        libraryVersion = $ResolvedVersion
        version = $ResolvedVersion
        versionSource = $VersionSource
        sourceVersion = $BaseVersion
        defaultFlinkReleaseLine = "$($CompatibilityManifest.defaultReleaseLine)"
        defaultApiVersion = "$($CompatibilityManifest.defaultApiVersion)"
        supportedFlinkReleaseLines = @($SupportedFlinkReleaseLines)
        artifactBaseName = $ArtifactBaseName
        commit = $Commit
        dirty = $Dirty
        buildDate = $BuildDate
        goVersion = $ActualGoVersion
        govulncheck = $GovulncheckVersion
        vulnerabilityDatabase = $VulnerabilityDatabase
        securityGatePassed = $SecurityGatePassed
        reachableScanExitCode = $ReachableScanExitCode
        moduleScanExitCode = $ModuleScanExitCode
    }
    $BuildInfo | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $BuildInfoPath -Encoding UTF8

    $ArchiveItems = @(
        ".gitignore",
        ".github",
        "AGENTS.md",
        "VERSION",
        "compatibility.yaml",
        "README.md",
        "build.ps1",
        "go.mod",
        "go.sum",
        "docs",
        "flinksqlgateway",
        "flinkrest",
        "integration",
        "internal",
        "testdata",
        "prompt_management"
    ) | ForEach-Object { Join-Path $RepositoryRoot $_ } | Where-Object { Test-Path -LiteralPath $_ }
    Compress-Archive -LiteralPath $ArchiveItems -DestinationPath $ArchivePath -CompressionLevel Optimal -Force
    Compress-Archive -LiteralPath @($BuildInfoPath, $CompatibilityInfoPath) -DestinationPath $ArchivePath -Update

    $ChecksumTargets = @(
        $ArchivePath,
        $BuildInfoPath,
        $CompatibilityInfoPath,
        $ModuleList,
        $ReachableReport,
        $ModuleReport,
        $CoverageReport
    )
    $ChecksumLines = foreach ($target in $ChecksumTargets) {
        $hash = Get-FileHash -Algorithm SHA256 -LiteralPath $target
        "$($hash.Hash.ToLowerInvariant())  $([IO.Path]::GetFileName($target))"
    }
    $ChecksumLines | Set-Content -LiteralPath $ChecksumPath -Encoding ASCII

    Write-Host ""
    Write-Host "Build completed successfully."
    Write-Host "Version:         $ResolvedVersion"
    Write-Host "Flink releases:  $($SupportedFlinkReleaseLines -join ', ')"
    Write-Host "Archive:         $ArchivePath"
    Write-Host "Compatibility:   $CompatibilityInfoPath"
    Write-Host "Checksums:        $ChecksumPath"
    Write-Host "Security:         $ReachableReport"
    Write-Host "                  $ModuleReport"
} finally {
    Pop-Location
}
