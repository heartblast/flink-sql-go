[CmdletBinding()]
param(
    [string]$Version,
    [string]$FlinkVersion,
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

    $DefaultFlinkVersion = (Get-Content -Raw -LiteralPath "FLINK_VERSION").Trim()
    Assert-SemVer $DefaultFlinkVersion
    $FlinkVersionSource = "FLINK_VERSION"
    if (-not [string]::IsNullOrWhiteSpace($FlinkVersion)) {
        $ResolvedFlinkVersion = $FlinkVersion.Trim().TrimStart('v')
        $FlinkVersionSource = "parameter"
    } elseif (-not [string]::IsNullOrWhiteSpace($env:SUPPORTED_FLINK_VERSION)) {
        $ResolvedFlinkVersion = $env:SUPPORTED_FLINK_VERSION.Trim().TrimStart('v')
        $FlinkVersionSource = "SUPPORTED_FLINK_VERSION"
    } else {
        $ResolvedFlinkVersion = $DefaultFlinkVersion
    }
    Assert-SemVer $ResolvedFlinkVersion

    $HasGit = $null -ne (Get-Command git -ErrorAction SilentlyContinue)
    $HasCommit = $false
    $Commit = "uncommitted"
    $ExactTag = ""
    $Dirty = $true

    if ($HasGit) {
        $insideWorkTree = (& git rev-parse --is-inside-work-tree 2>$null)
        if ($LASTEXITCODE -eq 0 -and "$insideWorkTree".Trim() -eq "true") {
            $commitOutput = (& git rev-parse --verify --quiet HEAD)
            $HasCommit = $LASTEXITCODE -eq 0
            if ($HasCommit) {
                $Commit = "$commitOutput".Trim()
                $tagOutput = @(& git tag --points-at HEAD --list "v[0-9]*")
                if ($LASTEXITCODE -ne 0) {
                    throw "Unable to inspect Git tags."
                }
                if ($tagOutput.Count -gt 0) {
                    $ExactTag = "$($tagOutput[0])".Trim()
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
        if ($Dirty -and -not $AllowDirty) {
            throw "Release builds require a clean worktree. Commit or stash changes, or explicitly use -AllowDirty."
        }
        if ($VersionSource -eq "VERSION") {
            throw "Release builds require an exact v* Git tag, -Version, or BUILD_VERSION."
        }
        if ($AllowVulnerabilities) {
            throw "Release builds cannot use -AllowVulnerabilities."
        }
    }

    New-Item -ItemType Directory -Path $DistDirectory -Force | Out-Null

    $ArtifactVersion = $ResolvedVersion
    $ArtifactBaseName = "flink-sql-go-$ArtifactVersion-flink-$ResolvedFlinkVersion"
    $ReachableReport = Join-Path $DistDirectory "$ArtifactBaseName.govulncheck.txt"
    $ModuleReport = Join-Path $DistDirectory "$ArtifactBaseName.govulncheck-modules.txt"
    $CoverageReport = Join-Path $DistDirectory "$ArtifactBaseName.coverage.out"
    $ModuleList = Join-Path $DistDirectory "$ArtifactBaseName.modules.txt"
    $BuildInfoPath = Join-Path $DistDirectory "$ArtifactBaseName.build-info.json"
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
        "-X $ModulePath/flinksqlgateway.buildFlinkVersion=$ResolvedFlinkVersion",
        "-X $ModulePath/flinksqlgateway.buildCommit=$Commit",
        "-X $ModulePath/flinksqlgateway.buildDate=$BuildDate",
        "-X $ModulePath/flinksqlgateway.buildDirty=$DirtyText"
    ) -join ' '

    Write-Host "==> Build version: $ResolvedVersion ($VersionSource)"
    Write-Host "==> Supported Flink: $ResolvedFlinkVersion ($FlinkVersionSource)"
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
    $unformatted = @(& gofmt -l flinksqlgateway flinkrest)
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
    $BuildInfo = [ordered]@{
        schemaVersion = 2
        module = $ModulePath
        version = $ResolvedVersion
        versionSource = $VersionSource
        sourceVersion = $BaseVersion
        supportedFlinkVersion = $ResolvedFlinkVersion
        flinkVersionSource = $FlinkVersionSource
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
    $BuildInfo | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $BuildInfoPath -Encoding UTF8

    $ArchiveItems = @(
        ".gitignore",
        "VERSION",
        "FLINK_VERSION",
        "README.md",
        "build.ps1",
        "go.mod",
        "go.sum",
        "docs",
        "flinksqlgateway",
        "flinkrest",
        "prompt_management"
    ) | ForEach-Object { Join-Path $RepositoryRoot $_ }
    Compress-Archive -LiteralPath $ArchiveItems -DestinationPath $ArchivePath -CompressionLevel Optimal -Force
    Compress-Archive -LiteralPath $BuildInfoPath -DestinationPath $ArchivePath -Update

    $ChecksumTargets = @(
        $ArchivePath,
        $BuildInfoPath,
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
    Write-Host "Supported Flink: $ResolvedFlinkVersion"
    Write-Host "Archive:         $ArchivePath"
    Write-Host "Checksums:        $ChecksumPath"
    Write-Host "Security:         $ReachableReport"
    Write-Host "                  $ModuleReport"
} finally {
    Pop-Location
}
