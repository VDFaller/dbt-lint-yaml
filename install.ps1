#!/usr/bin/env pwsh
[CmdletBinding()]
param(
    [string]$Version,
    [string]$Target,
    [string]$Destination,
    [switch]$Update,
    [switch]$Help
)

$PackageName = "dbt-lint-yaml"
$RepoOwner = "VDFaller"
$RepoName = "dbt-lint-yaml"
$Repo = "$RepoOwner/$RepoName"
$DefaultDest = Join-Path $env:USERPROFILE ".local/bin"
$ScriptName = "ruamel_model_changes.py"

if ($Help) {
    @'
Usage: install.ps1 [options]

Options:
  -Version <VER>   Install a specific version (defaults to latest release)
  -Target <TRIPLE> Install for a specific target (defaults to host platform)
  -Destination <DIR> Install into the provided directory (default: $env:USERPROFILE\.local\bin)
  -Update          Overwrite an existing installation
  -Help            Show this message

Examples:
  .\install.ps1                         # install latest release for current platform
  .\install.ps1 -Version 0.2.0          # install version 0.2.0
  .\install.ps1 -Update                 # reinstall or update to the latest release
'@ | Write-Host
    exit 0
}

if (-not $Destination) {
    $Destination = $DefaultDest
}

$Destination = [System.IO.Path]::GetFullPath($Destination)

function Write-Log {
    param([string]$Message)
    Write-Host "install.ps1: $Message"
}

function Write-ErrorAndExit {
    param([string]$Message, [string]$Detail)
    if ($Detail) {
        Write-Error "install.ps1: $Message -- $Detail"
    } else {
        Write-Error "install.ps1: $Message"
    }
    exit 1
}

function Normalize-Version {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return ""
    }
    return $Value.TrimStart('v', 'V')
}

function Detect-TargetPlatform {
    param([string]$ExplicitTarget)

    $KnownTargets = @{
        'windows-x86_64' = 'windows-x86_64'
        'windows-x86_64-msvc' = 'windows-x86_64'
        'linux-x86_64-musl' = 'linux-x86_64-musl'
        'macos-x86_64' = 'macos-x86_64'
    }

    if ($ExplicitTarget) {
        if ($KnownTargets.ContainsKey($ExplicitTarget)) {
            return $KnownTargets[$ExplicitTarget]
        }
        Write-ErrorAndExit "unknown target" $ExplicitTarget
    }

    if (-not [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Windows)) {
        Write-ErrorAndExit "unsupported operating system" "install.ps1 is intended for Windows platforms"
    }

    switch ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture) {
        "X64" { return 'windows-x86_64' }
        "Arm64" {
            Write-ErrorAndExit "unsupported Windows architecture" "arm64 support is not yet available"
        }
        default {
            Write-ErrorAndExit "unsupported Windows architecture" ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString())
        }
    }
}

function Fetch-ReleaseMetadata {
    param([string]$RequestedVersion)

    $ApiBase = "https://api.github.com/repos/$Repo/releases"
    if ($RequestedVersion) {
        $Normalized = Normalize-Version $RequestedVersion
        $Tag = "v$Normalized"
        $Url = "$ApiBase/tags/$Tag"
        Write-Log "Fetching release metadata for $Tag"
    } else {
        $Url = "$ApiBase/latest"
        Write-Log "Fetching latest release metadata"
    }

    try {
        return Invoke-RestMethod -Uri $Url -Headers @{ Accept = "application/vnd.github+json" }
    } catch {
        Write-ErrorAndExit "failed to retrieve release metadata" $Url
    }
}

function Select-Asset {
    param($Release, [string]$Version, [string]$TargetPlatform)

    foreach ($Extension in @("zip", "tar.gz")) {
        $Candidate = "${PackageName}-${Version}-${TargetPlatform}.${Extension}"
        $Asset = $Release.assets | Where-Object { $_.name -eq $Candidate } | Select-Object -First 1
        if ($Asset) {
            return [PSCustomObject]@{
                Name = $Candidate
                Url = $Asset.browser_download_url
                Extension = $Extension
            }
        }
    }

    Write-ErrorAndExit "release does not contain asset" "${PackageName}-${Version}-${TargetPlatform}"
}

function Ensure-Destination {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
    }
}

function Check-CurrentVersion {
    param([string]$BinaryPath)

    if (-not (Test-Path -LiteralPath $BinaryPath)) {
        return ""
    }

    try {
        $Output = & $BinaryPath --version 2>$null
        if ($Output) {
            $Parts = $Output -split '\s+'
            if ($Parts.Length -ge 2) {
                return $Parts[1]
            }
        }
    } catch {
        return ""
    }

    return ""
}

$TargetPlatform = Detect-TargetPlatform $Target
$Release = Fetch-ReleaseMetadata $Version

if (-not $Release.tag_name) {
    Write-ErrorAndExit "release metadata missing tag name" ""
}

$TargetVersion = (Normalize-Version $Release.tag_name)
Write-Log "Selected version $TargetVersion"

$AssetInfo = Select-Asset -Release $Release -Version $TargetVersion -TargetPlatform $TargetPlatform
$BinaryFilename = if ($TargetPlatform -like "*windows*") { "$PackageName.exe" } else { $PackageName }

Ensure-Destination $Destination
$DestinationBinaryPath = Join-Path $Destination $BinaryFilename

$CurrentVersion = Check-CurrentVersion $DestinationBinaryPath
if ($CurrentVersion) {
    if ($CurrentVersion -eq $TargetVersion) {
        Write-Log "$PackageName $TargetVersion is already installed at $Destination"
        exit 0
    }
    if (-not $Update) {
        Write-ErrorAndExit "$PackageName $CurrentVersion already exists at $Destination" "use -Update to overwrite"
    }
    Write-Log "Updating $PackageName from $CurrentVersion to $TargetVersion"
} else {
    Write-Log "Installing $PackageName $TargetVersion"
}

$TempDir = Join-Path ([System.IO.Path]::GetTempPath()) ([System.Guid]::NewGuid().ToString())
New-Item -ItemType Directory -Path $TempDir -Force | Out-Null

try {
    $ArchivePath = Join-Path $TempDir $AssetInfo.Name
    Write-Log "Downloading $($AssetInfo.Url)"
    try {
        Invoke-WebRequest -Uri $AssetInfo.Url -OutFile $ArchivePath -UseBasicParsing
    } catch {
        Write-ErrorAndExit "failed to download asset" $AssetInfo.Url
    }

    switch ($AssetInfo.Extension) {
        "zip" {
            Expand-Archive -LiteralPath $ArchivePath -DestinationPath $TempDir -Force
        }
        "tar.gz" {
            Write-ErrorAndExit "unsupported archive format" $AssetInfo.Extension
        }
        default {
            Write-ErrorAndExit "unsupported archive format" $AssetInfo.Extension
        }
    }

    $ExtractedBinary = Get-ChildItem -Path $TempDir -Recurse -Filter $BinaryFilename | Select-Object -First 1
    if (-not $ExtractedBinary) {
        Write-ErrorAndExit "extracted archive does not contain $BinaryFilename" ""
    }

    Copy-Item -LiteralPath $ExtractedBinary.FullName -Destination $DestinationBinaryPath -Force

    $ScriptPath = Get-ChildItem -Path $TempDir -Recurse -Filter $ScriptName | Select-Object -First 1
    if (-not $ScriptPath) {
        Write-ErrorAndExit "extracted archive does not contain $ScriptName" ""
    }

    $ScriptsDir = Join-Path $Destination "scripts"
    Ensure-Destination $ScriptsDir
    Copy-Item -LiteralPath $ScriptPath.FullName -Destination (Join-Path $ScriptsDir $ScriptName) -Force
}
finally {
    if (Test-Path -LiteralPath $TempDir) {
        Remove-Item -LiteralPath $TempDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

$PathEntries = ($env:PATH -split ';') | Where-Object { $_ }
if (-not ($PathEntries -contains $Destination)) {
    Write-Host ""
    Write-Host "NOTE: $Destination is not on your PATH."
    Write-Host "Add it with:"
    Write-Host ('  setx PATH "`$env:PATH;{0}"' -f $Destination)
}

Write-Log "$PackageName $TargetVersion installed to $Destination"
