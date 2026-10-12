param(
  [Parameter(Mandatory = $true)][string]$Version,
  [string]$NodeId = $env:COMPUTERNAME,
  [string]$AllowRootFile,
  [string[]]$AllowRoot = @("C:\Users", "C:\ProgramData\machine-fabric"),
  [string]$PolicyUserFile,
  [string]$PolicyUser,
  [string]$PolicyHome = $env:USERPROFILE,
  [string[]]$ManagedPathMapping = @(),
  [string]$ArchivePath,
  [string]$ExpectedSha256,
  [switch]$SideBySide
)

$ErrorActionPreference = "Stop"
if ($AllowRootFile) {
  $AllowRoot = @(Get-Content -LiteralPath $AllowRootFile -Encoding UTF8)
  if ($AllowRoot.Count -eq 0) { throw "allow-root file must not be empty" }
  foreach ($root in $AllowRoot) {
    if ($root -notmatch '^(?:[A-Za-z]:[\\/]|\\\\[^\\]+\\[^\\]+(?:[\\/]|$))') {
      throw "allow-root must be absolute: $root"
    }
  }
}
if ($PolicyUserFile) {
  $PolicyUser = (Get-Content -Raw -LiteralPath $PolicyUserFile -Encoding UTF8).Trim()
  if (-not $PolicyUser -or $PolicyUser -match '[\r\n]') {
    throw "desktop policy user file must contain one account name"
  }
}

$Version = $Version.TrimStart("v")
if ($Version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+$') { throw "version must be exact semver: $Version" }
$baseUrl = $env:MACHINE_FABRIC_RELEASE_BASE_URL
if ([bool]$ArchivePath -ne [bool]$ExpectedSha256) {
  throw 'ArchivePath and ExpectedSha256 must be supplied together'
}
if ($ArchivePath -and $ExpectedSha256 -notmatch '^[a-fA-F0-9]{64}$') {
  throw 'ExpectedSha256 must be an exact SHA-256'
}
if (-not $ArchivePath -and (-not $baseUrl -or $baseUrl -notmatch '^https://')) {
  throw "set MACHINE_FABRIC_RELEASE_BASE_URL to the internal release CDN root"
}

$nativeArchitecture = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE }
$target = switch ($nativeArchitecture.ToUpperInvariant()) {
  'AMD64' { 'x86_64-pc-windows-msvc' }
  'ARM64' { 'aarch64-pc-windows-msvc' }
  default { throw "unsupported Windows OS architecture: $nativeArchitecture" }
}
$archive = "machine-fabric-$Version-$target.zip"
$base = if ($baseUrl) { $baseUrl.TrimEnd('/') + "/releases/v$Version" } else { '' }
$headers = @{ "X-Tos-Access" = "internal" }
$temporary = Join-Path ([System.IO.Path]::GetTempPath()) ("machine-fabric-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $temporary | Out-Null
try {
  if ($ArchivePath) {
    if ([IO.Path]::GetFileName($ArchivePath) -ne $archive) { throw 'staged archive does not match native architecture and exact version' }
    $source = Get-Item -LiteralPath $ArchivePath -ErrorAction Stop
    if ($source.PSIsContainer -or ($source.Attributes -band [IO.FileAttributes]::ReparsePoint)) { throw 'staged archive must be a regular file' }
    Copy-Item -LiteralPath $source.FullName -Destination (Join-Path $temporary $archive)
    $expected = $ExpectedSha256.ToLowerInvariant()
  } else {
    Invoke-WebRequest "$base/$archive" -Headers $headers -OutFile (Join-Path $temporary $archive)
    Invoke-WebRequest "$base/SHA256SUMS" -Headers $headers -OutFile (Join-Path $temporary "SHA256SUMS")
    $sumLines = @(Get-Content (Join-Path $temporary "SHA256SUMS") | Where-Object { $_ -match ('^[a-fA-F0-9]{64} [ *]' + [regex]::Escape($archive) + '$') })
    if ($sumLines.Count -ne 1) { throw "expected one checksum for $archive" }
    $expected = ($sumLines[0] -split '\s+')[0].ToLowerInvariant()
  }
  $actual = (Get-FileHash -Algorithm SHA256 (Join-Path $temporary $archive)).Hash.ToLowerInvariant()
  if ($actual -ne $expected) { throw "checksum mismatch" }
  Expand-Archive -Path (Join-Path $temporary $archive) -DestinationPath $temporary
  $root = Join-Path $temporary "machine-fabric-$Version-$target"
  $mappingOptions = @{}
  if ($SideBySide) { $mappingOptions.SideBySide = $true }
  if ($PSBoundParameters.ContainsKey('ManagedPathMapping')) { $mappingOptions.ManagedPathMapping = $ManagedPathMapping }
  & (Join-Path $root "scripts\install-windows.ps1") -Binary (Join-Path $root "bin\machine-fabric.exe") -NodeId $NodeId -AllowRoot $AllowRoot -PolicyUser $PolicyUser -PolicyHome $PolicyHome @mappingOptions
} finally {
  Remove-Item -Recurse -Force $temporary -ErrorAction SilentlyContinue
}
