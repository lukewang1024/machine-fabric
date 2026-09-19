param(
  [Parameter(Mandatory = $true)][string]$Version,
  [string]$NodeId = $env:COMPUTERNAME,
  [string[]]$AllowRoot = @("C:\Users", "C:\ProgramData\machine-fabric")
)

$ErrorActionPreference = "Stop"
$Version = $Version.TrimStart("v")
if ($Version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+$') { throw "version must be exact semver: $Version" }
$baseUrl = $env:MACHINE_FABRIC_RELEASE_BASE_URL
if (-not $baseUrl -or $baseUrl -notmatch '^https://') {
  throw "set MACHINE_FABRIC_RELEASE_BASE_URL to the internal release CDN root"
}

$target = "x86_64-pc-windows-msvc"
$archive = "machine-fabric-$Version-$target.zip"
$base = $baseUrl.TrimEnd('/') + "/releases/v$Version"
$headers = @{ "X-Tos-Access" = "internal" }
$temporary = Join-Path ([System.IO.Path]::GetTempPath()) ("machine-fabric-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $temporary | Out-Null
try {
  Invoke-WebRequest "$base/$archive" -Headers $headers -OutFile (Join-Path $temporary $archive)
  Invoke-WebRequest "$base/SHA256SUMS" -Headers $headers -OutFile (Join-Path $temporary "SHA256SUMS")
  $sumLine = Get-Content (Join-Path $temporary "SHA256SUMS") | Where-Object { $_ -match ("  " + [regex]::Escape($archive) + '$') }
  if (-not $sumLine) { throw "checksum missing for $archive" }
  $expected = ($sumLine -split '\s+')[0].ToLowerInvariant()
  $actual = (Get-FileHash -Algorithm SHA256 (Join-Path $temporary $archive)).Hash.ToLowerInvariant()
  if ($actual -ne $expected) { throw "checksum mismatch" }
  Expand-Archive -Path (Join-Path $temporary $archive) -DestinationPath $temporary
  $root = Join-Path $temporary "machine-fabric-$Version-$target"
  & (Join-Path $root "scripts\install-windows.ps1") -Binary (Join-Path $root "bin\machine-fabric.exe") -NodeId $NodeId -AllowRoot $AllowRoot
} finally {
  Remove-Item -Recurse -Force $temporary -ErrorAction SilentlyContinue
}
