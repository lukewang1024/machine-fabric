# Return only a verified installed binary path. Never execute receipt contents.
$ErrorActionPreference = 'Stop'
$root = Join-Path $env:ProgramFiles 'machine-fabric'
$receiptPath = Join-Path $root 'installation.json'
if (-not (Test-Path -LiteralPath $receiptPath)) {
  # Compatibility with installations predating immutable upgrade receipts.
  $legacy = Join-Path $root 'machine-fabric.exe'
  if (-not (Test-Path -LiteralPath $legacy -PathType Leaf)) { throw 'Machine Fabric installation is missing' }
  Write-Output $legacy
  return
}
$receipt = Get-Content -LiteralPath $receiptPath -Raw -Encoding UTF8 | ConvertFrom-Json
$hash = [string]$receipt.installedSha256
if ($hash -cnotmatch '^[a-f0-9]{64}$') { throw 'invalid installed binary digest' }
$binary = [string]$receipt.installedBinary
$legacy = Join-Path $root 'machine-fabric.exe'
$immutable = Join-Path (Join-Path (Join-Path $root 'versions') $hash) 'machine-fabric.exe'
if ($binary -cne $legacy -and $binary -cne $immutable) { throw 'installed binary path is outside the managed installation' }
foreach ($path in @($root, $receiptPath, $binary)) {
  $item = Get-Item -LiteralPath $path -Force
  if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'installed binary resolution contains a redirect' }
}
if ($binary -ceq $immutable) {
  foreach ($path in @((Join-Path $root 'versions'), (Split-Path -Parent $immutable))) {
    if ((Get-Item -LiteralPath $path -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) {
      throw 'immutable binary resolution contains a redirect'
    }
  }
}
if ((Get-FileHash -LiteralPath $binary -Algorithm SHA256).Hash.ToLowerInvariant() -cne $hash) {
  throw 'installed binary digest mismatch'
}
Write-Output $binary
