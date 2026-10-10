$ErrorActionPreference = 'Stop'
$prior = $env:ProgramFiles
$fixture = Join-Path $env:TEMP ('fabric-resolution-' + [guid]::NewGuid().ToString('N'))
try {
  $env:ProgramFiles = $fixture
  $root = Join-Path $fixture 'machine-fabric'
  New-Item -ItemType Directory -Path $root -Force | Out-Null
  $legacy = Join-Path $root 'machine-fabric.exe'
  [IO.File]::WriteAllText($legacy, 'legacy fixture')
  $resolver = Join-Path $PSScriptRoot 'resolve-windows-binary.ps1'
  if ((& $resolver) -cne $legacy) { throw 'legacy compatibility failed' }
  $hash = (Get-FileHash -LiteralPath $legacy -Algorithm SHA256).Hash.ToLowerInvariant()
  $directory = Join-Path (Join-Path $root 'versions') $hash
  New-Item -ItemType Directory -Path $directory -Force | Out-Null
  $immutable = Join-Path $directory 'machine-fabric.exe'
  Copy-Item -LiteralPath $legacy -Destination $immutable
  $receipt = Join-Path $root 'installation.json'
  @{installedBinary=$immutable; installedSha256=$hash} | ConvertTo-Json -Compress | Set-Content -LiteralPath $receipt -Encoding UTF8
  if ((& $resolver) -cne $immutable) { throw 'immutable resolution failed' }
  # SSH starts Windows PowerShell with the ordinary machine execution policy.
  # Verify the process-scoped bootstrap invocation without changing that policy.
  $beforePolicy = Get-ExecutionPolicy -Scope CurrentUser
  $quotedResolver = $resolver.Replace("'", "''")
  $quotedFixture = $fixture.Replace("'", "''")
  $resolveCommand = "`$ErrorActionPreference='Stop'; `$env:ProgramFiles='$quotedFixture'; & '$quotedResolver'"
  $resolveEncoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($resolveCommand))
  $savedErrorPreference = $ErrorActionPreference
  try {
    $ErrorActionPreference = 'Continue'
    $blocked = & powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Restricted -EncodedCommand $resolveEncoded 2>&1
    $blockedCode = $LASTEXITCODE
  } finally { $ErrorActionPreference = $savedErrorPreference }
  if ($blockedCode -eq 0) { throw 'restricted policy did not reject the script fixture' }
  $outerCommand = "& powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -EncodedCommand '$resolveEncoded'; exit `$LASTEXITCODE"
  $outerEncoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($outerCommand))
  $resolved = & powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Restricted -EncodedCommand $outerEncoded
  if ($LASTEXITCODE -ne 0 -or ($resolved -join '').Trim() -cne $immutable) { throw 'bootstrap resolution under restricted policy failed' }
  if ((Get-ExecutionPolicy -Scope CurrentUser) -cne $beforePolicy) { throw 'persistent execution policy changed' }
  foreach ($path in @((Join-Path $fixture 'outside.exe'), (Join-Path $root 'versions\..\machine-fabric.exe'))) {
    @{installedBinary=$path; installedSha256=$hash} | ConvertTo-Json -Compress | Set-Content -LiteralPath $receipt -Encoding UTF8
    $rejected = $false
    try { & $resolver | Out-Null } catch { $rejected = $true }
    if (-not $rejected) { throw 'unmanaged binary path accepted' }
  }
  @{installedBinary=$immutable; installedSha256=$hash} | ConvertTo-Json -Compress | Set-Content -LiteralPath $receipt -Encoding UTF8
  [IO.File]::WriteAllText($immutable, 'corrupted fixture')
  $rejected = $false
  try { & $resolver | Out-Null } catch { $rejected = $true }
  if (-not $rejected) { throw 'corrupted immutable binary accepted' }
  Write-Output 'Windows installed binary resolution verified'
} finally {
  $env:ProgramFiles = $prior
  if (Test-Path -LiteralPath $fixture) { Remove-Item -LiteralPath $fixture -Recurse -Force }
}
