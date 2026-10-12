$ErrorActionPreference = 'Stop'
$temporary = Join-Path ([IO.Path]::GetTempPath()) ('machine-fabric-staged-test-' + [guid]::NewGuid())
New-Item -ItemType Directory -Path $temporary | Out-Null
$oldBase = $env:MACHINE_FABRIC_RELEASE_BASE_URL
$oldResult = $env:MACHINE_FABRIC_TEST_STAGE_RESULT
try {
  $env:MACHINE_FABRIC_RELEASE_BASE_URL = $null
  $env:MACHINE_FABRIC_TEST_STAGE_RESULT = Join-Path $temporary 'result.json'
  function Invoke-WebRequest { throw 'staged release must never access the network' }
  $native = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE }
  $target = switch ($native) { 'AMD64' { 'x86_64-pc-windows-msvc' }; 'ARM64' { 'aarch64-pc-windows-msvc' }; default { throw 'unsupported test OS' } }
  $name = "machine-fabric-1.2.3-$target"
  $root = Join-Path $temporary $name
  New-Item -ItemType Directory -Path (Join-Path $root 'scripts') | Out-Null
  New-Item -ItemType Directory -Path (Join-Path $root 'bin') | Out-Null
  [IO.File]::WriteAllText((Join-Path $root 'bin/machine-fabric.exe'), 'fixture binary, never executed')
  @'
param($Binary, $NodeId, $AllowRoot, $PolicyUser, $PolicyHome, $ManagedPathMapping, [switch]$SideBySide)
@{NodeId=$NodeId; PolicyUser=$PolicyUser; SideBySide=[bool]$SideBySide; BinaryExists=(Test-Path -LiteralPath $Binary)} | ConvertTo-Json | Set-Content -LiteralPath $env:MACHINE_FABRIC_TEST_STAGE_RESULT
'@ | Set-Content -LiteralPath (Join-Path $root 'scripts/install-windows.ps1')
  $archive = Join-Path $temporary ($name + '.zip')
  Compress-Archive -Path $root -DestinationPath $archive
  $sum = (Get-FileHash -Algorithm SHA256 -LiteralPath $archive).Hash
  $installer = Join-Path $PSScriptRoot 'install-from-release.ps1'
  & $installer -Version '1.2.3' -ArchivePath $archive -ExpectedSha256 $sum -NodeId 'fixture-node' -PolicyUser 'fixture-user' -SideBySide
  $result = Get-Content -Raw -LiteralPath $env:MACHINE_FABRIC_TEST_STAGE_RESULT | ConvertFrom-Json
  if ($result.NodeId -ne 'fixture-node' -or $result.PolicyUser -ne 'fixture-user' -or -not $result.SideBySide -or -not $result.BinaryExists) { throw 'staged installer option forwarding failed' }
  Remove-Item -LiteralPath $env:MACHINE_FABRIC_TEST_STAGE_RESULT
  try {
    & $installer -Version '1.2.3' -ArchivePath $archive -ExpectedSha256 ('0' * 64)
    throw 'expected checksum rejection'
  } catch { if ($_.Exception.Message -ne 'checksum mismatch') { throw } }
  if (Test-Path -LiteralPath $env:MACHINE_FABRIC_TEST_STAGE_RESULT) { throw 'corrupt staged archive executed installer' }
  try {
    & $installer -Version '1.2.4' -ArchivePath $archive -ExpectedSha256 $sum
    throw 'expected exact version rejection'
  } catch { if ($_.Exception.Message -ne 'staged archive does not match native architecture and exact version') { throw } }
  Write-Output 'native staged release and checksum rejection passed without network access'
} finally {
  $env:MACHINE_FABRIC_RELEASE_BASE_URL = $oldBase
  $env:MACHINE_FABRIC_TEST_STAGE_RESULT = $oldResult
  Remove-Item Function:Invoke-WebRequest -ErrorAction SilentlyContinue
  Remove-Item -LiteralPath $temporary -Recurse -Force
}
