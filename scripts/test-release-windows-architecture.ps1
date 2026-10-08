$ErrorActionPreference = 'Stop'
$oldArchitecture = $env:PROCESSOR_ARCHITECTURE
$oldNative = $env:PROCESSOR_ARCHITEW6432
$oldBase = $env:MACHINE_FABRIC_RELEASE_BASE_URL
try {
  $env:MACHINE_FABRIC_RELEASE_BASE_URL = 'https://example.invalid/fabric'
  function Invoke-WebRequest {
    param([string]$Uri, $Headers, [string]$OutFile)
    $env:MACHINE_FABRIC_TEST_REQUESTED_URI = $Uri
    throw 'architecture-test-stop-before-network'
  }
  foreach ($case in @(
    @{ Process = 'AMD64'; Native = ''; Target = 'x86_64-pc-windows-msvc' },
    @{ Process = 'ARM64'; Native = ''; Target = 'aarch64-pc-windows-msvc' },
    @{ Process = 'AMD64'; Native = 'ARM64'; Target = 'aarch64-pc-windows-msvc' }
  )) {
    $env:PROCESSOR_ARCHITECTURE = $case.Process
    $env:PROCESSOR_ARCHITEW6432 = $case.Native
    $env:MACHINE_FABRIC_TEST_REQUESTED_URI = $null
    try {
      & (Join-Path $PSScriptRoot 'install-from-release.ps1') -Version '1.2.3'
      throw 'expected download interception'
    } catch {
      if ($_.Exception.Message -ne 'architecture-test-stop-before-network') { throw }
    }
    $expected = "https://example.invalid/fabric/releases/v1.2.3/machine-fabric-1.2.3-$($case.Target).zip"
    if ($env:MACHINE_FABRIC_TEST_REQUESTED_URI -ne $expected) { throw "wrong architecture URL: $env:MACHINE_FABRIC_TEST_REQUESTED_URI" }
  }
} finally {
  $env:PROCESSOR_ARCHITECTURE = $oldArchitecture
  $env:PROCESSOR_ARCHITEW6432 = $oldNative
  $env:MACHINE_FABRIC_RELEASE_BASE_URL = $oldBase
  Remove-Item Env:MACHINE_FABRIC_TEST_REQUESTED_URI -ErrorAction SilentlyContinue
  Remove-Item Function:Invoke-WebRequest -ErrorAction SilentlyContinue
}
