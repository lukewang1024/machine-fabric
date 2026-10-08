param([Parameter(Mandatory = $true)][string]$Binary)
$ErrorActionPreference = 'Stop'
# A PowerShell 7 parent can leave incompatible modules on PSModulePath.
# Load the native 5.1 security module before changing fixture environment.
Import-Module (Join-Path $PSHOME 'Modules\Microsoft.PowerShell.Security\Microsoft.PowerShell.Security.psd1') -ErrorAction Stop
$Binary = (Resolve-Path -LiteralPath $Binary).Path
$installer = Join-Path $PSScriptRoot 'install-windows.ps1'
$tokens = $null
$parseErrors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($installer, [ref]$tokens, [ref]$parseErrors)
if ($parseErrors.Count) { throw 'installer syntax errors' }
$quoteFunction = $ast.Find({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Quote-Arg' }, $true)
if (-not $quoteFunction) { throw 'missing service argument quoting function' }
# Evaluate only the repository's parsed quoting function, never the installer.
Invoke-Expression $quoteFunction.Extent.Text
Add-Type @'
using System;
using System.Runtime.InteropServices;
public static class ServiceArgvTest {
  [DllImport("shell32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
  static extern IntPtr CommandLineToArgvW(string command, out int count);
  [DllImport("kernel32.dll")] static extern IntPtr LocalFree(IntPtr pointer);
  public static string[] Decode(string command) {
    int count; IntPtr pointer = CommandLineToArgvW(command, out count);
    if (pointer == IntPtr.Zero) throw new Exception("argument decode failed");
    try {
      string[] args = new string[count];
      for (int i=0; i<count; i++) args[i] = Marshal.PtrToStringUni(Marshal.ReadIntPtr(pointer, i*IntPtr.Size));
      return args;
    } finally { LocalFree(pointer); }
  }
}
'@
foreach ($value in @('', 'C:\', 'C:\path with spaces\', 'logical=physical', 'embedded"quote', 'slashes\\"quote')) {
  $decoded = [ServiceArgvTest]::Decode('program.exe ' + (Quote-Arg $value))
  if ($decoded.Length -ne 2 -or $decoded[1] -cne $value) { throw "service argument roundtrip failed: $value" }
}
foreach ($value in @("line`nbreak", "null`0byte")) {
  $rejected = $false
  try { Quote-Arg $value | Out-Null } catch { $rejected = $true }
  if (-not $rejected) { throw 'control character was accepted' }
}
$fixture = Join-Path $env:TEMP ('fabric-policy-test-' + [guid]::NewGuid().ToString('N'))
$priorProgramFiles = $env:ProgramFiles
$priorProgramData = $env:ProgramData
try {
  $homePath = Join-Path $fixture 'home'
  New-Item -ItemType Directory -Path $homePath -Force | Out-Null
  & $Binary executor validate-path-policy --policy-home $homePath
  if ($LASTEXITCODE -ne 0) { throw 'valid empty mapping policy rejected' }
  $env:ProgramFiles = Join-Path $fixture 'program-files'
  $env:ProgramData = Join-Path $fixture 'program-data'
  $rejected = $false
  try {
    & $installer -Binary $Binary -PolicyHome $homePath -ManagedPathMapping 'relative=missing'
  } catch { $rejected = $true }
  if (-not $rejected) { throw 'invalid mapping installer preflight succeeded' }
  if ((Test-Path $env:ProgramFiles) -or (Test-Path $env:ProgramData)) {
    throw 'invalid mapping caused installation writes before preflight'
  }
  $configRoot = Join-Path $env:ProgramFiles 'machine-fabric'
  New-Item -ItemType Directory -Path $configRoot -Force | Out-Null
  $config = Join-Path $configRoot 'managed-path-mappings.json'
  $mapping = (Join-Path $homePath 'Documents') + '=' + (Join-Path $fixture 'disk-data')
  @{version=1; mappings=@($mapping)} | ConvertTo-Json -Compress | Set-Content -LiteralPath $config -Encoding UTF8
  $acl = New-Object Security.AccessControl.FileSecurity
  $acl.SetAccessRuleProtection($true, $false)
  $acl.SetOwner((New-Object Security.Principal.SecurityIdentifier('S-1-5-32-544')))
  foreach ($sid in @('S-1-5-18', 'S-1-5-32-544')) {
    $acl.AddAccessRule((New-Object Security.AccessControl.FileSystemAccessRule((New-Object Security.Principal.SecurityIdentifier($sid)), 'FullControl', 'Allow')))
  }
  Set-Acl -LiteralPath $config -AclObject $acl
  $directoryAcl = New-Object Security.AccessControl.DirectorySecurity
  $directoryAcl.SetAccessRuleProtection($true, $false)
  $directoryAcl.SetOwner((New-Object Security.Principal.SecurityIdentifier('S-1-5-32-544')))
  foreach ($sid in @('S-1-5-18', 'S-1-5-32-544')) {
    $directoryAcl.AddAccessRule((New-Object Security.AccessControl.FileSystemAccessRule((New-Object Security.Principal.SecurityIdentifier($sid)), 'FullControl', 'Allow')))
  }
  Set-Acl -LiteralPath $configRoot -AclObject $directoryAcl
  # This candidate records preflight arguments and deliberately fails before
  # any service or installation mutation, even when the policy is valid.
  $probe = Join-Path $fixture 'preflight-probe.ps1'
  $env:FABRIC_POLICY_TEST_ARGS = Join-Path $fixture 'arguments.json'
  '$args | ConvertTo-Json -Compress | Set-Content -LiteralPath $env:FABRIC_POLICY_TEST_ARGS; $global:LASTEXITCODE=47' | Set-Content -LiteralPath $probe -Encoding UTF8
  foreach ($explicitEmpty in @($false, $true)) {
    $options = @{}
    if ($explicitEmpty) { $options.ManagedPathMapping = @() }
    $rejected = $false
    try { & $installer -Binary $probe -PolicyHome $homePath @options } catch { $rejected = $true }
    if (-not $rejected) { throw 'probe did not stop installation before service mutations' }
    $recorded = @(Get-Content -LiteralPath $env:FABRIC_POLICY_TEST_ARGS -Raw | ConvertFrom-Json)
    if (($recorded -contains $mapping) -eq $explicitEmpty) { throw 'upgrade mapping inheritance or explicit clearing failed' }
  }
  Remove-Item -LiteralPath $env:FABRIC_POLICY_TEST_ARGS
  $acl.AddAccessRule((New-Object Security.AccessControl.FileSystemAccessRule((New-Object Security.Principal.SecurityIdentifier('S-1-1-0')), 'Write', 'Allow')))
  Set-Acl -LiteralPath $config -AclObject $acl
  $rejected = $false
  try { & $installer -Binary $probe -PolicyHome $homePath } catch { $rejected = $true }
  if (-not $rejected -or (Test-Path $env:FABRIC_POLICY_TEST_ARGS)) { throw 'untrusted writable mapping configuration reached preflight' }
  $acl.PurgeAccessRules((New-Object Security.Principal.SecurityIdentifier('S-1-1-0')))
  Set-Acl -LiteralPath $config -AclObject $acl
  $directoryAcl.AddAccessRule((New-Object Security.AccessControl.FileSystemAccessRule((New-Object Security.Principal.SecurityIdentifier('S-1-1-0')), 'Write', 'Allow')))
  Set-Acl -LiteralPath $configRoot -AclObject $directoryAcl
  $rejected = $false
  try { & $installer -Binary $probe -PolicyHome $homePath } catch { $rejected = $true }
  if (-not $rejected -or (Test-Path $env:FABRIC_POLICY_TEST_ARGS)) { throw 'untrusted configuration directory reached preflight' }
} finally {
  $env:ProgramFiles = $priorProgramFiles
  $env:ProgramData = $priorProgramData
  Remove-Item Env:FABRIC_POLICY_TEST_ARGS -ErrorAction SilentlyContinue
  if (Test-Path -LiteralPath $fixture) { Remove-Item -LiteralPath $fixture -Recurse -Force }
}
Write-Output 'Windows policy preflight and service quoting tests passed'
$global:LASTEXITCODE = 0
