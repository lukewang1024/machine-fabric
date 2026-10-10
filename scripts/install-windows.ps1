param(
  [Parameter(Mandatory = $true)][string]$Binary,
  [string]$NodeId = $env:COMPUTERNAME,
  [string[]]$AllowRoot = @("C:\Users", "C:\ProgramData\machine-fabric"),
  [string]$PolicyUser,
  [string]$PolicyHome = $env:USERPROFILE,
  [string[]]$ManagedPathMapping = @(),
  [switch]$SideBySide
)

function Stage-ImmutableBinary([string]$Source, [string]$Root) {
  $digest = (Get-FileHash -LiteralPath $Source -Algorithm SHA256).Hash.ToLowerInvariant()
  $versions = Join-Path $Root 'versions'
  $directory = Join-Path $versions $digest
  $destination = Join-Path $directory 'machine-fabric.exe'
  foreach ($path in @($versions, $directory, $destination)) {
    if ((Test-Path -LiteralPath $path) -and ((Get-Item -LiteralPath $path -Force).Attributes -band [IO.FileAttributes]::ReparsePoint)) {
      throw 'immutable binary installation contains a redirect'
    }
  }
  if (-not (Test-Path -LiteralPath $destination)) {
    New-Item -ItemType Directory -Path $directory -Force | Out-Null
    Copy-Item -LiteralPath $Source -Destination $destination
  }
  if ((Get-FileHash -LiteralPath $destination -Algorithm SHA256).Hash.ToLowerInvariant() -ne $digest) {
    throw 'immutable binary digest mismatch; reconcile existing installation'
  }
  return $destination
}

$ErrorActionPreference = "Stop"
foreach ($module in @('Microsoft.PowerShell.Utility', 'Microsoft.PowerShell.Management', 'Microsoft.PowerShell.Security', 'CimCmdlets')) {
  Import-Module (Join-Path $PSHOME ("Modules\$module\$module.psd1")) -Force -ErrorAction Stop
}
if ($PolicyUser) {
  try {
    $account = New-Object System.Security.Principal.NTAccount($PolicyUser)
    $sid = $account.Translate([System.Security.Principal.SecurityIdentifier]).Value
  } catch {
    throw "desktop policy user could not be resolved: $PolicyUser"
  }
  $profile = Get-CimInstance -ClassName Win32_UserProfile -Filter "SID = '$sid'"
  if (-not $profile -or -not $profile.LocalPath) {
    throw "desktop policy user has no local profile: $PolicyUser"
  }
  $PolicyHome = $profile.LocalPath
}
if (-not $PolicyHome -or $PolicyHome -notmatch '^(?:[A-Za-z]:[\\/]|\\\\[^\\]+\\[^\\]+(?:[\\/]|$))' -or
    -not (Test-Path -LiteralPath $PolicyHome -PathType Container)) {
  throw "desktop policy home must be an existing absolute directory: $PolicyHome"
}
$installRoot = Join-Path $env:ProgramFiles "machine-fabric"
$stateRoot = Join-Path $env:ProgramData "machine-fabric"
$installedBinary = Join-Path $installRoot "machine-fabric.exe"
$legacyBinary = $installedBinary
# Existing executables may be held by peer acceptors or user-owned clients.
# Stage by content identity instead of overwriting or stopping those processes.
# Reapplying the same content reuses the immutable binary, even while it is live.
if (Test-Path -LiteralPath $legacyBinary) { $SideBySide = $true }
$controllerSocket = Join-Path $stateRoot "controller.sock"
$executorSocket = Join-Path $stateRoot "executor.sock"
$controllerState = Join-Path $stateRoot "controller.json"
$executorState = Join-Path $stateRoot "executor-fences.json"
$backupRoot = Join-Path $stateRoot ("backups\" + (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ"))
$mappingConfig = Join-Path $installRoot "managed-path-mappings.json"
foreach ($trustedPath in @($installRoot, $mappingConfig)) {
  if (-not (Test-Path -LiteralPath $trustedPath)) { continue }
  $item = Get-Item -LiteralPath $trustedPath -Force
  if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'managed mapping configuration must not be a redirect' }
  $acl = Get-Acl -LiteralPath $trustedPath
  $trustedSids = @('S-1-5-18', 'S-1-5-32-544')
  # Program Files inherits an effective FullControl ACE for Windows Modules
  # Installer. Trust that fixed OS service SID only on the installation root;
  # persisted mapping configuration remains writable by SYSTEM/Admins only.
  $trustedWriterSids = @($trustedSids)
  if ($trustedPath -eq $installRoot) {
    $trustedWriterSids += 'S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464'
  }
  if ($acl.GetOwner([Security.Principal.SecurityIdentifier]).Value -notin $trustedSids) { throw 'managed mapping configuration owner is not an administrator' }
  $writeRights = [Security.AccessControl.FileSystemRights]::Write -bor [Security.AccessControl.FileSystemRights]::Delete -bor [Security.AccessControl.FileSystemRights]::ChangePermissions -bor [Security.AccessControl.FileSystemRights]::TakeOwnership
  foreach ($rule in $acl.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier])) {
    if ($rule.AccessControlType -eq [Security.AccessControl.AccessControlType]::Allow -and
        ($rule.FileSystemRights -band $writeRights) -and $rule.IdentityReference.Value -notin $trustedWriterSids) {
      throw 'managed mapping configuration is writable by an untrusted identity'
    }
  }
}
if (Test-Path -LiteralPath $mappingConfig) {
  if (-not $PSBoundParameters.ContainsKey('ManagedPathMapping')) {
    $savedMappings = Get-Content -LiteralPath $mappingConfig -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($savedMappings.version -ne 1 -or $null -eq $savedMappings.mappings) { throw 'invalid managed mapping configuration' }
    $ManagedPathMapping = @($savedMappings.mappings)
    foreach ($mapping in $ManagedPathMapping) { if ($mapping -isnot [string]) { throw 'managed mapping must be a string' } }
  }
}
# Run the candidate's exact policy validation before any service interruption.
$validationArgs = @('executor', 'validate-path-policy', '--policy-home', $PolicyHome)
foreach ($mapping in $ManagedPathMapping) { $validationArgs += @('--managed-path-mapping', $mapping) }
& $Binary @validationArgs
if ($LASTEXITCODE -ne 0) { throw 'managed path policy preflight failed; services were not stopped' }

$initialServiceProcessIds = @(
  Get-CimInstance -ClassName Win32_Service -Filter "Name = 'MachineFabricController' OR Name = 'MachineFabricExecutor'" |
    Where-Object { $_.ProcessId -gt 0 } | Select-Object -ExpandProperty ProcessId
)
$initialFabricProcesses = @(Get-CimInstance -ClassName Win32_Process -Filter "Name = 'machine-fabric.exe'")
$ownedProcessIds = @($initialServiceProcessIds)
do {
  $children = @($initialFabricProcesses | Where-Object { $_.ParentProcessId -in $ownedProcessIds -and $_.ProcessId -notin $ownedProcessIds } | Select-Object -ExpandProperty ProcessId)
  $ownedProcessIds += $children
} while ($children.Count)
$ownedProcesses = @($initialFabricProcesses | Where-Object { $_.ProcessId -in $ownedProcessIds })
$foreignHolders = @($initialFabricProcesses | Where-Object {
  $_.ExecutablePath -and $_.ExecutablePath.Equals($legacyBinary, [StringComparison]::OrdinalIgnoreCase) -and $_.ProcessId -notin $ownedProcessIds
})
if ($foreignHolders.Count -and -not $SideBySide) {
  throw 'unowned processes hold the legacy binary; use -SideBySide or arrange an owner-controlled stop; services were not stopped'
}
if ($SideBySide -and -not (Test-Path -LiteralPath $legacyBinary)) {
  throw 'side-by-side upgrade requires an existing legacy installation'
}

New-Item -ItemType Directory -Force -Path $installRoot, $stateRoot | Out-Null
if ($SideBySide) { $installedBinary = Stage-ImmutableBinary -Source $Binary -Root $installRoot }
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'resolve-windows-binary.ps1') -Destination (Join-Path $installRoot 'resolve-windows-binary.ps1') -Force
$mappingTemporary = Join-Path $installRoot ("managed-path-mappings-" + [guid]::NewGuid().ToString('N') + '.tmp')
try {
  $mappingJson = @{ version = 1; mappings = @($ManagedPathMapping) } | ConvertTo-Json -Compress
  [IO.File]::WriteAllText($mappingTemporary, $mappingJson, (New-Object System.Text.UTF8Encoding $false))
  $mappingAcl = New-Object Security.AccessControl.FileSecurity
  $mappingAcl.SetAccessRuleProtection($true, $false)
  $administrators = New-Object Security.Principal.SecurityIdentifier('S-1-5-32-544')
  $mappingAcl.SetOwner($administrators)
  foreach ($sid in @('S-1-5-18', 'S-1-5-32-544')) {
    $identity = New-Object Security.Principal.SecurityIdentifier($sid)
    $rule = New-Object Security.AccessControl.FileSystemAccessRule($identity, 'FullControl', 'Allow')
    $mappingAcl.AddAccessRule($rule)
  }
  Set-Acl -LiteralPath $mappingTemporary -AclObject $mappingAcl
  Move-Item -LiteralPath $mappingTemporary -Destination $mappingConfig -Force
} finally {
  if (Test-Path -LiteralPath $mappingTemporary) { Remove-Item -LiteralPath $mappingTemporary -Force }
}
if ((Test-Path $controllerState) -or (Test-Path $executorState)) {
  New-Item -ItemType Directory -Force -Path $backupRoot | Out-Null
  foreach ($stateFile in @($controllerState, $executorState)) {
    if (Test-Path $stateFile) { Copy-Item -LiteralPath $stateFile -Destination $backupRoot }
  }
}
$fabricRoot = Join-Path $stateRoot "fabric"
New-Item -ItemType Directory -Force -Path $fabricRoot | Out-Null
# peer accept runs as the authenticated OpenSSH user and creates only proxy
# endpoints in this directory. Controller state remains writable by services.
& icacls.exe $fabricRoot /grant '*S-1-5-11:(OI)(CI)M' /T /C | Out-Null
if ($LASTEXITCODE -ne 0) { throw "failed to grant fabric IPC directory access" }
foreach ($serviceName in @("MachineFabricController", "MachineFabricExecutor")) {
  if (Get-Service -Name $serviceName -ErrorAction SilentlyContinue) {
    Stop-Service -Name $serviceName -Force -ErrorAction SilentlyContinue
  }
}
foreach ($serviceName in @("MachineFabricController", "MachineFabricExecutor")) {
  $service = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
  if ($service -and $service.Status -ne [System.ServiceProcess.ServiceControllerStatus]::Stopped) {
    $service.WaitForStatus(
      [System.ServiceProcess.ServiceControllerStatus]::Stopped,
      [TimeSpan]::FromSeconds(30)
    )
  }
}
$processDeadline = (Get-Date).AddSeconds(30)
do {
  $runningProcesses = @(
    Get-CimInstance -ClassName Win32_Process -Filter "Name = 'machine-fabric.exe'" |
      Where-Object {
        $process = $_
        @($ownedProcesses | Where-Object { $_.ProcessId -eq $process.ProcessId -and $_.CreationDate -eq $process.CreationDate }).Count -gt 0
      }
  )
  if ($runningProcesses.Count -eq 0) { break }
  if ((Get-Date) -ge $processDeadline) {
    $serviceProcessIdsText = $initialServiceProcessIds -join ", "
    $processDetails = ($runningProcesses | ForEach-Object {
      $owner = if ($initialServiceProcessIds -contains $_.ProcessId) {
        "service"
      } elseif ($initialServiceProcessIds -contains $_.ParentProcessId) {
        "service-child"
      } else {
        "unmatched"
      }
      "pid=$($_.ProcessId), parent=$($_.ParentProcessId), created=$($_.CreationDate), owner=$owner"
    }) -join "; "
    throw "Machine Fabric processes still hold the installed binary (service pids: $serviceProcessIdsText; remaining: $processDetails)"
  }
  Start-Sleep -Milliseconds 200
} while ($true)
if (-not $SideBySide) { Copy-Item -Force -LiteralPath $Binary -Destination $installedBinary }

function Quote-Arg([string]$Value) {
  if ($Value -match '[\r\n\x00]') { throw 'service argument contains a control character' }
  # CommandLineToArgvW/CRT quoting: double backslashes before a quote and
  # before the closing delimiter, including drive roots ending in backslash.
  $escaped = [regex]::Replace($Value, '(\\*)"', '$1$1\"')
  $escaped = [regex]::Replace($escaped, '(\\+)$', '$1$1')
  return '"' + $escaped + '"'
}

$controllerArgs = @(
  (Quote-Arg $installedBinary)
  "--service", (Quote-Arg "MachineFabricController")
  "--socket", (Quote-Arg $controllerSocket)
  "controller", "serve", "--state", (Quote-Arg $controllerState), "--id", (Quote-Arg $NodeId)
) -join " "
$executorParts = @(
  (Quote-Arg $installedBinary)
  "--service", (Quote-Arg "MachineFabricExecutor")
  "--socket", (Quote-Arg $executorSocket)
  "executor", "serve", "--id", (Quote-Arg ($NodeId + "-native"))
  "--state", (Quote-Arg $executorState)
  "--path-policy", "desktop", "--policy-home", (Quote-Arg $PolicyHome)
)
foreach ($root in $AllowRoot) {
  # IsPathFullyQualified is unavailable in Windows PowerShell 5.1's .NET Framework.
  if ($root -notmatch '^(?:[A-Za-z]:[\\/]|\\\\[^\\]+\\[^\\]+(?:[\\/]|$))') {
    throw "allow-root must be absolute: $root"
  }
  $executorParts += @("--allow-root", (Quote-Arg $root))
}
foreach ($mapping in $ManagedPathMapping) {
  if (-not $mapping.Contains('=') -or $mapping -match '[\r\n]') { throw 'managed mapping must be LOGICAL=PHYSICAL' }
  $executorParts += @("--managed-path-mapping", (Quote-Arg $mapping))
}
$executorArgs = $executorParts -join " "

foreach ($service in @(
  @{ Name = "MachineFabricController"; Display = "Machine Fabric Controller"; Command = $controllerArgs },
  @{ Name = "MachineFabricExecutor"; Display = "Machine Fabric Executor"; Command = $executorArgs }
)) {
  $existing = Get-Service -Name $service.Name -ErrorAction SilentlyContinue
  if ($existing) {
    Stop-Service -Name $service.Name -Force -ErrorAction SilentlyContinue
    $serviceKey = "HKLM:\SYSTEM\CurrentControlSet\Services\$($service.Name)"
    if (!(Test-Path -LiteralPath $serviceKey)) { throw "service registry key is missing: $($service.Name)" }
    Set-ItemProperty -LiteralPath $serviceKey -Name ImagePath -Value $service.Command
    Set-Service -Name $service.Name -StartupType Automatic
  } else {
    New-Service -Name $service.Name -BinaryPathName $service.Command -StartupType Automatic -DisplayName $service.Display | Out-Null
  }
  & sc.exe failure $service.Name reset= 86400 actions= restart/1000/restart/5000/restart/30000 | Out-Null
  if ($LASTEXITCODE -ne 0) { throw "failed to configure recovery actions for $($service.Name)" }
  Start-Service -Name $service.Name
}

$deadline = (Get-Date).AddSeconds(20)
do {
  & $installedBinary --socket $controllerSocket status 2>$null | Out-Null
  $controllerReady = $LASTEXITCODE -eq 0
  & $installedBinary --socket $executorSocket status 2>$null | Out-Null
  $executorReady = $LASTEXITCODE -eq 0
  if ($controllerReady -and $executorReady) {
    break
  }
  if ((Get-Date) -ge $deadline) { throw "services did not become ready" }
  Start-Sleep -Milliseconds 200
} while ($true)

$registration = @{
  executorId = $NodeId + "-native"
  endpoint = @{ transport = "local"; socket = $executorSocket }
} | ConvertTo-Json -Compress
$registrationPath = Join-Path $env:TEMP ("machine-fabric-registration-" + [guid]::NewGuid().ToString("N") + ".json")
try {
  [IO.File]::WriteAllText($registrationPath, $registration, (New-Object System.Text.UTF8Encoding $false))
  & $installedBinary --socket $controllerSocket call executor.register --params-file $registrationPath | Out-Null
  if ($LASTEXITCODE -ne 0) { throw "failed to register local executor" }
} finally {
  Remove-Item -LiteralPath $registrationPath -Force -ErrorAction SilentlyContinue
}
$receipt = @{
  installedBinary = $installedBinary
  installedSha256 = (Get-FileHash -LiteralPath $installedBinary -Algorithm SHA256).Hash.ToLowerInvariant()
  sideBySide = [bool]$SideBySide
  legacyBinary = $legacyBinary
  legacyBinaryUpdated = (-not $SideBySide)
  retainedForeignProcessIds = @($foreignHolders | Select-Object -ExpandProperty ProcessId)
  completedAt = (Get-Date).ToUniversalTime().ToString('o')
} | ConvertTo-Json -Depth 5
$receiptTemporary = Join-Path $installRoot ('installation-' + [guid]::NewGuid().ToString('N') + '.tmp')
try {
  [IO.File]::WriteAllText($receiptTemporary, $receipt, (New-Object System.Text.UTF8Encoding $false))
  Move-Item -LiteralPath $receiptTemporary -Destination (Join-Path $installRoot 'installation.json') -Force
} finally {
  if (Test-Path -LiteralPath $receiptTemporary) { Remove-Item -LiteralPath $receiptTemporary -Force }
}
Write-Output $installedBinary
