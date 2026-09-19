param(
  [Parameter(Mandatory = $true)][string]$Binary,
  [string]$NodeId = $env:COMPUTERNAME,
  [string[]]$AllowRoot = @("C:\Users", "C:\ProgramData\machine-fabric")
)

$ErrorActionPreference = "Stop"
$installRoot = Join-Path $env:ProgramFiles "machine-fabric"
$stateRoot = Join-Path $env:ProgramData "machine-fabric"
$installedBinary = Join-Path $installRoot "machine-fabric.exe"
$controllerSocket = Join-Path $stateRoot "controller.sock"
$executorSocket = Join-Path $stateRoot "executor.sock"
$controllerState = Join-Path $stateRoot "controller.json"
$executorState = Join-Path $stateRoot "executor-fences.json"
$backupRoot = Join-Path $stateRoot ("backups\" + (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ"))

New-Item -ItemType Directory -Force -Path $installRoot, $stateRoot | Out-Null
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
Copy-Item -Force -LiteralPath $Binary -Destination $installedBinary

function Quote-Arg([string]$Value) {
  return '"' + $Value.Replace('"', '\"') + '"'
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
)
foreach ($root in $AllowRoot) {
  # IsPathFullyQualified is unavailable in Windows PowerShell 5.1's .NET Framework.
  if ($root -notmatch '^(?:[A-Za-z]:[\\/]|\\\\[^\\]+\\[^\\]+(?:[\\/]|$))') {
    throw "allow-root must be absolute: $root"
  }
  $executorParts += @("--allow-root", (Quote-Arg $root))
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
& $installedBinary --socket $controllerSocket call executor.register $registration | Out-Null
if ($LASTEXITCODE -ne 0) { throw "failed to register local executor" }
Write-Output $installedBinary
