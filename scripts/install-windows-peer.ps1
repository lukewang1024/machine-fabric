param(
  [Parameter(Mandatory = $true)][string]$PeerId,
  [Parameter(Mandatory = $true)][string]$LocalId,
  [Parameter(Mandatory = $true)][string]$HostName,
  [Parameter(Mandatory = $true)][ValidateSet("posix", "windows")][string]$RemotePlatform,
  [Parameter(Mandatory = $true)][string]$RemoteExecutable,
  [Parameter(Mandatory = $true)][string]$RemoteStateRoot
)

$ErrorActionPreference = "Stop"
if ($PeerId -notmatch '^[0-9A-Za-z._-]+$') { throw "invalid peer id: $PeerId" }
$binary = Join-Path $env:ProgramFiles "machine-fabric\machine-fabric.exe"
$stateRoot = Join-Path $env:ProgramData "machine-fabric"
$peerRoot = Join-Path $stateRoot ("peers\" + $PeerId)
New-Item -ItemType Directory -Force -Path $peerRoot | Out-Null

function Q([string]$Value) { return '"' + $Value.Replace('"', '\"') + '"' }
$command = @(
  (Q $binary), "peer", "connect",
  "--id", (Q $PeerId), "--local-id", (Q $LocalId), "--host", (Q $HostName),
  "--local-controller-socket", (Q (Join-Path $stateRoot "controller.sock")),
  "--local-executor-socket", (Q (Join-Path $stateRoot "executor.sock")),
  "--expose-controller-socket", (Q (Join-Path $peerRoot "controller.sock")),
  "--expose-executor-socket", (Q (Join-Path $peerRoot "executor.sock")),
  "--remote-executable", (Q $RemoteExecutable),
  "--remote-state-root", (Q $RemoteStateRoot),
  "--remote-platform", $RemotePlatform,
  "--state", (Q (Join-Path $peerRoot "status.json"))
) -join " "
$name = "MachineFabricPeer_" + $PeerId
$existing = Get-Service -Name $name -ErrorAction SilentlyContinue
if ($existing) {
  Stop-Service -Name $name -Force -ErrorAction SilentlyContinue
  & sc.exe config $name binPath= $command start= auto | Out-Null
} else {
  & sc.exe create $name binPath= $command start= auto DisplayName= ("Machine Fabric Peer " + $PeerId) | Out-Null
}
if ($LASTEXITCODE -ne 0) { throw "failed to configure $name" }
$statusPath = Join-Path $peerRoot "status.json"
if (Test-Path $statusPath) {
  Move-Item -Force -LiteralPath $statusPath -Destination ($statusPath + ".previous")
}
& sc.exe failure $name reset= 86400 actions= restart/1000/restart/5000/restart/30000 | Out-Null
Start-Service -Name $name
Write-Output $statusPath
