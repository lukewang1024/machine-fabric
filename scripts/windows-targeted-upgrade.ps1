param(
  [Parameter(Mandatory=$true)][ValidateSet('Start','Worker','Status','Cleanup')][string]$Action,
  [Parameter(Mandatory=$true)][ValidatePattern('^[A-Za-z0-9][A-Za-z0-9_-]{7,63}$')][string]$TransactionId,
  [string]$PlanPath,
  [string]$PlanSha256,
  [string]$ArchivePath,
  [string]$ToolSha256
)

$ErrorActionPreference = 'Stop'
foreach ($module in @('Microsoft.PowerShell.Utility','Microsoft.PowerShell.Management','Microsoft.PowerShell.Security','CimCmdlets','ScheduledTasks')) {
  Import-Module (Join-Path $PSHOME ("Modules\$module\$module.psd1")) -Force -ErrorAction Stop
}
$root = Join-Path $env:ProgramFiles 'machine-fabric\targeted-upgrades'
$stage = Join-Path $root $TransactionId
$taskName = 'MachineFabricUpgrade-' + $TransactionId

function Get-Hash([string]$Path) { (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant() }
function Assert-Hash([string]$Path, [string]$Hash) {
  if ($Hash -notmatch '^[a-f0-9]{64}$' -or (Get-Hash $Path) -ne $Hash) { throw "SHA-256 mismatch: $Path" }
}
function Assert-Regular([string]$Path) {
  $item = Get-Item -LiteralPath $Path -Force -ErrorAction Stop
  if ($item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) { throw 'Expected a regular file' }
}
function Save-Receipt($Value) {
  $temporary = Join-Path $stage ('receipt-' + [guid]::NewGuid().ToString('N') + '.tmp')
  [IO.File]::WriteAllText($temporary, ($Value | ConvertTo-Json -Depth 16), (New-Object Text.UTF8Encoding $false))
  Move-Item -LiteralPath $temporary -Destination (Join-Path $stage 'receipt.json') -Force
}
function Quote-Fabric([string]$Value) {
  if ($Value -match '[\r\n\x00]') { throw 'Invalid service argument' }
  $escaped = [regex]::Replace($Value, '(\\*)"', '$1$1\"')
  $escaped = [regex]::Replace($escaped, '(\\+)$', '$1$1')
  return '"' + $escaped + '"'
}
function Get-ExpectedPaths($Plan, [string]$Binary) {
  $stateRoot = Join-Path $env:ProgramData 'machine-fabric'
  $c = @((Quote-Fabric $Binary), '--service', (Quote-Fabric 'MachineFabricController'), '--socket', (Quote-Fabric (Join-Path $stateRoot 'controller.sock')), 'controller', 'serve', '--state', (Quote-Fabric (Join-Path $stateRoot 'controller.json')), '--id', (Quote-Fabric $Plan.nodeId)) -join ' '
  $e = @((Quote-Fabric $Binary), '--service', (Quote-Fabric 'MachineFabricExecutor'), '--socket', (Quote-Fabric (Join-Path $stateRoot 'executor.sock')), 'executor', 'serve', '--id', (Quote-Fabric ($Plan.nodeId + '-native')), '--state', (Quote-Fabric (Join-Path $stateRoot 'executor-fences.json')), '--path-policy', 'desktop', '--policy-home', (Quote-Fabric $Plan.policyHome))
  foreach ($item in $Plan.allowRoots) { $e += @('--allow-root', (Quote-Fabric $item)) }
  foreach ($item in $Plan.managedPathMappings) { $e += @('--managed-path-mapping', (Quote-Fabric $item)) }
  return @{MachineFabricController=$c; MachineFabricExecutor=($e -join ' ')}
}
function Assert-Plan($Plan) {
  if ($Plan.version -notmatch '^\d+\.\d+\.\d+$' -or $Plan.nodeId -notmatch '^[A-Za-z0-9._-]+$' -or !$Plan.maintenanceOwner) { throw 'Invalid plan identity' }
  if ($Plan.archiveSha256 -notmatch '^[a-f0-9]{64}$' -or $Plan.previousBinarySha256 -notmatch '^[a-f0-9]{64}$') { throw 'Invalid plan digest' }
  if ($Plan.policyHome -notmatch '^[A-Za-z]:[\\/]' -or @($Plan.allowRoots).Count -lt 1) { throw 'Invalid desktop policy' }
  foreach ($item in $Plan.allowRoots) { if ($item -notmatch '^[A-Za-z]:[\\/]' -or $item -match '[\r\n\x00]') { throw 'Invalid allow root' } }
  foreach ($item in $Plan.managedPathMappings) { if ($item -notmatch '^[A-Za-z]:[\\/].+=[A-Za-z]:[\\/].+' -or $item -match '[\r\n\x00]') { throw 'Invalid managed mapping' } }
}
function Get-Services($Plan, [string]$Binary) {
  $expected = Get-ExpectedPaths $Plan $Binary
  $services = @(Get-CimInstance Win32_Service -Filter "Name='MachineFabricController' OR Name='MachineFabricExecutor'")
  if ($services.Count -ne 2) { throw 'Expected exactly two Fabric services' }
  foreach ($service in $services) {
    if (-not $expected.ContainsKey($service.Name) -or $service.State -ne 'Running' -or $service.StartMode -ine 'Auto' -or $service.StartName -ine 'LocalSystem' -or $service.ProcessId -le 0 -or $service.PathName -cne $expected[$service.Name]) { throw 'Service identity or policy changed' }
    $process = Get-CimInstance Win32_Process -Filter ('ProcessId=' + $service.ProcessId)
    if (-not $process -or $process.ExecutablePath -ine $Binary) { throw 'Service process identity changed' }
  }
  return $services
}
function Invoke-LocalFabric([string]$Binary, [string]$Socket, [string]$Action, [string]$InputJson) {
  # Windows PowerShell 5.1 can strip quotes from JSON passed as native argv.
  # The install root has already passed the trusted-directory check.
  $paramsFile = Join-Path (Join-Path $env:ProgramFiles 'machine-fabric') ('upgrade-rpc-' + [guid]::NewGuid().ToString('N') + '.json')
  try {
    $stream = [IO.File]::Open($paramsFile, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
    try {
      $bytes = (New-Object Text.UTF8Encoding $false).GetBytes($InputJson)
      $stream.Write($bytes, 0, $bytes.Length); $stream.Flush()
    } finally { $stream.Dispose() }
    $output = & $Binary --socket $Socket call $Action --params-file $paramsFile
    if ($LASTEXITCODE -ne 0) { throw "Fabric $Action is unavailable" }
  } finally {
    if (Test-Path -LiteralPath $paramsFile) { Remove-Item -LiteralPath $paramsFile -Force -ErrorAction Stop }
    if (Test-Path -LiteralPath $paramsFile) { throw 'Temporary RPC file cleanup not confirmed' }
  }
  $response = ($output | Out-String | ConvertFrom-Json)
  if (-not $response.ok -or $null -eq $response.result) { throw "Fabric $Action returned an invalid response" }
  return $response.result
}
function Assert-Quiescent($Plan, [string]$Binary, [bool]$AllowSubmittingCommand = $false) {
  $stateRoot = Join-Path $env:ProgramData 'machine-fabric'
  $executorSocket = Join-Path $stateRoot 'executor.sock'; $controllerSocket = Join-Path $stateRoot 'controller.sock'
  # The state directory may be writable by ordinary users. Admission evidence
  # must come from the live service, never from its persisted JSON files.
  $q = Invoke-LocalFabric $Binary $executorSocket 'desktop.list' '{"includeTerminal":false}'
  foreach ($field in @('maintenanceOwner','blocked','inFlight','jobs','safePoint')) { if ($field -notin $q.PSObject.Properties.Name) { throw 'Unknown desktop queue schema' } }
  if ($q.blocked -isnot [bool] -or $q.inFlight -isnot [bool] -or $q.safePoint -isnot [bool] -or $q.jobs -isnot [array]) { throw 'Unknown desktop queue schema' }
  if ($q.maintenanceOwner -cne $Plan.maintenanceOwner -or $q.blocked -or $q.inFlight -or -not $q.safePoint -or @($q.jobs).Count) { throw 'Desktop maintenance is not quiescent' }
  $controller = Invoke-LocalFabric $Binary $controllerSocket 'status' '{}'
  if ($controller.leases -isnot [array]) { throw 'Unknown controller lease schema' }
  $now = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
  foreach ($lease in $controller.leases) { if (($lease.expiresAt -isnot [long] -and $lease.expiresAt -isnot [int]) -or $lease.expiresAt -lt 0 -or $lease.expiresAt -gt $now) { throw 'Controller has an active or unknown lease' } }
  foreach ($state in @('running','queued')) {
    $filter = @{state=$state;limit=1} | ConvertTo-Json -Compress
    $page = Invoke-LocalFabric $Binary $controllerSocket 'task.list' $filter
    if (($page.total -isnot [long] -and $page.total -isnot [int]) -or $page.total -ne 0 -or $page.tasks -isnot [array]) { throw 'Controller has unfinished or unknown tasks' }
  }
  $result = Invoke-LocalFabric $Binary $executorSocket 'availability' '{}'
  foreach ($field in @('active','queued','heldResourceCount')) { if ($result.$field -isnot [int] -and $result.$field -isnot [long]) { throw 'Executor availability schema is unknown' } }
  if ($null -eq $result.active -or $null -eq $result.queued -or $null -eq $result.heldResourceCount -or $result.queued -ne 0) { throw 'Executor has active, queued or unknown work' }
  $idle = $result.active -eq 0 -and $result.heldResourceCount -eq 0
  $submitting = $AllowSubmittingCommand -and $result.active -eq 1 -and $result.heldResourceCount -eq 1
  if (-not $idle -and -not $submitting) { throw 'Executor has active, queued or unknown work' }
  $processResult = Invoke-LocalFabric $Binary $executorSocket 'process.list' '{}'
  if ($processResult.processes -isnot [array] -or @($processResult.processes | Where-Object { $_.state -notin @('stopped','failed') }).Count) { throw 'Managed processes are running or unknown' }
  $helpers = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'computer-use|bridge' -or ($_.Name -in @('node.exe','python.exe','python3.exe') -and $_.CommandLine -match 'computer-use-host|pi-computer-use.*serve') })
  if ($helpers.Count) { throw 'Desktop helpers remain active' }
}
function Assert-TrustedDirectory([string]$Path, [bool]$AllowWindowsInstaller = $false) {
  $item = Get-Item -LiteralPath $Path -Force -ErrorAction Stop
  if (-not $item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) { throw 'Upgrade directory is redirected' }
  $acl = Get-Acl -LiteralPath $Path
  $trusted = @('S-1-5-18','S-1-5-32-544')
  if ($acl.GetOwner([Security.Principal.SecurityIdentifier]).Value -notin $trusted) { throw 'Upgrade directory owner is untrusted' }
  $writers = @($trusted)
  if ($AllowWindowsInstaller) { $writers += 'S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464' }
  $writes = [Security.AccessControl.FileSystemRights]::Write -bor [Security.AccessControl.FileSystemRights]::Delete -bor [Security.AccessControl.FileSystemRights]::ChangePermissions -bor [Security.AccessControl.FileSystemRights]::TakeOwnership
  foreach ($rule in $acl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier])) { if ($rule.AccessControlType -eq 'Allow' -and ($rule.FileSystemRights -band $writes) -and $rule.IdentityReference.Value -notin $writers) { throw 'Upgrade directory is writable by an untrusted identity' } }
}
function Set-TrustedDirectory([string]$Path) {
  $acl = New-Object Security.AccessControl.DirectorySecurity
  $acl.SetAccessRuleProtection($true,$false)
  $acl.SetOwner((New-Object Security.Principal.SecurityIdentifier('S-1-5-32-544')))
  foreach ($sid in @('S-1-5-18','S-1-5-32-544')) {
    $identity = New-Object Security.Principal.SecurityIdentifier($sid)
    $rule = New-Object Security.AccessControl.FileSystemAccessRule($identity,'FullControl','ContainerInherit,ObjectInherit','None','Allow')
    $acl.AddAccessRule($rule)
  }
  Set-Acl -LiteralPath $Path -AclObject $acl
  Assert-TrustedDirectory $Path
}

Assert-TrustedDirectory (Join-Path $env:ProgramFiles 'machine-fabric') $true
if ($Action -eq 'Status') {
  Assert-TrustedDirectory $root; Assert-TrustedDirectory $stage
  $receipt = Get-Content -LiteralPath (Join-Path $stage 'receipt.json') -Raw | ConvertFrom-Json
  $task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
  @{receipt=$receipt; taskState=if ($task) {[string]$task.State} else {$null}; lastTaskResult=if ($task) {(Get-ScheduledTaskInfo -TaskName $taskName).LastTaskResult} else {$null}} | ConvertTo-Json -Depth 16 -Compress
  exit 0
}
if ($Action -eq 'Cleanup') {
  Assert-TrustedDirectory $root; Assert-TrustedDirectory $stage
  $receipt = Get-Content -LiteralPath (Join-Path $stage 'receipt.json') -Raw | ConvertFrom-Json
  $task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
  if ($receipt.phase -cne 'succeeded' -or ($task -and $task.State -notin @('Ready','Disabled'))) { throw 'Transaction is not safely complete; retain the receipt and reconcile' }
  if ($task) {
    $expectedArgs = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + (Join-Path $stage 'worker.ps1') + '" -Action Worker -TransactionId ' + $TransactionId
    if (@($task.Actions).Count -ne 1 -or $task.Actions[0].Execute -ine 'powershell.exe' -or $task.Actions[0].Arguments -cne $expectedArgs -or $task.Principal.UserId -notin @('SYSTEM','S-1-5-18')) { throw 'Scheduled task identity changed; refusing cleanup' }
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) { throw 'Scheduled task cleanup not confirmed' }
  }
  Remove-Item -LiteralPath $stage -Recurse -Force
  if (Test-Path -LiteralPath $stage) { throw 'Transaction cleanup not confirmed' }
  @{transactionId=$TransactionId; cleaned=$true} | ConvertTo-Json -Compress
  exit 0
}
if ($Action -eq 'Start') {
  Assert-Regular $PSCommandPath; Assert-Hash $PSCommandPath $ToolSha256
  Assert-Regular $PlanPath; Assert-Regular $ArchivePath; Assert-Hash $PlanPath $PlanSha256
  $plan = Get-Content -LiteralPath $PlanPath -Raw | ConvertFrom-Json; Assert-Plan $plan; Assert-Hash $ArchivePath $plan.archiveSha256
  $old = (Get-Content -LiteralPath (Join-Path $env:ProgramFiles 'machine-fabric\installation.json') -Raw | ConvertFrom-Json).installedBinary
  Assert-Regular $old; Assert-Hash $old $plan.previousBinarySha256; $null = Get-Services $plan $old; Assert-Quiescent $plan $old $true
  if (!(Test-Path -LiteralPath $root)) { New-Item -ItemType Directory -Path $root -ErrorAction Stop | Out-Null; Set-TrustedDirectory $root }
  Assert-TrustedDirectory $root
  if ((Test-Path -LiteralPath $stage) -or (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue)) { throw 'Transaction already exists; observe rather than replay' }
  New-Item -ItemType Directory -Path $stage -ErrorAction Stop | Out-Null; Set-TrustedDirectory $stage
  Copy-Item -LiteralPath $PSCommandPath -Destination (Join-Path $stage 'worker.ps1'); Assert-Hash (Join-Path $stage 'worker.ps1') $ToolSha256
  Copy-Item -LiteralPath $PlanPath -Destination (Join-Path $stage 'plan.json'); Assert-Hash (Join-Path $stage 'plan.json') $PlanSha256
  $archive = Join-Path $stage 'release.zip'; Copy-Item -LiteralPath $ArchivePath -Destination $archive; Assert-Hash $archive $plan.archiveSha256
  Save-Receipt @{transactionId=$TransactionId;phase='prepared';planSha256=$PlanSha256;oldBinary=$old;workerSha256=(Get-Hash (Join-Path $stage 'worker.ps1'));createdAt=[DateTime]::UtcNow.ToString('o');installerAttempted=$false;version=$null;installedSha256=$null;error=$null;recovery=@();finishedAt=$null}
  $actionSpec = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument ('-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + (Join-Path $stage 'worker.ps1') + '" -Action Worker -TransactionId ' + $TransactionId)
  $principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
  $settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 10)
  Register-ScheduledTask -TaskName $taskName -Action $actionSpec -Principal $principal -Settings $settings | Out-Null
  $receipt = Get-Content -LiteralPath (Join-Path $stage 'receipt.json') -Raw | ConvertFrom-Json
  $receipt.phase='submitted'; Save-Receipt $receipt
  Start-ScheduledTask -TaskName $taskName
  @{transactionId=$TransactionId;phase='submitted';task=$taskName} | ConvertTo-Json -Compress
  exit 0
}

# This worker is owned by Task Scheduler, not by either Fabric service. It is
# never automatically resubmitted after an interrupted connection.
Assert-TrustedDirectory $root; Assert-TrustedDirectory $stage
$receipt = Get-Content -LiteralPath (Join-Path $stage 'receipt.json') -Raw | ConvertFrom-Json
if ($receipt.phase -cne 'submitted') { throw 'Transaction already started or not submitted' }
$receipt.phase='running'; Save-Receipt $receipt
try {
  Assert-Hash $PSCommandPath $receipt.workerSha256; Assert-Hash (Join-Path $stage 'plan.json') $receipt.planSha256
  $plan = Get-Content -LiteralPath (Join-Path $stage 'plan.json') -Raw | ConvertFrom-Json; Assert-Plan $plan
  Start-Sleep -Seconds 10
  Assert-Hash (Join-Path $stage 'release.zip') $plan.archiveSha256
  Assert-Hash $receipt.oldBinary $plan.previousBinarySha256
  $null = Get-Services $plan $receipt.oldBinary; Assert-Quiescent $plan $receipt.oldBinary
  $release = Join-Path $stage 'release'; Expand-Archive -LiteralPath (Join-Path $stage 'release.zip') -DestinationPath $release
  $native = if ($env:PROCESSOR_ARCHITEW6432) {$env:PROCESSOR_ARCHITEW6432} else {$env:PROCESSOR_ARCHITECTURE}
  $target = switch ($native.ToUpperInvariant()) { 'AMD64' {'x86_64-pc-windows-msvc'} 'ARM64' {'aarch64-pc-windows-msvc'} default {throw 'Unsupported Windows architecture'} }
  $package = Join-Path $release ("machine-fabric-$($plan.version)-$target")
  $binary = Join-Path $package 'bin\machine-fabric.exe'; $installer = Join-Path $package 'scripts\install-windows.ps1'
  Assert-Regular $binary; Assert-Regular $installer
  $candidateHash = Get-Hash $binary
  $candidateBinary = Join-Path (Join-Path (Join-Path $env:ProgramFiles 'machine-fabric\versions') $candidateHash) 'machine-fabric.exe'
  $receipt.phase='installing'; $receipt.installerAttempted=$true; Save-Receipt $receipt
  & $installer -Binary $binary -NodeId $plan.nodeId -PolicyHome $plan.policyHome -AllowRoot @($plan.allowRoots) -ManagedPathMapping @($plan.managedPathMappings) -SideBySide *> (Join-Path $stage 'installer.log')
  $installation = Get-Content -LiteralPath (Join-Path $env:ProgramFiles 'machine-fabric\installation.json') -Raw | ConvertFrom-Json
  if ((Get-Hash $binary) -cne $installation.installedSha256) { throw 'Installed binary digest differs' }
  $receipt.version = (& $installation.installedBinary --version | Out-String).Trim()
  if ($receipt.version -cne ('machine-fabric ' + $plan.version)) { throw 'Installed version differs' }
  $null = Get-Services $plan $installation.installedBinary
  $receipt.phase='succeeded'; $receipt.installedSha256=$installation.installedSha256
} catch {
  $receipt.phase='failed'; $receipt.error=$_.Exception.Message
  # Only an attempted installation could have stopped a service. Never
  # restart services for a failed admission check or replay an installation.
  if ($receipt.PSObject.Properties.Name -contains 'installerAttempted' -and $receipt.installerAttempted) {
    $oldPaths = Get-ExpectedPaths $plan $receipt.oldBinary
    $candidatePaths = Get-ExpectedPaths $plan $candidateBinary
    foreach ($name in @('MachineFabricController','MachineFabricExecutor')) {
      try {
        $service = Get-CimInstance Win32_Service -Filter ("Name='" + $name + "'")
        if (-not $service -or ($service.PathName -cne $oldPaths[$name] -and $service.PathName -cne $candidatePaths[$name])) { throw 'Service definition changed; recovery refused' }
        if ((Get-Service $name -ErrorAction Stop).Status -ne 'Running') { Start-Service $name -ErrorAction Stop }
        $receipt.recovery += @{service=$name;state=[string](Get-Service $name -ErrorAction Stop).Status}
      } catch { $receipt.recovery += @{service=$name;error=$_.Exception.Message} }
    }
  }
} finally {
  $receipt.finishedAt=[DateTime]::UtcNow.ToString('o'); Save-Receipt $receipt
}
if ($receipt.phase -ne 'succeeded') { exit 1 }
