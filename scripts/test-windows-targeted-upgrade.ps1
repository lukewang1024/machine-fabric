$ErrorActionPreference = 'Stop'
$tokens=$null; $parseErrors=$null
$source = Join-Path $PSScriptRoot 'windows-targeted-upgrade.ps1'
$tree = [System.Management.Automation.Language.Parser]::ParseFile($source,[ref]$tokens,[ref]$parseErrors)
if ($parseErrors.Count) { throw ($parseErrors | Out-String) }
foreach ($name in @('Save-Receipt','Quote-Fabric','Get-ExpectedPaths','Assert-Plan','Get-Services','Invoke-LocalFabric','Assert-Quiescent')) {
  $function = $tree.Find({param($node) $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $name}, $true)
  if (-not $function) { throw "Missing helper $name" }
  Invoke-Expression $function.Extent.Text
}
function Expect-Failure([scriptblock]$Call) {
  $failed=$false
  try { & $Call } catch { $failed=$true }
  if (-not $failed) { throw 'Expected fail-closed preflight' }
}

$oldProgramData=$env:ProgramData
$oldProgramFiles=$env:ProgramFiles
$temporary=Join-Path ([IO.Path]::GetTempPath()) ('fabric-upgrade-test-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path (Join-Path $temporary 'machine-fabric') -Force | Out-Null
try {
  $env:ProgramData=$temporary
  $env:ProgramFiles=$temporary
  $stage=$temporary
  $receipt=@{transactionId='test-transaction';phase='prepared';installerAttempted=$false;version=$null;installedSha256=$null;error=$null;finishedAt=$null}
  Save-Receipt $receipt
  $receipt=Get-Content -LiteralPath (Join-Path $stage 'receipt.json') -Raw | ConvertFrom-Json
  $receipt.phase='installing'; $receipt.installerAttempted=$true; $receipt.version='machine-fabric 0.1.91'; $receipt.error='diagnostic'; $receipt.finishedAt='now'; Save-Receipt $receipt
  $readback=Get-Content -LiteralPath (Join-Path $stage 'receipt.json') -Raw | ConvertFrom-Json
  if (-not $readback.installerAttempted -or $readback.phase -ne 'installing' -or $readback.version -ne 'machine-fabric 0.1.91') { throw 'Receipt state did not persist' }
  $plan=[pscustomobject]@{version='0.1.91';nodeId='fixture';maintenanceOwner='owner';archiveSha256=('a'*64);previousBinarySha256=('b'*64);policyHome='C:\Users\Admin';allowRoots=@('C:/Users','D:/work root');managedPathMappings=@('C:/Users/Admin/AppData/Local/doubao-office-dev=D:/mapped')}
  Assert-Plan $plan
  $invalid=$plan.PSObject.Copy(); $invalid.allowRoots=@('relative'); Expect-Failure { Assert-Plan $invalid }
  $invalid=$plan.PSObject.Copy(); $invalid.nodeId='bad node'; Expect-Failure { Assert-Plan $invalid }
  $invalid=$plan.PSObject.Copy(); $invalid.archiveSha256='bad'; Expect-Failure { Assert-Plan $invalid }
  $paths=Get-ExpectedPaths $plan 'C:\Program Files\fabric\machine-fabric.exe'
  if ($paths.MachineFabricExecutor -notlike '*--allow-root "D:/work root"*' -or $paths.MachineFabricExecutor -notlike '*--managed-path-mapping*') { throw 'Service policy arguments were not preserved' }
  $binary='C:\Program Files\fabric\machine-fabric.exe'
  $script:fakeServices=@([pscustomobject]@{Name='MachineFabricController';State='Running';StartMode='Auto';StartName='LocalSystem';ProcessId=1;PathName=$paths.MachineFabricController},[pscustomobject]@{Name='MachineFabricExecutor';State='Running';StartMode='Auto';StartName='LocalSystem';ProcessId=2;PathName=$paths.MachineFabricExecutor})
  $script:fakeHelpers=@()
  function Get-CimInstance {
    param([string]$ClassName,[string]$Filter)
    if ($ClassName -eq 'Win32_Service') { return $script:fakeServices }
    if ($Filter) { return [pscustomobject]@{ExecutablePath=$binary} }
    return $script:fakeHelpers
  }
  $null=Get-Services $plan $binary
  $script:fakeServices[1].PathName += ' --unexpected-option'; Expect-Failure { $null=Get-Services $plan $binary }
  $script:fakeServices[1].PathName=$paths.MachineFabricExecutor
  $state=Join-Path $temporary 'machine-fabric'
  $queue=@{maintenanceOwner='owner';blocked=$false;inFlight=$false;safePoint=$true;jobs=@()}
  $controller=@{tasks=@();leases=@()}
  $fakeCli=Join-Path $temporary 'fake-cli.ps1'
  @'
$global:LASTEXITCODE=0
if ($args -contains 'desktop.list') { Write-Output $env:FABRIC_TEST_QUEUE }
elseif ($args -contains 'status') { Write-Output $env:FABRIC_TEST_CONTROLLER }
elseif ($args -contains 'task.list') {
  $index=[array]::IndexOf($args,'--params-file'); $filter=Get-Content -Raw -LiteralPath $args[$index+1] | ConvertFrom-Json
  $controller=$env:FABRIC_TEST_CONTROLLER | ConvertFrom-Json
  $found=@($controller.result.tasks | Where-Object { $_.state -eq $filter.state })
  @{ok=$true;result=@{total=$found.Count;tasks=$found}} | ConvertTo-Json -Compress -Depth 8
}
elseif ($args -contains 'process.list') { Write-Output $env:FABRIC_TEST_PROCESSES }
else { Write-Output $env:FABRIC_TEST_AVAILABILITY }
'@ | Set-Content -LiteralPath $fakeCli -Encoding UTF8
  $env:FABRIC_TEST_AVAILABILITY='{"ok":true,"result":{"active":0,"queued":0,"heldResourceCount":0}}'
  $env:FABRIC_TEST_PROCESSES='{"ok":true,"result":{"processes":[]}}'
  function Save-State {
    $env:FABRIC_TEST_QUEUE=@{ok=$true;result=$queue} | ConvertTo-Json -Compress -Depth 8
    $env:FABRIC_TEST_CONTROLLER=@{ok=$true;result=$controller} | ConvertTo-Json -Compress -Depth 8
  }
  Save-State; Assert-Quiescent $plan $fakeCli
  $queue.jobs=@(@{state='queued'}); Save-State; Expect-Failure { Assert-Quiescent $plan $fakeCli }; $queue.jobs=@()
  $queue.maintenanceOwner='someone-else'; Save-State; Expect-Failure { Assert-Quiescent $plan $fakeCli }; $queue.maintenanceOwner='owner'
  $queue.Remove('blocked'); Save-State; Expect-Failure { Assert-Quiescent $plan $fakeCli }; $queue.blocked=$false
  $controller.tasks=@(@{state='running'}); Save-State; Expect-Failure { Assert-Quiescent $plan $fakeCli }; $controller.tasks=@()
  $controller.leases=@(@{expiresAt=[DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()+60000}); Save-State; Expect-Failure { Assert-Quiescent $plan $fakeCli }; $controller.leases=@(); Save-State
  $controller.leases=@(@{expiresAt='not-a-time'}); Save-State; Expect-Failure { Assert-Quiescent $plan $fakeCli }; $controller.leases=@(); Save-State
  $env:FABRIC_TEST_AVAILABILITY='{"ok":true,"result":{"active":1,"queued":0,"heldResourceCount":1}}'; Expect-Failure { Assert-Quiescent $plan $fakeCli }; Assert-Quiescent $plan $fakeCli $true
  $env:FABRIC_TEST_AVAILABILITY='{"ok":true,"result":{"active":0,"queued":0}}'; Expect-Failure { Assert-Quiescent $plan $fakeCli }
  $env:FABRIC_TEST_AVAILABILITY='{"ok":true,"result":{"active":0,"queued":0,"heldResourceCount":0}}'; $env:FABRIC_TEST_PROCESSES='{"ok":true,"result":{"processes":[{"state":"running"}]}}'; Expect-Failure { Assert-Quiescent $plan $fakeCli }; $env:FABRIC_TEST_PROCESSES='{"ok":true,"result":{"processes":[]}}'
  $script:fakeHelpers=@([pscustomobject]@{Name='computer-use.exe';CommandLine=''}); $env:FABRIC_TEST_AVAILABILITY='{"ok":true,"result":{"active":0,"queued":0,"heldResourceCount":0}}'; Expect-Failure { Assert-Quiescent $plan $fakeCli }
  Remove-Item Function:\Get-CimInstance

  # Prove that the native scheduler runs an independently submitted job after
  # its submitting PowerShell process has exited; never touch Fabric services.
  $task='MachineFabricUpgradeTest-' + [guid]::NewGuid().ToString('N')
  $marker=Join-Path $temporary 'scheduler-marker.txt'
  $worker=Join-Path $temporary 'scheduler-worker.ps1'
  $submit=Join-Path $temporary 'scheduler-submit.ps1'
  @'
param($Marker)
Start-Sleep -Seconds 3
[IO.File]::WriteAllText($Marker,'finished')
'@ | Set-Content -LiteralPath $worker -Encoding UTF8
  @'
param($TaskName,$Worker,$Marker)
$action=New-ScheduledTaskAction -Execute 'powershell.exe' -Argument ('-NoProfile -NonInteractive -File "' + $Worker + '" -Marker "' + $Marker + '"')
$principal=New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
Register-ScheduledTask -TaskName $TaskName -Action $action -Principal $principal -Force | Out-Null
Start-ScheduledTask -TaskName $TaskName
'@ | Set-Content -LiteralPath $submit -Encoding UTF8
  try {
    & powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $submit $task $worker $marker
    if ($LASTEXITCODE -ne 0) { throw 'Scheduler submission failed' }
    $deadline=(Get-Date).AddSeconds(45)
    while (!(Test-Path -LiteralPath $marker) -and (Get-Date) -lt $deadline) { Start-Sleep -Milliseconds 500 }
    if (!(Test-Path -LiteralPath $marker) -or (Get-Content -LiteralPath $marker -Raw) -ne 'finished') { throw 'Independent scheduled worker did not complete' }
  } finally { Unregister-ScheduledTask -TaskName $task -Confirm:$false -ErrorAction SilentlyContinue }
  Write-Output 'Targeted upgrade preflight and independent scheduler tests passed'
} finally {
  $env:ProgramData=$oldProgramData
  $env:ProgramFiles=$oldProgramFiles
  Remove-Item Env:\FABRIC_TEST_AVAILABILITY -ErrorAction SilentlyContinue
  Remove-Item Env:\FABRIC_TEST_PROCESSES -ErrorAction SilentlyContinue
  Remove-Item Env:\FABRIC_TEST_QUEUE -ErrorAction SilentlyContinue
  Remove-Item Env:\FABRIC_TEST_CONTROLLER -ErrorAction SilentlyContinue
  Remove-Item -LiteralPath $temporary -Recurse -Force -ErrorAction SilentlyContinue
}
