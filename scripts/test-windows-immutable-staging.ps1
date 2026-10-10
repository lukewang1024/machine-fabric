$ErrorActionPreference='Stop'
$tokens=$null;$parseErrors=$null
$ast=[System.Management.Automation.Language.Parser]::ParseFile(
  (Join-Path $PSScriptRoot 'install-windows.ps1'),[ref]$tokens,[ref]$parseErrors)
if ($parseErrors.Count) { throw 'native installer parse failed' }
$functions=@($ast.FindAll({param($node)
  $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Stage-ImmutableBinary'
},$false))
if ($functions.Count -ne 1) { throw 'immutable staging function missing' }
. ([scriptblock]::Create($functions[0].Extent.Text))
$selection=@($ast.EndBlock.Statements | Where-Object {
  $_ -is [System.Management.Automation.Language.AssignmentStatementAst] -and
  $_.Left.Extent.Text -eq '$installedBinary' -and $_.Right.Extent.Text -match '^Stage-ImmutableBinary\b'
})
if ($selection.Count -ne 1) { throw 'fresh and repeated installation must select the immutable path unconditionally' }
$root=Join-Path ([IO.Path]::GetTempPath()) ('machine-fabric-staging-test-'+[guid]::NewGuid().ToString('N'))
$reader=$null
try {
  New-Item -ItemType Directory -Path $root | Out-Null
  $source=Join-Path $root 'source.exe'
  [IO.File]::WriteAllBytes($source,[Text.Encoding]::UTF8.GetBytes('release executable fixture'))
  $install=Join-Path $root 'fresh-install'
  $digest=(Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash.ToLowerInvariant()
  $first=Stage-ImmutableBinary -Source $source -Root $install
  $expected=Join-Path (Join-Path (Join-Path $install 'versions') $digest) 'machine-fabric.exe'
  if ($first -ne $expected -or (Test-Path (Join-Path $install 'machine-fabric.exe'))) { throw 'fresh install used legacy path' }
  $written=(Get-Item -LiteralPath $first).LastWriteTimeUtc
  # Deny writes to the running candidate. Reuse must work without copying it.
  $reader=[IO.File]::Open($first,[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::Read)
  $second=Stage-ImmutableBinary -Source $source -Root $install
  if ($second -ne $first -or (Get-Item -LiteralPath $second).LastWriteTimeUtc -ne $written) { throw 'reapply changed immutable executable path or contents' }
  $reader.Dispose();$reader=$null
  [IO.File]::WriteAllBytes($first,[Text.Encoding]::UTF8.GetBytes('corrupt cached executable'))
  $rejected=$false
  try { Stage-ImmutableBinary -Source $source -Root $install | Out-Null } catch {
    if ($_.Exception.Message -notmatch 'immutable binary digest mismatch') { throw }
    $rejected=$true
  }
  if (-not $rejected) { throw 'corrupt candidate was silently reused or repaired' }
  Write-Output 'Windows immutable fresh/reapply staging checks passed'
} finally {
  if ($reader) { $reader.Dispose() }
  if (Test-Path -LiteralPath $root) { Remove-Item -LiteralPath $root -Recurse -Force }
}
