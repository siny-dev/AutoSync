param(
    [string]$Version,
    [string]$ModuleSourceRoot='\\H255\AI-Work\제작중인 프로그램\AutoUpdate\outbox\modules',
    [string]$StateRoot=(Join-Path ([IO.Path]::GetTempPath()) ('AutoSync-publish-state-'+[guid]::NewGuid().ToString('N')))
)
$ErrorActionPreference='Stop'
$projectRoot=Split-Path -Parent $PSScriptRoot
$autoUpdateRoot=Join-Path (Split-Path -Parent $projectRoot) 'AutoUpdate'
$builder=Join-Path $autoUpdateRoot 'tools\build-module-package.ps1'
$cli=Join-Path $autoUpdateRoot 'tools\autoupdate.ps1'
$init=Get-Content -LiteralPath (Join-Path $projectRoot 'src\__init__.py') -Raw
if(-not $Version){
    if($init -notmatch '(?m)^__version__\s*=\s*["''](?<version>\d+\.\d+\.\d+)["'']\s*$'){throw 'Could not read AutoSync semantic version from src/__init__.py.'}
    $Version=$Matches.version
}
if($Version -notmatch '^\d+\.\d+\.\d+$'){throw 'AutoSync module version must be semantic X.Y.Z.'}
foreach($required in @($builder,$cli)){
    if(-not (Test-Path -LiteralPath $required -PathType Leaf)){throw "AutoUpdate module tooling is missing: $required"}
}

$tempRoot=Join-Path ([IO.Path]::GetTempPath()) ('AutoSync-module-build-'+[guid]::NewGuid().ToString('N'))
$sourceRoot=Join-Path $tempRoot 'source'
$packageRoot=Join-Path $tempRoot 'package'
$srcRoot=Join-Path $sourceRoot 'src'
[void][IO.Directory]::CreateDirectory($srcRoot)
try{
    foreach($name in @(
        '__init__.py',
        'autosync_client.py',
        'autosync_credentials.py',
        'autosync_firebase_auth.py',
        'autosync_firebase_rtdb.py',
        'autosync_firestore_partitioned.py',
        'autosync_firestore_store.py',
        'autosync_host.py',
        'autosync_maintenance.py',
        'autosync_protocol.py',
        'autosync_server.py'
    )){
        $from=Join-Path $projectRoot ('src\'+$name)
        if(-not (Test-Path -LiteralPath $from -PathType Leaf)){throw "AutoSync client source is missing: $name"}
        [IO.File]::Copy($from,(Join-Path $srcRoot $name),$false)
    }
    & $builder -ModuleId AutoSync -Version $Version -ApiMajor 1 -MinimumEngine '0.1.0' `
        -SourceRoot $sourceRoot -OutputRoot $packageRoot -ActivationMode restart | Out-Null
    try{
    $output=& $cli -Command publish-module -ModuleId AutoSync -PackageRoot $packageRoot -ModuleSourceRoot $ModuleSourceRoot -StateRoot $StateRoot
    $outputText=(@($output)|ForEach-Object{[string]$_}) -join [Environment]::NewLine
    try{$result=ConvertFrom-Json -InputObject $outputText -ErrorAction Stop}catch{throw 'AutoUpdate returned invalid publication JSON.'}
}catch{
    throw
}
    if($result.ModuleId -cne 'AutoSync' -or $result.Version -cne $Version -or $result.Status -notin @('published','already-published')){
        throw "AutoSync module publication did not succeed: $($result.Status) $($result.Message)"
    }
    $result | ConvertTo-Json -Depth 8 -Compress
}finally{
    $resolved=[IO.Path]::GetFullPath($tempRoot)
    $tempPrefix=[IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\')+'\'
    if(-not $resolved.StartsWith($tempPrefix,[StringComparison]::OrdinalIgnoreCase) -or -not (Split-Path -Leaf $resolved).StartsWith('AutoSync-module-build-')){
        throw 'Refusing unsafe temporary build cleanup.'
    }
    if(Test-Path -LiteralPath $resolved){Remove-Item -LiteralPath $resolved -Recurse -Force}
    if((Split-Path -Leaf $StateRoot).StartsWith('AutoSync-publish-state-') -and (Test-Path -LiteralPath $StateRoot)){
        $stateResolved=[IO.Path]::GetFullPath($StateRoot)
        if($stateResolved.StartsWith($tempPrefix,[StringComparison]::OrdinalIgnoreCase)){Remove-Item -LiteralPath $stateResolved -Recurse -Force}
    }
}
