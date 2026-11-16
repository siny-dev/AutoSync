[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$SourceRoot,
    [Parameter(Mandatory=$true)][string]$CandidateRoot,
    [Parameter(Mandatory=$true)][ValidatePattern('^\d+\.\d+\.\d+$')][string]$Version,
    [ValidatePattern('^\d+\.\d+\.\d+$')][string]$MinimumEngine='0.1.0'
)
$ErrorActionPreference='Stop'

$moduleRoot=[IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$autoUpdateRoot=[IO.Path]::GetFullPath((Join-Path (Split-Path -Parent $moduleRoot) 'AutoUpdate'))
$reviewRoot=[IO.Path]::GetFullPath((Join-Path $autoUpdateRoot 'review-candidates')).TrimEnd('\')
$source=[IO.Path]::GetFullPath($SourceRoot)
$final=[IO.Path]::GetFullPath($CandidateRoot)
$builder=Join-Path $autoUpdateRoot 'tools\build-module-package.ps1'
$prefix=$reviewRoot+[IO.Path]::DirectorySeparatorChar

if(-not (Test-Path -LiteralPath $source -PathType Container)){throw "SourceRoot does not exist: $source"}
if(-not (Test-Path -LiteralPath $builder -PathType Leaf)){throw "Generic AutoUpdate module builder is missing: $builder"}
if(-not $final.StartsWith($prefix,[StringComparison]::OrdinalIgnoreCase)){throw 'CandidateRoot must be inside AutoUpdate\review-candidates.'}
if(Test-Path -LiteralPath $final){throw "CandidateRoot already exists; candidate outputs are immutable: $final"}

# Never package the active AutoSync checkout (including a parent or child of it).
$projectPrefix=$moduleRoot.TrimEnd('\')+[IO.Path]::DirectorySeparatorChar
$sourcePrefix=$source.TrimEnd('\')+[IO.Path]::DirectorySeparatorChar
if($source.Equals($moduleRoot,[StringComparison]::OrdinalIgnoreCase) -or
   $source.Equals((Join-Path $moduleRoot 'src'),[StringComparison]::OrdinalIgnoreCase) -or
   $source.StartsWith($projectPrefix,[StringComparison]::OrdinalIgnoreCase) -or
   $moduleRoot.StartsWith($sourcePrefix,[StringComparison]::OrdinalIgnoreCase)){
    throw 'SourceRoot overlaps the live AutoSync checkout. Copy a reviewed version into an isolated source tree first.'
}
if($source.Equals($final,[StringComparison]::OrdinalIgnoreCase) -or
   $source.StartsWith($final.TrimEnd('\')+[IO.Path]::DirectorySeparatorChar,[StringComparison]::OrdinalIgnoreCase) -or
   $final.StartsWith($source.TrimEnd('\')+[IO.Path]::DirectorySeparatorChar,[StringComparison]::OrdinalIgnoreCase)){
    throw 'SourceRoot and CandidateRoot must not contain one another.'
}

$initPath=Join-Path $source 'src\__init__.py'
if(-not (Test-Path -LiteralPath $initPath -PathType Leaf)){throw 'SourceRoot must contain src\__init__.py with a semantic module version.'}
$initText=[IO.File]::ReadAllText($initPath)
if($initText -notmatch '(?m)^__version__\s*=\s*["''](?<version>\d+\.\d+\.\d+)["'']\s*$'){
    throw 'Could not read semantic version from src\__init__.py.'
}
if($Matches.version -cne $Version){throw "Requested version $Version does not match source version $($Matches.version)."}

$parent=Split-Path -Parent $final
[void][IO.Directory]::CreateDirectory($parent)
$stage=Join-Path $parent ('.'+(Split-Path -Leaf $final)+'.staging-'+[guid]::NewGuid().ToString('N'))
$stageSource=Join-Path $stage 'source'
$package=Join-Path $stage 'package'
$moduleVersion=Join-Path $stage (Join-Path 'modules\AutoSync' (Join-Path ('versions\'+$Version) ''))
$excludedDirs=@('.git','.hg','.svn','__pycache__','.pytest_cache','.mypy_cache','.ruff_cache','.tox','.venv','venv','node_modules','cache','caches')
$excludedFiles=@('.DS_Store','Thumbs.db','desktop.ini','module.json','current.json','manifest.json')

try{
    [void][IO.Directory]::CreateDirectory($stageSource)
    $files=@(Get-ChildItem -LiteralPath $source -File -Recurse -Force|Sort-Object FullName)
    $copied=0
    foreach($file in $files){
        if(($file.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0){throw "Refusing a reparse-point source file: $($file.FullName)"}
        $relative=$file.FullName.Substring($source.Length).TrimStart([IO.Path]::DirectorySeparatorChar,[IO.Path]::AltDirectorySeparatorChar)
        $parts=@($relative -split '[\\/]')
        if(@($parts|Where-Object{$_.StartsWith('.') -or $excludedDirs -contains $_}).Count -gt 0){continue}
        if($file.Name -in $excludedFiles -or $file.Name -match '\.s2bak\.' -or
           $file.Name -match '\.(pyc|pyo|tmp|bak|orig|swp)$' -or $file.Name -match '\.cache$') {continue}
        $destination=Join-Path $stageSource $relative
        [void][IO.Directory]::CreateDirectory((Split-Path -Parent $destination))
        [IO.File]::Copy($file.FullName,$destination,$false)
        $copied++
    }
    if($copied -eq 0){throw 'No source files remained after excluding cache and backup artifacts.'}

    # Generic recursive AutoUpdate package builder; this wrapper never calls publish-module.
    & $builder -ModuleId AutoSync -Version $Version -ApiMajor 1 -MinimumEngine $MinimumEngine `
        -SourceRoot $stageSource -OutputRoot $package -ActivationMode restart | Out-Null

    [void][IO.Directory]::CreateDirectory($moduleVersion)
    [IO.File]::Copy((Join-Path $package 'module.json'),(Join-Path $moduleVersion 'module.json'),$false)
    [void][IO.Directory]::CreateDirectory((Join-Path $moduleVersion 'payload'))
    Get-ChildItem -LiteralPath (Join-Path $package 'payload') -File -Recurse|ForEach-Object{
        $relative=$_.FullName.Substring((Join-Path $package 'payload').Length).TrimStart([IO.Path]::DirectorySeparatorChar,[IO.Path]::AltDirectorySeparatorChar)
        $destination=Join-Path (Join-Path $moduleVersion 'payload') $relative
        [void][IO.Directory]::CreateDirectory((Split-Path -Parent $destination))
        [IO.File]::Copy($_.FullName,$destination,$false)
    }
    $manifest=Get-Content -LiteralPath (Join-Path $moduleVersion 'module.json') -Raw|ConvertFrom-Json
    if($manifest.module_id -cne 'AutoSync' -or $manifest.version -cne $Version -or $manifest.api_major -ne 1 -or $manifest.activation_mode -cne 'restart'){
        throw 'Built AutoSync module manifest does not satisfy the Workboard API 1/restart contract.'
    }
    foreach($entry in @($manifest.files)){
        $relative=$entry.path.Replace('/',[IO.Path]::DirectorySeparatorChar)
        $payloadFile=Join-Path (Join-Path $moduleVersion 'payload') $relative
        if(-not (Test-Path -LiteralPath $payloadFile -PathType Leaf) -or (Get-FileHash -LiteralPath $payloadFile -Algorithm SHA256).Hash.ToLowerInvariant() -cne $entry.sha256){
            throw "Candidate module payload hash verification failed: $($entry.path)"
        }
    }
    $manifestHash=(Get-FileHash -LiteralPath (Join-Path $moduleVersion 'module.json') -Algorithm SHA256).Hash.ToLowerInvariant()
    $pointer=[ordered]@{schema=1;module_id='AutoSync';version=$Version;api_major=1;manifest_sha256=$manifestHash;published_utc=[DateTime]::UtcNow.ToString('o')}
    $pointerPath=Join-Path (Join-Path $stage 'modules\AutoSync') 'current.json'
    $pointerTemp=$pointerPath+'.tmp'
    [IO.File]::WriteAllText($pointerTemp,($pointer|ConvertTo-Json -Depth 6),[Text.UTF8Encoding]::new($false))
    Move-Item -LiteralPath $pointerTemp -Destination $pointerPath

    [IO.Directory]::Move($stage,$final)
    [pscustomobject]@{Status='candidate-built';ModuleId='AutoSync';Version=$Version;ApiMajor=1;FileCount=@($manifest.files).Count;CandidateRoot=$final;PackageRoot=(Join-Path $final 'package');CandidateModuleRoot=(Join-Path $final 'modules\AutoSync')}|ConvertTo-Json -Compress
}catch{
    if(Test-Path -LiteralPath $stage){
        $resolved=[IO.Path]::GetFullPath($stage)
        $stagePrefix=$parent.TrimEnd('\')+[IO.Path]::DirectorySeparatorChar+'.'+(Split-Path -Leaf $final)+'.staging-'
        if($resolved.StartsWith($stagePrefix,[StringComparison]::OrdinalIgnoreCase)){Remove-Item -LiteralPath $resolved -Recurse -Force -ErrorAction SilentlyContinue}
    }
    throw
}
