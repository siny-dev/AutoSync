param(
    [Parameter(Mandatory=$true)][string]$Server,
    [string]$CaFile,
    [string]$DataDirectory=(Join-Path $env:LOCALAPPDATA 'Siny\AutoSyncMemoDemo'),
    [string]$StateRoot=(Join-Path $env:LOCALAPPDATA 'Siny\AutoUpdate'),
    [string]$ModuleSourceRoot='\\H255\AI-Work\제작중인 프로그램\AutoUpdate\outbox\modules',
    [string]$PythonExecutable='python',
    [string]$DemoArgumentsJson='["sync"]'
)

$ErrorActionPreference='Stop'
$projectRoot=Split-Path -Parent $PSScriptRoot
$autoUpdateRoot=Join-Path (Split-Path -Parent $projectRoot) 'AutoUpdate'
$cli=Join-Path $autoUpdateRoot 'tools\autoupdate.ps1'
$manifest=Join-Path $PSScriptRoot 'app-modules.json'
$demo=Join-Path $PSScriptRoot 'memo_demo.py'
foreach($required in @($cli,$manifest,$demo)){
    if(-not (Test-Path -LiteralPath $required -PathType Leaf)){throw "Required AutoSync module-link file is missing: $required"}
}
if([string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)){throw 'LOCALAPPDATA is not available.'}

$powershell=(Get-Command powershell.exe -ErrorAction Stop).Source
$resolveArgs=@('-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',$cli,'-Command','resolve-modules',
    '-AppManifestPath',$manifest,'-ModuleSourceRoot',$ModuleSourceRoot,'-StateRoot',$StateRoot)
$start=New-Object System.Diagnostics.ProcessStartInfo
$start.FileName=$powershell
$start.Arguments=(($resolveArgs|ForEach-Object{'"'+([string]$_).Replace('"','\"')+'"'}) -join ' ')
$start.UseShellExecute=$false;$start.CreateNoWindow=$true;$start.RedirectStandardOutput=$true;$start.RedirectStandardError=$true
$process=New-Object System.Diagnostics.Process;$process.StartInfo=$start
if(-not $process.Start()){throw 'Could not start the AutoUpdate module resolver.'}
$stdoutTask=$process.StandardOutput.ReadToEndAsync();$stderrTask=$process.StandardError.ReadToEndAsync()
$process.WaitForExit();$resolveOutput=$stdoutTask.Result;$resolveError=$stderrTask.Result;$resolveExitCode=$process.ExitCode;$process.Dispose()
if($resolveExitCode -ne 0){throw "AutoUpdate could not resolve the required AutoSync module. $resolveError"}
try{$resolution=$resolveOutput|ConvertFrom-Json}catch{throw 'AutoUpdate returned invalid module-resolution JSON.'}
if($resolution.status -cne 'ready' -or $resolution.app_id -cne 'autosync-memo-demo'){
    throw 'AutoUpdate did not return a ready result for autosync-memo-demo.'
}
$module=@($resolution.modules|Where-Object{$_.module_id -ceq 'AutoSync' -and $_.api_major -eq 1})|Select-Object -First 1
if(-not $module -or -not $module.verified -or $module.delivery_level -ne 'L1'){
    throw 'The AutoSync module is not present as a verified AutoUpdate cache entry.'
}
$modulePath=[string]$module.module_path
if(-not (Test-Path -LiteralPath (Join-Path $modulePath 'src\__init__.py') -PathType Leaf)){
    throw 'The verified AutoSync module payload does not contain its Python client package.'
}
try{$parsedArguments=ConvertFrom-Json -InputObject $DemoArgumentsJson -ErrorAction Stop}catch{throw 'DemoArgumentsJson must be a JSON array of strings.'}
if($parsedArguments -is [Array]){$DemoArguments=[string[]]$parsedArguments}else{$DemoArguments=@([string]$parsedArguments)}
$nonStringArguments=@($DemoArguments|Where-Object{$_ -isnot [string]})
if($DemoArguments.Count -eq 0 -or $nonStringArguments.Count -gt 0){
    throw 'DemoArgumentsJson must be a non-empty JSON array of strings.'
}

$env:PYTHONPATH=$modulePath
$launchArgs=@('-B',$demo,'--server',$Server,'--data-dir',$DataDirectory)
if($CaFile){$launchArgs+=@('--ca-file',$CaFile)}
$launchArgs+=$DemoArguments
function ConvertTo-WindowsArgument([string]$Value) {
    if ($Value.Length -gt 0 -and $Value -notmatch '[\s"]') { return $Value }
    $builder = New-Object System.Text.StringBuilder
    [void]$builder.Append('"')
    $slashes = 0
    foreach ($character in $Value.ToCharArray()) {
        if ($character -eq [char]92) { $slashes++; continue }
        if ($character -eq [char]34) {
            [void]$builder.Append([string]::new([char]92, (2 * $slashes + 1)))
            [void]$builder.Append([char]34)
            $slashes = 0
            continue
        }
        if ($slashes -gt 0) { [void]$builder.Append([string]::new([char]92, $slashes)); $slashes = 0 }
        [void]$builder.Append($character)
    }
    if ($slashes -gt 0) { [void]$builder.Append([string]::new([char]92, 2 * $slashes)) }
    [void]$builder.Append('"')
    return $builder.ToString()
}

$start = New-Object System.Diagnostics.ProcessStartInfo
$start.FileName = $PythonExecutable
$start.Arguments = (($launchArgs | ForEach-Object { ConvertTo-WindowsArgument ([string]$_) }) -join ' ')
$start.UseShellExecute = $false
$start.CreateNoWindow = $true
$start.RedirectStandardOutput = $true
$start.RedirectStandardError = $true
$start.StandardOutputEncoding = [System.Text.Encoding]::UTF8
$start.StandardErrorEncoding = [System.Text.Encoding]::UTF8
$process = New-Object System.Diagnostics.Process
$process.StartInfo = $start
if (-not $process.Start()) { throw 'Could not start AutoSync memo app.' }
$stdoutTask = $process.StandardOutput.ReadToEndAsync()
$stderrTask = $process.StandardError.ReadToEndAsync()
$process.WaitForExit()
$stdout = $stdoutTask.Result
$stderr = $stderrTask.Result
$exitCode = $process.ExitCode
$process.Dispose()
if ($stdout) { [Console]::Out.Write($stdout) }
if ($stderr) { [Console]::Error.Write($stderr) }
exit $exitCode
