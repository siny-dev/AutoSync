$ErrorActionPreference = 'Stop'
$global:AutoSyncTaskInstallRegisteredArgs = $null
$global:AutoSyncTaskInstallRegistered = $false
$global:AutoSyncTaskInstallStarted = 0
$global:AutoSyncTaskInstallDenied = $false

function Get-ScheduledTask {
    [CmdletBinding()]
    param([string]$TaskName)
    if (-not $global:AutoSyncTaskInstallRegistered) { return $null }
    [pscustomobject]@{
        TaskName = $TaskName
        Description = 'Starts the AutoUpdate-verified AutoSync host for the signed-in user.'
        State = if ($global:AutoSyncTaskInstallStarted -gt 0) { 'Running' } else { 'Ready' }
        Principal = $global:AutoSyncTaskInstallRegisteredArgs.Principal
    }
}
function Get-ScheduledTaskInfo {
    [CmdletBinding()]
    param([string]$TaskName)
    [pscustomobject]@{ LastRunTime = [datetime]::MinValue; LastTaskResult = 0; NextRunTime = [datetime]::MinValue }
}
function New-ScheduledTaskAction {
    [CmdletBinding()]
    param([string]$Execute,[string]$Argument,[string]$WorkingDirectory)
    [pscustomobject]@{ Execute=$Execute; Argument=$Argument; WorkingDirectory=$WorkingDirectory }
}
function New-ScheduledTaskTrigger {
    [CmdletBinding()]
    param([switch]$AtLogOn,[string]$User)
    [pscustomobject]@{ AtLogOn=[bool]$AtLogOn; User=$User }
}
function New-ScheduledTaskPrincipal {
    [CmdletBinding()]
    param([string]$UserId,[string]$LogonType,[string]$RunLevel)
    [pscustomobject]@{ UserId=$UserId; LogonType=$LogonType; RunLevel=$RunLevel }
}
function New-ScheduledTaskSettingsSet {
    [CmdletBinding()]
    param([switch]$StartWhenAvailable,[switch]$AllowStartIfOnBatteries,[switch]$DontStopIfGoingOnBatteries,[int]$RestartCount,[timespan]$RestartInterval,[string]$MultipleInstances,[timespan]$ExecutionTimeLimit)
    [pscustomobject]@{ StartWhenAvailable=[bool]$StartWhenAvailable; AllowStartIfOnBatteries=[bool]$AllowStartIfOnBatteries; DontStopIfGoingOnBatteries=[bool]$DontStopIfGoingOnBatteries; RestartCount=$RestartCount; RestartInterval=$RestartInterval; MultipleInstances=$MultipleInstances; ExecutionTimeLimit=$ExecutionTimeLimit }
}
function Register-ScheduledTask {
    [CmdletBinding()]
    param([string]$TaskName,[object]$Action,[object]$Trigger,[object]$Principal,[object]$Settings,[string]$Description,[switch]$Force)
    if ($global:AutoSyncTaskInstallDenied) { throw [UnauthorizedAccessException]::new('Access is denied.') }
    $global:AutoSyncTaskInstallRegisteredArgs = [pscustomobject]@{ TaskName=$TaskName; Action=$Action; Trigger=$Trigger; Principal=$Principal; Settings=$Settings; Description=$Description; Force=[bool]$Force }
    $global:AutoSyncTaskInstallRegistered = $true
}
function Start-ScheduledTask {
    [CmdletBinding()]
    param([string]$TaskName)
    $global:AutoSyncTaskInstallStarted++
}

$tempRoot = [IO.Path]::GetFullPath($env:TEMP).TrimEnd('\')
$testRoot = Join-Path $tempRoot ('AutoSyncTaskInstallTest-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $testRoot | Out-Null
$configPath = Join-Path $testRoot 'host-config.json'
$invalidConfigPath = Join-Path $testRoot 'invalid-host-config.json'
try {
    $python = (Get-Command python.exe -ErrorAction Stop).Source
    $config = [ordered]@{
        schema=1; contract='autosync-host-config-v1'
        auto_update_cli='C:\test\autoupdate.ps1'; module_source_root='C:\test\modules'
        state_root=(Join-Path $testRoot 'state'); app_manifest_path='C:\test\host-modules.json'
        update_policy_path='C:\test\update-policy.json'; python_executable=$python
        data_dir=(Join-Path $testRoot 'database'); backup_dir=(Join-Path $testRoot 'backups')
        log_file=(Join-Path $testRoot 'host.jsonl'); bind_host='127.0.0.1'; port=8765
        allow_lan=$false; credential_apps=@('workboard'); credential_target_prefix='AutoSync/server/'
        app_id='AutoSyncHost'; startup_timeout_seconds=45; drain_timeout_seconds=45
        tls_cert=$null; tls_key=$null
    }
    [IO.File]::WriteAllText($configPath,($config | ConvertTo-Json -Depth 5),[Text.UTF8Encoding]::new($false))
    $installer = Join-Path $PSScriptRoot '..\tools\install_autosync_host_task.ps1'
    $savedUserName=$env:USERNAME; $savedUserDomain=$env:USERDOMAIN; $savedComputerName=$env:COMPUTERNAME
    try {
        $env:USERNAME=''; $env:USERDOMAIN=''; $env:COMPUTERNAME=''
        & $installer -Mode install -TaskName 'Siny-AutoSyncHost' -ConfigPath $configPath -StartNow | Out-Null
    } finally {
        $env:USERNAME=$savedUserName; $env:USERDOMAIN=$savedUserDomain; $env:COMPUTERNAME=$savedComputerName
    }
    $expectedUser = [string][Security.Principal.WindowsIdentity]::GetCurrent().Name
    $task = $global:AutoSyncTaskInstallRegisteredArgs
    if (-not $global:AutoSyncTaskInstallRegistered -or $global:AutoSyncTaskInstallStarted -ne 1) { throw 'Install did not register and start exactly one managed task.' }
    if ($task.TaskName -ne 'Siny-AutoSyncHost' -or $task.Description -ne 'Starts the AutoUpdate-verified AutoSync host for the signed-in user.') { throw 'Unexpected task identity or ownership description.' }
    if ($task.Action.Execute -ne $python) { throw 'Installer did not use the Python executable declared in host config.' }
    if (-not $task.Trigger.AtLogOn -or $task.Trigger.User -ne $expectedUser) { throw "Task is not scoped to the current user logon." }
    if ($task.Principal.UserId -ne $expectedUser -or $task.Principal.RunLevel -ne 'Limited') { throw 'Task principal is not the current user at least privilege.' }
    if (-not $task.Action.Execute -or $task.Action.Argument -notmatch 'autosync_host_launcher\.py' -or $task.Action.Argument -notmatch [regex]::Escape($configPath)) { throw 'Task action does not use the stable launcher and validated config.' }
    if (-not $task.Settings.StartWhenAvailable -or -not $task.Settings.AllowStartIfOnBatteries -or -not $task.Settings.DontStopIfGoingOnBatteries -or $task.Settings.MultipleInstances -ne 'IgnoreNew' -or $task.Settings.ExecutionTimeLimit -ne [timespan]::Zero) { throw 'Task settings do not provide the expected persistent single-instance behavior.' }
    if ($task.Settings.RestartCount -ne 3 -or $task.Settings.RestartInterval -ne [timespan]::FromMinutes(1)) { throw 'Task crash-restart policy differs from the expected bounded retry.' }
    [pscustomobject]@{ Result='PASS'; Registered=$global:AutoSyncTaskInstallRegistered; Started=$global:AutoSyncTaskInstallStarted; RunAs=$task.Principal.UserId; RunLevel=$task.Principal.RunLevel; Trigger='CurrentUserLogon'; MultipleInstances=$task.Settings.MultipleInstances; ExecutionTimeLimitSeconds=$task.Settings.ExecutionTimeLimit.TotalSeconds } | ConvertTo-Json -Compress
    [IO.File]::WriteAllText($invalidConfigPath,'{"schema":1,"contract":"bad"}',[Text.UTF8Encoding]::new($false))
    $global:AutoSyncTaskInstallRegisteredArgs = $null
    $global:AutoSyncTaskInstallRegistered = $false
    $global:AutoSyncTaskInstallStarted = 0
$global:AutoSyncTaskInstallDenied = $false
    $validationFailed = $false
    try {
        & $installer -Mode install -TaskName 'Siny-AutoSyncHost' -ConfigPath $invalidConfigPath | Out-Null
    } catch {
        if ($_.Exception.Message -notmatch 'exit code [1-9]') { throw }
        $validationFailed = $true
    }
    if (-not $validationFailed) { throw 'A real Python child-process failure was not detected.' }
    if ($global:AutoSyncTaskInstallRegistered -or $global:AutoSyncTaskInstallStarted -ne 0) { throw 'A task was registered after child-process validation failed.' }
    [pscustomobject]@{Result='PASS';RealChildExitCodeDetected=$true;InvalidConfigRegistered=$global:AutoSyncTaskInstallRegistered}|ConvertTo-Json -Compress

    $global:AutoSyncTaskInstallRegisteredArgs = $null
    $global:AutoSyncTaskInstallRegistered = $false
    $global:AutoSyncTaskInstallStarted = 0
    $global:AutoSyncTaskInstallDenied = $true
    $registrationDenied = $false
    try {
        & $installer -Mode install -TaskName 'Siny-AutoSyncHost' -ConfigPath $configPath | Out-Null
    } catch {
        if ($_.Exception.Message -notmatch '0x80070005') { throw }
        $registrationDenied = $true
    } finally {
        $global:AutoSyncTaskInstallDenied = $false
    }
    if (-not $registrationDenied) { throw 'Task Scheduler access denial did not return an actionable error.' }
    if ($global:AutoSyncTaskInstallRegistered -or $global:AutoSyncTaskInstallStarted -ne 0) { throw 'A task was recorded after Task Scheduler denied registration.' }
    [pscustomobject]@{Result='PASS';AccessDeniedIsActionable=$registrationDenied;DeniedRegistrationCreatedTask=$global:AutoSyncTaskInstallRegistered}|ConvertTo-Json -Compress
}
finally {
    $resolved = (Resolve-Path -LiteralPath $testRoot).Path.TrimEnd('\')
    if (-not $resolved.StartsWith($tempRoot + '\',[StringComparison]::OrdinalIgnoreCase)) { throw "Refusing cleanup outside temp root: $resolved" }
    if (Test-Path -LiteralPath $invalidConfigPath -PathType Leaf) { Remove-Item -LiteralPath $invalidConfigPath }
    $items = @(Get-ChildItem -LiteralPath $resolved -Force)
    if ($items.Count -eq 1 -and $items[0].FullName -eq $configPath) {
        Remove-Item -LiteralPath $configPath
        Remove-Item -LiteralPath $resolved
    } elseif ($items.Count -eq 0) {
        Remove-Item -LiteralPath $resolved
    } else {
        throw 'Unexpected files remain in installer test temp root; refusing cleanup.'
    }
}