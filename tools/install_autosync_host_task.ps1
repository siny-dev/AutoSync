[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('install','remove','start','stop','status')]
    [string]$Mode,
    [string]$TaskName = 'Siny-AutoSyncHost',
    [string]$ConfigPath = (Join-Path $env:LOCALAPPDATA 'Siny\AutoSync\host\host-config.json'),
    [string]$PythonExecutable = '',
    [switch]$StartNow
)

$ErrorActionPreference = 'Stop'
$launcher = Join-Path $PSScriptRoot 'autosync_host_launcher.py'
if (-not (Test-Path -LiteralPath $launcher -PathType Leaf)) {
    throw "AutoSync stable launcher is missing: $launcher"
}

$taskDescription = 'Starts the AutoUpdate-verified AutoSync host for the signed-in user.'

function Assert-AutoSyncTask($Task) {
    if ($Task -and $Task.Description -ne $taskDescription) {
        throw "Refusing to modify an unrelated scheduled task named $TaskName."
    }
}

function Get-CurrentUserId {
    $process = [Diagnostics.Process]::GetCurrentProcess()
    if ($process.SessionId -eq 0) {
        throw 'AutoSync host task must be installed from the signed-in interactive desktop session.'
    }
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $userId = [string]$identity.Name
    if ([string]::IsNullOrWhiteSpace($userId) -or $userId -notmatch '^[^\\]+\\[^\\]+$' -or $userId -match '^(NT AUTHORITY|NT SERVICE)\\') {
        if ($identity.User) {
            try { $userId = [string]$identity.User.Translate([Security.Principal.NTAccount]).Value } catch { $userId = '' }
        }
    }
    if ([string]::IsNullOrWhiteSpace($userId) -or $userId -notmatch '^[^\\]+\\[^\\]+$' -or $userId -match '^(NT AUTHORITY|NT SERVICE)\\') {
        throw 'Could not resolve a valid signed-in interactive user for the AutoSync host task.'
    }
    return $userId
}

switch ($Mode) {
    'install' {
        if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
            throw "Host config is missing: $ConfigPath"
        }
        $hostConfig = Get-Content -Raw -LiteralPath $ConfigPath | ConvertFrom-Json
        if ([string]::IsNullOrWhiteSpace($PythonExecutable)) {
            $PythonExecutable = [string]$hostConfig.python_executable
            if ([string]::IsNullOrWhiteSpace($PythonExecutable)) {
                $PythonExecutable = (Get-Command python.exe -ErrorAction Stop).Source
            }
        }
        if (-not (Test-Path -LiteralPath $PythonExecutable -PathType Leaf)) {
            throw "Python executable is missing: $PythonExecutable"
        }
        $captureId = [guid]::NewGuid().ToString('N')
        $validationStdout = Join-Path $env:TEMP ('autosync-config-' + $captureId + '.out')
        $validationStderr = Join-Path $env:TEMP ('autosync-config-' + $captureId + '.err')
        $quotedLauncher = '"' + $launcher + '"'
        $quotedConfig = '"' + $ConfigPath + '"'
        try {
            $validationProcess = Start-Process -FilePath $PythonExecutable -ArgumentList @('-B', $quotedLauncher, '--config', $quotedConfig, '--validate-config') -WindowStyle Hidden -PassThru -Wait -RedirectStandardOutput $validationStdout -RedirectStandardError $validationStderr
            $validationExitCode = [int]$validationProcess.ExitCode
            if ($validationExitCode -ne 0) {
                $diagnostic = ''
                if (Test-Path -LiteralPath $validationStderr -PathType Leaf) {
                    $diagnostic = (Get-Content -Raw -LiteralPath $validationStderr -ErrorAction SilentlyContinue).Trim()
                    if ($diagnostic.Length -gt 240) { $diagnostic = $diagnostic.Substring(0,240) }
                }
                throw "AutoSync host config validation failed (exit code $validationExitCode); the task was not installed. $diagnostic"
            }
        } finally {
            foreach ($capturePath in @($validationStdout,$validationStderr)) {
                if (Test-Path -LiteralPath $capturePath -PathType Leaf) {
                    Remove-Item -LiteralPath $capturePath -Force
                }
            }
        }
        $existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
        Assert-AutoSyncTask -Task $existing
        $userId = Get-CurrentUserId
        $argumentLine = '-B "{0}" --config "{1}"' -f $launcher, $ConfigPath
        $action = New-ScheduledTaskAction -Execute $PythonExecutable -Argument $argumentLine -WorkingDirectory $PSScriptRoot
        $trigger = New-ScheduledTaskTrigger -AtLogOn -User $userId
        $principal = New-ScheduledTaskPrincipal -UserId $userId -LogonType Interactive -RunLevel Limited
        $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::Zero)
        try {
            Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Description $taskDescription -Force | Out-Null
        } catch {
            if ($_.Exception.HResult -eq -2147024891 -or $_.Exception.Message.IndexOf('access is denied', [StringComparison]::OrdinalIgnoreCase) -ge 0) {
                throw 'Task Scheduler denied registration for the signed-in user (0x80070005). No task was installed. Run this installer in a Windows session permitted to create the per-user Interactive/Limited task, or have the Windows administrator review Task Scheduler policy or ACL. The installer will not switch accounts or elevate.'
            }
            throw
        }
        if ($StartNow) {
            Start-ScheduledTask -TaskName $TaskName
        }
        Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State, @{Name='RunAsUser';Expression={$_.Principal.UserId}}
    }
    'remove' {
        $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
        Assert-AutoSyncTask -Task $task
        if ($task) {
            if ($task.State -eq 'Running') {
                Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
            }
            Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        }
        [pscustomobject]@{TaskName=$TaskName;Status='removed'}
    }
    'start' {
        $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
        Assert-AutoSyncTask -Task $task
        Start-ScheduledTask -TaskName $TaskName
        Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName,State
    }
    'stop' {
        $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
        Assert-AutoSyncTask -Task $task
        Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
        Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName,State
    }
    'status' {
        $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
        Assert-AutoSyncTask -Task $task
        $info = Get-ScheduledTaskInfo -TaskName $TaskName
        [pscustomobject]@{
            TaskName = $task.TaskName
            State = $task.State
            RunAsUser = $task.Principal.UserId
            LastRunTime = $info.LastRunTime
            LastTaskResult = ('0x{0:X8}' -f [uint32]$info.LastTaskResult)
            NextRunTime = $info.NextRunTime
        }
    }
}
