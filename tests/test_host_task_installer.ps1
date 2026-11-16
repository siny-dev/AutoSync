$ErrorActionPreference = 'Stop'
$script:MutationCalls = 0
$installer = Join-Path $PSScriptRoot '..\tools\install_autosync_host_task.ps1'

function Get-ScheduledTask {
    [CmdletBinding()]
    param([string]$TaskName)
    [pscustomobject]@{
        TaskName = $TaskName
        Description = 'Unrelated test task'
        State = 'Running'
        Principal = [pscustomobject]@{ UserId = 'test-user' }
    }
}
function Get-ScheduledTaskInfo {
    [CmdletBinding()]
    param([string]$TaskName)
    $script:MutationCalls++
}
function Start-ScheduledTask {
    [CmdletBinding()]
    param([string]$TaskName)
    $script:MutationCalls++
}
function Stop-ScheduledTask {
    [CmdletBinding()]
    param([string]$TaskName)
    $script:MutationCalls++
}
function Unregister-ScheduledTask {
    [CmdletBinding()]
    param([string]$TaskName, [switch]$Confirm)
    $script:MutationCalls++
}

foreach ($mode in @('remove', 'start', 'stop', 'status')) {
    $blocked = $false
    try {
        & $installer -Mode $mode -TaskName 'Siny-AutoSyncHost' | Out-Null
    } catch {
        if ($_.Exception.Message -match 'Refusing to modify an unrelated scheduled task') {
            $blocked = $true
        } else {
            throw
        }
    }
    if (-not $blocked) {
        throw "Unrelated same-name task was not blocked for mode $mode."
    }
}
if ($script:MutationCalls -ne 0) {
    throw "The installer attempted $script:MutationCalls mutation(s) against an unrelated task."
}
[pscustomobject]@{
    UnrelatedTaskMutations = $script:MutationCalls
    GuardedModes = 'remove,start,stop,status'
    Result = 'PASS'
} | ConvertTo-Json -Compress
