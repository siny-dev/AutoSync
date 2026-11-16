param(
    [string]$ExpectedComputerName = "H255",
    [switch]$SaveReportToSharedFolder
)

$ErrorActionPreference = "Stop"
$computer = Get-CimInstance Win32_ComputerSystem
$os = Get-CimInstance Win32_OperatingSystem
$python = Get-Command python -ErrorAction SilentlyContinue
$pythonInfo = $null
if ($python) {
    $pythonJson = & $python.Source -c "import json,platform,sqlite3,sys; print(json.dumps({'version':platform.python_version(),'executable':sys.executable,'pythonSupported':sys.version_info >= (3,9),'sqlite':sqlite3.sqlite_version,'sqliteVersionSupported':tuple(map(int,sqlite3.sqlite_version.split('.'))) >= (3,24,0)}))"
    if ($LASTEXITCODE -eq 0) { $pythonInfo = $pythonJson | ConvertFrom-Json }
}

$ipConfigurations = @(Get-CimInstance Win32_NetworkAdapterConfiguration -Filter "IPEnabled = TRUE" -ErrorAction SilentlyContinue | ForEach-Object {
    [pscustomobject]@{
        Interface = $_.Description
        DHCPEnabled = [bool]$_.DHCPEnabled
        IPv4 = @($_.IPAddress | Where-Object { $_ -match "^\d+\.\d+\.\d+\.\d+$" -and $_ -notlike "169.254.*" })
        Gateway = @($_.DefaultIPGateway | Where-Object { $_ -match "^\d+\.\d+\.\d+\.\d+$" })
    }
})
$sleepConfig = @(powercfg /q SCHEME_CURRENT SUB_SLEEP 2>$null)
$standbyStart = -1
for ($index = 0; $index -lt $sleepConfig.Count; $index++) {
    if ($sleepConfig[$index] -match "STANDBYIDLE") { $standbyStart = $index; break }
}
$standbyLines = @()
if ($standbyStart -ge 0) {
    for ($index = $standbyStart + 1; $index -lt $sleepConfig.Count; $index++) {
        if ($sleepConfig[$index] -match "현재 AC 전원 설정 색인|Current AC Power Setting Index|현재 DC 전원 설정 색인|Current DC Power Setting Index") {
            $standbyLines += $sleepConfig[$index]
            if ($standbyLines.Count -eq 2) { break }
        }
        if ($index -gt $standbyStart + 10) { break }
    }
}
$standbySeconds = @($standbyLines | ForEach-Object {
    if ($_ -match "0x([0-9A-Fa-f]+)") { [Convert]::ToInt64($Matches[1], 16) } else { $null }
})
$localAppData = $env:LOCALAPPDATA
$localDrive = if ($localAppData -match "^([A-Za-z]):") { $Matches[1] } else { "C" }
$drive = Get-PSDrive -Name $localDrive -ErrorAction SilentlyContinue

$report = [pscustomobject]@{
    checkedAt = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss K")
    computerName = $computer.Name
    expectedComputerName = $ExpectedComputerName
    computerNameMatches = ($computer.Name -eq $ExpectedComputerName)
    osCaption = $os.Caption
    osVersion = $os.Version
    architecture = $os.OSArchitecture
    python = $pythonInfo
    pythonFound = [bool]$pythonInfo
    pythonSupported = [bool]($pythonInfo -and $pythonInfo.pythonSupported)
    sqliteVersionSupported = [bool]($pythonInfo -and $pythonInfo.sqliteVersionSupported)
    localDataRoot = (Join-Path $localAppData "Siny\AutoSync\data")
    localDataDrive = $localDrive
    localDataDriveFreeBytes = if ($drive) { [int64]$drive.Free } else { $null }
    networkInterfaces = $ipConfigurations
    activePowerPlan = (powercfg /getactivescheme 2>$null | Out-String).Trim()
    sleepAfterAcSeconds = if ($standbySeconds.Count -gt 0) { $standbySeconds[0] } else { $null }
    sleepAfterDcSeconds = if ($standbySeconds.Count -gt 1) { $standbySeconds[1] } else { $null }
    sleepSettingRaw = $standbyLines
    note = "Read-only preflight. No service, firewall, certificate, credential, or power setting was changed."
}
$json = $report | ConvertTo-Json -Depth 6
$json
if ($SaveReportToSharedFolder) {
    $reportDirectory = Join-Path $PSScriptRoot "reports"
    New-Item -ItemType Directory -Path $reportDirectory -Force | Out-Null
    $safeName = $computer.Name -replace "[^A-Za-z0-9._-]", "_"
    $reportPath = Join-Path $reportDirectory ($safeName + "-preflight.json")
    [IO.File]::WriteAllText($reportPath, $json, [Text.UTF8Encoding]::new($false))
    Write-Output ("Saved report: " + $reportPath)
}
