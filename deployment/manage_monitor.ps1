param([ValidateSet('Install', 'Start', 'Stop', 'Disable', 'Status')][string]$Action = 'Start')
$ErrorActionPreference = 'Stop'
$taskName = 'AH-Taoli-Monitor'
$projectRoot = Split-Path -Parent $PSScriptRoot
$task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue

function Stop-ProjectMonitor {
    if ($task) { Stop-ScheduledTask -TaskName $taskName }
    # Task Scheduler can leave children of the venv launcher running.
    # Match the full project path before terminating any process tree.
    $supervisorPath = Join-Path $PSScriptRoot 'run_monitor.py'
    $appPath = Join-Path $projectRoot 'app.py'
    $processes = Get-CimInstance Win32_Process | Where-Object {
        $_.Name -like 'python*' -and (
            $_.CommandLine -like ('*"' + $supervisorPath + '"*') -or
            ($_.CommandLine -like '*-m streamlit run*' -and $_.CommandLine -like ('*"' + $appPath + '"*'))
        )
    }
    foreach ($process in $processes) {
        # Recheck the PID in case its parent already terminated it.
        $current = Get-CimInstance Win32_Process -Filter "ProcessId=$($process.ProcessId)"
        if ($current -and $current.CommandLine -eq $process.CommandLine) {
            & taskkill.exe /PID $process.ProcessId /T /F | Out-Null
        }
    }
}

if ($Action -eq 'Install' -or ($Action -eq 'Start' -and -not $task)) {
    $python = Join-Path $projectRoot '.venv\Scripts\pythonw.exe'
    if (-not (Test-Path -LiteralPath $python)) { throw "Missing Python runtime: $python" }
    $user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
    $taskAction = New-ScheduledTaskAction -Execute $python -Argument ('"' + (Join-Path $PSScriptRoot 'run_monitor.py') + '"') -WorkingDirectory $projectRoot
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
    $principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
    $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -DontStopOnIdleEnd -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1)
    Register-ScheduledTask -TaskName $taskName -Action $taskAction -Trigger $trigger -Principal $principal -Settings $settings -Description 'Local A/H dashboard: start at Windows logon and restart after process exits.' -Force | Out-Null
}

switch ($Action) {
    { $_ -in 'Install', 'Start' } {
        Enable-ScheduledTask -TaskName $taskName | Out-Null
        Start-ScheduledTask -TaskName $taskName
        Write-Host 'Dashboard started: http://127.0.0.1:8510'
    }
    'Stop' {
        Stop-ProjectMonitor
        Write-Host 'Dashboard stopped. It will start again at the next Windows logon.'
    }
    'Disable' {
        if ($task) {
            Disable-ScheduledTask -TaskName $taskName | Out-Null
        }
        Stop-ProjectMonitor
        Write-Host 'Dashboard stopped and automatic startup disabled.'
    }
    'Status' {
        if ($task) {
            $task | Select-Object TaskName, State
            Get-ScheduledTaskInfo -TaskName $taskName | Select-Object LastRunTime, LastTaskResult
        } else { Write-Host 'Automatic startup is not installed.' }
    }
}
