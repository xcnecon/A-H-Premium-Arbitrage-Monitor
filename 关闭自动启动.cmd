@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0deployment\manage_monitor.ps1" -Action Disable
if errorlevel 1 pause
