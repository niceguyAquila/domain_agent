@echo off
REM Edit VPS host and user, then double-click or run from cmd.
REM Uses PowerShell to call scp (OpenSSH). Install "OpenSSH Client" optional feature if scp is missing.

set VPS_HOST=YOUR_VPS_IP_OR_HOSTNAME
set VPS_USER=deploy

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0fetch-exports-from-vps.ps1" -VpsHost "%VPS_HOST%" -User "%VPS_USER%"
pause
