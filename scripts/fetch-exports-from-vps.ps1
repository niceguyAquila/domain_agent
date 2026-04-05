# Download the VPS exports folder via SCP (recursive).
# Requires: OpenSSH Client (Windows optional feature) so `scp` is in PATH.
#
# Usage:
#   .\scripts\fetch-exports-from-vps.ps1 -VpsHost "203.0.113.10" -User "deploy"
#   .\scripts\fetch-exports-from-vps.ps1 -VpsHost "v.example.com" -User "deploy" -KeyFile "$env:USERPROFILE\.ssh\id_ed25519"
#
param(
    [Parameter(Mandatory = $true)]
    [string] $VpsHost,
    [Parameter(Mandatory = $true)]
    [string] $User,
    [string] $RemoteExportsDir = "/home/deploy/domain_agent/exports",
    [string] $LocalDir = "",
    [string] $KeyFile = ""
)

$ErrorActionPreference = "Stop"
if (-not $LocalDir) {
    $repoRoot = Split-Path $PSScriptRoot -Parent
    $LocalDir = Join-Path $repoRoot "exports-downloaded"
}

New-Item -ItemType Directory -Force -Path $LocalDir | Out-Null

$remote = "${User}@${VpsHost}:${RemoteExportsDir}/"
$scpArgs = @()
if ($KeyFile) {
    $scpArgs += @("-i", $KeyFile)
}
$scpArgs += @("-r", $remote, $LocalDir)

Write-Host "Running: scp $($scpArgs -join ' ')"
& scp @scpArgs
if ($LASTEXITCODE -ne 0) {
    throw "scp failed with exit code $LASTEXITCODE"
}
Write-Host "Done. Files in: $LocalDir"
