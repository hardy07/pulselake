$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
if ($dockerCommand) {
    $dockerPath = $dockerCommand.Source
} else {
    $candidates = @(
        "$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin\docker.exe",
        "$env:ProgramFiles\Docker\Docker\resources\bin\docker.exe"
    )
    $dockerPath = $candidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
}
if (-not $dockerPath) { throw 'Docker was not found. Install Docker Desktop, then reopen PowerShell.' }
Write-Output "Docker CLI: $dockerPath"
& $dockerPath compose --project-directory $projectRoot -f "$projectRoot\compose.yaml" config --quiet
if ($LASTEXITCODE -ne 0) { throw 'Compose configuration failed validation.' }
& $dockerPath info --format 'Docker engine: {{.ServerVersion}} ({{.OSType}})'
if ($LASTEXITCODE -ne 0) { throw 'Open Docker Desktop and finish first-run setup. Wait for the engine to be running.' }
Write-Output 'Compose and Docker engine checks passed.'
