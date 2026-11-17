$ErrorActionPreference = 'Stop'
$pulselakeRoot = Split-Path -Parent $PSScriptRoot
& "$pulselakeRoot\.venv\Scripts\python.exe" "$PSScriptRoot\run_dbt.py" @args
if ($LASTEXITCODE -ne 0) { throw 'dbt runner failed. See output above.' }
