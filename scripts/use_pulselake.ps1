# Optional: dot-source to put project tools on PATH in this terminal.
$pulselakeRoot = Split-Path -Parent $PSScriptRoot
$env:PATH = "$pulselakeRoot\.tools\databricks;$pulselakeRoot\.venv\Scripts;" + $env:PATH
$env:DATABRICKS_CONFIG_PROFILE = 'pulselake'
$env:DBT_SEND_ANONYMOUS_USAGE_STATS = 'false'
# Python runners load connection details from ignored runtime/settings.json.
