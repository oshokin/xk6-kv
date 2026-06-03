param(
    [Parameter(Mandatory = $true)]
    [string]$Scenario,

    [Parameter(Mandatory = $true)]
    [string]$K6Bin,

    [string]$Vus = "40",

    [string]$Iterations = "400"
)

$ErrorActionPreference = "Stop"

$scenarioFile = "e2e/$Scenario.js"

if (-not (Test-Path $scenarioFile)) {
    throw "E2E scenario not found: $scenarioFile"
}

if (-not (Test-Path $K6Bin)) {
    throw "k6 binary not found: $K6Bin. Run: task build-k6"
}

function Remove-IfExists {
    param([string[]]$Paths)

    foreach ($path in $Paths) {
        Remove-Item -Force -ErrorAction SilentlyContinue -Path $path
    }
}

function Remove-GeneratedExports {
    Remove-IfExists @(
        "tmp/api-output-validation-*.csv",
        "tmp/api-output-validation-*.jsonl",
        "tmp/export-csv-response-capture*.csv",
        "tmp/export-jsonl-portable-seed*.jsonl",
        "tmp/import-csv-portable-seed*.csv",
        "tmp/import-jsonl-portable-seed*.jsonl"
    )
}

function Clear-Scenario {
    Remove-IfExists @(
        "tmp/e2e/e2e-$Scenario.kv",
        "tmp/e2e/e2e-$Scenario.snapshot.kv"
    )

    Remove-GeneratedExports
}

function Clear-DiskDatabase {
    Remove-IfExists @(
        "tmp/e2e/e2e-$Scenario.kv"
    )
}

function Invoke-TestCase {
    param(
        [string]$Backend,
        [string]$TrackKeys
    )

    if ($Backend -eq "disk") {
        Clear-DiskDatabase
    }

    Write-Host "Testing $Scenario - $Backend backend, trackKeys=$TrackKeys"

    $env:KV_BACKEND = $Backend
    $env:KV_TRACK_KEYS = $TrackKeys
    $env:VUS = $Vus
    $env:ITERATIONS = $Iterations

    & $K6Bin run $scenarioFile

    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

Clear-Scenario

Invoke-TestCase -Backend "memory" -TrackKeys "true"
Invoke-TestCase -Backend "memory" -TrackKeys "false"
Invoke-TestCase -Backend "disk" -TrackKeys "true"
Invoke-TestCase -Backend "disk" -TrackKeys "false"

Clear-Scenario
