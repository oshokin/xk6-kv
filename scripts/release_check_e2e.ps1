#!/usr/bin/env pwsh
# PowerShell script to run release-check E2E suite with a timestamped artifact file.
# The script writes full E2E output to tmp/e2e/release-check-<timestamp>.log,
# highlights failures by lines containing the "✗" symbol, and keeps the
# artifact for post-run inspection.

# Stop execution on any error.
$ErrorActionPreference = "Stop"

# Ensure artifact directory exists.
$artifactDir = "tmp/e2e"
New-Item -ItemType Directory -Force -Path $artifactDir | Out-Null

# Build timestamped artifact path.
$timestamp = Get-Date -Format "yyyy-MM-dd-HH-mm-ss"
$artifact = Join-Path $artifactDir ("release-check-" + $timestamp + ".log")

# Print artifact location before execution.
Write-Host ("release-check e2e artifact: " + $artifact)

# Run E2E suite and always capture output to the artifact.
& task test-e2e-all *> $artifact
$taskExitCode = $LASTEXITCODE

# Search for threshold/check failures in k6 output.
# The single required marker for release-check is "✗".
$failureMatches = Select-String -Path $artifact -Pattern "✗"

# Treat either non-zero task exit or any failure marker as a failed release-check.
if (($taskExitCode -ne 0) -or $failureMatches) {
    Write-Host ("x e2e release-check failed; artifact kept: " + $artifact)

    # Emit matching lines with "x" prefix for easier scanning in CI/terminal logs.
    foreach ($match in $failureMatches) {
        Write-Host ("x " + $match.Path + ":" + $match.LineNumber + ":" + $match.Line)
    }

    if (-not $failureMatches) {
        # Fallback diagnostic when task failed but no explicit "✗" line was found.
        Write-Host ("x task test-e2e-all exited with code " + $taskExitCode + " (no '✗' markers found)")
    }

    exit 1
}

# Success path: keep artifact for manual post-run analysis.
Write-Host ("e2e release-check passed; artifact kept: " + $artifact)
