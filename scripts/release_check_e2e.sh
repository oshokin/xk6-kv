#!/usr/bin/env bash
# Bash script to run release-check E2E suite with a timestamped artifact file.
# The script writes full E2E output to tmp/e2e/release-check-<timestamp>.log,
# highlights failures by lines containing the "✗" symbol, and keeps the
# artifact for post-run inspection.

# Exit immediately on error, treat unset variables as errors, and fail on pipe errors.
set -euo pipefail

# Ensure artifact directory exists.
artifact_dir="tmp/e2e"
mkdir -p "${artifact_dir}"

# Build timestamped artifact path.
timestamp="$(date +"%Y-%m-%d-%H-%M-%S")"
artifact_file="${artifact_dir}/release-check-${timestamp}.log"

# Print artifact location before execution.
echo "release-check e2e artifact: ${artifact_file}"

# Run E2E suite and always capture output to the artifact.
# Use set +e to inspect both exit code and output markers ourselves.
set +e
task test-e2e-all > "${artifact_file}" 2>&1
task_exit_code=$?
set -e

# Search for threshold/check failures in k6 output.
# The single required marker for release-check is "✗".
failure_lines="$(rg -n '✗' "${artifact_file}" || true)"

# Treat either non-zero task exit or any failure marker as a failed release-check.
if [ "${task_exit_code}" -ne 0 ] || [ -n "${failure_lines}" ]; then
  echo "x e2e release-check failed; artifact kept: ${artifact_file}"

  # Emit matching lines with "x" prefix for easier scanning in CI/terminal logs.
  if [ -n "${failure_lines}" ]; then
    while IFS= read -r line; do
      [ -n "${line}" ] && echo "x ${line}"
    done <<< "${failure_lines}"
  else
    # Fallback diagnostic when task failed but no explicit "✗" line was found.
    echo "x task test-e2e-all exited with code ${task_exit_code} (no '✗' markers found)"
  fi

  exit 1
fi

# Success path: keep artifact for manual post-run analysis.
echo "e2e release-check passed; artifact kept: ${artifact_file}"
