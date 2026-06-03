#!/usr/bin/env bash
set -euo pipefail

scenario="${1:-}"
k6_bin="${2:-}"
vus="${3:-40}"
iterations="${4:-400}"

if [[ -z "${scenario}" ]]; then
  echo "scenario name is required" >&2
  exit 1
fi

scenario_file="e2e/${scenario}.js"

if [[ ! -f "${scenario_file}" ]]; then
  echo "E2E scenario not found: ${scenario_file}" >&2
  exit 1
fi

if [[ -z "${k6_bin}" || ! -x "${k6_bin}" ]]; then
  echo "k6 binary not found or not executable: ${k6_bin}" >&2
  echo "Run: task build-k6" >&2
  exit 1
fi

remove_generated_exports() {
  rm -f \
    tmp/api-output-validation-*.csv \
    tmp/api-output-validation-*.jsonl \
    tmp/export-csv-response-capture*.csv \
    tmp/export-jsonl-portable-seed*.jsonl \
    tmp/import-csv-portable-seed*.csv \
    tmp/import-jsonl-portable-seed*.jsonl
}

clean_scenario() {
  rm -f \
    "tmp/e2e/e2e-${scenario}.kv" \
    "tmp/e2e/e2e-${scenario}.snapshot.kv"

  remove_generated_exports
}

clean_disk_db() {
  rm -f "tmp/e2e/e2e-${scenario}.kv"
}

run_case() {
  local backend="$1"
  local track_keys="$2"

  if [[ "${backend}" == "disk" ]]; then
    clean_disk_db
  fi

  echo "Testing ${scenario} - ${backend} backend, trackKeys=${track_keys}"

  KV_BACKEND="${backend}" \
  KV_TRACK_KEYS="${track_keys}" \
  VUS="${vus}" \
  ITERATIONS="${iterations}" \
    "${k6_bin}" run "${scenario_file}"
}

clean_scenario

run_case memory true
run_case memory false
run_case disk true
run_case disk false

clean_scenario
