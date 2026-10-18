#!/usr/bin/env bash
# Smoke-test the running API: every endpoint returns the expected status code.
# Usage: scripts/smoke_api.sh [base_url]   (default http://127.0.0.1:5080)
set -euo pipefail
BASE="${1:-http://127.0.0.1:5080}"
fail=0

check() {  # check <expected_status> <path> [jq-free grep pattern the body must contain]
  local want="$1" path="$2" pattern="${3:-}"
  local body code
  body="$(curl -s -w '\n%{http_code}' "$BASE$path")"
  code="${body##*$'\n'}"; body="${body%$'\n'*}"
  if [[ "$code" != "$want" ]] || { [[ -n "$pattern" ]] && ! grep -q -- "$pattern" <<<"$body"; }; then
    echo "FAIL $path -> $code (wanted $want${pattern:+, body containing $pattern})"; echo "  ${body:0:300}"; fail=1
  else
    echo "ok   $path -> $code"
  fi
}

check 200 /health '"status":"ok"'
check 200 /api/lines '"line_id":"L1"'
check 200 /api/lines/L1/status '"status":'
check 404 /api/lines/L9/status
check 400 "/api/lines/1%3BDROP/status"
check 200 "/api/alerts?lineId=L1" '"affected_lots"'
check 400 "/api/alerts?status=bogus"
check 200 "/api/shifts?lineId=L2" '"giveaway_cost_cad"'
check 200 "/api/signals/L1/oven_z3_c?minutes=30" '"ewma_dev"'
check 400 "/api/signals/L1/not_a_signal"
check 200 /api/data-quality '"rule"'
check 200 /api/handover '"figures_verified"'
check 200 /api/investigations '"root_cause"'
check 200 /api/investigations/L1-oven_z3_c-20261019T0809 '"steps"'
check 404 /api/investigations/L9-oven_z3_c-20261019T0809
check 400 "/api/investigations/x%3Bdrop"
check 200 /api/investigations/evaluation '"methods"'
check 200 "/api/signals/L2/belt_speed_mpm?minutes=30" '"ewma_dev"'

post() {  # post <expected_status> <path> <json> [pattern]
  local want="$1" path="$2" data="$3" pattern="${4:-}" body code
  body="$(curl -s -w '\n%{http_code}' -H 'Content-Type: application/json' -d "$data" "$BASE$path")"
  code="${body##*$'\n'}"; body="${body%$'\n'*}"
  if [[ "$code" != "$want" ]] || { [[ -n "$pattern" ]] && ! grep -q -- "$pattern" <<<"$body"; }; then
    echo "FAIL POST $path -> $code (wanted $want)"; echo "  ${body:0:300}"; fail=1
  else
    echo "ok   POST $path -> $code"
  fi
}
post 200 /api/investigations/L1-oven_z3_c-20261019T0809/review '{"decision":"accept"}' '"root_cause":"burner_fault"'
post 400 /api/investigations/L1-oven_z3_c-20261019T0809/review '{"decision":"override","root_cause":"gremlins"}'
check 200 /api/investigations/L1-oven_z3_c-20261019T0809 '"decision":"accept"'
check 200 / 'LinePulse'
exit $fail
