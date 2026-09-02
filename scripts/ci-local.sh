#!/usr/bin/env bash
# Local replica of .github/workflows/build.yml.
#
# Exists because hosted Actions runs for this private repo currently fail before
# any step executes (billing block, see issue #78): every job reports steps=0
# after 3-6 seconds, so hosted red/green carries no information about the code.
# Run this before pushing to get a real verdict.
#
# Usage: scripts/ci-local.sh
# Exit code is non-zero if any gate fails.

set -uo pipefail
cd "$(dirname "$0")/.."

fail=0
run() {
	local label="$1"
	shift
	printf '\n=== %s ===\n' "$label"
	if "$@"; then
		printf '[PASS] %s\n' "$label"
	else
		printf '[FAIL] %s (rc=%d)\n' "$label" "$?"
		fail=1
	fi
}

expected_bun="$(grep -o 'bun-version: [0-9.]*' .github/workflows/build.yml | head -1 | awk '{print $2}')"
actual_bun="$(bun --version 2>/dev/null || echo missing)"
printf 'bun: workflow pins %s, local has %s\n' "${expected_bun:-unknown}" "$actual_bun"
if [ -n "${expected_bun:-}" ] && [ "$expected_bun" != "$actual_bun" ]; then
	printf '[WARN] bun version mismatch; results may diverge from CI\n'
fi

run "bun install --frozen-lockfile" bun install --frozen-lockfile
run "biome ci packages/" bunx biome ci packages/
run "tsc --noEmit" bunx tsc --noEmit -p tsconfig.json
run "bun test packages/" env GAJAEWAY_E2E_GJC=0 bun test packages/
if [ "${GAJAEWAY_E2E_GJC:-}" = "1" ]; then
	run "persistent-session real GJC E2E" env GAJAEWAY_E2E_GJC=1 bun test packages/gateway/test/persistent-session.e2e.test.ts
else
	printf '\n=== persistent-session real GJC E2E ===\n'
	printf '[SKIP] set GAJAEWAY_E2E_GJC=1 with inherited provider credentials to run the scratch broker check\n'
fi
run "bench gate" env GAJAEWAY_BENCH=1 bun test packages/gateway/bench
run "build + binary smoke" scripts/smoke-binary.sh

printf '\n=== summary ===\n'
if [ "$fail" -eq 0 ]; then
	printf 'all gates passed (commit %s)\n' "$(git rev-parse --short HEAD)"
else
	printf 'at least one gate FAILED (commit %s)\n' "$(git rev-parse --short HEAD)"
fi
exit "$fail"
