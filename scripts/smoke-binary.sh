#!/usr/bin/env bash
# Build the single gajaeway binary and prove it behaves as a binary, not just
# as source: exact dist contents, help/version, offline config check, a
# socket-less client failure with the expected exit status, and (gated on a
# live gjc) a real daemon boot/status/shutdown round trip.
#
# Usage: scripts/smoke-binary.sh
# Exit code is non-zero on the first failed assertion.

set -euo pipefail
cd "$(dirname "$0")/.."

fail() {
	printf '[FAIL] %s\n' "$1" >&2
	exit 1
}

rm -rf dist
bun run build >/dev/null
[ "$(ls dist)" = "gajaeway" ] || fail "dist must contain exactly gajaeway, got: $(ls dist | tr '\n' ' ')"

help_out="$(dist/gajaeway --help)" || fail "--help exited non-zero"
printf '%s' "$help_out" | grep -q "daemon run" || fail "--help does not mention daemon run"

version_out="$(dist/gajaeway --version)" || fail "--version exited non-zero"
printf '%s' "$version_out" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+' || fail "--version is not semver: $version_out"

home="$(mktemp -d)"
trap 'rm -rf "$home"' EXIT
printf '{"schemaVersion":1}\n' >"$home/config.json"
check_out="$(GAJAEWAY_HOME="$home" dist/gajaeway config check)" || fail "config check exited non-zero"
printf '%s' "$check_out" | grep -q '^OK ' || fail "config check did not report OK: $check_out"

set +e
status_out="$(dist/gajaeway status --socket "$home/none.sock" 2>&1)"
status_rc=$?
set -e
[ "$status_rc" = 1 ] || fail "status without a daemon must exit 1, got $status_rc"
printf '%s' "$status_out" | grep -Eq 'Unable to connect|Failed to connect' || fail "status without a daemon must explain the missing socket: $status_out"

set +e
dist/gajaeway >/dev/null 2>&1
usage_rc=$?
set -e
[ "$usage_rc" = 2 ] || fail "no arguments must exit 2, got $usage_rc"

if [ "${GAJAEWAY_E2E_GJC:-}" = "1" ]; then
	# GAJAEWAY_ADMIN_PORT rejects 0 by contract (1-65535); pick a free high port.
	admin_port="$(bun -e 'const s=Bun.listen({hostname:"127.0.0.1",port:0,socket:{data(){}}});console.log(s.port);s.stop()')"
	GAJAEWAY_HOME="$home" GAJAEWAY_ADMIN_PORT="$admin_port" dist/gajaeway daemon run >"$home/daemon.log" 2>&1 &
	daemon_pid=$!
	sock="$home/gateway.sock"
	for _ in $(seq 1 100); do
		if [ -S "$sock" ] && dist/gajaeway status --socket "$sock" 2>/dev/null | grep -q "\"pid\":$daemon_pid"; then break; fi
		sleep 0.2
	done
	dist/gajaeway status --socket "$sock" | grep -q "\"pid\":$daemon_pid" || {
		cat "$home/daemon.log" >&2
		fail "compiled daemon never answered status with its own pid"
	}
	dist/gajaeway shutdown --socket "$sock" || fail "shutdown verb failed"
	for _ in $(seq 1 50); do
		kill -0 "$daemon_pid" 2>/dev/null || break
		sleep 0.2
	done
	if kill -0 "$daemon_pid" 2>/dev/null; then
		kill -9 "$daemon_pid"
		fail "daemon did not exit within 10s after shutdown"
	fi
	wait "$daemon_pid"
	daemon_rc=$?
	[ "$daemon_rc" = 0 ] || fail "daemon exited $daemon_rc after shutdown"
	[ ! -e "$sock" ] || fail "gateway.sock survived shutdown"
	[ ! -e "$home/gajaeway.pid" ] || fail "gajaeway.pid survived shutdown"
	printf '[PASS] compiled daemon round trip\n'
else
	printf '[SKIP] compiled daemon round trip (set GAJAEWAY_E2E_GJC=1)\n'
fi

printf '[PASS] binary smoke: dist/gajaeway\n'
