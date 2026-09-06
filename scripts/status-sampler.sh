#!/usr/bin/env bash
# I10 soak sampler: one `gateway.status` + `ops.cycle` snapshot every 5 minutes into
# artifacts/soak/status-<ISO>.json. Non-product; feeds scripts/cutover-report.ts.
#
# usage: scripts/status-sampler.sh <gajaeway-bin> <socket-path> <out-dir> [interval-seconds]
set -euo pipefail
BIN="${1:?gajaeway binary}"
SOCKET="${2:?gateway socket path}"
OUT="${3:?output directory}"
INTERVAL="${4:-300}"
mkdir -p "$OUT"
while true; do
	STAMP="$(date -u +%Y-%m-%dT%H-%M-%SZ)"
	STATUS="$("$BIN" --socket "$SOCKET" status 2>/dev/null || echo '{"error":"status_unavailable"}')"
	CYCLE="$("$BIN" --socket "$SOCKET" ops cycle --json 2>/dev/null || echo '{"error":"cycle_unavailable"}')"
	printf '{"sampleAt":"%s","status":%s,"cycle":%s}\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$STATUS" "$CYCLE" > "$OUT/status-$STAMP.json"
	sleep "$INTERVAL"
done
