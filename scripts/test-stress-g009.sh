#!/usr/bin/env bash
set -euo pipefail

# G009 stress harness: overlap independent Bun test workers with deliberate CPU/IO
# pressure. It preserves each worker's real failure output and exits non-zero on
# the first failing suite.
load_jobs=()
cleanup() {
  for pid in "${load_jobs[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  rm -f "${TMPDIR:-/tmp}"/g009-load-{1,2,3,4}
}
trap cleanup EXIT INT TERM

for worker in 1 2 3 4; do
  (
    while :; do
      python3 - <<'PY'
import hashlib
payload = b"g009-load" * 4096
for _ in range(300): hashlib.sha256(payload).digest()
PY
      dd if=/dev/zero of="${TMPDIR:-/tmp}/g009-load-${worker}" bs=1m count=4 conv=sync 2>/dev/null
    done
  ) &
  load_jobs+=("$!")
done

bun test "$@"
