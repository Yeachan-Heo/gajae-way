#!/bin/bash
set -euo pipefail

# All generated binaries, test homes, sockets, and logs stay outside the checkout.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BUN="$(command -v bun)" || { echo "FAIL: bun is required" >&2; exit 1; }
REPORT="${CONSOLE_VERIFY_REPORT_DIR:-$(mktemp -d /tmp/gw-console-report.XXXXXX)}"
mkdir -p "$REPORT"
REPORT="$(cd "$REPORT" && pwd)"
WORK="$(mktemp -d /tmp/gw-console-verify.XXXXXX)"
finish() {
  local status=$?
  trap - EXIT
  "$BUN" -e 'const [path, code] = process.argv.slice(1); await Bun.write(path, JSON.stringify({status: Number(code) === 0 ? "passed" : "failed", exitCode: Number(code), log: "verification.log"}, null, 2) + "\n");' "$REPORT/result.json" "$status"
  rm -rf "$WORK"
  echo "Verification report: $REPORT/result.json"
  if [ "$status" -ne 0 ]; then echo "FAIL: console verification exited $status; see $REPORT/verification.log" >&2; fi
  exit "$status"
}
trap finish EXIT

# Copy rather than link dependencies: workspace symlinks must resolve in isolation.
"$BUN" -e '
import { cpSync, mkdirSync } from "node:fs";
import { join } from "node:path";
const [root, work] = process.argv.slice(1);
mkdirSync(join(work, "repo"));
for (const name of ["packages", "scripts", "node_modules", "package.json", "bun.lock", "bunfig.toml", "tsconfig.json"]) {
  cpSync(join(root, name), join(work, "repo", name), { recursive: true, verbatimSymlinks: true });
}
' "$ROOT" "$WORK"
mkdir -p "$WORK/home" "$WORK/tmp" "$WORK/config" "$WORK/cache" "$WORK/data" "$WORK/runtime"

# Do not inherit operator credentials, gateway endpoints, or code-signing settings.
env -i PATH="$PATH" HOME="$WORK/home" TMPDIR="$WORK/tmp" \
  XDG_CONFIG_HOME="$WORK/config" XDG_CACHE_HOME="$WORK/cache" \
  XDG_DATA_HOME="$WORK/data" XDG_RUNTIME_DIR="$WORK/runtime" \
  GAJAEWAY_HOME="$WORK/home/.gajaeway" CI=1 NO_COLOR=1 \
  /bin/bash -s -- "$WORK/repo" "$BUN" "$REPORT" <<'VERIFY' 2>&1 | tee "$REPORT/verification.log"
set -euo pipefail
cd "$1"
BUN="$2"
REPORT="$3"
STEP="structure"
trap 'echo "FAIL: $STEP (line $LINENO)" >&2' ERR

echo "=== Console Package Verification ==="
for file in package.json src/tui-core.ts src/tui-components.ts src/setup-wizard.ts src/console-dashboard.ts src/cli-commands.ts src/main.ts src/index.ts; do
  test -f "packages/console/$file" || { echo "FAIL: missing packages/console/$file" >&2; exit 1; }
done
echo "PASS: all source files present"

STEP="import resolution"
"$BUN" build packages/console/src/index.ts --outdir "$TMPDIR/import-build" --target bun --format esm
echo "PASS: imports resolve"

STEP="console tests"
"$BUN" test packages/console/test --bail --reporter=junit --reporter-outfile="$REPORT/console.junit.xml"
STEP="CLI command tests"
"$BUN" test packages/cli/test/console-commands.test.ts --bail --reporter=junit --reporter-outfile="$REPORT/cli.junit.xml"
STEP="setup and console integration tests"
"$BUN" test packages/cli/test/gajaeway-console-integration.test.ts --bail --reporter=junit --reporter-outfile="$REPORT/integration.junit.xml"
echo "PASS: all targeted tests pass"

STEP="minimum test coverage"
"$BUN" -e '
const directory = process.argv[1];
let passed = 0;
for (const name of ["console", "cli", "integration"]) {
  const xml = await Bun.file(`${directory}/${name}.junit.xml`).text();
  const cases = xml.match(/<testcase\b[^>]*(?:\/>|>[\s\S]*?<\/testcase>)/g) ?? [];
  passed += cases.filter(test => !/<(?:failure|error|skipped)\b/.test(test)).length;
}
await Bun.write(`${directory}/test-count.json`, JSON.stringify({passed, minimum: 30}, null, 2) + "\n");
if (passed < 30) throw new Error(`Expected at least 30 passing tests, got ${passed}`);
console.log(`PASS: ${passed} relevant tests passed`);
' "$REPORT"

STEP="root binary build"
"$BUN" run build
for binary in gajaeway-gateway gajaeway-discord gajaeway-telegram gajaeway-slack gajaeway-admin gajaeway gajaeway-console; do
  test -x "dist/$binary" || { echo "FAIL: dist/$binary is missing or not executable" >&2; exit 1; }
done
echo "PASS: root binaries built and executable"

STEP="compiled binary help"
./dist/gajaeway-console --help
./dist/gajaeway console --help
./dist/gajaeway setup --help

echo "=== Verification Complete ==="
VERIFY
