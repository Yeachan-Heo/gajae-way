#!/bin/sh
# Re-sign the compiled dist/ binaries with one stable local code-signing identity.
#
# `bun build --compile` emits ad-hoc signatures (identifier "a.out"). macOS privacy
# grants (TCC: Documents, Desktop, Downloads, Full Disk Access) for an ad-hoc binary
# are pinned to that build's exact hash, so every rebuild re-prompts, and a gateway
# child reading ~/Documents blocks in the kernel until someone clicks Allow
# (2026-09-29: one prompt stayed unanswered for 7h37m and wedged every reply).
# A certificate signature with a fixed identifier yields a designated requirement
# that survives rebuilds, so a grant is given once.
#
# The identity comes from GAJAEWAY_CODESIGN_IDENTITY or ~/.gajaeway/codesign-identity
# (first line: SHA-1 or common name from `security find-identity -v -p codesigning`).
# Without either, the ad-hoc signatures are left as they are.
set -eu

[ "$(uname -s)" = "Darwin" ] || exit 0

identity="${GAJAEWAY_CODESIGN_IDENTITY:-}"
identity_file="${HOME}/.gajaeway/codesign-identity"
if [ -z "$identity" ] && [ -r "$identity_file" ]; then
	identity="$(head -n 1 "$identity_file")"
fi
[ -n "$identity" ] || exit 0

cd "$(dirname "$0")/../dist"
for pair in gajaeway:cli gajaeway-gateway:gateway gajaeway-slack:slack gajaeway-discord:discord \
	gajaeway-telegram:telegram gajaeway-admin:admin; do
	file="${pair%%:*}"
	codesign --force --sign "$identity" --identifier "dev.gajaeway.${pair##*:}" "$file"
	codesign --verify --strict "$file"
done
