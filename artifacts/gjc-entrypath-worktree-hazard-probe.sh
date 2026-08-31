#!/bin/bash
# Archive the two primary observations behind refusing `--worktree`.
#
# Both are captured directly against native gjc, bypassing the gateway's
# refusal, because the refusal is exactly what these observations justify:
#   A. `gjc sdk session raw ... --op session.create` rejects `--session-dir`,
#      so the bound session's store cannot be pinned by the gateway.
#   B. Resuming that bound session WITH `--worktree` makes native gjc enter a
#      generated worktree and offer to FORK the managed session.
set +u
REPO="/Users/bellman/Documents/Workspace/gajae-way/.worktrees/feat-gajae-way-gjc-cli-86d8c50c"
cd "$REPO" || exit 1

H="/tmp/gjc-worktree-hazard"
rm -rf "$H"; mkdir -p "$H/workspace"
printf '/.worktrees\n' > "$H/workspace/.gitignore"
( cd "$H/workspace" && git init -q . && git add -A && git commit -q -m init ) 2>/dev/null

echo "gjc version: $(gjc --version 2>&1 | head -1)"
echo

echo "############ A. session.create rejects --session-dir ############"
echo "\$ gjc sdk session raw global --op session.create --idempotency-key hazard-A --session-dir $H/store --json-input-stdin"
( cd "$H/workspace" && echo "{\"cwd\":\"$H/workspace\"}" | \
  gjc sdk session raw global --op session.create --idempotency-key hazard-A --session-dir "$H/store" --json-input-stdin 2>&1 | head -3 )
echo
echo "-> The gateway therefore cannot pin the native store for the session it binds."
echo

echo "############ B. bind a session the way the gateway does, then resume WITH --worktree ############"
echo "\$ gjc sdk session raw global --op session.create --idempotency-key hazard-B --json-input-stdin   (cwd = persona workspace)"
CREATE=$( cd "$H/workspace" && echo "{\"cwd\":\"$H/workspace\"}" | \
  gjc sdk session raw global --op session.create --idempotency-key hazard-B --json-input-stdin 2>&1 )
echo "$CREATE" | head -3
SID=$(echo "$CREATE" | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1)
echo "bound session id: ${SID:-<none parsed>}"
echo

if [ -n "$SID" ]; then
  echo "\$ gjc --resume $SID --worktree hazard-branch   (under a PTY, in the persona workspace)"
  ( cd "$H/workspace" && script -q /dev/null gjc --resume "$SID" --worktree hazard-branch > "$H/worktree.out" 2>&1 & 
    CPID=$!
    for _ in $(seq 1 150); do
      if grep -q "Fork into current directory" "$H/worktree.out" 2>/dev/null; then break; fi
      if ! kill -0 "$CPID" 2>/dev/null; then break; fi
      sleep 0.5
    done
    kill -TERM "$CPID" 2>/dev/null; wait "$CPID" 2>/dev/null )
  echo "--- observed output ---"
  tr -d '\r' < "$H/worktree.out" | tr -s '\n' | tail -c 700
  echo
  echo "--- worktree directory native gjc created (note the generated suffix) ---"
  ls "$H/workspace/.worktrees/" 2>/dev/null
  echo
  if grep -q "Fork into current directory" "$H/worktree.out" 2>/dev/null; then
    echo "-> CONFIRMED: resuming the managed session under --worktree offers to FORK it."
  else
    echo "-> NOT REPRODUCED in this run; see raw output above."
  fi
fi

echo
echo "############ C. control: same resume WITHOUT --worktree ############"
if [ -n "$SID" ]; then
  ( cd "$H/workspace" && script -q /dev/null gjc --resume "$SID" > "$H/control.out" 2>&1 &
    CPID=$!
    for _ in $(seq 1 60); do
      if ! kill -0 "$CPID" 2>/dev/null; then break; fi
      sleep 0.5
    done
    kill -TERM "$CPID" 2>/dev/null; wait "$CPID" 2>/dev/null )
  tr -d '\r' < "$H/control.out" | tr -s '\n' | tail -c 400
  echo
  if grep -q "Fork into current directory" "$H/control.out" 2>/dev/null; then
    echo "-> unexpected: fork prompt without --worktree"
  else
    echo "-> No fork prompt without --worktree: the hazard is specific to the flag."
  fi
fi
