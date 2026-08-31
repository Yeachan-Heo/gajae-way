#!/bin/bash
# Settle two BLOCK claims empirically:
#  (a) does forwarding --worktree strand the bound session?
#  (b) does a daemon/CLI GJC_CODING_AGENT_DIR mismatch strand it?
set +u
REPO="/Users/bellman/Documents/Workspace/gajae-way/.worktrees/feat-gajae-way-gjc-cli-86d8c50c"
cd "$REPO" || exit 1

probe() {
  local name="$1"; shift
  local agent_dir_daemon="$1"; shift
  local agent_dir_cli="$1"; shift
  local extra_args=("$@")

  local H="/tmp/gjc-scope-$name"
  rm -rf "$H"; mkdir -p "$H/workspace"
  printf '{"schemaVersion":1,"home":"%s","logVerbosity":"info","dmPolicy":"open"}' "$H" > "$H/config.json"
  printf 'Probe persona.\n' > "$H/workspace/SOUL.md"
  printf 'notes\n' > "$H/workspace/AGENTS.md"
  printf 'operator\n' > "$H/workspace/USER.md"
  # A git repo, so `--worktree` has something to work with.
  printf '/.worktrees\n' > "$H/workspace/.gitignore"
  ( cd "$H/workspace" && git init -q . && git add -A && git commit -q -m init ) 2>/dev/null

  echo ""
  echo "############ $name ############"
  echo "daemon agent dir: ${agent_dir_daemon:-<inherit>}"
  echo "cli    agent dir: ${agent_dir_cli:-<inherit>}"
  echo "extra args: ${extra_args[*]:-<none>}"

  if [ -n "$agent_dir_daemon" ]; then
    env -u GAJAEWAY_TEST_STUB_GJC GAJAEWAY_HOME="$H" GJC_CODING_AGENT_DIR="$agent_dir_daemon" \
      bun packages/gateway/src/main.ts daemon > "$H/daemon.log" 2>&1 &
  else
    env -u GAJAEWAY_TEST_STUB_GJC GAJAEWAY_HOME="$H" \
      bun packages/gateway/src/main.ts daemon > "$H/daemon.log" 2>&1 &
  fi
  local dpid=$!
  for _ in $(seq 1 80); do
    if bun -e "const s=await Bun.connect({unix:'$H/gateway.sock',socket:{data(){}}}); s.end(); process.exit(0);" >/dev/null 2>&1; then break; fi
    sleep 0.5
  done

  run_once() {
    local label="$1"; shift
    local out="$H/$label.out"
    if [ -n "$agent_dir_cli" ]; then
      env -u GAJAEWAY_TEST_STUB_GJC GAJAEWAY_HOME="$H" GJC_CODING_AGENT_DIR="$agent_dir_cli" \
        script -q /dev/null bun packages/cli/src/main.ts --socket "$H/gateway.sock" gjc "$@" > "$out" 2>&1 &
    else
      env -u GAJAEWAY_TEST_STUB_GJC GAJAEWAY_HOME="$H" \
        script -q /dev/null bun packages/cli/src/main.ts --socket "$H/gateway.sock" gjc "$@" > "$out" 2>&1 &
    fi
    local cpid=$!
    for _ in $(seq 1 150); do
      if ! kill -0 "$cpid" 2>/dev/null; then break; fi
      sleep 0.5
    done
    kill -TERM "$cpid" 2>/dev/null; wait "$cpid" 2>/dev/null
    echo "--- $label ---"
    if grep -q "not found" "$out"; then
      echo "RESULT: SESSION NOT FOUND  <-- stranded"
    fi
    if grep -qi "refuses\|bound to" "$out"; then
      echo "RESULT: REFUSED by gateway:"
      grep -io "session.attach refuses.*\|this session is bound to.*" "$out" | head -1
    fi
    if grep -qi "Fork into current directory" "$out"; then
      echo "RESULT: TUI OFFERED TO FORK THE MANAGED SESSION  <-- hazard"
    fi
    grep -o "gjc --resume [0-9a-f-]*" "$out" | tail -1 || echo "(no resume hint)"
  }

  run_once first "${extra_args[@]}"
  run_once second "${extra_args[@]}"

  echo "gateway row:"
  bun -e "
    const { Database } = require('bun:sqlite');
    const db = new Database('$H/gateway.db', { readonly: true });
    const r = db.query('SELECT gjc_session_id, epoch FROM sessions WHERE origin_key = ?').get('loopback/loopback/terminal');
    console.log(r ? r.gjc_session_id + ' @e' + r.epoch : 'none');
    process.exit(0);
  " 2>/dev/null

  kill "$dpid" 2>/dev/null; wait "$dpid" 2>/dev/null
}

# Control: no worktree, same env. Expect matching resume hints.
# (a) worktree forwarded, same env.
# (b) daemon and CLI disagree about the native agent-state root.

probe control "" ""
mkdir -p /tmp/gjc-scope-agentA /tmp/gjc-scope-agentB
probe envsplit /tmp/gjc-scope-agentA /tmp/gjc-scope-agentB

# The refusal path: --worktree must be rejected with a reason, never forwarded.
probe worktree "" "" --worktree probe-branch
