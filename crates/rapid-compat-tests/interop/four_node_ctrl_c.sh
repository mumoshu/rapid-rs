#!/usr/bin/env bash
# four_node_ctrl_c.sh — Phase 6 / F3 gate: 4 Rust agents bootstrap;
# Ctrl-C of one causes exactly one new `ViewChange` line at each of the
# three survivors.
#
# Required env:
#   RAPID_RUST_BIN  — path to the `rapid-example` release binary.
#
# Optional env:
#   BASE_PORT       — base port for agents (default 19000).
#   SETTLE_SECONDS  — settle time after bootstrap (default 15).
#   FAIL_SECONDS    — wait after SIGINT (default 15).
#
# Exit 0 on PASS; non-zero on FAIL.

set -euo pipefail

: "${RAPID_RUST_BIN:?set RAPID_RUST_BIN to the rapid-example binary}"
BASE_PORT="${BASE_PORT:-19000}"
SETTLE_SECONDS="${SETTLE_SECONDS:-15}"
FAIL_SECONDS="${FAIL_SECONDS:-15}"

WORKDIR="$(mktemp -d)"
PIDS=()
PORTS=()

cleanup() {
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT INT TERM

spawn() {
  local listen="$1" seed="$2"
  "$RAPID_RUST_BIN" -l "$listen" -s "$seed" --print-view-every 500 \
    > "$WORKDIR/agent-$listen.log" 2>&1 &
  PIDS+=("$!")
  PORTS+=("$listen")
}

SEED="127.0.0.1:$BASE_PORT"
spawn "$SEED" "$SEED"
sleep 2
for offset in 1 2 3; do
  spawn "127.0.0.1:$((BASE_PORT+offset))" "$SEED"
done

echo "four_node_ctrl_c: 4 agents spawned, settling ${SETTLE_SECONDS}s..."
sleep "$SETTLE_SECONDS"

count_viewchanges() {
  if [ -f "$1" ]; then
    grep -c '^ViewChange:' "$1" || true
  else
    echo 0
  fi
}

# Snapshot ViewChange counts at all four agents.
declare -a PRE_COUNTS=()
for port in "${PORTS[@]}"; do
  PRE_COUNTS+=("$(count_viewchanges "$WORKDIR/agent-$port.log")")
done

# Sigint the second agent (port BASE_PORT+1).
VICTIM_IDX=1
VICTIM_PORT="${PORTS[$VICTIM_IDX]}"
VICTIM_PID="${PIDS[$VICTIM_IDX]}"
echo "four_node_ctrl_c: SIGINT $VICTIM_PORT (pid $VICTIM_PID)"
kill -INT "$VICTIM_PID" 2>/dev/null || true
unset 'PIDS['"$VICTIM_IDX"']' 'PORTS['"$VICTIM_IDX"']'
PIDS=("${PIDS[@]}")
PORTS=("${PORTS[@]}")

echo "four_node_ctrl_c: waiting ${FAIL_SECONDS}s for FD convergence..."
sleep "$FAIL_SECONDS"

# Each survivor must show exactly one *more* ViewChange than before.
fail=0
SURVIVOR_INDICES=(0 1 2)  # post-unset PORTS holds 3 entries; indices 0,1,2.
for i in "${SURVIVOR_INDICES[@]}"; do
  port="${PORTS[$i]}"
  # Map back to pre-shutdown index. Pre-shutdown was [0,1,2,3]; victim
  # was at 1, so survivors are pre-indices [0, 2, 3] → post-indices [0,1,2].
  case "$i" in
    0) pre_idx=0 ;;
    1) pre_idx=2 ;;
    2) pre_idx=3 ;;
  esac
  pre="${PRE_COUNTS[$pre_idx]}"
  post="$(count_viewchanges "$WORKDIR/agent-$port.log")"
  delta=$((post - pre))
  if [ "$delta" -ne 1 ]; then
    echo "  $port: delta=$delta (pre=$pre post=$post) — expected exactly 1" >&2
    fail=1
  else
    echo "  $port: +1 ViewChange (pre=$pre post=$post)"
  fi
done

if [ "$fail" -eq 0 ]; then
  echo "four_node_ctrl_c: PASS"
else
  echo "four_node_ctrl_c: FAIL" >&2
  exit 1
fi
