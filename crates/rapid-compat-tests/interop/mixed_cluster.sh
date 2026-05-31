#!/usr/bin/env bash
# mixed_cluster.sh — Phase 6 / F3 gate: 3 Java + 3 Rust agents on the
# same gRPC fabric all agree on the same configuration id.
#
# Required env:
#   RAPID_JAVA_JAR   — path to standalone-agent.jar (built via `mvn package`
#                      in references/rapid-java).
#   RAPID_RUST_BIN   — path to the `rapid-example` release binary.
#
# Optional env:
#   JAVA_HOME        — JDK 11+ root (Java 21 tested). When unset, `java`
#                      is taken from PATH.
#   BASE_PORT        — base port for agents (default 1234). Agents bind
#                      BASE_PORT .. BASE_PORT+5.
#   SETTLE_SECONDS   — how long to wait for convergence before sampling
#                      (default 20).
#   FAIL_SECONDS     — how long to wait after killing two agents before
#                      sampling the survivors (default 20).
#
# Exit status: 0 if every surviving agent reported the same sorted
# endpoint list in its last `view:` line; non-zero otherwise.

set -euo pipefail

: "${RAPID_JAVA_JAR:?set RAPID_JAVA_JAR to standalone-agent.jar}"
: "${RAPID_RUST_BIN:?set RAPID_RUST_BIN to the rapid-example binary}"
BASE_PORT="${BASE_PORT:-1234}"
SETTLE_SECONDS="${SETTLE_SECONDS:-20}"
FAIL_SECONDS="${FAIL_SECONDS:-20}"

if [ -n "${JAVA_HOME:-}" ]; then
  JAVA_BIN="$JAVA_HOME/bin/java"
else
  JAVA_BIN="$(command -v java)"
fi

WORKDIR="$(mktemp -d)"
trap 'cleanup' EXIT INT TERM

PIDS=()
PORTS=()
KIND=()

cleanup() {
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$WORKDIR"
}

spawn_java() {
  local listen="$1" seed="$2"
  "$JAVA_BIN" \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens java.base/java.nio=ALL-UNNAMED \
    -jar "$RAPID_JAVA_JAR" \
    -l "$listen" -s "$seed" \
    > "$WORKDIR/java-$listen.log" 2>&1 &
  PIDS+=("$!")
  PORTS+=("$listen")
  KIND+=("java")
}

spawn_rust() {
  local listen="$1" seed="$2"
  "$RAPID_RUST_BIN" -l "$listen" -s "$seed" --print-view-every 500 \
    > "$WORKDIR/rust-$listen.log" 2>&1 &
  PIDS+=("$!")
  PORTS+=("$listen")
  KIND+=("rust")
}

SEED="127.0.0.1:$BASE_PORT"

# Seed is Java (matches Java cluster-startup parity for the harness).
spawn_java "$SEED" "$SEED"
sleep 2

# Two more Java joiners.
spawn_java "127.0.0.1:$((BASE_PORT+1))" "$SEED"
spawn_java "127.0.0.1:$((BASE_PORT+2))" "$SEED"

# Three Rust joiners.
spawn_rust "127.0.0.1:$((BASE_PORT+3))" "$SEED"
spawn_rust "127.0.0.1:$((BASE_PORT+4))" "$SEED"
spawn_rust "127.0.0.1:$((BASE_PORT+5))" "$SEED"

echo "mixed_cluster: 6 agents spawned, settling ${SETTLE_SECONDS}s..."
sleep "$SETTLE_SECONDS"

assert_agreement() {
  local expected_count="$1"
  local first=""
  local fail=0
  for i in "${!PORTS[@]}"; do
    local port="${PORTS[$i]}"
    local kind="${KIND[$i]}"
    local log="$WORKDIR/$kind-$port.log"
    local last_view
    last_view="$(grep -oE 'view: [0-9a-f-]+ \[[^]]+\]' "$log" | tail -n1 || true)"
    if [ -z "$last_view" ]; then
      echo "  $kind/$port: no view: line yet" >&2
      fail=1
      continue
    fi
    local list
    list="$(echo "$last_view" | sed -E 's/.*\[([^]]+)\].*/\1/')"
    if [ -z "$first" ]; then
      first="$list"
    elif [ "$list" != "$first" ]; then
      echo "  $kind/$port: $list != $first" >&2
      fail=1
    fi
    local n
    n="$(echo "$list" | tr ',' '\n' | wc -l)"
    if [ "$n" != "$expected_count" ]; then
      echo "  $kind/$port: $n entries, expected $expected_count" >&2
      fail=1
    fi
  done
  return $fail
}

if assert_agreement 6; then
  echo "mixed_cluster: 6-agent convergence OK"
else
  echo "mixed_cluster: 6-agent convergence FAILED" >&2
  exit 1
fi

# Kill one Java and one Rust agent (indices 1 and 4 — port+1 and port+4).
echo "mixed_cluster: killing 127.0.0.1:$((BASE_PORT+1)) (java) and 127.0.0.1:$((BASE_PORT+4)) (rust)"
kill "${PIDS[1]}" "${PIDS[4]}" 2>/dev/null || true
unset 'PIDS[1]' 'PIDS[4]' 'PORTS[1]' 'PORTS[4]' 'KIND[1]' 'KIND[4]'
PIDS=("${PIDS[@]}")
PORTS=("${PORTS[@]}")
KIND=("${KIND[@]}")

echo "mixed_cluster: waiting ${FAIL_SECONDS}s for failure-detector convergence..."
sleep "$FAIL_SECONDS"

if assert_agreement 4; then
  echo "mixed_cluster: 4-agent convergence after failures OK"
else
  echo "mixed_cluster: 4-agent convergence after failures FAILED" >&2
  exit 1
fi

echo "mixed_cluster: PASS"
