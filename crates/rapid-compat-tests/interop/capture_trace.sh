#!/usr/bin/env bash
# capture_trace.sh — drive a small Java cluster scenario and capture
# its inbound wire traffic as NDJSON via the `NdjsonTraceWriter`
# patch in references/rapid-java/rapid/src/main/java/com/vrg/rapid/messaging/impl/.
#
# Produces $OUTPUT_TRACE (default /tmp/rapid-trace.ndjson) suitable for
# feeding into `cargo test -p rapid-compat-tests --test ndjson_replay`
# via the RAPID_NDJSON_TRACE_REPLAY env var.
#
# Required env:
#   RAPID_JAVA_JAR  — path to standalone-agent.jar.
#
# Optional env:
#   OUTPUT_TRACE    — destination file (default /tmp/rapid-trace.ndjson).
#   JAVA_HOME       — JDK root (Java 21 tested).
#   BASE_PORT       — base port (default 22500).

set -euo pipefail

: "${RAPID_JAVA_JAR:?set RAPID_JAVA_JAR to standalone-agent.jar}"
OUTPUT_TRACE="${OUTPUT_TRACE:-/tmp/rapid-trace.ndjson}"
BASE_PORT="${BASE_PORT:-22500}"

if [ -n "${JAVA_HOME:-}" ]; then
  JAVA_BIN="$JAVA_HOME/bin/java"
else
  JAVA_BIN="$(command -v java)"
fi

rm -f "$OUTPUT_TRACE"

PIDS=()
cleanup() {
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

run_agent() {
  local listen="$1" seed="$2"
  RAPID_NDJSON_TRACE="$OUTPUT_TRACE" \
    "$JAVA_BIN" \
      --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
      --add-opens java.base/java.nio=ALL-UNNAMED \
      -jar "$RAPID_JAVA_JAR" -l "$listen" -s "$seed" \
      >/dev/null 2>&1 &
  PIDS+=("$!")
}

SEED="127.0.0.1:$BASE_PORT"
run_agent "$SEED" "$SEED"
sleep 3
run_agent "127.0.0.1:$((BASE_PORT+1))" "$SEED"
sleep 2
run_agent "127.0.0.1:$((BASE_PORT+2))" "$SEED"
sleep 8

echo "capture_trace: wrote $(wc -l <"$OUTPUT_TRACE") records to $OUTPUT_TRACE"
