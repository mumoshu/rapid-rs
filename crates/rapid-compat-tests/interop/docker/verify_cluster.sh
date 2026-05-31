#!/bin/sh
# Verifier: tails every agent's stdout via `docker logs`, requires every
# agent to report the same memberlist for >= 5 consecutive sampling
# rounds (sampled every 1s). Total bootstrap budget: 60s.
#
# Exit 0 on PASS, 1 on FAIL (timeout or divergence).

set -eu

# Service names as declared in docker-compose.yml.
AGENTS="seed j1 j2 r1 r2 r3"
SAMPLES_REQUIRED=5
TIMEOUT=60

# Resolve the actual container name for a docker-compose service by
# matching the `com.docker.compose.service` label. Works regardless of
# the project name (`docker-seed-1`, `interop-seed-1`, …).
resolve_container() {
  agent="$1"
  docker ps --filter "label=com.docker.compose.service=$agent" --format '{{.Names}}' | head -n1
}

last_view() {
  agent="$1"
  container="$(resolve_container "$agent")"
  if [ -z "$container" ]; then
    return
  fi
  docker logs "$container" 2>/dev/null \
    | grep -oE 'view: [0-9a-f-]+ \[[^]]+\]' \
    | tail -n1
}

extract_list() {
  echo "$1" | sed -E 's/.*\[([^]]+)\].*/\1/'
}

stable=0
elapsed=0
while [ "$elapsed" -lt "$TIMEOUT" ]; do
  first=""
  ok=1
  for a in $AGENTS; do
    v="$(last_view "$a" || true)"
    if [ -z "$v" ]; then
      ok=0
      break
    fi
    list="$(extract_list "$v")"
    if [ -z "$first" ]; then
      first="$list"
    elif [ "$list" != "$first" ]; then
      ok=0
      break
    fi
  done
  if [ "$ok" -eq 1 ]; then
    stable=$((stable + 1))
    echo "verifier: stable sample $stable/$SAMPLES_REQUIRED (list=$first)"
    if [ "$stable" -ge "$SAMPLES_REQUIRED" ]; then
      echo "verifier: PASS"
      exit 0
    fi
  else
    stable=0
  fi
  sleep 1
  elapsed=$((elapsed + 1))
done

echo "verifier: FAIL (no stable sample within ${TIMEOUT}s)" >&2
exit 1
