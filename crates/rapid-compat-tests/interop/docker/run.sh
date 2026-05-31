#!/usr/bin/env bash
# run.sh — convenience wrapper for the docker-compose interop harness.
#
# 1. Copies the pre-built Java jar and Rust binary into the build context.
# 2. Runs `docker compose up --build --abort-on-container-exit
#    --exit-code-from verifier`.
# 3. Cleans up containers and copied artifacts.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../../.." && pwd)"

JAR="$repo/references/rapid-java/examples/target/standalone-agent.jar"
BIN="$repo/target/release/rapid-example"

if [ ! -f "$JAR" ]; then
  echo "missing $JAR — build with: (cd $repo/references/rapid-java && mvn package -DskipTests)" >&2
  exit 2
fi
if [ ! -f "$BIN" ]; then
  echo "missing $BIN — build with: cargo build --release -p rapid-example" >&2
  exit 2
fi

cp "$JAR" "$here/standalone-agent.jar"
cp "$BIN" "$here/rapid-example"
chmod +x "$here/rapid-example"

cleanup() {
  ( cd "$here" && docker compose down -v --remove-orphans 2>/dev/null || true )
  rm -f "$here/standalone-agent.jar" "$here/rapid-example"
}
trap cleanup EXIT INT TERM

cd "$here"
# Don't use --abort-on-container-exit: joiners that race ahead of the
# seed will exit non-zero on their first try, but the `restart:
# on-failure` policy brings them back up once the seed is reachable.
# Only the verifier's exit code matters.
docker compose up --build --exit-code-from verifier
