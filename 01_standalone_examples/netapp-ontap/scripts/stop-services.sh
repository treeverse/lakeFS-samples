#!/usr/bin/env bash
# stop-services.sh
# Stops lakeFS and PostgreSQL.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
PID_FILE="$ROOT/.lakefs.pid"

cd "$ROOT"

echo "Stopping services..."

if [ -f "$PID_FILE" ]; then
  PID=$(cat "$PID_FILE")
  if kill -0 "$PID" 2>/dev/null; then
    kill "$PID"
    echo "  lakeFS stopped (was PID $PID)"
  else
    echo "  lakeFS was not running"
  fi
  rm -f "$PID_FILE"
else
  echo "  No .lakefs.pid file found — lakeFS may not be running"
fi

docker compose down
echo "  PostgreSQL stopped"
echo "Done."
