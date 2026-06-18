#!/usr/bin/env bash
# start-services.sh
# Starts PostgreSQL (Docker) and lakeFS (native binary).
# Run from the project root directory.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
PID_FILE="$ROOT/.lakefs.pid"

cd "$ROOT"

# ── Sanity checks ─────────────────────────────────────────────────────────────
if [ ! -f ".env" ]; then
  echo "ERROR: .env not found. Copy .env.example to .env and fill in values."
  exit 1
fi
if [ ! -f "lakefs.yaml" ]; then
  echo "ERROR: lakefs.yaml not found. Copy lakefs.yaml.example to lakefs.yaml and fill in values."
  exit 1
fi

# Load env vars for Postgres settings
set -a; source .env; set +a

# ── Locate lakeFS binary ──────────────────────────────────────────────────────
if command -v lakefs &>/dev/null; then
  LAKEFS_BIN="lakefs"
elif [ -f "./bin/lakefs" ]; then
  LAKEFS_BIN="./bin/lakefs"
  export PATH="$ROOT/bin:$PATH"
else
  echo "ERROR: lakeFS binary not found."
  echo "Run:  bash scripts/install-lakefs.sh"
  exit 1
fi

echo ""
echo "Starting lakeFS + NetApp ONTAP demo environment"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── Step 1: Start PostgreSQL ──────────────────────────────────────────────────
echo ""
echo "[1/3] Starting PostgreSQL..."
docker compose up -d postgres

echo "      Waiting for PostgreSQL to accept connections..."
for i in $(seq 1 30); do
  if docker compose exec -T postgres pg_isready -U "${POSTGRES_USER:-lakefs}" -q 2>/dev/null; then
    echo "      PostgreSQL is ready."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: PostgreSQL did not become ready in time."
    exit 1
  fi
  sleep 1
done

# ── Step 2: Run lakeFS migrations ─────────────────────────────────────────────
echo ""
echo "[2/3] Running lakeFS database migrations..."
$LAKEFS_BIN migrate up --config lakefs.yaml
echo "      Migrations complete."

# ── Step 3: Start lakeFS ──────────────────────────────────────────────────────
echo ""
echo "[3/3] Starting lakeFS..."

if [ -f "$PID_FILE" ]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "      lakeFS already running (PID $OLD_PID). Stop it first with: bash scripts/stop-services.sh"
    exit 0
  else
    rm -f "$PID_FILE"
  fi
fi

$LAKEFS_BIN run --config lakefs.yaml > "$ROOT/lakefs.log" 2>&1 &
LAKEFS_PID=$!
echo $LAKEFS_PID > "$PID_FILE"

echo "      lakeFS starting (PID $LAKEFS_PID)..."
sleep 3

# Verify it started
if ! kill -0 "$LAKEFS_PID" 2>/dev/null; then
  echo "ERROR: lakeFS failed to start. Check logs:"
  tail -20 "$ROOT/lakefs.log"
  exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Services running:"
echo "    PostgreSQL  → localhost:5432"
echo "    lakeFS      → http://localhost:8000   (PID $LAKEFS_PID)"
echo ""
echo "  lakeFS UI:  http://localhost:8000"
echo "  Logs:       tail -f lakefs.log"
echo "  Stop:       bash scripts/stop-services.sh"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Next: run  bash scripts/verify-connectivity.sh  to confirm ONTAP S3 is reachable."
echo ""
