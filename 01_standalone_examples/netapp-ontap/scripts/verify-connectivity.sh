#!/usr/bin/env bash
# verify-connectivity.sh
# Tests that the Mac can reach the ONTAP S3 endpoint AND that lakeFS is up.
# Run after start-services.sh and after the ONTAP VM is booted and configured.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"

cd "$ROOT"

if [ ! -f ".env" ]; then
  echo "ERROR: .env not found."
  exit 1
fi

set -a; source .env; set +a

PASS=0; FAIL=0

ok()   { echo "  [OK]  $*"; PASS=$((PASS+1)); }
fail() { echo "  [!!]  $*"; FAIL=$((FAIL+1)); }
info() { echo "        $*"; }

echo ""
echo "Verifying connectivity..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── 1. ONTAP S3 endpoint reachable? ──────────────────────────────────────────
echo ""
echo "── ONTAP S3 ─────────────────────────────────────────────"
info "Endpoint: $ONTAP_S3_ENDPOINT"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  --max-time 5 \
  "$ONTAP_S3_ENDPOINT" 2>/dev/null || echo "000")

if [ "$HTTP_CODE" = "000" ]; then
  fail "Cannot reach $ONTAP_S3_ENDPOINT (connection refused or timed out)"
  info "Check: Is the ONTAP VM running? Is the S3 server enabled on the SVM?"
  info "Check: Is the data LIF IP correct? Try: ping $(echo $ONTAP_S3_ENDPOINT | sed 's|http[s]*://||' | cut -d: -f1)"
elif [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "301" ] || [ "$HTTP_CODE" = "403" ] || [ "$HTTP_CODE" = "405" ]; then
  ok "ONTAP S3 endpoint responded (HTTP $HTTP_CODE)"
else
  fail "Unexpected HTTP $HTTP_CODE from ONTAP S3 endpoint"
fi

# ── 2. ONTAP S3 bucket accessible? ───────────────────────────────────────────
echo ""
info "Testing bucket access with AWS CLI (if installed)..."
if command -v aws &>/dev/null; then
  if aws s3 ls "s3://$ONTAP_S3_BUCKET/" \
       --endpoint-url "$ONTAP_S3_ENDPOINT" \
       --access-key-id "${ONTAP_S3_ACCESS_KEY:-x}" \
       --secret-access-key "${ONTAP_S3_SECRET_KEY:-x}" \
       --no-sign-request 2>/dev/null || \
     AWS_ACCESS_KEY_ID="$ONTAP_S3_ACCESS_KEY" \
     AWS_SECRET_ACCESS_KEY="$ONTAP_S3_SECRET_KEY" \
     aws s3 ls "s3://$ONTAP_S3_BUCKET/" \
       --endpoint-url "$ONTAP_S3_ENDPOINT" \
       --region us-east-1 2>/dev/null; then
    ok "Bucket '$ONTAP_S3_BUCKET' is accessible via AWS CLI"
  else
    fail "Cannot list bucket '$ONTAP_S3_BUCKET' — check credentials and bucket policy"
  fi
else
  info "AWS CLI not installed — skipping bucket list test"
  info "Install with: brew install awscli"
fi

# ── 3. lakeFS health ──────────────────────────────────────────────────────────
echo ""
echo "── lakeFS ───────────────────────────────────────────────"
LAKEFS_HOST="${LAKEFS_HOST:-http://localhost:8000}"
info "Host: $LAKEFS_HOST"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  --max-time 5 \
  "$LAKEFS_HOST/api/v1/healthcheck" 2>/dev/null || echo "000")

if [ "$HTTP_CODE" = "204" ] || [ "$HTTP_CODE" = "200" ]; then
  ok "lakeFS healthcheck passed (HTTP $HTTP_CODE)"
else
  fail "lakeFS healthcheck failed (HTTP $HTTP_CODE)"
  info "Is lakeFS running? Check: bash scripts/start-services.sh"
  info "Check logs: tail -50 lakefs.log"
fi

# ── 4. lakeFS storage check ───────────────────────────────────────────────────
HTTP_CODE=$(curl -s -o /tmp/lakefs_storage_check.json -w "%{http_code}" \
  --max-time 5 \
  -u "${LAKEFS_ACCESS_KEY_ID:-AKIAIOSFODNN7EXAMPLE}:${LAKEFS_SECRET_ACCESS_KEY:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY}" \
  "$LAKEFS_HOST/api/v1/config" 2>/dev/null || echo "000")

if [ "$HTTP_CODE" = "200" ]; then
  STORAGE_TYPE=$(python3 -c "import json,sys; d=json.load(open('/tmp/lakefs_storage_check.json')); print(d.get('storage_config',{}).get('blockstore_type','?'))" 2>/dev/null || echo "unknown")
  ok "lakeFS config endpoint reachable — blockstore type: $STORAGE_TYPE"
elif [ "$HTTP_CODE" = "401" ]; then
  fail "lakeFS auth failed (HTTP 401) — check LAKEFS_ACCESS_KEY_ID / LAKEFS_SECRET_ACCESS_KEY in .env"
else
  fail "lakeFS config endpoint returned HTTP $HTTP_CODE"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  PASSED: $PASS    FAILED: $FAIL"
if [ "$FAIL" -eq 0 ]; then
  echo "  All connectivity checks passed.  Run the demo:"
  echo "    cd demo && python3 demo_flow.py"
else
  echo "  Fix the issues above before running the demo."
  echo "  See README.md — Troubleshooting section."
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
