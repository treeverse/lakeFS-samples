#!/usr/bin/env bash
# check-prereqs.sh
# Verifies that every tool needed for the demo is present.
# Run this first before anything else.

set -euo pipefail

PASS=0
FAIL=0

ok()   { echo "  [OK]  $*"; PASS=$((PASS+1)); }
fail() { echo "  [!!]  $*"; FAIL=$((FAIL+1)); }
info() { echo "        $*"; }

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   lakeFS + NetApp ONTAP Demo — Prerequisites Check  ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# ── Docker ────────────────────────────────────────────────────────────────────
echo "── Docker ───────────────────────────────────────────────"
if docker --version &>/dev/null; then
  ok "Docker: $(docker --version)"
else
  fail "Docker not found. Install Docker Desktop from https://docs.docker.com/desktop/install/mac-install/"
fi

if docker compose version &>/dev/null; then
  ok "Docker Compose: $(docker compose version)"
else
  fail "Docker Compose not found (requires Docker Desktop >= 4.x)"
fi

if docker info &>/dev/null; then
  ok "Docker daemon is running"
else
  fail "Docker daemon is not running — open Docker Desktop"
fi
echo ""

# ── VMware Fusion ─────────────────────────────────────────────────────────────
echo "── VMware Fusion (required to run the ONTAP OVA) ────────"
if [ -d "/Applications/VMware Fusion.app" ]; then
  ok "VMware Fusion is installed"
else
  fail "VMware Fusion not found"
  info "Install VMware Fusion Pro (free) from:"
  info "  https://support.broadcom.com/group/ecx/productdownloads?subfamily=VMware+Fusion"
  info "  Sign in with a free Broadcom account, then download 'VMware Fusion Pro'."
  info "  Alternatively: brew install --cask vmware-fusion (may be disabled; use direct download)"
fi
echo ""

# ── Python ────────────────────────────────────────────────────────────────────
echo "── Python ───────────────────────────────────────────────"
if python3 --version &>/dev/null; then
  PYVER=$(python3 --version)
  ok "Python: $PYVER"
else
  fail "python3 not found — install via: brew install python"
fi

if python3 -c "import requests" 2>/dev/null; then
  ok "requests library available"
else
  fail "requests not installed — run: pip3 install requests"
fi

if python3 -c "import lakefs" 2>/dev/null; then
  ok "lakefs SDK available: $(python3 -c 'import lakefs; print(lakefs.__version__)')"
else
  info "lakefs SDK not installed (optional, demo uses requests directly)"
fi
echo ""

# ── lakeFS binary ─────────────────────────────────────────────────────────────
echo "── lakeFS binary ────────────────────────────────────────"
LAKEFS_BIN=""
if command -v lakefs &>/dev/null; then
  LAKEFS_BIN="lakefs"
  ok "lakefs in PATH: $(lakefs --version)"
elif [ -f "$(dirname "$0")/../bin/lakefs" ]; then
  LAKEFS_BIN="$(dirname "$0")/../bin/lakefs"
  ok "lakefs binary found at ./bin/lakefs: $($LAKEFS_BIN --version)"
else
  fail "lakefs binary not found"
  info "Download with scripts/install-lakefs.sh  (or manually):"
  info "  VERSION=1.80.0"
  info "  curl -L https://github.com/treeverse/lakeFS/releases/download/v\$VERSION/lakeFS_\${VERSION}_Darwin_arm64.tar.gz \\"
  info "    | tar -xz -C ./bin"
fi
echo ""

# ── Config files ──────────────────────────────────────────────────────────────
echo "── Config files ─────────────────────────────────────────"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"

if [ -f "$ROOT/.env" ]; then
  ok ".env exists"
  # Check for unfilled placeholders
  if grep -q "REPLACE_ME" "$ROOT/.env" 2>/dev/null; then
    fail ".env still contains REPLACE_ME — fill in your ONTAP credentials"
  fi
else
  fail ".env not found — copy .env.example to .env and fill in values"
fi

if [ -f "$ROOT/lakefs.yaml" ]; then
  ok "lakefs.yaml exists"
  if grep -q "REPLACE_ME" "$ROOT/lakefs.yaml" 2>/dev/null; then
    fail "lakefs.yaml still contains REPLACE_ME — fill in your values"
  fi
else
  fail "lakefs.yaml not found — copy lakefs.yaml.example to lakefs.yaml and fill in values"
fi
echo ""

# ── OVA file ──────────────────────────────────────────────────────────────────
echo "── ONTAP Simulator OVA ──────────────────────────────────"
OVA="$HOME/Downloads/vsim-netapp-DOT9.18.1-cm_nodar.ova"
if [ -f "$OVA" ]; then
  SIZE=$(du -sh "$OVA" | cut -f1)
  ok "OVA found: $OVA ($SIZE)"
else
  fail "OVA not found at $OVA"
  info "Expected: vsim-netapp-DOT9.18.1-cm_nodar.ova"
fi
echo ""

# ── Summary ───────────────────────────────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  PASSED: $PASS    FAILED: $FAIL"
if [ "$FAIL" -eq 0 ]; then
  echo "  All prerequisites met. You are ready to proceed."
else
  echo "  Fix the items above, then re-run this script."
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
