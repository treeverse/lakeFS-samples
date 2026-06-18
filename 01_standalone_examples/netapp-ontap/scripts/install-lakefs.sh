#!/usr/bin/env bash
# install-lakefs.sh
# Downloads the lakeFS macOS arm64 binary to ./bin/lakefs.
# Run once from the project root directory.

set -euo pipefail

VERSION="1.80.0"
ARCH="arm64"
OS="Darwin"
DEST="$(cd "$(dirname "$0")/.." && pwd)/bin"
TARBALL="lakeFS_${VERSION}_${OS}_${ARCH}.tar.gz"
URL="https://github.com/treeverse/lakeFS/releases/download/v${VERSION}/${TARBALL}"

echo "Downloading lakeFS v${VERSION} for ${OS}/${ARCH}..."
echo "From: $URL"
echo "To:   $DEST/lakefs"
echo ""

mkdir -p "$DEST"
curl -L --progress-bar "$URL" | tar -xz -C "$DEST" lakefs

chmod +x "$DEST/lakefs"
echo "Done. Verify:"
"$DEST/lakefs" --version
echo ""
echo "Add to PATH for this session:"
echo "  export PATH=\"$DEST:\$PATH\""
