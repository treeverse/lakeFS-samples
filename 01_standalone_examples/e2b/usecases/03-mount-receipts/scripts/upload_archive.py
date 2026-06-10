"""Upload a large, never-opened ``archive/`` to lakeFS to showcase Mount's lazy fetch.

The agent only ever globs ``inbox/``, so nothing under ``archive/`` is read. When the
branch is mounted, everest fetches file *bytes* lazily — so a multi-GB archive can be
mounted instantly and never downloaded. This makes the "bring over a massive dataset,
use only a sliver" story concrete.

Bulk upload goes through ``lakectl fs upload -r`` (streams via presigned URLs), which is
far more efficient than the SDK for many/large files.

Size is tunable (it's a one-time, bandwidth-bound upload):
    ARCHIVE_FILES (default 1000) × ARCHIVE_SIZE_KB (default 256)  ≈ 250 MB by default
    e.g.  ARCHIVE_FILES=4000 ARCHIVE_SIZE_KB=512  uv run python scripts/upload_archive.py   # ~2 GB
"""
from __future__ import annotations

import os
import subprocess
import tempfile

from mount_receipts.config import load_config


def main() -> None:
    cfg = load_config()
    n_files = int(os.environ.get("ARCHIVE_FILES", "1000"))
    size_kb = int(os.environ.get("ARCHIVE_SIZE_KB", "256"))
    total_mb = n_files * size_kb / 1024
    env = {**os.environ, **cfg.lakectl_envs()}
    uri = f"lakefs://{cfg.lakefs_repository}/{cfg.lakefs_branch}/archive/"

    print(f"  Generating {n_files} decoy files × {size_kb} KB (~{total_mb:.0f} MB)...")
    with tempfile.TemporaryDirectory() as tmp:
        nbytes = size_kb * 1024
        for i in range(n_files):
            # unique content per file (avoids content-addressed dedup hiding the size)
            with open(os.path.join(tmp, f"blob_{i:06d}.bin"), "wb") as f:
                f.write(f"decoy {i}\n".encode() + os.urandom(nbytes))
        print(f"  Uploading to {uri} via lakectl (presigned, direct to object store)...")
        subprocess.run(["lakectl", "fs", "upload", "-r", "-s", tmp, uri], env=env, check=True)

    print("  Committing...")
    subprocess.run(
        ["lakectl", "commit", f"lakefs://{cfg.lakefs_repository}/{cfg.lakefs_branch}",
         "-m", f"Add archive/: {n_files} files (~{total_mb:.0f} MB), never opened by the agent"],
        env=env, check=True,
    )
    print(f"  Done. archive/ now holds {n_files} files (~{total_mb:.0f} MB) the agent never reads.")


if __name__ == "__main__":
    main()
