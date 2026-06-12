"""Convenience wrapper: set up repo + gate, upload the inbox, then run the agent."""
from __future__ import annotations

import subprocess
import sys


def run(cmd: list[str]) -> None:
    print(f"\n>>> {' '.join(cmd)}\n")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        sys.exit(result.returncode)


def main() -> None:
    run(["uv", "run", "python", "scripts/setup_lakefs_repo.py"])
    run(["uv", "run", "python", "scripts/upload_sample_data.py"])
    run(["uv", "run", "python", "-m", "mount_receipts.main"])


if __name__ == "__main__":
    main()
