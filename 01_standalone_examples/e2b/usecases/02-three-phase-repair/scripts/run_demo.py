"""Convenience wrapper: run setup, upload sample data, then launch the agent loop."""
from __future__ import annotations

import subprocess
import sys


def run(cmd: list[str]) -> None:
    """Run a command and exit if it fails."""
    print(f"\n>>> {' '.join(cmd)}\n")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        sys.exit(result.returncode)


def main() -> None:
    """Execute the full demo sequence end to end."""
    run(["uv", "run", "python", "scripts/setup_lakefs_repo.py"])
    run(["uv", "run", "python", "scripts/upload_sample_data.py"])
    run(["uv", "run", "python", "-m", "agent_demo.main"])


if __name__ == "__main__":
    main()
