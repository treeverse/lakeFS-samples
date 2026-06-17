"""Entry point: run the receipts → ledger agent over a lakeFS Mount in E2B."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent.parent / ".env")

from mount_receipts.config import load_config
from mount_receipts.orchestrator import print_report, run


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Receipts → Clean Ledger: an agent curating files on a lakeFS Mount in E2B"
    )
    parser.add_argument("--no-merge", action="store_true", help="Do not merge into main even if validation passes")
    parser.add_argument("--branch", default=None, help="Source branch to start from (default LAKEFS_BRANCH or 'main')")
    parser.add_argument("--keep-sandbox", action="store_true", help="Do not kill the sandbox at the end (for debugging)")
    args = parser.parse_args()

    cfg = load_config()
    result = run(cfg, source_branch=args.branch, do_merge=not args.no_merge, keep_sandbox=args.keep_sandbox)
    print_report(cfg, result)

    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    os.makedirs("outputs", exist_ok=True)
    out = f"outputs/report-{ts}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(
            {
                "branch_name": result.branch_name,
                "sandbox_id": result.sandbox_id,
                "sandbox_url": result.sandbox_url,
                "passed": result.passed,
                "merged": result.merged,
                "gate_blocked": result.gate_blocked,
                "gate_message": result.gate_message,
                "phases": result.phases,
                "validation": result.validation,
            },
            f,
            indent=2,
        )
    print(f"  Report saved to {out}")
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
