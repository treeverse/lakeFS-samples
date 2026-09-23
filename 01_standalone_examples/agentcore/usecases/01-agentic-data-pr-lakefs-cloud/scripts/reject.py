"""`make reject PR_ID=<id>` -- close the Data PR without merging."""

from __future__ import annotations

import argparse

from common.runstate import RunState
from orchestrator.approval import ApprovalError, reject


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("pr_id", nargs="?", default=None)
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    rs = RunState.load(args.run_id) if args.run_id else RunState.load_current()
    pr_id = args.pr_id or rs.pull_request_id
    if not pr_id:
        print("ERROR: no PR id. Pass PR_ID=<id>.")
        return 2
    try:
        reject(rs, pr_id)
    except ApprovalError as exc:
        print(f"ERROR: {exc}")
        return 1
    print(f"Closed PR {pr_id} without merging. Baseline and source are unchanged.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
