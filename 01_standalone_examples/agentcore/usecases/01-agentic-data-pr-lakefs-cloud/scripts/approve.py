"""`make approve PR_ID=<id>` -- human approval that merges the Data PR.

Calls lakeFS Cloud directly (not the AgentCore Gateway). Verifies ownership,
source/destination, workspace-vs-validated commit (race detection), re-runs
deterministic validation, shows the diff, requires explicit confirmation, merges,
and verifies the baseline changed while the source branch did not.
"""

from __future__ import annotations

import argparse
import sys

from common.runstate import RunState
from orchestrator.approval import ApprovalError, approve


def _confirm(ctx: dict) -> bool:
    diff = ctx["diff"]["summary"]
    val = ctx["validation"]
    pr = ctx["pull_request"]
    print("\n--- Pull Request review ---")
    print(f"PR:          {pr.get('id')}  ({pr.get('status')})")
    print(f"Source:      {pr.get('source_branch')}")
    print(f"Destination: {pr.get('destination_branch')}")
    print(f"Diff:        +{diff.get('added',0)} added / "
          f"{diff.get('changed',0)} changed / {diff.get('removed',0)} removed")
    print(f"Validation:  {'PASSED' if val['valid'] else 'FAILED'} "
          f"({val['reconciliation']['curated']} curated, "
          f"{val['reconciliation']['quarantined']} quarantined)")
    print(f"Review URL:  {ctx['web_url']}")
    if not sys.stdin.isatty():
        print("\nNon-interactive: set APPROVE_CONFIRM=yes to auto-confirm.")
        import os

        return os.getenv("APPROVE_CONFIRM", "").lower() in {"y", "yes"}
    return input("\nType 'merge' to approve and merge: ").strip().lower() == "merge"


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
        result = approve(rs, pr_id, confirm=_confirm)
    except ApprovalError as exc:
        print(f"\nApproval aborted: {exc}")
        return 1

    print("\nMerged.")
    print(f"Baseline: {result['baseline_before']} -> {result['baseline_after']}")
    print(f"Source branch unchanged: {result['source_unchanged']} "
          f"(HEAD {result['source_commit']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
