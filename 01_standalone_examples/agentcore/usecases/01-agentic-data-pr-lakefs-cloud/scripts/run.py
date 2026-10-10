"""`make run` -- invoke the deployed AgentCore agent for the current run.

`make run-local` (``--local``) runs the curation deterministically through the
same curated capabilities without AWS/Bedrock, so the lakeFS Pull Request flow
can be demonstrated end-to-end without deploying AgentCore.
"""

from __future__ import annotations

import argparse

from common.runstate import RunState
from orchestrator.curation_runner import run_local_curation, run_remote_agent


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local",
        action="store_true",
        help="Run curation deterministically without AgentCore/Bedrock.",
    )
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    rs = RunState.load(args.run_id) if args.run_id else RunState.load_current()
    print(f"Run id: {rs.run_id}  |  repository: {rs.repository}")

    if args.local:
        print("Running LOCAL deterministic curation (no AgentCore/Bedrock)...\n")
        rs = run_local_curation(rs)
        print(f"Validation:        {rs.validation_status}")
        print(f"Workspace commit:  {rs.workspace_commit}")
        print(f"Pull Request id:   {rs.pull_request_id}  (status: {rs.pull_request_status})")
        print(f"Trace id:          {rs.agentcore_trace_id}")
        print("\nPR targets the baseline branch (never main).")
        print(f"Next: review, then `make approve PR_ID={rs.pull_request_id}`.")
        return 0

    print("Invoking the deployed AgentCore Runtime agent...\n")
    result = run_remote_agent(rs)
    print(result.get("response", ""))
    rs = RunState.load(rs.run_id)
    if rs.pull_request_id:
        print(f"\nPull Request id: {rs.pull_request_id}")
        print(f"Next: `make show-pr PR_ID={rs.pull_request_id}` then `make approve ...`.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
