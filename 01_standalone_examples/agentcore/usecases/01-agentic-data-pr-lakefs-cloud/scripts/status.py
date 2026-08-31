"""`make show-status` -- print the current run's state and source-branch integrity."""

from __future__ import annotations

import argparse

from common.runstate import RunState
from orchestrator.context import lakefs_client_from_env
from orchestrator.seeding import verify_source_unchanged


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    rs = RunState.load(args.run_id) if args.run_id else RunState.load_current()
    scope = rs.scope
    print(f"Run id:            {rs.run_id}")
    print(f"Stage:             {rs.stage}")
    print(f"Repository:        {rs.repository}")
    print(f"Source branch:     {rs.source_branch} @ {rs.source_commit}")
    print(f"Baseline branch:   {scope.baseline_branch} @ {rs.baseline_commit}")
    print(f"Workspace branch:  {scope.workspace_branch} @ {rs.workspace_commit}")
    print(f"Validation:        {rs.validation_status}")
    print(f"Pull Request:      {rs.pull_request_id} ({rs.pull_request_status})")
    print(f"AgentCore session: {rs.agentcore_session_id}")
    print(f"AgentCore trace:   {rs.agentcore_trace_id}")
    print(f"Gateway:           {rs.gateway_id or '-'}")
    print(f"Runtime:           {rs.runtime_arn or '-'}")

    try:
        client, _ = lakefs_client_from_env()
        unchanged = verify_source_unchanged(client, rs)
        print(f"\nSource-branch integrity: {'UNCHANGED (ok)' if unchanged else 'CHANGED (!!)'}")
    except Exception as exc:  # noqa: BLE001
        print(f"\nSource-branch integrity: could not verify ({exc})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
