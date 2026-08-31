"""`make show-pr PR_ID=<id>` -- show a Data PR's status, diff, validation, URL."""

from __future__ import annotations

import argparse
import json

from common.runstate import RunState
from gateway.handler import Capabilities
from orchestrator.context import lakefs_client_from_env


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

    client, _ = lakefs_client_from_env()
    caps = Capabilities(
        client,
        run_id=rs.run_id,
        repository=rs.repository,
        source_branch=rs.source_branch,
        source_commit=rs.source_commit,
        baseline_commit=rs.baseline_commit,
        session_id="show-pr",
        trace_id="show-pr",
    )
    info = caps.get_data_pull_request({"pull_request_id": pr_id})
    print(json.dumps(info, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
