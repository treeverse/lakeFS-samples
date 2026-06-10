"""Orchestrate an agentic data PR: lakeFS branch + E2B sandbox.

Usage:
    uv run python -m lakefs_e2b_poc.orchestrator --scenario pass --merge
    uv run python -m lakefs_e2b_poc.orchestrator --scenario fail
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
from datetime import datetime, timezone

from e2b import Sandbox

from .config import check_endpoint_not_localhost, load_config
from .lakefs_client import (
    commit_branch,
    create_branch,
    delete_branch,
    get_repo,
    list_uncommitted,
    make_client,
    merge_into_main,
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run an agentic data PR with E2B + lakeFS")
    p.add_argument("--scenario", choices=["pass", "fail"], default="pass", help="Which scenario to run")
    p.add_argument("--merge", action="store_true", help="Merge into main if the scenario passes")
    p.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete the feature branch after a successful merge",
    )
    return p.parse_args()


def _agent_job_source() -> str:
    return (pathlib.Path(__file__).parent / "agent_job.py").read_text()


def _parse_result(stdout: str) -> dict:
    """Find the last JSON object in stdout."""
    for line in reversed(stdout.strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    return {"status": "fail", "reason": "Agent produced no parseable JSON output", "outputs": []}


def _section(title: str) -> None:
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


def main() -> None:
    args = _parse_args()
    config = load_config()
    check_endpoint_not_localhost(config)

    # ── 1. Create branch name ──────────────────────────────────────────────────
    now = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    branch_name = f"agent-run-{now}-{args.scenario}"

    _section("lakeFS: create branch")
    print(f"  Connecting to {config.lakefs_endpoint} ...")
    client = make_client(
        config.lakefs_endpoint,
        config.lakefs_access_key_id,
        config.lakefs_secret_access_key,
    )
    repo = get_repo(client, config.lakefs_repo)

    print(f"  Creating '{branch_name}' from '{config.lakefs_main_branch}' ...")
    branch = create_branch(repo, branch_name, config.lakefs_main_branch)
    print(f"  Branch created.")

    # ── 2. E2B sandbox ────────────────────────────────────────────────────────
    _section("E2B: start sandbox")
    sandbox = None
    result: dict = {"status": "fail", "reason": "Sandbox did not complete", "outputs": []}
    sandbox_id: str | None = None

    try:
        sandbox = Sandbox.create(
            envs={
                "LAKEFS_ENDPOINT": config.lakefs_endpoint,
                "LAKEFS_S3_ENDPOINT": config.lakefs_s3_endpoint,
                "LAKEFS_ACCESS_KEY_ID": config.lakefs_access_key_id,
                "LAKEFS_SECRET_ACCESS_KEY": config.lakefs_secret_access_key,
                "LAKEFS_REPO": config.lakefs_repo,
                "LAKEFS_BRANCH": branch_name,
                "SCENARIO": args.scenario,
            }
        )
        sandbox_id = getattr(sandbox, "sandbox_id", None) or getattr(sandbox, "id", None)
        print(f"  Sandbox ID : {sandbox_id}")

        # ── 3. Upload agent_job.py ─────────────────────────────────────────────
        _section("E2B: upload agent_job.py")
        sandbox.files.write("/home/user/agent_job.py", _agent_job_source())
        print("  agent_job.py uploaded.")

        # ── 4. Install runtime deps ────────────────────────────────────────────
        _section("E2B: install dependencies")
        install = sandbox.commands.run(
            "python -m pip install -q pandas pyarrow boto3 botocore",
            timeout=180,
        )
        if install.exit_code != 0:
            print(f"  WARNING: pip install exited {install.exit_code}")
            if install.stderr:
                print(install.stderr)
        else:
            print("  Dependencies installed.")

        # ── 5. Run agent ───────────────────────────────────────────────────────
        _section(f"E2B: run agent (scenario={args.scenario})")
        run = sandbox.commands.run(
            "python /home/user/agent_job.py",
            timeout=300,
        )

        if run.stderr:
            print("  [stderr]")
            for line in run.stderr.strip().splitlines():
                print(f"    {line}")

        print(f"\n  [stdout]\n    {run.stdout.strip() if run.stdout else '(empty)'}")
        print(f"\n  Exit code: {run.exit_code}")

        result = _parse_result(run.stdout or "")

    except Exception as exc:
        print(f"\n  ERROR: {exc}", file=sys.stderr)
        result = {"status": "fail", "reason": str(exc), "outputs": []}

    finally:
        if sandbox is not None:
            try:
                sandbox.kill()
            except Exception:
                pass

    # ── 6. Commit lakeFS branch ────────────────────────────────────────────────
    _section("lakeFS: uncommitted changes")
    changes = list_uncommitted(branch)
    if changes:
        for c in changes:
            print(f"  {c.type:10s}  {c.path}")
    else:
        print("  (none)")

    _section("lakeFS: commit branch")
    metadata: dict[str, str] = {
        "scenario": args.scenario,
        "validation_status": result.get("status", "unknown"),
    }
    if sandbox_id:
        metadata["e2b_sandbox_id"] = sandbox_id
    if "rows_in" in result:
        metadata["rows_in"] = str(result["rows_in"])
    if "rows_out" in result:
        metadata["rows_out"] = str(result["rows_out"])

    commit_ref = commit_branch(
        branch,
        f"Agent run {args.scenario} from E2B sandbox",
        metadata,
    )
    print(f"  Committed: {commit_ref}")

    # ── 7. Merge or report failure ────────────────────────────────────────────
    if result.get("status") == "pass" and args.merge:
        _section("lakeFS: merge into main")
        main_branch = repo.branch(config.lakefs_main_branch)
        merge_result = merge_into_main(branch, main_branch)
        print(f"  Merged '{branch_name}' → '{config.lakefs_main_branch}'")
        print(f"  Merge result: {merge_result}")

        if args.cleanup:
            print(f"  Deleting branch '{branch_name}' (--cleanup) ...")
            delete_branch(branch)
            print("  Branch deleted.")
        else:
            print(f"\n  Branch '{branch_name}' kept for inspection (pass --cleanup to delete).")

    elif result.get("status") == "fail":
        _section("Result: FAILED — branch NOT merged")
        print(f"  Reason  : {result.get('reason', 'unknown')}")
        print(f"  Branch  : {branch_name}")
        print(f"  Inspect : uv run python -m lakefs_e2b_poc.inspect --ref {branch_name!r}")

    else:
        _section("Result: PASSED — no merge requested")
        print(f"  Branch  : {branch_name}")
        print("  (Re-run with --merge to merge into main.)")

    _section("Done")
    print(f"  status   : {result.get('status')}")
    if sandbox_id:
        print(f"  sandbox  : {sandbox_id}")
    print(f"  branch   : {branch_name}")


if __name__ == "__main__":
    main()
