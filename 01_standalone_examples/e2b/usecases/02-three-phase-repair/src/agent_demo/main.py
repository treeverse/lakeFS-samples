"""Entry point for the AI Data Agent demo."""
from __future__ import annotations

import argparse
import sys
from datetime import datetime

from agent_demo.agent_loop import run
from agent_demo.config import load_config
from lakefs_e2b_common.lakefs_client import (
    create_branch,
    get_or_create_repo,
    make_client,
    merge_branch,
)
from agent_demo.reporting import print_final_report, save_report


def main() -> None:
    """Parse arguments, run the agent loop, and optionally merge the result."""
    parser = argparse.ArgumentParser(
        description="AI Data Agent: iterative LLM-driven data repair with E2B + lakeFS"
    )
    parser.add_argument(
        "--no-merge",
        action="store_true",
        help="Do not auto-merge into main even if validation passes",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=None,
        help="Override the maximum number of repair attempts",
    )
    parser.add_argument(
        "--branch",
        default=None,
        help="Source branch to read data from (default: value of LAKEFS_BRANCH or 'main')",
    )
    args = parser.parse_args()

    config = load_config()

    if args.max_attempts is not None:
        config.max_attempts = args.max_attempts

    source_branch = args.branch or config.lakefs_branch

    client = make_client(
        config.lakefs_endpoint,
        config.lakefs_access_key_id,
        config.lakefs_secret_access_key,
    )
    repo = get_or_create_repo(client, config.lakefs_repository, config.lakefs_storage_namespace)

    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    feature_branch_name = f"agent-run-{timestamp}"

    print(f"  Creating branch '{feature_branch_name}' from '{source_branch}'...")
    create_branch(repo, feature_branch_name, source_branch)
    print(f"  Branch created.")

    result = run(config, client, repo, feature_branch_name)

    print_final_report(result, config)

    report_path = f"outputs/report-{timestamp}.json"
    save_report(result, report_path)
    print(f"  Report saved to {report_path}")

    if result.passed and not args.no_merge:
        print(f"\n  Merging '{feature_branch_name}' into '{source_branch}'...")
        feature_branch = repo.branch(feature_branch_name)
        main_branch = repo.branch(source_branch)
        merge_ref = merge_branch(feature_branch, main_branch)
        print(f"  Merged successfully. Merge ref: {merge_ref}")
    elif not result.passed:
        print(
            f"\n  Branch not merged. Inspect with:\n"
            f"  uv run python -m agent_demo.main --inspect {feature_branch_name}"
        )


if __name__ == "__main__":
    main()
