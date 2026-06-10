"""Final report printing and saving utilities."""
from __future__ import annotations

import json
import os

from agent_demo.agent_loop import AgentRunResult
from agent_demo.config import Config


def print_final_report(result: AgentRunResult, config: Config) -> None:
    """Print a human-readable summary of the agent run to stdout."""
    divider = "─" * 60
    print(f"\n{divider}")
    print("  Final Report")
    print(divider)
    print(f"  Branch      : {result.branch_name}")
    print(f"  Total attempts: {result.total_attempts}")
    print(f"  Outcome     : {'PASSED' if result.passed else 'FAILED'}")
    print()

    for a in result.attempts:
        status_label = "PASS" if a["status"] == "passed" else "FAIL"
        print(
            f"  Attempt {a['attempt']}: [{status_label}]  "
            f"commit={a['commit_id'][:12]}  "
            f"{a['validation_summary']}"
        )

    if result.passed and result.final_commit_id:
        print()
        print(f"  Final commit: {result.final_commit_id}")
        print(
            f"  Merge with : uv run python -m agent_demo.main"
            f" --branch {config.lakefs_branch}"
        )
        print(
            f"  lakeFS UI  : {config.lakefs_endpoint}/repositories/"
            f"{config.lakefs_repository}/objects?ref={result.branch_name}"
        )
    else:
        print()
        print(f"  All {config.max_attempts} attempts failed.")
        print(f"  Inspect branch: {result.branch_name}")
        print(
            f"  lakeFS UI  : {config.lakefs_endpoint}/repositories/"
            f"{config.lakefs_repository}/objects?ref={result.branch_name}"
        )
    print(divider)


def save_report(result: AgentRunResult, output_path: str) -> None:
    """Save the run result as JSON to output_path."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    data = {
        "branch_name": result.branch_name,
        "passed": result.passed,
        "total_attempts": result.total_attempts,
        "attempts": result.attempts,
        "final_commit_id": result.final_commit_id,
        "final_validation": (
            result.final_validation.to_dict() if result.final_validation else None
        ),
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
