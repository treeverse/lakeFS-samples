"""Upload the sample bad data CSV to lakeFS for the agent demo."""
from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from agent_demo.config import load_config
from lakefs_e2b_common.lakefs_client import commit, get_or_create_repo, make_client, write_object


def main() -> None:
    """Upload data/sample_bad_data.csv to raw/orders.csv on the main branch and commit."""
    config = load_config()

    print(f"  Connecting to lakeFS at {config.lakefs_endpoint}...")
    client = make_client(
        config.lakefs_endpoint,
        config.lakefs_access_key_id,
        config.lakefs_secret_access_key,
    )

    repo = get_or_create_repo(client, config.lakefs_repository, config.lakefs_storage_namespace)

    data_path = Path(__file__).parent.parent / "data" / "sample_bad_data.csv"
    csv_content = data_path.read_text(encoding="utf-8")
    row_count = len(csv_content.strip().splitlines()) - 1  # subtract header

    branch = repo.branch(config.lakefs_branch)
    object_path = "raw/orders.csv"

    print(f"  Uploading to {object_path} on branch '{config.lakefs_branch}'...")
    write_object(branch, object_path, csv_content)

    try:
        commit_id = commit(
            branch,
            message="Add sample bad data for agent demo",
            metadata={"source": "upload_sample_data.py", "rows": str(row_count)},
        )
        commit_label = commit_id
    except Exception as exc:
        if "no changes" in str(exc).lower():
            commit_label = "(already up to date — no new commit needed)"
        else:
            raise

    print("  Upload complete.")
    print()
    print("  Done.")
    print(f"    lakeFS repo   : {config.lakefs_repository}")
    print(f"    Branch        : {config.lakefs_branch}")
    print(f"    Object        : {object_path}  ({row_count} data rows)")
    print(f"    Commit        : {commit_label}")


if __name__ == "__main__":
    main()
