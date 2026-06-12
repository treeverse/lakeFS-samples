"""Seed demo data into the lakeFS repository.

Usage:
    uv run python -m lakefs_e2b_poc.seed
"""
from __future__ import annotations

import sys

import boto3
from botocore.config import Config as BotocoreConfig

from .config import load_config
from .lakefs_client import commit_branch, ensure_repo_exists, list_uncommitted, make_client

SEED_CSV = """\
order_id,customer_id,amount,currency,status,created_at
1001,c001,120.50,USD,PAID,2026-05-01
1002,c002,-7.00,USD,PAID,2026-05-01
1003,c003,50.00,EUR,PAID,2026-05-02
1004,c004,,USD,CANCELLED,2026-05-03
1005,c005,200.00,USD,PAID,2026-05-04
1006,,15.00,USD,PAID,2026-05-04
"""


def main() -> None:
    config = load_config()

    print(f"Connecting to lakeFS at {config.lakefs_endpoint} ...")
    client = make_client(
        config.lakefs_endpoint,
        config.lakefs_access_key_id,
        config.lakefs_secret_access_key,
    )

    repo = ensure_repo_exists(client, config.lakefs_repo, config.lakefs_storage_namespace)
    print(f"Repository '{config.lakefs_repo}' is ready.")

    # Verify the main branch exists
    branch = repo.branch(config.lakefs_main_branch)
    try:
        branch.get_commit()
        print(f"Branch '{config.lakefs_main_branch}' exists.")
    except Exception as exc:
        print(f"ERROR: Cannot access branch '{config.lakefs_main_branch}': {exc}", file=sys.stderr)
        sys.exit(1)

    # Upload seed CSV via the S3 gateway (writes to the branch's staging area)
    s3 = boto3.client(
        "s3",
        endpoint_url=config.lakefs_s3_endpoint,
        aws_access_key_id=config.lakefs_access_key_id,
        aws_secret_access_key=config.lakefs_secret_access_key,
        region_name="us-east-1",
        config=BotocoreConfig(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            s3={"addressing_style": "path"},
        ),
    )

    key = f"{config.lakefs_main_branch}/raw/orders.csv"
    print(f"Uploading s3://{config.lakefs_repo}/{key} ...")
    s3.put_object(Bucket=config.lakefs_repo, Key=key, Body=SEED_CSV.encode("utf-8"))
    print("Upload complete.")

    # Only commit if there are staged changes (idempotency)
    changes = list_uncommitted(branch)
    if not changes:
        print("No uncommitted changes — seed already applied, nothing to commit.")
        return

    print(f"Committing {len(changes)} staged change(s) ...")
    ref = commit_branch(
        branch,
        "Seed demo orders data for E2B/lakeFS POC",
        {"source": "seed"},
    )
    print(f"Committed: {ref}")
    print("\nSeed complete.")
    print(f"  lakeFS repo : {config.lakefs_repo}")
    print(f"  branch      : {config.lakefs_main_branch}")
    print(f"  object      : raw/orders.csv  ({len(SEED_CSV)} bytes, {len(SEED_CSV.splitlines())-1} data rows)")


if __name__ == "__main__":
    main()
