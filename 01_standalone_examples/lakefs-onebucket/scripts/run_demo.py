#!/usr/bin/env python3
"""
lakeFS + OneBucket demo workflow.

All data access goes through the lakeFS S3 Gateway and REST API.
Nothing is written directly to OneBucket in this script.

Flow:
  1. Upload raw/customers.csv  → main branch (via lakeFS S3 Gateway)
  2. Commit on main
  3. Create 'experiment' branch from main
  4. Upload processed/customers_clean.csv → experiment branch (via lakeFS S3 Gateway)
  5. Commit on experiment
  6. [optional] Merge experiment → main  (MERGE_EXPERIMENT=true)
  7. List objects on both branches
  8. Read objects back
"""

import os
import sys
from pathlib import Path

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env", override=True)

# ── Sample data ───────────────────────────────────────────────────────────────

RAW_CSV = b"""\
id,name,email,signup_date
1,Alice Smith,alice@example.com,2024-01-10
2,Bob Jones,bob@example.com,2024-02-14
3,Carol White,carol@example.com,2024-03-22
"""

CLEAN_CSV = b"""\
id,name,email,signup_year
1,Alice Smith,alice@example.com,2024
2,Bob Jones,bob@example.com,2024
3,Carol White,carol@example.com,2024
"""

EXPERIMENT_BRANCH = "experiment"


# ── Config ────────────────────────────────────────────────────────────────────

def get_config() -> dict:
    return {
        "endpoint": os.environ.get("LAKEFS_ENDPOINT", "http://localhost:8000").rstrip("/"),
        "access_key": os.environ.get("LAKEFS_ACCESS_KEY_ID", ""),
        "secret_key": os.environ.get("LAKEFS_SECRET_ACCESS_KEY", ""),
        "repo": os.environ.get("LAKEFS_REPO", "onebucket-demo"),
        "branch": os.environ.get("LAKEFS_BRANCH", "main"),
        "merge": os.environ.get("MERGE_EXPERIMENT", "false").lower() == "true",
    }


# ── S3 client (lakeFS Gateway) ────────────────────────────────────────────────

def make_lakefs_s3_client(endpoint: str, access_key: str, secret_key: str) -> boto3.client:
    """
    boto3 client pointed at the lakeFS S3 Gateway.

    - addressing_style=path: lakeFS gateway requires path-style URLs.
    - checksum settings: disable automatic checksum negotiation that some
      lakeFS gateway versions reject.
    """
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


# ── lakeFS REST API helpers ───────────────────────────────────────────────────

def api(endpoint: str, path: str) -> str:
    return f"{endpoint}/api/v1{path}"


def lfs_commit(endpoint: str, auth: tuple, repo: str, branch: str, message: str) -> str:
    resp = requests.post(
        api(endpoint, f"/repositories/{repo}/branches/{branch}/commits"),
        auth=auth,
        json={"message": message},
        timeout=30,
    )
    if resp.status_code not in (200, 201):
        print(f"  ERROR: Commit failed (HTTP {resp.status_code}): {resp.text[:200]}")
        sys.exit(1)
    return resp.json().get("id", "unknown")


def lfs_create_branch(endpoint: str, auth: tuple, repo: str, name: str, source: str) -> None:
    resp = requests.post(
        api(endpoint, f"/repositories/{repo}/branches"),
        auth=auth,
        json={"name": name, "source": source},
        timeout=10,
    )
    if resp.status_code == 409:
        print(f"  Branch '{name}' already exists — reusing.")
        return
    if resp.status_code not in (200, 201):
        print(f"  ERROR: Create branch failed (HTTP {resp.status_code}): {resp.text[:200]}")
        sys.exit(1)


def lfs_merge(endpoint: str, auth: tuple, repo: str, source: str, dest: str) -> str:
    resp = requests.post(
        api(endpoint, f"/repositories/{repo}/refs/{source}/merge/{dest}"),
        auth=auth,
        json={},
        timeout=30,
    )
    if resp.status_code not in (200, 201):
        print(f"  ERROR: Merge failed (HTTP {resp.status_code}): {resp.text[:200]}")
        sys.exit(1)
    return resp.json().get("reference", "unknown")


# ── S3 helpers (via lakeFS gateway) ──────────────────────────────────────────

def put_object(s3: boto3.client, repo: str, key: str, body: bytes) -> None:
    try:
        s3.put_object(Bucket=repo, Key=key, Body=body)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        msg = exc.response["Error"].get("Message", str(exc))
        print(f"  ERROR: PutObject failed: {code} — {msg}")
        sys.exit(1)


def list_branch_objects(s3: boto3.client, repo: str, branch: str) -> list[str]:
    """Return the logical paths (strip the branch/ prefix) of all objects on a branch."""
    prefix = f"{branch}/"
    paginator = s3.get_paginator("list_objects_v2")
    paths: list[str] = []
    try:
        for page in paginator.paginate(Bucket=repo, Prefix=prefix):
            for obj in page.get("Contents", []):
                paths.append(obj["Key"][len(prefix):])
    except ClientError as exc:
        print(f"  WARNING: ListObjects failed: {exc.response['Error']['Code']}")
    return paths


def read_object(s3: boto3.client, repo: str, key: str) -> bytes | None:
    try:
        resp = s3.get_object(Bucket=repo, Key=key)
        return resp["Body"].read()
    except ClientError as exc:
        print(f"  WARNING: GetObject '{key}' failed: {exc.response['Error']['Code']}")
        return None


# ── Demo steps ────────────────────────────────────────────────────────────────

def sep() -> None:
    print("-" * 60)


def main() -> None:
    cfg = get_config()
    endpoint = cfg["endpoint"]
    repo = cfg["repo"]
    main_branch = cfg["branch"]
    auth = (cfg["access_key"], cfg["secret_key"])

    print("=" * 60)
    print("  lakeFS + OneBucket demo")
    print("=" * 60)
    print(f"  lakeFS endpoint:  {endpoint}")
    print(f"  Repository:       {repo}")
    print(f"  Main branch:      {main_branch}")
    print(f"  Merge experiment: {cfg['merge']}")
    print("=" * 60)
    print()

    if not cfg["access_key"] or not cfg["secret_key"]:
        print("ERROR: LAKEFS_ACCESS_KEY_ID or LAKEFS_SECRET_ACCESS_KEY is not set in .env.")
        sys.exit(1)

    s3 = make_lakefs_s3_client(endpoint, cfg["access_key"], cfg["secret_key"])

    # ── Step 1: upload raw CSV to main ───────────────────────────────────────
    sep()
    raw_key = f"{main_branch}/raw/customers.csv"
    print(f"[1] Writing object through lakeFS S3 Gateway:")
    print(f"    Bucket: {repo}  Key: {raw_key}")
    put_object(s3, repo, raw_key, RAW_CSV)
    print("    OK")

    # ── Step 2: commit on main ───────────────────────────────────────────────
    sep()
    print(f"[2] Committing to '{main_branch}'...")
    main_commit = lfs_commit(endpoint, auth, repo, main_branch, "Add raw customer data")
    print(f"    Commit ID: {main_commit}")

    # ── Step 3: create experiment branch ─────────────────────────────────────
    sep()
    print(f"[3] Creating branch '{EXPERIMENT_BRANCH}' from '{main_branch}'...")
    lfs_create_branch(endpoint, auth, repo, EXPERIMENT_BRANCH, main_branch)
    print("    OK")

    # ── Step 4: upload processed CSV to experiment ────────────────────────────
    sep()
    clean_key = f"{EXPERIMENT_BRANCH}/processed/customers_clean.csv"
    print(f"[4] Writing transformed object through lakeFS S3 Gateway:")
    print(f"    Bucket: {repo}  Key: {clean_key}")
    put_object(s3, repo, clean_key, CLEAN_CSV)
    print("    OK")

    # ── Step 5: commit on experiment ─────────────────────────────────────────
    sep()
    print(f"[5] Committing to '{EXPERIMENT_BRANCH}'...")
    exp_commit = lfs_commit(endpoint, auth, repo, EXPERIMENT_BRANCH, "Add processed customer data")
    print(f"    Commit ID: {exp_commit}")

    # ── Step 6: optional merge ────────────────────────────────────────────────
    merge_ref: str | None = None
    sep()
    if cfg["merge"]:
        print(f"[6] Merging '{EXPERIMENT_BRANCH}' → '{main_branch}'...")
        merge_ref = lfs_merge(endpoint, auth, repo, EXPERIMENT_BRANCH, main_branch)
        print(f"    Merge commit: {merge_ref}")
    else:
        print(f"[6] Skipping merge (set MERGE_EXPERIMENT=true in .env to enable)")

    # ── Step 7: list objects on both branches ─────────────────────────────────
    sep()
    print(f"[7] Objects on '{main_branch}':")
    main_objects = list_branch_objects(s3, repo, main_branch)
    for path in main_objects:
        print(f"    {path}")
    if not main_objects:
        print("    (none)")

    print(f"\n    Objects on '{EXPERIMENT_BRANCH}':")
    exp_objects = list_branch_objects(s3, repo, EXPERIMENT_BRANCH)
    for path in exp_objects:
        print(f"    {path}")
    if not exp_objects:
        print("    (none)")

    # ── Step 8: read back ─────────────────────────────────────────────────────
    sep()
    print("[8] Reading objects back...")
    raw_data = read_object(s3, repo, raw_key)
    if raw_data is not None:
        print(f"    {raw_key}: {len(raw_data)} bytes — OK")
    clean_data = read_object(s3, repo, clean_key)
    if clean_data is not None:
        print(f"    {clean_key}: {len(clean_data)} bytes — OK")

    # ── Summary ───────────────────────────────────────────────────────────────
    sep()
    print()
    print("=" * 60)
    print("  Demo Summary")
    print("=" * 60)
    print(f"  Repository:            {repo}")
    print(f"  Main commit:           {main_commit}")
    print(f"  Experiment commit:     {exp_commit}")
    if merge_ref:
        print(f"  Merge commit:          {merge_ref}")
    print(f"  Objects on main:       {len(main_objects)}")
    print(f"  Objects on experiment: {len(exp_objects)}")
    print()
    print("  Demo completed successfully.")
    print()
    print(f"  lakeFS UI: {endpoint}/repositories/{repo}/objects")
    print("=" * 60)


if __name__ == "__main__":
    main()
