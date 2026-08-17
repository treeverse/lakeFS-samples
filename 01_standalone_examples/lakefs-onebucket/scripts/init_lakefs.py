#!/usr/bin/env python3
"""
Initialize lakeFS for the OneBucket demo.

Modes (controlled by CREATE_LAKEFS_REPO in .env):

  CREATE_LAKEFS_REPO=true  (default)
    - Waits for lakeFS to be healthy and authenticated.
    - Creates the repository named LAKEFS_REPO.
    - Uses LAKEFS_STORAGE_NAMESPACE as-is if provided, or derives a safe default.
    - Idempotent: skips creation if the repository already exists.

  CREATE_LAKEFS_REPO=false
    - Waits for lakeFS to be healthy and authenticated.
    - Validates that LAKEFS_REPO already exists.
    - Prints the detected repository info.
"""

import os
import sys
import time
import urllib.parse
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

HEALTH_RETRIES = 30
HEALTH_DELAY = 5  # seconds between retries


def get_config() -> dict:
    return {
        "endpoint": os.environ.get("LAKEFS_ENDPOINT", "http://localhost:8000").rstrip("/"),
        "access_key_id": os.environ.get("LAKEFS_ACCESS_KEY_ID", ""),
        "secret_access_key": os.environ.get("LAKEFS_SECRET_ACCESS_KEY", ""),
        "repo": os.environ.get("LAKEFS_REPO", "onebucket-demo"),
        "branch": os.environ.get("LAKEFS_BRANCH", "main"),
        "storage_namespace": os.environ.get("LAKEFS_STORAGE_NAMESPACE", ""),
        "bucket": os.environ.get("ONEBUCKET_BUCKET", ""),
        "create_repo": os.environ.get("CREATE_LAKEFS_REPO", "true").lower() != "false",
    }


def redact(value: str) -> str:
    if not value:
        return "(empty)"
    return (value[:4] + "***") if len(value) > 6 else "***"


def api(endpoint: str, path: str) -> str:
    return f"{endpoint}/api/v1{path}"


# ── Health check ────────────────────────────────────────────────────────────

def wait_for_health(endpoint: str) -> None:
    print(f"Waiting for lakeFS at {endpoint}/_health ...")
    for attempt in range(1, HEALTH_RETRIES + 1):
        try:
            resp = requests.get(f"{endpoint}/_health", timeout=5)
            if resp.status_code == 200:
                print("  lakeFS is healthy.\n")
                return
        except (requests.ConnectionError, requests.Timeout):
            pass
        if attempt < HEALTH_RETRIES:
            print(f"  Not ready (attempt {attempt}/{HEALTH_RETRIES}), retrying in {HEALTH_DELAY}s...")
            time.sleep(HEALTH_DELAY)

    print("\nERROR: lakeFS did not become healthy after all retries.")
    print("  Check container logs: docker compose logs lakefs")
    sys.exit(1)


# ── Auth check ───────────────────────────────────────────────────────────────

def wait_for_auth(endpoint: str, key_id: str, secret: str) -> None:
    """
    Poll until credentials are accepted.

    Admin user bootstrap is handled by `make init` via `lakefs setup` before
    this script runs. A short retry loop here handles the brief window between
    setup completion and the credential cache becoming active.
    """
    print("Verifying lakeFS credentials...")
    auth = (key_id, secret)
    for attempt in range(1, HEALTH_RETRIES + 1):
        try:
            resp = requests.get(api(endpoint, "/repositories"), auth=auth, timeout=10)
            if resp.status_code == 200:
                print("  Credentials accepted.\n")
                return
            if resp.status_code in (401, 403):
                if attempt >= HEALTH_RETRIES:
                    print(f"\n  ERROR: Authentication failed (HTTP {resp.status_code}).")
                    print("  Check LAKEFS_ACCESS_KEY_ID and LAKEFS_SECRET_ACCESS_KEY in .env.")
                    print("  Check container logs: docker compose logs lakefs")
                    sys.exit(1)
        except (requests.ConnectionError, requests.Timeout):
            pass
        if attempt < HEALTH_RETRIES:
            time.sleep(HEALTH_DELAY)

    print("ERROR: Could not authenticate with lakeFS after all retries.")
    sys.exit(1)


# ── Storage namespace ─────────────────────────────────────────────────────────

def resolve_storage_namespace(ns: str, bucket: str, repo: str) -> str:
    if not ns:
        if not bucket:
            print("ERROR: LAKEFS_STORAGE_NAMESPACE is empty and ONEBUCKET_BUCKET is not set.")
            print("  Set LAKEFS_STORAGE_NAMESPACE or ONEBUCKET_BUCKET in .env.")
            sys.exit(1)
        derived = f"s3://{bucket}/lakefs/{repo}/"
        print(f"  LAKEFS_STORAGE_NAMESPACE not set — using derived default: {derived}")
        return derived
    return ns


def validate_storage_namespace(ns: str) -> None:
    if not ns.startswith("s3://"):
        print(f"ERROR: LAKEFS_STORAGE_NAMESPACE must start with s3://")
        print(f"  Got: {ns!r}")
        sys.exit(1)

    parsed = urllib.parse.urlparse(ns)
    path = parsed.path.lstrip("/").rstrip("/")

    if not path:
        # e.g. s3://my-bucket  or  s3://my-bucket/
        print(f"  WARNING: Storage namespace is the bucket root: {ns}")
        print("           lakeFS metadata and demo data will sit at the root.")
        print("           A prefix (e.g. s3://my-bucket/lakefs/my-repo/) is safer for cleanup.")


# ── Repository operations ────────────────────────────────────────────────────

def create_repository(endpoint: str, auth: tuple, repo: str, branch: str, ns: str) -> None:
    print(f"Creating lakeFS repository: {repo}")
    print(f"  Storage namespace: {ns}")
    print(f"  Default branch:    {branch}")

    resp = requests.post(
        api(endpoint, "/repositories"),
        auth=auth,
        json={"name": repo, "storage_namespace": ns, "default_branch": branch},
        timeout=30,
    )

    if resp.status_code == 201:
        data = resp.json()
        print("\nRepository created successfully.")
        _print_repo_data(data)
    elif resp.status_code == 409:
        print("  Repository already exists — skipping creation (idempotent).")
        _fetch_and_print_repo(endpoint, auth, repo)
    else:
        print(f"\nERROR: Failed to create repository (HTTP {resp.status_code})")
        _print_api_error(resp)
        sys.exit(1)


def validate_existing_repository(endpoint: str, auth: tuple, repo: str) -> None:
    print(f"Validating existing repository: {repo}")
    resp = requests.get(api(endpoint, f"/repositories/{repo}"), auth=auth, timeout=10)
    if resp.status_code == 200:
        print("  Repository found.")
        _print_repo_data(resp.json())
    elif resp.status_code == 404:
        print(f"\nERROR: Repository '{repo}' was not found in lakeFS.")
        print("  Options:")
        print("    1. Set CREATE_LAKEFS_REPO=true in .env to auto-create it, or")
        print("    2. Create the repository manually via the lakeFS UI or API.")
        sys.exit(1)
    else:
        print(f"\nERROR: Unexpected response (HTTP {resp.status_code})")
        _print_api_error(resp)
        sys.exit(1)


def _fetch_and_print_repo(endpoint: str, auth: tuple, repo: str) -> None:
    try:
        resp = requests.get(api(endpoint, f"/repositories/{repo}"), auth=auth, timeout=10)
        if resp.status_code == 200:
            _print_repo_data(resp.json())
    except Exception:
        pass


def _print_repo_data(data: dict) -> None:
    print(f"  Name:              {data.get('id', '?')}")
    print(f"  Storage namespace: {data.get('storage_namespace', '?')}")
    print(f"  Default branch:    {data.get('default_branch', '?')}")


def _print_api_error(resp: requests.Response) -> None:
    try:
        print(f"  Detail: {resp.json()}")
    except Exception:
        print(f"  Body:   {resp.text[:300]}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = get_config()

    print("=" * 60)
    print("  lakeFS initialization")
    print("=" * 60)
    print(f"\n  Endpoint:    {cfg['endpoint']}")
    print(f"  Access key:  {redact(cfg['access_key_id'])}")
    print(f"  Repository:  {cfg['repo']}")
    print(f"  Branch:      {cfg['branch']}")
    print(f"  Create repo: {cfg['create_repo']}")
    print()

    if not cfg["access_key_id"] or not cfg["secret_access_key"]:
        print("ERROR: LAKEFS_ACCESS_KEY_ID or LAKEFS_SECRET_ACCESS_KEY is not set in .env.")
        sys.exit(1)

    wait_for_health(cfg["endpoint"])
    wait_for_auth(cfg["endpoint"], cfg["access_key_id"], cfg["secret_access_key"])

    auth = (cfg["access_key_id"], cfg["secret_access_key"])

    if cfg["create_repo"]:
        ns = resolve_storage_namespace(cfg["storage_namespace"], cfg["bucket"], cfg["repo"])
        validate_storage_namespace(ns)
        create_repository(cfg["endpoint"], auth, cfg["repo"], cfg["branch"], ns)
    else:
        validate_existing_repository(cfg["endpoint"], auth, cfg["repo"])

    print()
    print("=" * 60)
    print(f"  lakeFS UI: {cfg['endpoint']}/repositories/{cfg['repo']}/objects")
    print("=" * 60)
    print()
    print("Next step: make demo")


if __name__ == "__main__":
    main()
