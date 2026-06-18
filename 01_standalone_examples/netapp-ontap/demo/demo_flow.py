#!/usr/bin/env python3
"""
Phase 1 Demo: lakeFS + NetApp ONTAP S3
Use case: Customer Churn — ML Feature Engineering with Dataset Versioning

Demonstrates:
  1. Repository creation backed by native ONTAP S3
  2. Ingesting a baseline ML training dataset
  3. Creating an experiment branch
  4. Engineering a new feature on the branch
  5. Committing with rich metadata
  6. Diff between branch and main
  7. Inspecting commit history
  8. Merging the validated feature back to main

Run from the demo/ directory (or project root):
    python3 demo/demo_flow.py

Prerequisites:
  - lakeFS running at http://localhost:8000
  - .env loaded (or env vars set directly)
  - See README.md for full setup steps.
"""

import os
import sys
import json
import time
import textwrap
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

# ─────────────────────────────────────────────────────────────────────────────
# Configuration — reads from environment (set via .env / source .env)
# ─────────────────────────────────────────────────────────────────────────────

LAKEFS_HOST       = os.getenv("LAKEFS_HOST",            "http://localhost:8000")
LAKEFS_ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY_ID",   "AKIAIOSFODNN7EXAMPLE")
LAKEFS_SECRET_KEY = os.getenv("LAKEFS_SECRET_ACCESS_KEY","wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
ONTAP_S3_BUCKET   = os.getenv("ONTAP_S3_BUCKET",        "lakefs-data")

REPO_NAME         = "churn-features"
MAIN_BRANCH       = "main"
EXP_BRANCH        = "feature-add-payment-history"
STORAGE_NS        = f"s3://{ONTAP_S3_BUCKET}/{REPO_NAME}"

BASE_URL          = f"{LAKEFS_HOST}/api/v1"
AUTH              = HTTPBasicAuth(LAKEFS_ACCESS_KEY, LAKEFS_SECRET_KEY)
SEED_DIR          = Path(__file__).parent / "seed_data"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def banner(title: str):
    width = 62
    print(f"\n╔{'═' * width}╗")
    print(f"║  {title:<{width - 2}}║")
    print(f"╚{'═' * width}╝")


def step(n: int, total: int, msg: str):
    print(f"\n── Step {n}/{total}: {msg} {'─' * max(1, 50 - len(msg))}")


def ok(msg: str):
    print(f"   ✓  {msg}")


def info(msg: str):
    print(f"   →  {msg}")


def fatal(msg: str):
    print(f"\n   ✗  ERROR: {msg}")
    print("      See README.md — Troubleshooting section.\n")
    sys.exit(1)


def api(method: str, path: str, **kwargs) -> requests.Response:
    """Authenticated lakeFS REST call. Raises on non-2xx."""
    url = f"{BASE_URL}{path}"
    resp = requests.request(method, url, auth=AUTH, timeout=30, **kwargs)
    if not resp.ok:
        print(f"\n   ✗  API {method} {path} → HTTP {resp.status_code}")
        try:
            detail = resp.json()
            print(f"      {json.dumps(detail, indent=6)}")
        except Exception:
            print(f"      {resp.text[:300]}")
        resp.raise_for_status()
    return resp


# ─────────────────────────────────────────────────────────────────────────────
# Demo steps
# ─────────────────────────────────────────────────────────────────────────────

def check_lakefs():
    """Verify lakeFS is running and credentials work."""
    try:
        api("GET", "/healthcheck")
    except requests.exceptions.ConnectionError:
        fatal(
            f"Cannot connect to lakeFS at {LAKEFS_HOST}\n"
            "      Is lakeFS running?  bash scripts/start-services.sh"
        )
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            fatal("Authentication failed — check LAKEFS_ACCESS_KEY_ID / LAKEFS_SECRET_ACCESS_KEY")
        raise

    resp = api("GET", "/config")
    cfg = resp.json()
    blockstore = cfg.get("storage_config", {}).get("blockstore_type", "unknown")
    ok(f"lakeFS is reachable at {LAKEFS_HOST}")
    ok(f"Blockstore type: {blockstore}  (should be 's3' → ONTAP)")


def create_repository():
    """Create (or re-create) the demo repository backed by ONTAP S3."""
    info(f"Repository  : {REPO_NAME}")
    info(f"Default branch: {MAIN_BRANCH}")

    # Delete if it already exists so the demo is idempotent
    try:
        api("DELETE", f"/repositories/{REPO_NAME}")
        info("Previous run detected — deleted existing repository (clean start)")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 404:
            raise

    # Try base namespace first, then increment version if namespace is already in use
    base_ns = STORAGE_NS
    storage_ns = base_ns
    for attempt in range(1, 20):
        try:
            api("POST", "/repositories", json={
                "name":              REPO_NAME,
                "storage_namespace": storage_ns,
                "default_branch":    MAIN_BRANCH,
            })
            break
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400 and "already in use" in e.response.text:
                storage_ns = f"{base_ns}-v{attempt + 1}"
                info(f"Namespace in use — retrying with: {storage_ns}")
            else:
                raise

    ok(f"Repository '{REPO_NAME}' created")
    ok(f"Data will be stored at: {storage_ns} (on ONTAP S3 bucket '{ONTAP_S3_BUCKET}')")


def upload_baseline():
    """Upload the baseline customer churn dataset to the main branch."""
    csv_file = SEED_DIR / "customers_baseline.csv"
    size_kb = csv_file.stat().st_size / 1024
    info(f"File: {csv_file.name}  ({size_kb:.1f} KB, 20 customer records)")
    info(f"Target: lakefs://{REPO_NAME}/{MAIN_BRANCH}/data/customers.csv")

    with open(csv_file, "rb") as fh:
        api(
            "POST",
            f"/repositories/{REPO_NAME}/branches/{MAIN_BRANCH}/objects",
            params={"path": "data/customers.csv"},
            files={"content": ("customers.csv", fh, "text/csv")},
        )
    ok("Uploaded data/customers.csv to main branch")


def commit_to_main() -> str:
    """Commit the baseline upload on main with descriptive metadata."""
    resp = api(
        "POST",
        f"/repositories/{REPO_NAME}/branches/{MAIN_BRANCH}/commits",
        json={
            "message": "feat: add baseline customer churn features (20 records, v1.0)",
            "metadata": {
                "source":     "crm_export",
                "version":    "1.0",
                "rows":       "20",
                "columns":    "tenure_months,monthly_charges,total_charges,contract_type,"
                              "payment_method,tech_support,internet_service,num_products,churned",
                "owner":      "data-team",
            },
        },
    )
    commit_id = resp.json()["id"]
    ok(f"Committed on main   → {commit_id[:16]}…")
    info("Metadata: source=crm_export, version=1.0, rows=20")
    return commit_id


def create_experiment_branch():
    """Branch off main for the payment-history feature experiment."""
    info(f"New branch  : {EXP_BRANCH}")
    info(f"From        : {MAIN_BRANCH}  (isolated copy, no data duplication)")

    api("POST", f"/repositories/{REPO_NAME}/branches", json={
        "name":   EXP_BRANCH,
        "source": MAIN_BRANCH,
    })
    ok(f"Branch '{EXP_BRANCH}' created")
    ok("lakeFS uses copy-on-write — zero extra storage until data changes")


def upload_experiment_data():
    """Upload the feature-engineered CSV to the experiment branch."""
    csv_file = SEED_DIR / "customers_experiment.csv"
    size_kb = csv_file.stat().st_size / 1024
    info(f"File: {csv_file.name}  ({size_kb:.1f} KB)")
    info("Change: added column 'payment_history_score' (engineered from billing data)")
    info(f"Target: lakefs://{REPO_NAME}/{EXP_BRANCH}/data/customers.csv")

    with open(csv_file, "rb") as fh:
        api(
            "POST",
            f"/repositories/{REPO_NAME}/branches/{EXP_BRANCH}/objects",
            params={"path": "data/customers.csv"},
            files={"content": ("customers.csv", fh, "text/csv")},
        )
    ok("Uploaded modified data/customers.csv to experiment branch")


def commit_on_branch() -> str:
    """Commit the feature engineering experiment on the branch."""
    resp = api(
        "POST",
        f"/repositories/{REPO_NAME}/branches/{EXP_BRANCH}/commits",
        json={
            "message": "feat: add payment_history_score feature (engineered from billing data)",
            "metadata": {
                "experiment_id":  "exp-2025-001",
                "feature_added":  "payment_history_score",
                "derivation":     "rolling_30d_on_time_payment_ratio",
                "auc_improvement": "0.03",
                "owner":          "data-team",
                "status":         "pending-review",
            },
        },
    )
    commit_id = resp.json()["id"]
    ok(f"Committed on branch → {commit_id[:16]}…")
    info("Metadata: experiment_id=exp-2025-001, feature=payment_history_score")
    return commit_id


def show_diff():
    """Show what changed between the experiment branch and main."""
    resp = api(
        "GET",
        f"/repositories/{REPO_NAME}/refs/{EXP_BRANCH}/diff/{MAIN_BRANCH}",
    )
    results = resp.json().get("results", [])

    if results:
        ok(f"{len(results)} change(s) detected:")
        for item in results:
            path       = item.get("path", "?")
            change     = item.get("type", "?").upper()
            size_bytes = item.get("size_bytes", None)
            size_str   = f"  ({size_bytes:,} bytes)" if size_bytes else ""
            print(f"       [{change:8s}]  {path}{size_str}")
    else:
        info("No differences (unexpected — check upload steps)")

    info("This diff proves the branch diverged from main with exactly one change.")


def show_commit_log():
    """Print the recent commit history of the experiment branch."""
    resp = api(
        "GET",
        f"/repositories/{REPO_NAME}/refs/{EXP_BRANCH}/commits",
        params={"amount": 5},
    )
    commits = resp.json().get("results", [])
    ok(f"Last {len(commits)} commit(s) on '{EXP_BRANCH}':")
    for c in commits:
        cid     = c.get("id", "")[:16]
        msg     = c.get("message", "")[:55]
        creator = c.get("committer", "")
        print(f"       {cid}…  {msg}")
        if creator:
            print(f"       {'':18}committer: {creator}")


def merge_to_main():
    """Merge the validated experiment branch back to main."""
    info(f"Merging '{EXP_BRANCH}' → '{MAIN_BRANCH}'")

    resp = api(
        "POST",
        f"/repositories/{REPO_NAME}/refs/{EXP_BRANCH}/merge/{MAIN_BRANCH}",
        json={"message": "merge: payment_history_score feature — validated, AUC +0.03"},
    )
    ref = resp.json().get("reference", "")
    ok(f"Merge committed     → {ref[:16]}…")


def verify_main_after_merge():
    """Confirm main now has the new feature and show the final log."""
    resp = api(
        "GET",
        f"/repositories/{REPO_NAME}/refs/{MAIN_BRANCH}/commits",
        params={"amount": 4},
    )
    commits = resp.json().get("results", [])
    ok(f"Main branch — last {len(commits)} commits:")
    for c in commits:
        cid = c.get("id", "")[:16]
        msg = c.get("message", "")[:55]
        print(f"       {cid}…  {msg}")

    # Confirm the file is present on main
    resp = api(
        "HEAD",
        f"/repositories/{REPO_NAME}/refs/{MAIN_BRANCH}/objects",
        params={"path": "data/customers.csv"},
    )
    ok("data/customers.csv is present on main (with payment_history_score column)")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    TOTAL = 11

    banner("lakeFS + NetApp ONTAP S3  |  Phase 1 Demo")
    print(f"\n   Use case : Customer Churn — ML Feature Engineering")
    print(f"   lakeFS   : {LAKEFS_HOST}")
    print(f"   Bucket   : s3://{ONTAP_S3_BUCKET}  (NetApp ONTAP native S3)")
    print(f"   Repo     : {REPO_NAME}")

    step(1, TOTAL, "Verifying lakeFS connectivity")
    check_lakefs()

    step(2, TOTAL, "Creating repository")
    create_repository()

    step(3, TOTAL, "Uploading baseline dataset → main branch")
    upload_baseline()

    step(4, TOTAL, "Committing baseline to main")
    main_commit = commit_to_main()

    step(5, TOTAL, "Creating experiment branch")
    create_experiment_branch()

    step(6, TOTAL, "Uploading feature-engineered dataset → experiment branch")
    upload_experiment_data()

    step(7, TOTAL, "Committing experiment on branch")
    exp_commit = commit_on_branch()

    step(8, TOTAL, "Diff: experiment branch vs main")
    show_diff()

    step(9, TOTAL, "Commit history on experiment branch")
    show_commit_log()

    step(10, TOTAL, "Merging experiment → main")
    merge_to_main()

    step(11, TOTAL, "Verifying main post-merge")
    verify_main_after_merge()

    # ── Done ──────────────────────────────────────────────────────────────────
    width = 62
    print(f"\n╔{'═' * width}╗")
    print(f"║  {'DEMO COMPLETE':^{width - 2}}  ║")
    print(f"╠{'═' * width}╣")
    lines = [
        f"Repository : {REPO_NAME}",
        f"Storage    : {STORAGE_NS}",
        f"           : (data physically on NetApp ONTAP S3)",
        f"UI         : {LAKEFS_HOST}/repositories",
    ]
    for line in lines:
        print(f"║  {line:<{width - 2}}║")
    print(f"╚{'═' * width}╝\n")


if __name__ == "__main__":
    main()
