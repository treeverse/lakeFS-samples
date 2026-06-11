"""Inspect objects and commits on a lakeFS branch.

Usage:
    uv run python -m lakefs_e2b_poc.inspect --ref main
    uv run python -m lakefs_e2b_poc.inspect --ref agent/run-20260515-120000-pass
"""
from __future__ import annotations

import argparse
import sys

import boto3
from botocore.config import Config as BotocoreConfig

from .config import load_config
from .lakefs_client import get_repo, make_client


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Inspect lakeFS objects and commits for a ref")
    p.add_argument("--ref", default="main", help="Branch name or commit SHA to inspect")
    return p.parse_args()


def _hr() -> None:
    print("─" * 60)


def main() -> None:
    args = _parse_args()
    config = load_config()

    client = make_client(
        config.lakefs_endpoint,
        config.lakefs_access_key_id,
        config.lakefs_secret_access_key,
    )
    repo = get_repo(client, config.lakefs_repo)

    print(f"\nRepo  : {config.lakefs_repo}")
    print(f"Ref   : {args.ref}")
    print(f"Host  : {config.lakefs_endpoint}\n")

    # ── list objects via S3 gateway ─────────────────────────────────────────
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

    _hr()
    print("Objects")
    _hr()

    prefixes = [f"{args.ref}/raw/", f"{args.ref}/curated/", f"{args.ref}/reports/"]
    found_any = False
    for prefix in prefixes:
        try:
            response = s3.list_objects_v2(Bucket=config.lakefs_repo, Prefix=prefix)
            for obj in response.get("Contents", []):
                found_any = True
                rel = obj["Key"][len(f"{args.ref}/"):]
                size = obj["Size"]
                print(f"  {rel:<50s}  {size:>8,} bytes")
        except Exception as exc:
            print(f"  ERROR listing {prefix}: {exc}", file=sys.stderr)

    if not found_any:
        print("  (no objects found under raw/, curated/, or reports/)")

    # ── recent commits ───────────────────────────────────────────────────────
    _hr()
    print("Recent commits")
    _hr()

    try:
        branch = repo.branch(args.ref)
        commits = list(branch.log(max_amount=5))
        if not commits:
            print("  (no commits)")
        for commit in commits:
            short_id = str(getattr(commit, "id", "?"))[:12]
            msg = getattr(commit, "message", "")
            committer = getattr(commit, "committer", "")
            meta = getattr(commit, "metadata", {}) or {}
            print(f"  {short_id}  {msg!r}")
            if committer:
                print(f"            committer: {committer}")
            if meta:
                for k, v in meta.items():
                    print(f"            {k}: {v}")
    except Exception as exc:
        err = str(exc).lower()
        if "not found" in err or "404" in err:
            print(f"  ERROR: ref '{args.ref}' does not exist.", file=sys.stderr)
        else:
            print(f"  ERROR: {exc}", file=sys.stderr)

    _hr()


if __name__ == "__main__":
    main()
