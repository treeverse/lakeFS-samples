#!/usr/bin/env python3
"""Agent job that runs inside an E2B sandbox.

This file is uploaded to the sandbox by the orchestrator and executed there.
It must be fully self-contained: only standard-library + pip-installed packages.
Do NOT import anything from lakefs_e2b_poc here.

Flow:
  1. Read raw/orders.csv from lakeFS via the S3 gateway.
  2. Clean / transform the data (pass scenario) or simulate a failure (fail scenario).
  3. Write output objects back to the lakeFS branch via the S3 gateway.
  4. Print a single JSON result object on the last line of stdout.
"""
from __future__ import annotations

import io
import json
import os
import sys
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config as BotocoreConfig

# ── column contract ────────────────────────────────────────────────────────────
REQUIRED_COLUMNS = {"order_id", "customer_id", "amount", "currency", "status", "created_at"}
REQUIRED_COLUMNS_ORDERED = ["order_id", "customer_id", "amount", "currency", "status", "created_at"]


# ── helpers ────────────────────────────────────────────────────────────────────

def _require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        _die(f"Missing required environment variable: {name!r}")
    return val


def _log(msg: str) -> None:
    """Progress messages go to stderr so stdout stays clean for the JSON result."""
    print(f"[agent] {msg}", file=sys.stderr, flush=True)


def _die(msg: str) -> None:
    _log(f"FATAL: {msg}")
    sys.exit(1)


def _make_s3():
    s3_endpoint = os.environ.get("LAKEFS_S3_ENDPOINT", "").strip() or _require_env("LAKEFS_ENDPOINT")
    return boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=_require_env("LAKEFS_ACCESS_KEY_ID"),
        aws_secret_access_key=_require_env("LAKEFS_SECRET_ACCESS_KEY"),
        region_name="us-east-1",
        config=BotocoreConfig(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def _put(s3, repo: str, key: str, body: bytes | str) -> None:
    if isinstance(body, str):
        body = body.encode("utf-8")
    s3.put_object(Bucket=repo, Key=key, Body=body)
    _log(f"wrote s3://{repo}/{key}")


# ── data cleaning (mirrors validation.py) ─────────────────────────────────────

def _clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    available = [c for c in REQUIRED_COLUMNS_ORDERED if c in df.columns]
    df = df[available]
    df = df[df["order_id"].notna() & (df["order_id"].astype(str).str.strip() != "")]
    df = df[df["customer_id"].notna() & (df["customer_id"].astype(str).str.strip() != "")]
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df[df["status"] == "PAID"]
    df = df[df["currency"] == "USD"]
    df = df[df["amount"] > 0]
    return df.reset_index(drop=True)


# ── scenarios ─────────────────────────────────────────────────────────────────

def _run_pass(s3, repo: str, branch: str) -> dict:
    input_key = f"{branch}/raw/orders.csv"
    _log(f"reading s3://{repo}/{input_key}")

    obj = s3.get_object(Bucket=repo, Key=input_key)
    raw_csv = obj["Body"].read().decode("utf-8")
    df_in = pd.read_csv(io.StringIO(raw_csv))
    _log(f"read {len(df_in)} rows")

    missing = REQUIRED_COLUMNS - set(df_in.columns)
    if missing:
        _die(f"schema error — missing columns: {sorted(missing)}")

    df_out = _clean_orders(df_in)
    _log(f"after cleaning: {len(df_out)} valid rows")

    # ── curated parquet ──────────────────────────────────────────────────────
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df_out), buf)
    parquet_key = f"{branch}/curated/orders.parquet"
    _put(s3, repo, parquet_key, buf.getvalue())

    # ── data-quality report ──────────────────────────────────────────────────
    ts = datetime.now(timezone.utc).isoformat()
    quality = {
        "status": "pass",
        "rows_in": len(df_in),
        "rows_out": len(df_out),
        "rows_dropped": len(df_in) - len(df_out),
        "filters": ["status==PAID", "currency==USD", "amount>0", "non_empty_ids"],
        "timestamp": ts,
    }
    quality_key = f"{branch}/reports/data_quality.json"
    _put(s3, repo, quality_key, json.dumps(quality, indent=2))

    # ── markdown summary ─────────────────────────────────────────────────────
    md = f"""\
# Agent Run Summary

**Scenario:** pass
**Timestamp:** {ts}
**Status:** PASSED ✓

## Data Quality

| Metric        | Value |
|---------------|-------|
| Rows in       | {len(df_in)} |
| Rows out      | {len(df_out)} |
| Rows dropped  | {len(df_in) - len(df_out)} |

## Filters Applied

- `status == "PAID"`
- `currency == "USD"`
- `amount > 0`
- `order_id` and `customer_id` non-empty

## Output Files

- `curated/orders.parquet`
- `reports/data_quality.json`
- `reports/agent_summary.md`
"""
    summary_key = f"{branch}/reports/agent_summary.md"
    _put(s3, repo, summary_key, md)

    return {
        "status": "pass",
        "rows_in": len(df_in),
        "rows_out": len(df_out),
        "outputs": [parquet_key, quality_key, summary_key],
    }


def _run_fail(s3, repo: str, branch: str) -> dict:
    """Reads data but applies an intentionally too-strict filter → 0 output rows → failure."""
    input_key = f"{branch}/raw/orders.csv"
    _log(f"reading s3://{repo}/{input_key}")

    obj = s3.get_object(Bucket=repo, Key=input_key)
    raw_csv = obj["Body"].read().decode("utf-8")
    df_in = pd.read_csv(io.StringIO(raw_csv))
    _log(f"read {len(df_in)} rows")

    # Apply a deliberately too-strict filter so no rows survive
    df_strict = df_in[pd.to_numeric(df_in.get("amount", pd.Series(dtype=float)), errors="coerce") > 1000]
    _log(f"after strict filter (amount > 1000): {len(df_strict)} rows")

    ts = datetime.now(timezone.utc).isoformat()
    reason = (
        f"Validation failed: expected > 0 output rows but got {len(df_strict)} "
        "(applied intentional strict filter: amount > 1000)."
    )
    _log(reason)

    quality = {
        "status": "fail",
        "rows_in": len(df_in),
        "rows_out": len(df_strict),
        "reason": reason,
        "timestamp": ts,
    }
    quality_key = f"{branch}/reports/data_quality.json"
    _put(s3, repo, quality_key, json.dumps(quality, indent=2))

    md = f"""\
# Agent Run Summary

**Scenario:** fail
**Timestamp:** {ts}
**Status:** FAILED ✗

## Reason

{reason}

## What happened

The agent read {len(df_in)} rows from `raw/orders.csv` but applied an intentionally
strict filter (`amount > 1000`) that produced **{len(df_strict)} valid rows**.

Because the output is empty the host orchestrator will refuse to merge this branch
into `main`, keeping the main data branch clean.

## Branch available for inspection

The branch remains open so you can inspect the partial outputs in the lakeFS UI.
"""
    summary_key = f"{branch}/reports/agent_summary.md"
    _put(s3, repo, summary_key, md)

    return {
        "status": "fail",
        "reason": reason,
        "outputs": [quality_key, summary_key],
    }


# ── entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    repo = _require_env("LAKEFS_REPO")
    branch = _require_env("LAKEFS_BRANCH")
    scenario = os.environ.get("SCENARIO", "pass").strip().lower()

    _log(f"starting — repo={repo}  branch={branch}  scenario={scenario}")

    s3 = _make_s3()

    if scenario == "pass":
        result = _run_pass(s3, repo, branch)
    elif scenario == "fail":
        result = _run_fail(s3, repo, branch)
    else:
        result = {"status": "fail", "reason": f"unknown scenario: {scenario!r}", "outputs": []}

    _log(f"done — status={result['status']}")

    # The orchestrator parses the last JSON line from stdout.
    print(json.dumps(result))


if __name__ == "__main__":
    main()
