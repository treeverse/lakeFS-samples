"""Pure-Python validation and cleaning logic — no lakeFS or E2B dependencies.

These functions are used by both agent_job.py (inside the E2B sandbox) and
the test suite.  agent_job.py duplicates this logic so it can run as a
standalone script without the lakefs_e2b_poc package installed.
"""
from __future__ import annotations

import pandas as pd

REQUIRED_COLUMNS = {"order_id", "customer_id", "amount", "currency", "status", "created_at"}
REQUIRED_COLUMNS_ORDERED = ["order_id", "customer_id", "amount", "currency", "status", "created_at"]


def validate_schema(df: pd.DataFrame) -> tuple[bool, str]:
    """Return (True, '') if df has all required columns, else (False, error_message)."""
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        return False, f"Missing required columns: {sorted(missing)}"
    return True, ""


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Return only valid PAID/USD/positive-amount rows with non-empty IDs."""
    df = df.copy()

    # Keep only the required columns (preserves order)
    available = [c for c in REQUIRED_COLUMNS_ORDERED if c in df.columns]
    df = df[available]

    # Drop rows with empty or null order_id / customer_id
    df = df[df["order_id"].notna() & (df["order_id"].astype(str).str.strip() != "")]
    df = df[df["customer_id"].notna() & (df["customer_id"].astype(str).str.strip() != "")]

    # Coerce amount to numeric (invalid → NaN, which will be filtered out below)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # Apply business filters
    df = df[df["status"] == "PAID"]
    df = df[df["currency"] == "USD"]
    df = df[df["amount"] > 0]

    return df.reset_index(drop=True)


def build_quality_report(df_in: pd.DataFrame, df_out: pd.DataFrame) -> dict:
    return {
        "rows_in": len(df_in),
        "rows_out": len(df_out),
        "rows_dropped": len(df_in) - len(df_out),
        "filters_applied": ["status==PAID", "currency==USD", "amount>0", "non_empty_ids"],
    }
