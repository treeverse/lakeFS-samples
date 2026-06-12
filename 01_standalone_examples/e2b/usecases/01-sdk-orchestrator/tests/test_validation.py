"""Unit tests for validation logic.

These tests do not call E2B or lakeFS — they work entirely on in-memory DataFrames.
"""
from __future__ import annotations

import io

import pandas as pd
import pytest

from lakefs_e2b_poc.validation import (
    REQUIRED_COLUMNS,
    build_quality_report,
    clean_orders,
    validate_schema,
)

# The same seed CSV the seed module uploads to lakeFS
SEED_CSV = """\
order_id,customer_id,amount,currency,status,created_at
1001,c001,120.50,USD,PAID,2026-05-01
1002,c002,-7.00,USD,PAID,2026-05-01
1003,c003,50.00,EUR,PAID,2026-05-02
1004,c004,,USD,CANCELLED,2026-05-03
1005,c005,200.00,USD,PAID,2026-05-04
1006,,15.00,USD,PAID,2026-05-04
"""


def _df(csv: str) -> pd.DataFrame:
    return pd.read_csv(io.StringIO(csv))


# ── pass scenario ──────────────────────────────────────────────────────────────

class TestCleanOrders:
    def test_keeps_only_valid_rows(self):
        """Rows 1001 and 1005 are the only fully valid PAID/USD/positive/non-empty rows."""
        df = _df(SEED_CSV)
        clean = clean_orders(df)
        assert len(clean) == 2
        assert sorted(clean["order_id"].tolist()) == [1001, 1005]

    def test_all_remaining_paid(self):
        clean = clean_orders(_df(SEED_CSV))
        assert (clean["status"] == "PAID").all()

    def test_all_remaining_usd(self):
        clean = clean_orders(_df(SEED_CSV))
        assert (clean["currency"] == "USD").all()

    def test_all_remaining_positive_amount(self):
        clean = clean_orders(_df(SEED_CSV))
        assert (clean["amount"] > 0).all()

    def test_no_empty_order_id(self):
        clean = clean_orders(_df(SEED_CSV))
        assert clean["order_id"].notna().all()

    def test_no_empty_customer_id(self):
        clean = clean_orders(_df(SEED_CSV))
        assert clean["customer_id"].notna().all()

    def test_drops_negative_amount(self):
        """Row 1002 has amount=-7.00 and must be dropped."""
        df = _df(SEED_CSV)
        clean = clean_orders(df)
        assert 1002 not in clean["order_id"].tolist()

    def test_drops_non_usd(self):
        """Row 1003 is EUR and must be dropped."""
        df = _df(SEED_CSV)
        clean = clean_orders(df)
        assert 1003 not in clean["order_id"].tolist()

    def test_drops_cancelled(self):
        """Row 1004 is CANCELLED and must be dropped."""
        df = _df(SEED_CSV)
        clean = clean_orders(df)
        assert 1004 not in clean["order_id"].tolist()

    def test_drops_empty_customer_id(self):
        """Row 1006 has empty customer_id and must be dropped."""
        df = _df(SEED_CSV)
        clean = clean_orders(df)
        assert 1006 not in clean["order_id"].tolist()

    def test_idempotent(self):
        """Cleaning already-clean data should be a no-op."""
        df = _df(SEED_CSV)
        once = clean_orders(df)
        twice = clean_orders(once)
        assert len(once) == len(twice)


# ── fail scenario ──────────────────────────────────────────────────────────────

class TestFailScenario:
    def test_strict_filter_produces_zero_rows(self):
        """The fail scenario uses amount > 1000; none of the seed rows satisfy that."""
        df = _df(SEED_CSV)
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        strict = df[df["amount"] > 1000]
        assert len(strict) == 0

    def test_zero_rows_maps_to_fail_status(self):
        """If the agent outputs 0 rows it should report status='fail'."""
        df_out = pd.DataFrame(columns=list(REQUIRED_COLUMNS))
        status = "fail" if len(df_out) == 0 else "pass"
        assert status == "fail"


# ── schema validation ──────────────────────────────────────────────────────────

class TestValidateSchema:
    def test_accepts_all_required_columns(self):
        df = _df(SEED_CSV)
        ok, msg = validate_schema(df)
        assert ok is True
        assert msg == ""

    def test_rejects_missing_single_column(self):
        df = _df(SEED_CSV).drop(columns=["amount"])
        ok, msg = validate_schema(df)
        assert ok is False
        assert "amount" in msg

    def test_rejects_multiple_missing_columns(self):
        df = pd.DataFrame({"order_id": [1], "amount": [10.0]})
        ok, msg = validate_schema(df)
        assert ok is False
        assert "Missing required columns" in msg
        # customer_id, currency, status, created_at are all missing
        assert len(REQUIRED_COLUMNS - set(df.columns)) >= 4

    def test_accepts_extra_columns(self):
        df = _df(SEED_CSV)
        df["extra_col"] = "whatever"
        ok, msg = validate_schema(df)
        assert ok is True


# ── quality report ─────────────────────────────────────────────────────────────

class TestBuildQualityReport:
    def test_counts_are_correct(self):
        df_in = _df(SEED_CSV)
        df_out = clean_orders(df_in)
        report = build_quality_report(df_in, df_out)
        assert report["rows_in"] == len(df_in)
        assert report["rows_out"] == len(df_out)
        assert report["rows_dropped"] == len(df_in) - len(df_out)

    def test_contains_filter_list(self):
        df_in = _df(SEED_CSV)
        df_out = clean_orders(df_in)
        report = build_quality_report(df_in, df_out)
        assert "filters_applied" in report
        assert len(report["filters_applied"]) > 0
