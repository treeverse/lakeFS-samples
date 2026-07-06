"""Unit tests for the pure parts of extraction.py (no OpenAI calls)."""
from __future__ import annotations

from mount_receipts.extraction import missing_required


def _record(**kw):
    base = dict(vendor="Acme", date="2026-01-10", invoice_no="A-1", currency="USD",
                line_items=[{"name": "x", "amount": 10.0}], total=10.0)
    base.update(kw)
    return base


def test_clean_record_has_no_missing_fields():
    assert missing_required(_record()) == []


def test_blank_currency_is_not_a_missing_field():
    # A blank currency isn't an extraction failure — it reaches Phase 3, where the
    # validator decides whether it's ambiguous (see validation.RULES_SPEC rule 7) rather
    # than the record being dropped here as incomplete.
    assert missing_required(_record(currency="")) == []


def test_blank_vendor_is_still_a_missing_field():
    assert "vendor" in missing_required(_record(vendor=""))


def test_missing_total_is_still_a_missing_field():
    assert "total" in missing_required(_record(total=None))
