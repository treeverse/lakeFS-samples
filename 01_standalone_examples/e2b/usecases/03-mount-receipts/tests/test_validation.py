"""Unit tests for the receipts ledger validation (no live services needed)."""
from __future__ import annotations

from datetime import date

import pytest

from mount_receipts.validation import (
    FileOutcome,
    apply_cross_row_uniqueness,
    business_rule_outcomes,
    check_business_rules,
    gate_input_rows,
    to_iso_date,
    validate_ledger,
)


def test_to_iso_date_normalises_and_handles_bad_input():
    assert to_iso_date("2026-03-12") == "2026-03-12"
    assert to_iso_date("1/20/2026") == "2026-01-20"   # any printed format -> ISO
    assert to_iso_date("") == ""
    assert to_iso_date(None) == ""
    assert to_iso_date("not a date") == ""

TODAY = date(2026, 6, 5)


def _rec(**kw):
    base = dict(
        vendor="Acme",
        invoice_no="A-1",
        date="2026-01-10",
        currency="USD",
        line_items=[{"name": "x", "amount": 4.0}, {"name": "y", "amount": 6.0}],
        total=10.0,
    )
    base.update(kw)
    return base


# --- business rules --------------------------------------------------------

def test_clean_record_passes():
    assert check_business_rules(_rec(), today=TODAY, seen_invoice_nos=set()) == []


def test_total_mismatch():
    reasons = check_business_rules(_rec(total=12.0), today=TODAY, seen_invoice_nos=set())
    assert any("sum(line items)" in r for r in reasons)


def test_future_date():
    reasons = check_business_rules(_rec(date="2027-08-01"), today=TODAY, seen_invoice_nos=set())
    assert any("future-dated" in r for r in reasons)


def test_stale_date():
    reasons = check_business_rules(_rec(date="2019-01-01", total=10.0), today=TODAY, seen_invoice_nos=set())
    assert any("stale date" in r for r in reasons)


def test_non_usd():
    reasons = check_business_rules(_rec(currency="EUR"), today=TODAY, seen_invoice_nos=set())
    assert any("non-USD" in r for r in reasons)


def test_over_cap():
    reasons = check_business_rules(
        _rec(line_items=[{"name": "suite", "amount": 600.0}], total=600.0),
        today=TODAY, seen_invoice_nos=set(),
    )
    assert any("policy cap" in r for r in reasons)


def test_duplicate_invoice():
    reasons = check_business_rules(_rec(invoice_no="DUP"), today=TODAY, seen_invoice_nos={"DUP"})
    assert any("duplicate invoice" in r for r in reasons)


def test_amounts_as_strings_are_parsed():
    rec = _rec(total="$10.00", line_items=[{"name": "x", "amount": "4.00"}, {"name": "y", "amount": "6.00"}])
    assert check_business_rules(rec, today=TODAY, seen_invoice_nos=set()) == []


# --- ledger completeness ---------------------------------------------------

def _acc(f):  # accepted
    return FileOutcome(f, "accepted", 3, "", _rec())


def test_full_accounting_passes():
    inbox = ["a.jpg", "b.jpg", "c.jpg"]
    outcomes = [
        _acc("a.jpg"),
        FileOutcome("b.jpg", "rejected", 3, "non-USD currency"),
        FileOutcome("c.jpg", "dropped", 1, "corrupt"),
    ]
    res = validate_ledger(inbox, outcomes)
    assert res.passed, res.summary
    assert res.failed_phase is None


def test_unaccounted_file_fails():
    res = validate_ledger(["a.jpg", "b.jpg"], [_acc("a.jpg")])
    assert not res.passed
    assert res.failed_phase == 1
    assert "unaccounted" in res.summary


def test_double_counted_fails():
    outcomes = [_acc("a.jpg"), FileOutcome("a.jpg", "dropped", 1, "dup")]
    res = validate_ledger(["a.jpg"], outcomes)
    assert not res.passed
    assert "double-counted" in res.summary


def test_accepted_with_reason_fails():
    bad = FileOutcome("a.jpg", "accepted", 3, "non-USD currency", _rec(currency="EUR"))
    res = validate_ledger(["a.jpg"], [bad])
    assert not res.passed
    assert res.failed_phase == 3


def test_empty_ledger_fails():
    res = validate_ledger(["a.jpg"], [FileOutcome("a.jpg", "dropped", 1, "corrupt")])
    assert not res.passed
    assert res.failed_phase == 2


def test_unexpected_file_fails():
    res = validate_ledger(["a.jpg"], [_acc("a.jpg"), _acc("ghost.jpg")])
    assert not res.passed
    assert "unexpected" in res.summary


# --- business_rule_outcomes (the spec the generated validator reproduces) -------

def _row(f, **kw):
    return {"source_file": f, "record": _rec(**kw)}


def test_outcomes_accept_and_reject():
    rows = [_row("ok.jpg", invoice_no="OK-1"), _row("bad.jpg", invoice_no="BAD-1", currency="EUR")]
    out = {o["source_file"]: o for o in business_rule_outcomes(rows, today=TODAY)}
    assert out["ok.jpg"]["outcome"] == "accepted" and out["ok.jpg"]["reasons"] == []
    assert out["bad.jpg"]["outcome"] == "rejected"
    assert any("non-USD" in r for r in out["bad.jpg"]["reasons"])


def test_outcomes_reject_both_sides_of_a_duplicate_invoice():
    rows = [_row("a.jpg", invoice_no="DUP"), _row("b.jpg", invoice_no="DUP")]
    out = business_rule_outcomes(rows, today=TODAY)
    assert all(o["outcome"] == "rejected" for o in out)
    assert all(any("duplicate invoice" in r for r in o["reasons"]) for o in out)


def test_outcomes_without_uniqueness_ignores_duplicates():
    # The per-receipt variant (what the LLM validator produces) must NOT flag duplicates.
    rows = [_row("a.jpg", invoice_no="DUP"), _row("b.jpg", invoice_no="DUP")]
    out = business_rule_outcomes(rows, today=TODAY, include_uniqueness=False)
    assert all(o["outcome"] == "accepted" for o in out)


# --- apply_cross_row_uniqueness (the host's deterministic duplicate pass) --------

def test_uniqueness_rejects_duplicates_the_validator_accepted():
    rows = [_row("a.jpg", invoice_no="DUP"), _row("b.jpg", invoice_no="DUP"), _row("c.jpg", invoice_no="UNIQ")]
    # The generated validator (per-receipt only) accepted all three.
    generated = {f: {"outcome": "accepted", "reasons": []} for f in ("a.jpg", "b.jpg", "c.jpg")}
    final = apply_cross_row_uniqueness(rows, generated)
    assert final["a.jpg"]["outcome"] == "rejected" and any("duplicate" in r for r in final["a.jpg"]["reasons"])
    assert final["b.jpg"]["outcome"] == "rejected"
    assert final["c.jpg"]["outcome"] == "accepted"


def test_uniqueness_preserves_prior_rejections():
    rows = [_row("a.jpg", invoice_no="X")]
    generated = {"a.jpg": {"outcome": "rejected", "reasons": ["non-USD currency (EUR)"]}}
    final = apply_cross_row_uniqueness(rows, generated)
    assert final["a.jpg"]["outcome"] == "rejected"
    assert final["a.jpg"]["reasons"] == ["non-USD currency (EUR)"]


def test_uniqueness_defaults_missing_outcome_to_rejected():
    rows = [_row("a.jpg", invoice_no="X")]
    final = apply_cross_row_uniqueness(rows, {})   # validator produced nothing for this row
    assert final["a.jpg"]["outcome"] == "rejected"


def test_outcomes_one_per_row():
    rows = [_row("a.jpg", invoice_no="A"), _row("b.jpg", invoice_no="B"), _row("c.jpg", invoice_no="C")]
    out = business_rule_outcomes(rows, today=TODAY)
    assert {o["source_file"] for o in out} == {"a.jpg", "b.jpg", "c.jpg"}


# --- gate_input_rows (typed inputs the lakeFS hook re-validates) ----------------

def test_gate_input_typing_and_normalisation():
    rows = [_row("r.jpg", date="1/20/2026", currency="usd", total="$10.00",
                 line_items=[{"name": "x", "amount": "4.00"}, {"name": "y", "amount": "6.00"}])]
    g = gate_input_rows(rows, {"r.jpg": "accepted"})[0]
    assert g["decided"] == "accepted"
    assert g["currency"] == "USD"            # upper-cased
    assert g["total"] == 10.0                # parsed from "$10.00"
    assert g["date_iso"] == "2026-01-20" and g["year"] == 2026   # normalised to ISO
    assert g["item_amounts"] == [4.0, 6.0] and g["check_sum"] is True


def test_gate_input_unparseable_total_is_null_and_decided_defaults_rejected():
    rows = [_row("r.jpg", total="N/A", line_items=[])]
    g = gate_input_rows(rows, {})[0]      # no decision provided
    assert g["total"] is None
    assert g["check_sum"] is False and g["item_amounts"] == []
    assert g["decided"] == "rejected"     # safe default when the validator gave no outcome
