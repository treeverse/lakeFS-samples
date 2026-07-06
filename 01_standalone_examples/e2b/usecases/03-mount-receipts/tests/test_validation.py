"""Unit tests for the receipts ledger validation (no live services needed)."""
from __future__ import annotations

from datetime import date

import pytest

from mount_receipts.validation import (
    FX_TO_USD,
    FileOutcome,
    apply_cross_row_uniqueness,
    business_rule_outcomes,
    check_business_rules,
    classify_ambiguous,
    gate_input_rows,
    resolve_ambiguous,
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


def test_outcomes_accept_and_ambiguous():
    # A blank currency is the row's ONLY problem — rule 7 flags it "ambiguous", not a hard
    # reject (see the dedicated rule-7 tests below for the reject-vs-ambiguous line).
    rows = [_row("ok.jpg", invoice_no="OK-1"), _row("bad.jpg", invoice_no="BAD-1", currency="")]
    out = {o["source_file"]: o for o in business_rule_outcomes(rows, today=TODAY)}
    assert out["ok.jpg"]["outcome"] == "accepted" and out["ok.jpg"]["reasons"] == []
    assert out["bad.jpg"]["outcome"] == "ambiguous"
    assert any("missing currency" in r for r in out["bad.jpg"]["reasons"])


def test_outcomes_confident_foreign_currency_is_hard_reject_not_ambiguous():
    # A CONFIDENTLY-read non-USD currency is a plain policy violation, not ambiguous — only
    # a genuinely blank/unreadable currency qualifies for rule 7.
    rows = [_row("bad.jpg", invoice_no="BAD-1", currency="EUR")]
    out = {o["source_file"]: o for o in business_rule_outcomes(rows, today=TODAY)}
    assert out["bad.jpg"]["outcome"] == "rejected"


def test_outcomes_reject_when_multiple_problems():
    # Blank currency AND missing vendor — more than one problem, so it's a hard reject, not
    # ambiguous (rule 7 only applies when the ambiguous field is the SOLE issue).
    rows = [_row("bad.jpg", invoice_no="BAD-1", currency="", vendor="")]
    out = {o["source_file"]: o for o in business_rule_outcomes(rows, today=TODAY)}
    assert out["bad.jpg"]["outcome"] == "rejected"


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


def test_uniqueness_passes_ambiguous_through_untouched():
    rows = [_row("a.jpg", invoice_no="X", currency="EUR")]
    generated = {"a.jpg": {"outcome": "ambiguous", "reasons": ["currency EUR not covered by policy"]}}
    final = apply_cross_row_uniqueness(rows, generated)
    assert final["a.jpg"] == generated["a.jpg"]


def test_uniqueness_overrides_ambiguous_on_duplicate():
    # A clear-cut duplicate doesn't need a human's attention, regardless of what the
    # validator tagged it — it's force-rejected either way.
    rows = [_row("a.jpg", invoice_no="DUP", currency="EUR"), _row("b.jpg", invoice_no="DUP")]
    generated = {
        "a.jpg": {"outcome": "ambiguous", "reasons": ["currency EUR not covered by policy"]},
        "b.jpg": {"outcome": "accepted", "reasons": []},
    }
    final = apply_cross_row_uniqueness(rows, generated)
    assert final["a.jpg"]["outcome"] == "rejected"
    assert any("duplicate" in r for r in final["a.jpg"]["reasons"])
    assert final["b.jpg"]["outcome"] == "rejected"


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


# --- classify_ambiguous (rule 7's reference implementation) ---------------------

def test_classify_ambiguous_blank_currency_only():
    assert classify_ambiguous(_rec(currency=""), today=TODAY) == "currency_unclear"


def test_classify_ambiguous_none_for_confident_foreign_currency():
    # Confidently-read EUR is a plain rule-5 reject, not ambiguous — only a blank/unreadable
    # currency field qualifies for rule 7.
    assert classify_ambiguous(_rec(currency="EUR"), today=TODAY) is None


def test_classify_ambiguous_small_total_mismatch():
    # off by 0.50 on a $16.25 total — inside max($5, 5%) but outside the 0.01 tolerance
    rec = _rec(total=16.25, line_items=[{"name": "x", "amount": 11.00}, {"name": "y", "amount": 4.75}])
    assert classify_ambiguous(rec, today=TODAY) == "total_mismatch"


def test_classify_ambiguous_none_when_clean():
    assert classify_ambiguous(_rec(), today=TODAY) is None


def test_classify_ambiguous_none_when_mismatch_too_large():
    rec = _rec(total=100.00, line_items=[{"name": "x", "amount": 4.0}, {"name": "y", "amount": 6.0}])
    assert classify_ambiguous(rec, today=TODAY) is None


def test_classify_ambiguous_none_when_multiple_problems():
    # blank currency AND future-dated — more than one problem, not the sole issue
    assert classify_ambiguous(_rec(currency="", date="2027-08-01"), today=TODAY) is None


# --- resolve_ambiguous (applying a human's decision) ----------------------------

def _pending():
    # Blank currency, no line_items — keeps this fixture focused on identifying/converting
    # currency, not the total/line-item check. Note: resolve_ambiguous doesn't care WHY a
    # row was flagged ambiguous (that's the agent's call, trusted as-is) — naming a currency
    # is always an available decision, regardless of the underlying reason.
    rec = _rec(currency="", line_items=[])
    return {"source_file": "r.jpg", "record": rec, "reasons": ["currency could not be determined"]}


def test_resolve_reject():
    res = resolve_ambiguous(_pending(), "reject")
    assert res["outcome"] == "rejected"
    assert "reject" in res["reason"]


def test_resolve_approve_leaves_record_unchanged_and_still_fails_policy():
    item = _pending()
    res = resolve_ambiguous(item, "approve")
    assert res["outcome"] == "accepted"
    assert res["record"]["currency"] == ""   # untouched — approving doesn't launder policy
    # Proof that "approve" alone can't get an unresolved-currency row past the (unchanged)
    # policy check the lakeFS pre-merge gate independently re-derives — it must still fail.
    assert check_business_rules(res["record"], today=TODAY, seen_invoice_nos=set()) != []


def test_resolve_identify_as_usd_no_conversion_needed():
    item = _pending()
    res = resolve_ambiguous(item, "USD")
    assert res["outcome"] == "accepted"
    assert res["record"]["currency"] == "USD"
    assert res["record"]["total"] == 10.0   # unchanged — no conversion for USD
    assert check_business_rules(res["record"], today=TODAY, seen_invoice_nos=set()) == []


def test_resolve_identify_currency_converts_and_normalises():
    item = _pending()
    res = resolve_ambiguous(item, "EUR")   # the human identified the actual currency
    assert res["outcome"] == "accepted"
    assert res["record"]["currency"] == "USD"
    assert res["record"]["total"] == round(10.0 * FX_TO_USD["EUR"], 2)
    # A converted record is genuinely policy-clean, unlike a bare "approve".
    assert check_business_rules(res["record"], today=TODAY, seen_invoice_nos=set()) == []


def test_resolve_identify_currency_with_line_items_keeps_total_consistent():
    # Converting must derive the total from the converted line items (not convert total
    # independently) — otherwise total != sum(items) post-conversion and the lakeFS gate
    # would block a legitimately converted row on a rule it never should have failed.
    item = _pending()
    item["record"]["line_items"] = [{"name": "Plat", "amount": 24.0}, {"name": "Tip", "amount": 3.0}]
    item["record"]["total"] = 27.0
    res = resolve_ambiguous(item, "EUR")
    assert res["outcome"] == "accepted"
    assert res["record"]["total"] == round(sum(it["amount"] for it in res["record"]["line_items"]), 2)
    assert check_business_rules(res["record"], today=TODAY, seen_invoice_nos=set()) == []


def test_resolve_unrecognised_decision_falls_back_to_rejected():
    item = _pending()
    res = resolve_ambiguous(item, "XYZ")   # not "approve"/"reject", not a known currency
    assert res["outcome"] == "rejected"
    assert "unrecognised" in res["reason"]


def test_resolve_currency_name_available_regardless_of_why_it_was_ambiguous():
    # resolve_ambiguous doesn't know or care why the validator called a row ambiguous —
    # naming a currency is universally available, not gated behind a specific reason. Here
    # the row's *stated* reason isn't about currency at all, but identifying a currency
    # still resolves it (a human would simply only use this option when it's relevant).
    item = {"source_file": "r.jpg", "record": _rec(currency="", line_items=[]),
            "reasons": ["some other reason the agent gave"]}
    res = resolve_ambiguous(item, "EUR")
    assert res["outcome"] == "accepted"
    assert res["record"]["currency"] == "USD"


# --- ValidationResult.status / pending_review (the human-review gate) ----------

def test_pending_review_is_accounted_for_but_not_passed():
    inbox = ["a.jpg", "b.jpg"]
    outcomes = [_acc("a.jpg"), FileOutcome("b.jpg", "pending_review", 3, "", _rec(currency="EUR"))]
    res = validate_ledger(inbox, outcomes)
    assert res.passed is False
    assert res.status == "awaiting_review"
    assert res.to_dict()["pending_review"] == 1
    # still fully accounted for — this isn't the "incomplete accounting" failure mode
    assert "unaccounted" not in res.summary


def test_status_passed_and_failed_unaffected_by_pending_review_absence():
    res_pass = validate_ledger(["a.jpg"], [_acc("a.jpg")])
    assert res_pass.status == "passed"
    res_fail = validate_ledger(["a.jpg", "b.jpg"], [_acc("a.jpg")])
    assert res_fail.status == "failed"
