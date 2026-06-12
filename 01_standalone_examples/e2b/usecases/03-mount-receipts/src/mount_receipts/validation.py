"""Three-phase validation for the receipts → ledger demo.

Phases (progressive, mirroring the SDK demo's structure):

- **Phase 1 — Triage (structural):** each inbox file is readable, a real receipt,
  and not an exact duplicate. Failures here mean the file is *dropped*.
- **Phase 2 — Extract (multimodal):** every kept file yields the required fields
  (vendor, date, total, currency). Missing fields fail extraction.
- **Phase 3 — Validate (business rules):** total == sum(line items), date parseable
  and not future / not older than 2023, currency == USD, invoice number unique, and
  total within the policy cap. Failures here mean the row is *rejected* (with reason).

This module is pure Python (only ``python-dateutil``) so it runs identically on the
host and inside the E2B sandbox, and is unit-tested without live services.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from dateutil import parser as dateparser

PHASE_NAMES = {
    1: "Triage (structural)",
    2: "Extract (multimodal)",
    3: "Validate (business rules)",
}

POLICY_CAP_USD = 500.00
MIN_YEAR = 2023
AMOUNT_TOLERANCE = 0.01


@dataclass
class FileOutcome:
    """Final disposition of a single inbox file."""
    source_file: str
    outcome: str                 # "accepted" | "rejected" | "dropped"
    phase: int                   # phase that decided the outcome (3 for accepted)
    reason: str = ""
    record: dict | None = None   # extracted fields, when available

    def to_dict(self) -> dict:
        return {
            "source_file": self.source_file,
            "outcome": self.outcome,
            "phase": self.phase,
            "reason": self.reason,
            "record": self.record,
        }


def _parse_amount(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", "").replace("$", "").strip())
    except ValueError:
        return None


def check_business_rules(
    record: dict,
    *,
    today: date,
    seen_invoice_nos: set[str],
    policy_cap: float = POLICY_CAP_USD,
) -> list[str]:
    """Return a list of business-rule failure reasons for an extracted record (empty = pass).

    ``seen_invoice_nos`` is the set of invoice numbers already accepted; this call does
    NOT mutate it (the caller adds the invoice number only once a row is accepted).
    """
    reasons: list[str] = []

    vendor = (record.get("vendor") or "").strip()
    currency = (record.get("currency") or "").strip().upper()
    invoice_no = (record.get("invoice_no") or "").strip()
    total = _parse_amount(record.get("total"))
    items = record.get("line_items") or []

    # Required fields (a Phase-2 concern, re-checked defensively here).
    if not vendor:
        reasons.append("missing vendor")
    if total is None:
        reasons.append("missing or unparseable total")

    # total == sum(line items)
    if total is not None and items:
        item_sum = 0.0
        ok = True
        for it in items:
            amt = _parse_amount(it.get("amount") if isinstance(it, dict) else None)
            if amt is None:
                ok = False
                break
            item_sum += amt
        if ok and abs(item_sum - total) > AMOUNT_TOLERANCE:
            reasons.append(f"total {total:.2f} != sum(line items) {item_sum:.2f}")

    # date sanity
    raw_date = (record.get("date") or "").strip()
    if not raw_date:
        reasons.append("missing date")
    else:
        try:
            parsed = dateparser.parse(raw_date).date()
            if parsed > today:
                reasons.append(f"future-dated ({parsed.isoformat()})")
            elif parsed.year < MIN_YEAR:
                reasons.append(f"stale date ({parsed.isoformat()}, before {MIN_YEAR})")
        except (ValueError, OverflowError, TypeError):
            reasons.append(f"unparseable date ({raw_date!r})")

    # currency
    if currency and currency != "USD":
        reasons.append(f"non-USD currency ({currency})")
    elif not currency:
        reasons.append("missing currency")

    # policy cap
    if total is not None and total > policy_cap:
        reasons.append(f"total {total:.2f} exceeds policy cap {policy_cap:.2f}")

    # duplicate invoice number
    if invoice_no and invoice_no in seen_invoice_nos:
        reasons.append(f"duplicate invoice number ({invoice_no})")

    return reasons


@dataclass
class ValidationResult:
    passed: bool
    summary: str
    failed_phase: int | None
    inbox_files: list[str]
    outcomes: list[FileOutcome] = field(default_factory=list)

    @property
    def accepted(self) -> list[FileOutcome]:
        return [o for o in self.outcomes if o.outcome == "accepted"]

    def to_dict(self) -> dict:
        return {
            "status": "passed" if self.passed else "failed",
            "summary": self.summary,
            "failed_phase": self.failed_phase,
            "inbox_file_count": len(self.inbox_files),
            "accepted": len(self.accepted),
            "rejected": sum(1 for o in self.outcomes if o.outcome == "rejected"),
            "dropped": sum(1 for o in self.outcomes if o.outcome == "dropped"),
            "outcomes": [o.to_dict() for o in self.outcomes],
        }


def validate_ledger(
    inbox_files: list[str],
    outcomes: list[FileOutcome],
    *,
    require_nonempty: bool = True,
) -> ValidationResult:
    """Validate completeness + correctness of a fully-processed inbox.

    A run *passes* only when:
      - every inbox file is accounted for exactly once (accepted / rejected / dropped),
      - at least one row was accepted (unless ``require_nonempty`` is False),
      - no accepted row carries a business-rule reason.

    This is what the pre-merge gate ultimately enforces (via the serialized result).
    """
    by_file: dict[str, list[FileOutcome]] = {}
    for o in outcomes:
        by_file.setdefault(o.source_file, []).append(o)

    inbox = set(inbox_files)
    decided = set(by_file)

    missing = sorted(inbox - decided)
    unexpected = sorted(decided - inbox)
    duplicated = sorted(f for f, lst in by_file.items() if len(lst) > 1)

    if missing or unexpected or duplicated:
        problems = []
        if missing:
            problems.append(f"{len(missing)} unaccounted ({', '.join(missing[:3])}…)" if len(missing) > 3 else f"unaccounted: {', '.join(missing)}")
        if unexpected:
            problems.append(f"unexpected: {', '.join(unexpected)}")
        if duplicated:
            problems.append(f"double-counted: {', '.join(duplicated)}")
        return ValidationResult(
            passed=False,
            summary="incomplete accounting — " + "; ".join(problems),
            failed_phase=1,
            inbox_files=inbox_files,
            outcomes=outcomes,
        )

    accepted = [o for o in outcomes if o.outcome == "accepted"]
    bad_accepted = [o for o in accepted if o.reason]
    if bad_accepted:
        return ValidationResult(
            passed=False,
            summary=f"{len(bad_accepted)} accepted row(s) still carry failures",
            failed_phase=3,
            inbox_files=inbox_files,
            outcomes=outcomes,
        )

    if require_nonempty and not accepted:
        return ValidationResult(
            passed=False,
            summary="no receipts accepted — ledger is empty",
            failed_phase=2,
            inbox_files=inbox_files,
            outcomes=outcomes,
        )

    n_rej = sum(1 for o in outcomes if o.outcome == "rejected")
    n_drop = sum(1 for o in outcomes if o.outcome == "dropped")
    return ValidationResult(
        passed=True,
        summary=f"{len(accepted)} accepted, {n_rej} rejected, {n_drop} dropped — ledger valid",
        failed_phase=None,
        inbox_files=inbox_files,
        outcomes=outcomes,
    )
