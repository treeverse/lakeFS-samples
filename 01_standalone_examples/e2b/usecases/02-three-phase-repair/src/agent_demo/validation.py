"""Three-phase progressive CSV validation — no external dependencies."""
from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from datetime import datetime


VALID_STATUSES = {"completed", "pending", "shipped", "cancelled"}
VALID_CATEGORIES = {"electronics", "clothing", "books", "home", "sports"}

AMOUNT_CEILING = 350.00
MIN_ORDER_YEAR = 2023

PHASE_NAMES = {
    1: "Structural Integrity",
    2: "Format & Value Conformance",
    3: "Business Rules",
}

PHASE_RULES = {
    1: """Phase 1 — Structural Integrity:
  - order_id: required (not empty), must be unique across all rows
  - customer_id: required (not empty, not whitespace, not NaN/null)
  - amount: must be present and parseable as a number
  Fix these issues first. Later phases check format, values, and business rules.""",

    2: """Phase 2 — Format & Value Conformance:
  - order_date: must be YYYY-MM-DD format and a valid calendar date
  - amount: must be strictly greater than 0
  - currency: must be exactly "USD"
  - status: must be one of: completed, pending, shipped, cancelled (all lowercase)
  - product_category: must be one of: electronics, clothing, books, home, sports (all lowercase)
  Normalize case where possible; drop rows with values that cannot be repaired.""",

    3: f"""Phase 3 — Business Rules:
  - order_date year must be {MIN_ORDER_YEAR} or later (orders older than {MIN_ORDER_YEAR} are stale and excluded)
  - amount must not exceed {AMOUNT_CEILING} (orders above this threshold require manual review and must be excluded)
  Drop rows that violate these constraints — do not modify amounts or dates.""",
}


@dataclass
class ValidationFailure:
    rule: str
    description: str
    affected_rows: list[int]


@dataclass
class ValidationResult:
    passed: bool
    failures: list[ValidationFailure]
    row_count: int
    failed_phase: int  # 0 = all phases passed; 1/2/3 = first phase that failed

    @property
    def phase_name(self) -> str:
        """Human-readable name of the failing phase."""
        return PHASE_NAMES.get(self.failed_phase, "All phases passed") if self.failed_phase else "All phases passed"

    @property
    def summary(self) -> str:
        """One-line human-readable summary."""
        if self.passed:
            return f"{self.row_count} rows, all phases passed"
        return (
            f"{self.row_count} rows, phase {self.failed_phase} ({self.phase_name}) failed — "
            + "; ".join(f.rule for f in self.failures)
        )

    def to_dict(self) -> dict:
        """Serialise to a plain dict for JSON output."""
        return {
            "status": "passed" if self.passed else "failed",
            "failed_phase": self.failed_phase,
            "phase_name": self.phase_name,
            "row_count": self.row_count,
            "summary": self.summary,
            "failures": [
                {
                    "rule": f.rule,
                    "description": f.description,
                    "affected_rows": f.affected_rows,
                }
                for f in self.failures
            ],
        }


def _is_valid_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def _phase1(rows: list[dict]) -> list[ValidationFailure]:
    """Structural: required fields, uniqueness, amount parseability."""
    failures: list[ValidationFailure] = []

    missing_order_id = [i + 1 for i, r in enumerate(rows) if not r.get("order_id", "").strip()]
    if missing_order_id:
        failures.append(ValidationFailure("order_id_required", "order_id is missing or empty", missing_order_id))

    seen: dict[str, int] = {}
    duplicates: list[int] = []
    for i, r in enumerate(rows):
        oid = r.get("order_id", "").strip()
        if oid:
            if oid in seen:
                duplicates.append(i + 1)
            else:
                seen[oid] = i + 1
    if duplicates:
        failures.append(ValidationFailure("order_id_unique", "Duplicate order_id values found", duplicates))

    missing_customer = [i + 1 for i, r in enumerate(rows) if not r.get("customer_id", "").strip()]
    if missing_customer:
        failures.append(ValidationFailure("customer_id_required", "customer_id is missing or empty", missing_customer))

    bad_amount = []
    for i, r in enumerate(rows):
        try:
            float(r.get("amount", "").strip())
        except (ValueError, AttributeError):
            bad_amount.append(i + 1)
    if bad_amount:
        failures.append(ValidationFailure("amount_parseable", "amount must be a valid number", bad_amount))

    return failures


def _phase2(rows: list[dict]) -> list[ValidationFailure]:
    """Format & values: date format, positive amount, currency, enumerations."""
    failures: list[ValidationFailure] = []

    bad_date = [i + 1 for i, r in enumerate(rows) if not _is_valid_date(r.get("order_date", "").strip())]
    if bad_date:
        failures.append(ValidationFailure("order_date_format", "order_date must be a valid YYYY-MM-DD date", bad_date))

    bad_amount = []
    for i, r in enumerate(rows):
        try:
            if float(r.get("amount", "").strip()) <= 0:
                bad_amount.append(i + 1)
        except (ValueError, AttributeError):
            pass  # already caught in phase 1
    if bad_amount:
        failures.append(ValidationFailure("amount_positive", "amount must be strictly greater than 0", bad_amount))

    bad_currency = [i + 1 for i, r in enumerate(rows) if r.get("currency", "").strip() != "USD"]
    if bad_currency:
        failures.append(ValidationFailure("currency_usd", "currency must be exactly 'USD'", bad_currency))

    bad_status = [i + 1 for i, r in enumerate(rows) if r.get("status", "").strip() not in VALID_STATUSES]
    if bad_status:
        failures.append(
            ValidationFailure("status_valid", f"status must be one of: {', '.join(sorted(VALID_STATUSES))}", bad_status)
        )

    bad_category = [i + 1 for i, r in enumerate(rows) if r.get("product_category", "").strip() not in VALID_CATEGORIES]
    if bad_category:
        failures.append(
            ValidationFailure(
                "product_category_valid",
                f"product_category must be one of: {', '.join(sorted(VALID_CATEGORIES))}",
                bad_category,
            )
        )

    return failures


def _phase3(rows: list[dict]) -> list[ValidationFailure]:
    """Business rules: date recency, amount ceiling."""
    failures: list[ValidationFailure] = []

    stale_rows = []
    for i, r in enumerate(rows):
        date_str = r.get("order_date", "").strip()
        if _is_valid_date(date_str):
            year = int(date_str[:4])
            if year < MIN_ORDER_YEAR:
                stale_rows.append(i + 1)
    if stale_rows:
        failures.append(
            ValidationFailure(
                "date_recency",
                f"order_date year must be {MIN_ORDER_YEAR} or later — stale orders are excluded",
                stale_rows,
            )
        )

    over_ceiling = []
    for i, r in enumerate(rows):
        try:
            if float(r.get("amount", "").strip()) > AMOUNT_CEILING:
                over_ceiling.append(i + 1)
        except (ValueError, AttributeError):
            pass
    if over_ceiling:
        failures.append(
            ValidationFailure(
                "amount_ceiling",
                f"amount must not exceed {AMOUNT_CEILING} — orders above this require manual review",
                over_ceiling,
            )
        )

    return failures


def validate(csv_text: str) -> ValidationResult:
    """Run all three phases in order, stopping at the first failure."""
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    row_count = len(rows)

    for phase, check_fn in [(1, _phase1), (2, _phase2), (3, _phase3)]:
        failures = check_fn(rows)
        if failures:
            return ValidationResult(passed=False, failures=failures, row_count=row_count, failed_phase=phase)

    return ValidationResult(passed=True, failures=[], row_count=row_count, failed_phase=0)
