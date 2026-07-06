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

# Rule 7 (ambiguous) thresholds — a mismatch inside this band reads as a scan/OCR
# artifact rather than a real discrepancy, so it's worth a human's attention instead
# of an automatic reject.
AMBIGUOUS_TOTAL_MIN_USD = 5.00
AMBIGUOUS_TOTAL_FRACTION = 0.05

# Fixed, illustrative demo rates (NOT live FX) — used only when a human identifies the
# actual currency on a currency-ambiguous row (the validator itself has no network access
# and can't look one up).
FX_TO_USD = {"EUR": 1.08, "GBP": 1.27, "CHF": 1.10, "CAD": 0.73}


# The Phase-3 business rules, written as a *specification* rather than as the code that
# enforces them. At runtime the agent is handed this spec plus the extracted receipts and
# is asked to WRITE its own Python validator, which is then executed inside the E2B
# sandbox (see ``codegen.py``). That is the whole point of running it in E2B: the
# validation logic is code we have never seen until the model emits it, and the sandbox
# contains whatever it does. ``check_business_rules`` / ``business_rule_outcomes`` below
# are the canonical reference implementation of this same spec — used by the unit tests
# and as the fallback if code generation can't produce a working validator.
RULES_SPEC = f"""\
You are given receipts that were already extracted into structured records. Decide, for
each record INDEPENDENTLY, whether it is `accepted`, `rejected`, or `ambiguous` under
these per-receipt business rules. A record is REJECTED if it violates ANY rule (except as
carved out by rule 7 below); list every violated rule as a short reason string.

Rules (each applies to a single record on its own):
  1. `vendor` must be a non-empty string.
  2. `total` must be present and parseable as a number. Amounts may arrive as numbers or
     as strings like "$1,234.50" — strip "$" and thousands separators before parsing.
  3. If `line_items` is non-empty, `total` must equal the sum of the item `amount`s,
     within a tolerance of {AMOUNT_TOLERANCE}.
  4. `date` is given as an ISO `YYYY-MM-DD` string ("" if it was missing/unparseable). It
     must be non-empty, must NOT be after `today` (also ISO `YYYY-MM-DD` — compare the two
     strings directly, do NOT re-parse), and its year (first 4 chars) must be >= {MIN_YEAR}.
  5. `currency` must be present and equal to "USD" (case-insensitive).
  6. `total` must be <= the policy cap (`policy_cap`, ${POLICY_CAP_USD:.2f}).
  7. Instead of `rejected`, mark a record `ambiguous` whenever the SCAN ITSELF is too
     unclear to trust a field on it — a low-contrast, smudged, or otherwise degraded
     receipt where a value could reasonably be read more than one way. This is not limited
     to any one field; judge it broadly, the same way a person skimming the image would.
     YOU decide what counts; there is no fixed list. Two illustrative (non-exhaustive)
     examples of the kind of thing that qualifies:
       - `currency` is BLANK because you can't confidently tell which currency this receipt
         is in (e.g. an illegible symbol, or no currency indicator visible at all) — reason
         should say the currency couldn't be determined. A CONFIDENTLY-read non-USD currency
         (e.g. "EUR") is NOT this — that's a straightforward rule-5 reject; only genuine
         uncertainty about WHICH currency qualifies.
       - `line_items` is non-empty and `total` differs from sum(`line_items`) by an amount
         small enough that a smudged or misread digit — not a real discrepancy — is the
         likely explanation (roughly more than {AMOUNT_TOLERANCE} but no more than
         max(${AMBIGUOUS_TOTAL_MIN_USD:.2f}, {AMBIGUOUS_TOTAL_FRACTION*100:.0f}% of `total`)) —
         reason should describe the mismatch, e.g. "total 16.25 vs sum(items) 15.75 (off by 0.50)".
     These two are examples of the underlying principle, not the full list — a genuinely
     illegible vendor name, date, or anything else on a poor scan qualifies just as much.
     Use `ambiguous` for genuine uncertainty about what the scan actually shows, not as an
     escape hatch from clear violations — a record with other unambiguous problems (a
     clearly-printed value that's simply out of policy, wildly over cap, a huge mismatch)
     is still more honestly `rejected` than punted to a human.

Do NOT implement cross-record / uniqueness checks — the host enforces invoice-number
uniqueness deterministically after your validator runs (and the lakeFS pre-merge gate
independently re-checks ALL rules, including uniqueness). Judge each record by itself.

I/O contract for the script you write:
  - It is invoked as:  python <script> <input_json_path> <output_json_path>
  - The input JSON has shape:
      {{"today": "YYYY-MM-DD", "policy_cap": <number>, "min_year": <int>,
        "rows": [{{"source_file": str,
                   "record": {{"vendor": str, "date": str, "invoice_no": str,
                               "currency": str, "total": <number|string|null>,
                               "line_items": [{{"name": str, "amount": <number|string>}}]}}}}]}}
    `record.date` is pre-normalised to ISO `YYYY-MM-DD` (or "" if it couldn't be parsed) and
    `today` is ISO `YYYY-MM-DD`; other fields are as extracted (amounts may be strings).
  - It must write the output JSON with shape:
      {{"outcomes": [{{"source_file": str, "outcome": "accepted"|"rejected"|"ambiguous",
                       "reasons": [str]}}]}}
    with EXACTLY ONE outcome per input row (same `source_file`s), and reasons empty for
    accepted rows.
  - You may use the Python standard library and `python-dateutil` (already installed).
    Do NOT use the network or any other third-party package.
"""


@dataclass
class FileOutcome:
    """Final disposition of a single inbox file."""
    source_file: str
    outcome: str                 # "accepted" | "rejected" | "dropped" | "pending_review"
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


def to_iso_date(raw) -> str:
    """Normalise an extracted date (any printed format) to ISO ``YYYY-MM-DD``.

    Returns ``""`` if missing/unparseable. The host runs this before handing receipts to the
    generated validator, so the LLM-written code compares plain ISO strings instead of doing
    flexible date parsing — the most error-prone thing it was asked to do.
    """
    raw = (str(raw).strip() if raw is not None else "")
    if not raw:
        return ""
    try:
        return dateparser.parse(raw).date().isoformat()
    except (ValueError, OverflowError, TypeError):
        return ""


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


def classify_ambiguous(
    record: dict,
    *,
    today: date,
    policy_cap: float = POLICY_CAP_USD,
) -> str | None:
    """Deterministic approximation of ``RULES_SPEC`` rule 7 — the two illustrative examples
    from the spec, expressed as code. Used ONLY by the reference/fallback validator
    (:func:`business_rule_outcomes`), for when the LLM-generated validator can't produce a
    conforming script at all. Rule 7 itself is deliberately open-ended ("YOU decide what
    counts; there is no fixed list") — the generated validator's own "ambiguous" calls are
    trusted as-is by the host and are NOT re-checked against this function. This is only a
    reasonable fallback approximation for the two cases the spec calls out by name, not the
    full extent of what "ambiguous" is allowed to mean.

    Returns ``"currency_unclear"`` or ``"total_mismatch"`` when relaxing exactly that one
    field would make the record pass every other rule, else ``None`` (a hard reject, or
    the record already passes cleanly).
    """
    reasons = check_business_rules(record, today=today, seen_invoice_nos=set(), policy_cap=policy_cap)
    if not reasons:
        return None

    currency = (record.get("currency") or "").strip().upper()
    if not currency:
        # Blank means the extractor couldn't confidently read a currency at all — that's
        # the ambiguous case. A CONFIDENTLY-read non-USD currency is a plain rule-5 reject,
        # not ambiguous (no relaxation attempted for it here).
        relaxed = {**record, "currency": "USD"}
        if not check_business_rules(relaxed, today=today, seen_invoice_nos=set(), policy_cap=policy_cap):
            return "currency_unclear"

    total = _parse_amount(record.get("total"))
    items = record.get("line_items") or []
    if total is not None and items:
        amounts = [_parse_amount(it.get("amount") if isinstance(it, dict) else None) for it in items]
        if all(a is not None for a in amounts):
            item_sum = sum(amounts)
            gap = abs(item_sum - total)
            band = max(AMBIGUOUS_TOTAL_MIN_USD, AMBIGUOUS_TOTAL_FRACTION * total)
            if AMOUNT_TOLERANCE < gap <= band:
                relaxed = {**record, "total": item_sum}
                if not check_business_rules(relaxed, today=today, seen_invoice_nos=set(), policy_cap=policy_cap):
                    return "total_mismatch"

    return None


def _convert_record_to_usd(record: dict, currency: str, rate: float) -> dict | None:
    """Convert every line item (and derive the total from THEIR sum, not independently —
    otherwise rounding could make total != sum(items) and the unchanged lakeFS gate would
    block a legitimately converted row on a rule it never should have failed). Returns
    ``None`` if a line item amount couldn't be parsed."""
    items = record.get("line_items") or []
    if items:
        converted_items, running_sum = [], 0.0
        for it in items:
            amt = _parse_amount(it.get("amount") if isinstance(it, dict) else None)
            if amt is None:
                return None
            c = round(amt * rate, 2)
            converted_items.append({**it, "amount": c})
            running_sum += c
        record["line_items"] = converted_items
        record["total"] = round(running_sum, 2)
    else:
        total = _parse_amount(record.get("total"))
        if total is None:
            return None
        record["total"] = round(total * rate, 2)
    record["currency"] = "USD"
    return record


def resolve_ambiguous(item: dict, decision: str) -> dict:
    """Apply a human's decision to a row the (agent-written) validator flagged ambiguous —
    for WHATEVER reason it did; this function doesn't need or check the reason.

    ``item`` is ``{"source_file", "record", "reasons"}`` (as surfaced in
    ``pending_review.json``). ``decision`` is ``"approve"``, ``"reject"``, or a currency the
    human identified (e.g. ``"USD"``, ``"EUR"``; case-insensitive) — always available as an
    option regardless of why the row was ambiguous, since the validator has no network
    access and can't look up an exchange rate itself; naming a currency the fixed demo rate
    table knows converts the row, and naming "USD" needs no conversion.

    Returns ``{"outcome": "accepted"|"rejected", "record": dict, "reason": str}``.

    ``"approve"`` leaves the record untouched on purpose: an approved-but-still-unresolved
    problem still fails the lakeFS pre-merge gate's independent policy check — only actually
    fixing it (or ``"reject"``) changes the merge outcome. No one, not even a human
    reviewer, bypasses the policy encoded in lakeFS itself.
    """
    record = dict(item["record"])
    reason_hint = "; ".join(item.get("reasons") or []) or "ambiguous"

    if decision == "reject":
        return {"outcome": "rejected", "record": record, "reason": f"human review: rejected ({reason_hint})"}

    if decision == "approve":
        return {"outcome": "accepted", "record": record, "reason": ""}

    currency = (decision or "").strip().upper()
    if currency == "USD":
        record["currency"] = "USD"
        return {"outcome": "accepted", "record": record, "reason": "human review: identified as USD"}

    if currency in FX_TO_USD:
        rate = FX_TO_USD[currency]
        converted = _convert_record_to_usd(record, currency, rate)
        if converted is None:
            return {"outcome": "rejected", "record": record,
                     "reason": f"human review: identified as {currency} but couldn't convert an unparseable amount"}
        return {"outcome": "accepted", "record": converted,
                "reason": f"human review: identified as {currency}, converted to USD at {rate}"}

    return {"outcome": "rejected", "record": record, "reason": f"human review: unrecognised decision {decision!r}"}


def business_rule_outcomes(
    rows: list[dict],
    *,
    today: date,
    policy_cap: float = POLICY_CAP_USD,
    include_uniqueness: bool = True,
) -> list[dict]:
    """Reference implementation of the business rules over the drafted rows.

    ``rows`` is the ``ledger_draft.json`` shape: ``[{"source_file": str, "record": {...}}]``.
    Returns one ``{"source_file", "outcome", "reasons"}`` dict per row, ``outcome`` one of
    ``"accepted"``, ``"rejected"``, ``"ambiguous"`` (see rule 7 of :data:`RULES_SPEC`).

    With ``include_uniqueness=True`` this is the complete policy (the unit-test oracle).
    With ``include_uniqueness=False`` it covers only the per-receipt rules — matching what
    the LLM-generated validator is asked to produce (see :data:`RULES_SPEC`); the host then
    layers cross-row uniqueness on top via :func:`apply_cross_row_uniqueness`. The
    code-generation fallback uses the ``False`` variant so both paths behave identically.
    """
    inv_counts: dict[str, int] = {}
    if include_uniqueness:
        for row in rows:
            inv = (row["record"].get("invoice_no") or "").strip()
            if inv:
                inv_counts[inv] = inv_counts.get(inv, 0) + 1

    outcomes: list[dict] = []
    for row in sorted(rows, key=lambda r: r["source_file"]):
        rec = row["record"]
        reasons = check_business_rules(rec, today=today, seen_invoice_nos=set(), policy_cap=policy_cap)
        ambiguity = classify_ambiguous(rec, today=today, policy_cap=policy_cap) if reasons else None
        if include_uniqueness:
            inv = (rec.get("invoice_no") or "").strip()
            if inv and inv_counts.get(inv, 0) > 1:
                reasons.append(f"duplicate invoice number ({inv})")
                ambiguity = None  # a clear-cut duplicate doesn't need a human's attention
        if not reasons:
            outcome = "accepted"
        elif ambiguity:
            outcome = "ambiguous"
        else:
            outcome = "rejected"
        outcomes.append({
            "source_file": row["source_file"],
            "outcome": outcome,
            "reasons": reasons,
        })
    return outcomes


def apply_cross_row_uniqueness(rows: list[dict], generated: dict[str, dict]) -> dict[str, dict]:
    """Layer deterministic invoice-number uniqueness on top of the per-receipt verdicts.

    The LLM-generated validator judges each receipt on its own (RULES_SPEC rules 1–7) — it
    reliably mishandles the cross-row uniqueness rule, so the host owns that one rule here.
    ``generated`` maps source_file -> {"outcome", "reasons"} (the validator's per-receipt
    verdict, one of "accepted"/"rejected"/"ambiguous"). Any row whose non-empty invoice
    number repeats across the batch is force-rejected with a duplicate reason — a clear-cut
    duplicate doesn't need a human's attention regardless of what the validator tagged it.
    Returns the final source_file -> {"outcome", "reasons"} map.
    """
    inv_counts: dict[str, int] = {}
    for row in rows:
        inv = (row["record"].get("invoice_no") or "").strip()
        if inv:
            inv_counts[inv] = inv_counts.get(inv, 0) + 1

    final: dict[str, dict] = {}
    for row in rows:
        sf = row["source_file"]
        g = generated.get(sf, {"outcome": "rejected", "reasons": ["validator produced no outcome"]})
        reasons = [r for r in (g.get("reasons") or []) if r]
        outcome = g.get("outcome")
        if outcome not in ("accepted", "rejected", "ambiguous"):
            outcome = "rejected"
        elif outcome == "accepted" and reasons:
            outcome = "rejected"  # inconsistent validator output — don't trust a bare "accepted"
        inv = (row["record"].get("invoice_no") or "").strip()
        if inv and inv_counts.get(inv, 0) > 1:
            reasons.append(f"duplicate invoice number ({inv})")
            outcome = "rejected"
        final[sf] = {"outcome": outcome, "reasons": reasons}
    return final


def gate_input_rows(rows: list[dict], decided: dict[str, str]) -> list[dict]:
    """Project the drafted rows into typed fields the lakeFS pre-merge hook re-validates.

    The hook cannot parse dates or arbitrary CSV in its (pattern-less) Lua runtime, so the
    host normalises each drafted row here into primitives the hook can re-check with plain
    arithmetic and string compares: parsed ``total``/amounts, an ISO ``date``, the ``year``,
    and the validator's ``decided`` verdict for the row. The hook independently re-derives
    accept/reject from these fields and blocks the merge if the (LLM-written) validator
    accepted a row that violates policy. ``decided`` maps source_file -> "accepted"|"rejected".

    Note this is *data preparation*, not the verdict: the policy decision is re-made in Lua.
    """
    out: list[dict] = []
    for row in sorted(rows, key=lambda r: r["source_file"]):
        rec = row["record"]
        sf = row["source_file"]
        total = _parse_amount(rec.get("total"))
        items = rec.get("line_items") or []
        amounts = [_parse_amount(it.get("amount") if isinstance(it, dict) else None) for it in items]
        all_parseable = items and all(a is not None for a in amounts)

        date_iso = to_iso_date(rec.get("date"))
        year = int(date_iso[:4]) if date_iso else 0

        out.append({
            "source_file": sf,
            "decided": decided.get(sf, "rejected"),
            "vendor": (rec.get("vendor") or "").strip(),
            "currency": (rec.get("currency") or "").strip().upper(),
            "total": total,                      # JSON null when unparseable/missing
            "item_amounts": [a for a in amounts if a is not None] if all_parseable else [],
            "check_sum": bool(all_parseable),
            "date_iso": date_iso,                # "" when missing/unparseable
            "year": year,                        # 0 when unknown
            "invoice_no": (rec.get("invoice_no") or "").strip(),
        })
    return out


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

    @property
    def pending_review(self) -> list[FileOutcome]:
        return [o for o in self.outcomes if o.outcome == "pending_review"]

    @property
    def status(self) -> str:
        """"awaiting_review" takes priority — a run with any pending row is never mergeable,
        regardless of how the rest of the batch turned out."""
        if self.pending_review:
            return "awaiting_review"
        return "passed" if self.passed else "failed"

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "summary": self.summary,
            "failed_phase": self.failed_phase,
            "inbox_file_count": len(self.inbox_files),
            "accepted": len(self.accepted),
            "rejected": sum(1 for o in self.outcomes if o.outcome == "rejected"),
            "dropped": sum(1 for o in self.outcomes if o.outcome == "dropped"),
            "pending_review": len(self.pending_review),
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
      - every inbox file is accounted for exactly once (accepted / rejected / dropped /
        pending_review),
      - no row is still ``pending_review`` (awaiting a human decision),
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

    pending = [o for o in outcomes if o.outcome == "pending_review"]
    if pending:
        return ValidationResult(
            passed=False,
            summary=f"{len(pending)} row(s) awaiting human review",
            failed_phase=3,
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
