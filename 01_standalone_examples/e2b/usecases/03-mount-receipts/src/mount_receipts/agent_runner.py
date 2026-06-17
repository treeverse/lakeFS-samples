"""The agent, as it runs *inside* the E2B sandbox over the mounted lakeFS branch.

It is invoked once per phase by the host orchestrator (which does an ``everest commit``
after each phase, producing a progressive commit history on the branch):

    python -m mount_receipts.agent_runner triage   /mnt/repo
    python -m mount_receipts.agent_runner extract   /mnt/repo
    python -m mount_receipts.agent_runner validate  /mnt/repo

Everything is ordinary filesystem I/O against ``<root>`` — the agent has no idea it
is talking to lakeFS. State is passed between phases through files on the mount:

    inbox/<receipt files>            (input)
    sidecars/<file>.json             (per-file extraction + outcome, written in triage)
    triage.json                      (kept / dropped lists)
    ledger_draft.json                (rows that reached business-rule validation)
    validation/generated_validator.py (Phase-3 validator the LLM wrote at runtime)
    validation/validator_input.json   (records + policy handed to that script)
    validation/rule_outcomes.json      (per-row accept/reject the script produced)
    ledger.csv                       (final accepted rows; gate re-validates this)
    rejects.csv                      (dropped + rejected rows, with reasons)
    validation/latest_result.json    (read by the lakeFS pre-merge gate)
"""
from __future__ import annotations

import csv
import glob
import json
import os
import sys
from datetime import date

from dateutil import parser as dateparser

from mount_receipts import codegen
from mount_receipts.extraction import (
    extract,
    load_image_png,
    missing_required,
    sha256_file,
)
from mount_receipts.validation import (
    POLICY_CAP_USD,
    FileOutcome,
    validate_ledger,
)

INBOX = "inbox"
SIDECARS = "sidecars"


def _p(root: str, *parts: str) -> str:
    return os.path.join(root, *parts)


def _write_json(path: str, obj) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


def _read_json(path: str):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _inbox_files(root: str) -> list[str]:
    files = [
        os.path.basename(p)
        for p in glob.glob(_p(root, INBOX, "*"))
        if os.path.isfile(p)
    ]
    return sorted(files)


# --- Phase 1: triage -------------------------------------------------------

def phase_triage(root: str, model: str) -> dict:
    inbox = _inbox_files(root)
    os.makedirs(_p(root, SIDECARS), exist_ok=True)
    seen_hashes: dict[str, str] = {}
    kept: list[str] = []
    dropped: list[dict] = []

    for fn in inbox:
        path = _p(root, INBOX, fn)
        digest = sha256_file(path)
        if digest in seen_hashes:
            dropped.append({"file": fn, "phase": 1, "reason": f"exact duplicate of {seen_hashes[digest]}"})
            continue
        seen_hashes[digest] = fn

        png = load_image_png(path)
        if png is None:
            dropped.append({"file": fn, "phase": 1, "reason": "corrupt / unreadable file"})
            continue

        record = extract(png, model=model)
        _write_json(_p(root, SIDECARS, fn + ".json"), record)
        if not record.get("is_receipt"):
            dropped.append({"file": fn, "phase": 1, "reason": "not a receipt"})
            continue
        kept.append(fn)

    _write_json(_p(root, "triage.json"), {"inbox": inbox, "kept": kept, "dropped": dropped})
    summary = {"phase": "triage", "inbox": len(inbox), "kept": len(kept), "dropped": len(dropped)}
    print(json.dumps(summary))
    return summary


# --- Phase 2: extract ------------------------------------------------------

def phase_extract(root: str) -> dict:
    triage = _read_json(_p(root, "triage.json"))
    draft: list[dict] = []
    failed: list[dict] = []

    for fn in triage["kept"]:
        record = _read_json(_p(root, SIDECARS, fn + ".json"))
        miss = missing_required(record)
        if miss:
            failed.append({"file": fn, "phase": 2, "reason": f"missing fields: {', '.join(miss)}"})
        else:
            draft.append({"source_file": fn, "record": record})

    _write_json(_p(root, "ledger_draft.json"), {"rows": draft, "extraction_failed": failed})
    summary = {"phase": "extract", "kept": len(triage["kept"]), "extracted": len(draft), "extraction_failed": len(failed)}
    print(json.dumps(summary))
    return summary


# --- Phase 3: validate -----------------------------------------------------

def _today() -> date:
    override = os.environ.get("DEMO_TODAY", "").strip()
    return date.fromisoformat(override) if override else date.today()


def _iso_date(raw) -> str:
    """Normalise an extracted date to ISO ``YYYY-MM-DD`` so the lakeFS gate can re-check it.

    Accepted rows have already passed validation (date is parseable), so this rarely falls
    through; it returns the raw string defensively if parsing ever fails.
    """
    try:
        return dateparser.parse(str(raw)).date().isoformat()
    except (ValueError, OverflowError, TypeError):
        return str(raw or "")


def _total_number(raw) -> str:
    """Render ``total`` as a plain, comma-free number string for the ledger CSV / gate."""
    if raw is None:
        return ""
    if isinstance(raw, (int, float)):
        return f"{float(raw):.2f}"
    try:
        return f"{float(str(raw).replace(',', '').replace('$', '').strip()):.2f}"
    except ValueError:
        return str(raw)


def phase_validate(root: str, model: str) -> dict:
    triage = _read_json(_p(root, "triage.json"))
    draft_doc = _read_json(_p(root, "ledger_draft.json"))
    draft = sorted(draft_doc["rows"], key=lambda r: r["source_file"])
    today = _today()

    # The business-rule validator is WRITTEN BY THE MODEL at runtime and executed inside
    # this sandbox — code we have never seen, contained by E2B. (Structural phase-1 drops
    # and phase-2 extraction failures below stay deterministic; they were already decided.)
    gen = codegen.generate_and_run_validator(
        root, draft, today=today, policy_cap=POLICY_CAP_USD, model=model
    )
    rule_outcomes = {o["source_file"]: o for o in gen["outcomes"]}

    outcomes: list[FileOutcome] = []
    # phase-1 drops
    for d in triage["dropped"]:
        outcomes.append(FileOutcome(d["file"], "dropped", d.get("phase", 1), d["reason"]))
    # phase-2 extraction failures → rejected
    for d in draft_doc.get("extraction_failed", []):
        outcomes.append(FileOutcome(d["file"], "rejected", 2, d["reason"]))

    # phase-3 outcomes, as decided by the generated validator
    for row in draft:
        rec = row["record"]
        o = rule_outcomes.get(row["source_file"], {"outcome": "rejected", "reasons": ["validator produced no outcome"]})
        reasons = [r for r in (o.get("reasons") or []) if r]
        if o.get("outcome") == "accepted" and not reasons:
            outcomes.append(FileOutcome(row["source_file"], "accepted", 3, "", rec))
        else:
            outcomes.append(FileOutcome(row["source_file"], "rejected", 3, "; ".join(reasons) or "rejected", rec))

    result = validate_ledger(triage["inbox"], outcomes)

    # human-facing ledger + rejects. Policy-relevant columns come first and are comma-free
    # (ISO date, currency code, numeric total); the free-text `vendor` is last so the
    # lakeFS pre-merge hook can re-parse the ledger without a full CSV reader.
    with open(_p(root, "ledger.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source_file", "invoice_no", "date", "currency", "total", "num_items", "vendor"])
        for o in result.accepted:
            r = o.record or {}
            w.writerow([o.source_file, r.get("invoice_no", ""), _iso_date(r.get("date", "")),
                        (r.get("currency", "") or "").upper(), _total_number(r.get("total")),
                        len(r.get("line_items") or []), r.get("vendor", "")])
    with open(_p(root, "rejects.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source_file", "outcome", "phase", "reason"])
        for o in outcomes:
            if o.outcome != "accepted":
                w.writerow([o.source_file, o.outcome, o.phase, o.reason])

    result_dict = result.to_dict()
    result_dict["validator"] = {
        "generated": gen["generated"],
        "attempts": gen["attempts"],
        "source": "validation/generated_validator.py" if gen["generated"] else "reference-fallback",
    }
    _write_json(_p(root, "validation", "latest_result.json"), result_dict)
    summary = dict(result_dict)
    summary["phase"] = "validate"
    print(json.dumps({k: summary[k] for k in ("phase", "status", "summary", "accepted", "rejected", "dropped")}))
    return summary


PHASES = {"triage": 1, "extract": 2, "validate": 3}


def main(argv: list[str]) -> int:
    if len(argv) < 3 or argv[1] not in PHASES:
        print(f"usage: agent_runner <{'|'.join(PHASES)}> <mount_dir>", file=sys.stderr)
        return 2
    phase, root = argv[1], argv[2]
    model = os.environ.get("OPENAI_MODEL", "gpt-4o")
    if phase == "triage":
        phase_triage(root, model)
    elif phase == "extract":
        phase_extract(root)
    else:
        result = phase_validate(root, model)
        return 0 if result["status"] == "passed" else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
