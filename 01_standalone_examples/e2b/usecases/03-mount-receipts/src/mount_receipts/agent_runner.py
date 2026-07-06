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
    validation/rule_outcomes.json      (per-row accept/reject/ambiguous the script produced)
    validation/pending_review.json     (rows flagged "ambiguous", awaiting a human decision)
    validation/human_decisions.json    (host writes this after a human answers; resumes Phase 3)
    validation/human_review_log.json   (audit: what was asked, what was decided)
    validation/gate_input.json         (typed rows the lakeFS gate re-validates)
    ledger.csv                       (final accepted rows)
    rejects.csv                      (dropped + rejected rows, with reasons)
    validation/latest_result.json    (read by the lakeFS pre-merge gate; status can be
                                       "passed" | "failed" | "awaiting_review")
"""
from __future__ import annotations

import csv
import glob
import json
import os
import sys
from datetime import date


from mount_receipts import codegen
from mount_receipts.extraction import (
    extract,
    load_image_png,
    missing_required,
    sha256_file,
)
from mount_receipts.validation import (
    MIN_YEAR,
    POLICY_CAP_USD,
    FileOutcome,
    apply_cross_row_uniqueness,
    gate_input_rows,
    resolve_ambiguous,
    to_iso_date,
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
    """ISO ``YYYY-MM-DD`` for the human-facing ledger (falls back to raw if unparseable)."""
    return to_iso_date(raw) or str(raw or "")


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


def _tamper_accepted(result) -> None:
    """Corrupt one accepted row's record in place so it violates policy (currency → EUR).

    Used only when DEMO_TAMPER is set. The row's FileOutcome stays "accepted" (so the
    self-reported result still says passed), but the committed ledger + gate inputs now
    carry a non-USD row — which the lakeFS pre-merge gate independently rejects.
    """
    accepted = result.accepted
    if not accepted:
        print(json.dumps({"demo_tamper": "skipped — no accepted rows"}), file=sys.stderr)
        return
    victim = accepted[0]
    rec = victim.record or {}
    original = rec.get("currency")
    rec["currency"] = "EUR"
    print(json.dumps({"demo_tamper": "active", "row": victim.source_file,
                      "currency": f"{original} -> EUR (gate should block this merge)"}),
          file=sys.stderr)


def _load_or_generate_rule_outcomes(root: str, draft: list[dict], *, today, model: str) -> tuple[dict, dict]:
    """Run the LLM-generated Phase-3 validator once per branch; on a re-run (after a human
    has answered), reuse what's already on disk instead of paying for codegen again.

    Returns ``(generated, codegen_meta)`` — ``generated`` maps source_file -> {"outcome",
    "reasons"} (outcome one of accepted/rejected/ambiguous, per ``RULES_SPEC``);
    ``codegen_meta`` is ``{"generated": bool, "attempts": int}`` for the final report.
    """
    outcomes_path = _p(root, "validation", "rule_outcomes.json")
    meta_path = _p(root, "validation", "codegen_meta.json")
    if os.path.exists(outcomes_path) and os.path.exists(meta_path):
        generated = {o["source_file"]: o for o in _read_json(outcomes_path)["outcomes"]}
        return generated, _read_json(meta_path)

    gen = codegen.generate_and_run_validator(
        root, draft, today=today, policy_cap=POLICY_CAP_USD, model=model
    )
    generated = {o["source_file"]: o for o in gen["outcomes"]}
    meta = {"generated": gen["generated"], "attempts": gen["attempts"]}
    _write_json(meta_path, meta)
    return generated, meta


def phase_validate(root: str, model: str) -> dict:
    triage = _read_json(_p(root, "triage.json"))
    draft_doc = _read_json(_p(root, "ledger_draft.json"))
    draft = sorted(draft_doc["rows"], key=lambda r: r["source_file"])
    today = _today()

    # The per-receipt validator is WRITTEN BY THE MODEL at runtime and executed inside this
    # sandbox — code we have never seen, contained by E2B. It judges each receipt on its own
    # (RULES_SPEC rules 1–7, including which rows are "ambiguous" rather than a clean
    # accept/reject); the host then layers the cross-row invoice-uniqueness rule on top
    # deterministically (LLMs reliably mishandle that whole-batch rule). Structural phase-1
    # drops and phase-2 extraction failures below stay deterministic too.
    generated, codegen_meta = _load_or_generate_rule_outcomes(root, draft, today=today, model=model)
    rule_outcomes = apply_cross_row_uniqueness(draft, generated)

    # A resolved decision updates a row's record (e.g. currency/total once identified) for
    # everything downstream.
    records_by_file: dict[str, dict] = {row["source_file"]: row["record"] for row in draft}

    decisions_path = _p(root, "validation", "human_decisions.json")
    decisions = _read_json(decisions_path) if os.path.exists(decisions_path) else {}

    # Resolve any row the validator flagged "ambiguous" — either with a human's decision
    # (if one has come back), or by adding it to the pending list this pass will report.
    # The agent decides WHETHER a row is ambiguous and the host trusts that call as-is —
    # for whatever reason the validator gave, not just the two examples RULES_SPEC calls
    # out by name. The host never re-derives or second-guesses that judgment.
    review_log: list[dict] = []
    pending: list[dict] = []
    for row in draft:
        sf = row["source_file"]
        o = rule_outcomes[sf]
        if o["outcome"] != "ambiguous":
            continue

        item = {"source_file": sf, "record": records_by_file[sf], "reasons": o["reasons"]}
        answer = decisions.get(sf)
        if answer is None:
            pending.append(item)
            continue
        resolved = resolve_ambiguous(item, answer.get("decision", ""))
        rule_outcomes[sf] = {
            "outcome": resolved["outcome"],
            "reasons": [] if resolved["outcome"] == "accepted" else [resolved["reason"]],
        }
        records_by_file[sf] = resolved["record"]
        review_log.append({
            "source_file": sf,
            "agent_reasons": o["reasons"],
            "decision": answer.get("decision", ""),
            "note": answer.get("note", ""),
            "resolved_outcome": resolved["outcome"],
            "resolved_reason": resolved["reason"],
        })

    if pending:
        # Not everything is decided yet — stop here without touching ledger.csv/rejects.csv/
        # gate_input.json. The host will pause the sandbox, ask a human about each pending
        # row, write validation/human_decisions.json, and re-run this exact same phase.
        _write_json(_p(root, "validation", "pending_review.json"), pending)
        result_dict = {
            "status": "awaiting_review",
            "summary": f"{len(pending)} row(s) flagged ambiguous by the validator — awaiting human review",
            "failed_phase": 3,
            "inbox_file_count": len(triage["inbox"]),
            "pending_review": len(pending),
        }
        _write_json(_p(root, "validation", "latest_result.json"), result_dict)
        summary = dict(result_dict)
        summary["phase"] = "validate"
        print(json.dumps({k: summary[k] for k in ("phase", "status", "summary", "pending_review")}))
        return summary

    if review_log:
        _write_json(_p(root, "validation", "human_review_log.json"), review_log)

    outcomes: list[FileOutcome] = []
    # phase-1 drops
    for d in triage["dropped"]:
        outcomes.append(FileOutcome(d["file"], "dropped", d.get("phase", 1), d["reason"]))
    # phase-2 extraction failures → rejected
    for d in draft_doc.get("extraction_failed", []):
        outcomes.append(FileOutcome(d["file"], "rejected", 2, d["reason"]))

    # phase-3 outcomes (generated per-receipt verdict + deterministic uniqueness + any
    # resolved human decisions, all fully settled to accepted/rejected by this point)
    decided: dict[str, str] = {}
    for row in draft:
        sf = row["source_file"]
        rec = records_by_file[sf]
        o = rule_outcomes[sf]
        decided[sf] = o["outcome"]
        if o["outcome"] == "accepted":
            outcomes.append(FileOutcome(sf, "accepted", 3, "", rec))
        else:
            outcomes.append(FileOutcome(sf, "rejected", 3, "; ".join(o["reasons"]) or "rejected", rec))

    result = validate_ledger(triage["inbox"], outcomes)

    # DEMO_TAMPER (off by default): corrupt one accepted row AFTER it passed validation, so
    # the agent still self-reports "passed" but the committed ledger actually violates
    # policy — letting you watch the lakeFS gate independently catch it and block the merge.
    if os.environ.get("DEMO_TAMPER", "").strip() not in ("", "0", "false", "False"):
        _tamper_accepted(result)

    # Typed inputs for the lakeFS pre-merge gate. The hook re-derives accept/reject for
    # every drafted row from these primitives — independently of the LLM-written validator
    # (and of any human "approve") — and blocks the merge if a row that violates policy was
    # accepted. gate_input_rows only ever sees the final (post human-review) records.
    gate_rows = gate_input_rows(
        [{"source_file": sf, "record": records_by_file[sf]} for sf in decided], decided
    )

    _write_json(_p(root, "validation", "gate_input.json"), {
        "today": today.isoformat(),
        "policy_cap": POLICY_CAP_USD,
        "min_year": MIN_YEAR,
        "rows": gate_rows,
    })

    # human-facing ledger + rejects (ISO dates, numeric totals for downstream consumers)
    with open(_p(root, "ledger.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source_file", "vendor", "invoice_no", "date", "currency", "total", "num_items"])
        for o in result.accepted:
            r = o.record or {}
            w.writerow([o.source_file, r.get("vendor", ""), r.get("invoice_no", ""),
                        _iso_date(r.get("date", "")), (r.get("currency", "") or "").upper(),
                        _total_number(r.get("total")), len(r.get("line_items") or [])])
    with open(_p(root, "rejects.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source_file", "outcome", "phase", "reason"])
        for o in outcomes:
            if o.outcome != "accepted":
                w.writerow([o.source_file, o.outcome, o.phase, o.reason])

    result_dict = result.to_dict()
    result_dict["validator"] = {
        "generated": codegen_meta["generated"],
        "attempts": codegen_meta["attempts"],
        "source": "validation/generated_validator.py" if codegen_meta["generated"] else "reference-fallback",
    }
    result_dict["human_reviewed"] = len(review_log)
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
        # "awaiting_review" is an expected pause (the host will resume and re-run this same
        # phase after a human answers), not a failure.
        return 0 if result["status"] in ("passed", "awaiting_review") else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
