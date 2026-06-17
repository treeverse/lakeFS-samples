"""Phase 3, as agent-*written* code: the model emits a validator, the sandbox runs it.

In the earlier version of this demo the agent simply executed a validator we shipped in
the repo (``validation.check_business_rules``). That worked, but it didn't need E2B —
trusted, known code can run anywhere. Here the model is instead handed the rule
*specification* (:data:`mount_receipts.validation.RULES_SPEC`) and the extracted receipts,
and asked to **write a Python validator from scratch**. We then execute that
never-before-seen script as a child process inside the sandbox.

That is where E2B earns its place: we are running code we have never seen, generated at
runtime, and the sandbox contains whatever it does. lakeFS still owns the *gate* — its
pre-merge hook independently re-derives pass/fail from the committed ledger and does not
trust whatever this generated code reported (see ``lakefs_actions/validate_ledger.yaml``).

If the model can't produce a script that runs and conforms to the I/O contract after a
few self-repair attempts, we fall back to the reference implementation so the demo still
completes — but the happy path is all generated code.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

from mount_receipts.validation import (
    MIN_YEAR,
    POLICY_CAP_USD,
    RULES_SPEC,
    business_rule_outcomes,
)

MAX_ATTEMPTS = 3
RUN_TIMEOUT_S = 120

GENERATOR_SYSTEM_PROMPT = (
    "You are a senior Python engineer. You write small, correct, self-contained command-"
    "line scripts. You output ONLY Python source code — no prose, no markdown fences."
)


def _strip_fences(text: str) -> str:
    """Remove ```python ... ``` fences if the model wrapped its answer in them."""
    t = text.strip()
    if t.startswith("```"):
        lines = t.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        t = "\n".join(lines)
    return t.strip() + "\n"


def _build_prompt(repair_from: str | None = None, error: str | None = None) -> str:
    if repair_from is None:
        return (
            "Write a complete Python 3 script that implements the following validation "
            "specification.\n\n"
            f"{RULES_SPEC}\n"
            "The script must read its two path arguments from sys.argv, do the validation, "
            "and write the output JSON. Print nothing to stdout. Exit non-zero only on an "
            "internal error."
        )
    return (
        "The validator script you wrote failed when executed. Here is the script:\n\n"
        f"{repair_from}\n\n"
        f"It failed with:\n{error}\n\n"
        "Return a corrected, complete replacement script (full file, not a diff) that "
        "satisfies the same specification and I/O contract."
    )


def _generate_source(model, client, *, repair_from=None, error=None) -> str:
    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        messages=[
            {"role": "system", "content": GENERATOR_SYSTEM_PROMPT},
            {"role": "user", "content": _build_prompt(repair_from, error)},
        ],
    )
    return _strip_fences(resp.choices[0].message.content or "")


def _validate_output(doc, expected_files: set[str]) -> list[dict]:
    """Check the generated script's output against the contract; raise ValueError if off."""
    if not isinstance(doc, dict) or not isinstance(doc.get("outcomes"), list):
        raise ValueError("output is not an object with an 'outcomes' list")
    outcomes = doc["outcomes"]
    got_files = set()
    for o in outcomes:
        if not isinstance(o, dict):
            raise ValueError("each outcome must be an object")
        sf = o.get("source_file")
        outcome = o.get("outcome")
        reasons = o.get("reasons", [])
        if outcome not in ("accepted", "rejected"):
            raise ValueError(f"bad outcome value for {sf!r}: {outcome!r}")
        if not isinstance(reasons, list):
            raise ValueError(f"reasons must be a list for {sf!r}")
        got_files.add(sf)
    if got_files != expected_files:
        missing = expected_files - got_files
        extra = got_files - expected_files
        raise ValueError(f"source_file mismatch (missing={sorted(missing)}, extra={sorted(extra)})")
    return outcomes


def generate_and_run_validator(
    root: str,
    rows: list[dict],
    *,
    today,
    policy_cap: float = POLICY_CAP_USD,
    model: str,
    client=None,
) -> dict:
    """Generate a validator with the LLM, run it in-sandbox, repair on failure, fall back.

    Returns ``{"outcomes": [...], "generated": bool, "attempts": int,
    "validator_path": str|None}``. The generated script, its input, and its output are
    written under ``<root>/validation/`` so they are committed to the lakeFS branch and
    can be audited (you can read the exact code the model ran).
    """
    val_dir = os.path.join(root, "validation")
    os.makedirs(val_dir, exist_ok=True)
    input_path = os.path.join(val_dir, "validator_input.json")
    output_path = os.path.join(val_dir, "rule_outcomes.json")
    script_path = os.path.join(val_dir, "generated_validator.py")

    payload = {
        "today": today.isoformat(),
        "policy_cap": policy_cap,
        "min_year": MIN_YEAR,
        "rows": rows,
    }
    with open(input_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    expected_files = {r["source_file"] for r in rows}

    if client is None:
        from openai import OpenAI

        client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    source: str | None = None
    last_error: str | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            source = _generate_source(model, client, repair_from=source, error=last_error)
            with open(script_path, "w", encoding="utf-8") as f:
                f.write(source)

            # Run the model's code as a child process. This is the untrusted step the E2B
            # sandbox exists to contain.
            proc = subprocess.run(
                [sys.executable, script_path, input_path, output_path],
                capture_output=True, text=True, timeout=RUN_TIMEOUT_S,
            )
            if proc.returncode != 0:
                raise RuntimeError(f"exit {proc.returncode}: {(proc.stderr or proc.stdout)[-1500:]}")

            with open(output_path, encoding="utf-8") as f:
                doc = json.load(f)
            outcomes = _validate_output(doc, expected_files)

            print(json.dumps({"validator": "generated", "attempt": attempt, "rows": len(outcomes)}))
            return {"outcomes": outcomes, "generated": True, "attempts": attempt, "validator_path": script_path}
        except Exception as exc:  # codegen produced unrunnable / non-conforming code
            last_error = f"{type(exc).__name__}: {exc}"
            print(json.dumps({"validator": "generated", "attempt": attempt, "error": last_error[:500]}),
                  file=sys.stderr)

    # All attempts exhausted — keep the demo runnable with the reference implementation.
    # Per-receipt rules only (uniqueness is layered on by the host), mirroring RULES_SPEC.
    print(json.dumps({"validator": "fallback-reference", "attempts": MAX_ATTEMPTS,
                      "last_error": (last_error or "")[:500]}), file=sys.stderr)
    outcomes = business_rule_outcomes(rows, today=today, policy_cap=policy_cap, include_uniqueness=False)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"outcomes": outcomes}, f, indent=2)
    return {"outcomes": outcomes, "generated": False, "attempts": MAX_ATTEMPTS, "validator_path": None}
