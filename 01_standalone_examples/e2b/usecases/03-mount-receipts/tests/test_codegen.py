"""Tests for the code-generation harness (no OpenAI calls — the client is stubbed).

These exercise the generate → write → run-in-subprocess → schema-check → repair → fallback
flow. The "model" returns canned Python scripts so the harness runs entirely offline.
"""
from __future__ import annotations

import json
import os
from datetime import date

from mount_receipts import codegen
from mount_receipts.validation import business_rule_outcomes

TODAY = date(2026, 6, 5)

# A correct validator, expressed via the package's reference implementation. Stands in for
# what a capable model would emit; proves the harness runs generated code end to end.
GOOD_SCRIPT = """\
import json, sys
from datetime import date
from mount_receipts.validation import business_rule_outcomes
inp = json.load(open(sys.argv[1]))
out = business_rule_outcomes(inp["rows"], today=date.fromisoformat(inp["today"]),
                             policy_cap=inp["policy_cap"], include_uniqueness=False)
json.dump({"outcomes": out}, open(sys.argv[2], "w"))
"""

# Conforms to the contract but is wrong about *which* rows — forces a schema repair.
WRONG_SHAPE_SCRIPT = """\
import json, sys
json.dump({"outcomes": [{"source_file": "ghost.jpg", "outcome": "accepted", "reasons": []}]}, open(sys.argv[2], "w"))
"""

BROKEN_SCRIPT = "this is not valid python :("


class _Msg:
    def __init__(self, content):
        self.content = content


class _Choice:
    def __init__(self, content):
        self.message = _Msg(content)


class _Resp:
    def __init__(self, content):
        self.choices = [_Choice(content)]


class _Completions:
    def __init__(self, scripts):
        self._scripts = scripts
        self.calls = 0

    def create(self, **kwargs):
        script = self._scripts[min(self.calls, len(self._scripts) - 1)]
        self.calls += 1
        return _Resp(script)


class _StubClient:
    """Returns canned scripts in order; repeats the last one once exhausted."""
    def __init__(self, scripts):
        self.chat = type("_Chat", (), {"completions": _Completions(scripts)})()


def _rows():
    rec = dict(vendor="Acme", invoice_no="OK-1", date="2026-01-10", currency="USD",
               line_items=[{"name": "x", "amount": 10.0}], total=10.0)
    bad = dict(rec, invoice_no="BAD-1", currency="EUR")
    return [{"source_file": "ok.jpg", "record": rec}, {"source_file": "bad.jpg", "record": dict(bad)}]


def test_generated_validator_runs_and_is_used(tmp_path):
    rows = _rows()
    client = _StubClient([GOOD_SCRIPT])
    res = codegen.generate_and_run_validator(str(tmp_path), rows, today=TODAY, model="x", client=client)

    assert res["generated"] is True
    assert res["attempts"] == 1
    # The generated script and its committed artefacts are on disk for audit.
    assert os.path.exists(tmp_path / "validation" / "generated_validator.py")
    assert os.path.exists(tmp_path / "validation" / "rule_outcomes.json")
    by_file = {o["source_file"]: o for o in res["outcomes"]}
    assert by_file["ok.jpg"]["outcome"] == "accepted"
    assert by_file["bad.jpg"]["outcome"] == "rejected"


def test_schema_mismatch_triggers_repair(tmp_path):
    rows = _rows()
    # First attempt returns the wrong rows (schema check fails), second is correct.
    client = _StubClient([WRONG_SHAPE_SCRIPT, GOOD_SCRIPT])
    res = codegen.generate_and_run_validator(str(tmp_path), rows, today=TODAY, model="x", client=client)

    assert res["generated"] is True
    assert res["attempts"] == 2


def test_falls_back_to_reference_after_exhaustion(tmp_path):
    rows = _rows()
    client = _StubClient([BROKEN_SCRIPT])  # never runs → all attempts fail
    res = codegen.generate_and_run_validator(str(tmp_path), rows, today=TODAY, model="x", client=client)

    assert res["generated"] is False
    assert res["attempts"] == codegen.MAX_ATTEMPTS
    # Fallback output must match the per-receipt reference (uniqueness added later by host).
    assert res["outcomes"] == business_rule_outcomes(rows, today=TODAY, include_uniqueness=False)
    # The contract output file is still written so downstream/audit stays consistent.
    doc = json.load(open(tmp_path / "validation" / "rule_outcomes.json"))
    assert {o["source_file"] for o in doc["outcomes"]} == {"ok.jpg", "bad.jpg"}
