"""Deterministic synthetic corpus.

Every document is declared with its *expected* curation outcome. The corpus
bytes and ``expected-results.json`` are both generated from these declarations,
so the expectation is an independent specification -- not something computed by
the curation engine it is used to test.

Run ``python -m seed.generate`` to (re)write ``seed/expected-results.json``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

EXPECTED_PATH = Path(__file__).resolve().parent / "expected-results.json"


@dataclass
class CorpusDoc:
    relpath: str
    content_type: str
    record: dict[str, Any] | None  # None => raw text object (content is `text`)
    expected: dict[str, Any]
    text: str | None = None  # used when record is None

    def content(self) -> str:
        if self.record is not None:
            return json.dumps(self.record, indent=2, sort_keys=True)
        return self.text or ""


def _doc(
    relpath: str,
    *,
    title: str,
    product_id: str,
    region: str | None,
    classification: str | None,
    status: str,
    body: str,
    document_id: str,
    version: int = 1,
    expected: dict[str, Any],
) -> CorpusDoc:
    return CorpusDoc(
        relpath=relpath,
        content_type="application/json",
        record={
            "document_id": document_id,
            "title": title,
            "product_id": product_id,
            "region": region,
            "classification": classification,
            "status": status,
            "version": version,
            "body": body,
        },
        expected=expected,
    )


_DUP_BODY = "To restart the ACME sync agent, run `acme-agent restart` and wait 30s.\n"

CORPUS: list[CorpusDoc] = [
    # 1. valid approved US document -> curated unchanged
    _doc(
        "records/clean-us-approved.json",
        title="Resetting your ACME Cloud password",
        product_id="PROD-ACME-CLOUD",
        region="US",
        classification="public",
        status="approved",
        document_id="SUP-1001",
        body="Open the ACME Cloud console, choose Account, then Reset password.\n",
        expected={"outcome": "curated", "action": "curated_unchanged"},
    ),
    # 2. clean document that should remain unchanged -> curated unchanged
    _doc(
        "records/clean-unchanged.json",
        title="Configuring SSO for the ACME Console",
        product_id="PROD-ACME-CONSOLE",
        region="US",
        classification="internal",
        status="approved",
        document_id="SUP-1012",
        body="Enable SSO under Settings > Security and upload your IdP metadata.\n",
        expected={"outcome": "curated", "action": "curated_unchanged"},
    ),
    # 3. document assigned to another region -> quarantine
    _doc(
        "records/region-eu.json",
        title="ACME Cloud data residency (EU)",
        product_id="PROD-ACME-CLOUD",
        region="EU",
        classification="public",
        status="approved",
        document_id="SUP-1002",
        body="EU tenants store data in the Frankfurt region.\n",
        expected={"outcome": "quarantined", "reason_code": "non_us_region"},
    ),
    # 4. draft document -> quarantine
    _doc(
        "records/draft-us.json",
        title="Upcoming billing changes (DRAFT)",
        product_id="PROD-ACME-BILLING",
        region="US",
        classification="internal",
        status="draft",
        document_id="SUP-1003",
        body="Draft notes on the new metered billing model.\n",
        expected={"outcome": "quarantined", "reason_code": "draft_status"},
    ),
    # 5. missing classification metadata -> quarantine (cannot safely invent)
    _doc(
        "records/missing-classification.json",
        title="Exporting audit logs",
        product_id="PROD-ACME-AUDIT",
        region="US",
        classification=None,
        status="approved",
        document_id="SUP-1004",
        body="Use the Audit > Export button to download logs as CSV.\n",
        expected={"outcome": "quarantined", "reason_code": "missing_required_field"},
    ),
    # 6 + 7. two duplicate documents -> first curated, second quarantined
    _doc(
        "records/dup-a.json",
        title="Restarting the ACME sync agent",
        product_id="PROD-ACME-SYNC",
        region="US",
        classification="public",
        status="approved",
        document_id="SUP-1005",
        body=_DUP_BODY,
        expected={"outcome": "curated", "action": "curated_unchanged"},
    ),
    _doc(
        "records/dup-b.json",
        title="Restarting the ACME sync agent",
        product_id="PROD-ACME-SYNC",
        region="US",
        classification="public",
        status="approved",
        document_id="SUP-1005",
        body=_DUP_BODY,
        expected={"outcome": "quarantined", "reason_code": "duplicate_content"},
    ),
    # 8 + 9. conflicting versions -> latest approved curated, older quarantined
    _doc(
        "records/conflict-v1.json",
        title="Rotating API keys",
        product_id="PROD-ACME-API",
        region="US",
        classification="internal",
        status="approved",
        document_id="SUP-1006",
        version=1,
        body="Old procedure: rotate keys yearly from the console.\n",
        expected={"outcome": "quarantined", "reason_code": "superseded_by_newer_version"},
    ),
    _doc(
        "records/conflict-v2.json",
        title="Rotating API keys",
        product_id="PROD-ACME-API",
        region="US",
        classification="internal",
        status="approved",
        document_id="SUP-1006",
        version=2,
        body="New procedure: rotate keys every 90 days via the API.\n",
        expected={"outcome": "curated", "action": "curated_unchanged"},
    ),
    # 10. superseded document -> quarantine
    _doc(
        "records/superseded.json",
        title="Legacy dashboard shortcuts",
        product_id="PROD-ACME-CONSOLE",
        region="US",
        classification="public",
        status="superseded",
        document_id="SUP-1007",
        body="These shortcuts applied to the retired v1 dashboard.\n",
        expected={"outcome": "quarantined", "reason_code": "superseded_status"},
    ),
    # 11. explicit synthetic PII marker -> quarantine
    _doc(
        "records/pii-doc.json",
        title="Escalating a support ticket",
        product_id="PROD-ACME-SUPPORT",
        region="US",
        classification="internal",
        status="approved",
        document_id="SUP-1008",
        body=(
            "For urgent issues contact SYNTHETIC_PII at demo.user@example.invalid "
            "and reference your ticket id.\n"
        ),
        expected={"outcome": "quarantined", "reason_code": "synthetic_pii"},
    ),
    # 12. missing required title / product id -> quarantine
    _doc(
        "records/missing-title.json",
        title="",
        product_id="",
        region="US",
        classification="public",
        status="approved",
        document_id="SUP-1009",
        body="Body present but title and product id are blank.\n",
        expected={"outcome": "quarantined", "reason_code": "missing_required_field"},
    ),
    # 13. safe metadata normalisation -> curated corrected
    _doc(
        "records/needs-normalization.json",
        title="  Enabling two-factor authentication  ",
        product_id="PROD-ACME-CLOUD",
        region="usa",
        classification="public",
        status="approved",
        document_id="SUP-1011",
        body="Turn on 2FA under Account > Security.\n",
        expected={
            "outcome": "curated",
            "action": "curated_corrected",
            "corrections": [
                "region 'usa' -> 'US'",
                "trimmed title whitespace",
            ],
        },
    ),
    # 14. restricted classification -> quarantine
    _doc(
        "records/restricted-doc.json",
        title="Internal incident runbook",
        product_id="PROD-ACME-OPS",
        region="US",
        classification="restricted",
        status="approved",
        document_id="SUP-1013",
        body="Restricted runbook for on-call engineers.\n",
        expected={"outcome": "quarantined", "reason_code": "restricted_classification"},
    ),
    # 15. stray non-record text object -> quarantine unrecognized format
    CorpusDoc(
        relpath="notes/handoff.md",
        content_type="text/markdown",
        record=None,
        text="# Handoff notes\n\nMisc notes that are not a structured support record.\n",
        expected={"outcome": "quarantined", "reason_code": "unrecognized_format"},
    ),
]


def corpus_objects() -> list[CorpusDoc]:
    """Return the corpus documents (deterministic order by relpath)."""
    return sorted(CORPUS, key=lambda d: d.relpath)


def expected_results() -> dict[str, Any]:
    """Independent expectation, keyed by corpus-relative path."""
    docs = corpus_objects()
    objects = {d.relpath: d.expected for d in docs}
    curated = [p for p, e in objects.items() if e["outcome"] == "curated"]
    quarantined = [p for p, e in objects.items() if e["outcome"] == "quarantined"]
    corrected = [
        p for p, e in objects.items() if e.get("action") == "curated_corrected"
    ]
    return {
        "objects": objects,
        "summary": {
            "examined": len(objects),
            "curated": len(curated),
            "corrected": len(corrected),
            "quarantined": len(quarantined),
        },
    }


def write_expected_results(path: Path = EXPECTED_PATH) -> Path:
    path.write_text(json.dumps(expected_results(), indent=2, sort_keys=True) + "\n")
    return path


if __name__ == "__main__":
    p = write_expected_results()
    print(f"wrote {p}")
