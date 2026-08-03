"""Deterministic, fail-closed validation of an agent's curated workspace.

The validator recomputes the correct outcome from the source corpus and checks
the objects the agent actually wrote against it. The agent cannot declare its own
output valid: validation lives here, runs deterministically, and fails closed --
any deviation, missing provenance, or unbalanced count yields ``valid == False``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from common.provenance import PROVENANCE_KEYS, is_provenance_complete

from . import rules
from .rules import (
    ALL_REASON_CODES,
    CURATED_REGION,
    RESTRICTED_CLASSIFICATIONS,
    SourceObject,
    contains_pii,
    curate,
)


@dataclass
class WrittenCurated:
    """A curated object the agent wrote to the workspace output prefix."""

    path: str
    document_id: str
    action: str
    record: dict[str, Any]
    provenance: dict[str, Any]


@dataclass
class ValidationResult:
    valid: bool
    checks: list[dict[str, Any]]
    errors: list[str]
    reconciliation: dict[str, Any]
    expected: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "checks": self.checks,
            "errors": self.errors,
            "reconciliation": self.reconciliation,
            "expected": self.expected,
        }


def expected_from_corpus(corpus: list[SourceObject]) -> dict[str, Any]:
    result = curate(corpus)
    return {
        "curated": {c.document_id: c for c in result.curated},
        "quarantined": {q.source_path: q for q in result.quarantined},
        "reconciliation": result.reconciliation(),
        "result": result,
    }


def validate_workspace(
    corpus: list[SourceObject],
    curated_written: list[WrittenCurated],
    quarantine_written: list[dict[str, Any]],
) -> ValidationResult:
    """Validate the agent's workspace outputs against the recomputed expectation."""
    checks: list[dict[str, Any]] = []
    errors: list[str] = []

    def check(name: str, passed: bool, detail: str = "") -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})
        if not passed:
            errors.append(f"{name}: {detail}" if detail else name)

    exp = expected_from_corpus(corpus)
    exp_curated: dict[str, rules.CuratedItem] = exp["curated"]
    exp_quar: dict[str, rules.QuarantineItem] = exp["quarantined"]

    written_by_doc = {w.document_id: w for w in curated_written}
    quar_by_path = {q.get("source_path"): q for q in quarantine_written}

    # 1. Curated set matches expected exactly.
    check(
        "curated_set_matches",
        set(written_by_doc) == set(exp_curated),
        f"expected {sorted(exp_curated)}, got {sorted(written_by_doc)}",
    )

    # 2. Per curated object: content-policy invariants + provenance + action.
    for doc_id, w in written_by_doc.items():
        rec = w.record
        region = rules.normalize_region(rec.get("region"))
        check(f"curated[{doc_id}].region_is_us", region == CURATED_REGION, str(region))
        check(
            f"curated[{doc_id}].not_draft",
            str(rec.get("status", "")).lower() != "draft",
            str(rec.get("status")),
        )
        check(
            f"curated[{doc_id}].not_restricted",
            str(rec.get("classification", "")).lower() not in RESTRICTED_CLASSIFICATIONS,
            str(rec.get("classification")),
        )
        check(
            f"curated[{doc_id}].no_pii",
            not contains_pii(str(rec.get("body", ""))),
            "synthetic PII present" if contains_pii(str(rec.get("body", ""))) else "",
        )
        missing = [f for f in rules.REQUIRED_FIELDS if not str(rec.get(f, "")).strip()]
        check(f"curated[{doc_id}].required_fields", not missing, f"missing {missing}")
        check(
            f"curated[{doc_id}].provenance_complete",
            is_provenance_complete(w.provenance),
            f"need keys {list(PROVENANCE_KEYS)}",
        )
        exp_item = exp_curated.get(doc_id)
        if exp_item is not None:
            check(
                f"curated[{doc_id}].action_matches",
                w.action == exp_item.action,
                f"expected {exp_item.action}, got {w.action}",
            )

    # 3. No duplicate content hashes among curated.
    hashes = [rules.content_hash(str(w.record.get("body", ""))) for w in curated_written]
    check("curated_no_duplicate_hashes", len(hashes) == len(set(hashes)))

    # 4. Quarantine set matches expected, every item has a valid reason code.
    check(
        "quarantine_set_matches",
        set(quar_by_path) == set(exp_quar),
        f"expected {sorted(exp_quar)}, got {sorted(quar_by_path)}",
    )
    for path, item in quar_by_path.items():
        code = item.get("reason_code")
        check(
            f"quarantine[{path}].valid_reason",
            code in ALL_REASON_CODES,
            f"reason {code!r} not recognised",
        )
        exp_item = exp_quar.get(path)
        if exp_item is not None:
            check(
                f"quarantine[{path}].reason_matches",
                code == exp_item.reason_code,
                f"expected {exp_item.reason_code}, got {code}",
            )

    # 5. Reconciliation: every source object appears exactly once; counts balance.
    source_paths = {o.path for o in corpus}
    curated_source_paths = {
        w.provenance.get("original_object_path") for w in curated_written
    }
    quar_source_paths = set(quar_by_path)
    accounted = curated_source_paths | quar_source_paths
    check(
        "every_source_reconciled",
        accounted == source_paths and not (curated_source_paths & quar_source_paths),
        f"unaccounted: {sorted(source_paths - accounted)}; "
        f"double-counted: {sorted(curated_source_paths & quar_source_paths)}",
    )
    balanced = len(curated_written) + len(quarantine_written) == len(corpus)
    check(
        "counts_balance",
        balanced,
        f"{len(curated_written)} + {len(quarantine_written)} != {len(corpus)}",
    )

    reconciliation = {
        "examined": len(corpus),
        "curated": len(curated_written),
        "corrected": sum(
            1 for w in curated_written if w.action == rules.ACTION_CURATED_CORRECTED
        ),
        "quarantined": len(quarantine_written),
        "balanced": balanced,
    }

    valid = all(c["passed"] for c in checks)
    return ValidationResult(
        valid=valid,
        checks=checks,
        errors=errors,
        reconciliation=reconciliation,
        expected={
            "curated_document_ids": sorted(exp_curated),
            "quarantined_source_paths": sorted(exp_quar),
        },
    )
