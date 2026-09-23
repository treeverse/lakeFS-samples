"""Deterministic curation rules.

Given the raw corpus, ``curate()`` returns the single correct outcome: which
objects are curated (optionally after a safe metadata correction), which are
quarantined and why, and a reconciliation that balances. The same function is
the reference the server-side validator recomputes to check the agent's work,
and the reference the unit tests lock against ``seed/expected-results.json``.

Design principles:

* Fail closed. Anything a rule cannot positively clear is quarantined.
* One primary reason code per object, chosen by a fixed priority order, so the
  outcome is fully deterministic and explainable.
* Corrections are limited to safe *normalisations* (e.g. ``"usa" -> "US"``);
  the code never invents missing classifications, regions, or titles.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

# --- reason codes ---------------------------------------------------------
REASON_UNRECOGNIZED_FORMAT = "unrecognized_format"
REASON_MISSING_REQUIRED_FIELD = "missing_required_field"
REASON_SYNTHETIC_PII = "synthetic_pii"
REASON_RESTRICTED_CLASSIFICATION = "restricted_classification"
REASON_DRAFT_STATUS = "draft_status"
REASON_SUPERSEDED_STATUS = "superseded_status"
REASON_NON_US_REGION = "non_us_region"
REASON_DUPLICATE_CONTENT = "duplicate_content"
REASON_SUPERSEDED_BY_NEWER_VERSION = "superseded_by_newer_version"

ALL_REASON_CODES = frozenset(
    {
        REASON_UNRECOGNIZED_FORMAT,
        REASON_MISSING_REQUIRED_FIELD,
        REASON_SYNTHETIC_PII,
        REASON_RESTRICTED_CLASSIFICATION,
        REASON_DRAFT_STATUS,
        REASON_SUPERSEDED_STATUS,
        REASON_NON_US_REGION,
        REASON_DUPLICATE_CONTENT,
        REASON_SUPERSEDED_BY_NEWER_VERSION,
    }
)

# --- actions --------------------------------------------------------------
ACTION_CURATED_UNCHANGED = "curated_unchanged"
ACTION_CURATED_CORRECTED = "curated_corrected"

# --- required raw-record fields ------------------------------------------
REQUIRED_FIELDS = ("title", "product_id", "region", "classification", "status")

# --- allowed vocabularies -------------------------------------------------
CURATED_REGION = "US"
REGION_ALIASES = {
    "us": CURATED_REGION,
    "usa": CURATED_REGION,
    "u.s.": CURATED_REGION,
    "u.s.a.": CURATED_REGION,
    "united states": CURATED_REGION,
    "united states of america": CURATED_REGION,
    "us-east": CURATED_REGION,
}
VALID_STATUSES = {"approved", "draft", "superseded"}
RESTRICTED_CLASSIFICATIONS = {"restricted", "secret", "confidential"}

# Explicit synthetic PII markers. Detection is content based; the corpus never
# contains real PII.
PII_MARKERS = ("SYNTHETIC_PII", "demo.user@example.invalid")


def content_hash(body: str) -> str:
    """Stable content hash used for duplicate detection and provenance."""
    return "sha256:" + hashlib.sha256(body.encode("utf-8")).hexdigest()


def normalize_region(raw: str | None) -> str | None:
    if raw is None:
        return None
    return REGION_ALIASES.get(raw.strip().lower(), raw.strip())


def contains_pii(text: str) -> bool:
    return any(marker in text for marker in PII_MARKERS)


@dataclass
class SourceObject:
    """A raw corpus object as read from lakeFS."""

    path: str
    content_type: str
    content: str  # decoded text/JSON string

    def as_record(self) -> dict[str, Any] | None:
        """Parse the JSON record, or None if this is not a JSON record."""
        if not (self.content_type.endswith("json") or self.path.endswith(".json")):
            return None
        try:
            data = json.loads(self.content)
        except (ValueError, TypeError):
            return None
        return data if isinstance(data, dict) else None


@dataclass
class CuratedItem:
    source_path: str
    document_id: str
    action: str
    content_hash: str
    record: dict[str, Any]  # normalised record (metadata corrected)
    corrections: list[str] = field(default_factory=list)


@dataclass
class QuarantineItem:
    source_path: str
    reason_code: str
    detail: str
    document_id: str | None = None


@dataclass
class CurationResult:
    curated: list[CuratedItem]
    quarantined: list[QuarantineItem]
    examined: int

    def reconciliation(self) -> dict[str, int]:
        corrected = sum(1 for c in self.curated if c.action == ACTION_CURATED_CORRECTED)
        return {
            "examined": self.examined,
            "curated": len(self.curated),
            "corrected": corrected,
            "quarantined": len(self.quarantined),
            "balanced": len(self.curated) + len(self.quarantined) == self.examined,
        }

    def balances(self) -> bool:
        return len(self.curated) + len(self.quarantined) == self.examined


def _missing_fields(record: dict[str, Any]) -> list[str]:
    missing = []
    for f in REQUIRED_FIELDS:
        val = record.get(f)
        if val is None or (isinstance(val, str) and not val.strip()):
            missing.append(f)
    return missing


def _classify_single(obj: SourceObject) -> tuple[str | None, str, dict[str, Any] | None]:
    """Per-object rule pass.

    Returns ``(reason_code_or_None, detail, normalised_record_or_None)``. A None
    reason means the object is a *candidate* for curation (subject to later
    cross-object dedup/version rules).
    """
    record = obj.as_record()
    if record is None:
        return REASON_UNRECOGNIZED_FORMAT, "not a JSON support record", None

    missing = _missing_fields(record)
    if missing:
        return (
            REASON_MISSING_REQUIRED_FIELD,
            f"missing required field(s): {', '.join(missing)}",
            None,
        )

    # Content-based synthetic PII detection over the record body.
    body = str(record.get("body", ""))
    if contains_pii(body) or contains_pii(obj.content):
        return REASON_SYNTHETIC_PII, "contains synthetic PII marker", None

    classification = str(record.get("classification", "")).strip().lower()
    if classification in RESTRICTED_CLASSIFICATIONS:
        return (
            REASON_RESTRICTED_CLASSIFICATION,
            f"restricted classification: {classification}",
            None,
        )

    status = str(record.get("status", "")).strip().lower()
    if status == "draft":
        return REASON_DRAFT_STATUS, "document is a draft", None
    if status == "superseded":
        return REASON_SUPERSEDED_STATUS, "document is superseded", None

    normalised_region = normalize_region(record.get("region"))
    if normalised_region != CURATED_REGION:
        return (
            REASON_NON_US_REGION,
            f"region {record.get('region')!r} is not US",
            None,
        )

    # Candidate. Produce a normalised record and record any safe corrections.
    normalised = dict(record)
    corrections: list[str] = []
    if record.get("region") != normalised_region:
        normalised["region"] = normalised_region
        corrections.append(f"region {record.get('region')!r} -> {normalised_region!r}")
    if isinstance(record.get("title"), str) and record["title"] != record["title"].strip():
        normalised["title"] = record["title"].strip()
        corrections.append("trimmed title whitespace")
    normalised["_corrections"] = corrections
    return None, "", normalised


def curate(objects: list[SourceObject]) -> CurationResult:
    """Compute the one correct curation outcome for a corpus.

    Order of evaluation: per-object rules first (fixed priority inside
    ``_classify_single``), then cross-object duplicate resolution, then
    conflicting-version resolution. Iteration is over path-sorted objects so the
    result is fully deterministic.
    """
    objects = sorted(objects, key=lambda o: o.path)
    examined = len(objects)

    curated: list[CuratedItem] = []
    quarantined: list[QuarantineItem] = []

    # Pass 1: per-object rules.
    candidates: list[tuple[SourceObject, dict[str, Any]]] = []
    for obj in objects:
        reason, detail, normalised = _classify_single(obj)
        if reason is not None:
            rec = obj.as_record() or {}
            quarantined.append(
                QuarantineItem(
                    source_path=obj.path,
                    reason_code=reason,
                    detail=detail,
                    document_id=rec.get("document_id"),
                )
            )
        else:
            candidates.append((obj, normalised))  # type: ignore[arg-type]

    # Pass 2: duplicate resolution by content hash (keep path-first).
    seen_hashes: dict[str, str] = {}
    survivors: list[tuple[SourceObject, dict[str, Any]]] = []
    for obj, rec in candidates:
        h = content_hash(str(rec.get("body", "")))
        if h in seen_hashes:
            quarantined.append(
                QuarantineItem(
                    source_path=obj.path,
                    reason_code=REASON_DUPLICATE_CONTENT,
                    detail=f"duplicate of {seen_hashes[h]}",
                    document_id=rec.get("document_id"),
                )
            )
        else:
            seen_hashes[h] = obj.path
            survivors.append((obj, rec))

    # Pass 3: conflicting-version resolution. Group by document_id; keep the
    # highest version among approved survivors, quarantine the rest.
    by_doc: dict[str, list[tuple[SourceObject, dict[str, Any]]]] = {}
    for obj, rec in survivors:
        by_doc.setdefault(str(rec.get("document_id", obj.path)), []).append((obj, rec))

    for doc_id, group in by_doc.items():
        if len(group) == 1:
            obj, rec = group[0]
            curated.append(_to_curated(obj, rec, doc_id))
            continue
        # Highest version wins; ties broken by path for determinism.
        winner_idx = max(
            range(len(group)),
            key=lambda i: (int(group[i][1].get("version", 1)), group[i][0].path),
        )
        winner = group[winner_idx]
        for idx, (obj, rec) in enumerate(group):
            if idx == winner_idx:
                curated.append(_to_curated(obj, rec, doc_id))
            else:
                quarantined.append(
                    QuarantineItem(
                        source_path=obj.path,
                        reason_code=REASON_SUPERSEDED_BY_NEWER_VERSION,
                        detail=(
                            f"version {rec.get('version')} superseded by version "
                            f"{winner[1].get('version')} ({winner[0].path})"
                        ),
                        document_id=doc_id,
                    )
                )

    curated.sort(key=lambda c: c.source_path)
    quarantined.sort(key=lambda q: q.source_path)
    return CurationResult(curated=curated, quarantined=quarantined, examined=examined)


def _to_curated(obj: SourceObject, rec: dict[str, Any], doc_id: str) -> CuratedItem:
    corrections = list(rec.get("_corrections", []))
    clean = {k: v for k, v in rec.items() if k != "_corrections"}
    return CuratedItem(
        source_path=obj.path,
        document_id=doc_id,
        action=ACTION_CURATED_CORRECTED if corrections else ACTION_CURATED_UNCHANGED,
        content_hash=content_hash(str(clean.get("body", ""))),
        record=clean,
        corrections=corrections,
    )
