import json
from pathlib import Path

from curation import rules
from curation.rules import SourceObject, curate
from seed.generate import EXPECTED_PATH, corpus_objects, expected_results


def _corpus():
    return [
        SourceObject(path="corpus/" + d.relpath, content_type=d.content_type, content=d.content())
        for d in corpus_objects()
    ]


def test_curation_matches_independent_expectation():
    res = curate(_corpus())
    actual = {}
    for c in res.curated:
        actual[c.source_path.replace("corpus/", "")] = {"outcome": "curated", "action": c.action, "corrections": c.corrections}
    for q in res.quarantined:
        actual[q.source_path.replace("corpus/", "")] = {"outcome": "quarantined", "reason_code": q.reason_code}

    exp = expected_results()["objects"]
    for path, e in exp.items():
        a = actual[path]
        assert a["outcome"] == e["outcome"], (path, a, e)
        if e["outcome"] == "curated":
            assert a["action"] == e["action"], (path, a, e)
            if e.get("corrections") is not None:
                assert a["corrections"] == e["corrections"], (path, a, e)
        else:
            assert a["reason_code"] == e["reason_code"], (path, a, e)


def test_reconciliation_balances():
    res = curate(_corpus())
    r = res.reconciliation()
    assert r["balanced"]
    assert r["examined"] == len(_corpus())
    assert r["curated"] + r["quarantined"] == r["examined"]


def test_duplicate_resolution_keeps_first_path():
    res = curate(_corpus())
    dup_q = [q for q in res.quarantined if q.reason_code == rules.REASON_DUPLICATE_CONTENT]
    assert len(dup_q) == 1
    assert dup_q[0].source_path.endswith("dup-b.json")  # dup-a is path-first, curated


def test_conflicting_version_resolution_keeps_latest():
    res = curate(_corpus())
    curated_ids = {c.source_path for c in res.curated}
    assert any(p.endswith("conflict-v2.json") for p in curated_ids)
    superseded = [q for q in res.quarantined if q.reason_code == rules.REASON_SUPERSEDED_BY_NEWER_VERSION]
    assert superseded and superseded[0].source_path.endswith("conflict-v1.json")


def test_synthetic_pii_rejected():
    res = curate(_corpus())
    pii = [q for q in res.quarantined if q.reason_code == rules.REASON_SYNTHETIC_PII]
    assert pii and pii[0].source_path.endswith("pii-doc.json")
    # And no curated object contains a PII marker.
    for c in res.curated:
        assert not rules.contains_pii(str(c.record.get("body", "")))


def test_missing_fields_and_restricted_and_region():
    res = curate(_corpus())
    codes = {q.source_path.split("/")[-1]: q.reason_code for q in res.quarantined}
    assert codes["missing-classification.json"] == rules.REASON_MISSING_REQUIRED_FIELD
    assert codes["missing-title.json"] == rules.REASON_MISSING_REQUIRED_FIELD
    assert codes["restricted-doc.json"] == rules.REASON_RESTRICTED_CLASSIFICATION
    assert codes["region-eu.json"] == rules.REASON_NON_US_REGION
    assert codes["draft-us.json"] == rules.REASON_DRAFT_STATUS
    assert codes["superseded.json"] == rules.REASON_SUPERSEDED_STATUS
    assert codes["handoff.md"] == rules.REASON_UNRECOGNIZED_FORMAT


def test_safe_normalisation_marks_corrected():
    res = curate(_corpus())
    corrected = [c for c in res.curated if c.action == rules.ACTION_CURATED_CORRECTED]
    assert len(corrected) == 1
    c = corrected[0]
    assert c.source_path.endswith("needs-normalization.json")
    assert c.record["region"] == "US"  # normalised from "usa"
    assert c.record["title"] == "Enabling two-factor authentication"  # trimmed


def test_expected_results_file_is_in_sync():
    on_disk = json.loads(Path(EXPECTED_PATH).read_text())
    assert on_disk == expected_results()
