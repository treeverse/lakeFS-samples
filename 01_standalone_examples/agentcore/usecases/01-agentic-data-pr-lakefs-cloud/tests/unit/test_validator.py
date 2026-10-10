from common.provenance import build_provenance
from curation.rules import ACTION_CURATED_UNCHANGED, SourceObject, curate
from curation.validator import WrittenCurated, validate_workspace
from seed.generate import corpus_objects


def _corpus():
    return [
        SourceObject(path="corpus/" + d.relpath, content_type=d.content_type, content=d.content())
        for d in corpus_objects()
    ]


def _valid_outputs(corpus):
    res = curate(corpus)
    curated = []
    for c in res.curated:
        prov = build_provenance(
            source_repository="repo", source_branch="main", source_commit="c1",
            original_object_path=c.source_path, original_content_hash=c.content_hash,
            agentcore_session_id="s", agentcore_trace_id="t", action_taken=c.action,
            processing_timestamp="2026-07-15T00:00:00Z",
        )
        curated.append(WrittenCurated(path=f"out/{c.document_id}.json", document_id=c.document_id,
                                      action=c.action, record=c.record, provenance=prov))
    quarantine = [{"source_path": q.source_path, "reason_code": q.reason_code, "detail": q.detail} for q in res.quarantined]
    return curated, quarantine


def test_valid_workspace_passes():
    corpus = _corpus()
    curated, quarantine = _valid_outputs(corpus)
    result = validate_workspace(corpus, curated, quarantine)
    assert result.valid
    assert result.reconciliation["balanced"]


def test_missing_curated_object_fails_closed():
    corpus = _corpus()
    curated, quarantine = _valid_outputs(corpus)
    result = validate_workspace(corpus, curated[:-1], quarantine)
    assert not result.valid
    assert result.errors


def test_non_us_sneaked_into_curated_fails():
    corpus = _corpus()
    curated, quarantine = _valid_outputs(corpus)
    bad_prov = build_provenance(
        source_repository="repo", source_branch="main", source_commit="c1",
        original_object_path="corpus/records/region-eu.json", original_content_hash="h",
        agentcore_session_id="s", agentcore_trace_id="t", action_taken="curated_unchanged",
        processing_timestamp="2026-07-15T00:00:00Z",
    )
    curated.append(WrittenCurated(path="out/BAD.json", document_id="BAD", action=ACTION_CURATED_UNCHANGED,
        record={"title": "t", "product_id": "p", "region": "EU", "classification": "public",
                "status": "approved", "body": "x"}, provenance=bad_prov))
    result = validate_workspace(corpus, curated, quarantine)
    assert not result.valid


def test_incomplete_provenance_fails():
    corpus = _corpus()
    curated, quarantine = _valid_outputs(corpus)
    curated[0].provenance = {"source_repository": "repo"}  # incomplete
    result = validate_workspace(corpus, curated, quarantine)
    assert not result.valid


def test_unbalanced_counts_fail():
    corpus = _corpus()
    curated, quarantine = _valid_outputs(corpus)
    result = validate_workspace(corpus, curated, quarantine[:-1])
    assert not result.valid
