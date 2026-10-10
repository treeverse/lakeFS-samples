import pytest

from common.provenance import PROVENANCE_KEYS, build_provenance, is_provenance_complete


def _kwargs(**over):
    base = {
        "source_repository": "repo",
        "source_branch": "main",
        "source_commit": "c1",
        "original_object_path": "corpus/x.json",
        "original_content_hash": "sha256:abc",
        "agentcore_session_id": "sess",
        "agentcore_trace_id": "trace",
        "action_taken": "curated_unchanged",
        "processing_timestamp": "2026-07-15T00:00:00Z",
    }
    base.update(over)
    return base


def test_build_provenance_complete():
    prov = build_provenance(**_kwargs())
    assert set(prov) == set(PROVENANCE_KEYS)
    assert is_provenance_complete(prov)


def test_build_provenance_rejects_missing_value():
    with pytest.raises(ValueError):
        build_provenance(**_kwargs(source_commit=""))


def test_is_provenance_complete_false_cases():
    assert not is_provenance_complete(None)
    assert not is_provenance_complete({})
    assert not is_provenance_complete({"source_repository": "r"})
