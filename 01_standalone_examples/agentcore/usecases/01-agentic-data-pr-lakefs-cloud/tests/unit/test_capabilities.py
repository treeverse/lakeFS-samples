import json

import pytest

from gateway.handler import Capabilities
from gateway.validation import CapabilityError
from seed.generate import corpus_objects
from tests.conftest import FakeLakeFSClient


def _caps_with_corpus():
    client = FakeLakeFSClient()
    run_id = "abcd1234ef"
    caps = Capabilities(
        client, run_id=run_id, repository="demo-repo", source_branch="main",
        source_commit="src0", baseline_commit="base0", session_id="sess-1", trace_id="trace-1",
        task="curate",
    )
    for d in corpus_objects():
        client.upload_object("demo-repo", caps.scope.baseline_branch,
                             caps.scope.corpus_prefix + d.relpath, d.content(), d.content_type)
    return caps


def test_get_demo_context_has_no_credentials():
    caps = _caps_with_corpus()
    ctx = caps.get_demo_context({})
    text = json.dumps(ctx).lower()
    assert "secret" not in text and "authorization" not in text
    assert ctx["workspace_branch"] == caps.scope.workspace_branch


def test_write_scoped_to_workspace_output():
    caps = _caps_with_corpus()
    out = caps.write_workspace_object({"path": "curated/us/x.json", "content": "{}", "content_type": "application/json"})
    assert out["path"].startswith(caps.scope.output_prefix)
    assert out["branch"] == caps.scope.workspace_branch


def test_write_rejects_traversal_and_bad_content_type():
    caps = _caps_with_corpus()
    with pytest.raises(CapabilityError):
        caps.write_workspace_object({"path": "../escape", "content": "x", "content_type": "text/plain"})
    with pytest.raises(CapabilityError):
        caps.write_workspace_object({"path": "x.bin", "content": "x", "content_type": "application/octet-stream"})


def test_read_rejects_out_of_scope_branch():
    caps = _caps_with_corpus()
    with pytest.raises(CapabilityError):
        caps.read_demo_object({"branch": "main", "path": caps.scope.corpus_prefix + "records/clean-us-approved.json"})


def test_pr_targets_baseline_never_main():
    caps = _caps_with_corpus()
    pr = caps.create_data_pull_request({"title": "t"})
    assert pr["destination_branch"] == caps.scope.baseline_branch
    assert pr["destination_branch"] != "main"


def test_pr_destination_guard_rejects_main_source():
    caps = _caps_with_corpus()
    # Force a pathological config where baseline resolves to main; the guard must fire.
    caps.source_branch = caps.scope.baseline_branch
    with pytest.raises(CapabilityError):
        caps.create_data_pull_request({"title": "t"})


def test_merge_capability_forbidden_by_default():
    caps = _caps_with_corpus()
    with pytest.raises(CapabilityError) as e:
        caps.merge_data_pull_request({"pull_request_id": "pr-1"})
    assert e.value.code == "merge_forbidden"


def test_commit_metadata_carries_agentcore_identity():
    caps = _caps_with_corpus()
    out = caps.commit_workspace({"stage": "curation", "validation_status": "passed"})
    md = out["metadata"]
    assert md["agentcore_session_id"] == "sess-1"
    assert md["agentcore_trace_id"] == "trace-1"
    assert md["run_id"] == caps.scope.run_id
    assert md["stage"] == "curation"


def test_dispatch_unknown_tool():
    caps = _caps_with_corpus()
    with pytest.raises(CapabilityError):
        caps.dispatch("delete_repository", {})
