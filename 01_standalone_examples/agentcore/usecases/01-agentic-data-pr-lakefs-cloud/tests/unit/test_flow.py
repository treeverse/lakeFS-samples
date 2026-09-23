"""End-to-end flow on the fake client: seed -> curate -> PR -> approve.

Also covers approval race detection, PR ownership, and cleanup resource scoping.
"""

import pytest

from orchestrator import approval
from orchestrator.curation_runner import run_local_curation
from orchestrator.seeding import seed_run, verify_source_unchanged
from tests.conftest import patch_client


@pytest.fixture
def ran(fake_client, tmp_generated, monkeypatch):
    patch_client(monkeypatch, ["orchestrator.curation_runner", "orchestrator.approval"], fake_client)
    rs = seed_run(fake_client, "demo-repo", "main")
    rs = run_local_curation(rs)
    return rs, fake_client


def test_run_local_curation_produces_valid_pr(ran):
    rs, client = ran
    assert rs.validation_status == "passed"
    assert rs.pull_request_id
    assert rs.workspace_commit
    pr = client.get_pull_request("demo-repo", rs.pull_request_id)
    assert pr["destination_branch"] == rs.scope.baseline_branch
    assert pr["destination_branch"] != "main"


def test_approval_merges_and_preserves_source(ran):
    rs, client = ran
    source_before = client.branch_head("demo-repo", "main")
    result = approval.approve(rs, rs.pull_request_id, confirm=lambda _c: True)
    assert result["merged"]
    assert result["source_unchanged"]
    assert client.branch_head("demo-repo", "main") == source_before
    assert verify_source_unchanged(client, rs)


def test_approval_race_detection(ran):
    rs, client = ran
    # Simulate the workspace advancing after validation.
    client.commit("demo-repo", rs.scope.workspace_branch, "sneaky change")
    with pytest.raises(approval.ApprovalError) as e:
        approval.preflight(rs, rs.pull_request_id)
    assert "changed since validation" in str(e.value)


def test_approval_rejects_wrong_pr_id(ran):
    rs, _ = ran
    with pytest.raises(approval.ApprovalError):
        approval.preflight(rs, "pr-does-not-belong")


def test_reject_closes_without_merging(ran):
    rs, client = ran
    baseline_before = client.branch_head("demo-repo", rs.scope.baseline_branch)
    approval.reject(rs, rs.pull_request_id)
    assert client.get_pull_request("demo-repo", rs.pull_request_id)["status"] == "closed"
    assert client.branch_head("demo-repo", rs.scope.baseline_branch) == baseline_before


def test_cleanup_scoping_only_touches_run_branches(ran, monkeypatch):
    rs, client = ran
    import scripts.cleanup as cleanup

    monkeypatch.setattr(cleanup, "lakefs_client_from_env", lambda: (client, None))
    failures: list[str] = []
    cleanup._cleanup_lakefs(rs, failures)

    assert set(client.deleted_branches) == {rs.scope.workspace_branch, rs.scope.baseline_branch}
    assert "main" not in client.deleted_branches
    assert client.branch_exists("demo-repo", "main")
    assert not failures
