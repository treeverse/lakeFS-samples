"""Opt-in LIVE integration tests against a real lakeFS Cloud installation.

Deselected by default (marker: ``live``). Enable with:

    RUN_LIVE_LAKEFS=1 make test-live

Requires LAKEFS_ENDPOINT / LAKEFS_ACCESS_KEY_ID / LAKEFS_SECRET_ACCESS_KEY and an
accessible repository. Every resource is created inside a unique run namespace
and cleaned up in a finally block. The source branch is never written.

Covers: authentication, branch creation, synthetic data seeding, object listing
and reading, workspace writes, commits, diff, Pull Request creation, human
approval (merge into the disposable baseline), and original-source integrity.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.live

LIVE = os.getenv("RUN_LIVE_LAKEFS") == "1"
skip_reason = "set RUN_LIVE_LAKEFS=1 and lakeFS credentials to run live lakeFS tests"


@pytest.mark.skipif(not LIVE, reason=skip_reason)
def test_live_end_to_end_lakefs_flow():
    from common.config import LakeFSConfig
    from orchestrator import approval
    from orchestrator.context import lakefs_client_from_env
    from orchestrator.curation_runner import run_local_curation
    from orchestrator.repository import select_repository
    from orchestrator.seeding import seed_run, verify_source_unchanged

    client, cfg = lakefs_client_from_env()
    cfg = LakeFSConfig.from_env()

    # auth + repository
    repo = select_repository(client, cfg.repository)
    source_before = client.branch_head(repo, cfg.source_branch)

    rs = None
    try:
        # seed (branch creation + data seeding + commit)
        rs = seed_run(client, repo, cfg.source_branch)
        assert rs.baseline_commit

        # list + read
        listing = client.list_objects(repo, rs.scope.baseline_branch, prefix=rs.scope.corpus_prefix, amount=200)
        assert listing["results"]

        # workspace writes + commit + diff + PR (deterministic runner)
        rs = run_local_curation(rs)
        assert rs.validation_status == "passed"
        assert rs.pull_request_id

        diff = client.diff_refs(repo, rs.scope.baseline_branch, rs.scope.workspace_branch)
        assert diff

        # human approval merges into the disposable baseline
        result = approval.approve(rs, rs.pull_request_id, confirm=lambda _c: True)
        assert result["merged"] and result["source_unchanged"]

        # original source branch integrity
        assert verify_source_unchanged(client, rs)
        assert client.branch_head(repo, cfg.source_branch) == source_before
    finally:
        if rs is not None:
            for branch in (rs.scope.workspace_branch, rs.scope.baseline_branch):
                try:
                    if client.branch_exists(repo, branch):
                        client.delete_branch(repo, branch)
                except Exception:
                    pass
