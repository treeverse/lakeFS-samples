"""Local human approval / rejection of the Data Pull Request.

These call lakeFS Cloud DIRECTLY with the local endpoint and keys -- NOT through
the AgentCore Gateway. This is the human action AgentCore Policy reserves for a
person: the autonomous agent can never reach ``merge_pull_request``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from common.runstate import RunState
from gateway.handler import Capabilities

from .context import lakefs_client_from_env


class ApprovalError(RuntimeError):
    pass


def _read_only_caps(rs: RunState, client) -> Capabilities:
    return Capabilities(
        client,
        run_id=rs.run_id,
        repository=rs.repository,
        source_branch=rs.source_branch,
        source_commit=rs.source_commit,
        baseline_commit=rs.baseline_commit,
        session_id="human-approval",
        trace_id="human-approval",
    )


def preflight(rs: RunState, pr_id: str) -> dict[str, Any]:
    """Run every safety check short of the merge itself. Raises on any failure."""
    client, _ = lakefs_client_from_env()
    scope = rs.scope

    # Ownership: the PR must be the one this run created.
    if rs.pull_request_id and str(rs.pull_request_id) != str(pr_id):
        raise ApprovalError(
            f"PR {pr_id} is not the PR created by run {rs.run_id} "
            f"(expected {rs.pull_request_id})"
        )

    pr = client.get_pull_request(rs.repository, pr_id)
    if pr.get("source_branch") not in (None, scope.workspace_branch):
        raise ApprovalError(
            f"PR source {pr.get('source_branch')!r} is not the expected workspace "
            f"{scope.workspace_branch!r}"
        )
    if pr.get("destination_branch") not in (None, scope.baseline_branch):
        raise ApprovalError(
            f"PR destination {pr.get('destination_branch')!r} is not the expected "
            f"baseline {scope.baseline_branch!r}"
        )

    # Race detection: workspace HEAD must match the validated commit.
    workspace_head = client.branch_head(rs.repository, scope.workspace_branch)
    if rs.workspace_commit and workspace_head != rs.workspace_commit:
        raise ApprovalError(
            "workspace branch changed since validation "
            f"(validated {rs.workspace_commit}, now {workspace_head}). Re-run the demo."
        )

    # Re-run deterministic validation.
    caps = _read_only_caps(rs, client)
    validation = caps.validate_workspace({})
    if not validation["valid"]:
        raise ApprovalError(f"validation no longer passes: {validation['errors'][:5]}")

    diff = caps.diff_workspace({})
    return {
        "pull_request": pr,
        "validation": validation,
        "diff": diff,
        "workspace_head": workspace_head,
        "web_url": client.pull_request_web_url(rs.repository, pr_id),
    }


def approve(
    rs: RunState,
    pr_id: str,
    confirm: Callable[[dict[str, Any]], bool] = lambda _ctx: True,
) -> dict[str, Any]:
    client, _ = lakefs_client_from_env()
    ctx = preflight(rs, pr_id)

    if not confirm(ctx):
        raise ApprovalError("approval cancelled by human reviewer")

    baseline_before = client.branch_head(rs.repository, rs.scope.baseline_branch)
    source_before = client.branch_head(rs.repository, rs.source_branch)

    merge_result = client.merge_pull_request(rs.repository, pr_id)

    baseline_after = client.branch_head(rs.repository, rs.scope.baseline_branch)
    source_after = client.branch_head(rs.repository, rs.source_branch)

    if baseline_after == baseline_before:
        raise ApprovalError("merge reported success but baseline HEAD did not change")
    if source_after != source_before:
        raise ApprovalError(
            "SOURCE BRANCH CHANGED during merge -- this must never happen "
            f"({source_before} -> {source_after})"
        )

    rs.pull_request_status = "merged"
    rs.baseline_commit = baseline_after
    rs.stage = "merged"
    rs.save()
    return {
        "merged": True,
        "baseline_before": baseline_before,
        "baseline_after": baseline_after,
        "source_unchanged": source_after == source_before,
        "source_commit": source_after,
        "result": merge_result,
    }


def reject(rs: RunState, pr_id: str) -> dict[str, Any]:
    client, _ = lakefs_client_from_env()
    if rs.pull_request_id and str(rs.pull_request_id) != str(pr_id):
        raise ApprovalError(f"PR {pr_id} does not belong to run {rs.run_id}")
    client.update_pull_request(rs.repository, pr_id, status="closed")
    rs.pull_request_status = "closed"
    rs.stage = "pr-rejected"
    rs.save()
    return {"closed": True, "pull_request_id": pr_id}
