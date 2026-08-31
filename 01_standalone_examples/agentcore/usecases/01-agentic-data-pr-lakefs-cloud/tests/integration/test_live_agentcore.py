"""Opt-in LIVE integration tests against deployed AgentCore resources.

Deselected by default (marker: ``live``). These require a completed `make deploy`
for the current run (a Gateway with the Cedar policy attached, and optionally the
Runtime agent). Enable with:

    RUN_LIVE_AGENTCORE=1 make test-live

Covers: AgentCore Gateway invocation and the AgentCore Policy merge denial.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.live

LIVE = os.getenv("RUN_LIVE_AGENTCORE") == "1"
skip_reason = "set RUN_LIVE_AGENTCORE=1 and `make deploy` first to run live AgentCore tests"


def _current_run():
    from common.runstate import RunState

    return RunState.load_current()


@pytest.mark.skipif(not LIVE, reason=skip_reason)
def test_gateway_lists_curated_tools():
    from agent.app import build_mcp_client

    rs = _current_run()
    if not rs.gateway_url:
        pytest.skip("no deployed Gateway for the current run")
    region = os.environ.get("AWS_REGION", "us-west-2")
    client = build_mcp_client(rs.gateway_url, region)
    with client:
        tools = {t.tool_name for t in client.list_tools_sync()}
    # The curated capabilities are exposed (merge may be hidden by policy).
    assert "list_demo_objects" in tools
    assert "create_data_pull_request" in tools


@pytest.mark.skipif(not LIVE, reason=skip_reason)
def test_policy_denies_merge():
    from agent.app import build_mcp_client
    from scripts.policy_denial import _looks_like_denial

    rs = _current_run()
    if not rs.gateway_url:
        pytest.skip("no deployed Gateway for the current run")
    region = os.environ.get("AWS_REGION", "us-west-2")
    client = build_mcp_client(rs.gateway_url, region)
    try:
        with client:
            result = client.call_tool_sync(
                tool_use_id="live-denial",
                name="merge_data_pull_request",
                arguments={"pull_request_id": rs.pull_request_id or "pr-x"},
            )
            text = str(result)
    except Exception as exc:  # a raised authorization error is a valid denial
        text = f"{exc.__class__.__name__}: {exc}"
    assert _looks_like_denial(text), f"merge was not denied by policy: {text}"
