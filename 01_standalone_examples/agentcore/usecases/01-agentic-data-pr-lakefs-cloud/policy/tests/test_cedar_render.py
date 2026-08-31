"""Offline checks that the rendered Cedar policy matches the authorization model.

These do not replace the live denial (`make test-policy-denial`), which makes a
real Gateway call with the agent identity and captures AgentCore Policy's actual
refusal. They ensure the deployed policy text is internally consistent: it
permits exactly the allowed tools and forbids merge.
"""

from __future__ import annotations

from gateway.handler import CAPABILITIES
from policy import authz


def _rendered() -> str:
    return authz.render_policy(
        agent_principal="arn:aws:iam::123456789012:role/agentcore-data-pr-demo-agent",
        gateway_arn="arn:aws:bedrock-agentcore:us-west-2:123456789012:gateway/demo",
        target="lakefs-mcp-target",
    )


def test_all_placeholders_substituted():
    text = _rendered()
    assert "%%" not in text, "unsubstituted placeholder remains in rendered policy"


def test_permits_every_allowed_tool():
    text = _rendered()
    for tool in authz.ALLOWED_TOOLS:
        assert authz.action_id("lakefs-mcp-target", tool) in text


def test_forbids_merge():
    text = _rendered()
    assert "forbid(" in text
    assert authz.action_id("lakefs-mcp-target", "merge_data_pull_request") in text


def test_model_denies_merge_and_unknown():
    assert authz.decision("merge_data_pull_request") == "deny"
    assert authz.decision("delete_repository") == "deny"  # default-deny
    for tool in authz.ALLOWED_TOOLS:
        assert authz.decision(tool) == "allow"


def test_merge_not_in_allowed_set():
    assert "merge_data_pull_request" not in authz.ALLOWED_TOOLS


def test_model_covers_every_gateway_capability():
    # Every capability the Gateway advertises must be either allowed or denied
    # by the policy model -- no capability is left unclassified.
    modelled = set(authz.ALLOWED_TOOLS) | set(authz.DENIED_TOOLS)
    assert modelled == set(CAPABILITIES)
