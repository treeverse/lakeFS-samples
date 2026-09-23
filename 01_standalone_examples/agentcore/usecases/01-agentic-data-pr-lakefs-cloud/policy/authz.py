"""Authorization model for the agent, and Cedar policy rendering.

``ALLOWED_TOOLS`` / ``DENIED_TOOLS`` are the source of truth. ``render_policy``
substitutes the concrete gateway ARN, target name, and agent principal into
``agent-policy.cedar`` -- the exact text deployed to AgentCore Policy. Tests
assert both the model (merge denied, writes scoped) and that the rendered Cedar
permits every allowed tool and forbids merge.
"""

from __future__ import annotations

from pathlib import Path

POLICY_DIR = Path(__file__).resolve().parent
POLICY_TEMPLATE = POLICY_DIR / "agent-policy.cedar"

# The ten capabilities the agent is permitted to invoke.
ALLOWED_TOOLS = (
    "get_demo_context",
    "list_demo_objects",
    "read_demo_object",
    "plan_curation",
    "write_workspace_object",
    "update_workspace_metadata",
    "commit_workspace",
    "diff_workspace",
    "validate_workspace",
    "create_data_pull_request",
    "get_data_pull_request",
)

# The capability that must always be denied to the autonomous agent identity.
DENIED_TOOLS = ("merge_data_pull_request",)


def decision(tool: str) -> str:
    """Model decision mirroring Cedar's default-deny + forbid-wins semantics."""
    if tool in DENIED_TOOLS:
        return "deny"
    if tool in ALLOWED_TOOLS:
        return "allow"
    return "deny"  # default-deny


def action_id(target: str, tool: str) -> str:
    """Cedar action id for a gateway tool: '<target>___<tool>'."""
    return f"{target}___{tool}"


def render_policy(*, agent_principal: str, gateway_arn: str, target: str) -> str:
    """Render the concrete Cedar policy text for deployment."""
    text = POLICY_TEMPLATE.read_text()
    return (
        text.replace("%%AGENT_PRINCIPAL%%", agent_principal)
        .replace("%%GATEWAY_ARN%%", gateway_arn)
        .replace("%%TARGET%%", target)
    )


def render_statements(*, agent_principal: str, gateway_arn: str, target: str) -> list[str]:
    """Return the individual Cedar statements (comments stripped).

    AgentCore's CreatePolicy accepts a single statement per policy, so the
    permit and forbid statements are deployed as separate policies.
    """
    text = render_policy(
        agent_principal=agent_principal, gateway_arn=gateway_arn, target=target
    )
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("//")]
    body = "\n".join(lines)
    return [s.strip() + ";" for s in body.split(";") if s.strip()]
