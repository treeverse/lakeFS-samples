"""`make test-policy-denial` -- prove AgentCore Policy denies the merge tool.

Uses the SAME identity as the agent (the runtime IAM role / the configured AWS
credentials) to call ``merge_data_pull_request`` directly through the AgentCore
Gateway, and captures the REAL policy denial. The denial is never faked in the
prompt, the Lambda, or the UI.

If the forbidden capability is omitted from ``tools/list`` by policy, this makes a
direct Gateway tool call anyway and verifies the denial.
"""

from __future__ import annotations

from common.runstate import RunState

# Substrings that indicate a genuine authorization denial from AgentCore Policy.
# Observed live forms: "Tool Execution Denied: ... policy enforcement [Policy
# evaluation denied due to <policy-id>]" and, when the forbidden tool is hidden
# from tools/list, "Unknown tool: merge_data_pull_request".
_DENIAL_MARKERS = (
    "access denied",
    "accessdenied",
    "not authorized",
    "not allowed",
    "unauthorized",
    "forbidden",
    "denied",
    "policy enforcement",
    "policy evaluation denied",
    "denied by policy",
    "policy",
    "cedar",
    "unknown tool",
)


def _looks_like_denial(text: str) -> bool:
    low = text.lower()
    return any(marker in low for marker in _DENIAL_MARKERS)


def main() -> int:
    rs = RunState.load_current()
    gateway_url = rs.gateway_url
    if not gateway_url:
        print(
            "ERROR: no Gateway URL for the current run. Run `make deploy` first.\n"
            "This test requires a live AgentCore Gateway with the Cedar policy "
            "attached."
        )
        return 2

    import os

    from agent.app import build_mcp_client

    region = os.environ.get("AWS_REGION", "us-west-2")
    pr_id = rs.pull_request_id or "pr-does-not-need-to-exist"

    print("Attempting merge_data_pull_request through the AgentCore Gateway")
    print("using the agent identity. Expecting a policy DENIAL...\n")

    mcp_client = build_mcp_client(gateway_url, region)
    try:
        with mcp_client:
            tools = [t.tool_name for t in mcp_client.list_tools_sync()]
            print(f"Tools visible to the agent identity: {tools}")
            if "merge_data_pull_request" not in tools:
                print(
                    "merge_data_pull_request is not even listed for this identity "
                    "(policy hides it). Making a direct call to confirm denial..."
                )
            result = mcp_client.call_tool_sync(
                tool_use_id="policy-denial-probe",
                name="merge_data_pull_request",
                arguments={"pull_request_id": pr_id},
            )
            text = str(result)
    except Exception as exc:  # a raised authorization error is also a valid denial
        text = f"{exc.__class__.__name__}: {exc}"

    print(f"\nGateway response:\n{text}\n")
    if _looks_like_denial(text):
        print("PASS: AgentCore Policy denied merge_data_pull_request for the agent.")
        return 0
    print(
        "FAIL: the merge call was NOT denied. Check the Cedar policy is attached "
        "and that inbound auth uses the agent identity."
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
