"""AgentCore Runtime entrypoint for the curation agent.

Deployed with the AgentCore CLI / starter toolkit:

    agentcore configure --entrypoint agent/app.py --name <prefix>-<run>-agent
    agentcore launch

At invocation the agent connects to its AgentCore Gateway over MCP (SigV4-signed,
since the Gateway uses AWS_IAM inbound auth), lists the curated tools, and hands
them to a Strands agent driven by the system prompt. The agent identity is the
runtime's IAM role -- the same principal AgentCore Policy evaluates, so the merge
tool is denied to it.

Heavy imports (strands, mcp, bedrock_agentcore) are optional extras and are done
lazily so the rest of the sample's unit tests do not require them.
"""

from __future__ import annotations

import os
from pathlib import Path

# Self-contained (no package-relative imports): AgentCore Runtime loads this file
# as a top-level entrypoint, where `from . import ...` would fail at startup.
_HERE = Path(__file__).resolve().parent

DEFAULT_TASK = (
    "Prepare an approved US support corpus from the supplied data. Identify "
    "missing metadata, duplicates, conflicting or superseded versions, drafts, "
    "non-US documents, and synthetic PII. Create a curated corpus, quarantine "
    "anything that fails policy, produce a complete report, validate the result, "
    "and create a Data Pull Request. Do not modify or merge into the source branch."
)


def system_prompt() -> str:
    return (_HERE / "system_prompt.md").read_text()


def _sigv4_httpx_auth(region: str, service: str = "bedrock-agentcore"):
    """Return an httpx.Auth that SigV4-signs requests with the caller's creds."""
    import boto3
    import httpx
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest

    credentials = boto3.Session().get_credentials()

    class _SigV4Auth(httpx.Auth):
        requires_request_body = True

        def auth_flow(self, request):  # type: ignore[override]
            aws_request = AWSRequest(
                method=request.method,
                url=str(request.url),
                data=request.content,
                headers=dict(request.headers),
            )
            SigV4Auth(credentials, service, region).add_auth(aws_request)
            request.headers.update(dict(aws_request.headers))
            yield request

    return _SigV4Auth()


def build_mcp_client(gateway_url: str, region: str):
    """Build an MCP client for the AgentCore Gateway (SigV4 streamable HTTP)."""
    from mcp.client.streamable_http import streamablehttp_client
    from strands.tools.mcp import MCPClient

    return MCPClient(
        lambda: streamablehttp_client(gateway_url, auth=_sigv4_httpx_auth(region))
    )


def run_curation(prompt: str | None = None) -> dict:
    """Run one curation session against the configured Gateway.

    Reads configuration from the environment injected by the deploy step:
    AGENTCORE_GATEWAY_URL, AWS_REGION, BEDROCK_MODEL_ID.
    """
    from strands import Agent
    from strands.models import BedrockModel

    gateway_url = os.environ["AGENTCORE_GATEWAY_URL"]
    region = os.environ.get("AWS_REGION", "us-west-2")
    model_id = os.environ["BEDROCK_MODEL_ID"]
    task = prompt or os.environ.get("AGENT_TASK") or DEFAULT_TASK
    # Claude handles streaming tool use; some Bedrock models (Nova, Llama) require
    # non-streaming for tool use. Set AGENT_STREAMING=false for those.
    streaming = os.environ.get("AGENT_STREAMING", "true").lower() != "false"

    mcp_client = build_mcp_client(gateway_url, region)
    with mcp_client:
        tools = mcp_client.list_tools_sync()
        agent = Agent(
            model=BedrockModel(model_id=model_id, region_name=region, streaming=streaming),
            system_prompt=system_prompt(),
            tools=tools,
        )
        result = agent(task)
        return {"result": str(result)}


# --- AgentCore Runtime app (optional import) --------------------------------
try:
    from bedrock_agentcore.runtime import BedrockAgentCoreApp

    app = BedrockAgentCoreApp()

    @app.entrypoint
    def invoke(payload: dict, context=None) -> dict:  # noqa: D401
        """AgentCore Runtime entrypoint."""
        return run_curation((payload or {}).get("prompt"))

except ImportError:  # pragma: no cover - only when the agent extra isn't installed
    app = None


if __name__ == "__main__":  # pragma: no cover
    if app is None:
        raise SystemExit(
            "bedrock-agentcore is not installed. Install the 'agent' extra:\n"
            "  pip install -e '.[agent]'"
        )
    app.run()
