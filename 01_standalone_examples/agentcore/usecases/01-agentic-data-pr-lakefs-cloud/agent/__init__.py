"""The AgentCore-hosted curation agent.

``app`` is the AgentCore Runtime entrypoint. It builds a Strands agent whose only
tools are the curated capabilities served by the AgentCore Gateway (reached over
MCP with SigV4 auth). The agent never receives lakeFS credentials.
"""

from pathlib import Path

AGENT_DIR = Path(__file__).resolve().parent
SYSTEM_PROMPT_PATH = AGENT_DIR / "system_prompt.md"

DEFAULT_TASK = (
    "Prepare an approved US support corpus from the supplied data. Identify "
    "missing metadata, duplicates, conflicting or superseded versions, drafts, "
    "non-US documents, and synthetic PII. Create a curated corpus, quarantine "
    "anything that fails policy, produce a complete report, validate the result, "
    "and create a Data Pull Request. Do not modify or merge into the source branch."
)


def system_prompt() -> str:
    return SYSTEM_PROMPT_PATH.read_text()
