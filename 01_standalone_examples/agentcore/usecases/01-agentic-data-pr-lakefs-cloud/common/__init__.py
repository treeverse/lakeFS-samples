"""Shared foundations for the Agentic Data PR sample.

Everything the CLI, the Streamlit UI, the Gateway Lambda, and the tests need in
common lives here: configuration loading, deterministic run-scope naming,
run-state persistence, provenance construction, and secret redaction.

The naming module is the single source of truth for which repository, branches,
and prefixes a run is allowed to touch. Both the local orchestrator and the
server-side Lambda derive their authorization scope from it -- never from
model-supplied arguments.
"""
