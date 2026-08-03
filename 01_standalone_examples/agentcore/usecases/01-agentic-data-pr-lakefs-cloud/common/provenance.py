"""Provenance records attached to every curated object.

Each curated record must let a reviewer trace it back to the exact source object
and to the AgentCore execution that produced it. This is the single builder used
by the Lambda so every curated write is provenance-complete.
"""

from __future__ import annotations

from typing import Any

# The provenance keys required by the spec. Reports reconcile against this set.
PROVENANCE_KEYS = (
    "source_repository",
    "source_branch",
    "source_commit",
    "original_object_path",
    "original_content_hash",
    "agentcore_session_id",
    "agentcore_trace_id",
    "action_taken",
    "processing_timestamp",
)


def build_provenance(
    *,
    source_repository: str,
    source_branch: str,
    source_commit: str,
    original_object_path: str,
    original_content_hash: str,
    agentcore_session_id: str,
    agentcore_trace_id: str,
    action_taken: str,
    processing_timestamp: str,
) -> dict[str, Any]:
    """Return a complete provenance dict. Missing values raise, never silently blank."""
    prov = {
        "source_repository": source_repository,
        "source_branch": source_branch,
        "source_commit": source_commit,
        "original_object_path": original_object_path,
        "original_content_hash": original_content_hash,
        "agentcore_session_id": agentcore_session_id,
        "agentcore_trace_id": agentcore_trace_id,
        "action_taken": action_taken,
        "processing_timestamp": processing_timestamp,
    }
    missing = [k for k in PROVENANCE_KEYS if not prov.get(k)]
    if missing:
        raise ValueError(f"incomplete provenance, missing: {missing}")
    return prov


def is_provenance_complete(prov: dict[str, Any] | None) -> bool:
    return bool(prov) and all(prov.get(k) for k in PROVENANCE_KEYS)
