"""Deterministic report rendering.

``build_curation_report`` assembles the machine-readable report from the curation
decisions and run context; ``render_markdown`` renders the human-readable view.
The validation-results report is emitted verbatim from the server-side
``ValidationResult`` -- the agent never authors validation verdicts itself.
"""

from __future__ import annotations

from typing import Any


def build_curation_report(
    *,
    run_context: dict[str, Any],
    curated: list[dict[str, Any]],
    quarantined: list[dict[str, Any]],
    reconciliation: dict[str, Any],
) -> dict[str, Any]:
    """Machine-readable curation report (written to curation-report.json)."""
    reason_breakdown: dict[str, int] = {}
    for q in quarantined:
        code = q.get("reason_code", "unknown")
        reason_breakdown[code] = reason_breakdown.get(code, 0) + 1

    return {
        "task": run_context.get("task"),
        "run_id": run_context.get("run_id"),
        "repository": run_context.get("repository"),
        "source_branch": run_context.get("source_branch"),
        "source_commit": run_context.get("source_commit"),
        "baseline_branch": run_context.get("baseline_branch"),
        "baseline_commit": run_context.get("baseline_commit"),
        "workspace_branch": run_context.get("workspace_branch"),
        "agentcore_session_id": run_context.get("agentcore_session_id"),
        "agentcore_trace_id": run_context.get("agentcore_trace_id"),
        "reconciliation": reconciliation,
        "quarantine_reason_breakdown": reason_breakdown,
        "curated": [
            {
                "document_id": c.get("document_id"),
                "output_path": c.get("path"),
                "action": c.get("action"),
                "source_path": c.get("provenance", {}).get("original_object_path"),
                "corrections": c.get("corrections", []),
            }
            for c in curated
        ],
        "quarantined": [
            {
                "source_path": q.get("source_path"),
                "reason_code": q.get("reason_code"),
                "detail": q.get("detail"),
                "document_id": q.get("document_id"),
            }
            for q in quarantined
        ],
    }


def render_markdown(report: dict[str, Any]) -> str:
    r = report.get("reconciliation", {})
    lines = [
        "# Curation report",
        "",
        f"**Task:** {report.get('task')}",
        "",
        f"- Run id: `{report.get('run_id')}`",
        f"- Repository: `{report.get('repository')}`",
        f"- Source branch: `{report.get('source_branch')}` @ `{report.get('source_commit')}`",
        f"- Baseline branch: `{report.get('baseline_branch')}` @ `{report.get('baseline_commit')}`",
        f"- Workspace branch: `{report.get('workspace_branch')}`",
        f"- AgentCore session: `{report.get('agentcore_session_id')}`",
        f"- AgentCore trace: `{report.get('agentcore_trace_id')}`",
        "",
        "## Reconciliation",
        "",
        "| Examined | Curated | Corrected | Quarantined | Balanced |",
        "|---:|---:|---:|---:|:---:|",
        f"| {r.get('examined')} | {r.get('curated')} | {r.get('corrected')} "
        f"| {r.get('quarantined')} | {'yes' if r.get('balanced') else 'NO'} |",
        "",
        "## Curated (US)",
        "",
        "| Document | Action | Source | Corrections |",
        "|---|---|---|---|",
    ]
    for c in report.get("curated", []):
        corr = "; ".join(c.get("corrections", [])) or "-"
        lines.append(
            f"| `{c.get('document_id')}` | {c.get('action')} "
            f"| `{c.get('source_path')}` | {corr} |"
        )
    lines += [
        "",
        "## Quarantined",
        "",
        "| Source | Reason | Detail |",
        "|---|---|---|",
    ]
    for q in report.get("quarantined", []):
        lines.append(
            f"| `{q.get('source_path')}` | `{q.get('reason_code')}` | {q.get('detail')} |"
        )
    lines.append("")
    return "\n".join(lines)
