"""Run the curation stage and create the Data Pull Request.

Two paths:

* ``run_remote_agent`` invokes the deployed AgentCore Runtime agent, which does
  the reasoning and drives the curated Gateway tools. This is the real demo.
* ``run_local_curation`` performs the same curated operations deterministically,
  server-side, through the same ``Capabilities`` class -- no Bedrock, no AWS. It
  exists so the lakeFS Pull Request flow can be demonstrated and integration
  tested without deploying AgentCore, and so the outputs are byte-for-byte
  predictable. It is clearly not "the agent"; the agent is the remote path.
"""

from __future__ import annotations

import json

from common.runstate import RunState, utc_now_iso
from curation import reports
from curation.rules import curate
from gateway.handler import Capabilities, make_provenance

from .context import lakefs_client_from_env


def _build_capabilities(rs: RunState, session_id: str, trace_id: str) -> Capabilities:
    client, _ = lakefs_client_from_env()
    from agent import DEFAULT_TASK

    return Capabilities(
        client,
        run_id=rs.run_id,
        repository=rs.repository,
        source_branch=rs.source_branch,
        source_commit=rs.source_commit,
        baseline_commit=rs.baseline_commit,
        session_id=session_id,
        trace_id=trace_id,
        task=DEFAULT_TASK,
    )


def run_local_curation(rs: RunState) -> RunState:
    from agent import DEFAULT_TASK

    session_id = f"local-{rs.run_id}"
    trace_id = f"local-trace-{rs.run_id}"
    caps = _build_capabilities(rs, session_id, trace_id)

    corpus = caps._read_corpus()
    result = curate(corpus)

    # Write curated objects with complete provenance.
    curated_meta: list[dict] = []
    for item in result.curated:
        provenance = make_provenance(
            caps,
            source_path=item.source_path,
            content_hash=item.content_hash,
            action=item.action,
        )
        doc = {
            "document_id": item.document_id,
            "action": item.action,
            "corrections": item.corrections,
            "record": item.record,
            "provenance": provenance,
        }
        out = caps.write_workspace_object(
            {
                "path": f"curated/us/{item.document_id}.json",
                "content": json.dumps(doc, indent=2, sort_keys=True),
                "content_type": "application/json",
            }
        )
        curated_meta.append(
            {
                "document_id": item.document_id,
                "path": out["path"],
                "action": item.action,
                "corrections": item.corrections,
                "provenance": provenance,
            }
        )

    # Quarantine manifest.
    quarantine_items = [
        {
            "source_path": q.source_path,
            "reason_code": q.reason_code,
            "detail": q.detail,
            "document_id": q.document_id,
        }
        for q in result.quarantined
    ]
    caps.write_workspace_object(
        {
            "path": "quarantine/manifest.json",
            "content": json.dumps({"items": quarantine_items}, indent=2, sort_keys=True),
            "content_type": "application/json",
        }
    )

    # Reports.
    run_context = {
        "task": DEFAULT_TASK,
        "run_id": rs.run_id,
        "repository": rs.repository,
        "source_branch": rs.source_branch,
        "source_commit": rs.source_commit,
        "baseline_branch": rs.scope.baseline_branch,
        "baseline_commit": rs.baseline_commit,
        "workspace_branch": rs.scope.workspace_branch,
        "agentcore_session_id": session_id,
        "agentcore_trace_id": trace_id,
    }
    report = reports.build_curation_report(
        run_context=run_context,
        curated=curated_meta,
        quarantined=quarantine_items,
        reconciliation=result.reconciliation(),
    )
    caps.write_workspace_object(
        {
            "path": "reports/curation-report.json",
            "content": json.dumps(report, indent=2, sort_keys=True),
            "content_type": "application/json",
        }
    )
    caps.write_workspace_object(
        {
            "path": "reports/curation-report.md",
            "content": reports.render_markdown(report),
            "content_type": "text/markdown",
        }
    )

    caps.commit_workspace({"stage": "curation-outputs", "validation_status": "unknown"})

    # Authoritative, deterministic validation.
    validation = caps.validate_workspace({})
    if not validation["valid"]:
        raise RuntimeError(f"validation failed: {validation['errors'][:5]}")

    caps.write_workspace_object(
        {
            "path": "reports/validation-results.json",
            "content": json.dumps(validation, indent=2, sort_keys=True),
            "content_type": "application/json",
        }
    )
    commit = caps.commit_workspace(
        {"stage": "validation-and-report", "validation_status": "passed"}
    )
    # Use the id returned by the commit itself; re-reading the branch head would
    # be a second round-trip that could observe a later commit.
    rs.workspace_commit = commit["commit_id"]

    # Create the real lakeFS Cloud Pull Request (workspace -> baseline).
    pr_description = _pr_description(rs, run_context, report, validation)
    pr = caps.create_data_pull_request(
        {"title": f"Agentic data PR: curated US support corpus ({rs.run_id})",
         "description": pr_description}
    )

    rs.agentcore_session_id = session_id
    rs.agentcore_trace_id = trace_id
    rs.pull_request_id = str(pr["pull_request_id"])
    rs.pull_request_status = pr.get("status") or "open"
    rs.validation_status = "passed"
    rs.stage = "pull-request-created"
    rs.record_lakefs_resource(type="pull_request", id=rs.pull_request_id)
    rs.save()
    return rs


def _pr_description(rs: RunState, ctx: dict, report: dict, validation: dict) -> str:
    r = report["reconciliation"]
    return "\n".join(
        [
            f"**Task:** {ctx['task']}",
            "",
            f"- Source branch/commit: `{rs.source_branch}` @ `{rs.source_commit}`",
            f"- Baseline branch/commit: `{rs.scope.baseline_branch}` @ `{rs.baseline_commit}`",
            f"- Workspace branch/commit: `{rs.scope.workspace_branch}` @ `{rs.workspace_commit}`",
            f"- AgentCore session: `{ctx['agentcore_session_id']}`",
            f"- AgentCore trace: `{ctx['agentcore_trace_id']}`",
            "",
            f"- Objects examined: {r['examined']}",
            f"- Objects curated: {r['curated']}",
            f"- Objects corrected: {r['corrected']}",
            f"- Objects quarantined: {r['quarantined']}",
            f"- Validation: {'PASSED' if validation['valid'] else 'FAILED'}",
            "",
            f"Reproduce: `make seed && make run-local` for run `{rs.run_id}`.",
        ]
    )


def run_remote_agent(rs: RunState, prompt: str | None = None) -> dict:
    """Invoke the deployed AgentCore Runtime agent for this run."""
    if not rs.runtime_arn:
        raise RuntimeError(
            "No AgentCore Runtime is deployed for this run. Run `make deploy` "
            "first, or use `make run-local` to demonstrate the lakeFS flow "
            "without AgentCore."
        )
    from agent import DEFAULT_TASK

    from .context import aws_session

    session = aws_session()
    client = session.client("bedrock-agentcore")
    payload = json.dumps({"prompt": prompt or DEFAULT_TASK}).encode("utf-8")
    resp = client.invoke_agent_runtime(
        agentRuntimeArn=rs.runtime_arn,
        runtimeSessionId=rs.agentcore_session_id or f"session-{rs.run_id}",
        payload=payload,
    )
    body = resp.get("response")
    text = body.read().decode("utf-8") if hasattr(body, "read") else str(body)
    rs.stage = "agent-invoked"
    rs.updated_at = utc_now_iso()
    rs.save()
    return {"response": text}
