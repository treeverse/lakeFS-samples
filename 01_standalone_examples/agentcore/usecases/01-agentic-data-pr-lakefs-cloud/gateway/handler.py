"""AWS Lambda handler behind the AgentCore Gateway: the curated lakeFS toolset.

The Gateway exposes each function below as an MCP tool. The model never receives
lakeFS credentials -- the Lambda loads them from AWS Secrets Manager server-side.
Every capability derives its permitted repository, branches, and prefixes from
the server-controlled run scope (environment) and validates all input before it
touches lakeFS.

The business logic lives in the ``Capabilities`` class, which takes an injected
``LakeFSClient`` and run scope so it can be unit tested with no AWS or network.
``lambda_handler`` wires the AWS specifics (Secrets Manager, environment, the
Gateway event shape) around it.
"""

from __future__ import annotations

import json
import os
from typing import Any

from common.naming import RunScope
from common.provenance import build_provenance
from common.redaction import redact
from common.runstate import utc_now_iso
from curation import reports, rules
from curation.rules import curate
from curation.validator import WrittenCurated, validate_workspace

from .lakefs_client import LakeFSClient, LakeFSError
from .validation import (
    CapabilityError,
    clamp_amount,
    ensure_content_type,
    ensure_list_prefix_allowed,
    ensure_read_allowed,
    ensure_size,
    ensure_write_allowed,
)

# Capabilities the Gateway advertises. merge_data_pull_request is advertised so
# AgentCore Policy (Cedar) can demonstrably forbid it for the agent identity.
CAPABILITIES = (
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
    "merge_data_pull_request",
)


class Capabilities:
    def __init__(
        self,
        client: LakeFSClient,
        *,
        run_id: str,
        repository: str,
        source_branch: str,
        source_commit: str | None,
        baseline_commit: str | None,
        session_id: str,
        trace_id: str,
        task: str = "",
    ) -> None:
        self.client = client
        self.repository = repository
        self.source_branch = source_branch
        self.source_commit = source_commit
        self.baseline_commit = baseline_commit
        self.session_id = session_id
        self.trace_id = trace_id
        self.task = task
        self.scope = RunScope(run_id=run_id, source_branch=source_branch)

    # --- dispatch --------------------------------------------------------
    def dispatch(self, tool: str, args: dict[str, Any]) -> dict[str, Any]:
        if tool not in CAPABILITIES:
            raise CapabilityError("unknown_tool", f"unknown capability {tool!r}")
        handler = getattr(self, tool)
        return handler(args or {})

    # --- 1. context ------------------------------------------------------
    def get_demo_context(self, args: dict[str, Any]) -> dict[str, Any]:
        s = self.scope
        return {
            "run_id": s.run_id,
            "repository": self.repository,
            "source_branch": self.source_branch,
            "source_commit": self.source_commit,
            "baseline_branch": s.baseline_branch,
            "baseline_commit": self.baseline_commit,
            "workspace_branch": s.workspace_branch,
            "allowed_input_prefix": s.corpus_prefix,
            "allowed_output_prefix": s.output_prefix,
            "agentcore_session_id": self.session_id,
            "agentcore_trace_id": self.trace_id,
        }

    # --- 2. list ---------------------------------------------------------
    def list_demo_objects(self, args: dict[str, Any]) -> dict[str, Any]:
        s = self.scope
        branch = s.baseline_branch
        prefix = s.corpus_prefix
        ensure_list_prefix_allowed(s, branch, prefix)
        amount = clamp_amount(args.get("amount"))
        after = str(args.get("after") or "")
        page = self.client.list_objects(
            self.repository, branch, prefix=prefix, after=after, amount=amount
        )
        objects = [
            {
                "path": r.get("path"),
                "size": r.get("size_bytes"),
                "content_hash": r.get("checksum"),
                "content_type": r.get("content_type"),
                "metadata": r.get("metadata") or {},
            }
            for r in page.get("results", [])
            if r.get("path_type", "object") == "object"
        ]
        pag = page.get("pagination", {})
        return {
            "branch": branch,
            "prefix": prefix,
            "objects": objects,
            "pagination": {
                "has_more": bool(pag.get("has_more")),
                "next_offset": pag.get("next_offset", ""),
            },
        }

    # --- 3. read ---------------------------------------------------------
    def read_demo_object(self, args: dict[str, Any]) -> dict[str, Any]:
        s = self.scope
        branch = str(args.get("branch") or s.baseline_branch)
        path = str(args.get("path") or "")
        ensure_read_allowed(s, branch, path)
        stat = self.client.stat_object(self.repository, branch, path)
        ensure_content_type(stat.content_type)
        raw = self.client.get_object(self.repository, branch, path)
        ensure_size(raw)
        return {
            "branch": branch,
            "path": path,
            "content": raw.decode("utf-8"),
            "content_type": stat.content_type,
            "size": stat.size_bytes,
            "content_hash": stat.checksum,
        }

    # --- 4. write --------------------------------------------------------
    def _resolve_output_path(self, path: str) -> str:
        s = self.scope
        if path.startswith(s.output_prefix):
            return path
        if path.startswith(s.prefix):  # inside run namespace but wrong subtree
            return path  # ensure_write_allowed will reject it
        return s.output_prefix + path.lstrip("/")

    def write_workspace_object(self, args: dict[str, Any]) -> dict[str, Any]:
        s = self.scope
        branch = s.workspace_branch
        path = self._resolve_output_path(str(args.get("path") or ""))
        content = args.get("content")
        if not isinstance(content, str):
            raise CapabilityError("invalid_content", "content must be a string")
        content_type = ensure_content_type(args.get("content_type") or "application/json")
        ensure_write_allowed(s, branch, path)
        ensure_size(content)
        stat = self.client.upload_object(
            self.repository, branch, path, content, content_type
        )
        return {
            "branch": branch,
            "path": stat.path,
            "size": stat.size_bytes,
            "content_hash": stat.checksum,
        }

    # --- 5. metadata -----------------------------------------------------
    def update_workspace_metadata(self, args: dict[str, Any]) -> dict[str, Any]:
        s = self.scope
        branch = s.workspace_branch
        path = self._resolve_output_path(str(args.get("path") or ""))
        metadata = args.get("metadata")
        if not isinstance(metadata, dict):
            raise CapabilityError("invalid_metadata", "metadata must be an object")
        ensure_write_allowed(s, branch, path)
        raw = self.client.get_object(self.repository, branch, path)
        try:
            doc = json.loads(raw.decode("utf-8"))
        except ValueError:
            raise CapabilityError(
                "not_json", "metadata can only be updated on JSON objects"
            ) from None
        if not isinstance(doc, dict):
            raise CapabilityError("not_json_object", "object is not a JSON object")
        doc.setdefault("metadata", {})
        if isinstance(doc["metadata"], dict):
            doc["metadata"].update({str(k): v for k, v in metadata.items()})
        else:
            doc["metadata"] = dict(metadata)
        new_content = json.dumps(doc, indent=2, sort_keys=True)
        ensure_size(new_content)
        stat = self.client.upload_object(
            self.repository, branch, path, new_content, "application/json"
        )
        return {"branch": branch, "path": stat.path, "content_hash": stat.checksum}

    # --- 6. commit -------------------------------------------------------
    def commit_workspace(self, args: dict[str, Any]) -> dict[str, Any]:
        s = self.scope
        branch = s.workspace_branch
        stage = str(args.get("stage") or "curation")
        validation_status = str(args.get("validation_status") or "unknown")
        message = str(args.get("message") or f"agentcore: {stage}")
        metadata = {
            "agentcore_session_id": self.session_id,
            "agentcore_trace_id": self.trace_id,
            "run_id": s.run_id,
            "task": self.task or str(args.get("task") or ""),
            "stage": stage,
            "validation_status": validation_status,
            "timestamp": utc_now_iso(),
        }
        commit = self.client.commit(self.repository, branch, message, metadata)
        return {
            "branch": branch,
            "commit_id": commit.get("id"),
            "message": message,
            "metadata": metadata,
        }

    # --- 7. diff ---------------------------------------------------------
    def diff_workspace(self, args: dict[str, Any]) -> dict[str, Any]:
        s = self.scope
        changes = self.client.diff_refs(
            self.repository, s.baseline_branch, s.workspace_branch
        )
        summary = {"added": 0, "removed": 0, "changed": 0}
        for c in changes:
            t = c.get("type", "changed")
            summary[t] = summary.get(t, 0) + 1
        return {
            "from": s.baseline_branch,
            "to": s.workspace_branch,
            "summary": summary,
            "changes": [
                {"type": c.get("type"), "path": c.get("path")} for c in changes
            ],
        }

    # --- 8. validate (authoritative, deterministic, fail-closed) --------
    def _read_corpus(self) -> list[rules.SourceObject]:
        s = self.scope
        objs: list[rules.SourceObject] = []
        after = ""
        while True:
            page = self.client.list_objects(
                self.repository,
                s.baseline_branch,
                prefix=s.corpus_prefix,
                after=after,
                amount=200,
            )
            for r in page.get("results", []):
                if r.get("path_type", "object") != "object":
                    continue
                path = r["path"]
                content = self.client.get_object(
                    self.repository, s.baseline_branch, path
                ).decode("utf-8")
                objs.append(
                    rules.SourceObject(
                        path=path,
                        content_type=r.get("content_type") or "application/json",
                        content=content,
                    )
                )
            pag = page.get("pagination", {})
            if not pag.get("has_more"):
                break
            after = pag.get("next_offset", "")
        return objs

    def _read_workspace_outputs(
        self,
    ) -> tuple[list[WrittenCurated], list[dict[str, Any]]]:
        s = self.scope
        curated: list[WrittenCurated] = []
        after = ""
        while True:
            try:
                page = self.client.list_objects(
                    self.repository,
                    s.workspace_branch,
                    prefix=s.curated_us_prefix,
                    after=after,
                    amount=200,
                )
            except LakeFSError:
                page = {"results": [], "pagination": {}}
            for r in page.get("results", []):
                if r.get("path_type", "object") != "object":
                    continue
                raw = self.client.get_object(
                    self.repository, s.workspace_branch, r["path"]
                ).decode("utf-8")
                try:
                    doc = json.loads(raw)
                except ValueError:
                    continue
                curated.append(
                    WrittenCurated(
                        path=r["path"],
                        document_id=str(doc.get("document_id", "")),
                        action=str(doc.get("action", "")),
                        record=doc.get("record", {}),
                        provenance=doc.get("provenance", {}),
                    )
                )
            pag = page.get("pagination", {})
            if not pag.get("has_more"):
                break
            after = pag.get("next_offset", "")

        quarantine: list[dict[str, Any]] = []
        try:
            manifest_raw = self.client.get_object(
                self.repository, s.workspace_branch, s.quarantine_manifest_path
            ).decode("utf-8")
            manifest = json.loads(manifest_raw)
            quarantine = manifest.get("items", []) if isinstance(manifest, dict) else []
        except (LakeFSError, ValueError):
            quarantine = []
        return curated, quarantine

    def validate_workspace(self, args: dict[str, Any]) -> dict[str, Any]:
        corpus = self._read_corpus()
        curated, quarantine = self._read_workspace_outputs()
        result = validate_workspace(corpus, curated, quarantine)
        return result.to_dict()

    # --- 8b. plan (deterministic assist so any model can produce valid output) -
    def plan_curation(self, args: dict[str, Any]) -> dict[str, Any]:
        """Return the exact, provenance-complete objects the agent should write.

        The deterministic curation core computes the correct outcome and this
        returns ready-to-write file contents (paths + content). The agent stays
        in control -- it orchestrates the writes, commits, validation, and PR --
        but does not need to get the exact JSON/provenance right itself, so even
        a smaller Bedrock model produces a validation-passing result.
        """
        s = self.scope
        result = curate(self._read_corpus())
        writes: list[dict[str, Any]] = []
        curated_meta: list[dict[str, Any]] = []
        for item in result.curated:
            provenance = make_provenance(
                self,
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
            path = f"{s.curated_us_prefix}{item.document_id}.json"
            writes.append(
                {"path": path, "content_type": "application/json",
                 "content": json.dumps(doc, indent=2, sort_keys=True)}
            )
            curated_meta.append(
                {"document_id": item.document_id, "path": path, "action": item.action,
                 "corrections": item.corrections, "provenance": provenance}
            )
        quarantine_items = [
            {"source_path": q.source_path, "reason_code": q.reason_code,
             "detail": q.detail, "document_id": q.document_id}
            for q in result.quarantined
        ]
        writes.append(
            {"path": s.quarantine_manifest_path, "content_type": "application/json",
             "content": json.dumps({"items": quarantine_items}, indent=2, sort_keys=True)}
        )
        run_context = {
            "task": self.task, "run_id": s.run_id, "repository": self.repository,
            "source_branch": self.source_branch, "source_commit": self.source_commit,
            "baseline_branch": s.baseline_branch, "baseline_commit": self.baseline_commit,
            "workspace_branch": s.workspace_branch,
            "agentcore_session_id": self.session_id, "agentcore_trace_id": self.trace_id,
        }
        report = reports.build_curation_report(
            run_context=run_context, curated=curated_meta,
            quarantined=quarantine_items, reconciliation=result.reconciliation(),
        )
        writes.append(
            {"path": s.report_json_path, "content_type": "application/json",
             "content": json.dumps(report, indent=2, sort_keys=True)}
        )
        writes.append(
            {"path": s.report_md_path, "content_type": "text/markdown",
             "content": reports.render_markdown(report)}
        )
        return {
            "writes": writes,
            "reconciliation": result.reconciliation(),
            "next_steps": (
                "Write each item in `writes` verbatim with write_workspace_object "
                "(pass the given path, content, and content_type). Then "
                "commit_workspace(stage='curation-outputs'); validate_workspace; "
                "write the returned validation result to "
                f"'{s.validation_path}'; commit_workspace(stage='validation-and-report', "
                "validation_status='passed'); create_data_pull_request; "
                "get_data_pull_request; then stop. Do not attempt to merge."
            ),
        }

    # --- 9. create PR ----------------------------------------------------
    def create_data_pull_request(self, args: dict[str, Any]) -> dict[str, Any]:
        s = self.scope
        source = s.workspace_branch
        dest = s.baseline_branch
        # Belt-and-braces: never target the source branch or main.
        if dest in {"main", self.source_branch}:
            raise CapabilityError(
                "invalid_pr_destination",
                f"refusing to create a PR targeting {dest!r}",
            )
        title = str(args.get("title") or f"Agentic data PR ({s.run_id})")
        description = str(args.get("description") or "")
        pr = self.client.create_pull_request(
            self.repository, title, description, source, dest
        )
        return {
            "pull_request_id": pr.get("id"),
            "source_branch": source,
            "destination_branch": dest,
            "status": pr.get("status"),
            "web_url": self.client.pull_request_web_url(
                self.repository, pr.get("id")
            ),
        }

    # --- 10. get PR ------------------------------------------------------
    def get_data_pull_request(self, args: dict[str, Any]) -> dict[str, Any]:
        s = self.scope
        pull_id = str(args.get("pull_request_id") or "")
        if not pull_id:
            raise CapabilityError("missing_pr_id", "pull_request_id is required")
        pr = self.client.get_pull_request(self.repository, pull_id)
        diff = self.diff_workspace({})
        validation = None
        try:
            v_raw = self.client.get_object(
                self.repository, s.workspace_branch, s.validation_path
            ).decode("utf-8")
            validation = json.loads(v_raw)
        except (LakeFSError, ValueError):
            validation = None
        return {
            "pull_request_id": pr.get("id"),
            "title": pr.get("title"),
            "status": pr.get("status"),
            "source_branch": pr.get("source_branch"),
            "destination_branch": pr.get("destination_branch"),
            "diff_summary": diff["summary"],
            "validation": {"valid": validation.get("valid")} if validation else None,
            "web_url": self.client.pull_request_web_url(self.repository, pull_id),
        }

    # --- 11. merge (Cedar-forbidden for the agent; defence in depth here) -
    def merge_data_pull_request(self, args: dict[str, Any]) -> dict[str, Any]:
        # The demonstrated denial is AgentCore Policy (Cedar) at the Gateway:
        # this code is never reached by the agent identity. This guard is pure
        # defence-in-depth so a misconfigured policy still cannot auto-merge.
        if os.environ.get("AGENTCORE_ALLOW_GATEWAY_MERGE") != "1":
            raise CapabilityError(
                "merge_forbidden",
                "merging is not permitted through the agent Gateway. A human "
                "approves and merges via the local approval tool.",
            )
        pull_id = str(args.get("pull_request_id") or "")
        merged = self.client.merge_pull_request(self.repository, pull_id)
        return {"pull_request_id": pull_id, "merged": True, "result": merged}


# =============================================================================
# AWS Lambda wiring
# =============================================================================
_SECRET_CACHE: dict[str, dict[str, str]] = {}


def _load_lakefs_secret(secret_arn: str) -> dict[str, str]:
    if secret_arn in _SECRET_CACHE:
        return _SECRET_CACHE[secret_arn]
    import boto3  # imported here so unit tests need no AWS deps

    sm = boto3.client("secretsmanager")
    value = sm.get_secret_value(SecretId=secret_arn)["SecretString"]
    creds = json.loads(value)
    _SECRET_CACHE[secret_arn] = creds
    return creds


def _extract_identity(event: dict[str, Any], context: Any) -> tuple[str, str, str]:
    """Resolve (tool_name, session_id, trace_id) from the Gateway event/context.

    AgentCore delivers the invoked tool name and session identifiers via the
    Lambda client context. Key names are read defensively; adjust here if your
    AgentCore version uses different keys.
    """
    tool_name = ""
    session_id = ""
    custom: dict[str, Any] = {}
    client_context = getattr(context, "client_context", None)
    if client_context is not None:
        custom = getattr(client_context, "custom", None) or {}
        tool_name = custom.get("bedrockAgentCoreToolName", "") or custom.get(
            "toolName", ""
        )
        session_id = custom.get("bedrockAgentCoreSessionId", "") or custom.get(
            "sessionId", ""
        )
    # Tool name may be namespaced as "<target>___<tool>".
    if "___" in tool_name:
        tool_name = tool_name.split("___", 1)[1]
    tool_name = tool_name or event.get("__tool_name__", "") or event.get("tool", "")
    trace_id = (
        getattr(context, "aws_request_id", "")
        or custom.get("traceId", "")
        or "trace-unknown"
    )
    session_id = session_id or f"session-{trace_id}"
    return tool_name, session_id, trace_id


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    tool_name, session_id, trace_id = _extract_identity(event or {}, context)
    args = {k: v for k, v in (event or {}).items() if not k.startswith("__")}
    args.pop("tool", None)

    print(
        json.dumps(
            {"tool": tool_name, "session": session_id, "args": redact(args)}
        )
    )

    try:
        secret_arn = os.environ["LAKEFS_SECRET_ARN"]
        creds = _load_lakefs_secret(secret_arn)
        client = LakeFSClient(
            creds["endpoint"], creds["access_key_id"], creds["secret_access_key"]
        )
        caps = Capabilities(
            client,
            run_id=os.environ["RUN_ID"],
            repository=os.environ["LAKEFS_REPOSITORY"],
            source_branch=os.environ.get("SOURCE_BRANCH", "main"),
            source_commit=os.environ.get("SOURCE_COMMIT"),
            baseline_commit=os.environ.get("BASELINE_COMMIT"),
            session_id=session_id,
            trace_id=trace_id,
            task=os.environ.get("AGENT_TASK", ""),
        )
        result = caps.dispatch(tool_name, args)
        return result
    except CapabilityError as exc:
        return exc.to_dict()
    except LakeFSError as exc:
        return {"error": "lakefs_error", "message": str(exc)}
    except Exception as exc:  # never leak stack traces / secrets
        return {"error": "internal_error", "message": exc.__class__.__name__}


# Convenience for building a fully-populated provenance dict from a source object
# and the run identity -- used by the agent tooling and tests.
def make_provenance(
    caps: Capabilities, *, source_path: str, content_hash: str, action: str
) -> dict[str, Any]:
    return build_provenance(
        source_repository=caps.repository,
        source_branch=caps.source_branch,
        source_commit=caps.source_commit or "",
        original_object_path=source_path,
        original_content_hash=content_hash,
        agentcore_session_id=caps.session_id,
        agentcore_trace_id=caps.trace_id,
        action_taken=action,
        processing_timestamp=utc_now_iso(),
    )
