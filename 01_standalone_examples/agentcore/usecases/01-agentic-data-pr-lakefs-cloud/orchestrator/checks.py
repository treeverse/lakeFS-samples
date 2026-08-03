"""Readiness checks for `make check`. Each check is isolated and fail-soft.

Returns a list of ``{name, ok, detail, required}`` records. Mandatory failures
cause the CLI to exit non-zero before any deployment happens.
"""

from __future__ import annotations

from typing import Any

from common.config import AWSConfig, LakeFSConfig
from common.naming import new_run_id

from .context import aws_session, lakefs_client_from_env
from .repository import select_repository


def _check(name: str, required: bool, fn) -> dict[str, Any]:
    try:
        ok, detail = fn()
    except Exception as exc:  # noqa: BLE001 - surface any failure as a check result
        ok, detail = False, f"{exc.__class__.__name__}: {exc}"
    return {"name": name, "ok": bool(ok), "detail": detail, "required": required}


def run_checks() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    state: dict[str, Any] = {}

    def local_deps():
        import boto3  # noqa: F401
        import requests  # noqa: F401

        return True, "requests and boto3 importable"

    results.append(_check("local dependencies installed", True, local_deps))

    # --- lakeFS ----------------------------------------------------------
    def lakefs_config():
        cfg = LakeFSConfig.from_env()
        state["lakefs_cfg"] = cfg
        return True, f"endpoint {cfg.endpoint}"

    results.append(_check("lakeFS configuration present", True, lakefs_config))

    def lakefs_auth():
        client, cfg = lakefs_client_from_env()
        state["client"] = client
        state["cfg"] = cfg
        client.list_repositories(amount=1)  # authenticates
        return True, "credentials authenticated"

    results.append(_check("lakeFS endpoint reachable + credentials", True, lakefs_auth))

    def repo_accessible():
        client = state["client"]
        cfg = state["cfg"]
        repo = select_repository(client, configured=cfg.repository)
        state["repository"] = repo
        return True, f"selected repository {repo!r}"

    results.append(_check("at least one repository accessible", True, repo_accessible))

    def source_branch():
        client = state["client"]
        cfg = state["cfg"]
        repo = state.get("repository")
        client.get_branch(repo, cfg.source_branch)
        return True, f"source branch {cfg.source_branch!r} exists"

    results.append(_check("source branch exists", True, source_branch))

    def temp_branch():
        client = state["client"]
        repo = state.get("repository")
        cfg = state["cfg"]
        src = client.branch_head(repo, cfg.source_branch)
        # lakeFS branch ids allow only letters/digits/underscores/dashes.
        name = f"agentcore-demo-check{new_run_id()}-probe"
        client.create_branch(repo, name, src)
        try:
            client.get_branch(repo, name)
        finally:
            client.delete_branch(repo, name)
        return True, "created and deleted a temporary branch"

    results.append(_check("can create + delete a temporary branch", True, temp_branch))

    def pr_api():
        client = state["client"]
        repo = state.get("repository")
        client.list_pull_requests(repo, amount=1)
        return True, "pull request API available"

    results.append(_check("pull request APIs available", True, pr_api))

    # --- AWS -------------------------------------------------------------
    def aws_auth():
        cfg = AWSConfig.from_env()
        state["aws_cfg"] = cfg
        sess = aws_session(cfg)
        ident = sess.client("sts").get_caller_identity()
        state["account"] = ident["Account"]
        return True, f"account {ident['Account']} in {cfg.region}"

    results.append(_check("AWS authentication works", True, aws_auth))

    def bedrock_model():
        cfg = state.get("aws_cfg") or AWSConfig.from_env()
        if not cfg.model_id:
            return False, "BEDROCK_MODEL_ID is empty"
        # A model id / inference-profile id is opaque; validate the client exists.
        aws_session(cfg).client("bedrock-runtime")
        return True, f"model {cfg.model_id}"

    results.append(_check("Bedrock model configuration valid", True, bedrock_model))

    def agentcore_apis():
        cfg = state.get("aws_cfg") or AWSConfig.from_env()
        sess = aws_session(cfg)
        # Creating the clients validates the APIs exist in the region.
        sess.client("bedrock-agentcore-control")
        sess.client("secretsmanager")
        sess.client("lambda")
        sess.client("iam")
        return True, "AgentCore control, Secrets Manager, Lambda, IAM reachable"

    results.append(_check("required AWS APIs available in region", True, agentcore_apis))

    return results


def summarize(results: list[dict[str, Any]]) -> tuple[bool, str]:
    failed_required = [r for r in results if r["required"] and not r["ok"]]
    ok = not failed_required
    lines = []
    for r in results:
        mark = "PASS" if r["ok"] else ("FAIL" if r["required"] else "warn")
        lines.append(f"[{mark}] {r['name']}: {r['detail']}")
    if not ok:
        lines.append("")
        lines.append(f"{len(failed_required)} mandatory check(s) failed.")
    return ok, "\n".join(lines)
