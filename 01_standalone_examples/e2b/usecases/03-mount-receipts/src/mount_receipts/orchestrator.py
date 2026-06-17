"""Host orchestration: branch → sandbox → mount → phased agent → commit → gate → merge.

The host owns lakeFS branch creation and the final merge; the agent (inside the sandbox)
owns the work on the mounted filesystem. Each phase is committed with ``everest commit``,
so the branch ends up with a progressive, auditable commit per phase.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime

from e2b import Sandbox

from lakefs_e2b_common.lakefs_client import (
    commit,
    create_branch,
    get_or_create_repo,
    make_client,
    merge_branch,
    write_object,
)
from mount_receipts import e2b_session as sess
from mount_receipts.config import Config

PHASES = [
    ("triage", "Triage (structural)"),
    ("extract", "Extract (multimodal)"),
    ("validate", "Validate (business rules)"),
]

# Agent output artefacts. A merging run promotes these to `main`, so a re-run would
# otherwise branch from a "polluted" main and the phases would no-op. We clear them on
# the fresh agent branch first, so every run starts from a clean inbox and produces real
# per-phase commits. (The branch descends from main, so the later merge stays conflict-free.)
OUTPUT_FILES = ["ledger.csv", "rejects.csv", "ledger_draft.json", "triage.json", "run_manifest.json"]
OUTPUT_PREFIXES = ["sidecars/", "validation/"]


def _reset_branch_outputs(repo, branch_name: str) -> int:
    """Delete prior agent outputs from the branch so the run starts inbox-only."""
    b = repo.branch(branch_name)
    to_delete = [f for f in OUTPUT_FILES if b.object(f).exists()]
    for prefix in OUTPUT_PREFIXES:
        to_delete.extend(o.path for o in b.objects(prefix=prefix, max_amount=100000))
    if to_delete:
        b.delete_objects(to_delete)
        commit(b, "Prepare clean run: clear prior agent outputs", {"reset": "true"})
    return len(to_delete)


@dataclass
class RunResult:
    branch_name: str
    sandbox_id: str
    sandbox_url: str
    passed: bool
    merged: bool
    phases: list[dict] = field(default_factory=list)
    validation: dict | None = None
    gate_blocked: bool = False      # the lakeFS pre-merge gate rejected the merge
    gate_message: str = ""          # the gate's reason, when it blocked


def _divider() -> None:
    print("─" * 60)


def _extract_gate_reason(msg: str) -> str:
    """Pull the human-readable gate reason out of a lakeFS pre-merge hook error blob."""
    marker = "Pre-merge gate"
    i = msg.find(marker)
    reason = msg[i:] if i != -1 else msg
    # Trim the Lua stack traceback that lakeFS appends after the message.
    for cut in ("\\nstack traceback", "\nstack traceback"):
        j = reason.find(cut)
        if j != -1:
            reason = reason[:j]
    return reason.strip()


def run(cfg: Config, *, source_branch: str | None = None, do_merge: bool = True, keep_sandbox: bool = False) -> RunResult:
    source = source_branch or cfg.lakefs_branch
    client = make_client(cfg.lakefs_endpoint, cfg.lakefs_access_key_id, cfg.lakefs_secret_access_key)
    repo = get_or_create_repo(client, cfg.lakefs_repository, cfg.lakefs_storage_namespace)

    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    branch_name = f"agent-run-{ts}"
    print(f"  Creating branch '{branch_name}' from '{source}'...")
    create_branch(repo, branch_name, source)
    cleared = _reset_branch_outputs(repo, branch_name)
    if cleared:
        print(f"  Cleared {cleared} prior output object(s) — starting from a clean inbox.")

    use_template = bool(cfg.e2b_template)
    create_kwargs = {"api_key": cfg.e2b_api_key, "envs": cfg.sandbox_envs(), "timeout": 900}
    if use_template:
        create_kwargs["template"] = cfg.e2b_template
    sbx = Sandbox.create(**create_kwargs)
    print(f"  Sandbox: {sbx.sandbox_id}  (template={cfg.e2b_template or 'default+runtime-install'})")

    phase_summaries: list[dict] = []
    validation: dict | None = None
    try:
        print("  Provisioning sandbox (everest + fuse + agent)..." if not use_template else "  Using prebaked template...")
        sess.provision(sbx, use_template=use_template)
        print(f"  Mounting lakefs://{cfg.lakefs_repository}/{branch_name} (FUSE, write-mode)...")
        sess.mount(sbx, cfg.lakefs_repository, branch_name)

        for i, (phase, name) in enumerate(PHASES, 1):
            _divider()
            print(f"  Phase {i} — {name}")
            _divider()
            r = sess.run_phase(sbx, phase)
            summary_line = (r.stdout or r.stderr).strip().splitlines()[-1] if (r.stdout or r.stderr).strip() else ""
            print(f"    {summary_line}")
            commit_res = sess.commit(sbx, f"Phase {i} — {name}")
            commit_id = ""
            for ln in commit_res.stdout.splitlines():
                if ln.strip().startswith("ID:"):
                    commit_id = ln.split("ID:", 1)[1].strip()
                    break
            print(f"    committed: {commit_id[:12] or '(no changes)'}")
            phase_summaries.append({"phase": phase, "name": name, "summary": summary_line, "commit_id": commit_id})

        result_json = sess.read_mount_file(sbx, "validation/latest_result.json")
        validation = json.loads(result_json)
        # Keep the mount live when keeping the sandbox, so it can be browsed (e.g. the
        # E2B dashboard Filesystem tab shows /home/user/mnt as the live lakeFS branch).
        if not keep_sandbox:
            sess.umount(sbx)
    finally:
        if not keep_sandbox:
            try:
                sbx.kill()
            except Exception:
                pass

    # Tie the lakeFS branch to the exact E2B sandbox that produced it. `everest commit`
    # can't set lakeFS commit metadata, so the host adds a manifest commit via the SDK
    # carrying a clickable "E2B Sandbox" link (the ::lakefs::<name>::url[url:ui] convention)
    # — restoring the E2B↔lakeFS link the SDK-based use cases show in their commits.
    sandbox_url = f"https://e2b.dev/dashboard/{cfg.e2b_team}/sandboxes/{sbx.sandbox_id}/monitoring" if cfg.e2b_team else ""
    try:
        b = repo.branch(branch_name)
        write_object(b, "run_manifest.json", json.dumps({
            "sandbox_id": sbx.sandbox_id,
            "sandbox_url": sandbox_url,
            "model": cfg.openai_model,
            "phases": phase_summaries,
            "validation": validation,
        }, indent=2))
        meta = {
            "model": cfg.openai_model,
            "sandbox_id": sbx.sandbox_id,
            "validation_status": (validation or {}).get("status", "unknown"),
        }
        if sandbox_url:
            meta["::lakefs::E2B Sandbox::url[url:ui]"] = sandbox_url
        commit(b, "Agent run manifest — E2B sandbox link", meta)
        print(f"  Run manifest committed (E2B sandbox: {sandbox_url or 'n/a — set E2B_TEAM'})")
    except Exception as exc:
        print(f"  (could not write run manifest: {exc})")

    passed = bool(validation and validation.get("status") == "passed")
    merged = False
    gate_blocked = False
    gate_message = ""
    if passed and do_merge:
        print(f"\n  Merging '{branch_name}' into '{source}'...")
        try:
            merge_ref = merge_branch(repo.branch(branch_name), repo.branch(source))
            merged = True
            print(f"  Merged. ref: {merge_ref}")
        except Exception as exc:
            # A pre-merge hook rejection is an EXPECTED outcome — the gate independently
            # re-validated the (LLM-written) ledger and refused a policy violation. Report
            # it cleanly instead of crashing; the branch is left un-merged for inspection.
            msg = str(exc)
            if "pre-merge hook" in msg or "Pre-merge gate" in msg:
                gate_blocked = True
                gate_message = _extract_gate_reason(msg)
                print(f"  Pre-merge gate BLOCKED the merge — '{branch_name}' left un-merged.")
                print(f"  Gate: {gate_message}")
            else:
                raise
    elif not passed:
        print(f"\n  Validation did not pass — '{branch_name}' left un-merged for inspection.")

    return RunResult(
        branch_name=branch_name,
        sandbox_id=sbx.sandbox_id,
        sandbox_url=sandbox_url,
        passed=passed,
        merged=merged,
        phases=phase_summaries,
        validation=validation,
        gate_blocked=gate_blocked,
        gate_message=gate_message,
    )


def print_report(cfg: Config, result: RunResult) -> None:
    _divider()
    print("  Final Report")
    _divider()
    print(f"  Branch   : {result.branch_name}")
    print(f"  Sandbox  : {result.sandbox_id}")
    if result.sandbox_url:
        print(f"  E2B link : {result.sandbox_url}")
    print(f"  Outcome  : {'PASSED' if result.passed else 'FAILED'}   merged={result.merged}")
    if result.validation:
        v = result.validation
        print(f"  Ledger   : {v.get('accepted')} accepted, {v.get('rejected')} rejected, {v.get('dropped')} dropped")
        print(f"  Summary  : {v.get('summary')}")
    if result.gate_blocked:
        # The validator passed its own check, but lakeFS's independent gate caught a
        # policy violation it missed — exactly what the gate exists for.
        print(f"  Gate     : BLOCKED MERGE — the LLM-written validator self-reported pass,")
        print(f"             but lakeFS independently rejected: {result.gate_message}")
    print(f"  lakeFS UI: {cfg.lakefs_endpoint}/repositories/{cfg.lakefs_repository}/objects?ref={result.branch_name}")
    _divider()
