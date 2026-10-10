"""Streamlit demo UI. Launch with `make demo`.

This UI is a thin view over the SAME orchestrator functions the CLI uses -- it
contains no business logic of its own. It never displays credentials, auth
headers, secret ARNs, private reasoning, or signed URLs.

    AgentCore governs what the agent is allowed to do.
    lakeFS governs what happens to the data it changes.
"""

from __future__ import annotations

import json

import streamlit as st

from common.config import LakeFSConfig
from common.runstate import RunState
from gateway.handler import Capabilities
from orchestrator import approval
from orchestrator.context import lakefs_client_from_env
from orchestrator.curation_runner import run_local_curation
from orchestrator.repository import select_repository
from orchestrator.seeding import seed_run, verify_source_unchanged

st.set_page_config(page_title="Agentic Data PRs — AgentCore + lakeFS Cloud", page_icon="🔀", layout="wide")


def _load_current_run() -> RunState | None:
    try:
        return RunState.load_current()
    except Exception:
        return None


def _caps(rs: RunState, client) -> Capabilities:
    return Capabilities(
        client,
        run_id=rs.run_id,
        repository=rs.repository,
        source_branch=rs.source_branch,
        source_commit=rs.source_commit,
        baseline_commit=rs.baseline_commit,
        session_id="ui",
        trace_id="ui",
    )


st.title("Agentic Data PRs")
st.caption(
    "**AgentCore governs what the agent is allowed to do. "
    "lakeFS governs what happens to the data it changes.**"
)

# --- connection (no credentials shown) --------------------------------------
try:
    cfg = LakeFSConfig.from_env()
except Exception as exc:  # noqa: BLE001
    st.error(f"Configuration missing: {exc}. Run `make configure`.")
    st.stop()

client, _ = lakefs_client_from_env()

with st.sidebar:
    st.header("Connection")
    st.write(f"**lakeFS endpoint:** {cfg.endpoint}")
    st.caption("Credentials are never displayed. This connects to an existing "
               "lakeFS Cloud installation and does not configure it.")
    rs = _load_current_run()
    if rs is None:
        st.info("No active run. Seed one below.")
        repo = select_repository(client, cfg.repository)
        st.write(f"Repository: **{repo}**  |  source: **{cfg.source_branch}**")
        if st.button("Seed a new run", type="primary"):
            with st.spinner("Creating baseline + workspace branches, seeding corpus..."):
                seed_run(client, repo, cfg.source_branch)
            st.rerun()
        st.stop()
    st.success(f"Run {rs.run_id}")
    st.write(f"Stage: **{rs.stage}**")

scope = rs.scope

# --- story panel ------------------------------------------------------------
c1, c2, c3 = st.columns(3)
c1.metric("Repository", rs.repository)
c1.write(f"Source: `{rs.source_branch}`")
c1.caption(f"@ {rs.source_commit}")
c2.write(f"Baseline: `{scope.baseline_branch}`")
c2.caption(f"@ {rs.baseline_commit}")
c3.write(f"Workspace: `{scope.workspace_branch}`")
c3.caption(f"@ {rs.workspace_commit or '(not yet curated)'}")

st.divider()

# --- run curation -----------------------------------------------------------
st.subheader("1 — Agent curation")
st.write("The agent reads the corpus on the baseline and writes only to its "
         "workspace output prefix. Deterministic, fail-closed validation decides "
         "the outcome — the agent cannot declare its own output valid.")
colr1, colr2 = st.columns([1, 3])
if colr1.button("Run curation (local, no AWS)"):
    with st.spinner("Curating, validating, and creating the Pull Request..."):
        try:
            run_local_curation(rs)
            st.rerun()
        except Exception as exc:  # noqa: BLE001
            st.error(f"Run failed: {exc}")
colr2.caption("Use the CLI `make run` to invoke the deployed AgentCore agent.")

# --- discovered objects + decisions -----------------------------------------
caps = _caps(rs, client)
try:
    listing = caps.list_demo_objects({})
    st.write(f"**Objects discovered:** {len(listing['objects'])} under `{scope.corpus_prefix}`")
except Exception as exc:  # noqa: BLE001
    st.warning(f"Could not list corpus: {exc}")
    listing = {"objects": []}

if rs.pull_request_id:
    try:
        report_raw = client.get_object(rs.repository, scope.workspace_branch, scope.report_json_path)
        report = json.loads(report_raw.decode("utf-8"))
        st.subheader("2 — Curation decisions")
        rec = report.get("reconciliation", {})
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Examined", rec.get("examined"))
        m2.metric("Curated", rec.get("curated"))
        m3.metric("Corrected", rec.get("corrected"))
        m4.metric("Quarantined", rec.get("quarantined"))
        with st.expander("Curated"):
            st.table(report.get("curated", []))
        with st.expander("Quarantined (with reason codes)"):
            st.table(report.get("quarantined", []))
    except Exception as exc:  # noqa: BLE001
        st.info(f"Reports not available yet: {exc}")

    # --- validation + diff --------------------------------------------------
    st.subheader("3 — Validation and data diff")
    try:
        validation = caps.validate_workspace({})
        (st.success if validation["valid"] else st.error)(
            f"Deterministic validation: {'PASSED' if validation['valid'] else 'FAILED'}"
        )
        with st.expander("Validation checks"):
            st.json(validation)
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Validation unavailable: {exc}")
    try:
        diff = caps.diff_workspace({})
        st.write(f"Diff `{scope.baseline_branch}` → `{scope.workspace_branch}`: "
                 f"+{diff['summary'].get('added',0)} / ~{diff['summary'].get('changed',0)} / "
                 f"-{diff['summary'].get('removed',0)}")
        with st.expander("Changed objects"):
            st.table(diff["changes"])
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Diff unavailable: {exc}")

    # --- policy denial ------------------------------------------------------
    st.subheader("4 — AgentCore policy denial")
    st.write("The agent identity is forbidden from merging. Run "
             "`make test-policy-denial` for the live Gateway proof; the merge tool "
             "here on the Gateway is denied by Cedar (forbid-wins).")

    # --- pull request + human approval -------------------------------------
    st.subheader("5 — Pull Request & human approval")
    st.write(f"Pull Request **{rs.pull_request_id}** ({rs.pull_request_status})")
    st.write(f"[Open in lakeFS Cloud]({client.pull_request_web_url(rs.repository, rs.pull_request_id)})")
    st.caption(f"AgentCore trace id: {rs.agentcore_trace_id}")

    ca, cb = st.columns(2)
    with ca:
        confirm = st.checkbox("I reviewed the diff and validation")
        if st.button("Approve & merge", type="primary", disabled=not confirm):
            try:
                result = approval.approve(rs, rs.pull_request_id, confirm=lambda _c: True)
                st.success(f"Merged. Baseline {result['baseline_before'][:8]} → "
                           f"{result['baseline_after'][:8]}. "
                           f"Source unchanged: {result['source_unchanged']}.")
                st.rerun()
            except Exception as exc:  # noqa: BLE001
                st.error(f"Approval failed: {exc}")
    with cb:
        if st.button("Reject (close without merge)"):
            try:
                approval.reject(rs, rs.pull_request_id)
                st.rerun()
            except Exception as exc:  # noqa: BLE001
                st.error(f"Reject failed: {exc}")

# --- source integrity -------------------------------------------------------
st.divider()
st.subheader("Source-branch integrity")
try:
    unchanged = verify_source_unchanged(client, rs)
    (st.success if unchanged else st.error)(
        f"Source branch `{rs.source_branch}` HEAD is "
        f"{'unchanged' if unchanged else 'CHANGED!'}: {rs.source_commit}"
    )
except Exception as exc:  # noqa: BLE001
    st.warning(f"Could not verify: {exc}")
